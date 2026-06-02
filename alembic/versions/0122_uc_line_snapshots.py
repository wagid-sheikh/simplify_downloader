"""Add UC line snapshot outcomes."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0122_uc_line_snapshots"
down_revision = "0121_td_leads_wrapper_ops"
branch_labels = None
depends_on = None

INVALID_DETAIL_UQ = "uq_stg_uc_archive_order_details_store_order_line"
OUTCOME_UQ = "uq_uc_order_detail_snapshots_run_order"


def _has_table(table_name: str) -> bool:
    return sa.inspect(op.get_bind()).has_table(table_name)


def _has_column(table_name: str, column_name: str) -> bool:
    if not _has_table(table_name):
        return False
    return column_name in {column["name"] for column in sa.inspect(op.get_bind()).get_columns(table_name)}


def upgrade() -> None:
    if _has_table("order_line_items") and not _has_column("order_line_items", "line_sequence"):
        with op.batch_alter_table("order_line_items") as batch_op:
            batch_op.add_column(sa.Column("line_sequence", sa.Integer(), nullable=True))

    if _has_table("stg_uc_archive_order_details") and not _has_column("stg_uc_archive_order_details", "ingest_row_seq"):
        with op.batch_alter_table("stg_uc_archive_order_details") as batch_op:
            batch_op.add_column(sa.Column("ingest_row_seq", sa.Integer(), nullable=True))

    op.execute(f"DROP INDEX IF EXISTS {INVALID_DETAIL_UQ}")

    if not _has_table("stg_uc_order_detail_snapshots"):
        op.create_table(
            "stg_uc_order_detail_snapshots",
            sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), primary_key=True, autoincrement=True),
            sa.Column("run_id", sa.Text(), nullable=False),
            sa.Column("run_date", sa.DateTime(timezone=True)),
            sa.Column("cost_center", sa.String(length=8)),
            sa.Column("store_code", sa.String(length=8), nullable=False),
            sa.Column("order_code", sa.String(length=24), nullable=False),
            sa.Column("normalized_order_number", sa.String(length=32), nullable=False),
            sa.Column("snapshot_outcome", sa.String(length=32), nullable=False),
            sa.Column("detail_row_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("source_file", sa.Text()),
            sa.Column("ingest_remarks", sa.Text()),
            sa.CheckConstraint(
                "snapshot_outcome IN ('complete_with_rows', 'complete_empty', 'incomplete_or_failed')",
                name="ck_uc_order_detail_snapshot_outcome",
            ),
        )

    op.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS {OUTCOME_UQ}
        ON stg_uc_order_detail_snapshots (run_id, store_code, normalized_order_number)
        """
    )


def downgrade() -> None:
    # Forward-only migration: intentionally do not restore the invalid UC staging
    # uniqueness constraint because it collapses legitimate duplicate line rows.
    pass
