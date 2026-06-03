"""Add OLI rebuild progress table."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0123_oli_rebuild_progress"
down_revision = "0122_uc_line_snapshots"
branch_labels = None
depends_on = None


def _has_table(table_name: str) -> bool:
    return sa.inspect(op.get_bind()).has_table(table_name)


def upgrade() -> None:
    if not _has_table("order_line_items_rebuild_progress"):
        op.create_table(
            "order_line_items_rebuild_progress",
            sa.Column(
                "id",
                sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
                primary_key=True,
                autoincrement=True,
            ),
            sa.Column("source", sa.String(length=8), nullable=False),
            sa.Column("store_code", sa.String(length=16), nullable=False),
            sa.Column("cost_center", sa.String(length=16)),
            sa.Column("window_start", sa.Date(), nullable=False),
            sa.Column("window_end", sa.Date(), nullable=False),
            sa.Column("run_id", sa.String(length=64), nullable=False),
            sa.Column("status", sa.String(length=64), nullable=False),
            sa.Column(
                "attempt_no", sa.Integer(), nullable=False, server_default=sa.text("1")
            ),
            sa.Column("error_message", sa.Text()),
            sa.Column(
                "complete_with_rows_orders",
                sa.BigInteger(),
                nullable=False,
                server_default=sa.text("0"),
            ),
            sa.Column(
                "complete_empty_orders",
                sa.BigInteger(),
                nullable=False,
                server_default=sa.text("0"),
            ),
            sa.Column(
                "skipped_incomplete_orders",
                sa.BigInteger(),
                nullable=False,
                server_default=sa.text("0"),
            ),
            sa.Column(
                "deleted_rows",
                sa.BigInteger(),
                nullable=False,
                server_default=sa.text("0"),
            ),
            sa.Column(
                "inserted_rows",
                sa.BigInteger(),
                nullable=False,
                server_default=sa.text("0"),
            ),
            sa.Column(
                "orphan_rows",
                sa.BigInteger(),
                nullable=False,
                server_default=sa.text("0"),
            ),
            sa.Column(
                "dry_run", sa.Boolean(), nullable=False, server_default=sa.text("false")
            ),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.text("CURRENT_TIMESTAMP"),
            ),
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.text("CURRENT_TIMESTAMP"),
            ),
            sa.UniqueConstraint(
                "source",
                "store_code",
                "window_start",
                "window_end",
                name="uq_oli_rebuild_progress_window",
            ),
        )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_oli_rebuild_progress_lookup "
        "ON order_line_items_rebuild_progress (source, store_code, status)"
    )


def downgrade() -> None:
    # Forward-only migration: progress history is operational audit data.
    pass
