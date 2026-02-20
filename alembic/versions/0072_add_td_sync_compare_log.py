"""Add table for TD API/UI compare logs."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0072_add_td_sync_compare_log"
down_revision = "0071_add_td_garment_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "td_sync_compare_log",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("run_env", sa.String(length=32), nullable=False),
        sa.Column("store_code", sa.String(length=8), nullable=False),
        sa.Column("from_date", sa.Date(), nullable=False),
        sa.Column("to_date", sa.Date(), nullable=False),
        sa.Column("source_mode", sa.String(length=16), nullable=False),
        sa.Column("total_rows", sa.BigInteger()),
        sa.Column("matched_rows", sa.BigInteger()),
        sa.Column("missing_in_api", sa.BigInteger()),
        sa.Column("missing_in_ui", sa.BigInteger()),
        sa.Column("amount_mismatches", sa.BigInteger()),
        sa.Column("status_mismatches", sa.BigInteger()),
        sa.Column("sample_mismatch_keys", sa.JSON()),
        sa.Column("decision", sa.Text()),
        sa.Column("reason", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index(
        "ix_td_sync_compare_log_run_id_store_code",
        "td_sync_compare_log",
        ["run_id", "store_code"],
    )
    op.create_index(
        "ix_td_sync_compare_log_store_code_from_date_to_date",
        "td_sync_compare_log",
        ["store_code", "from_date", "to_date"],
    )


def downgrade() -> None:
    op.drop_index("ix_td_sync_compare_log_store_code_from_date_to_date", table_name="td_sync_compare_log")
    op.drop_index("ix_td_sync_compare_log_run_id_store_code", table_name="td_sync_compare_log")
    op.drop_table("td_sync_compare_log")
