"""Add UC archive staging tables for base/order/payment detail ingests.

This revision introduces archive-only staging tables used by the UC archive
ingestion path. Existing GST UC staging (`stg_uc_orders`) is intentionally left
untouched.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0067_add_uc_archive_stg_tables"
down_revision = "0066_mark_adjusted_sales_edited"
branch_labels = None
depends_on = None


def _has_table(table_name: str) -> bool:
    return sa.inspect(op.get_bind()).has_table(table_name)


def upgrade() -> None:
    if not _has_table("stg_uc_archive_orders_base"):
        op.create_table(
            "stg_uc_archive_orders_base",
            sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
            sa.Column("run_id", sa.Text()),
            sa.Column("run_date", sa.DateTime(timezone=True)),
            sa.Column("cost_center", sa.String(length=8)),
            sa.Column("store_code", sa.String(length=8)),
            sa.Column("ingest_remarks", sa.Text()),
            sa.Column("order_code", sa.String(length=24)),
            sa.Column("pickup_raw", sa.Text()),
            sa.Column("delivery_raw", sa.Text()),
            sa.Column("customer_name", sa.String(length=128)),
            sa.Column("customer_phone", sa.String(length=24)),
            sa.Column("address", sa.Text()),
            sa.Column("payment_text", sa.Text()),
            sa.Column("instructions", sa.Text()),
            sa.Column("customer_source", sa.String(length=64)),
            sa.Column("status", sa.String(length=32)),
            sa.Column("status_date_raw", sa.Text()),
            sa.Column("source_file", sa.Text()),
        )

    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_stg_uc_archive_orders_base_store_order
        ON stg_uc_archive_orders_base (store_code, order_code)
        """
    )

    if not _has_table("stg_uc_archive_order_details"):
        op.create_table(
            "stg_uc_archive_order_details",
            sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
            sa.Column("run_id", sa.Text()),
            sa.Column("run_date", sa.DateTime(timezone=True)),
            sa.Column("cost_center", sa.String(length=8)),
            sa.Column("store_code", sa.String(length=8)),
            sa.Column("ingest_remarks", sa.Text()),
            sa.Column("order_code", sa.String(length=24)),
            sa.Column("order_mode", sa.String(length=64)),
            sa.Column("order_datetime_raw", sa.Text()),
            sa.Column("pickup_datetime_raw", sa.Text()),
            sa.Column("delivery_datetime_raw", sa.Text()),
            sa.Column("service", sa.Text()),
            sa.Column("hsn_sac", sa.String(length=32)),
            sa.Column("item_name", sa.Text()),
            sa.Column("rate", sa.Numeric(12, 2)),
            sa.Column("quantity", sa.Numeric(12, 2)),
            sa.Column("weight", sa.Numeric(12, 3)),
            sa.Column("addons", sa.Text()),
            sa.Column("amount", sa.Numeric(12, 2)),
            sa.Column("line_hash", sa.String(length=64)),
            sa.Column("source_file", sa.Text()),
        )

    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_stg_uc_archive_order_details_store_order_line
        ON stg_uc_archive_order_details (store_code, order_code, line_hash)
        """
    )

    if not _has_table("stg_uc_archive_payment_details"):
        op.create_table(
            "stg_uc_archive_payment_details",
            sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
            sa.Column("run_id", sa.Text()),
            sa.Column("run_date", sa.DateTime(timezone=True)),
            sa.Column("cost_center", sa.String(length=8)),
            sa.Column("store_code", sa.String(length=8)),
            sa.Column("ingest_remarks", sa.Text()),
            sa.Column("order_code", sa.String(length=24)),
            sa.Column("payment_mode", sa.String(length=32)),
            sa.Column("amount", sa.Numeric(12, 2)),
            sa.Column("payment_date_raw", sa.Text()),
            sa.Column("transaction_id", sa.String(length=128)),
            sa.Column("source_file", sa.Text()),
        )

    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_stg_uc_archive_payment_details_idempotency
        ON stg_uc_archive_payment_details (
            store_code,
            order_code,
            payment_date_raw,
            payment_mode,
            amount,
            COALESCE(transaction_id, '')
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS uq_stg_uc_archive_payment_details_idempotency")
    op.execute("DROP INDEX IF EXISTS uq_stg_uc_archive_order_details_store_order_line")
    op.execute("DROP INDEX IF EXISTS uq_stg_uc_archive_orders_base_store_order")

    if _has_table("stg_uc_archive_payment_details"):
        op.drop_table("stg_uc_archive_payment_details")
    if _has_table("stg_uc_archive_order_details"):
        op.drop_table("stg_uc_archive_order_details")
    if _has_table("stg_uc_archive_orders_base"):
        op.drop_table("stg_uc_archive_orders_base")

