"""Initial tables for simplify downloader"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0001_init"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "missed_leads",
        sa.Column("pickup_row_id", sa.Integer(), nullable=False),
        sa.Column("mobile_number", sa.String(), nullable=False),
        sa.Column("pickup_no", sa.String(), nullable=True),
        sa.Column("pickup_created_date", sa.Date(), nullable=True),
        sa.Column("pickup_created_time", sa.String(), nullable=True),
        sa.Column("store_code", sa.String(), nullable=False),
        sa.Column("store_name", sa.String(), nullable=True),
        sa.Column("pickup_date", sa.Date(), nullable=True),
        sa.Column("pickup_time", sa.String(), nullable=True),
        sa.Column("customer_name", sa.String(), nullable=True),
        sa.Column("special_instruction", sa.String(), nullable=True),
        sa.Column("source", sa.String(), nullable=True),
        sa.Column("final_source", sa.String(), nullable=True),
        sa.Column("customer_type", sa.String(), nullable=True),
        sa.Column("is_order_placed", sa.Boolean(), nullable=True),
        sa.PrimaryKeyConstraint("pickup_row_id"),
        sa.UniqueConstraint("store_code", "mobile_number", name="uq_missed_leads_store_mobile"),
    )
    op.create_table(
        "undelivered_orders",
        sa.Column("order_id", sa.String(), nullable=False),
        sa.Column("order_date", sa.Date(), nullable=True),
        sa.Column("store_code", sa.String(), nullable=True),
        sa.Column("store_name", sa.String(), nullable=True),
        sa.Column("taxable_amount", sa.Float(), nullable=True),
        sa.Column("net_amount", sa.Float(), nullable=True),
        sa.Column("service_code", sa.String(), nullable=True),
        sa.Column("mobile_no", sa.String(), nullable=True),
        sa.Column("status", sa.String(), nullable=True),
        sa.Column("customer_id", sa.String(), nullable=True),
        sa.Column("expected_deliver_on", sa.Date(), nullable=True),
        sa.Column("actual_deliver_on", sa.Date(), nullable=True),
        sa.PrimaryKeyConstraint("order_id"),
        sa.UniqueConstraint("order_id", name="uq_undelivered_order_id"),
    )
    op.create_table(
        "repeat_customers",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("store_code", sa.String(), nullable=False),
        sa.Column("mobile_no", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("store_code", "mobile_no", name="uq_repeat_store_mobile"),
    )


def downgrade() -> None:
    op.drop_table("repeat_customers")
    op.drop_table("undelivered_orders")
    op.drop_table("missed_leads")
