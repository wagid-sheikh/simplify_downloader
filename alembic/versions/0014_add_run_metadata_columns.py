"""Add run metadata to ingest tables and create nonpackage_orders"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0014_add_run_metadata_columns"
down_revision = "0013_remove_store_lists_config"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("missed_leads", sa.Column("run_id", sa.String(length=64), nullable=True))
    op.add_column("missed_leads", sa.Column("run_date", sa.Date(), nullable=True))

    op.add_column("undelivered_orders", sa.Column("run_id", sa.String(length=64), nullable=True))
    op.add_column("undelivered_orders", sa.Column("run_date", sa.Date(), nullable=True))

    op.create_table(
        "nonpackage_orders",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("store_code", sa.String(), nullable=False),
        sa.Column("store_name", sa.String(), nullable=True),
        sa.Column("mobile_no", sa.String(), nullable=False),
        sa.Column("taxable_amount", sa.Float(), nullable=True),
        sa.Column("order_date", sa.Date(), nullable=False),
        sa.Column("expected_delivery_date", sa.Date(), nullable=True),
        sa.Column("actual_delivery_date", sa.Date(), nullable=True),
        sa.Column("run_id", sa.String(length=64), nullable=True),
        sa.Column("run_date", sa.Date(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("store_code", "mobile_no", name="uq_nonpackage_store_mobile"),
    )


def downgrade() -> None:
    op.drop_constraint("uq_nonpackage_store_mobile", "nonpackage_orders", type_="unique")
    op.drop_table("nonpackage_orders")

    op.drop_column("undelivered_orders", "run_date")
    op.drop_column("undelivered_orders", "run_id")

    op.drop_column("missed_leads", "run_date")
    op.drop_column("missed_leads", "run_id")
