"""Add customer source/address columns to stg_uc_orders."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0068_add_uc_stg_customer_fields"
down_revision = "0067_add_uc_archive_stg_tables"
branch_labels = None
depends_on = None


def _has_column(table_name: str, column_name: str) -> bool:
    inspector = sa.inspect(op.get_bind())
    return column_name in {column["name"] for column in inspector.get_columns(table_name)}


def upgrade() -> None:
    with op.batch_alter_table("stg_uc_orders") as batch_op:
        if not _has_column("stg_uc_orders", "customer_source"):
            batch_op.add_column(sa.Column("customer_source", sa.String(length=64), nullable=True))
        if not _has_column("stg_uc_orders", "customer_address"):
            batch_op.add_column(sa.Column("customer_address", sa.Text(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("stg_uc_orders") as batch_op:
        if _has_column("stg_uc_orders", "customer_address"):
            batch_op.drop_column("customer_address")
        if _has_column("stg_uc_orders", "customer_source"):
            batch_op.drop_column("customer_source")
