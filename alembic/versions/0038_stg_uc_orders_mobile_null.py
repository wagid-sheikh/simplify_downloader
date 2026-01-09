"""Make stg_uc_orders.mobile_number nullable."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0038_stg_uc_orders_mobile_null"
down_revision = "0037_widen_orders_invoice_number"
branch_labels = None
depends_on = None


def _get_mobile_number_column(table_name: str) -> dict[str, object] | None:
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table(table_name):
        return None
    for column in inspector.get_columns(table_name):
        if column.get("name") == "mobile_number":
            return column
    return None


def upgrade() -> None:
    column = _get_mobile_number_column("stg_uc_orders")
    if not column or column.get("nullable", True):
        return
    with op.batch_alter_table("stg_uc_orders") as batch_op:
        batch_op.alter_column(
            "mobile_number",
            existing_type=column.get("type", sa.String(length=16)),
            existing_nullable=False,
            nullable=True,
        )


def downgrade() -> None:
    column = _get_mobile_number_column("stg_uc_orders")
    if not column or not column.get("nullable", False):
        return
    with op.batch_alter_table("stg_uc_orders") as batch_op:
        batch_op.alter_column(
            "mobile_number",
            existing_type=column.get("type", sa.String(length=16)),
            existing_nullable=True,
            nullable=False,
        )
