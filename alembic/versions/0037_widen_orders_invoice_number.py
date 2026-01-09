"""Widen invoice_number on orders table."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0037_widen_orders_invoice_number"
down_revision = "0036_widen_uc_invoice_number"
branch_labels = None
depends_on = None


def _get_invoice_number_column(table_name: str) -> dict[str, object] | None:
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table(table_name):
        return None
    for column in inspector.get_columns(table_name):
        if column.get("name") == "invoice_number":
            return column
    return None


def _alter_invoice_number(table_name: str, length: int, *, widen: bool) -> None:
    column = _get_invoice_number_column(table_name)
    if not column:
        return
    current_type = column.get("type")
    current_length = getattr(current_type, "length", None)
    if current_length == length:
        return
    if current_length is not None:
        if widen and current_length > length:
            return
        if not widen and current_length < length:
            return
    with op.batch_alter_table(table_name) as batch_op:
        batch_op.alter_column(
            "invoice_number",
            type_=sa.String(length=length),
            existing_type=sa.String(length=current_length),
            existing_nullable=column.get("nullable", True),
        )


def upgrade() -> None:
    _alter_invoice_number("orders", 20, widen=True)


def downgrade() -> None:
    _alter_invoice_number("orders", 12, widen=False)
