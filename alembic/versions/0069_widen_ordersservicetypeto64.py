"""Widen orders.service_type from 24 to 64 chars."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0069_widen_ordersservicetypeto64"
down_revision = "0068_add_uc_stg_customer_fields"
branch_labels = None
depends_on = None


def _service_type_length() -> int | None:
    inspector = sa.inspect(op.get_bind())
    for column in inspector.get_columns("orders"):
        if column["name"] != "service_type":
            continue
        col_type = column.get("type")
        return getattr(col_type, "length", None)
    return None


def upgrade() -> None:
    current_length = _service_type_length()
    if current_length == 64:
        return
    with op.batch_alter_table("orders") as batch_op:
        batch_op.alter_column(
            "service_type",
            existing_type=sa.String(length=current_length or 24),
            type_=sa.String(length=64),
            existing_nullable=True,
        )


def downgrade() -> None:
    current_length = _service_type_length()
    if current_length == 24:
        return
    with op.batch_alter_table("orders") as batch_op:
        batch_op.alter_column(
            "service_type",
            existing_type=sa.String(length=current_length or 64),
            type_=sa.String(length=24),
            existing_nullable=True,
        )
