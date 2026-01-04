"""Rename orders.ingest_remarks to ingest_remark."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0030_ingest_remark_orders"
down_revision = "0029_ingest_remarks_orders"
branch_labels = None
depends_on = None


def _orders_columns() -> set[str]:
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table("orders"):
        return set()
    return {column["name"] for column in inspector.get_columns("orders")}


def upgrade() -> None:
    columns = _orders_columns()
    if not columns:
        return

    with op.batch_alter_table("orders") as batch_op:
        if "ingest_remarks" in columns and "ingest_remark" not in columns:
            batch_op.alter_column("ingest_remarks", new_column_name="ingest_remark")
        elif "ingest_remark" not in columns:
            batch_op.add_column(sa.Column("ingest_remark", sa.Text(), nullable=True))


def downgrade() -> None:
    columns = _orders_columns()
    if not columns:
        return

    with op.batch_alter_table("orders") as batch_op:
        if "ingest_remark" in columns and "ingest_remarks" not in columns:
            batch_op.alter_column("ingest_remark", new_column_name="ingest_remarks")
        elif "ingest_remarks" not in columns:
            batch_op.add_column(sa.Column("ingest_remarks", sa.Text(), nullable=True))
