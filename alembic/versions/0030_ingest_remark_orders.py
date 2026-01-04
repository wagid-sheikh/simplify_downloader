"""Ensure orders.ingest_remarks is used instead of ingest_remark."""

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


def _ensure_ingest_remarks_column() -> None:
    columns = _orders_columns()
    if not columns:
        return

    with op.batch_alter_table("orders") as batch_op:
        if "ingest_remark" in columns and "ingest_remarks" in columns:
            batch_op.execute(
                sa.text(
                    "UPDATE orders SET ingest_remarks = ingest_remark "
                    "WHERE ingest_remarks IS NULL AND ingest_remark IS NOT NULL"
                )
            )
            batch_op.drop_column("ingest_remark")
            return

        if "ingest_remark" in columns:
            batch_op.alter_column("ingest_remark", new_column_name="ingest_remarks")
            return

        if "ingest_remarks" not in columns:
            batch_op.add_column(sa.Column("ingest_remarks", sa.Text(), nullable=True))


def upgrade() -> None:
    _ensure_ingest_remarks_column()


def downgrade() -> None:
    _ensure_ingest_remarks_column()
