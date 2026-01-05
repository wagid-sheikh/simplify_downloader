"""Ensure TD sales tables use ingest_remarks."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0031_ingest_remarks_td_sales"
down_revision = "0030_ingest_remark_orders"
branch_labels = None
depends_on = None


def _table_columns(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table(table_name):
        return set()
    return {column["name"] for column in inspector.get_columns(table_name)}


def _ensure_ingest_remarks(table_name: str) -> None:
    columns = _table_columns(table_name)
    if not columns:
        return

    with op.batch_alter_table(table_name) as batch_op:
        if "ingest_remark" in columns and "ingest_remarks" in columns:
            batch_op.execute(
                sa.text(
                    f"UPDATE {table_name} "
                    "SET ingest_remarks = ingest_remark "
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
    _ensure_ingest_remarks("stg_td_sales")
    _ensure_ingest_remarks("td_sales")


def downgrade() -> None:
    _ensure_ingest_remarks("stg_td_sales")
    _ensure_ingest_remarks("td_sales")
