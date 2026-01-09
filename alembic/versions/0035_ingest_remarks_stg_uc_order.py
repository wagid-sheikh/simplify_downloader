"""Add ingest_remarks to stg_uc_orders (and uc_orders if present)."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0035_ingest_remarks_stg_uc_order"
down_revision = "0034_add_leads_assignment"
branch_labels = None
depends_on = None


def _table_columns(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table(table_name):
        return set()
    return {column["name"] for column in inspector.get_columns(table_name)}


def _add_ingest_remarks(table_name: str) -> None:
    columns = _table_columns(table_name)
    if not columns or "ingest_remarks" in columns:
        return
    with op.batch_alter_table(table_name) as batch_op:
        batch_op.add_column(sa.Column("ingest_remarks", sa.Text(), nullable=True))


def _drop_ingest_remarks(table_name: str) -> None:
    columns = _table_columns(table_name)
    if not columns or "ingest_remarks" not in columns:
        return
    with op.batch_alter_table(table_name) as batch_op:
        batch_op.drop_column("ingest_remarks")


def upgrade() -> None:
    _add_ingest_remarks("stg_uc_orders")
    _add_ingest_remarks("uc_orders")


def downgrade() -> None:
    _drop_ingest_remarks("uc_orders")
    _drop_ingest_remarks("stg_uc_orders")
