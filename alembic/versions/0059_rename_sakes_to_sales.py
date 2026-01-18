"""Rename sakes table to sales and remove td_sales view."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0059_rename_sakes_to_sales"
down_revision = "0058_seed_daily_salesreportnotif"
branch_labels = None
depends_on = None


def _is_postgres() -> bool:
    return op.get_bind().dialect.name == "postgresql"


def _rename_constraint(table_name: str, *, old: str, new: str) -> None:
    if not _is_postgres():
        return
    connection = op.get_bind()
    exists = connection.execute(
        sa.text("SELECT 1 FROM pg_constraint WHERE conname = :name"),
        {"name": old},
    ).scalar()
    if not exists:
        return

    preparer = connection.dialect.identifier_preparer
    quoted_table = preparer.quote(table_name)
    quoted_old = preparer.quote(old)
    quoted_new = preparer.quote(new)
    op.execute(
        f"ALTER TABLE {quoted_table} RENAME CONSTRAINT {quoted_old} TO {quoted_new}"
    )


def upgrade() -> None:
    op.execute(sa.text("DROP VIEW IF EXISTS td_sales"))

    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table("sakes"):
        return

    op.rename_table("sakes", "sales")
    _rename_constraint("sales", old="pk_sakes", new="pk_sales")
    _rename_constraint(
        "sales",
        old="uq_sakes_cost_center_order_number_payment_date",
        new="uq_sales_cost_center_order_number_payment_date",
    )


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table("sales"):
        return

    _rename_constraint("sales", old="pk_sales", new="pk_sakes")
    _rename_constraint(
        "sales",
        old="uq_sales_cost_center_order_number_payment_date",
        new="uq_sakes_cost_center_order_number_payment_date",
    )
    op.rename_table("sales", "sakes")
    op.execute(sa.text("CREATE OR REPLACE VIEW td_sales AS SELECT * FROM sakes"))
