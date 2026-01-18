"""Rename td_sales table to sakes with compatibility view."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0056_rename_td_sales_to_sakes"
down_revision = "0055_simplify_profiler_email_tem"
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
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table("td_sales"):
        return

    op.rename_table("td_sales", "sakes")
    _rename_constraint("sakes", old="pk_td_sales", new="pk_sakes")
    _rename_constraint(
        "sakes",
        old="uq_td_sales_cost_center_order_number_payment_date",
        new="uq_sakes_cost_center_order_number_payment_date",
    )
    op.execute(sa.text("CREATE OR REPLACE VIEW td_sales AS SELECT * FROM sakes"))


def downgrade() -> None:
    op.execute(sa.text("DROP VIEW IF EXISTS td_sales"))

    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table("sakes"):
        return

    _rename_constraint("sakes", old="pk_sakes", new="pk_td_sales")
    _rename_constraint(
        "sakes",
        old="uq_sakes_cost_center_order_number_payment_date",
        new="uq_td_sales_cost_center_order_number_payment_date",
    )
    op.rename_table("sakes", "td_sales")
