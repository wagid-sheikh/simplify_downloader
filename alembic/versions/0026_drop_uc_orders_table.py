"""Drop uc_orders table created by 0025

Drops uc_orders and its unique constraint if they were created by the
0025_create_crm_tables migration. Leaves existing tables/constraints intact
when they predate that revision.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0026_drop_uc_orders_table"
down_revision = "0025_create_crm_tables"
branch_labels = None
depends_on = None


UC_ORDERS_TABLE = "uc_orders"
UC_ORDERS_UNIQUE = "uq_uc_orders_cost_center_order_invoice_date"
CREATED_BY_0025_COMMENT = "Created by 0025_create_crm_tables"


def _created_by_0025(inspector: sa.inspection.Inspector) -> bool:
    if not inspector.has_table(UC_ORDERS_TABLE):
        return False

    comment = inspector.get_table_comment(UC_ORDERS_TABLE) or {}
    return comment.get("text") == CREATED_BY_0025_COMMENT


def _drop_uc_orders_unique(inspector: sa.inspection.Inspector) -> None:
    for constraint in inspector.get_unique_constraints(UC_ORDERS_TABLE):
        if constraint.get("name") == UC_ORDERS_UNIQUE:
            op.drop_constraint(UC_ORDERS_UNIQUE, UC_ORDERS_TABLE, type_="unique")
            return

    for index in inspector.get_indexes(UC_ORDERS_TABLE):
        if index.get("unique") and index.get("name") == UC_ORDERS_UNIQUE:
            op.drop_index(UC_ORDERS_UNIQUE, table_name=UC_ORDERS_TABLE)
            return


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())

    if not _created_by_0025(inspector):
        return

    _drop_uc_orders_unique(inspector)
    op.drop_table(UC_ORDERS_TABLE)


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())

    if inspector.has_table(UC_ORDERS_TABLE):
        return

    op.create_table(
        UC_ORDERS_TABLE,
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.Text()),
        sa.Column("run_date", sa.DateTime(timezone=True)),
        sa.Column("cost_center", sa.String(length=8)),
        sa.Column("store_code", sa.String(length=8)),
        sa.Column("s_no", sa.Numeric(10, 0)),
        sa.Column("order_number", sa.String(length=12)),
        sa.Column("invoice_number", sa.String(length=12)),
        sa.Column("invoice_date", sa.DateTime(timezone=True)),
        sa.Column("customer_name", sa.String(length=128)),
        sa.Column("mobile_number", sa.String(length=16), nullable=False),
        sa.Column("payment_status", sa.String(length=24)),
        sa.Column("customer_gstin", sa.String(length=32)),
        sa.Column("place_of_supply", sa.String(length=32)),
        sa.Column("net_amount", sa.Numeric(12, 2)),
        sa.Column("cgst", sa.Numeric(12, 2)),
        sa.Column("sgst", sa.Numeric(12, 2)),
        sa.Column("gross_amount", sa.Numeric(12, 2)),
        sa.PrimaryKeyConstraint("id", name="pk_uc_orders"),
        sa.UniqueConstraint(
            "cost_center",
            "order_number",
            "invoice_date",
            name=UC_ORDERS_UNIQUE,
        ),
        comment=CREATED_BY_0025_COMMENT,
    )
