"""Create cost center tables and add store_master FK."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0057_create_cost_center_tables"
down_revision = "0056_rename_td_sales_to_sakes"
branch_labels = None
depends_on = None


COST_CENTER_SEEDS = [
    {"cost_center": "UN3668", "description": "Uttam Nagar", "target_type": "value"},
    {"cost_center": "SC3567", "description": "Sector 56", "target_type": "value"},
    {"cost_center": "KN3817", "description": "Kirti Nagar", "target_type": "value"},
    {"cost_center": "SL1610", "description": "Sushant Lok 1", "target_type": "value"},
    {"cost_center": "TSV001", "description": "TSV Head Office", "target_type": "none"},
    {"cost_center": "TSV002", "description": "TSV Delhi", "target_type": "orders"},
    {"cost_center": "TSV003", "description": "TSV Gurgaon", "target_type": "orders"},
]

COST_CENTER_TARGET_SEEDS = [
    {"month": 1, "year": 2026, "cost_center": "UN3668", "sale_target": 270000, "collection_target": 243000},
    {"month": 1, "year": 2026, "cost_center": "SC3567", "sale_target": 150000, "collection_target": 135000},
    {"month": 1, "year": 2026, "cost_center": "KN3817", "sale_target": 270000, "collection_target": 243000},
    {"month": 1, "year": 2026, "cost_center": "SL1610", "sale_target": 150000, "collection_target": 135000},
    {"month": 1, "year": 2026, "cost_center": "TSV002", "sale_target": 180, "collection_target": 171},
]


def _cost_center_table() -> sa.Table:
    return sa.table(
        "cost_center",
        sa.column("cost_center", sa.String(length=8)),
        sa.column("description", sa.String(length=32)),
        sa.column("target_type", sa.String(length=16)),
        sa.column("is_active", sa.Boolean()),
    )


def _cost_center_targets_table() -> sa.Table:
    return sa.table(
        "cost_center_targets",
        sa.column("month", sa.Integer()),
        sa.column("year", sa.Integer()),
        sa.column("cost_center", sa.String(length=8)),
        sa.column("sale_target", sa.Numeric(12, 2)),
        sa.column("collection_target", sa.Numeric(12, 2)),
    )


def _add_cost_center_table(inspector: sa.inspection.Inspector) -> None:
    if inspector.has_table("cost_center"):
        return
    op.create_table(
        "cost_center",
        sa.Column("cost_center", sa.String(length=8), primary_key=True),
        sa.Column("description", sa.String(length=32), nullable=False),
        sa.Column("target_type", sa.String(length=16), nullable=False, server_default=sa.text("'value'")),
        sa.Column("is_active", sa.Boolean(), nullable=True, server_default=sa.text("true")),
        sa.CheckConstraint("target_type in ('value', 'orders', 'none')", name="ck_cost_center_target_type"),
    )


def _add_cost_center_targets_table(inspector: sa.inspection.Inspector) -> None:
    if inspector.has_table("cost_center_targets"):
        return
    op.create_table(
        "cost_center_targets",
        sa.Column("month", sa.Integer(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("cost_center", sa.String(length=8), nullable=False),
        sa.Column("sale_target", sa.Numeric(12, 2)),
        sa.Column("collection_target", sa.Numeric(12, 2)),
        sa.Column("sales_mtd", sa.Numeric(12, 2)),
        sa.Column("collection_mtd", sa.Numeric(12, 2)),
        sa.Column("sales_target_met", sa.Boolean()),
        sa.Column("collection_target_met", sa.Boolean()),
        sa.ForeignKeyConstraint(["cost_center"], ["cost_center.cost_center"]),
        sa.UniqueConstraint("month", "year", "cost_center", name="uq_cost_center_targets_month_year_center"),
    )


def _add_store_master_fk(inspector: sa.inspection.Inspector) -> None:
    if not inspector.has_table("store_master"):
        return
    existing_fks = inspector.get_foreign_keys("store_master")
    for fk in existing_fks:
        if fk.get("referred_table") == "cost_center":
            return
    op.create_foreign_key(
        "fk_store_master_cost_center",
        "store_master",
        "cost_center",
        ["cost_center"],
        ["cost_center"],
        ondelete=None,
    )


def _seed_cost_centers(connection: sa.Connection) -> None:
    table = _cost_center_table()
    stmt = (
        postgresql.insert(table)
        .values(COST_CENTER_SEEDS)
        .on_conflict_do_nothing(index_elements=[table.c.cost_center])
    )
    connection.execute(stmt)


def _seed_cost_center_targets(connection: sa.Connection) -> None:
    table = _cost_center_targets_table()
    stmt = (
        postgresql.insert(table)
        .values(COST_CENTER_TARGET_SEEDS)
        .on_conflict_do_nothing(index_elements=[table.c.month, table.c.year, table.c.cost_center])
    )
    connection.execute(stmt)


def upgrade() -> None:
    connection = op.get_bind()
    inspector = sa.inspect(connection)

    _add_cost_center_table(inspector)
    _seed_cost_centers(connection)

    inspector = sa.inspect(connection)
    _add_cost_center_targets_table(inspector)
    _seed_cost_center_targets(connection)

    inspector = sa.inspect(connection)
    _add_store_master_fk(inspector)


def downgrade() -> None:
    connection = op.get_bind()
    inspector = sa.inspect(connection)

    if inspector.has_table("store_master"):
        for fk in inspector.get_foreign_keys("store_master"):
            if fk.get("name") == "fk_store_master_cost_center":
                op.drop_constraint("fk_store_master_cost_center", "store_master", type_="foreignkey")
                break

    inspector = sa.inspect(connection)
    if inspector.has_table("cost_center_targets"):
        op.drop_table("cost_center_targets")

    inspector = sa.inspect(connection)
    if inspector.has_table("cost_center"):
        op.drop_table("cost_center")
