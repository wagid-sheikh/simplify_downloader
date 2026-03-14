"""Add TD garment staging and order line item tables."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0071_add_td_garment_tables"
down_revision = "0070_widenordersservicetypeto256"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "stg_td_garments",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.Text()),
        sa.Column("run_date", sa.DateTime(timezone=True)),
        sa.Column("window_from_date", sa.Date(), nullable=False),
        sa.Column("window_to_date", sa.Date(), nullable=False),
        sa.Column("cost_center", sa.String(length=8), nullable=False),
        sa.Column("store_code", sa.String(length=8), nullable=False),
        sa.Column("api_order_id", sa.String(length=64)),
        sa.Column("api_line_item_id", sa.String(length=64)),
        sa.Column("api_garment_id", sa.String(length=64)),
        sa.Column("order_number", sa.String(length=32), nullable=False),
        sa.Column("line_item_key", sa.String(length=128), nullable=False),
        sa.Column("line_item_uid", sa.String(length=160), nullable=False),
        sa.Column("garment_name", sa.String(length=128)),
        sa.Column("service_name", sa.String(length=128)),
        sa.Column("quantity", sa.Numeric(12, 2)),
        sa.Column("amount", sa.Numeric(12, 2)),
        sa.Column("order_date", sa.DateTime(timezone=True)),
        sa.Column("updated_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String(length=32)),
        sa.Column("raw_payload", sa.JSON()),
        sa.Column("ingest_remarks", sa.Text()),
    )
    op.create_unique_constraint(
        "uq_stg_td_garments_store_line_item_uid",
        "stg_td_garments",
        ["store_code", "line_item_uid"],
    )

    op.create_table(
        "order_line_items",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.Text()),
        sa.Column("run_date", sa.DateTime(timezone=True)),
        sa.Column("cost_center", sa.String(length=8), nullable=False),
        sa.Column("store_code", sa.String(length=8), nullable=False),
        sa.Column("order_id", sa.BigInteger(), nullable=True),
        sa.Column("order_number", sa.String(length=32), nullable=False),
        sa.Column("api_order_id", sa.String(length=64)),
        sa.Column("api_line_item_id", sa.String(length=64)),
        sa.Column("api_garment_id", sa.String(length=64)),
        sa.Column("line_item_key", sa.String(length=128), nullable=False),
        sa.Column("line_item_uid", sa.String(length=160), nullable=False),
        sa.Column("garment_name", sa.String(length=128)),
        sa.Column("service_name", sa.String(length=128)),
        sa.Column("quantity", sa.Numeric(12, 2)),
        sa.Column("amount", sa.Numeric(12, 2)),
        sa.Column("order_date", sa.DateTime(timezone=True)),
        sa.Column("updated_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String(length=32)),
        sa.Column("is_orphan", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("ingest_remarks", sa.Text()),
        sa.ForeignKeyConstraint(["order_id"], ["orders.id"], name="fk_order_line_items_order_id"),
    )
    op.create_unique_constraint(
        "uq_order_line_items_cost_center_line_item_uid",
        "order_line_items",
        ["cost_center", "line_item_uid"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_order_line_items_cost_center_line_item_uid", "order_line_items", type_="unique")
    op.drop_table("order_line_items")
    op.drop_constraint("uq_stg_td_garments_store_line_item_uid", "stg_td_garments", type_="unique")
    op.drop_table("stg_td_garments")
