"""Create CRM staging/production tables for TD/UC/bank pipelines.

This migration creates the staging and production tables required for the
td_orders_sync, uc_orders_sync, and bank_sync pipelines when they are missing,
and enforces the business-key unique constraints expected by revision 0024.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0025_create_crm_tables"
down_revision = "0024_align_td_uc_bank_keys_seeds"
branch_labels = None
depends_on = None


CREATED_TABLE_COMMENT = "Created by 0025_create_crm_tables"

STAGING_UNIQUE_SPECS: list[tuple[str, list[str], str]] = [
    ("stg_td_orders", ["store_code", "order_number", "order_date"], "uq_stg_td_orders_store_order_date"),
    ("stg_td_sales", ["store_code", "order_number", "payment_date"], "uq_stg_td_sales_store_order_payment_date"),
    ("stg_uc_orders", ["store_code", "order_number", "invoice_date"], "uq_stg_uc_orders_store_order_invoice_date"),
    ("stg_bank", ["row_id"], "uq_stg_bank_row_id"),
]

PRODUCTION_UNIQUE_SPECS: list[tuple[str, list[str], str]] = [
    ("orders", ["cost_center", "order_number", "order_date"], "uq_orders_cost_center_order_number_order_date"),
    ("td_sales", ["cost_center", "order_number", "payment_date"], "uq_td_sales_cost_center_order_number_payment_date"),
    ("uc_orders", ["cost_center", "order_number", "invoice_date"], "uq_uc_orders_cost_center_order_invoice_date"),
    ("bank", ["row_id"], "uq_bank_row_id"),
]


def _has_table(table_name: str) -> bool:
    return sa.inspect(op.get_bind()).has_table(table_name)


def _ensure_unique_constraint(inspector: sa.inspection.Inspector, *, table: str, columns: list[str], name: str) -> None:
    for constraint in inspector.get_unique_constraints(table):
        column_names = constraint.get("column_names") or []
        if set(column_names) == set(columns):
            return

    for index in inspector.get_indexes(table):
        column_names = index.get("column_names") or []
        if index.get("unique") and set(column_names) == set(columns):
            return

    op.create_unique_constraint(name, table, columns)


def _created_by_this_migration(inspector: sa.inspection.Inspector, table: str) -> bool:
    if not inspector.has_table(table):
        return False
    comment = inspector.get_table_comment(table)
    return (comment or {}).get("text") == CREATED_TABLE_COMMENT


def _create_stg_td_orders() -> None:
    op.create_table(
        "stg_td_orders",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.Text()),
        sa.Column("run_date", sa.DateTime(timezone=True)),
        sa.Column("cost_center", sa.String(length=8)),
        sa.Column("store_code", sa.String(length=8)),
        sa.Column("order_date", sa.DateTime(timezone=True)),
        sa.Column("order_number", sa.String(length=12)),
        sa.Column("customer_code", sa.String(length=12)),
        sa.Column("customer_name", sa.String(length=128)),
        sa.Column("customer_address", sa.String(length=256)),
        sa.Column("mobile_number", sa.String(length=16)),
        sa.Column("preference", sa.String(length=128)),
        sa.Column("due_date", sa.DateTime(timezone=True)),
        sa.Column("last_activity", sa.DateTime(timezone=True)),
        sa.Column("pieces", sa.Numeric(12, 0)),
        sa.Column("weight", sa.Numeric(12, 2)),
        sa.Column("gross_amount", sa.Numeric(12, 2)),
        sa.Column("discount", sa.Numeric(12, 2)),
        sa.Column("tax_amount", sa.Numeric(12, 2)),
        sa.Column("net_amount", sa.Numeric(12, 2)),
        sa.Column("advance", sa.Numeric(12, 2)),
        sa.Column("paid", sa.Numeric(12, 2)),
        sa.Column("adjustment", sa.Numeric(12, 2)),
        sa.Column("balance", sa.Numeric(12, 2)),
        sa.Column("advance_received", sa.Numeric(12, 2)),
        sa.Column("advance_used", sa.Numeric(12, 2)),
        sa.Column("booked_by", sa.String(length=32)),
        sa.Column("workshop_note", sa.Text()),
        sa.Column("order_note", sa.Text()),
        sa.Column("home_delivery", sa.String(length=32)),
        sa.Column("area_location", sa.Text()),
        sa.Column("garments_inspected_by", sa.String(length=32)),
        sa.Column("customer_gstin", sa.String(length=32)),
        sa.Column("registration_source", sa.String(length=24)),
        sa.Column("order_from_pos", sa.String(length=32)),
        sa.Column("package", sa.String(length=32)),
        sa.Column("package_type", sa.String(length=32)),
        sa.Column("package_name", sa.String(length=32)),
        sa.Column("feedback", sa.String(length=32)),
        sa.Column("tags", sa.String(length=32)),
        sa.Column("comment", sa.Text()),
        sa.Column("primary_service", sa.String(length=24)),
        sa.Column("topup_service", sa.String(length=32)),
        sa.Column("order_status", sa.String(length=32)),
        sa.Column("last_payment_activity", sa.DateTime(timezone=True)),
        sa.Column("package_payment_info", sa.String(length=32)),
        sa.Column("coupon_code", sa.String(length=32)),
        sa.PrimaryKeyConstraint("id", name="pk_stg_td_order"),
        sa.UniqueConstraint(
            "store_code",
            "order_number",
            "order_date",
            name="uq_stg_td_orders_store_order_date",
        ),
        comment=CREATED_TABLE_COMMENT,
    )


def _create_stg_td_sales() -> None:
    op.create_table(
        "stg_td_sales",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.Text()),
        sa.Column("run_date", sa.DateTime(timezone=True)),
        sa.Column("cost_center", sa.String(length=8)),
        sa.Column("store_code", sa.String(length=16)),
        sa.Column("order_date", sa.DateTime(timezone=True)),
        sa.Column("payment_date", sa.DateTime(timezone=True)),
        sa.Column("order_number", sa.String(length=16)),
        sa.Column("customer_code", sa.String(length=16)),
        sa.Column("customer_name", sa.String(length=128)),
        sa.Column("customer_address", sa.String(length=256)),
        sa.Column("mobile_number", sa.String(length=16)),
        sa.Column("payment_received", sa.Numeric(12, 2)),
        sa.Column("adjustments", sa.Numeric(12, 2)),
        sa.Column("balance", sa.Numeric(12, 2)),
        sa.Column("accepted_by", sa.String(length=64)),
        sa.Column("payment_mode", sa.String(length=32)),
        sa.Column("transaction_id", sa.String(length=64)),
        sa.Column("payment_made_at", sa.String(length=128)),
        sa.Column("order_type", sa.String(length=32)),
        sa.Column("is_duplicate", sa.Boolean()),
        sa.Column("is_edited_order", sa.Boolean()),
        sa.PrimaryKeyConstraint("id", name="pk_stg_td_sales"),
        sa.UniqueConstraint(
            "store_code",
            "order_number",
            "payment_date",
            name="uq_stg_td_sales_store_order_payment_date",
        ),
        comment=CREATED_TABLE_COMMENT,
    )


def _create_stg_uc_orders() -> None:
    op.create_table(
        "stg_uc_orders",
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
        sa.PrimaryKeyConstraint("id", name="pk_stg_uc_orders"),
        sa.UniqueConstraint(
            "store_code",
            "order_number",
            "invoice_date",
            name="uq_stg_uc_orders_store_order_invoice_date",
        ),
        comment=CREATED_TABLE_COMMENT,
    )


def _create_stg_bank() -> None:
    op.create_table(
        "stg_bank",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.Text()),
        sa.Column("run_date", sa.DateTime(timezone=True)),
        sa.Column("bank_name", sa.String(length=8)),
        sa.Column("row_id", sa.String(length=11)),
        sa.Column("txn_date", sa.DateTime(timezone=True)),
        sa.Column("value_date", sa.DateTime(timezone=True)),
        sa.Column("description", sa.String(length=256)),
        sa.Column("ref_number", sa.String(length=256)),
        sa.Column("branch_code", sa.String(length=12)),
        sa.Column("debit", sa.Numeric(12, 2)),
        sa.Column("credit", sa.Numeric(12, 2)),
        sa.Column("balance", sa.Numeric(12, 2)),
        sa.Column("cost_center", sa.String(length=8)),
        sa.Column("order_number", sa.String(length=64)),
        sa.Column("category", sa.String(length=16)),
        sa.Column("sub_category", sa.String(length=32)),
        sa.Column("comments", sa.String(length=256)),
        sa.PrimaryKeyConstraint("id", name="pk_stg_bank"),
        sa.UniqueConstraint("row_id", name="uq_stg_bank_row_id"),
        comment=CREATED_TABLE_COMMENT,
    )


def _create_orders() -> None:
    op.create_table(
        "orders",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.Text()),
        sa.Column("run_date", sa.DateTime(timezone=True)),
        sa.Column("cost_center", sa.String(length=8), nullable=False),
        sa.Column("store_code", sa.String(length=8), nullable=False),
        sa.Column("source_system", sa.String(length=12), nullable=False),
        sa.Column("order_number", sa.String(length=12), nullable=False),
        sa.Column("invoice_number", sa.String(length=12)),
        sa.Column("order_date", sa.DateTime(timezone=True), nullable=False),
        sa.Column("customer_code", sa.String(length=12)),
        sa.Column("customer_name", sa.String(length=128), nullable=False),
        sa.Column("mobile_number", sa.String(length=16), nullable=False),
        sa.Column("customer_gstin", sa.String(length=32)),
        sa.Column("customer_source", sa.String(length=24)),
        sa.Column("package_flag", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("service_type", sa.String(length=24)),
        sa.Column("customer_address", sa.Text()),
        sa.Column("pieces", sa.Numeric(12, 0)),
        sa.Column("weight", sa.Numeric(12, 2)),
        sa.Column("due_date", sa.DateTime(timezone=True), nullable=False),
        sa.Column("default_due_date", sa.DateTime(timezone=True), nullable=False),
        sa.Column("due_days_delta", sa.Numeric(10, 0)),
        sa.Column("due_date_flag", sa.String(length=24)),
        sa.Column("complete_processing_by", sa.DateTime(timezone=True), nullable=False),
        sa.Column("gross_amount", sa.Numeric(12, 2)),
        sa.Column("discount_amount", sa.Numeric(12, 2)),
        sa.Column("tax_amount", sa.Numeric(12, 2)),
        sa.Column("net_amount", sa.Numeric(12, 2)),
        sa.Column("payment_status", sa.String(length=24)),
        sa.Column("order_status", sa.String(length=24)),
        sa.Column("payment_mode", sa.String(length=24)),
        sa.Column("payment_date", sa.DateTime(timezone=True)),
        sa.Column("payment_amount", sa.Numeric(12, 2)),
        sa.Column("order_edited_flag", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("system_order_status", sa.String(length=24), server_default=sa.text("'Active'")),
        sa.Column("google_maps_url", sa.String(length=256)),
        sa.Column("latitude", sa.Float()),
        sa.Column("longitude", sa.Float()),
        sa.Column("created_by", sa.BigInteger()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_by", sa.BigInteger()),
        sa.Column("updated_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("latitude BETWEEN -90 AND 90", name="ck_orders_latitude_range"),
        sa.CheckConstraint("longitude BETWEEN -180 AND 180", name="ck_orders_longitude_range"),
        sa.UniqueConstraint(
            "cost_center",
            "order_number",
            "order_date",
            name="uq_orders_cost_center_order_number_order_date",
        ),
        comment=CREATED_TABLE_COMMENT,
    )


def _create_td_sales() -> None:
    op.create_table(
        "td_sales",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.Text()),
        sa.Column("run_date", sa.DateTime(timezone=True)),
        sa.Column("cost_center", sa.String(length=8)),
        sa.Column("store_code", sa.String(length=16)),
        sa.Column("order_date", sa.DateTime(timezone=True)),
        sa.Column("payment_date", sa.DateTime(timezone=True)),
        sa.Column("order_number", sa.String(length=16)),
        sa.Column("customer_code", sa.String(length=16)),
        sa.Column("customer_name", sa.String(length=128)),
        sa.Column("customer_address", sa.String(length=256)),
        sa.Column("mobile_number", sa.String(length=16)),
        sa.Column("payment_received", sa.Numeric(12, 2)),
        sa.Column("adjustments", sa.Numeric(12, 2)),
        sa.Column("balance", sa.Numeric(12, 2)),
        sa.Column("accepted_by", sa.String(length=64)),
        sa.Column("payment_mode", sa.String(length=32)),
        sa.Column("transaction_id", sa.String(length=64)),
        sa.Column("payment_made_at", sa.String(length=128)),
        sa.Column("order_type", sa.String(length=32)),
        sa.Column("is_duplicate", sa.Boolean()),
        sa.Column("is_edited_order", sa.Boolean()),
        sa.PrimaryKeyConstraint("id", name="pk_td_sales"),
        sa.UniqueConstraint(
            "cost_center",
            "order_number",
            "payment_date",
            name="uq_td_sales_cost_center_order_number_payment_date",
        ),
        comment=CREATED_TABLE_COMMENT,
    )


def _create_uc_orders() -> None:
    op.create_table(
        "uc_orders",
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
            name="uq_uc_orders_cost_center_order_invoice_date",
        ),
        comment=CREATED_TABLE_COMMENT,
    )


def _create_bank() -> None:
    op.create_table(
        "bank",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.Text()),
        sa.Column("run_date", sa.DateTime(timezone=True)),
        sa.Column("bank_name", sa.String(length=8)),
        sa.Column("row_id", sa.String(length=11)),
        sa.Column("txn_date", sa.DateTime(timezone=True)),
        sa.Column("value_date", sa.DateTime(timezone=True)),
        sa.Column("description", sa.String(length=256)),
        sa.Column("ref_number", sa.String(length=256)),
        sa.Column("branch_code", sa.String(length=12)),
        sa.Column("debit", sa.Numeric(12, 2)),
        sa.Column("credit", sa.Numeric(12, 2)),
        sa.Column("balance", sa.Numeric(12, 2)),
        sa.Column("cost_center", sa.String(length=8)),
        sa.Column("order_number", sa.String(length=64)),
        sa.Column("category", sa.String(length=16)),
        sa.Column("sub_category", sa.String(length=32)),
        sa.Column("comments", sa.String(length=256)),
        sa.PrimaryKeyConstraint("id", name="pk_bank"),
        sa.UniqueConstraint("row_id", name="uq_bank_row_id"),
        comment=CREATED_TABLE_COMMENT,
    )


def upgrade() -> None:
    creators = [
        ("stg_td_orders", _create_stg_td_orders),
        ("stg_td_sales", _create_stg_td_sales),
        ("stg_uc_orders", _create_stg_uc_orders),
        ("stg_bank", _create_stg_bank),
        ("orders", _create_orders),
        ("td_sales", _create_td_sales),
        ("uc_orders", _create_uc_orders),
        ("bank", _create_bank),
    ]

    for table_name, creator in creators:
        if not _has_table(table_name):
            creator()

    connection = op.get_bind()
    for table, columns, name in [*STAGING_UNIQUE_SPECS, *PRODUCTION_UNIQUE_SPECS]:
        inspector = sa.inspect(connection)
        if inspector.has_table(table):
            _ensure_unique_constraint(inspector, table=table, columns=columns, name=name)


def _drop_unique_if_exists(table: str, name: str) -> None:
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table(table):
        return

    for constraint in inspector.get_unique_constraints(table):
        if constraint.get("name") == name:
            op.drop_constraint(name, table, type_="unique")
            return

    for index in inspector.get_indexes(table):
        if index.get("unique") and index.get("name") == name:
            op.drop_index(name, table_name=table)
            return


def _drop_table_if_created(table: str) -> None:
    inspector = sa.inspect(op.get_bind())
    if not _created_by_this_migration(inspector, table):
        return
    op.drop_table(table)


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    for table, columns, name in [*STAGING_UNIQUE_SPECS, *PRODUCTION_UNIQUE_SPECS]:
        if _created_by_this_migration(inspector, table):
            _drop_unique_if_exists(table, name)

    for table in (
        "bank",
        "uc_orders",
        "td_sales",
        "orders",
        "stg_bank",
        "stg_uc_orders",
        "stg_td_sales",
        "stg_td_orders",
    ):
        _drop_table_if_created(table)
