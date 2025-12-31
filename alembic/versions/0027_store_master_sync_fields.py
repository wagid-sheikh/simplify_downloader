"""Add sync columns and seed configs for CRM pipelines.

Adds the sync columns to store_master if they are missing and seeds the
TD/UC store rows with sync metadata required for the downloader.
"""

from __future__ import annotations

from datetime import date

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0027_store_master_sync_fields"
down_revision = "0026_drop_uc_orders_table"
branch_labels = None
depends_on = None


TABLE_NAME = "store_master"
CREATED_BY_COMMENT = "Created by 0027_store_master_sync_fields"

STORE_COLUMNS: dict[str, sa.Column] = {
    "sync_config": sa.Column(
        "sync_config", postgresql.JSONB(astext_type=sa.Text()), nullable=True, comment=CREATED_BY_COMMENT
    ),
    "sync_group": sa.Column("sync_group", sa.CHAR(length=2), nullable=True, comment=CREATED_BY_COMMENT),
    "sync_orders_flag": sa.Column(
        "sync_orders_flag", sa.Boolean(), nullable=False, server_default=sa.text("false"), comment=CREATED_BY_COMMENT
    ),
    "sync_bank_flag": sa.Column(
        "sync_bank_flag", sa.Boolean(), nullable=False, server_default=sa.text("false"), comment=CREATED_BY_COMMENT
    ),
    "start_date": sa.Column("start_date", sa.Date(), nullable=True, comment=CREATED_BY_COMMENT),
    "cost_center": sa.Column("cost_center", sa.String(length=8), nullable=True, comment=CREATED_BY_COMMENT),
}

STORE_SEEDS: dict[str, dict[str, object]] = {
    "A668": {
        "cost_center": "UN3668",
        "sync_group": "TD",
        "start_date": date(2025, 3, 1),
        "sync_orders_flag": True,
        "sync_bank_flag": True,
        "sync_config": {
            "urls": {
                "login": "https://subs.quickdrycleaning.com/Login",
                "home": "https://subs.quickdrycleaning.com/a668/App/home",
                "orders_link": "https://simplifytumbledry.in/tms/orders",
                "sales_link": "https://subs.quickdrycleaning.com/a668/App/Reports/NEWSalesAndDeliveryReport",
            },
            "login_selector": {
                "username": "txtUserId",
                "password": "txtPassword",
                "store_code": "txtBranchPin",
                "submit": "btnLogin",
            },
            "username": "tduttamnagar",
            "password": "123456",
        },
    },
    "A817": {
        "cost_center": "KN3817",
        "sync_group": "TD",
        "start_date": date(2025, 5, 10),
        "sync_orders_flag": True,
        "sync_bank_flag": True,
        "sync_config": {
            "urls": {
                "login": "https://subs.quickdrycleaning.com/Login",
                "home": "https://subs.quickdrycleaning.com/a668/App/home",
                "orders_link": "https://simplifytumbledry.in/tms/orders",
                "sales_link": "https://subs.quickdrycleaning.com/a668/App/Reports/NEWSalesAndDeliveryReport",
            },
            "login_selector": {
                "username": "txtUserId",
                "password": "txtPassword",
                "store_code": "txtBranchPin",
                "submit": "btnLogin",
            },
            "username": "tdkirtinagar",
            "password": "123456",
        },
    },
    "UC567": {
        "cost_center": "SC3567",
        "sync_group": "UC",
        "start_date": date(2025, 2, 8),
        "sync_orders_flag": True,
        "sync_bank_flag": True,
        "sync_config": {
            "urls": {
                "login": "https://store.ucleanlaundry.com/login",
                "home": "https://store.ucleanlaundry.com/dashboard",
                "orders_link": "https://store.ucleanlaundry.com/gst-report",
            },
            "login_selector": {
                "username": "input[placeholder='Email'][type='email']",
                "password": "input[placeholder='Password'][type='password']",
                "submit": "button.btn-primary[type='submit']",
            },
            "username": "UC567@uclean.in",
            "password": "guerwnvej@uc#67",
        },
    },
    "UC610": {
        "cost_center": "SL1610",
        "sync_group": "UC",
        "start_date": date(2025, 5, 11),
        "sync_orders_flag": True,
        "sync_bank_flag": True,
        "sync_config": {
            "urls": {
                "login": "https://store.ucleanlaundry.com/login",
                "home": "https://store.ucleanlaundry.com/dashboard",
                "orders_link": "https://store.ucleanlaundry.com/gst-report",
            },
            "login_selector": {
                "username": "input[placeholder='Email'][type='email']",
                "password": "input[placeholder='Password'][type='password']",
                "submit": "button.btn-primary[type='submit']",
            },
            "username": "UC610@uclean.in",
            "password": "vabfhwbf@uc#10",
        },
    },
}


def _add_missing_columns(inspector: sa.inspection.Inspector) -> None:
    if not inspector.has_table(TABLE_NAME):
        return

    existing_columns = {column["name"] for column in inspector.get_columns(TABLE_NAME)}
    for name, column in STORE_COLUMNS.items():
        if name not in existing_columns:
            op.add_column(TABLE_NAME, column)


def _seed_store_rows(bind: sa.Connection) -> None:
    inspector = sa.inspect(bind)
    if not inspector.has_table(TABLE_NAME):
        return

    meta = sa.MetaData()
    store_master = sa.Table(TABLE_NAME, meta, autoload_with=bind)

    for code, values in STORE_SEEDS.items():
        bind.execute(
            store_master.update()
            .where(sa.func.upper(store_master.c.store_code) == code)
            .values(values)
        )


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    _add_missing_columns(inspector)
    _seed_store_rows(bind)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not inspector.has_table(TABLE_NAME):
        return

    columns = {column["name"]: column for column in inspector.get_columns(TABLE_NAME)}

    for name in STORE_COLUMNS:
        column = columns.get(name)
        if column and column.get("comment") == CREATED_BY_COMMENT:
            op.drop_column(TABLE_NAME, name)
