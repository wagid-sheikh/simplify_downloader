from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Callable

import pytest
import sqlalchemy as sa

from alembic.migration import MigrationContext
from alembic.operations import Operations


def _load_migration_module():
    project_root = Path(__file__).resolve().parents[2]
    module_path = project_root / "alembic" / "versions" / "0106_missing_pay_net_type.py"
    spec = importlib.util.spec_from_file_location("v0106_missing_pay_net_type", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load migration module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


migration = _load_migration_module()


def _run_migration(
    connection: sa.Connection, fn: Callable[[], None], monkeypatch: pytest.MonkeyPatch
) -> None:
    context = MigrationContext.configure(connection)
    operations = Operations(context)
    original_op = migration.op
    monkeypatch.setattr(migration, "op", operations)
    try:
        fn()
    finally:
        monkeypatch.setattr(migration, "op", original_op)


def test_postgres_view_sql_preserves_existing_net_amount_numeric_type() -> None:
    normalized_sql = " ".join(migration.POSTGRES_VIEW_SQL.split()).lower()

    assert "create or replace view public.vw_orders_missing_in_payment_collections" in normalized_sql
    assert "from public.vw_orders as o" in normalized_sql
    assert "order_amount::numeric(12, 2) as net_amount" in normalized_sql
    assert "order_amount as net_amount" not in normalized_sql
    assert "order_amount > 0" in normalized_sql
    assert "not (paid_amount + 1 >= order_amount)" in normalized_sql
    assert "o.net_amount" not in normalized_sql
    assert "o.gross_amount" not in normalized_sql
    assert "o.adjustment" not in normalized_sql


def test_sqlite_view_sql_expresses_matching_net_amount_type_intent() -> None:
    normalized_sql = " ".join(migration.SQLITE_VIEW_SQL.split()).lower()

    assert "cast(order_amount as numeric(12, 2)) as net_amount" in normalized_sql
    assert "order_amount as net_amount" not in normalized_sql


def test_sqlite_upgrade_replaces_missing_payments_view_with_casted_order_amount(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                CREATE TABLE vw_orders (
                    cost_center TEXT NOT NULL,
                    order_number TEXT NOT NULL,
                    order_date TEXT NOT NULL,
                    customer_name TEXT,
                    mobile_number TEXT,
                    net_amount NUMERIC(12, 2),
                    gross_amount NUMERIC(12, 2),
                    adjustment NUMERIC(12, 2),
                    order_amount NUMERIC(12, 2) NOT NULL
                )
                """
            )
        )
        connection.execute(
            sa.text(
                """
                CREATE TABLE sales (
                    cost_center TEXT NOT NULL,
                    order_number TEXT NOT NULL
                )
                """
            )
        )
        connection.execute(
            sa.text(
                """
                CREATE TABLE payment_collections (
                    cost_center TEXT NOT NULL,
                    order_number TEXT,
                    amount NUMERIC(12, 2) NOT NULL
                )
                """
            )
        )
        connection.execute(
            sa.text(
                """
                CREATE VIEW vw_orders_missing_in_payment_collections AS
                SELECT cost_center, order_number, order_date, customer_name, mobile_number, net_amount
                FROM vw_orders
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO vw_orders (
                    cost_center, order_number, order_date, customer_name, mobile_number,
                    net_amount, gross_amount, adjustment, order_amount
                ) VALUES
                    ('CC1', 'MISSING', '2026-05-01', 'Missing', '9001', 0, 0, 999, 100),
                    ('CC1', 'PAID', '2026-05-01', 'Paid', '9002', 0, 0, 999, 100)
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO sales (cost_center, order_number)
                VALUES ('CC1', 'MISSING'), ('CC1', 'PAID')
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO payment_collections (cost_center, order_number, amount)
                VALUES ('CC1', 'PAID', 100)
                """
            )
        )

        _run_migration(connection, migration.upgrade, monkeypatch)

        rows = connection.execute(
            sa.text(
                """
                SELECT order_number, net_amount
                FROM vw_orders_missing_in_payment_collections
                ORDER BY order_number
                """
            )
        ).mappings().all()

    assert [dict(row) for row in rows] == [
        {"order_number": "MISSING", "net_amount": 100},
    ]
