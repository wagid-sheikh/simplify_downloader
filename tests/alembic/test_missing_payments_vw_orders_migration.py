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
    module_path = project_root / "alembic" / "versions" / "0105_missing_payments_vworders.py"
    spec = importlib.util.spec_from_file_location("v0105_missing_payments_vworders", module_path)
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


def test_missing_payments_view_uses_vw_orders_order_amount_and_one_rupee_tolerance(
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
                    ('CC1', 'NO-PAYMENT', '2026-05-01', 'No Payment', '9001', 1, 2, 3, 100),
                    ('CC1', 'PARTIAL', '2026-05-01', 'Partial', '9002', 1, 2, 3, 100),
                    ('CC1', 'TOLERATED', '2026-05-01', 'Tolerated', '9003', 1, 2, 3, 100),
                    ('CC1', 'FULL', '2026-05-01', 'Full', '9004', 1, 2, 3, 100),
                    ('CC1', 'ZERO', '2026-05-01', 'Zero', '9005', 100, 100, 0, 0),
                    ('CC1', 'USES-ORDER-AMOUNT', '2026-05-01', 'Canonical', '9006', 0, 0, 999, 100),
                    ('CC1', 'NO-SALE', '2026-05-01', 'No Sale', '9007', 0, 0, 999, 100)
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO sales (cost_center, order_number)
                VALUES
                    ('CC1', 'NO-PAYMENT'),
                    ('CC1', 'PARTIAL'),
                    ('CC1', 'TOLERATED'),
                    ('CC1', 'FULL'),
                    ('CC1', 'ZERO'),
                    ('CC1', 'USES-ORDER-AMOUNT')
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO payment_collections (cost_center, order_number, amount)
                VALUES
                    ('CC1', 'PARTIAL', 98),
                    ('CC1', 'TOLERATED', 99),
                    ('CC1', 'FULL', 100),
                    ('CC1', 'USES-ORDER-AMOUNT', 50)
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
        {"order_number": "NO-PAYMENT", "net_amount": 100},
        {"order_number": "PARTIAL", "net_amount": 100},
        {"order_number": "USES-ORDER-AMOUNT", "net_amount": 100},
    ]


def test_postgres_view_sql_uses_canonical_amount_and_not_source_amount_logic() -> None:
    normalized_sql = " ".join(migration.POSTGRES_VIEW_SQL.split()).lower()

    assert "from public.vw_orders as o" in normalized_sql
    assert "order_amount as net_amount" in normalized_sql
    assert "order_amount > 0" in normalized_sql
    assert "not (paid_amount + 1 >= order_amount)" in normalized_sql
    assert "o.net_amount" not in normalized_sql
    assert "o.gross_amount" not in normalized_sql
    assert "o.adjustment" not in normalized_sql
    assert "case" not in normalized_sql
