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
    module_path = project_root / "alembic" / "versions" / "0104_create_vw_orders.py"
    spec = importlib.util.spec_from_file_location("v0104_create_vw_orders", module_path)
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


def test_create_vw_orders_exposes_raw_columns_and_canonical_order_amount(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                CREATE TABLE orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_number TEXT NOT NULL,
                    source_system TEXT,
                    net_amount NUMERIC(12, 2),
                    gross_amount NUMERIC(12, 2),
                    adjustment NUMERIC(12, 2)
                )
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO orders (order_number, source_system, net_amount, gross_amount, adjustment)
                VALUES
                    ('TD-NET', 'TumbleDry', 100, 120, 10),
                    ('TD-GROSS-ZERO-NET', 'TumbleDry', 0, 120, 10),
                    ('TD-GROSS-NULL-NET', 'TumbleDry', NULL, 80, NULL),
                    ('TD-NULL-BASE', 'TumbleDry', NULL, NULL, NULL),
                    ('UC-GROSS', 'UClean', 20, 200, 0),
                    ('UNKNOWN-GROSS', 'FutureSystem', 30, 50, NULL),
                    ('FUTURE-GROSS', 'FutureSystem', 30, 50, 60),
                    ('NEGATIVE-ADJUSTMENT', 'UClean', 20, 100, -20)
                """
            )
        )

        _run_migration(connection, migration.upgrade, monkeypatch)

        columns = [column["name"] for column in sa.inspect(connection).get_columns("vw_orders")]
        assert columns == [
            "id",
            "order_number",
            "source_system",
            "net_amount",
            "gross_amount",
            "adjustment",
            "order_amount",
        ]

        rows = connection.execute(
            sa.text(
                """
                SELECT order_number, source_system, net_amount, gross_amount, adjustment, order_amount
                FROM vw_orders
                ORDER BY id
                """
            )
        ).mappings().all()

        assert [dict(row) for row in rows] == [
            {
                "order_number": "TD-NET",
                "source_system": "TumbleDry",
                "net_amount": 100,
                "gross_amount": 120,
                "adjustment": 10,
                "order_amount": 90,
            },
            {
                "order_number": "TD-GROSS-ZERO-NET",
                "source_system": "TumbleDry",
                "net_amount": 0,
                "gross_amount": 120,
                "adjustment": 10,
                "order_amount": 110,
            },
            {
                "order_number": "TD-GROSS-NULL-NET",
                "source_system": "TumbleDry",
                "net_amount": None,
                "gross_amount": 80,
                "adjustment": None,
                "order_amount": 80,
            },
            {
                "order_number": "TD-NULL-BASE",
                "source_system": "TumbleDry",
                "net_amount": None,
                "gross_amount": None,
                "adjustment": None,
                "order_amount": 0,
            },
            {
                "order_number": "UC-GROSS",
                "source_system": "UClean",
                "net_amount": 20,
                "gross_amount": 200,
                "adjustment": 0,
                "order_amount": 200,
            },
            {
                "order_number": "UNKNOWN-GROSS",
                "source_system": "FutureSystem",
                "net_amount": 30,
                "gross_amount": 50,
                "adjustment": None,
                "order_amount": 50,
            },
            {
                "order_number": "FUTURE-GROSS",
                "source_system": "FutureSystem",
                "net_amount": 30,
                "gross_amount": 50,
                "adjustment": 60,
                "order_amount": 0,
            },
            {
                "order_number": "NEGATIVE-ADJUSTMENT",
                "source_system": "UClean",
                "net_amount": 20,
                "gross_amount": 100,
                "adjustment": -20,
                "order_amount": 100,
            },
        ]

        _run_migration(connection, migration.downgrade, monkeypatch)
        assert sa.inspect(connection).has_table("vw_orders")
