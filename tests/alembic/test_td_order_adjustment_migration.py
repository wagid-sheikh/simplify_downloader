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
    module_path = project_root / "alembic" / "versions" / "0103_td_order_adjustment.py"
    spec = importlib.util.spec_from_file_location("v0103_td_order_adjustment", module_path)
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


def _column_names(connection: sa.Connection, table_name: str) -> set[str]:
    inspector = sa.inspect(connection)
    return {column["name"] for column in inspector.get_columns(table_name)}


def test_td_order_adjustment_upgrade_backfills_latest_staging_row_and_downgrades(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                CREATE TABLE orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cost_center TEXT NOT NULL,
                    order_number TEXT NOT NULL,
                    order_date TEXT NOT NULL,
                    source_system TEXT NOT NULL
                )
                """
            )
        )
        connection.execute(
            sa.text(
                """
                CREATE TABLE stg_td_orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cost_center TEXT,
                    order_number TEXT,
                    order_date TEXT,
                    adjustment NUMERIC(12, 2),
                    run_date TEXT
                )
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO orders (cost_center, order_number, order_date, source_system)
                VALUES
                    ('UN3668', 'ORD-001', '2025-05-10 09:30:00', 'TumbleDry'),
                    ('UN3668', 'ORD-002', '2025-05-11 10:00:00', 'TumbleDry'),
                    ('UN3668', 'ORD-003', '2025-05-12 10:00:00', 'TumbleDry'),
                    ('UN3668', 'ORD-001', '2025-05-10 09:30:00', 'UC')
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO stg_td_orders (id, cost_center, order_number, order_date, adjustment, run_date)
                VALUES
                    (1, 'UN3668', 'ORD-001', '2025-05-10 09:30:00', 10.00, '2025-05-20 08:00:00'),
                    (2, 'UN3668', 'ORD-001', '2025-05-10 09:30:00', 15.25, '2025-05-20 09:00:00'),
                    (3, 'UN3668', 'ORD-002', '2025-05-11 10:00:00', 20.00, NULL),
                    (4, 'UN3668', 'ORD-002', '2025-05-11 10:00:00', 25.50, NULL),
                    (5, 'UN3668', 'ORD-003', '2025-05-12 10:00:00', NULL, '2025-05-20 10:00:00')
                """
            )
        )

        assert "adjustment" not in _column_names(connection, "orders")

        _run_migration(connection, migration.upgrade, monkeypatch)

        assert "adjustment" in _column_names(connection, "orders")
        rows = connection.execute(
            sa.text(
                """
                SELECT order_number, source_system, adjustment
                FROM orders
                ORDER BY id
                """
            )
        ).mappings().all()
        assert [dict(row) for row in rows] == [
            {"order_number": "ORD-001", "source_system": "TumbleDry", "adjustment": 15.25},
            {"order_number": "ORD-002", "source_system": "TumbleDry", "adjustment": 25.5},
            {"order_number": "ORD-003", "source_system": "TumbleDry", "adjustment": None},
            {"order_number": "ORD-001", "source_system": "UC", "adjustment": None},
        ]

        _run_migration(connection, migration.downgrade, monkeypatch)
        assert "adjustment" not in _column_names(connection, "orders")
