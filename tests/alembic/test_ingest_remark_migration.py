from __future__ import annotations

import importlib.util
from pathlib import Path
from datetime import datetime
from typing import Callable
from zoneinfo import ZoneInfo

import pytest
import sqlalchemy as sa

from alembic.migration import MigrationContext
from alembic.operations import Operations


def _load_migration_module():
    project_root = Path(__file__).resolve().parents[2]
    module_path = project_root / "alembic" / "versions" / "0030_ingest_remark_orders.py"
    spec = importlib.util.spec_from_file_location("v0030_ingest_remark_orders", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load migration module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


migration = _load_migration_module()


def _order_columns(engine: sa.Engine) -> set[str]:
    with engine.connect() as connection:
        inspector = sa.inspect(connection)
        return {column["name"] for column in inspector.get_columns("orders")}


def _run_migration(connection: sa.Connection, fn: Callable[[], None], monkeypatch: pytest.MonkeyPatch) -> None:
    context = MigrationContext.configure(connection)
    operations = Operations(context)
    original_op = migration.op
    monkeypatch.setattr(migration, "op", operations)
    try:
        fn()
    finally:
        monkeypatch.setattr(migration, "op", original_op)


def test_orders_ingest_remark_migration_is_symmetrical(monkeypatch: pytest.MonkeyPatch) -> None:
    tz = ZoneInfo("Asia/Kolkata")
    order_date = datetime(2025, 5, 12, 10, 30, tzinfo=tz)
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                CREATE TABLE orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cost_center TEXT,
                    store_code TEXT,
                    order_number TEXT,
                    order_date TIMESTAMP,
                    ingest_remark TEXT
                )
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO orders (
                    cost_center,
                    store_code,
                    order_number,
                    order_date,
                    ingest_remark
                )
                VALUES (
                    :cost_center,
                    :store_code,
                    :order_number,
                    :order_date,
                    :ingest_remark
                )
                """
            ),
            {
                "cost_center": "UN1234",
                "store_code": "A123",
                "order_number": "ORD-001",
                "order_date": order_date,
                "ingest_remark": "invalid phone",
            },
        )

    assert "ingest_remark" in _order_columns(engine)

    with engine.begin() as connection:
        _run_migration(connection, migration.upgrade, monkeypatch)

    columns = _order_columns(engine)
    assert "ingest_remarks" in columns
    assert "ingest_remark" not in columns
    with engine.connect() as connection:
        remark = connection.execute(sa.text("SELECT ingest_remarks FROM orders")).scalar()
    assert remark == "invalid phone"

    with engine.begin() as connection:
        _run_migration(connection, migration.downgrade, monkeypatch)

    columns = _order_columns(engine)
    assert "ingest_remarks" in columns
    assert "ingest_remark" not in columns
    with engine.connect() as connection:
        remark = connection.execute(sa.text("SELECT ingest_remarks FROM orders")).scalar()
    assert remark == "invalid phone"
