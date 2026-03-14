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
    module_path = project_root / "alembic" / "versions" / "0069_widen_orders_service_type_to_64.py"
    spec = importlib.util.spec_from_file_location("v0069_widen_orders_service_type_to_64", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load migration module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


migration = _load_migration_module()


def _run_migration(connection: sa.Connection, fn: Callable[[], None], monkeypatch: pytest.MonkeyPatch) -> None:
    context = MigrationContext.configure(connection)
    operations = Operations(context)
    original_op = migration.op
    monkeypatch.setattr(migration, "op", operations)
    try:
        fn()
    finally:
        monkeypatch.setattr(migration, "op", original_op)


def _service_type_len(engine: sa.Engine) -> int | None:
    with engine.connect() as connection:
        inspector = sa.inspect(connection)
        for column in inspector.get_columns("orders"):
            if column["name"] == "service_type":
                return getattr(column["type"], "length", None)
    return None


def test_widen_orders_service_type_upgrade_and_downgrade(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                CREATE TABLE orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_number TEXT,
                    order_date TIMESTAMP,
                    service_type VARCHAR(24)
                )
                """
            )
        )

    assert _service_type_len(engine) == 24

    with engine.begin() as connection:
        _run_migration(connection, migration.upgrade, monkeypatch)

    assert _service_type_len(engine) == 64

    with engine.begin() as connection:
        _run_migration(connection, migration.downgrade, monkeypatch)

    assert _service_type_len(engine) == 24
