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
    module_path = project_root / "alembic" / "versions" / "0076_add_stg_td_garments_weight.py"
    spec = importlib.util.spec_from_file_location("v0076_add_stg_td_garments_weight", module_path)
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


def _column_names(engine: sa.Engine, table_name: str) -> set[str]:
    with engine.connect() as connection:
        inspector = sa.inspect(connection)
        return {column["name"] for column in inspector.get_columns(table_name)}


def test_add_stg_td_garments_weight_upgrade_is_idempotent_and_downgrade(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                CREATE TABLE stg_td_garments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    store_code TEXT NOT NULL,
                    line_item_uid TEXT NOT NULL
                )
                """
            )
        )

    assert "weight" not in _column_names(engine, "stg_td_garments")

    with engine.begin() as connection:
        _run_migration(connection, migration.upgrade, monkeypatch)

    assert "weight" in _column_names(engine, "stg_td_garments")

    # Verify idempotency in environments where column already exists.
    with engine.begin() as connection:
        _run_migration(connection, migration.upgrade, monkeypatch)

    assert "weight" in _column_names(engine, "stg_td_garments")

    with engine.begin() as connection:
        _run_migration(connection, migration.downgrade, monkeypatch)

    assert "weight" not in _column_names(engine, "stg_td_garments")
