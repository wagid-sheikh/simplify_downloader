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
    module_path = project_root / "alembic" / "versions" / "0097_payment_collections.py"
    spec = importlib.util.spec_from_file_location("v0097_payment_collections", module_path)
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


def test_payment_collections_uses_cost_center_for_missing_payments_views(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        _run_migration(connection, migration.upgrade, monkeypatch)

        inspector = sa.inspect(connection)
        columns = {column["name"] for column in inspector.get_columns("payment_collections")}
        assert "cost_center" in columns
        assert "store_code" not in columns

        indexes = {
            index["name"]: tuple(index["column_names"])
            for index in inspector.get_indexes("payment_collections")
        }
        assert indexes["idx_payment_collections_store_date"] == (
            "cost_center",
            "payment_date",
        )

        _run_migration(connection, migration.downgrade, monkeypatch)
        assert not inspector.has_table("payment_collections")
