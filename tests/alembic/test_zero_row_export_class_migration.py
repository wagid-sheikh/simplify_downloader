from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any, Callable

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations


def _load_migration_module() -> Any:
    project_root = Path(__file__).resolve().parents[2]
    module_path = (
        project_root / "alembic" / "versions" / "0119_zero_row_export_class.py"
    )
    spec = importlib.util.spec_from_file_location(
        "v0119_zero_row_export_class", module_path
    )
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


def _column_names(connection: sa.Connection) -> set[str]:
    return {
        column["name"]
        for column in sa.inspect(connection).get_columns("orders_sync_log")
    }


def test_zero_row_export_classification_upgrade_and_downgrade(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(
            sa.text("CREATE TABLE orders_sync_log (id INTEGER PRIMARY KEY)")
        )
        assert "zero_row_export_classification" not in _column_names(connection)

        _run_migration(connection, migration.upgrade, monkeypatch)
        columns = _column_names(connection)
        assert "zero_row_export_classification" in columns

        connection.execute(sa.text("""
                INSERT INTO orders_sync_log (id, zero_row_export_classification)
                VALUES (1, 'source_no_data')
                """))
        classification = connection.scalar(sa.text("""
                SELECT zero_row_export_classification
                FROM orders_sync_log
                WHERE id = 1
                """))
        assert classification == "source_no_data"

        _run_migration(connection, migration.downgrade, monkeypatch)
        assert "zero_row_export_classification" not in _column_names(connection)
