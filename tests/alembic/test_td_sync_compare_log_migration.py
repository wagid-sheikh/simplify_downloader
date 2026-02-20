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
    module_path = project_root / "alembic" / "versions" / "0072_add_td_sync_compare_log.py"
    spec = importlib.util.spec_from_file_location("v0072_add_td_sync_compare_log", module_path)
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


def test_td_sync_compare_log_upgrade_and_downgrade(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        _run_migration(connection, migration.upgrade, monkeypatch)

    with engine.connect() as connection:
        inspector = sa.inspect(connection)
        tables = set(inspector.get_table_names())
        assert "td_sync_compare_log" in tables

        columns = {column["name"] for column in inspector.get_columns("td_sync_compare_log")}
        assert {
            "run_id",
            "run_env",
            "store_code",
            "from_date",
            "to_date",
            "source_mode",
            "total_rows",
            "matched_rows",
            "missing_in_api",
            "missing_in_ui",
            "amount_mismatches",
            "status_mismatches",
            "sample_mismatch_keys",
            "decision",
            "reason",
            "created_at",
            "updated_at",
        }.issubset(columns)

        index_names = {index["name"] for index in inspector.get_indexes("td_sync_compare_log")}
        assert "ix_td_sync_compare_log_run_id_store_code" in index_names
        assert "ix_td_sync_compare_log_store_code_from_date_to_date" in index_names

    with engine.begin() as connection:
        _run_migration(connection, migration.downgrade, monkeypatch)

    with engine.connect() as connection:
        inspector = sa.inspect(connection)
        tables_after = set(inspector.get_table_names())
        assert "td_sync_compare_log" not in tables_after
