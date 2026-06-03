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
    module_path = project_root / "alembic" / "versions" / "0123_oli_rebuild_progress.py"
    spec = importlib.util.spec_from_file_location(
        "v0123_oli_rebuild_progress", module_path
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


def test_oli_rebuild_progress_upgrade_adds_resume_window_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")
    with engine.begin() as connection:
        _run_migration(connection, migration.upgrade, monkeypatch)

        inspector = sa.inspect(connection)
        assert "order_line_items_rebuild_progress" in inspector.get_table_names()
        columns = {
            column["name"]
            for column in inspector.get_columns("order_line_items_rebuild_progress")
        }
        assert {
            "source",
            "store_code",
            "window_start",
            "window_end",
            "status",
            "attempt_no",
            "inserted_rows",
            "dry_run",
        } <= columns
        unique_constraints = {
            tuple(constraint["column_names"])
            for constraint in inspector.get_unique_constraints(
                "order_line_items_rebuild_progress"
            )
        }
        assert (
            "source",
            "store_code",
            "window_start",
            "window_end",
        ) in unique_constraints

        connection.execute(sa.text("""
            INSERT INTO order_line_items_rebuild_progress
            (source, store_code, window_start, window_end, run_id, status)
            VALUES ('td', 'TD001', '2025-01-01', '2025-01-30', 'run-1', 'success')
        """))

        with pytest.raises(sa.exc.IntegrityError):
            connection.execute(sa.text("""
                INSERT INTO order_line_items_rebuild_progress
                (source, store_code, window_start, window_end, run_id, status)
                VALUES ('td', 'TD001', '2025-01-01', '2025-01-30', 'run-2', 'failed')
            """))
