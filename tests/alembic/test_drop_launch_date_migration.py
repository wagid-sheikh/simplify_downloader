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
    module_path = project_root / "alembic" / "versions" / "0125_drop_launch_date.py"
    spec = importlib.util.spec_from_file_location("v0125_drop_launch_date", module_path)
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
    return {column["name"] for column in sa.inspect(connection).get_columns("store_master")}


def test_drop_launch_date_upgrade_preserves_and_backfills_start_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                CREATE TABLE store_master (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    store_code TEXT NOT NULL UNIQUE,
                    start_date DATE,
                    launch_date DATE
                )
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO store_master (store_code, start_date, launch_date)
                VALUES
                    ('KEEP_START', '2025-01-01', '2024-01-01'),
                    ('BACKFILL_START', NULL, '2024-02-03'),
                    ('NO_DATES', NULL, NULL)
                """
            )
        )

        _run_migration(connection, migration.upgrade, monkeypatch)

        assert "launch_date" not in _column_names(connection)
        rows = connection.execute(
            sa.text("SELECT store_code, start_date FROM store_master ORDER BY store_code")
        ).all()
        assert rows == [
            ("BACKFILL_START", "2024-02-03"),
            ("KEEP_START", "2025-01-01"),
            ("NO_DATES", None),
        ]


def test_drop_launch_date_downgrade_restores_nullable_column_from_start_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                CREATE TABLE store_master (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    store_code TEXT NOT NULL UNIQUE,
                    start_date DATE
                )
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO store_master (store_code, start_date)
                VALUES ('HAS_START', '2025-03-04'), ('NO_START', NULL)
                """
            )
        )

        _run_migration(connection, migration.downgrade, monkeypatch)

        assert "launch_date" in _column_names(connection)
        rows = connection.execute(
            sa.text(
                "SELECT store_code, start_date, launch_date FROM store_master ORDER BY store_code"
            )
        ).all()
        assert rows == [
            ("HAS_START", "2025-03-04", "2025-03-04"),
            ("NO_START", None, None),
        ]


def test_drop_launch_date_revision_metadata() -> None:
    assert migration.revision == "0125_drop_launch_date"
    assert migration.down_revision == "0124_auto_recovered_category"
