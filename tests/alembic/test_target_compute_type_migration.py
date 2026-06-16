from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any, Callable

import pytest
import sqlalchemy as sa

from alembic.migration import MigrationContext
from alembic.operations import Operations


def _load_migration_module():
    project_root = Path(__file__).resolve().parents[2]
    module_path = project_root / "alembic" / "versions" / "0136_seed_target_compute_type.py"
    spec = importlib.util.spec_from_file_location("v0136_seed_target_compute_type", module_path)
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


def _system_config_rows(connection: sa.Connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        sa.text(
            """
            SELECT key, value, description, is_active
            FROM system_config
            ORDER BY key
            """
        )
    ).mappings().all()
    return [dict(row) for row in rows]


def _system_config_row(connection: sa.Connection, key: str) -> dict[str, Any] | None:
    row = connection.execute(
        sa.text(
            """
            SELECT key, value, description, is_active
            FROM system_config
            WHERE key = :key
            """
        ),
        {"key": key},
    ).mappings().one_or_none()
    return dict(row) if row is not None else None


def test_target_compute_type_upgrade_seeds_sales_and_downgrade_removes_only_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                CREATE TABLE system_config (
                    id INTEGER PRIMARY KEY,
                    key TEXT NOT NULL UNIQUE,
                    value TEXT,
                    description TEXT,
                    is_active BOOLEAN NOT NULL DEFAULT 1,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO system_config (key, value, description, is_active)
                VALUES (:key, :value, :description, :is_active)
                """
            ),
            {
                "key": "EXISTING_KEY",
                "value": "keep-me",
                "description": "Existing config row.",
                "is_active": True,
            },
        )

    with engine.begin() as connection:
        _run_migration(connection, migration.upgrade, monkeypatch)
        _run_migration(connection, migration.upgrade, monkeypatch)
        upgraded = _system_config_row(connection, "TARGET_COMPUTE_TYPE")
        upgraded_rows = _system_config_rows(connection)

    assert upgraded == {
        "key": "TARGET_COMPUTE_TYPE",
        "value": "SALES",
        "description": "Controls report target computation mode: SALES or COLLECTIONS.",
        "is_active": True,
    }
    assert [row["key"] for row in upgraded_rows] == ["EXISTING_KEY", "TARGET_COMPUTE_TYPE"]

    with engine.begin() as connection:
        _run_migration(connection, migration.downgrade, monkeypatch)
        downgraded_rows = _system_config_rows(connection)

    assert downgraded_rows == [
        {
            "key": "EXISTING_KEY",
            "value": "keep-me",
            "description": "Existing config row.",
            "is_active": True,
        }
    ]
