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
    module_path = project_root / "alembic" / "versions" / "0082_uc_pending_default_false.py"
    spec = importlib.util.spec_from_file_location("v0082_uc_pending_default_false", module_path)
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


def _system_config_row(connection: sa.Connection, key: str) -> dict[str, Any]:
    row = connection.execute(
        sa.text(
            """
            SELECT key, value, is_active, created_at, updated_at
            FROM system_config
            WHERE key = :key
            """
        ),
        {"key": key},
    ).mappings().one()
    return dict(row)


def test_uc_pending_default_false_upgrade_and_downgrade(monkeypatch: pytest.MonkeyPatch) -> None:
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
                "key": "SKIP_UC_Pending_Delivery",
                "value": "true",
                "description": "Skip UC orders in pending deliveries report.",
                "is_active": True,
            },
        )

    with engine.begin() as connection:
        _run_migration(connection, migration.upgrade, monkeypatch)
        upgraded = _system_config_row(connection, "SKIP_UC_Pending_Delivery")

    assert upgraded["key"] == "SKIP_UC_Pending_Delivery"
    assert upgraded["value"] == "false"
    assert bool(upgraded["is_active"]) is True
    assert upgraded["created_at"] is not None
    assert upgraded["updated_at"] is not None

    with engine.begin() as connection:
        _run_migration(connection, migration.downgrade, monkeypatch)
        downgraded = _system_config_row(connection, "SKIP_UC_Pending_Delivery")

    assert downgraded["key"] == "SKIP_UC_Pending_Delivery"
    assert downgraded["value"] == "true"
    assert bool(downgraded["is_active"]) is True
    assert downgraded["created_at"] is not None
    assert downgraded["updated_at"] is not None
