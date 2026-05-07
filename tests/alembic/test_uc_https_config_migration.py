from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any, Callable

import pytest
import sqlalchemy as sa

from alembic.migration import MigrationContext
from alembic.operations import Operations


CONFIG_KEY = "UC_IGNORE_HTTPS_ERRORS"


def _load_migration_module():
    project_root = Path(__file__).resolve().parents[2]
    module_path = project_root / "alembic" / "versions" / "0102_uc_https_config.py"
    spec = importlib.util.spec_from_file_location("v0102_uc_https_config", module_path)
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


def _system_config_rows(connection: sa.Connection, key: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        sa.text(
            """
            SELECT key, value, is_active, description
            FROM system_config
            WHERE key = :key
            """
        ),
        {"key": key},
    ).mappings().all()
    return [dict(row) for row in rows]


def test_uc_https_config_defaults_to_strict_tls_and_can_downgrade(
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

    with engine.begin() as connection:
        _run_migration(connection, migration.upgrade, monkeypatch)
        rows = _system_config_rows(connection, CONFIG_KEY)

    assert rows == [
        {
            "key": CONFIG_KEY,
            "value": "false",
            "is_active": True,
            "description": (
                "If true, UC Playwright contexts ignore HTTPS certificate errors; "
                "keep false except during emergency certificate incidents."
            ),
        }
    ]

    with engine.begin() as connection:
        _run_migration(connection, migration.downgrade, monkeypatch)
        rows = _system_config_rows(connection, CONFIG_KEY)

    assert rows == []
