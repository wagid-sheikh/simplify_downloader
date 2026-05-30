from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any, Callable

import pytest
import sqlalchemy as sa

from alembic.migration import MigrationContext
from alembic.operations import Operations


DEPRECATED_DASHBOARD_URL = "https://store.ucleanlaundry.com/dashboard"
CURRENT_DASHBOARD_URL = "https://storepanel.ucleanlaundry.com/dashboard"


def _load_migration_module():
    project_root = Path(__file__).resolve().parents[2]
    module_path = project_root / "alembic" / "versions" / "0118_uc_dashboard_host.py"
    spec = importlib.util.spec_from_file_location("v0118_uc_dashboard_host", module_path)
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


def _store_configs(connection: sa.Connection) -> dict[str, dict[str, Any]]:
    rows = connection.execute(
        sa.text("SELECT store_code, sync_config FROM store_master ORDER BY store_code")
    ).mappings()
    return {row["store_code"]: json.loads(row["sync_config"]) for row in rows}


def test_uc_dashboard_host_upgrade_and_conservative_downgrade(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")
    deprecated_uc_config = {
        "urls": {
            "login": "https://store.ucleanlaundry.com/login",
            "home": DEPRECATED_DASHBOARD_URL,
            "orders_link": "https://store.ucleanlaundry.com/gst-report",
        },
        "username": "uc-user",
        "password": "uc-password",
        "backfill_days": 7,
    }
    already_current_uc_config = {
        "urls": {"home": CURRENT_DASHBOARD_URL},
        "username": "already-current",
    }
    non_uc_config = {
        "urls": {"home": DEPRECATED_DASHBOARD_URL},
        "username": "td-user",
    }

    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                CREATE TABLE store_master (
                    store_code TEXT PRIMARY KEY,
                    sync_group TEXT,
                    sync_config TEXT
                )
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO store_master (store_code, sync_group, sync_config)
                VALUES
                    (:deprecated_uc_code, 'UC', :deprecated_uc_config),
                    (:current_uc_code, 'UC', :current_uc_config),
                    (:non_uc_code, 'TD', :non_uc_config)
                """
            ),
            {
                "deprecated_uc_code": "UC567",
                "deprecated_uc_config": json.dumps(deprecated_uc_config),
                "current_uc_code": "UC610",
                "current_uc_config": json.dumps(already_current_uc_config),
                "non_uc_code": "A668",
                "non_uc_config": json.dumps(non_uc_config),
            },
        )

    with engine.begin() as connection:
        _run_migration(connection, migration.upgrade, monkeypatch)
        upgraded = _store_configs(connection)

    assert upgraded["UC567"] == {
        **deprecated_uc_config,
        "urls": {**deprecated_uc_config["urls"], "home": CURRENT_DASHBOARD_URL},
    }
    assert upgraded["UC610"] == already_current_uc_config
    assert upgraded["A668"] == non_uc_config

    with engine.begin() as connection:
        _run_migration(connection, migration.downgrade, monkeypatch)
        downgraded = _store_configs(connection)

    assert downgraded["UC567"] == deprecated_uc_config
    assert downgraded["UC610"] == {
        **already_current_uc_config,
        "urls": {"home": DEPRECATED_DASHBOARD_URL},
    }
    assert downgraded["A668"] == non_uc_config
