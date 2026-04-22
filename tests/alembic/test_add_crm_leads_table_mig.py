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
    module_path = project_root / "alembic" / "versions" / "0083_add_crm_leads_table.py"
    spec = importlib.util.spec_from_file_location("v0083_add_crm_leads_table", module_path)
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


def test_add_crm_leads_table_upgrade_and_downgrade(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        _run_migration(connection, migration.upgrade, monkeypatch)
        inspector = sa.inspect(connection)
        assert "crm_leads" in inspector.get_table_names()
        columns = {column["name"] for column in inspector.get_columns("crm_leads")}
        assert {"lead_uid", "store_code", "status_bucket", "pickup_id", "run_id", "run_env"}.issubset(columns)
        indexes = {index["name"] for index in inspector.get_indexes("crm_leads")}
        assert {"ix_crm_leads_store_status", "ix_crm_leads_run_id"}.issubset(indexes)

    with engine.begin() as connection:
        _run_migration(connection, migration.downgrade, monkeypatch)
        inspector = sa.inspect(connection)
        assert "crm_leads" not in inspector.get_table_names()
