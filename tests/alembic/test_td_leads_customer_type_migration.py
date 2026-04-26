from __future__ import annotations

import importlib.util
from pathlib import Path

import sqlalchemy as sa

from alembic.migration import MigrationContext
from alembic.operations import Operations


def _load_migration_module():
    project_root = Path(__file__).resolve().parents[2]
    module_path = project_root / "alembic" / "versions" / "0094_td_leads_customer_type.py"
    spec = importlib.util.spec_from_file_location("v0094_td_leads_customer_type", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load migration module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


migration = _load_migration_module()


def test_upgrade_adds_customer_type_column(monkeypatch) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                CREATE TABLE crm_leads_current (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lead_uid TEXT,
                    store_code TEXT,
                    pickup_no TEXT,
                    status_bucket TEXT,
                    customer_name TEXT
                )
                """
            )
        )

        context = MigrationContext.configure(connection)
        operations = Operations(context)
        original_op = migration.op
        monkeypatch.setattr(migration, "op", operations)
        try:
            migration.upgrade()
        finally:
            monkeypatch.setattr(migration, "op", original_op)

        columns = connection.execute(sa.text("PRAGMA table_info('crm_leads_current')")).mappings().all()
        column_names = {str(row["name"]) for row in columns}

    assert "customer_type" in column_names
