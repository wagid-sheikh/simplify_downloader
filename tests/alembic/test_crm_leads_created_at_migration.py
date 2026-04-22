from __future__ import annotations

import importlib.util
from datetime import datetime
from pathlib import Path
from typing import Callable

import pytest
import sqlalchemy as sa

from alembic.migration import MigrationContext
from alembic.operations import Operations


def _load_migration_module():
    project_root = Path(__file__).resolve().parents[2]
    module_path = project_root / "alembic" / "versions" / "0087_crm_leads_created_at.py"
    spec = importlib.util.spec_from_file_location("v0087_crm_leads_created_at", module_path)
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


def _parse_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    assert isinstance(value, str)
    normalized = value.replace("T", " ")
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    return datetime.fromisoformat(normalized)


def test_crm_leads_created_at_upgrade_and_downgrade(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                CREATE TABLE crm_leads (
                    id INTEGER PRIMARY KEY,
                    lead_uid TEXT NOT NULL,
                    store_code TEXT NOT NULL,
                    status_bucket TEXT NOT NULL,
                    pickup_created_date TEXT,
                    pickup_time TEXT
                )
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO crm_leads (id, lead_uid, store_code, status_bucket, pickup_created_date, pickup_time)
                VALUES
                    (1, 'A1', 'A001', 'new', '21 Apr 2026 3:03:39 PM', '11:00 AM - 1:00 PM'),
                    (2, 'A2', 'A001', 'new', 'invalid value', '9:00 AM - 11:00 AM'),
                    (3, 'A3', 'A001', 'closed', NULL, NULL)
                """
            )
        )

    with engine.begin() as connection:
        _run_migration(connection, migration.upgrade, monkeypatch)

        inspector = sa.inspect(connection)
        columns = {column["name"] for column in inspector.get_columns("crm_leads")}
        assert "pickup_created_at" in columns

        indexes = {index["name"] for index in inspector.get_indexes("crm_leads")}
        assert "ix_crm_leads_store_status_created_at" in indexes

        parsed_value = connection.execute(
            sa.text("SELECT pickup_created_at FROM crm_leads WHERE id = 1")
        ).scalar_one()
        parsed_dt = _parse_datetime(parsed_value)

        assert parsed_dt.year == 2026
        assert parsed_dt.month == 4
        assert parsed_dt.day == 21
        assert parsed_dt.hour == 9
        assert parsed_dt.minute == 33
        assert parsed_dt.second == 39

        unparsable_value = connection.execute(
            sa.text("SELECT pickup_created_at FROM crm_leads WHERE id = 2")
        ).scalar_one()
        assert unparsable_value is None

    with engine.begin() as connection:
        _run_migration(connection, migration.downgrade, monkeypatch)
        inspector = sa.inspect(connection)
        columns = {column["name"] for column in inspector.get_columns("crm_leads")}
        assert "pickup_created_at" not in columns
