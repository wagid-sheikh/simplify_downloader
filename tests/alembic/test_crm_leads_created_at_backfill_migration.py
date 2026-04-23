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
    module_path = project_root / "alembic" / "versions" / "0088_backfill_crm_leads_created_at.py"
    spec = importlib.util.spec_from_file_location("v0088_backfill_crm_leads_created_at", module_path)
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


def test_crm_leads_created_at_backfill_upgrade(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                CREATE TABLE crm_leads (
                    id INTEGER PRIMARY KEY,
                    pickup_created_date TEXT,
                    pickup_created_at TEXT
                )
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO crm_leads (id, pickup_created_date, pickup_created_at)
                VALUES
                    (1, '21 Apr 2026 3:03:39 PM', NULL),
                    (2, '21 Apr 2026 3:03 PM', NULL),
                    (3, '21 Apr 2026', NULL),
                    (4, '2026-04-21T15:03:39', NULL),
                    (5, '2026-04-21T09:33:39Z', NULL),
                    (6, 'invalid value', NULL),
                    (7, NULL, NULL),
                    (8, '21 Apr 2026 3:03:39 PM', '2026-01-01T00:00:00+00:00')
                """
            )
        )

    with engine.begin() as connection:
        _run_migration(connection, migration.upgrade, monkeypatch)

        second_level = _parse_datetime(
            connection.execute(sa.text("SELECT pickup_created_at FROM crm_leads WHERE id = 1")).scalar_one()
        )
        assert (second_level.year, second_level.month, second_level.day) == (2026, 4, 21)
        assert (second_level.hour, second_level.minute, second_level.second) == (9, 33, 39)

        minute_level = _parse_datetime(
            connection.execute(sa.text("SELECT pickup_created_at FROM crm_leads WHERE id = 2")).scalar_one()
        )
        assert (minute_level.hour, minute_level.minute, minute_level.second) == (9, 33, 0)

        date_only = _parse_datetime(
            connection.execute(sa.text("SELECT pickup_created_at FROM crm_leads WHERE id = 3")).scalar_one()
        )
        assert (date_only.year, date_only.month, date_only.day) == (2026, 4, 20)
        assert (date_only.hour, date_only.minute, date_only.second) == (18, 30, 0)

        iso_naive = _parse_datetime(
            connection.execute(sa.text("SELECT pickup_created_at FROM crm_leads WHERE id = 4")).scalar_one()
        )
        assert (iso_naive.hour, iso_naive.minute, iso_naive.second) == (9, 33, 39)

        iso_utc = _parse_datetime(
            connection.execute(sa.text("SELECT pickup_created_at FROM crm_leads WHERE id = 5")).scalar_one()
        )
        assert (iso_utc.hour, iso_utc.minute, iso_utc.second) == (9, 33, 39)

        unparsable = connection.execute(sa.text("SELECT pickup_created_at FROM crm_leads WHERE id = 6")).scalar_one()
        assert unparsable is None

        null_source = connection.execute(sa.text("SELECT pickup_created_at FROM crm_leads WHERE id = 7")).scalar_one()
        assert null_source is None

        prefilled = connection.execute(sa.text("SELECT pickup_created_at FROM crm_leads WHERE id = 8")).scalar_one()
        assert _parse_datetime(prefilled) == datetime.fromisoformat("2026-01-01T00:00:00+00:00")
