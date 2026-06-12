from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Callable

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.exc import IntegrityError


def _load_migration_module(filename: str, module_name: str):
    project_root = Path(__file__).resolve().parents[2]
    module_path = project_root / "alembic" / "versions" / filename
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load migration module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


phase1_migration = _load_migration_module("0128_customer_retention_p1.py", "v0128_customer_retention_p1")
overlap_migration = _load_migration_module("0130_cfcc_no_overlap.py", "v0130_cfcc_no_overlap")


def _run_migration(
    connection: sa.Connection,
    migration_module,
    fn: Callable[[], None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = MigrationContext.configure(connection)
    operations = Operations(context)
    original_op = migration_module.op
    monkeypatch.setattr(migration_module, "op", operations)
    try:
        fn()
    finally:
        monkeypatch.setattr(migration_module, "op", original_op)


def _create_store_master(connection: sa.Connection) -> None:
    connection.execute(
        sa.text(
            """
            CREATE TABLE store_master (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                store_code TEXT NOT NULL UNIQUE,
                cost_center TEXT,
                etl_flag BOOLEAN NOT NULL DEFAULT false,
                report_flag BOOLEAN NOT NULL DEFAULT false
            )
            """
        )
    )


@pytest.fixture
def connection(monkeypatch: pytest.MonkeyPatch):
    engine = sa.create_engine("sqlite://")
    with engine.begin() as conn:
        _create_store_master(conn)
        _run_migration(conn, phase1_migration, phase1_migration.upgrade, monkeypatch)
        _run_migration(conn, overlap_migration, overlap_migration.upgrade, monkeypatch)
        yield conn


def _insert_cap_config(
    connection: sa.Connection,
    cap_config_id: int,
    *,
    cost_center: str | None,
    effective_from: str,
    effective_until: str | None = None,
    enabled: bool = True,
) -> None:
    connection.execute(
        sa.text(
            """
            INSERT INTO customer_followup_cap_config (
                cap_config_id,
                cost_center,
                lead_source_type,
                work_section,
                daily_cap,
                is_uncapped,
                enabled,
                effective_from,
                effective_until,
                created_at,
                updated_at
            ) VALUES (
                :cap_config_id,
                :cost_center,
                'EXTERNAL',
                'EXTERNAL_LEAD',
                5,
                false,
                :enabled,
                :effective_from,
                :effective_until,
                '2026-01-01 00:00:00',
                '2026-01-01 00:00:00'
            )
            """
        ),
        {
            "cap_config_id": cap_config_id,
            "cost_center": cost_center,
            "enabled": enabled,
            "effective_from": effective_from,
            "effective_until": effective_until,
        },
    )


def test_enabled_overlapping_global_cap_ranges_fail(connection: sa.Connection) -> None:
    _insert_cap_config(connection, 10, cost_center=None, effective_from="2026-01-01")

    with pytest.raises(IntegrityError):
        _insert_cap_config(connection, 11, cost_center=None, effective_from="2026-02-01")


def test_enabled_overlapping_store_cap_ranges_fail(connection: sa.Connection) -> None:
    _insert_cap_config(
        connection,
        20,
        cost_center="CC001",
        effective_from="2026-01-01",
        effective_until="2026-01-31",
    )

    with pytest.raises(IntegrityError):
        _insert_cap_config(
            connection,
            21,
            cost_center="CC001",
            effective_from="2026-01-15",
            effective_until="2026-02-15",
        )


def test_adjacent_enabled_cap_ranges_are_allowed(connection: sa.Connection) -> None:
    _insert_cap_config(
        connection,
        30,
        cost_center="CC002",
        effective_from="2026-01-01",
        effective_until="2026-01-31",
    )

    _insert_cap_config(
        connection,
        31,
        cost_center="CC002",
        effective_from="2026-02-01",
        effective_until="2026-02-28",
    )

    rows = connection.execute(
        sa.text(
            """
            SELECT cap_config_id
            FROM customer_followup_cap_config
            WHERE cost_center = 'CC002'
            ORDER BY cap_config_id
            """
        )
    ).all()
    assert rows == [(30,), (31,)]


def test_disabled_overlapping_cap_ranges_are_allowed(connection: sa.Connection) -> None:
    _insert_cap_config(
        connection,
        40,
        cost_center="CC003",
        effective_from="2026-01-01",
        effective_until="2026-12-31",
        enabled=False,
    )

    _insert_cap_config(
        connection,
        41,
        cost_center="CC003",
        effective_from="2026-06-01",
        effective_until="2026-06-30",
        enabled=False,
    )

    rows = connection.execute(
        sa.text(
            """
            SELECT cap_config_id
            FROM customer_followup_cap_config
            WHERE cost_center = 'CC003'
            ORDER BY cap_config_id
            """
        )
    ).all()
    assert rows == [(40,), (41,)]
