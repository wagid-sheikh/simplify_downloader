from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any, Callable

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.exc import IntegrityError

from tests.alembic.test_customer_retention_phase1_migration import _create_store_master


def _load_migration_module(filename: str, module_name: str) -> Any:
    project_root = Path(__file__).resolve().parents[2]
    module_path = project_root / "alembic" / "versions" / filename
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load migration module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


phase1_migration = _load_migration_module(
    "0128_customer_retention_p1.py", "v0128_customer_retention_p1_for_source_identity"
)
source_identity_migration = _load_migration_module(
    "0129_cfl_source_identity.py", "v0129_cfl_source_identity"
)


def _run_migration(
    connection: sa.Connection,
    migration: Any,
    fn: Callable[[], None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = MigrationContext.configure(connection)
    operations = Operations(context)
    original_op = migration.op
    monkeypatch.setattr(migration, "op", operations)
    try:
        fn()
    finally:
        monkeypatch.setattr(migration, "op", original_op)


def _upgrade_through_source_identity(
    connection: sa.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    _create_store_master(connection)
    _run_migration(connection, phase1_migration, phase1_migration.upgrade, monkeypatch)
    _run_migration(
        connection,
        source_identity_migration,
        source_identity_migration.upgrade,
        monkeypatch,
    )


def _insert_followup_lead(
    connection: sa.Connection,
    *,
    lead_id: int,
    lead_source_type: str,
    source_system: str | None = "TD_LEADS_SYNC",
    source_table_name: str | None = "crm_leads",
    source_record_id: str | None = "SRC-1",
) -> None:
    connection.execute(
        sa.text(
            """
            INSERT INTO trx_customer_followup_leads (
                lead_id, lead_uuid, lead_source_type, source_system, source_table_name,
                source_record_id, cost_center, normalized_mobile_number, lead_date,
                lead_status, lifecycle_bucket, created_at, updated_at
            ) VALUES (
                :lead_id, :lead_uuid, :lead_source_type, :source_system, :source_table_name,
                :source_record_id, 'CC001', :normalized_mobile_number, '2026-06-12',
                'OPEN', 'WARM', '2026-06-12 00:00:00', '2026-06-12 00:00:00'
            )
            """
        ),
        {
            "lead_id": lead_id,
            "lead_uuid": f"00000000-0000-0000-0000-{lead_id:012d}",
            "lead_source_type": lead_source_type,
            "source_system": source_system,
            "source_table_name": source_table_name,
            "source_record_id": source_record_id,
            "normalized_mobile_number": f"999999{lead_id:04d}",
        },
    )


@pytest.mark.parametrize(
    ("lead_source_type", "source_table_name", "source_record_id"),
    [
        pytest.param("TD", "crm_leads", None, id="td_null_source_record_id"),
        pytest.param("TD", None, "TD-1", id="td_null_source_table_name"),
        pytest.param("EXTERNAL", "external_file", None, id="external_null_source_record_id"),
    ],
)
def test_customer_retention_source_identity_required_for_td_and_external(
    monkeypatch: pytest.MonkeyPatch,
    lead_source_type: str,
    source_table_name: str | None,
    source_record_id: str | None,
) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        _upgrade_through_source_identity(connection, monkeypatch)

        with pytest.raises(IntegrityError):
            _insert_followup_lead(
                connection,
                lead_id=1,
                lead_source_type=lead_source_type,
                source_system="TD_LEADS_SYNC" if lead_source_type == "TD" else "EXTERNAL_UPLOAD",
                source_table_name=source_table_name,
                source_record_id=source_record_id,
            )


def test_customer_retention_duplicate_td_source_identity_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        _upgrade_through_source_identity(connection, monkeypatch)
        _insert_followup_lead(
            connection,
            lead_id=1,
            lead_source_type="TD",
            source_system="TD_LEADS_SYNC",
            source_table_name="crm_leads",
            source_record_id="TD-1",
        )

        with pytest.raises(IntegrityError):
            _insert_followup_lead(
                connection,
                lead_id=2,
                lead_source_type="TD",
                source_system="TD_LEADS_SYNC",
                source_table_name="crm_leads",
                source_record_id="TD-1",
            )


def test_customer_retention_duplicate_external_source_identity_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        _upgrade_through_source_identity(connection, monkeypatch)
        _insert_followup_lead(
            connection,
            lead_id=1,
            lead_source_type="EXTERNAL",
            source_system="EXTERNAL_UPLOAD",
            source_table_name="external_file",
            source_record_id="EXT-1",
        )

        with pytest.raises(IntegrityError):
            _insert_followup_lead(
                connection,
                lead_id=2,
                lead_source_type="EXTERNAL",
                source_system="EXTERNAL_UPLOAD",
                source_table_name="external_file",
                source_record_id="EXT-1",
            )
