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
    "0128_customer_retention_p1.py", "v0128_customer_retention_p1_for_retention_identity"
)
source_identity_migration = _load_migration_module(
    "0129_cfl_source_identity.py", "v0129_cfl_source_identity_for_retention_identity"
)
overlap_migration = _load_migration_module(
    "0130_cfcc_no_overlap.py", "v0130_cfcc_no_overlap_for_retention_identity"
)
retention_identity_migration = _load_migration_module(
    "0131_cfl_retention_identity.py", "v0131_cfl_retention_identity"
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


@pytest.fixture
def connection(monkeypatch: pytest.MonkeyPatch):
    engine = sa.create_engine("sqlite://")
    with engine.begin() as conn:
        _create_store_master(conn)
        _run_migration(conn, phase1_migration, phase1_migration.upgrade, monkeypatch)
        _run_migration(
            conn,
            source_identity_migration,
            source_identity_migration.upgrade,
            monkeypatch,
        )
        _run_migration(conn, overlap_migration, overlap_migration.upgrade, monkeypatch)
        _run_migration(
            conn,
            retention_identity_migration,
            retention_identity_migration.upgrade,
            monkeypatch,
        )
        yield conn


def _insert_followup_lead(
    connection: sa.Connection,
    *,
    lead_id: int,
    lead_source_type: str,
    cost_center: str = "CC001",
    normalized_mobile_number: str = "9999999999",
    lifecycle_bucket: str | None = "WARM",
    created_by_pipeline_run_id: str | None = "retention-run-1",
    source_system: str = "RETENTION_PIPELINE",
    source_table_name: str | None = None,
    source_record_id: str | None = None,
) -> None:
    connection.execute(
        sa.text(
            """
            INSERT INTO trx_customer_followup_leads (
                lead_id,
                lead_uuid,
                lead_source_type,
                source_system,
                source_table_name,
                source_record_id,
                cost_center,
                normalized_mobile_number,
                lead_date,
                lead_status,
                lifecycle_bucket,
                created_by_pipeline_run_id,
                created_at,
                updated_at
            ) VALUES (
                :lead_id,
                :lead_uuid,
                :lead_source_type,
                :source_system,
                :source_table_name,
                :source_record_id,
                :cost_center,
                :normalized_mobile_number,
                '2026-06-12',
                'OPEN',
                :lifecycle_bucket,
                :created_by_pipeline_run_id,
                '2026-06-12 00:00:00',
                '2026-06-12 00:00:00'
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
            "cost_center": cost_center,
            "normalized_mobile_number": normalized_mobile_number,
            "lifecycle_bucket": lifecycle_bucket,
            "created_by_pipeline_run_id": created_by_pipeline_run_id,
        },
    )


def test_retention_row_with_null_lifecycle_bucket_fails(connection: sa.Connection) -> None:
    with pytest.raises(IntegrityError):
        _insert_followup_lead(
            connection,
            lead_id=1,
            lead_source_type="RETENTION",
            lifecycle_bucket=None,
        )


def test_retention_row_with_null_created_by_pipeline_run_id_fails(
    connection: sa.Connection,
) -> None:
    with pytest.raises(IntegrityError):
        _insert_followup_lead(
            connection,
            lead_id=1,
            lead_source_type="RETENTION",
            created_by_pipeline_run_id=None,
        )


def test_duplicate_retention_customer_bucket_run_fails(connection: sa.Connection) -> None:
    _insert_followup_lead(
        connection,
        lead_id=1,
        lead_source_type="RETENTION",
        cost_center="CC001",
        normalized_mobile_number="9999999999",
        lifecycle_bucket="WARM",
        created_by_pipeline_run_id="retention-run-1",
    )

    with pytest.raises(IntegrityError):
        _insert_followup_lead(
            connection,
            lead_id=2,
            lead_source_type="RETENTION",
            cost_center="CC001",
            normalized_mobile_number="9999999999",
            lifecycle_bucket="WARM",
            created_by_pipeline_run_id="retention-run-1",
        )


@pytest.mark.parametrize(
    ("lead_source_type", "source_system", "source_table_name", "source_record_id"),
    [
        pytest.param("TD", "TD_LEADS_SYNC", "crm_leads", "TD-1", id="td"),
        pytest.param("EXTERNAL", "EXTERNAL_UPLOAD", "external_file", "EXT-1", id="external"),
    ],
)
def test_td_and_external_rows_are_not_forced_to_provide_retention_identity_fields(
    connection: sa.Connection,
    lead_source_type: str,
    source_system: str,
    source_table_name: str,
    source_record_id: str,
) -> None:
    _insert_followup_lead(
        connection,
        lead_id=1,
        lead_source_type=lead_source_type,
        lifecycle_bucket=None,
        created_by_pipeline_run_id=None,
        source_system=source_system,
        source_table_name=source_table_name,
        source_record_id=source_record_id,
    )

    row = connection.execute(
        sa.text(
            """
            SELECT lead_source_type, lifecycle_bucket, created_by_pipeline_run_id
            FROM trx_customer_followup_leads
            WHERE lead_id = 1
            """
        )
    ).one()
    assert row == (lead_source_type, None, None)
