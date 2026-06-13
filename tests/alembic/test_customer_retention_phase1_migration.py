from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Callable

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError

from alembic.migration import MigrationContext
from alembic.operations import Operations


def _load_migration_module():
    project_root = Path(__file__).resolve().parents[2]
    module_path = project_root / "alembic" / "versions" / "0128_customer_retention_p1.py"
    spec = importlib.util.spec_from_file_location("v0128_customer_retention_p1", module_path)
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


def test_customer_retention_phase1_upgrade_creates_schema_and_seed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        _create_store_master(connection)
        _run_migration(connection, migration.upgrade, monkeypatch)

        inspector = sa.inspect(connection)
        assert "customer_retention_pipeline" in {
            column["name"] for column in inspector.get_columns("store_master")
        }
        assert {
            "trx_customer_followup_leads",
            "trx_customer_followup_history",
            "trx_customer_suppression",
            "trx_external_leads",
            "customer_followup_cap_config",
        }.issubset(set(inspector.get_table_names()))

        lead_columns = {column["name"] for column in inspector.get_columns("trx_customer_followup_leads")}
        assert {
            "lead_id",
            "lead_uuid",
            "lead_source_type",
            "source_system",
            "source_table_name",
            "source_record_id",
            "cost_center",
            "normalized_mobile_number",
            "lead_status",
            "lifecycle_bucket",
            "created_by_pipeline_run_id",
        }.issubset(lead_columns)

        suppression_columns = {
            column["name"] for column in inspector.get_columns("trx_customer_suppression")
        }
        assert {
            "suppression_state",
            "approval_required",
            "approved_at",
            "approved_by",
            "approval_remarks",
        }.issubset(suppression_columns)

        lead_indexes = {index["name"] for index in inspector.get_indexes("trx_customer_followup_leads")}
        assert {
            "ix_cfl_cost_center_mobile",
            "ix_cfl_cost_center_status",
            "ix_cfl_cost_center_source",
            "ix_cfl_next_followup",
            "ix_cfl_source_record",
            "ix_cfl_closed_recovered",
            "uq_cfl_td_source_record",
            "uq_cfl_external_source_record",
            "uq_cfl_retention_customer_bucket_run",
        }.issubset(lead_indexes)

        assert {
            "ix_cfs_cost_center_mobile",
            "ix_cfs_until",
            "ix_cfs_permanent",
        }.issubset({index["name"] for index in inspector.get_indexes("trx_customer_suppression")})
        assert {
            "ix_external_leads_cost_center_mobile",
            "ix_external_leads_status",
            "ix_external_leads_converted",
        }.issubset({index["name"] for index in inspector.get_indexes("trx_external_leads")})

        seed_row = connection.execute(
            sa.text(
                """
                SELECT cost_center, lead_source_type, work_section, daily_cap, is_uncapped, enabled
                FROM customer_followup_cap_config
                """
            )
        ).one()
        assert seed_row == (None, "RETENTION", "FRESH_RETENTION", 13, 0, 1)


def test_customer_retention_phase1_constraints_and_idempotency_indexes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        _create_store_master(connection)
        _run_migration(connection, migration.upgrade, monkeypatch)

        with pytest.raises(IntegrityError):
            connection.execute(
                sa.text(
                    """
                    INSERT INTO customer_followup_cap_config (
                        cap_config_id, lead_source_type, work_section, daily_cap,
                        is_uncapped, enabled, effective_from, created_at, updated_at
                    ) VALUES (
                        2, 'RETENTION', 'FRESH_RETENTION', NULL,
                        false, true, '2026-01-01', '2026-01-01 00:00:00', '2026-01-01 00:00:00'
                    )
                    """
                )
            )

        with pytest.raises(IntegrityError):
            connection.execute(
                sa.text(
                    """
                    INSERT INTO customer_followup_cap_config (
                        cap_config_id, lead_source_type, work_section, daily_cap,
                        is_uncapped, enabled, effective_from, created_at, updated_at
                    ) VALUES (
                        3, 'TD', 'TD_LEAD', 1,
                        false, true, '2026-01-01', '2026-01-01 00:00:00', '2026-01-01 00:00:00'
                    )
                    """
                )
            )

        connection.execute(
            sa.text(
                """
                INSERT INTO trx_customer_followup_leads (
                    lead_id, lead_uuid, lead_source_type, source_system, source_table_name,
                    source_record_id, cost_center, normalized_mobile_number, lead_date,
                    lead_status, lifecycle_bucket, created_at, updated_at
                ) VALUES (
                    1, '00000000-0000-0000-0000-000000000001', 'TD', 'TD_LEADS_SYNC', 'crm_leads',
                    'TD-1', 'CC001', '9999999999', '2026-06-12',
                    'OPEN', 'WARM', '2026-06-12 00:00:00', '2026-06-12 00:00:00'
                )
                """
            )
        )
        with pytest.raises(IntegrityError):
            connection.execute(
                sa.text(
                    """
                    INSERT INTO trx_customer_followup_leads (
                        lead_id, lead_uuid, lead_source_type, source_system, source_table_name,
                        source_record_id, cost_center, normalized_mobile_number, lead_date,
                        lead_status, lifecycle_bucket, created_at, updated_at
                    ) VALUES (
                        2, '00000000-0000-0000-0000-000000000002', 'TD', 'TD_LEADS_SYNC', 'crm_leads',
                        'TD-1', 'CC001', '9999999999', '2026-06-12',
                        'OPEN', 'WARM', '2026-06-12 00:00:00', '2026-06-12 00:00:00'
                    )
                    """
                )
            )


def test_customer_retention_phase1_downgrade_is_forward_only_noop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        _create_store_master(connection)
        _run_migration(connection, migration.upgrade, monkeypatch)

        result = _run_migration(connection, migration.downgrade, monkeypatch)

        assert result is None
        inspector = sa.inspect(connection)
        assert "customer_retention_pipeline" in {
            column["name"] for column in inspector.get_columns("store_master")
        }
        assert {
            "trx_customer_followup_leads",
            "trx_customer_followup_history",
            "trx_customer_suppression",
            "trx_external_leads",
            "customer_followup_cap_config",
        }.issubset(set(inspector.get_table_names()))
