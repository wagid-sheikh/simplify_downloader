from __future__ import annotations

from app.customer_retention import constants
from app.customer_retention.db_tables import (
    customer_followup_cap_config,
    trx_customer_followup_history,
    trx_customer_followup_leads,
    trx_customer_suppression,
    trx_external_leads,
)


def test_customer_retention_constants_expose_phase1_canonical_values() -> None:
    assert constants.LEAD_SOURCE_TYPES == ("RETENTION", "TD", "EXTERNAL")
    assert "FRESH_RETENTION" in constants.CAP_CONFIG_WORK_SECTIONS
    assert "PENDING_APPROVAL" in constants.SUPPRESSION_STATES
    assert "Lead Stale" in constants.WORKBOOK_OUTCOME_LABELS


def test_customer_retention_table_contracts_expose_required_columns() -> None:
    assert {
        "lead_id",
        "lead_source_type",
        "source_system",
        "source_table_name",
        "source_record_id",
        "normalized_mobile_number",
        "lifecycle_bucket",
        "created_by_pipeline_run_id",
    }.issubset(trx_customer_followup_leads.c.keys())
    assert {"history_id", "lead_id", "raw_excel_value_json", "normalized_value_json"}.issubset(
        trx_customer_followup_history.c.keys()
    )
    assert {"suppression_state", "approval_required", "approved_at", "approved_by"}.issubset(
        trx_customer_suppression.c.keys()
    )
    assert {"external_lead_id", "external_lead_uuid", "converted_followup_lead_id"}.issubset(
        trx_external_leads.c.keys()
    )
    assert {"lead_source_type", "work_section", "daily_cap", "is_uncapped"}.issubset(
        customer_followup_cap_config.c.keys()
    )
    lead_constraint_names = {constraint.name for constraint in trx_customer_followup_leads.constraints}
    assert "ck_cfl_source_identity_required" in lead_constraint_names
    assert "ck_cfl_retention_identity_required" in lead_constraint_names
