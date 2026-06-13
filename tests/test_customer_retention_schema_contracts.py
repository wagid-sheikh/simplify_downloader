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


def test_customer_retention_cap_config_work_sections_exclude_uncapped_workbook_categories() -> None:
    uncapped_workbook_categories = {
        constants.CAP_WORK_SECTION_DUE_FOLLOWUP,
        constants.CAP_WORK_SECTION_PENDING_CARRY_FORWARD,
    }

    assert uncapped_workbook_categories.issubset(constants.CAP_WORK_SECTIONS)
    assert uncapped_workbook_categories.isdisjoint(constants.CAP_CONFIG_WORK_SECTIONS)

    cap_work_section_constraint = next(
        constraint
        for constraint in customer_followup_cap_config.constraints
        if constraint.name == "ck_customer_followup_cap_work_section"
    )
    constraint_sql = str(cap_work_section_constraint.sqltext)
    assert "DUE_FOLLOWUP" not in constraint_sql
    assert "PENDING_CARRY_FORWARD" not in constraint_sql


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
