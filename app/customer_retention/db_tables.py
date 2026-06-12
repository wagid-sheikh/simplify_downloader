"""SQLAlchemy table contracts for the customer retention Phase 1 schema."""

from __future__ import annotations

import sqlalchemy as sa

from .constants import (
    CAP_CONFIG_WORK_SECTIONS,
    LEAD_SOURCE_TYPES,
    LEAD_STATUSES,
    LIFECYCLE_BUCKETS,
    SUPPRESSION_STATES,
)

metadata = sa.MetaData()


def _sql_in(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


_LEAD_SOURCE_SQL = _sql_in(LEAD_SOURCE_TYPES)
_LEAD_STATUS_SQL = _sql_in(LEAD_STATUSES)
_LIFECYCLE_BUCKET_SQL = _sql_in(LIFECYCLE_BUCKETS)
_SUPPRESSION_STATE_SQL = _sql_in(SUPPRESSION_STATES)
_CAP_WORK_SECTION_SQL = _sql_in(CAP_CONFIG_WORK_SECTIONS)

trx_customer_followup_leads = sa.Table(
    "trx_customer_followup_leads",
    metadata,
    sa.Column("lead_id", sa.BigInteger(), primary_key=True, autoincrement=True),
    sa.Column("lead_uuid", sa.String(length=36), nullable=False),
    sa.Column("lead_source_type", sa.String(length=16), nullable=False),
    sa.Column("source_system", sa.String(length=64), nullable=False),
    sa.Column("source_table_name", sa.String(length=128), nullable=True),
    sa.Column("source_record_id", sa.String(length=128), nullable=True),
    sa.Column("source_reference", sa.Text(), nullable=True),
    sa.Column("cost_center", sa.String(length=16), nullable=False),
    sa.Column("customer_name", sa.String(length=256), nullable=True),
    sa.Column("mobile_number", sa.String(length=32), nullable=True),
    sa.Column("normalized_mobile_number", sa.String(length=32), nullable=False),
    sa.Column("lead_date", sa.Date(), nullable=False),
    sa.Column("lead_status", sa.String(length=32), nullable=False),
    sa.Column("lead_stage", sa.String(length=64), nullable=True),
    sa.Column("lifecycle_bucket", sa.String(length=32), nullable=True),
    sa.Column("last_order_date", sa.Date(), nullable=True),
    sa.Column("days_since_last_order", sa.Integer(), nullable=True),
    sa.Column("total_orders", sa.Integer(), nullable=True),
    sa.Column("lifetime_spend", sa.Numeric(14, 2), nullable=True),
    sa.Column("average_order_value", sa.Numeric(14, 2), nullable=True),
    sa.Column("last_order_amount", sa.Numeric(14, 2), nullable=True),
    sa.Column("priority_score", sa.Numeric(8, 2), nullable=True),
    sa.Column("recommended_strategy", sa.Text(), nullable=True),
    sa.Column("assigned_store", sa.String(length=16), nullable=True),
    sa.Column("assigned_to", sa.String(length=128), nullable=True),
    sa.Column("handled_by", sa.String(length=128), nullable=True),
    sa.Column("contact_attempted", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    sa.Column("contact_mode", sa.String(length=32), nullable=True),
    sa.Column("customer_response", sa.String(length=64), nullable=True),
    sa.Column("order_expected", sa.String(length=16), nullable=True),
    sa.Column("next_followup_date", sa.Date(), nullable=True),
    sa.Column("complaint_flag", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    sa.Column("do_not_contact_flag", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    sa.Column("staff_remarks", sa.Text(), nullable=True),
    sa.Column("target_cost_center", sa.String(length=16), nullable=True),
    sa.Column("shifted_from_lead_id", sa.BigInteger(), nullable=True),
    sa.Column("shifted_from_cost_center", sa.String(length=16), nullable=True),
    sa.Column("is_closed", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("closed_reason", sa.String(length=128), nullable=True),
    sa.Column("is_recovered", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    sa.Column("recovered_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("recovered_order_id", sa.String(length=128), nullable=True),
    sa.Column("suppression_applied", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    sa.Column("suppression_until", sa.Date(), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    sa.Column("created_by_pipeline_run_id", sa.String(length=64), nullable=True),
    sa.Column("updated_by_pipeline_run_id", sa.String(length=64), nullable=True),
    sa.ForeignKeyConstraint(["shifted_from_lead_id"], ["trx_customer_followup_leads.lead_id"]),
    sa.UniqueConstraint("lead_uuid", name="uq_customer_followup_leads_uuid"),
    sa.CheckConstraint(f"lead_source_type IN ({_LEAD_SOURCE_SQL})", name="ck_customer_followup_leads_source_type"),
    sa.CheckConstraint(f"lead_status IN ({_LEAD_STATUS_SQL})", name="ck_customer_followup_leads_status"),
    sa.CheckConstraint(
        "lead_source_type NOT IN ('TD', 'EXTERNAL') OR "
        "(source_system IS NOT NULL "
        "AND source_table_name IS NOT NULL "
        "AND source_record_id IS NOT NULL)",
        name="ck_cfl_source_identity_required",
    ),
    sa.CheckConstraint(
        "lead_source_type <> 'RETENTION' OR "
        "(lifecycle_bucket IS NOT NULL AND created_by_pipeline_run_id IS NOT NULL)",
        name="ck_cfl_retention_identity_required",
    ),
    sa.CheckConstraint(
        f"lifecycle_bucket IS NULL OR lifecycle_bucket IN ({_LIFECYCLE_BUCKET_SQL})",
        name="ck_customer_followup_leads_lifecycle_bucket",
    ),
)

trx_customer_followup_history = sa.Table(
    "trx_customer_followup_history",
    metadata,
    sa.Column("history_id", sa.BigInteger(), primary_key=True, autoincrement=True),
    sa.Column("lead_id", sa.BigInteger(), nullable=False),
    sa.Column("pipeline_run_id", sa.String(length=64), nullable=True),
    sa.Column("event_type", sa.String(length=64), nullable=False),
    sa.Column("previous_status", sa.String(length=32), nullable=True),
    sa.Column("new_status", sa.String(length=32), nullable=True),
    sa.Column("handled_by", sa.String(length=128), nullable=True),
    sa.Column("contact_attempted", sa.Boolean(), nullable=True),
    sa.Column("contact_mode", sa.String(length=32), nullable=True),
    sa.Column("customer_response", sa.String(length=64), nullable=True),
    sa.Column("order_expected", sa.String(length=16), nullable=True),
    sa.Column("next_followup_date", sa.Date(), nullable=True),
    sa.Column("complaint_flag", sa.Boolean(), nullable=True),
    sa.Column("do_not_contact_flag", sa.Boolean(), nullable=True),
    sa.Column("staff_remarks", sa.Text(), nullable=True),
    sa.Column("target_cost_center", sa.String(length=16), nullable=True),
    sa.Column("raw_excel_value_json", sa.JSON(), nullable=True),
    sa.Column("normalized_value_json", sa.JSON(), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    sa.ForeignKeyConstraint(["lead_id"], ["trx_customer_followup_leads.lead_id"], ondelete="CASCADE"),
)

trx_customer_suppression = sa.Table(
    "trx_customer_suppression",
    metadata,
    sa.Column("suppression_id", sa.BigInteger(), primary_key=True, autoincrement=True),
    sa.Column("cost_center", sa.String(length=16), nullable=False),
    sa.Column("mobile_number", sa.String(length=32), nullable=True),
    sa.Column("normalized_mobile_number", sa.String(length=32), nullable=False),
    sa.Column("suppression_reason", sa.String(length=128), nullable=False),
    sa.Column("suppression_state", sa.String(length=32), nullable=False),
    sa.Column("suppression_start_date", sa.Date(), nullable=False),
    sa.Column("suppression_until", sa.Date(), nullable=True),
    sa.Column("is_permanent", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    sa.Column("approval_required", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("approved_by", sa.String(length=128), nullable=True),
    sa.Column("approval_remarks", sa.Text(), nullable=True),
    sa.Column("source_lead_id", sa.BigInteger(), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    sa.Column("created_by_pipeline_run_id", sa.String(length=64), nullable=True),
    sa.ForeignKeyConstraint(["source_lead_id"], ["trx_customer_followup_leads.lead_id"]),
    sa.CheckConstraint(f"suppression_state IN ({_SUPPRESSION_STATE_SQL})", name="ck_customer_suppression_state"),
    sa.CheckConstraint(
        "(is_permanent AND suppression_until IS NULL) "
        "OR (NOT is_permanent AND suppression_until IS NOT NULL)",
        name="ck_customer_suppression_expiry_contract",
    ),
)

trx_external_leads = sa.Table(
    "trx_external_leads",
    metadata,
    sa.Column("external_lead_id", sa.BigInteger(), primary_key=True, autoincrement=True),
    sa.Column("external_lead_uuid", sa.String(length=36), nullable=False),
    sa.Column("lead_source", sa.String(length=64), nullable=False),
    sa.Column("campaign_name", sa.String(length=128), nullable=True),
    sa.Column("campaign_reference", sa.String(length=128), nullable=True),
    sa.Column("cost_center", sa.String(length=16), nullable=False),
    sa.Column("customer_name", sa.String(length=256), nullable=True),
    sa.Column("mobile_number", sa.String(length=32), nullable=True),
    sa.Column("normalized_mobile_number", sa.String(length=32), nullable=False),
    sa.Column("lead_date", sa.Date(), nullable=False),
    sa.Column("lead_status", sa.String(length=32), nullable=False),
    sa.Column("remarks", sa.Text(), nullable=True),
    sa.Column("assigned_to", sa.String(length=128), nullable=True),
    sa.Column("next_followup_date", sa.Date(), nullable=True),
    sa.Column("import_batch_id", sa.String(length=64), nullable=True),
    sa.Column("raw_payload_json", sa.JSON(), nullable=True),
    sa.Column("converted_to_followup_lead", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    sa.Column("converted_followup_lead_id", sa.BigInteger(), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    sa.ForeignKeyConstraint(["converted_followup_lead_id"], ["trx_customer_followup_leads.lead_id"]),
    sa.UniqueConstraint("external_lead_uuid", name="uq_external_leads_uuid"),
    sa.CheckConstraint(f"lead_status IN ({_LEAD_STATUS_SQL})", name="ck_external_leads_status"),
)

# Enabled cap config rows must not have overlapping inclusive effective date
# ranges within the same scope. Migration 0130 enforces this at the database
# level with PostgreSQL exclusion constraints (and SQLite test triggers):
# global scope is (cost_center IS NULL, lead_source_type, work_section); store
# scope is (cost_center, lead_source_type, work_section). NULL effective_until
# is an unbounded upper range. Disabled rows are historical and may overlap.
customer_followup_cap_config = sa.Table(
    "customer_followup_cap_config",
    metadata,
    sa.Column("cap_config_id", sa.BigInteger(), primary_key=True, autoincrement=True),
    sa.Column("cost_center", sa.String(length=16), nullable=True),
    sa.Column("lead_source_type", sa.String(length=16), nullable=False),
    sa.Column("work_section", sa.String(length=32), nullable=False),
    sa.Column("daily_cap", sa.Integer(), nullable=True),
    sa.Column("is_uncapped", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
    sa.Column("effective_from", sa.Date(), nullable=False),
    sa.Column("effective_until", sa.Date(), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    sa.CheckConstraint(f"lead_source_type IN ({_LEAD_SOURCE_SQL})", name="ck_customer_followup_cap_source_type"),
    sa.CheckConstraint(f"work_section IN ({_CAP_WORK_SECTION_SQL})", name="ck_customer_followup_cap_work_section"),
    sa.CheckConstraint("is_uncapped OR daily_cap IS NOT NULL", name="ck_customer_followup_cap_required"),
    sa.CheckConstraint("daily_cap IS NULL OR daily_cap > 0", name="ck_customer_followup_cap_positive"),
    sa.CheckConstraint("lead_source_type <> 'TD' OR is_uncapped", name="ck_customer_followup_cap_td_uncapped"),
    sa.CheckConstraint(
        "effective_until IS NULL OR effective_until >= effective_from",
        name="ck_customer_followup_cap_effective_range",
    ),
)
