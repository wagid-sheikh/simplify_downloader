"""Add customer retention Phase 1 schema."""

from __future__ import annotations

from alembic import op
from datetime import date, datetime, timezone
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0128_customer_retention_p1"
down_revision = "0127_oli_rebuild_rows_email"
branch_labels = None
depends_on = None

LEAD_SOURCE_TYPES = ("RETENTION", "TD", "EXTERNAL")
LEAD_STATUSES = (
    "OPEN",
    "PENDING",
    "DUE_FOLLOWUP",
    "WORKED",
    "CLOSED",
    "RECOVERED",
    "SUPPRESSED",
    "STALE",
    "ERROR",
)
LIFECYCLE_BUCKETS = ("ACTIVE", "WARM", "COOLING", "DORMANT", "COLD", "LOST")
SUPPRESSION_STATES = ("ACTIVE", "PENDING_APPROVAL", "REJECTED", "EXPIRED")
CAP_CONFIG_WORK_SECTIONS = ("TD_LEAD", "EXTERNAL_LEAD", "FRESH_RETENTION")


def _sql_in(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def _json_type() -> sa.types.TypeEngine:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        return postgresql.JSONB(astext_type=sa.Text())
    return sa.JSON()


def _partial_unique_index(name: str, table_name: str, columns: list[str], predicate: str) -> None:
    op.create_index(
        name,
        table_name,
        columns,
        unique=True,
        postgresql_where=sa.text(predicate),
        sqlite_where=sa.text(predicate),
    )


def upgrade() -> None:
    json_type = _json_type()
    lead_source_sql = _sql_in(LEAD_SOURCE_TYPES)
    lead_status_sql = _sql_in(LEAD_STATUSES)
    lifecycle_bucket_sql = _sql_in(LIFECYCLE_BUCKETS)
    suppression_state_sql = _sql_in(SUPPRESSION_STATES)
    cap_work_section_sql = _sql_in(CAP_CONFIG_WORK_SECTIONS)

    op.add_column(
        "store_master",
        sa.Column(
            "customer_retention_pipeline",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )

    op.create_table(
        "trx_customer_followup_leads",
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
        sa.CheckConstraint(f"lead_source_type IN ({lead_source_sql})", name="ck_customer_followup_leads_source_type"),
        sa.CheckConstraint(f"lead_status IN ({lead_status_sql})", name="ck_customer_followup_leads_status"),
        sa.CheckConstraint(
            f"lifecycle_bucket IS NULL OR lifecycle_bucket IN ({lifecycle_bucket_sql})",
            name="ck_customer_followup_leads_lifecycle_bucket",
        ),
    )

    op.create_table(
        "trx_customer_followup_history",
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
        sa.Column("raw_excel_value_json", json_type, nullable=True),
        sa.Column("normalized_value_json", json_type, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["lead_id"], ["trx_customer_followup_leads.lead_id"], ondelete="CASCADE"),
    )

    op.create_table(
        "trx_customer_suppression",
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
        sa.CheckConstraint(f"suppression_state IN ({suppression_state_sql})", name="ck_customer_suppression_state"),
        sa.CheckConstraint(
            "(is_permanent AND suppression_until IS NULL) "
            "OR (NOT is_permanent AND suppression_until IS NOT NULL)",
            name="ck_customer_suppression_expiry_contract",
        ),
    )

    op.create_table(
        "trx_external_leads",
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
        sa.Column("raw_payload_json", json_type, nullable=True),
        sa.Column("converted_to_followup_lead", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("converted_followup_lead_id", sa.BigInteger(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["converted_followup_lead_id"], ["trx_customer_followup_leads.lead_id"]),
        sa.UniqueConstraint("external_lead_uuid", name="uq_external_leads_uuid"),
        sa.CheckConstraint(f"lead_status IN ({lead_status_sql})", name="ck_external_leads_status"),
    )

    op.create_table(
        "customer_followup_cap_config",
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
        sa.CheckConstraint(f"lead_source_type IN ({lead_source_sql})", name="ck_customer_followup_cap_source_type"),
        sa.CheckConstraint(f"work_section IN ({cap_work_section_sql})", name="ck_customer_followup_cap_work_section"),
        sa.CheckConstraint("is_uncapped OR daily_cap IS NOT NULL", name="ck_customer_followup_cap_required"),
        sa.CheckConstraint("daily_cap IS NULL OR daily_cap > 0", name="ck_customer_followup_cap_positive"),
        sa.CheckConstraint("lead_source_type <> 'TD' OR is_uncapped", name="ck_customer_followup_cap_td_uncapped"),
        sa.CheckConstraint(
            "effective_until IS NULL OR effective_until >= effective_from",
            name="ck_customer_followup_cap_effective_range",
        ),
    )

    op.create_index(
        "ix_cfl_cost_center_mobile",
        "trx_customer_followup_leads",
        ["cost_center", "normalized_mobile_number"],
    )
    op.create_index("ix_cfl_cost_center_status", "trx_customer_followup_leads", ["cost_center", "lead_status"])
    op.create_index("ix_cfl_cost_center_source", "trx_customer_followup_leads", ["cost_center", "lead_source_type"])
    op.create_index("ix_cfl_next_followup", "trx_customer_followup_leads", ["cost_center", "next_followup_date"])
    op.create_index(
        "ix_cfl_source_record",
        "trx_customer_followup_leads",
        ["lead_source_type", "source_table_name", "source_record_id"],
    )
    op.create_index("ix_cfl_closed_recovered", "trx_customer_followup_leads", ["is_closed", "is_recovered"])
    _partial_unique_index(
        "uq_cfl_td_source_record",
        "trx_customer_followup_leads",
        ["source_system", "source_table_name", "source_record_id"],
        "lead_source_type = 'TD' AND source_record_id IS NOT NULL",
    )
    _partial_unique_index(
        "uq_cfl_external_source_record",
        "trx_customer_followup_leads",
        ["source_system", "source_table_name", "source_record_id"],
        "lead_source_type = 'EXTERNAL' AND source_record_id IS NOT NULL",
    )
    _partial_unique_index(
        "uq_cfl_retention_customer_bucket_run",
        "trx_customer_followup_leads",
        ["cost_center", "normalized_mobile_number", "lifecycle_bucket", "created_by_pipeline_run_id"],
        "lead_source_type = 'RETENTION' AND created_by_pipeline_run_id IS NOT NULL",
    )

    op.create_index(
        "ix_cfs_cost_center_mobile",
        "trx_customer_suppression",
        ["cost_center", "normalized_mobile_number"],
    )
    op.create_index("ix_cfs_until", "trx_customer_suppression", ["suppression_until"])
    op.create_index("ix_cfs_permanent", "trx_customer_suppression", ["is_permanent"])

    op.create_index(
        "ix_external_leads_cost_center_mobile",
        "trx_external_leads",
        ["cost_center", "normalized_mobile_number"],
    )
    op.create_index("ix_external_leads_status", "trx_external_leads", ["lead_status"])
    op.create_index("ix_external_leads_converted", "trx_external_leads", ["converted_to_followup_lead"])

    op.create_index(
        "ix_cfcc_scope_lookup",
        "customer_followup_cap_config",
        ["lead_source_type", "work_section", "enabled", "effective_from", "effective_until"],
    )
    _partial_unique_index(
        "uq_cfcc_global_scope_start",
        "customer_followup_cap_config",
        ["lead_source_type", "work_section", "effective_from"],
        "cost_center IS NULL",
    )
    _partial_unique_index(
        "uq_cfcc_store_scope_start",
        "customer_followup_cap_config",
        ["cost_center", "lead_source_type", "work_section", "effective_from"],
        "cost_center IS NOT NULL",
    )

    cap_config = sa.table(
        "customer_followup_cap_config",
        sa.column("cap_config_id"),
        sa.column("cost_center"),
        sa.column("lead_source_type"),
        sa.column("work_section"),
        sa.column("daily_cap"),
        sa.column("is_uncapped"),
        sa.column("enabled"),
        sa.column("effective_from"),
        sa.column("created_at"),
        sa.column("updated_at"),
    )
    seeded_at = datetime.now(timezone.utc)
    seed_values = {
        "cost_center": None,
        "lead_source_type": "RETENTION",
        "work_section": "FRESH_RETENTION",
        "daily_cap": 13,
        "is_uncapped": False,
        "enabled": True,
        "effective_from": date(2026, 1, 1),
        "created_at": seeded_at,
        "updated_at": seeded_at,
    }
    if op.get_bind().dialect.name == "sqlite":  # pragma: no cover - migration test compatibility
        seed_values["cap_config_id"] = 1
    op.get_bind().execute(cap_config.insert().values(**seed_values))


def downgrade() -> None:
    # Forward-only migration.
    return None
