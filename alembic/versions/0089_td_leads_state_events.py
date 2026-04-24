"""Replace crm_leads with current-state and status-event tables."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0089_td_leads_state_events"
down_revision = "0088_backfill_crmleadscreated_at"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if inspector.has_table("crm_leads"):
        op.drop_table("crm_leads")

    op.create_table(
        "crm_leads_current",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("lead_uid", sa.String(length=128), nullable=False),
        sa.Column("store_code", sa.String(length=8), nullable=False),
        sa.Column("pickup_no", sa.String(length=64), nullable=False),
        sa.Column("status_bucket", sa.String(length=16), nullable=False),
        sa.Column("customer_name", sa.String(length=256), nullable=True),
        sa.Column("address", sa.Text(), nullable=True),
        sa.Column("mobile", sa.String(length=32), nullable=True),
        sa.Column("pickup_date", sa.String(length=64), nullable=True),
        sa.Column("pickup_time", sa.String(length=64), nullable=True),
        sa.Column("pickup_created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("special_instruction", sa.Text(), nullable=True),
        sa.Column("source", sa.String(length=128), nullable=True),
        sa.Column("reason", sa.String(length=128), nullable=True),
        sa.Column("cancelled_flag", sa.String(length=16), nullable=True),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("run_env", sa.String(length=32), nullable=False),
        sa.Column("source_file", sa.Text(), nullable=True),
        sa.Column("scraped_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("lead_uid", name="uq_crm_leads_uid"),
    )
    op.create_index("ix_crm_leads_current_store_status", "crm_leads_current", ["store_code", "status_bucket"])
    op.create_index("ix_crm_leads_current_run_id", "crm_leads_current", ["run_id"])

    op.create_table(
        "crm_leads_status_events",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("lead_uid", sa.String(length=128), nullable=False),
        sa.Column("store_code", sa.String(length=8), nullable=False),
        sa.Column("pickup_no", sa.String(length=64), nullable=False),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column("previous_status_bucket", sa.String(length=16), nullable=True),
        sa.Column("status_bucket", sa.String(length=16), nullable=False),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("run_env", sa.String(length=32), nullable=False),
        sa.Column("source_file", sa.Text(), nullable=True),
        sa.Column("scraped_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_crm_lead_events_lead_uid", "crm_leads_status_events", ["lead_uid"])
    op.create_index("ix_crm_lead_events_run_id", "crm_leads_status_events", ["run_id"])


def downgrade() -> None:
    # Forward-only migration.
    return None
