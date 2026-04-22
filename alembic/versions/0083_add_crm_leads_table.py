"""Create crm_leads table for TD Pickup Scheduler ingestion."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0083_add_crm_leads_table"
down_revision = "0082_uc_pending_default_false"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "crm_leads",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("lead_uid", sa.String(length=128), nullable=False),
        sa.Column("store_code", sa.String(length=8), nullable=False),
        sa.Column("status_bucket", sa.String(length=16), nullable=False),
        sa.Column("pickup_id", sa.String(length=64), nullable=True),
        sa.Column("pickup_no", sa.String(length=64), nullable=True),
        sa.Column("customer_name", sa.String(length=256), nullable=True),
        sa.Column("address", sa.Text(), nullable=True),
        sa.Column("mobile", sa.String(length=32), nullable=True),
        sa.Column("pickup_created_date", sa.String(length=64), nullable=True),
        sa.Column("pickup_time", sa.String(length=64), nullable=True),
        sa.Column("special_instruction", sa.Text(), nullable=True),
        sa.Column("status_text", sa.String(length=64), nullable=True),
        sa.Column("reason", sa.String(length=128), nullable=True),
        sa.Column("source", sa.String(length=128), nullable=True),
        sa.Column("user_name", sa.String(length=128), nullable=True),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("run_env", sa.String(length=32), nullable=False),
        sa.Column("source_file", sa.Text(), nullable=True),
        sa.Column("scraped_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("lead_uid", name="uq_crm_leads_uid"),
    )
    op.create_index("ix_crm_leads_store_status", "crm_leads", ["store_code", "status_bucket"])
    op.create_index("ix_crm_leads_run_id", "crm_leads", ["run_id"])


def downgrade() -> None:
    op.drop_index("ix_crm_leads_run_id", table_name="crm_leads")
    op.drop_index("ix_crm_leads_store_status", table_name="crm_leads")
    op.drop_table("crm_leads")
