"""Add lead assignment tables and flags"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0017_lead_assignment_tables"
down_revision = "0016_add_etl_headless_config"
branch_labels = None
depends_on = None


def _add_assign_leads_flag(inspector: sa.inspection.Inspector) -> None:
    if not inspector.has_table("store_master"):
        return

    existing_columns = {column["name"] for column in inspector.get_columns("store_master")}
    if "assign_leads" not in existing_columns:
        op.add_column(
            "store_master",
            sa.Column("assign_leads", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        )
    else:
        op.alter_column(
            "store_master",
            "assign_leads",
            existing_type=sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        )


def _create_agents_master(inspector: sa.inspection.Inspector) -> None:
    if inspector.has_table("agents_master"):
        return

    op.create_table(
        "agents_master",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("agent_code", sa.CHAR(length=4), nullable=True),
        sa.Column("agent_name", sa.String(length=32), nullable=False),
        sa.Column("mobile_number", sa.String(length=16), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.UniqueConstraint("agent_code", name="uq_agents_master_code"),
    )


def _create_store_lead_assignment_map(inspector: sa.inspection.Inspector) -> None:
    if inspector.has_table("store_lead_assignment_map"):
        return

    op.create_table(
        "store_lead_assignment_map",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("store_code", sa.String(), nullable=False),
        sa.Column("agent_id", sa.BigInteger(), sa.ForeignKey("agents_master.id"), nullable=False),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("priority", sa.Integer(), nullable=False, server_default=sa.text("100")),
        sa.Column("max_existing_per_lot", sa.Integer(), nullable=True),
        sa.Column("max_new_per_lot", sa.Integer(), nullable=True),
        sa.Column("max_daily_leads", sa.Integer(), nullable=True),
        sa.UniqueConstraint("store_code", "agent_id", name="uq_store_lead_assignment_map_store_agent"),
    )


def _create_lead_assignment_batches(inspector: sa.inspection.Inspector) -> None:
    if inspector.has_table("lead_assignment_batches"):
        return

    op.create_table(
        "lead_assignment_batches",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("batch_date", sa.Date(), nullable=False),
        sa.Column("triggered_by", sa.Text(), nullable=True),
        sa.Column("run_id", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )


def _create_lead_assignments(inspector: sa.inspection.Inspector) -> None:
    if inspector.has_table("lead_assignments"):
        return

    op.create_table(
        "lead_assignments",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("assignment_batch_id", sa.BigInteger(), sa.ForeignKey("lead_assignment_batches.id"), nullable=False),
        sa.Column("lead_id", sa.Integer(), sa.ForeignKey("missed_leads.pickup_row_id"), nullable=False),
        sa.Column("agent_id", sa.BigInteger(), sa.ForeignKey("agents_master.id"), nullable=False),
        sa.Column("page_group_code", sa.Text(), nullable=False),
        sa.Column("rowid", sa.Integer(), nullable=False),
        sa.Column("lead_assignment_code", sa.Text(), nullable=False),
        sa.Column("store_code", sa.String(), nullable=False),
        sa.Column("store_name", sa.String(), nullable=True),
        sa.Column("lead_date", sa.Date(), nullable=True),
        sa.Column("lead_type", sa.CHAR(length=1), nullable=True),
        sa.Column("mobile_number", sa.String(), nullable=False),
        sa.Column("cx_name", sa.String(), nullable=True),
        sa.Column("address", sa.String(), nullable=True),
        sa.Column("lead_source", sa.String(), nullable=True),
        sa.Column("assigned_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("lead_assignment_code", name="uq_lead_assignments_code"),
        sa.UniqueConstraint("lead_id", name="uq_lead_assignments_lead"),
    )


def _create_lead_assignment_outcomes(inspector: sa.inspection.Inspector) -> None:
    if inspector.has_table("lead_assignment_outcomes"):
        return

    op.create_table(
        "lead_assignment_outcomes",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("lead_assignment_id", sa.BigInteger(), sa.ForeignKey("lead_assignments.id"), nullable=False),
        sa.Column("converted_flag", sa.Boolean(), nullable=True),
        sa.Column("order_number", sa.String(), nullable=True),
        sa.Column("order_date", sa.Date(), nullable=True),
        sa.Column("order_value", sa.Numeric(12, 2), nullable=True),
        sa.Column("payment_mode", sa.String(), nullable=True),
        sa.Column("payment_amount", sa.Numeric(12, 2), nullable=True),
        sa.Column("remarks", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("lead_assignment_id", name="uq_lead_assignment_outcomes_lead_assignment_id"),
    )


def _add_lead_assigned_flag(inspector: sa.inspection.Inspector) -> None:
    if not inspector.has_table("missed_leads"):
        return

    existing_columns = {column["name"] for column in inspector.get_columns("missed_leads")}
    if "lead_assigned" not in existing_columns:
        op.add_column(
            "missed_leads",
            sa.Column("lead_assigned", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        )
    else:
        op.alter_column(
            "missed_leads",
            "lead_assigned",
            existing_type=sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        )


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    _add_assign_leads_flag(inspector)
    _create_agents_master(inspector)
    _create_store_lead_assignment_map(inspector)
    _create_lead_assignment_batches(inspector)
    _create_lead_assignments(inspector)
    _create_lead_assignment_outcomes(inspector)
    _add_lead_assigned_flag(inspector)


def downgrade() -> None:
    if op.get_bind():
        inspector = sa.inspect(op.get_bind())
    else:
        inspector = None

    if inspector and inspector.has_table("lead_assignment_outcomes"):
        op.drop_table("lead_assignment_outcomes")

    if inspector and inspector.has_table("lead_assignments"):
        op.drop_table("lead_assignments")

    if inspector and inspector.has_table("lead_assignment_batches"):
        op.drop_table("lead_assignment_batches")

    if inspector and inspector.has_table("store_lead_assignment_map"):
        op.drop_table("store_lead_assignment_map")

    if inspector and inspector.has_table("agents_master"):
        op.drop_table("agents_master")

    if inspector and inspector.has_table("missed_leads"):
        existing_columns = {column["name"] for column in inspector.get_columns("missed_leads")}
        if "lead_assigned" in existing_columns:
            op.drop_column("missed_leads", "lead_assigned")

    if inspector and inspector.has_table("store_master"):
        existing_columns = {column["name"] for column in inspector.get_columns("store_master")}
        if "assign_leads" in existing_columns:
            op.drop_column("store_master", "assign_leads")
