"""Require retention followup lead idempotency identity fields."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0131_cfl_retention_identity"
down_revision = "0130_cfcc_no_overlap"
branch_labels = None
depends_on = None

_RETENTION_IDENTITY_CHECK = (
    "lead_source_type <> 'RETENTION' OR "
    "(lifecycle_bucket IS NOT NULL "
    "AND created_by_pipeline_run_id IS NOT NULL)"
)


def upgrade() -> None:
    with op.batch_alter_table("trx_customer_followup_leads") as batch_op:
        batch_op.create_check_constraint(
            "ck_cfl_retention_identity_required",
            sa.text(_RETENTION_IDENTITY_CHECK),
        )


def downgrade() -> None:
    # Forward-only migration.
    return None
