"""Require stable source identity for TD and external followup leads."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0129_cfl_source_identity"
down_revision = "0128_customer_retention_p1"
branch_labels = None
depends_on = None

_SOURCE_IDENTITY_CHECK = (
    "lead_source_type NOT IN ('TD', 'EXTERNAL') OR "
    "(source_system IS NOT NULL "
    "AND source_table_name IS NOT NULL "
    "AND source_record_id IS NOT NULL)"
)


def upgrade() -> None:
    with op.batch_alter_table("trx_customer_followup_leads") as batch_op:
        batch_op.create_check_constraint(
            "ck_cfl_source_identity_required",
            sa.text(_SOURCE_IDENTITY_CHECK),
        )


def downgrade() -> None:
    # Forward-only migration.
    return None
