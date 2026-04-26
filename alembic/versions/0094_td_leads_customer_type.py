"""Add customer_type to crm_leads_current."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0094_td_leads_customer_type"
down_revision = "0093_orders_recovery_lifecycle"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not inspector.has_table("crm_leads_current"):
        return

    existing_columns = {column["name"] for column in inspector.get_columns("crm_leads_current")}
    if "customer_type" in existing_columns:
        return

    op.add_column("crm_leads_current", sa.Column("customer_type", sa.String(length=64), nullable=True))


def downgrade() -> None:
    # Forward-only migration.
    return None
