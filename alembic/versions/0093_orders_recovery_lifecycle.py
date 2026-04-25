"""Add recovery lifecycle date fields to orders."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0093_orders_recovery_lifecycle"
down_revision = "0092_orders_recovery_tracking"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("orders") as batch_op:
        batch_op.add_column(sa.Column("recovery_opened_at", sa.DateTime(timezone=True), nullable=True))
        batch_op.add_column(sa.Column("recovery_closed_at", sa.DateTime(timezone=True), nullable=True))
        batch_op.add_column(sa.Column("recovery_expected_resolution_date", sa.Date(), nullable=True))


def downgrade() -> None:
    # Forward-only migration.
    return None
