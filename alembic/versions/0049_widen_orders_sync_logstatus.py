"""Widen orders_sync_log status column."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0049_widen_orders_sync_logstatus"
down_revision = "0048_uc_success_with_warnings"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "orders_sync_log",
        "status",
        existing_type=sa.String(length=16),
        type_=sa.String(length=64),
        existing_nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "orders_sync_log",
        "status",
        existing_type=sa.String(length=64),
        type_=sa.String(length=16),
        existing_nullable=False,
    )
