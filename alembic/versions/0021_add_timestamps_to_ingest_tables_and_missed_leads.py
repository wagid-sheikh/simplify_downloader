"""Add timestamps to ingest tables including missed_leads"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0021_add_ts_ingest_missed_leads"
down_revision = "0020_dashboard_nav_timeout"
branch_labels = None
depends_on = None


def _add_timestamps(table_name: str) -> None:
    op.add_column(
        table_name,
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
    )
    op.add_column(
        table_name,
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
    )


def _drop_timestamps(table_name: str) -> None:
    op.drop_column(table_name, "updated_at")
    op.drop_column(table_name, "created_at")


def upgrade() -> None:
    _add_timestamps("undelivered_orders")
    _add_timestamps("repeat_customers")
    _add_timestamps("nonpackage_orders")
    _add_timestamps("missed_leads")


def downgrade() -> None:
    _drop_timestamps("missed_leads")
    _drop_timestamps("nonpackage_orders")
    _drop_timestamps("repeat_customers")
    _drop_timestamps("undelivered_orders")
