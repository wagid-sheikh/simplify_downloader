"""Add UC zero-row export classification to orders sync logs."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0119_zero_row_export_class"
down_revision = "0118_uc_dashboard_host"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "orders_sync_log",
        sa.Column("zero_row_export_classification", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("orders_sync_log", "zero_row_export_classification")
