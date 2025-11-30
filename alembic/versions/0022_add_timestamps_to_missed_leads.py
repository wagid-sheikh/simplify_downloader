"""Add timestamps to missed_leads"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0022_add_timestamps_to_missed_leads"
down_revision = "0021_add_timestamp_ingest_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "missed_leads",
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
    )
    op.add_column(
        "missed_leads",
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
    )


def downgrade() -> None:
    op.drop_column("missed_leads", "updated_at")
    op.drop_column("missed_leads", "created_at")
