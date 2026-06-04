"""Drop legacy store launch date column."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0125_drop_launch_date"
down_revision = "0124_auto_recovered_category"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        sa.text(
            """
            UPDATE store_master
            SET start_date = launch_date
            WHERE start_date IS NULL
              AND launch_date IS NOT NULL
            """
        )
    )
    op.drop_column("store_master", "launch_date")


def downgrade() -> None:
    op.add_column("store_master", sa.Column("launch_date", sa.Date(), nullable=True))
    op.execute(
        sa.text(
            """
            UPDATE store_master
            SET launch_date = start_date
            WHERE start_date IS NOT NULL
            """
        )
    )
