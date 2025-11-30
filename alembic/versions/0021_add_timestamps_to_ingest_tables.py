"""Add timestamps to ingest tables"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0021_add_timestamps_to_ingest_tables"
down_revision = "0020_add_dashboard_nav_timeout_config"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "undelivered_orders",
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
    )
    op.add_column(
        "undelivered_orders",
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
    )

    op.add_column(
        "repeat_customers",
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
    )
    op.add_column(
        "repeat_customers",
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
    )

    op.add_column(
        "nonpackage_orders",
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
    )
    op.add_column(
        "nonpackage_orders",
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
    )


def downgrade() -> None:
    op.drop_column("nonpackage_orders", "updated_at")
    op.drop_column("nonpackage_orders", "created_at")

    op.drop_column("repeat_customers", "updated_at")
    op.drop_column("repeat_customers", "created_at")

    op.drop_column("undelivered_orders", "updated_at")
    op.drop_column("undelivered_orders", "created_at")
