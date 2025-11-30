"""Add timestamps to missed_leads (compatibility placeholder)"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0022_add_ts_to_missed_leads"
down_revision = "0021_add_ts_ingest_missed_leads"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {col["name"] for col in inspector.get_columns("missed_leads")}

    if "created_at" not in columns:
        op.add_column(
            "missed_leads",
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                server_default=sa.text("CURRENT_TIMESTAMP"),
                nullable=False,
            ),
        )

    if "updated_at" not in columns:
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
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {col["name"] for col in inspector.get_columns("missed_leads")}

    if "updated_at" in columns:
        op.drop_column("missed_leads", "updated_at")

    if "created_at" in columns:
        op.drop_column("missed_leads", "created_at")
