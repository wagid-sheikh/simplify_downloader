"""Create pipeline_run_summaries table."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "0005_pipeline_run_summaries"
down_revision = "0004_store_dashboard"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "pipeline_run_summaries",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("pipeline_name", sa.String(length=100), nullable=False),
        sa.Column("run_id", sa.String(length=64), nullable=False, unique=True),
        sa.Column("run_env", sa.String(length=32), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("total_time_taken", sa.String(length=8), nullable=False),
        sa.CheckConstraint(
            "total_time_taken ~ '^[0-9]{2}:[0-9]{2}:[0-9]{2}$'",
            name="ck_pipeline_run_summaries_total_time_format",
        ),
        sa.Column("report_date", sa.Date(), nullable=True),
        sa.Column("overall_status", sa.String(length=32), nullable=False),
        sa.Column("summary_text", sa.Text(), nullable=False),
        sa.Column("phases_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("metrics_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )


def downgrade() -> None:
    op.drop_table("pipeline_run_summaries")
