"""Add TD compare threshold verdict tracking columns."""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0073_add_td_compare_threshold"
down_revision = "0072_add_td_sync_compare_log"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        json_type = postgresql.JSONB(astext_type=sa.Text())
    else:  # pragma: no cover - sqlite tests
        json_type = sa.JSON()

    op.add_column("td_sync_compare_log", sa.Column("thresholds_json", json_type, nullable=True))
    op.add_column("td_sync_compare_log", sa.Column("threshold_verdict_json", json_type, nullable=True))
    op.add_column("td_sync_compare_log", sa.Column("consecutive_pass_windows", sa.Integer(), nullable=True))
    op.add_column("td_sync_compare_log", sa.Column("api_ready", sa.Boolean(), nullable=True))


def downgrade() -> None:
    op.drop_column("td_sync_compare_log", "api_ready")
    op.drop_column("td_sync_compare_log", "consecutive_pass_windows")
    op.drop_column("td_sync_compare_log", "threshold_verdict_json")
    op.drop_column("td_sync_compare_log", "thresholds_json")
