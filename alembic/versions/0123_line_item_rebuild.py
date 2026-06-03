"""Add line-item rebuild progress table."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0123_line_item_rebuild"
down_revision = "0122_uc_line_snapshots"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "order_line_item_rebuild_progress",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("source_system", sa.String(length=8), nullable=False),
        sa.Column("store_code", sa.String(length=16), nullable=False),
        sa.Column("window_from_date", sa.Date(), nullable=False),
        sa.Column("window_to_date", sa.Date(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("run_id", sa.Text(), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("metrics", sa.JSON(), nullable=True),
    )
    op.create_index(
        "ix_oli_rebuild_progress_scope",
        "order_line_item_rebuild_progress",
        ["source_system", "store_code", "window_from_date", "window_to_date", "status"],
    )


def downgrade() -> None:
    op.drop_index("ix_oli_rebuild_progress_scope", table_name="order_line_item_rebuild_progress")
    op.drop_table("order_line_item_rebuild_progress")
