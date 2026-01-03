"""Add ingest_remarks to stg_td_orders"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0028_ingest_remarks_stgtdorders"
down_revision = "0027_store_master_sync_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("stg_td_orders", sa.Column("ingest_remarks", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("stg_td_orders", "ingest_remarks")
