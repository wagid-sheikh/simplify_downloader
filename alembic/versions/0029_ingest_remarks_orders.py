"""Add ingest_remarks to orders"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0029_ingest_remarks_orders"
down_revision = "0028_ingest_remarks_stgtdorders"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("orders", sa.Column("ingest_remarks", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("orders", "ingest_remarks")
