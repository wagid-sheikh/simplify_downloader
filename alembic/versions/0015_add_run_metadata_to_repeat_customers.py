"""Add run metadata to repeat_customers table"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0015_run_metadata_repeat_cust"
down_revision = "0014_add_run_metadata_columns"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("repeat_customers", sa.Column("run_id", sa.String(length=64), nullable=True))
    op.add_column("repeat_customers", sa.Column("run_date", sa.Date(), nullable=True))


def downgrade() -> None:
    op.drop_column("repeat_customers", "run_date")
    op.drop_column("repeat_customers", "run_id")
