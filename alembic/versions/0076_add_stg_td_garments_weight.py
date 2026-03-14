"""Add weight column to stg_td_garments."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0076_add_stg_td_garments_weight"
down_revision = "0075_add_oli_weight"
branch_labels = None
depends_on = None


def _has_column(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return any(column["name"] == column_name for column in inspector.get_columns(table_name))


def upgrade() -> None:
    if not _has_column("stg_td_garments", "weight"):
        with op.batch_alter_table("stg_td_garments") as batch_op:
            batch_op.add_column(sa.Column("weight", sa.Numeric(12, 3), nullable=True))


def downgrade() -> None:
    if _has_column("stg_td_garments", "weight"):
        with op.batch_alter_table("stg_td_garments") as batch_op:
            batch_op.drop_column("weight")
