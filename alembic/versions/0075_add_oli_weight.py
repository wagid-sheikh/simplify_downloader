"""Add weight column to order_line_items."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0075_add_oli_weight"
down_revision = "0074_relax_td_garmentlineitemuid"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("order_line_items") as batch_op:
        batch_op.add_column(sa.Column("weight", sa.Numeric(12, 3), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("order_line_items") as batch_op:
        batch_op.drop_column("weight")
