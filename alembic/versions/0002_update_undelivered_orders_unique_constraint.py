"""Update unique constraint on undelivered_orders"""
from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "0002_undelivered_uc"
down_revision = "0001_init"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("uq_undelivered_order_id", "undelivered_orders", type_="unique")
    op.create_unique_constraint(
        "uq_undelivered_orders_store_code_order_id",
        "undelivered_orders",
        ["store_code", "order_id"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_undelivered_orders_store_code_order_id",
        "undelivered_orders",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_undelivered_order_id",
        "undelivered_orders",
        ["order_id"],
    )
