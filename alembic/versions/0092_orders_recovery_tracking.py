"""Add manual recovery tracking columns to orders."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0092_orders_recovery_tracking"
down_revision = "0091_backfill_td_pickup_at"
branch_labels = None
depends_on = None


_RECOVERY_STATUS_VALUES = (
    "NONE",
    "TO_BE_RECOVERED",
    "TO_BE_COMPENSATED",
    "RECOVERED",
    "COMPENSATED",
    "WRITE_OFF",
)

_RECOVERY_CATEGORY_VALUES = (
    "CRM_FORCED_PAID_90D",
    "DAMAGE_CLAIM",
    "CUSTOMER_DISPUTE",
    "OTHER",
)


def upgrade() -> None:
    with op.batch_alter_table("orders") as batch_op:
        batch_op.add_column(sa.Column("recovery_status", sa.String(length=32), nullable=True))
        batch_op.add_column(sa.Column("recovery_category", sa.String(length=32), nullable=True))
        batch_op.add_column(sa.Column("recovery_notes", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("recovery_marked_at", sa.DateTime(timezone=True), nullable=True))
        batch_op.add_column(sa.Column("recovery_marked_by", sa.BigInteger(), nullable=True))

        batch_op.create_check_constraint(
            "ck_orders_recovery_status",
            sa.text(
                "recovery_status IN ("
                + ", ".join(f"'{status}'" for status in _RECOVERY_STATUS_VALUES)
                + ")"
            ),
        )
        batch_op.create_check_constraint(
            "ck_orders_recovery_category",
            sa.text(
                "recovery_category IN ("
                + ", ".join(f"'{category}'" for category in _RECOVERY_CATEGORY_VALUES)
                + ")"
            ),
        )

    op.execute("UPDATE orders SET recovery_status = 'NONE' WHERE recovery_status IS NULL")

    with op.batch_alter_table("orders") as batch_op:
        batch_op.alter_column("recovery_status", server_default=sa.text("'NONE'"))


def downgrade() -> None:
    # Forward-only migration.
    return None
