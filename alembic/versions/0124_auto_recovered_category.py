"""Allow payment-proof auto recovery category."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0124_auto_recovered_category"
down_revision = "0123_oli_rebuild_progress"
branch_labels = None
depends_on = None


_RECOVERY_CATEGORY_VALUES = (
    "CRM_FORCED_PAID_90D",
    "DAMAGE_CLAIM",
    "CUSTOMER_DISPUTE",
    "OTHER",
    "WRITE_OFF_FULL",
    "WRITE_OFF_BALANCE",
    "RETURNED",
    "PAYMENT_PROOF_AUTO_RECOVERED",
)


def _category_check_sql() -> sa.TextClause:
    return sa.text(
        "recovery_category IN ("
        + ", ".join(f"'{category}'" for category in _RECOVERY_CATEGORY_VALUES)
        + ")"
    )


def upgrade() -> None:
    with op.batch_alter_table("orders") as batch_op:
        batch_op.drop_constraint("ck_orders_recovery_category", type_="check")
        batch_op.create_check_constraint(
            "ck_orders_recovery_category",
            _category_check_sql(),
        )


def downgrade() -> None:
    # Forward-only migration.
    return None
