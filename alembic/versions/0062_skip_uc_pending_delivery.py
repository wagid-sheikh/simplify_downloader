"""Add skip UC pending delivery flag to system_config."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0062_skip_uc_pending_delivery"
down_revision = "0061_recompute_salesedited_order"
branch_labels = None
depends_on = None


def _system_config_table() -> sa.Table:
    return sa.table(
        "system_config",
        sa.column("key", sa.String()),
        sa.column("value", sa.String()),
        sa.column("description", sa.String()),
        sa.column("is_active", sa.Boolean()),
    )


def upgrade() -> None:
    system_config = _system_config_table()
    op.execute(
        system_config.insert().values(
            key="SKIP_UC_Pending_Delivery",
            value="true",
            description="Skip UC orders in pending deliveries report.",
            is_active=True,
        )
    )


def downgrade() -> None:
    system_config = _system_config_table()
    op.execute(system_config.delete().where(system_config.c.key == "SKIP_UC_Pending_Delivery"))
