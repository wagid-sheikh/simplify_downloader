"""Deactivate SKIP_UC_Pending_Delivery config key."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0095_drop_skip_uc_pending"
down_revision = "0094_td_leads_customer_type"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    meta = sa.MetaData()
    meta.reflect(bind=bind, only=["system_config"])
    system_config = sa.Table("system_config", meta, autoload_with=bind)

    bind.execute(
        system_config.update()
        .where(system_config.c.key == "SKIP_UC_Pending_Delivery")
        .values(is_active=False, updated_at=sa.func.now())
    )


def downgrade() -> None:
    bind = op.get_bind()
    meta = sa.MetaData()
    meta.reflect(bind=bind, only=["system_config"])
    system_config = sa.Table("system_config", meta, autoload_with=bind)

    existing = bind.execute(
        sa.select(system_config.c.id).where(system_config.c.key == "SKIP_UC_Pending_Delivery")
    ).scalar()

    values = {
        "key": "SKIP_UC_Pending_Delivery",
        "value": "false",
        "description": "Skip UC orders in pending deliveries report.",
        "is_active": True,
    }
    if existing:
        bind.execute(
            system_config.update()
            .where(system_config.c.id == existing)
            .values(**values, updated_at=sa.func.now())
        )
    else:
        bind.execute(system_config.insert().values(**values))
