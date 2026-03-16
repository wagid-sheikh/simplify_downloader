"""Default SKIP_UC_Pending_Delivery to false."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0082_uc_pending_default_false"
down_revision = "0081_td_uc_template_contract_cleanup"
branch_labels = None
depends_on = None


def _upsert_config(bind, system_config: sa.Table, *, key: str, value: str, description: str) -> None:
    existing = bind.execute(sa.select(system_config.c.id).where(system_config.c.key == key)).scalar()
    params = {"key": key, "value": value, "description": description}
    if existing:
        bind.execute(
            system_config.update()
            .where(system_config.c.id == existing)
            .values(**params, is_active=True, updated_at=sa.func.now())
        )
    else:
        bind.execute(system_config.insert().values(**params, is_active=True))


def upgrade() -> None:
    bind = op.get_bind()
    meta = sa.MetaData()
    meta.reflect(bind=bind, only=["system_config"])
    system_config = sa.Table("system_config", meta, autoload_with=bind)

    _upsert_config(
        bind,
        system_config,
        key="SKIP_UC_Pending_Delivery",
        value="false",
        description="Skip UC orders in pending deliveries report.",
    )


def downgrade() -> None:
    bind = op.get_bind()
    meta = sa.MetaData()
    meta.reflect(bind=bind, only=["system_config"])
    system_config = sa.Table("system_config", meta, autoload_with=bind)

    _upsert_config(
        bind,
        system_config,
        key="SKIP_UC_Pending_Delivery",
        value="true",
        description="Skip UC orders in pending deliveries report.",
    )
