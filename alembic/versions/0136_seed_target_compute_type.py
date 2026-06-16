"""Seed target compute type config."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0136_seed_target_compute_type"
down_revision = "0135_cfl_owner_recipient"
branch_labels = None
depends_on = None

CONFIG_KEY = "TARGET_COMPUTE_TYPE"
DEFAULT_VALUE = "SALES"
DESCRIPTION = "Controls report target computation mode: SALES or COLLECTIONS."


def upgrade() -> None:
    bind = op.get_bind()
    meta = sa.MetaData()
    meta.reflect(bind=bind, only=["system_config"])
    system_config = sa.Table("system_config", meta, autoload_with=bind)

    existing = bind.execute(
        sa.select(system_config.c.id).where(system_config.c.key == CONFIG_KEY)
    ).scalar_one_or_none()
    values = {
        "key": CONFIG_KEY,
        "value": DEFAULT_VALUE,
        "description": DESCRIPTION,
        "is_active": True,
    }
    if existing is None:
        bind.execute(system_config.insert().values(**values))
    else:
        bind.execute(
            system_config.update()
            .where(system_config.c.id == existing)
            .values(value=DEFAULT_VALUE, description=DESCRIPTION, is_active=True)
        )


def downgrade() -> None:
    bind = op.get_bind()
    meta = sa.MetaData()
    meta.reflect(bind=bind, only=["system_config"])
    system_config = sa.Table("system_config", meta, autoload_with=bind)
    bind.execute(system_config.delete().where(system_config.c.key == CONFIG_KEY))
