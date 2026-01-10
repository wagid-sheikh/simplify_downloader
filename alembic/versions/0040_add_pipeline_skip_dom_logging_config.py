"""Add pipeline skip DOM logging flag to system_config"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0040_add_pipeline_skip_dom_logging_config"
down_revision = "0039_uc_orders_pipe_notitempl"
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
        key="pipeline_skip_dom_logging",
        value="false",
        description="If true, skip logging HTML DOM elements in all pipelines",
    )


def downgrade() -> None:
    bind = op.get_bind()
    meta = sa.MetaData()
    meta.reflect(bind=bind, only=["system_config"])
    system_config = sa.Table("system_config", meta, autoload_with=bind)

    bind.execute(
        system_config.delete().where(system_config.c.key == "pipeline_skip_dom_logging")
    )
