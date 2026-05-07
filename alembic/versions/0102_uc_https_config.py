"""Add UC HTTPS error handling config."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0102_uc_https_config"
down_revision = "0101_missing_pay_source_amount"
branch_labels = None
depends_on = None


CONFIG_KEY = "UC_IGNORE_HTTPS_ERRORS"


def _upsert_config(
    bind, system_config: sa.Table, *, key: str, value: str, description: str
) -> None:
    existing = bind.execute(
        sa.select(system_config.c.id).where(system_config.c.key == key)
    ).scalar()
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
        key=CONFIG_KEY,
        value="false",
        description=(
            "If true, UC Playwright contexts ignore HTTPS certificate errors; "
            "keep false except during emergency certificate incidents."
        ),
    )


def downgrade() -> None:
    bind = op.get_bind()
    meta = sa.MetaData()
    meta.reflect(bind=bind, only=["system_config"])
    system_config = sa.Table("system_config", meta, autoload_with=bind)

    bind.execute(system_config.delete().where(system_config.c.key == CONFIG_KEY))
