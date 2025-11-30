"""Add dashboard navigation timeout to system_config."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0020_dashboard_nav_timeout"
down_revision = "0019_leads_assignment_sum_templ"
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
        key="DASHBOARD_DOWNLOAD_NAV_TIMEOUT",
        value="90000",
        description="Timeout (ms) for navigating to TMS dashboards",
    )


def downgrade() -> None:
    bind = op.get_bind()
    meta = sa.MetaData()
    meta.reflect(bind=bind, only=["system_config"])
    system_config = sa.Table("system_config", meta, autoload_with=bind)

    bind.execute(system_config.delete().where(system_config.c.key == "DASHBOARD_DOWNLOAD_NAV_TIMEOUT"))
