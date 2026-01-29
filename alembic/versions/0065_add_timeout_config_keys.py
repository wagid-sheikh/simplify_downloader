"""Add ETL and PDF render timeout configs to system_config."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0065_add_timeout_config_keys"
down_revision = "0064_clear_overlap_sales_edits"
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
        key="ETL_STEP_TIMEOUT_SECONDS",
        value="600",
        description=(
            "Timeout (seconds) for ETL steps; set high enough to accommodate "
            "DASHBOARD_DOWNLOAD_NAV_TIMEOUT (ms) plus retries."
        ),
    )
    _upsert_config(
        bind,
        system_config,
        key="PDF_RENDER_TIMEOUT_SECONDS",
        value="120",
        description=(
            "Timeout (seconds) for PDF rendering; set high enough to accommodate "
            "DASHBOARD_DOWNLOAD_NAV_TIMEOUT (ms) plus retries."
        ),
    )


def downgrade() -> None:
    bind = op.get_bind()
    meta = sa.MetaData()
    meta.reflect(bind=bind, only=["system_config"])
    system_config = sa.Table("system_config", meta, autoload_with=bind)

    bind.execute(
        system_config.delete().where(system_config.c.key == "ETL_STEP_TIMEOUT_SECONDS")
    )
    bind.execute(
        system_config.delete().where(system_config.c.key == "PDF_RENDER_TIMEOUT_SECONDS")
    )
