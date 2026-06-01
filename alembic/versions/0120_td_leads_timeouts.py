"""Add bounded TD leads runtime timeout configuration."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0120_td_leads_timeouts"
down_revision = "0119_zero_row_export_class"
branch_labels = None
depends_on = None


_TIMEOUT_CONFIGS = (
    (
        "TD_LEADS_BROWSER_OPERATION_TIMEOUT_SECONDS",
        "90",
        "Deadline in seconds for an ordinary TD leads browser operation.",
    ),
    (
        "TD_LEADS_BROWSER_CLEANUP_TIMEOUT_SECONDS",
        "10",
        "Deadline in seconds for TD leads browser and context cleanup.",
    ),
    (
        "TD_LEADS_STORE_WORKER_TIMEOUT_SECONDS",
        "240",
        "Deadline in seconds for one TD leads store worker.",
    ),
    (
        "TD_LEADS_GATHER_TIMEOUT_SECONDS",
        "270",
        "Deadline in seconds for the complete TD leads worker collection.",
    ),
)


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
    for key, value, description in _TIMEOUT_CONFIGS:
        _upsert_config(
            bind, system_config, key=key, value=value, description=description
        )


def downgrade() -> None:
    bind = op.get_bind()
    meta = sa.MetaData()
    meta.reflect(bind=bind, only=["system_config"])
    system_config = sa.Table("system_config", meta, autoload_with=bind)
    bind.execute(
        system_config.delete().where(
            system_config.c.key.in_([key for key, _, _ in _TIMEOUT_CONFIGS])
        )
    )
