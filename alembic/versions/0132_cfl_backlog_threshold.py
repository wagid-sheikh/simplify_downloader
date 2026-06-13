"""Add customer followup backlog threshold config."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0132_cfl_backlog_threshold"
down_revision = "0131_cfl_retention_identity"
branch_labels = None
depends_on = None

CONFIG_KEY = "CUSTOMER_FOLLOWUP_BACKLOG_WARNING_THRESHOLD"
DEFAULT_VALUE = "20"
DESCRIPTION = "Rolling 14-day RETENTION backlog threshold before fresh retention workbook rows are frozen."


def upgrade() -> None:
    bind = op.get_bind()
    meta = sa.MetaData()
    meta.reflect(bind=bind, only=["system_config"])
    system_config = sa.Table("system_config", meta, autoload_with=bind)
    existing = bind.execute(sa.select(system_config.c.id).where(system_config.c.key == CONFIG_KEY)).scalar()
    values = {"key": CONFIG_KEY, "value": DEFAULT_VALUE, "description": DESCRIPTION, "is_active": True}
    if existing is None:
        bind.execute(system_config.insert().values(**values))
    else:
        bind.execute(
            system_config.update()
            .where(system_config.c.id == existing)
            .values(value=DEFAULT_VALUE, description=DESCRIPTION, is_active=True)
        )


def downgrade() -> None:
    # Forward-only migration.
    return None
