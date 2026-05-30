"""Move UC store dashboard home URLs to the current host."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0118_uc_dashboard_host"
down_revision = "0117_normalize_vw_recovery"
branch_labels = None
depends_on = None


DEPRECATED_DASHBOARD_URL = "https://store.ucleanlaundry.com/dashboard"
CURRENT_DASHBOARD_URL = "https://storepanel.ucleanlaundry.com/dashboard"

POSTGRES_UPDATE_SQL = """
UPDATE store_master
SET sync_config = jsonb_set(
    sync_config,
    '{urls,home}',
    to_jsonb(CAST(:new_dashboard_url AS text)),
    false
)
WHERE UPPER(sync_group) = 'UC'
  AND sync_config -> 'urls' ->> 'home' = :old_dashboard_url
"""

SQLITE_UPDATE_SQL = """
UPDATE store_master
SET sync_config = json_set(sync_config, '$.urls.home', :new_dashboard_url)
WHERE UPPER(sync_group) = 'UC'
  AND json_extract(sync_config, '$.urls.home') = :old_dashboard_url
"""


def _update_dashboard_url(*, old_dashboard_url: str, new_dashboard_url: str) -> None:
    """Update only the nested home URL while preserving the remaining JSON payload."""

    bind = op.get_bind()
    sql = POSTGRES_UPDATE_SQL if bind.dialect.name == "postgresql" else SQLITE_UPDATE_SQL
    bind.execute(
        sa.text(sql),
        {
            "old_dashboard_url": old_dashboard_url,
            "new_dashboard_url": new_dashboard_url,
        },
    )


def upgrade() -> None:
    _update_dashboard_url(
        old_dashboard_url=DEPRECATED_DASHBOARD_URL,
        new_dashboard_url=CURRENT_DASHBOARD_URL,
    )


def downgrade() -> None:
    _update_dashboard_url(
        old_dashboard_url=CURRENT_DASHBOARD_URL,
        new_dashboard_url=DEPRECATED_DASHBOARD_URL,
    )
