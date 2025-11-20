"""Add ETL/report flags to store_master and backfill"""

from __future__ import annotations

import re

from alembic import op
import sqlalchemy as sa


revision = "0012_add_store_flags"
down_revision = "0011_upd_store_dailyreport_templ"
branch_labels = None
depends_on = None


def _parse_codes(raw: str | None) -> list[str]:
    if not raw:
        return []
    tokens = re.split(r"[,\n]", raw)
    return [token.strip().upper() for token in tokens if token and token.strip()]


def upgrade() -> None:
    op.add_column(
        "store_master",
        sa.Column("etl_flag", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )
    op.add_column(
        "store_master",
        sa.Column("report_flag", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )

    bind = op.get_bind()
    meta = sa.MetaData(bind=bind)
    system_config = sa.Table("system_config", meta, autoload_with=bind)
    store_master = sa.Table("store_master", meta, autoload_with=bind)

    result = bind.execute(
        sa.select(system_config.c.key, system_config.c.value).where(system_config.c.is_active == sa.true())
    )
    config_values = {row.key: row.value for row in result}

    stores = set(_parse_codes(config_values.get("STORES_LIST")))
    report_stores = set(_parse_codes(config_values.get("REPORT_STORES_LIST")))

    if stores:
        bind.execute(
            store_master.update()
            .where(sa.func.upper(store_master.c.store_code).in_(stores))
            .values(etl_flag=True)
        )

    if report_stores:
        bind.execute(
            store_master.update()
            .where(sa.func.upper(store_master.c.store_code).in_(report_stores))
            .values(etl_flag=True, report_flag=True)
        )

    op.alter_column(
        "store_master", "etl_flag", existing_type=sa.Boolean(), nullable=False, server_default=sa.text("false")
    )
    op.alter_column(
        "store_master", "report_flag", existing_type=sa.Boolean(), nullable=False, server_default=sa.text("false")
    )


def downgrade() -> None:
    op.drop_column("store_master", "report_flag")
    op.drop_column("store_master", "etl_flag")
