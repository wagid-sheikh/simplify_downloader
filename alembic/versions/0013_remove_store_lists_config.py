"""Remove store list system_config entries in favor of store_master flags"""

from __future__ import annotations

import re

from alembic import op
import sqlalchemy as sa


revision = "0013_remove_store_lists_config"
down_revision = "0012_add_store_flags"
branch_labels = None
depends_on = None


_CONFIG_KEYS = {"STORES_LIST", "REPORT_STORES_LIST"}


def _parse_codes(raw: str | None) -> list[str]:
    if not raw:
        return []
    tokens = re.split(r"[,\n]", raw)
    return [token.strip().upper() for token in tokens if token and token.strip()]


def _upsert_config(bind, system_config: sa.Table, *, key: str, value: str, description: str) -> None:
    existing = bind.execute(sa.select(system_config.c.id).where(system_config.c.key == key)).scalar()
    params = {"value": value, "description": description, "is_active": True}
    if existing:
        bind.execute(
            system_config.update()
            .where(system_config.c.id == existing)
            .values(**params, updated_at=sa.func.now())
        )
    else:
        bind.execute(system_config.insert().values(key=key, **params))


def upgrade() -> None:
    bind = op.get_bind()
    meta = sa.MetaData()
    system_config = sa.Table("system_config", meta, autoload_with=bind)
    store_master = sa.Table("store_master", meta, autoload_with=bind)

    result = bind.execute(
        sa.select(system_config.c.key, system_config.c.value).where(system_config.c.key.in_(_CONFIG_KEYS))
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

    bind.execute(system_config.delete().where(system_config.c.key.in_(_CONFIG_KEYS)))


def downgrade() -> None:
    bind = op.get_bind()
    meta = sa.MetaData()
    system_config = sa.Table("system_config", meta, autoload_with=bind)
    store_master = sa.Table("store_master", meta, autoload_with=bind)

    etl_codes = {
        code
        for code, in bind.execute(
            sa.select(sa.func.upper(store_master.c.store_code)).where(store_master.c.etl_flag.is_(True))
        )
        if code
    }
    report_codes = {
        code
        for code, in bind.execute(
            sa.select(sa.func.upper(store_master.c.store_code)).where(store_master.c.report_flag.is_(True))
        )
        if code
    }

    stores_csv = ",".join(sorted(etl_codes))
    report_csv = ",".join(sorted(report_codes))

    _upsert_config(
        bind,
        system_config,
        key="STORES_LIST",
        value=stores_csv,
        description="Default scraping store codes",
    )
    _upsert_config(
        bind,
        system_config,
        key="REPORT_STORES_LIST",
        value=report_csv,
        description="Store codes for reporting pipelines",
    )
