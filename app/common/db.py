from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator

from alembic import command
from alembic.config import Config
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine


@dataclass(frozen=True)
class PoolSettings:
    pool_size: int | None = None
    max_overflow: int | None = None


_engine_cache: dict[tuple[str, int | None, int | None], AsyncEngine] = {}
_session_factory_cache: dict[tuple[str, int | None, int | None], async_sessionmaker[AsyncSession]] = {}


def _engine_cache_key(database_url: str, pool_settings: PoolSettings | None) -> tuple[str, int | None, int | None]:
    if pool_settings is None:
        return (database_url, None, None)
    return (database_url, pool_settings.pool_size, pool_settings.max_overflow)


def _ensure_async_engine(database_url: str, *, pool_settings: PoolSettings | None = None) -> AsyncEngine:
    cache_key = _engine_cache_key(database_url, pool_settings)
    if cache_key not in _engine_cache:
        engine_kwargs: dict[str, object] = {"future": True}
        if pool_settings and pool_settings.pool_size is not None:
            engine_kwargs["pool_size"] = pool_settings.pool_size
        if pool_settings and pool_settings.max_overflow is not None:
            engine_kwargs["max_overflow"] = pool_settings.max_overflow
        _engine_cache[cache_key] = create_async_engine(database_url, **engine_kwargs)
    return _engine_cache[cache_key]


def _ensure_sessionmaker(database_url: str, *, pool_settings: PoolSettings | None = None) -> async_sessionmaker[AsyncSession]:
    cache_key = _engine_cache_key(database_url, pool_settings)
    if cache_key not in _session_factory_cache:
        engine = _ensure_async_engine(database_url, pool_settings=pool_settings)
        _session_factory_cache[cache_key] = async_sessionmaker(engine, expire_on_commit=False)
    return _session_factory_cache[cache_key]


@asynccontextmanager
async def session_scope(database_url: str, *, pool_settings: PoolSettings | None = None) -> AsyncIterator[AsyncSession]:
    factory = _ensure_sessionmaker(database_url, pool_settings=pool_settings)
    session = factory()
    try:
        yield session
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


def run_alembic_upgrade(*, revision: str, database_url: str, alembic_config_path: str) -> None:
    alembic_cfg = Config(alembic_config_path)
    alembic_cfg.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(alembic_cfg, revision)
