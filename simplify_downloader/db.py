from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

from alembic import command
from alembic.config import Config
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

_engine_cache: dict[str, AsyncEngine] = {}
_session_factory_cache: dict[str, async_sessionmaker[AsyncSession]] = {}


def _ensure_async_engine(database_url: str) -> AsyncEngine:
    if database_url not in _engine_cache:
        _engine_cache[database_url] = create_async_engine(database_url, future=True)
    return _engine_cache[database_url]


def _ensure_sessionmaker(database_url: str) -> async_sessionmaker[AsyncSession]:
    if database_url not in _session_factory_cache:
        engine = _ensure_async_engine(database_url)
        _session_factory_cache[database_url] = async_sessionmaker(engine, expire_on_commit=False)
    return _session_factory_cache[database_url]


@asynccontextmanager
async def session_scope(database_url: str) -> AsyncIterator[AsyncSession]:
    factory = _ensure_sessionmaker(database_url)
    async with factory() as session:
        yield session


def _sync_url(database_url: str) -> str:
    if database_url.startswith("postgresql+asyncpg"):
        return database_url.replace("postgresql+asyncpg", "postgresql", 1)
    return database_url


def run_alembic_upgrade(revision: str) -> None:
    alembic_cfg = Config(os.getenv("ALEMBIC_CONFIG", "alembic.ini"))
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        alembic_cfg.set_main_option("sqlalchemy.url", _sync_url(db_url))
    command.upgrade(alembic_cfg, revision)
