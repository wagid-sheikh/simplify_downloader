from __future__ import annotations

from datetime import date
from io import StringIO

import pytest
import sqlalchemy as sa

from app.common.dashboard_store import (
    DASHBOARD_SUMMARY_COLUMNS,
    persist_dashboard_summary,
    store_master,
)
from app.common.db import _ensure_async_engine, session_scope
from app.dashboard_downloader.json_logger import JsonLogger


def test_store_master_schema_includes_customer_retention_pipeline_flag() -> None:
    assert "customer_retention_pipeline" in store_master.c.keys()


async def _create_tables(database_url: str) -> None:
    engine = _ensure_async_engine(database_url)
    numeric_columns = ",\n".join(f"{column} NUMERIC" for column in DASHBOARD_SUMMARY_COLUMNS)
    async with engine.begin() as connection:
        await connection.execute(
            sa.text(
                f"""
                CREATE TABLE store_master (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    store_code TEXT NOT NULL UNIQUE,
                    store_name TEXT,
                    gstin TEXT,
                    start_date DATE,
                    etl_flag BOOLEAN NOT NULL DEFAULT 0,
                    report_flag BOOLEAN NOT NULL DEFAULT 0,
                    is_active BOOLEAN NOT NULL DEFAULT 1,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        await connection.execute(
            sa.text(
                f"""
                CREATE TABLE store_dashboard_summary (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    store_id INTEGER NOT NULL REFERENCES store_master(id),
                    dashboard_date DATE NOT NULL,
                    run_date_time DATETIME NOT NULL,
                    {numeric_columns},
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (store_id, dashboard_date)
                )
                """
            )
        )


@pytest.mark.asyncio
async def test_persist_dashboard_summary_inserts_start_date_for_new_store(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'dashboard_store_new.db'}"
    await _create_tables(database_url)

    await persist_dashboard_summary(
        {
            "store_code": "A001",
            "store_name": "Demo Store",
            "start_date": date(2025, 2, 15),
            "gstin": "27ABCDE1234F1Z5",
            "dashboard_date": date(2026, 1, 1),
        },
        database_url=database_url,
        logger=JsonLogger(stream=StringIO(), log_file_path=None),
    )

    async with session_scope(database_url) as session:
        row = (
            await session.execute(
                sa.text("SELECT store_code, start_date FROM store_master WHERE store_code = 'A001'")
            )
        ).one()

    assert row == ("A001", "2025-02-15")


@pytest.mark.asyncio
async def test_persist_dashboard_summary_only_fills_null_start_date(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'dashboard_store_existing.db'}"
    await _create_tables(database_url)
    async with session_scope(database_url) as session:
        async with session.begin():
            await session.execute(
                sa.text(
                    """
                    INSERT INTO store_master (store_code, store_name, start_date)
                    VALUES ('KEEP', 'Keep Store', '2025-01-01'), ('FILL', 'Fill Store', NULL)
                    """
                )
            )

    for store_code in ("KEEP", "FILL"):
        await persist_dashboard_summary(
            {
                "store_code": store_code,
                "store_name": f"{store_code} Store",
                "start_date": date(2025, 2, 15),
                "dashboard_date": date(2026, 1, 1),
            },
            database_url=database_url,
            logger=JsonLogger(stream=StringIO(), log_file_path=None),
        )

    async with session_scope(database_url) as session:
        rows = (
            await session.execute(
                sa.text("SELECT store_code, start_date FROM store_master ORDER BY store_code")
            )
        ).all()

    assert rows == [("FILL", "2025-02-15"), ("KEEP", "2025-01-01")]
