from __future__ import annotations

from datetime import date, datetime

import pytest
import sqlalchemy as sa

from app.common.db import session_scope
from app.crm_downloader.td_orders_sync.garment_ingest import ingest_td_garment_rows
from app.crm_downloader.td_orders_sync.main import _compare_quality_passed


@pytest.mark.asyncio
async def test_garment_ingest_uses_fallback_key_and_quarantines_orphans(tmp_path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'garments.db'}"
    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                """
                CREATE TABLE orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cost_center TEXT,
                    order_number TEXT
                )
                """
            )
        )
        await session.execute(
            sa.text("INSERT INTO orders (cost_center, order_number) VALUES ('A001','ORD-1')")
        )
        await session.commit()

    rows = [
        {"order_number": "ORD-1", "line_item_key": "LI-1", "garment_name": "Shirt", "amount": "50"},
        {"order_number": "ORD-404", "line_item_key": "LI-404", "garment_name": "Pant", "amount": "80"},
    ]
    result = await ingest_td_garment_rows(
        rows=rows,
        store_code="S001",
        cost_center="A001",
        run_id="run-1",
        run_date=datetime.utcnow(),
        window_from_date=date(2025, 1, 1),
        window_to_date=date(2025, 1, 5),
        database_url=db_url,
    )
    assert result.row_count == 2
    assert result.orphan_rows == 1

    async with session_scope(db_url) as session:
        orphan_count = (
            await session.execute(sa.text("SELECT COUNT(*) FROM order_line_items WHERE is_orphan = 1"))
        ).scalar_one()
        fallback_uid = (
            await session.execute(
                sa.text(
                    "SELECT line_item_uid FROM order_line_items WHERE order_number='ORD-1'"
                )
            )
        ).scalar_one()
    assert orphan_count == 1
    assert fallback_uid == "A001|ORD-1|LI-1"


def test_compare_threshold_helper(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TD_GARMENT_COMPARE_MAX_MISSING", "1")
    monkeypatch.setenv("TD_GARMENT_COMPARE_MAX_AMOUNT_MISMATCH", "2")
    monkeypatch.setenv("TD_GARMENT_COMPARE_MAX_STATUS_MISMATCH", "3")
    assert _compare_quality_passed(
        {
            "missing_in_api": 1,
            "missing_in_ui": 0,
            "amount_mismatches": 2,
            "status_mismatches": 3,
        }
    )
    assert not _compare_quality_passed(
        {
            "missing_in_api": 2,
            "missing_in_ui": 0,
            "amount_mismatches": 0,
            "status_mismatches": 0,
        }
    )
