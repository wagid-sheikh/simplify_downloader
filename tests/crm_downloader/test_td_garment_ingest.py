from __future__ import annotations

from datetime import date, datetime

import pytest
import sqlalchemy as sa

from app.common.db import session_scope
from app.crm_downloader.td_orders_sync.garment_ingest import ingest_td_garment_rows
from app.crm_downloader.td_orders_sync.main import _compare_quality_passed


@pytest.mark.asyncio
async def test_garment_ingest_uses_row_sequence_fallback_uid_and_quarantines_orphans(tmp_path) -> None:
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
        uids = (
            await session.execute(
                sa.text(
                    "SELECT line_item_uid FROM order_line_items WHERE order_number='ORD-1'"
                )
            )
        ).scalars().all()
    assert orphan_count == 1
    assert uids == ["A001|ORD-1|LI-1|1"]


@pytest.mark.asyncio
async def test_garment_ingest_accepts_camel_case_rows_without_missing_order_warning(tmp_path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'garments_camel.db'}"
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
            sa.text("INSERT INTO orders (cost_center, order_number) VALUES ('A001','ORD-CAMEL-1')")
        )
        await session.commit()

    rows = [
        {
            "orderNumber": "ORD-CAMEL-1",
            "apiOrderId": "AO-1",
            "apiLineItemId": "ALI-1",
            "apiGarmentId": "AG-1",
            "garment": "Suit",
            "subGarment": "Jacket",
            "primaryService": "Dry Clean",
            "amount": "120.00",
            "quantity": "1",
            "status": "Completed",
        }
    ]

    result = await ingest_td_garment_rows(
        rows=rows,
        store_code="S001",
        cost_center="A001",
        run_id="run-camel",
        run_date=datetime.utcnow(),
        window_from_date=date(2025, 1, 1),
        window_to_date=date(2025, 1, 5),
        database_url=db_url,
    )

    assert result.row_count > 0
    assert "Skipped garment row without order number" not in result.warnings

    async with session_scope(db_url) as session:
        stg_count = (await session.execute(sa.text("SELECT COUNT(*) FROM stg_td_garments"))).scalar_one()
        line_count = (await session.execute(sa.text("SELECT COUNT(*) FROM order_line_items"))).scalar_one()
        key_data = (
            await session.execute(
                sa.text(
                    "SELECT line_item_key, api_order_id, api_line_item_id, api_garment_id "
                    "FROM order_line_items WHERE order_number='ORD-CAMEL-1'"
                )
            )
        ).one()

    assert stg_count > 0
    assert line_count > 0
    assert key_data.line_item_key == "Jacket|Dry Clean"
    assert key_data.api_order_id == "AO-1"
    assert key_data.api_line_item_id == "ALI-1"
    assert key_data.api_garment_id == "AG-1"


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


@pytest.mark.asyncio
async def test_garment_ingest_preserves_multiplicity_without_line_item_uid_uniqueness(tmp_path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'garments_multiplicity.db'}"
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
            sa.text("INSERT INTO orders (cost_center, order_number) VALUES ('A001','ORD-DUPE')")
        )
        await session.commit()

    rows = [
        {"order_number": "ORD-DUPE", "line_item_key": "LI-1", "garment_name": "Shirt", "amount": "50"},
        {"order_number": "ORD-DUPE", "line_item_key": "LI-1", "garment_name": "Shirt", "amount": "50"},
    ]

    result = await ingest_td_garment_rows(
        rows=rows,
        store_code="S001",
        cost_center="A001",
        run_id="run-dup",
        run_date=datetime.utcnow(),
        window_from_date=date(2025, 1, 1),
        window_to_date=date(2025, 1, 5),
        database_url=db_url,
    )

    assert result.row_count == 2
    assert result.staging_inserted == 2
    assert result.final_inserted == 2

    async with session_scope(db_url) as session:
        line_item_uids = (
            await session.execute(
                sa.text(
                    "SELECT line_item_uid FROM order_line_items "
                    "WHERE order_number='ORD-DUPE' ORDER BY ingest_row_seq"
                )
            )
        ).scalars().all()
        sequences = (
            await session.execute(
                sa.text(
                    "SELECT ingest_row_seq FROM order_line_items "
                    "WHERE order_number='ORD-DUPE' ORDER BY ingest_row_seq"
                )
            )
        ).scalars().all()

    assert line_item_uids == ["A001|ORD-DUPE|LI-1|1", "A001|ORD-DUPE|LI-1|2"]
    assert sequences == [1, 2]
