from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pytest
import sqlalchemy as sa

from app.common.db import session_scope
from app.crm_downloader.line_item_rebuild import (
    RebuildCounts,
    RebuildStore,
    SourceSnapshot,
    bounded_windows,
    ensure_rebuild_progress_table,
    run_rebuild,
)
from app.crm_downloader.td_orders_sync.garment_ingest import ingest_td_garment_rows


async def _create_orders(db_url: str) -> None:
    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                """
                CREATE TABLE orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cost_center TEXT,
                    store_code TEXT,
                    order_number TEXT,
                    order_date TEXT,
                    updated_at TEXT,
                    order_status TEXT
                )
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO orders (id, cost_center, store_code, order_number)
                VALUES
                    (1, 'CC01', 'TD001', 'ORD-ROWS'),
                    (2, 'CC01', 'TD001', 'ORD-EMPTY'),
                    (3, 'CC01', 'TD001', 'ORD-FAIL')
                """
            )
        )
        await session.commit()


@pytest.mark.asyncio
async def test_td_rebuild_dry_run_reports_counts_without_mutating_rows(tmp_path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'dry_run.sqlite'}"
    await _create_orders(db_url)
    await ingest_td_garment_rows(
        rows=[{"order_number": "ORD-ROWS", "line_item_key": "old", "garment_name": "Old"}],
        authoritative_order_scope=[{"order_number": "ORD-ROWS", "garment_snapshot_outcome": "complete_with_rows"}],
        store_code="TD001",
        cost_center="CC01",
        run_id="seed",
        run_date=datetime.utcnow(),
        window_from_date=date(2025, 1, 1),
        window_to_date=date(2025, 1, 1),
        database_url=db_url,
    )

    result = await ingest_td_garment_rows(
        rows=[
            {"order_number": "ORD-ROWS", "line_item_key": "new", "garment_name": "New"},
            {"order_number": "ORD-ORPH", "line_item_key": "orph", "garment_name": "Orphan"},
        ],
        authoritative_order_scope=[
            {"order_number": "ORD-ROWS", "garment_snapshot_outcome": "complete_with_rows"},
            {"order_number": "ORD-EMPTY", "garment_snapshot_outcome": "complete_empty"},
            {"order_number": "ORD-FAIL", "garment_snapshot_outcome": "incomplete_or_failed"},
        ],
        store_code="TD001",
        cost_center="CC01",
        run_id="dry",
        run_date=datetime.utcnow(),
        window_from_date=date(2025, 1, 1),
        window_to_date=date(2025, 1, 1),
        database_url=db_url,
        dry_run=True,
    )

    assert result.authoritative_orders_inspected == 3
    assert result.complete_empty_orders == 1
    assert result.replacement_skipped_incomplete_orders == 1
    assert result.deleted_final_rows == 1
    assert result.inserted_final_rows == 1
    assert result.orphan_rows == 1

    async with session_scope(db_url) as session:
        line_items = (await session.execute(sa.text("SELECT run_id, garment_name FROM order_line_items"))).all()
        staging_count = (await session.execute(sa.text("SELECT COUNT(*) FROM stg_td_garments WHERE run_id='dry'"))).scalar_one()
    assert [(row.run_id, row.garment_name) for row in line_items] == [("seed", "Old")]
    assert staging_count == 0


@pytest.mark.asyncio
async def test_td_rebuild_preserves_incomplete_and_deletes_complete_empty(tmp_path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'outcomes.sqlite'}"
    await _create_orders(db_url)
    await ingest_td_garment_rows(
        rows=[
            {"order_number": "ORD-EMPTY", "line_item_key": "empty-old", "garment_name": "Delete"},
            {"order_number": "ORD-FAIL", "line_item_key": "fail-old", "garment_name": "Keep"},
        ],
        authoritative_order_scope=[
            {"order_number": "ORD-EMPTY", "garment_snapshot_outcome": "complete_with_rows"},
            {"order_number": "ORD-FAIL", "garment_snapshot_outcome": "complete_with_rows"},
        ],
        store_code="TD001",
        cost_center="CC01",
        run_id="seed",
        run_date=datetime.utcnow(),
        window_from_date=date(2025, 1, 1),
        window_to_date=date(2025, 1, 1),
        database_url=db_url,
    )

    result = await ingest_td_garment_rows(
        rows=[],
        authoritative_order_scope=[
            {"order_number": "ORD-EMPTY", "garment_snapshot_outcome": "complete_empty"},
            {"order_number": "ORD-FAIL", "garment_snapshot_outcome": "incomplete_or_failed"},
        ],
        store_code="TD001",
        cost_center="CC01",
        run_id="replace",
        run_date=datetime.utcnow(),
        window_from_date=date(2025, 1, 1),
        window_to_date=date(2025, 1, 1),
        database_url=db_url,
    )

    assert result.complete_empty_orders == 1
    assert result.replacement_skipped_incomplete_orders == 1
    assert result.deleted_final_rows == 1
    async with session_scope(db_url) as session:
        rows = (await session.execute(sa.text("SELECT order_number, garment_name FROM order_line_items"))).all()
    assert [(row.order_number, row.garment_name) for row in rows] == [("ORD-FAIL", "Keep")]


def test_bounded_windows_uses_operator_window_size() -> None:
    windows = bounded_windows(date(2025, 1, 1), date(2025, 1, 10), window_days=3)
    assert [(w.from_date, w.to_date) for w in windows] == [
        (date(2025, 1, 1), date(2025, 1, 3)),
        (date(2025, 1, 4), date(2025, 1, 6)),
        (date(2025, 1, 7), date(2025, 1, 9)),
        (date(2025, 1, 10), date(2025, 1, 10)),
    ]


@pytest.mark.asyncio
async def test_rebuild_resume_skips_completed_store_windows(tmp_path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'resume.sqlite'}"
    await ensure_rebuild_progress_table(db_url)
    calls: list[tuple[str, date, date]] = []

    async def fetcher(**kwargs: Any) -> SourceSnapshot:
        store = kwargs["store"]
        window = kwargs["window"]
        calls.append((store.store_code, window.from_date, window.to_date))
        return SourceSnapshot()

    async def replacer(**_: Any) -> RebuildCounts:
        return RebuildCounts(inspected_orders=1)

    stores = [RebuildStore(source="TD", store_code="TD001", cost_center="CC01")]
    first = await run_rebuild(
        source="TD",
        from_date=date(2025, 1, 1),
        to_date=date(2025, 1, 2),
        stores=stores,
        run_id="resume-run-1",
        window_days=1,
        database_url=db_url,
        snapshot_fetcher=fetcher,
        replacement_service=replacer,
    )
    assert [result.status for result in first] == ["completed", "completed"]

    second = await run_rebuild(
        source="TD",
        from_date=date(2025, 1, 1),
        to_date=date(2025, 1, 2),
        stores=stores,
        run_id="resume-run-2",
        window_days=1,
        database_url=db_url,
        snapshot_fetcher=fetcher,
        replacement_service=replacer,
    )
    assert [result.status for result in second] == ["skipped_completed", "skipped_completed"]
    assert calls == [("TD001", date(2025, 1, 1), date(2025, 1, 1)), ("TD001", date(2025, 1, 2), date(2025, 1, 2))]


@pytest.mark.asyncio
async def test_rebuild_processes_stores_in_bounded_batches(tmp_path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'batches.sqlite'}"
    active = 0
    max_active = 0

    async def fetcher(**_: Any) -> SourceSnapshot:
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        active -= 1
        return SourceSnapshot()

    async def replacer(**_: Any) -> RebuildCounts:
        return RebuildCounts(inspected_orders=1)

    stores = [
        RebuildStore(source="TD", store_code="TD001", cost_center="CC01"),
        RebuildStore(source="TD", store_code="TD002", cost_center="CC02"),
    ]
    results = await run_rebuild(
        source="TD",
        from_date=date(2025, 1, 1),
        to_date=date(2025, 1, 1),
        stores=stores,
        run_id="batch-run",
        dry_run=True,
        store_batch_size=1,
        database_url=db_url,
        snapshot_fetcher=fetcher,
        replacement_service=replacer,
    )
    assert len(results) == 2
    assert max_active == 1
