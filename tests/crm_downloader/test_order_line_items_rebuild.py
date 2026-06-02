from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from typing import Any

import pytest
import sqlalchemy as sa

from app.common.db import session_scope
from app.crm_downloader import order_line_items_rebuild as rebuild


async def _create_common_tables(db_url: str) -> None:
    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                "CREATE TABLE orders (id INTEGER PRIMARY KEY AUTOINCREMENT, cost_center TEXT, store_code TEXT, order_number TEXT, order_date TEXT, updated_at TEXT, order_status TEXT, status TEXT)"
            )
        )
        await session.execute(sa.text("""
            CREATE TABLE order_line_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT, run_date TEXT, cost_center TEXT NOT NULL, store_code TEXT NOT NULL,
                order_id INTEGER, line_sequence INTEGER, order_number TEXT NOT NULL,
                api_order_id TEXT, api_line_item_id TEXT, api_garment_id TEXT,
                line_item_key TEXT NOT NULL, line_item_uid TEXT NOT NULL,
                garment_name TEXT, service_name TEXT, quantity NUMERIC, weight NUMERIC, amount NUMERIC,
                order_date TEXT, updated_at TEXT, status TEXT, ingest_row_seq INTEGER NOT NULL,
                is_orphan BOOLEAN NOT NULL DEFAULT 0, ingest_remarks TEXT
            )
        """))
        await session.execute(sa.text("""
            CREATE TABLE stg_uc_archive_order_details (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT, run_date TEXT, cost_center TEXT, store_code TEXT, order_code TEXT,
                service TEXT, item_name TEXT, rate NUMERIC, quantity NUMERIC, weight NUMERIC, amount NUMERIC,
                order_datetime_raw TEXT, line_hash TEXT, ingest_row_seq INTEGER, ingest_remarks TEXT
            )
        """))
        await session.execute(sa.text("""
            CREATE TABLE stg_uc_order_detail_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT, run_date TEXT, cost_center TEXT, store_code TEXT, order_code TEXT,
                normalized_order_number TEXT, snapshot_outcome TEXT, detail_row_count INTEGER, ingest_remarks TEXT
            )
        """))
        await session.commit()


async def _rows(db_url: str, sql: str) -> list[Any]:
    async with session_scope(db_url) as session:
        return (await session.execute(sa.text(sql))).all()


@pytest.fixture
def patch_config_and_stores(monkeypatch: pytest.MonkeyPatch, tmp_path):
    db_url = f"sqlite+aiosqlite:///{tmp_path/'rebuild.sqlite'}"
    monkeypatch.setattr(rebuild, "config", SimpleNamespace(database_url=db_url))

    async def load_stores(*, sources, store_codes, logger):
        stores = []
        if "td" in sources:
            stores.append(
                rebuild.RebuildStore(
                    source="td", store_code="TD001", cost_center="CC01"
                )
            )
        if "uc" in sources:
            stores.append(
                rebuild.RebuildStore(
                    source="uc", store_code="UC001", cost_center="CC01"
                )
            )
        return stores

    monkeypatch.setattr(rebuild, "load_rebuild_stores", load_stores)
    return db_url


def test_bounded_window_progression() -> None:
    windows = rebuild.iter_windows(date(2025, 1, 1), date(2025, 1, 10), 4)
    assert [(w.start, w.end) for w in windows] == [
        (date(2025, 1, 1), date(2025, 1, 4)),
        (date(2025, 1, 5), date(2025, 1, 8)),
        (date(2025, 1, 9), date(2025, 1, 10)),
    ]


@pytest.mark.asyncio
async def test_dry_run_reports_planned_replacements_without_mutation(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                "INSERT INTO order_line_items (run_id, cost_center, store_code, order_number, line_item_key, line_item_uid, garment_name, ingest_row_seq) VALUES ('old','CC01','TD001','ORD-1','old','old','Old',1)"
            )
        )
        await session.commit()

    async def fetcher(**kwargs):
        return rebuild.SourceSnapshot(
            line_item_rows=[
                {"order_number": "ORD-1", "line_item_key": "new", "garment_name": "New"}
            ],
            order_snapshots=[
                {
                    "order_number": "ORD-1",
                    "garment_snapshot_outcome": "complete_with_rows",
                }
            ],
        )

    metrics = await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=True,
        run_id="dry",
        fetch_snapshot=fetcher,
    )

    assert metrics[0].deleted_rows == 1
    assert metrics[0].inserted_rows == 1
    rows = await _rows(db_url, "SELECT garment_name FROM order_line_items")
    assert [row.garment_name for row in rows] == ["Old"]


@pytest.mark.asyncio
async def test_td_rebuild_uses_replacement_path(patch_config_and_stores) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                "INSERT INTO orders (id, cost_center, store_code, order_number) VALUES (1,'CC01','TD001','ORD-1')"
            )
        )
        await session.execute(
            sa.text(
                "INSERT INTO order_line_items (run_id, cost_center, store_code, order_id, order_number, line_item_key, line_item_uid, garment_name, ingest_row_seq) VALUES ('old','CC01','TD001',1,'ORD-1','old','old','Old',1)"
            )
        )
        await session.commit()

    async def fetcher(**kwargs):
        return rebuild.SourceSnapshot(
            line_item_rows=[
                {
                    "order_number": "ORD-1",
                    "line_item_key": "new",
                    "garment_name": "New Shirt",
                }
            ],
            order_snapshots=[
                {
                    "order_number": "ORD-1",
                    "garment_snapshot_outcome": "complete_with_rows",
                }
            ],
        )

    metrics = await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=False,
        run_id="td",
        fetch_snapshot=fetcher,
    )

    assert metrics[0].deleted_rows == 1
    assert metrics[0].inserted_rows == 1
    rows = await _rows(
        db_url, "SELECT garment_name FROM order_line_items WHERE order_number='ORD-1'"
    )
    assert [row.garment_name for row in rows] == ["New Shirt"]


@pytest.mark.asyncio
async def test_uc_rebuild_uses_publish_replacement_path(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                "INSERT INTO orders (id, cost_center, store_code, order_number) VALUES (1,'CC01','UC001','ORD-UC')"
            )
        )
        await session.execute(
            sa.text(
                "INSERT INTO order_line_items (run_id, cost_center, store_code, order_id, order_number, line_item_key, line_item_uid, garment_name, service_name, ingest_row_seq) VALUES ('old','CC01','UC001',1,'ORD-UC','old','old','Old','Old Service',1)"
            )
        )
        await session.commit()

    async def fetcher(**kwargs):
        return rebuild.SourceSnapshot(
            line_item_rows=[
                {
                    "order_number": "ORD-UC",
                    "line_hash": "new",
                    "item_name": "New UC",
                    "service": "Wash",
                    "quantity": 1,
                }
            ],
            order_snapshots=[
                {
                    "order_number": "ORD-UC",
                    "snapshot_outcome": "complete_with_rows",
                    "detail_row_count": 1,
                }
            ],
        )

    metrics = await rebuild.run_rebuild(
        source_selection="uc",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=False,
        run_id="uc",
        fetch_snapshot=fetcher,
    )

    assert metrics[0].deleted_rows == 1
    assert metrics[0].inserted_rows == 1
    rows = await _rows(
        db_url,
        "SELECT garment_name, service_name FROM order_line_items WHERE order_number='ORD-UC'",
    )
    assert [(row.garment_name, row.service_name) for row in rows] == [
        ("New UC", "Wash")
    ]


@pytest.mark.asyncio
async def test_complete_empty_deletes_existing_rows(patch_config_and_stores) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                "INSERT INTO order_line_items (run_id, cost_center, store_code, order_number, line_item_key, line_item_uid, garment_name, ingest_row_seq) VALUES ('old','CC01','TD001','ORD-EMPTY','old','old','Delete',1)"
            )
        )
        await session.commit()

    async def fetcher(**kwargs):
        return rebuild.SourceSnapshot(
            line_item_rows=[],
            order_snapshots=[
                {
                    "order_number": "ORD-EMPTY",
                    "garment_snapshot_outcome": "complete_empty",
                }
            ],
        )

    metrics = await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=False,
        run_id="empty",
        fetch_snapshot=fetcher,
    )

    assert metrics[0].complete_empty_orders == 1
    assert metrics[0].deleted_rows == 1
    assert await _rows(db_url, "SELECT * FROM order_line_items") == []


@pytest.mark.asyncio
async def test_incomplete_source_preserves_existing_rows(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                "INSERT INTO order_line_items (run_id, cost_center, store_code, order_number, line_item_key, line_item_uid, garment_name, ingest_row_seq) VALUES ('old','CC01','TD001','ORD-FAIL','old','old','Keep',1)"
            )
        )
        await session.commit()

    async def fetcher(**kwargs):
        return rebuild.SourceSnapshot(
            line_item_rows=[],
            order_snapshots=[
                {
                    "order_number": "ORD-FAIL",
                    "garment_snapshot_outcome": "incomplete_or_failed",
                }
            ],
        )

    metrics = await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=False,
        run_id="fail",
        fetch_snapshot=fetcher,
    )

    assert metrics[0].skipped_incomplete_orders == 1
    rows = await _rows(db_url, "SELECT garment_name FROM order_line_items")
    assert [row.garment_name for row in rows] == ["Keep"]


@pytest.mark.asyncio
async def test_resumability_emits_source_store_window_checkpoints(
    patch_config_and_stores, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    checkpoints: list[dict[str, Any]] = []

    def capture_log_event(**kwargs):
        if kwargs.get("phase") == "order_line_items_rebuild_window":
            checkpoints.append(kwargs["checkpoint"])

    async def fetcher(**kwargs):
        return rebuild.SourceSnapshot(line_item_rows=[], order_snapshots=[])

    monkeypatch.setattr(rebuild, "log_event", capture_log_event)
    await rebuild.run_rebuild(
        source_selection="both",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 3),
        window_size_days=2,
        dry_run=True,
        run_id="resume",
        fetch_snapshot=fetcher,
    )

    assert [
        (item["source"], item["store_code"], item["window_start"], item["window_end"])
        for item in checkpoints
    ] == [
        ("td", "TD001", "2025-01-01", "2025-01-02"),
        ("td", "TD001", "2025-01-03", "2025-01-03"),
        ("uc", "UC001", "2025-01-01", "2025-01-02"),
        ("uc", "UC001", "2025-01-03", "2025-01-03"),
    ]
    assert all(item["dry_run"] for item in checkpoints)
