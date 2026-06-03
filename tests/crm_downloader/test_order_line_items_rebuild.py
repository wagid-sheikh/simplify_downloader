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
async def test_uc_rebuild_stages_and_publishes_each_window_with_child_run_ids(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                "INSERT INTO orders (id, cost_center, store_code, order_number) VALUES "
                "(1,'CC01','UC001','ORD-W1'), (2,'CC01','UC001','ORD-W2')"
            )
        )
        await session.commit()

    async def fetcher(**kwargs):
        order_number = f"ORD-W{kwargs['window'].start.day}"
        return rebuild.SourceSnapshot(
            line_item_rows=[
                {
                    "order_number": order_number,
                    "line_hash": f"hash-{order_number}",
                    "item_name": f"Item {order_number}",
                    "service": "Wash",
                    "quantity": 1,
                }
            ],
            order_snapshots=[
                {
                    "order_number": order_number,
                    "snapshot_outcome": "complete_with_rows",
                    "detail_row_count": 1,
                }
            ],
        )

    metrics = await rebuild.run_rebuild(
        source_selection="uc",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 2),
        window_size_days=1,
        dry_run=False,
        run_id="parent",
        fetch_snapshot=fetcher,
    )

    assert [metric.uc_child_run_id for metric in metrics] == [
        "parent:uc:UC001:2025-01-01:2025-01-01",
        "parent:uc:UC001:2025-01-02:2025-01-02",
    ]
    assert [metric.inserted_rows for metric in metrics] == [1, 1]
    staged = await _rows(
        db_url,
        "SELECT run_id, order_code FROM stg_uc_order_detail_snapshots ORDER BY run_id",
    )
    assert [(row.run_id, row.order_code) for row in staged] == [
        ("parent:uc:UC001:2025-01-01:2025-01-01", "ORD-W1"),
        ("parent:uc:UC001:2025-01-02:2025-01-02", "ORD-W2"),
    ]
    final_rows = await _rows(
        db_url,
        "SELECT run_id, order_number FROM order_line_items ORDER BY order_number",
    )
    assert [(row.run_id, row.order_number) for row in final_rows] == [
        ("parent:uc:UC001:2025-01-01:2025-01-01", "ORD-W1"),
        ("parent:uc:UC001:2025-01-02:2025-01-02", "ORD-W2"),
    ]


@pytest.mark.asyncio
async def test_uc_same_order_in_multiple_windows_uses_distinct_staging_identity(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                "CREATE UNIQUE INDEX uq_test_uc_snapshot_run_store_order "
                "ON stg_uc_order_detail_snapshots (run_id, store_code, normalized_order_number)"
            )
        )
        await session.execute(
            sa.text(
                "INSERT INTO orders (id, cost_center, store_code, order_number) VALUES (1,'CC01','UC001','ORD-SAME')"
            )
        )
        await session.commit()

    async def fetcher(**kwargs):
        suffix = kwargs["window"].start.isoformat()
        return rebuild.SourceSnapshot(
            line_item_rows=[
                {
                    "order_number": "ORD-SAME",
                    "line_hash": f"line-{suffix}",
                    "item_name": f"Item {suffix}",
                    "service": "Wash",
                    "quantity": 1,
                }
            ],
            order_snapshots=[
                {
                    "order_number": "ORD-SAME",
                    "snapshot_outcome": "complete_with_rows",
                    "detail_row_count": 1,
                }
            ],
        )

    metrics = await rebuild.run_rebuild(
        source_selection="uc",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 2),
        window_size_days=1,
        dry_run=False,
        run_id="same-parent",
        fetch_snapshot=fetcher,
    )

    assert len(metrics) == 2
    assert [metric.deleted_rows for metric in metrics] == [0, 1]
    snapshots = await _rows(
        db_url,
        "SELECT run_id, normalized_order_number FROM stg_uc_order_detail_snapshots ORDER BY run_id",
    )
    assert [(row.run_id, row.normalized_order_number) for row in snapshots] == [
        ("same-parent:uc:UC001:2025-01-01:2025-01-01", "ORD-SAME"),
        ("same-parent:uc:UC001:2025-01-02:2025-01-02", "ORD-SAME"),
    ]
    final_rows = await _rows(
        db_url,
        "SELECT garment_name FROM order_line_items WHERE order_number='ORD-SAME'",
    )
    assert [row.garment_name for row in final_rows] == ["Item 2025-01-02"]


@pytest.mark.asyncio
async def test_uc_later_window_metrics_are_scoped_to_later_child_run(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                "INSERT INTO orders (id, cost_center, store_code, order_number) VALUES "
                "(1,'CC01','UC001','ORD-A'), (2,'CC01','UC001','ORD-B'), (3,'CC01','UC001','ORD-C')"
            )
        )
        await session.commit()

    async def fetcher(**kwargs):
        if kwargs["window"].start == date(2025, 1, 1):
            orders = ["ORD-A", "ORD-B"]
        else:
            orders = ["ORD-C"]
        return rebuild.SourceSnapshot(
            line_item_rows=[
                {
                    "order_number": order_number,
                    "line_hash": f"line-{order_number}",
                    "item_name": order_number,
                    "service": "Wash",
                    "quantity": 1,
                }
                for order_number in orders
            ],
            order_snapshots=[
                {
                    "order_number": order_number,
                    "snapshot_outcome": "complete_with_rows",
                    "detail_row_count": 1,
                }
                for order_number in orders
            ],
        )

    metrics = await rebuild.run_rebuild(
        source_selection="uc",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 2),
        window_size_days=1,
        dry_run=False,
        run_id="metrics-parent",
        fetch_snapshot=fetcher,
    )

    assert [metric.inspected_orders for metric in metrics] == [2, 1]
    assert [metric.inserted_rows for metric in metrics] == [2, 1]
    assert metrics[1].uc_child_run_id == "metrics-parent:uc:UC001:2025-01-02:2025-01-02"


@pytest.mark.asyncio
async def test_source_both_preserves_td_parent_and_uc_child_window_behavior(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                "INSERT INTO orders (id, cost_center, store_code, order_number) VALUES "
                "(1,'CC01','TD001','ORD-TD'), (2,'CC01','UC001','ORD-UC')"
            )
        )
        await session.commit()

    async def fetcher(**kwargs):
        if kwargs["source"] == "td":
            return rebuild.SourceSnapshot(
                line_item_rows=[
                    {
                        "order_number": "ORD-TD",
                        "line_item_key": "td-line",
                        "garment_name": "TD Shirt",
                    }
                ],
                order_snapshots=[
                    {
                        "order_number": "ORD-TD",
                        "garment_snapshot_outcome": "complete_with_rows",
                    }
                ],
            )
        return rebuild.SourceSnapshot(
            line_item_rows=[
                {
                    "order_number": "ORD-UC",
                    "line_hash": "uc-line",
                    "item_name": "UC Shirt",
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
        source_selection="both",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=False,
        run_id="both-parent",
        fetch_snapshot=fetcher,
    )

    assert [(metric.source, metric.uc_child_run_id) for metric in metrics] == [
        ("td", None),
        ("uc", "both-parent:uc:UC001:2025-01-01:2025-01-01"),
    ]
    final_rows = await _rows(
        db_url,
        "SELECT store_code, order_number, run_id FROM order_line_items ORDER BY store_code",
    )
    assert [(row.store_code, row.order_number, row.run_id) for row in final_rows] == [
        ("TD001", "ORD-TD", "both-parent"),
        ("UC001", "ORD-UC", "both-parent:uc:UC001:2025-01-01:2025-01-01"),
    ]
    progress = await _rows(
        db_url,
        "SELECT source, store_code, run_id, status FROM order_line_items_rebuild_progress ORDER BY source",
    )
    assert [
        (row.source, row.store_code, row.run_id, row.status) for row in progress
    ] == [
        ("td", "TD001", "both-parent", "success"),
        ("uc", "UC001", "both-parent", "success"),
    ]


@pytest.mark.asyncio
async def test_resume_progress_uses_source_store_window_while_uc_staging_uses_child_run(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    await rebuild._ensure_progress_table(db_url)
    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                "INSERT INTO order_line_items_rebuild_progress "
                "(source, store_code, cost_center, window_start, window_end, run_id, status, attempt_no) "
                "VALUES ('uc', 'UC001', 'CC01', '2025-01-01', '2025-01-01', 'old-parent', 'success', 1)"
            )
        )
        await session.execute(
            sa.text(
                "INSERT INTO orders (id, cost_center, store_code, order_number) VALUES (1,'CC01','UC001','ORD-RESUME')"
            )
        )
        await session.commit()
    seen: list[date] = []

    async def fetcher(**kwargs):
        seen.append(kwargs["window"].start)
        return rebuild.SourceSnapshot(
            line_item_rows=[
                {
                    "order_number": "ORD-RESUME",
                    "line_hash": "resume-line",
                    "item_name": "Resume Item",
                    "service": "Wash",
                    "quantity": 1,
                }
            ],
            order_snapshots=[
                {
                    "order_number": "ORD-RESUME",
                    "snapshot_outcome": "complete_with_rows",
                    "detail_row_count": 1,
                }
            ],
        )

    metrics = await rebuild.run_rebuild(
        source_selection="uc",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 2),
        window_size_days=1,
        dry_run=False,
        resume=True,
        run_id="resume-parent",
        fetch_snapshot=fetcher,
    )

    assert seen == [date(2025, 1, 2)]
    assert [metric.uc_child_run_id for metric in metrics] == [
        "resume-parent:uc:UC001:2025-01-02:2025-01-02"
    ]
    staged = await _rows(
        db_url,
        "SELECT run_id, order_code FROM stg_uc_order_detail_snapshots",
    )
    assert [(row.run_id, row.order_code) for row in staged] == [
        ("resume-parent:uc:UC001:2025-01-02:2025-01-02", "ORD-RESUME")
    ]
    progress = await _rows(
        db_url,
        "SELECT window_start, window_end, run_id, status FROM order_line_items_rebuild_progress ORDER BY window_start",
    )
    assert [
        (row.window_start, row.window_end, row.run_id, row.status) for row in progress
    ] == [
        ("2025-01-01", "2025-01-01", "old-parent", "success"),
        ("2025-01-02", "2025-01-02", "resume-parent", "success"),
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
    assert [
        item.get("uc_child_run_id") for item in checkpoints if item["source"] == "uc"
    ] == [
        "resume:uc:UC001:2025-01-01:2025-01-02",
        "resume:uc:UC001:2025-01-03:2025-01-03",
    ]


@pytest.mark.asyncio
async def test_full_range_invocation_expands_expected_crm_safe_windows(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    seen: list[tuple[date, date]] = []

    async def fetcher(**kwargs):
        seen.append((kwargs["window"].start, kwargs["window"].end))
        return rebuild.SourceSnapshot(line_item_rows=[], order_snapshots=[])

    await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 3, 5),
        window_size_days=90,
        dry_run=True,
        run_id="full-range",
        fetch_snapshot=fetcher,
    )

    assert seen == [
        (date(2025, 1, 1), date(2025, 1, 30)),
        (date(2025, 1, 31), date(2025, 3, 1)),
        (date(2025, 3, 2), date(2025, 3, 5)),
    ]


@pytest.mark.asyncio
async def test_retryable_failed_window_is_retried(patch_config_and_stores) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    attempts = 0

    async def fetcher(**kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise TimeoutError("navigation failed while loading CRM report")
        return rebuild.SourceSnapshot(line_item_rows=[], order_snapshots=[])

    metrics = await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=True,
        run_id="retry",
        fetch_snapshot=fetcher,
    )

    assert attempts == 2
    assert len(metrics) == 1
    rows = await _rows(
        db_url,
        "SELECT status, attempt_no FROM order_line_items_rebuild_progress WHERE run_id='retry'",
    )
    assert [(row.status, row.attempt_no) for row in rows] == [("success", 2)]


@pytest.mark.asyncio
async def test_missing_window_detection_reports_failed_windows(
    patch_config_and_stores, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    missing_events: list[dict[str, Any]] = []

    def capture_log_event(**kwargs):
        if kwargs.get("phase") == "order_line_items_rebuild_missing_windows":
            missing_events.append(kwargs)

    async def fetcher(**kwargs):
        if kwargs["window"].start == date(2025, 1, 2):
            raise RuntimeError("permanent crm failure")
        return rebuild.SourceSnapshot(line_item_rows=[], order_snapshots=[])

    monkeypatch.setattr(rebuild, "log_event", capture_log_event)
    metrics = await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 2),
        window_size_days=1,
        dry_run=True,
        run_id="missing",
        fetch_snapshot=fetcher,
    )

    assert len(metrics) == 1
    assert missing_events[-1]["missing_window_count"] == 1
    assert missing_events[-1]["missing_windows"] == [
        {
            "source": "td",
            "store_code": "TD001",
            "window_start": "2025-01-02",
            "window_end": "2025-01-02",
        }
    ]


@pytest.mark.asyncio
async def test_resume_skips_last_success_and_processes_remaining_window(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    await rebuild._ensure_progress_table(db_url)
    async with session_scope(db_url) as session:
        await session.execute(sa.text("""
            INSERT INTO order_line_items_rebuild_progress
            (source, store_code, cost_center, window_start, window_end, run_id, status, attempt_no)
            VALUES ('td', 'TD001', 'CC01', '2025-01-01', '2025-01-01', 'old', 'success', 1)
        """))
        await session.commit()
    seen: list[date] = []

    async def fetcher(**kwargs):
        seen.append(kwargs["window"].start)
        return rebuild.SourceSnapshot(line_item_rows=[], order_snapshots=[])

    await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 2),
        window_size_days=1,
        dry_run=True,
        resume=True,
        run_id="resume-success",
        fetch_snapshot=fetcher,
    )

    assert seen == [date(2025, 1, 2)]


@pytest.mark.asyncio
async def test_store_start_date_used_when_start_date_omitted(
    patch_config_and_stores, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    async with session_scope(db_url) as session:
        await session.execute(sa.text("""
            CREATE TABLE store_master (
                store_code TEXT, sync_group TEXT, start_date DATE
            )
        """))
        await session.execute(
            sa.text("INSERT INTO store_master VALUES ('TD001', 'TD', '2025-02-10')")
        )
        await session.commit()
    seen: list[date] = []

    async def fetcher(**kwargs):
        seen.append(kwargs["window"].start)
        return rebuild.SourceSnapshot(line_item_rows=[], order_snapshots=[])

    await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=None,
        end_date=date(2025, 2, 10),
        window_size_days=1,
        dry_run=True,
        run_id="store-start",
        fetch_snapshot=fetcher,
    )

    assert seen == [date(2025, 2, 10)]


@pytest.mark.asyncio
async def test_source_specific_lower_limit_overrides_thirty_day_cap(
    patch_config_and_stores, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)

    async def load_stores(*, sources, store_codes, logger):
        return [
            rebuild.RebuildStore(
                source="td",
                store_code="TD001",
                cost_center="CC01",
                sync_config={"td_crm_source_window_days": 10},
            )
        ]

    monkeypatch.setattr(rebuild, "load_rebuild_stores", load_stores)
    seen: list[tuple[date, date]] = []

    async def fetcher(**kwargs):
        seen.append((kwargs["window"].start, kwargs["window"].end))
        return rebuild.SourceSnapshot(line_item_rows=[], order_snapshots=[])

    await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 25),
        window_size_days=90,
        dry_run=True,
        run_id="lower-limit",
        fetch_snapshot=fetcher,
    )

    assert seen == [
        (date(2025, 1, 1), date(2025, 1, 10)),
        (date(2025, 1, 11), date(2025, 1, 20)),
        (date(2025, 1, 21), date(2025, 1, 25)),
    ]
