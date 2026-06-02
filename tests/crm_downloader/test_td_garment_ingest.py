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

async def _create_orders(db_url: str, order_numbers: list[str]) -> None:
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
        for order_number in order_numbers:
            await session.execute(
                sa.text("INSERT INTO orders (cost_center, order_number) VALUES ('A001', :order_number)"),
                {"order_number": order_number},
            )
        await session.commit()


async def _line_count(db_url: str, order_number: str | None = None) -> int:
    async with session_scope(db_url) as session:
        if order_number is None:
            return (await session.execute(sa.text("SELECT COUNT(*) FROM order_line_items"))).scalar_one()
        return (
            await session.execute(
                sa.text("SELECT COUNT(*) FROM order_line_items WHERE order_number = :order_number"),
                {"order_number": order_number},
            )
        ).scalar_one()


@pytest.mark.asyncio
async def test_garment_ingest_complete_snapshot_rerun_replaces_without_growth(tmp_path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'garments_rerun.db'}"
    await _create_orders(db_url, ["ORD-RERUN"])
    rows = [
        {"order_number": "ORD-RERUN", "line_item_key": "LI-1", "garment_name": "Shirt", "amount": "50"},
        {"order_number": "ORD-RERUN", "line_item_key": "LI-2", "garment_name": "Pant", "amount": "80"},
    ]
    scope = [{"order_number": "ORD-RERUN", "garment_snapshot_outcome": "complete_with_rows"}]

    first = await ingest_td_garment_rows(
        rows=rows,
        authoritative_order_scope=scope,
        store_code="S001",
        cost_center="A001",
        run_id="run-rerun-1",
        run_date=datetime.utcnow(),
        window_from_date=date(2025, 1, 1),
        window_to_date=date(2025, 1, 5),
        database_url=db_url,
    )
    second = await ingest_td_garment_rows(
        rows=rows,
        authoritative_order_scope=scope,
        store_code="S001",
        cost_center="A001",
        run_id="run-rerun-2",
        run_date=datetime.utcnow(),
        window_from_date=date(2025, 1, 1),
        window_to_date=date(2025, 1, 5),
        database_url=db_url,
    )

    assert first.inserted_final_rows == 2
    assert second.deleted_final_rows == 2
    assert second.inserted_final_rows == 2
    assert await _line_count(db_url, "ORD-RERUN") == 2


@pytest.mark.asyncio
async def test_garment_ingest_complete_snapshot_deletes_stale_removed_line(tmp_path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'garments_stale.db'}"
    await _create_orders(db_url, ["ORD-STALE"])
    scope = [{"order_number": "ORD-STALE", "garment_snapshot_outcome": "complete_with_rows"}]
    await ingest_td_garment_rows(
        rows=[
            {"order_number": "ORD-STALE", "line_item_key": "LI-1", "garment_name": "Shirt"},
            {"order_number": "ORD-STALE", "line_item_key": "LI-2", "garment_name": "Pant"},
        ],
        authoritative_order_scope=scope,
        store_code="S001",
        cost_center="A001",
        run_id="run-stale-1",
        run_date=datetime.utcnow(),
        window_from_date=date(2025, 1, 1),
        window_to_date=date(2025, 1, 5),
        database_url=db_url,
    )
    result = await ingest_td_garment_rows(
        rows=[{"order_number": "ORD-STALE", "line_item_key": "LI-1", "garment_name": "Shirt"}],
        authoritative_order_scope=scope,
        store_code="S001",
        cost_center="A001",
        run_id="run-stale-2",
        run_date=datetime.utcnow(),
        window_from_date=date(2025, 1, 1),
        window_to_date=date(2025, 1, 5),
        database_url=db_url,
    )

    assert result.deleted_final_rows == 2
    assert result.inserted_final_rows == 1
    assert await _line_count(db_url, "ORD-STALE") == 1


@pytest.mark.asyncio
async def test_garment_ingest_complete_snapshot_preserves_identical_duplicate_rows(tmp_path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'garments_identical.db'}"
    await _create_orders(db_url, ["ORD-SAME"])
    rows = [
        {"order_number": "ORD-SAME", "line_item_key": "LI-1", "garment_name": "Shirt", "amount": "50"},
        {"order_number": "ORD-SAME", "line_item_key": "LI-1", "garment_name": "Shirt", "amount": "50"},
    ]

    result = await ingest_td_garment_rows(
        rows=rows,
        authoritative_order_scope=[{"order_number": "ORD-SAME", "garment_snapshot_outcome": "complete_with_rows"}],
        store_code="S001",
        cost_center="A001",
        run_id="run-same",
        run_date=datetime.utcnow(),
        window_from_date=date(2025, 1, 1),
        window_to_date=date(2025, 1, 5),
        database_url=db_url,
    )

    assert result.inserted_final_rows == 2
    assert await _line_count(db_url, "ORD-SAME") == 2


@pytest.mark.asyncio
async def test_garment_ingest_complete_empty_deletes_all_local_rows(tmp_path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'garments_empty.db'}"
    await _create_orders(db_url, ["ORD-EMPTY"])
    await ingest_td_garment_rows(
        rows=[{"order_number": "ORD-EMPTY", "line_item_key": "LI-OLD", "garment_name": "Old"}],
        authoritative_order_scope=[{"order_number": "ORD-EMPTY", "garment_snapshot_outcome": "complete_with_rows"}],
        store_code="S001",
        cost_center="A001",
        run_id="run-empty-1",
        run_date=datetime.utcnow(),
        window_from_date=date(2025, 1, 1),
        window_to_date=date(2025, 1, 5),
        database_url=db_url,
    )
    result = await ingest_td_garment_rows(
        rows=[],
        authoritative_order_scope=[{"order_number": "ORD-EMPTY", "garment_snapshot_outcome": "complete_empty"}],
        store_code="S001",
        cost_center="A001",
        run_id="run-empty-2",
        run_date=datetime.utcnow(),
        window_from_date=date(2025, 1, 1),
        window_to_date=date(2025, 1, 5),
        database_url=db_url,
    )

    assert result.complete_empty_orders == 1
    assert result.deleted_final_rows == 1
    assert result.inserted_final_rows == 0
    assert await _line_count(db_url, "ORD-EMPTY") == 0


@pytest.mark.asyncio
async def test_garment_ingest_incomplete_scope_preserves_existing_and_writes_staging(tmp_path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'garments_incomplete.db'}"
    await _create_orders(db_url, ["ORD-INCOMPLETE"])
    await ingest_td_garment_rows(
        rows=[{"order_number": "ORD-INCOMPLETE", "line_item_key": "LI-OLD", "garment_name": "Old"}],
        authoritative_order_scope=[{"order_number": "ORD-INCOMPLETE", "garment_snapshot_outcome": "complete_with_rows"}],
        store_code="S001",
        cost_center="A001",
        run_id="run-incomplete-1",
        run_date=datetime.utcnow(),
        window_from_date=date(2025, 1, 1),
        window_to_date=date(2025, 1, 5),
        database_url=db_url,
    )
    result = await ingest_td_garment_rows(
        rows=[{"order_number": "ORD-INCOMPLETE", "line_item_key": "LI-NEW", "garment_name": "New"}],
        authoritative_order_scope=[{"order_number": "ORD-INCOMPLETE", "garment_snapshot_outcome": "incomplete_or_failed"}],
        replacement_allowed=False,
        store_code="S001",
        cost_center="A001",
        run_id="run-incomplete-2",
        run_date=datetime.utcnow(),
        window_from_date=date(2025, 1, 1),
        window_to_date=date(2025, 1, 5),
        database_url=db_url,
    )

    assert result.replacement_skipped_incomplete_orders == 1
    assert result.deleted_final_rows == 0
    assert result.inserted_final_rows == 0
    assert result.staging_rows == 1
    assert await _line_count(db_url, "ORD-INCOMPLETE") == 1


@pytest.mark.asyncio
async def test_garment_ingest_does_not_delete_orders_outside_authoritative_scope(tmp_path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'garments_scope.db'}"
    await _create_orders(db_url, ["ORD-IN", "ORD-OUT"])
    await ingest_td_garment_rows(
        rows=[
            {"order_number": "ORD-IN", "line_item_key": "LI-IN", "garment_name": "In"},
            {"order_number": "ORD-OUT", "line_item_key": "LI-OUT", "garment_name": "Out"},
        ],
        authoritative_order_scope=[
            {"order_number": "ORD-IN", "garment_snapshot_outcome": "complete_with_rows"},
            {"order_number": "ORD-OUT", "garment_snapshot_outcome": "complete_with_rows"},
        ],
        store_code="S001",
        cost_center="A001",
        run_id="run-scope-1",
        run_date=datetime.utcnow(),
        window_from_date=date(2025, 1, 1),
        window_to_date=date(2025, 1, 5),
        database_url=db_url,
    )
    await ingest_td_garment_rows(
        rows=[{"order_number": "ORD-IN", "line_item_key": "LI-IN2", "garment_name": "In2"}],
        authoritative_order_scope=[{"order_number": "ORD-IN", "garment_snapshot_outcome": "complete_with_rows"}],
        store_code="S001",
        cost_center="A001",
        run_id="run-scope-2",
        run_date=datetime.utcnow(),
        window_from_date=date(2025, 1, 1),
        window_to_date=date(2025, 1, 5),
        database_url=db_url,
    )

    assert await _line_count(db_url, "ORD-IN") == 1
    assert await _line_count(db_url, "ORD-OUT") == 1

@pytest.mark.asyncio
async def test_garment_ingest_stages_but_does_not_replace_rows_outside_supplied_scope(tmp_path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'garments_rows_outside_scope.db'}"
    await _create_orders(db_url, ["ORD-IN", "ORD-OUT"])
    await ingest_td_garment_rows(
        rows=[{"order_number": "ORD-OUT", "line_item_key": "LI-OLD", "garment_name": "Old"}],
        authoritative_order_scope=[{"order_number": "ORD-OUT", "garment_snapshot_outcome": "complete_with_rows"}],
        store_code="S001",
        cost_center="A001",
        run_id="run-outscope-1",
        run_date=datetime.utcnow(),
        window_from_date=date(2025, 1, 1),
        window_to_date=date(2025, 1, 5),
        database_url=db_url,
    )
    result = await ingest_td_garment_rows(
        rows=[{"order_number": "ORD-OUT", "line_item_key": "LI-NEW", "garment_name": "New"}],
        authoritative_order_scope=[{"order_number": "ORD-IN", "garment_snapshot_outcome": "complete_empty"}],
        store_code="S001",
        cost_center="A001",
        run_id="run-outscope-2",
        run_date=datetime.utcnow(),
        window_from_date=date(2025, 1, 1),
        window_to_date=date(2025, 1, 5),
        database_url=db_url,
    )

    assert result.staging_rows == 1
    assert result.complete_empty_orders == 1
    assert await _line_count(db_url, "ORD-OUT") == 1
