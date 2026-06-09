from __future__ import annotations

import asyncio
import importlib.util
import io
import json
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations

import app.__main__ as app_main

from app.common.db import session_scope
from app.crm_downloader import order_line_items_rebuild as rebuild
from app.crm_downloader.td_orders_sync import main as td_orders_main
from app.crm_downloader.uc_orders_sync import main as uc_orders_main
from app.dashboard_downloader.json_logger import JsonLogger


def _write_td_storage_state(path: Path, *, expires: int = 4_102_444_800) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "cookies": [
                    {
                        "name": "td_session",
                        "value": "valid",
                        "domain": ".tumbledry.in",
                        "path": "/",
                        "expires": expires,
                    }
                ],
                "origins": [],
            }
        ),
        encoding="utf-8",
    )
    return path


def _write_uc_storage_state(
    path: Path, *, token: str | None = "header.payload.signature"
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    value = (
        json.dumps({"accessToken": token}) if token else json.dumps({"other": "value"})
    )
    path.write_text(
        json.dumps(
            {
                "cookies": [],
                "origins": [
                    {
                        "origin": "https://store.ucleanlaundry.com",
                        "localStorage": [{"name": "auth", "value": value}],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


def _load_oli_progress_migration():
    project_root = Path(__file__).resolve().parents[2]
    module_path = project_root / "alembic" / "versions" / "0123_oli_rebuild_progress.py"
    spec = importlib.util.spec_from_file_location(
        "v0123_oli_rebuild_progress_for_rebuild_tests", module_path
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load migration module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_oli_progress_migration = _load_oli_progress_migration()


def _run_oli_progress_migration(
    connection: sa.Connection, fn: Callable[[], None]
) -> None:
    context = MigrationContext.configure(connection)
    operations = Operations(context)
    original_op = _oli_progress_migration.op
    _oli_progress_migration.op = operations
    try:
        fn()
    finally:
        _oli_progress_migration.op = original_op


async def _create_oli_progress_table_from_migration(db_url: str) -> None:
    async with session_scope(db_url) as session:
        connection = await session.connection()
        await connection.run_sync(
            lambda sync_connection: _run_oli_progress_migration(
                sync_connection, _oli_progress_migration.upgrade
            )
        )
        await session.commit()


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
    await _create_oli_progress_table_from_migration(db_url)


async def _rows(db_url: str, sql: str) -> list[Any]:
    async with session_scope(db_url) as session:
        return (await session.execute(sa.text(sql))).all()


@pytest.mark.asyncio
@pytest.mark.parametrize("sync_group", ["td", "TD"])
async def test_load_store_start_dates_matches_sync_group_case_insensitively(
    tmp_path, sync_group: str
) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path / f'start-{sync_group}.sqlite'}"
    async with session_scope(db_url) as session:
        await session.execute(sa.text("""
            CREATE TABLE store_master (
                store_code TEXT, sync_group TEXT, start_date DATE
            )
        """))
        await session.execute(
            sa.text(
                "INSERT INTO store_master (store_code, sync_group, start_date) "
                "VALUES ('TD001', :sync_group, '2025-02-10')"
            ),
            {"sync_group": sync_group},
        )
        await session.commit()

    start_dates = await rebuild._load_store_start_dates(
        database_url=db_url,
        stores=[
            rebuild.RebuildStore(source="td", store_code="TD001", cost_center="CC01")
        ],
    )

    assert start_dates == {("td", "TD001"): date(2025, 2, 10)}


@pytest.mark.asyncio
async def test_ensure_progress_table_requires_alembic_migration(tmp_path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'missing-progress.sqlite'}"

    with pytest.raises(RuntimeError, match="run Alembic migrations"):
        await rebuild._ensure_progress_table(db_url)


@pytest.mark.asyncio
async def test_write_progress_works_against_alembic_created_table(tmp_path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'alembic-progress.sqlite'}"
    await _create_oli_progress_table_from_migration(db_url)

    metrics = rebuild.WindowMetrics(
        source="td",
        store_code="TD001",
        cost_center="CC01",
        window_start=date(2025, 1, 1),
        window_end=date(2025, 1, 1),
        complete_with_rows_orders=2,
        inserted_rows=3,
    )
    await rebuild._write_progress(
        database_url=db_url,
        store=rebuild.RebuildStore(source="td", store_code="TD001", cost_center="CC01"),
        window=rebuild.RebuildWindow(date(2025, 1, 1), date(2025, 1, 1)),
        run_id="alembic-shape",
        status="success",
        attempt_no=1,
        metrics=metrics,
    )

    rows = await _rows(
        db_url,
        "SELECT id, source, store_code, run_id, status, complete_with_rows_orders, inserted_rows "
        "FROM order_line_items_rebuild_progress",
    )
    assert [
        (
            row.id,
            row.source,
            row.store_code,
            row.run_id,
            row.status,
            row.complete_with_rows_orders,
            row.inserted_rows,
        )
        for row in rows
    ] == [(1, "td", "TD001", "alembic-shape", "success", 2, 3)]


def test_rebuild_summary_payload_includes_per_window_and_store_row_counts() -> None:
    metrics = [
        rebuild.WindowMetrics(
            source="td",
            store_code="TD001",
            cost_center="CC01",
            window_start=date(2025, 1, 1),
            window_end=date(2025, 1, 31),
            inspected_orders=5,
            complete_with_rows_orders=4,
            complete_empty_orders=1,
            deleted_rows=7,
            inserted_rows=9,
            orphan_rows=2,
        ),
        rebuild.WindowMetrics(
            source="td",
            store_code="TD001",
            cost_center="CC01",
            window_start=date(2025, 2, 1),
            window_end=date(2025, 2, 28),
            inspected_orders=3,
            complete_with_rows_orders=3,
            deleted_rows=9,
            inserted_rows=11,
            orphan_rows=1,
        ),
    ]

    completed = rebuild._completed_window_payloads(
        successful_windows={
            ("td", "TD001", date(2025, 1, 1), date(2025, 1, 31)),
            ("td", "TD001", date(2025, 2, 1), date(2025, 2, 28)),
        },
        metrics=metrics,
    )
    store_rows = rebuild._aggregate_rebuild_store_rows(completed)

    assert [row["rows_rebuilt"] for row in completed] == [9, 11]
    assert store_rows == [
        {
            "source": "td",
            "store_code": "TD001",
            "cost_center": "CC01",
            "window_count": 2,
            "rows_rebuilt": 20,
            "inserted_rows": 20,
            "deleted_rows": 16,
            "orphan_rows": 3,
            "inspected_orders": 8,
            "complete_with_rows_orders": 7,
            "complete_empty_orders": 1,
            "skipped_incomplete_orders": 0,
        }
    ]


@pytest.fixture
def patch_config_and_stores(monkeypatch: pytest.MonkeyPatch, tmp_path):
    db_url = f"sqlite+aiosqlite:///{tmp_path/'rebuild.sqlite'}"
    monkeypatch.setattr(rebuild, "config", SimpleNamespace(database_url=db_url))

    td_state = _write_td_storage_state(
        tmp_path / "profiles" / "TD001_storage_state.json"
    )
    uc_state = _write_uc_storage_state(
        tmp_path / "profiles" / "UC001_storage_state.json"
    )

    async def load_stores(*, sources, store_codes, logger):
        stores = []
        if "td" in sources:
            stores.append(
                rebuild.RebuildStore(
                    source="td",
                    store_code="TD001",
                    cost_center="CC01",
                    raw_store=SimpleNamespace(storage_state_path=td_state),
                )
            )
        if "uc" in sources:
            stores.append(
                rebuild.RebuildStore(
                    source="uc",
                    store_code="UC001",
                    cost_center="CC01",
                    raw_store=SimpleNamespace(storage_state_path=uc_state),
                )
            )
        return stores

    async def td_auth_ready(store, *, run_id, logger):
        return "session_valid"

    monkeypatch.setattr(rebuild, "load_rebuild_stores", load_stores)
    monkeypatch.setattr(rebuild, "_td_api_auth_readiness_status", td_auth_ready)
    return db_url


class _InMemoryLogger:
    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        self.default_context = {"run_id": run_id}
        self.events: list[dict[str, Any]] = []

    def info(
        self, *, phase: str, status: str = "ok", message: str = "", **fields: Any
    ) -> None:
        self.events.append(
            {
                **self.default_context,
                "phase": phase,
                "status": status,
                "message": message,
                **fields,
            }
        )


def _event_run_ids(events: list[dict[str, Any]], messages: set[str]) -> list[str]:
    return [
        str(event.get("run_id")) for event in events if event.get("message") in messages
    ]


class _FakeAsyncPlaywright:
    def __init__(self, playwright: Any) -> None:
        self._playwright = playwright

    async def __aenter__(self) -> Any:
        return self._playwright

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


class _FakeBrowser:
    def __init__(self) -> None:
        self.contexts: list[_FakeContext] = []
        self.closed = False

    async def new_context(self, **kwargs: Any) -> "_FakeContext":
        context = _FakeContext(**kwargs)
        self.contexts.append(context)
        return context

    async def close(self) -> None:
        self.closed = True


class _FakeContext:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.pages: list[_FakePage] = []
        self.storage_state_paths: list[str] = []

    async def new_page(self) -> "_FakePage":
        page = _FakePage()
        self.pages.append(page)
        return page

    async def storage_state(self, *, path: str) -> dict[str, Any]:
        self.storage_state_paths.append(path)
        return {"cookies": [], "origins": []}


class _FakePage:
    def __init__(self) -> None:
        self.goto_calls: list[tuple[str, str | None]] = []
        self.url = "https://subs.quickdrycleaning.com/td001/App/home"

    async def goto(self, url: str, *, wait_until: str | None = None) -> None:
        self.goto_calls.append((url, wait_until))
        self.url = url


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["td", "uc"])
async def test_default_fetch_snapshot_launches_browser_with_keyword_arguments(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source: rebuild.Source
) -> None:
    playwright = SimpleNamespace(name=f"{source}-playwright")
    browser = _FakeBrowser()
    logger = SimpleNamespace(name=f"{source}-logger")
    launch_calls: list[tuple[Any, Any]] = []
    prepare_calls: list[dict[str, Any]] = []
    uc_prepare_calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        "playwright.async_api.async_playwright",
        lambda: _FakeAsyncPlaywright(playwright),
    )

    async def fake_launch_browser(*, playwright: Any, logger: Any) -> _FakeBrowser:
        launch_calls.append((playwright, logger))
        return browser

    monkeypatch.setattr(rebuild, "launch_browser", fake_launch_browser)

    async def fake_fetch_td_source_snapshot(**kwargs: Any) -> Any:
        prepare_calls.append(kwargs)
        return rebuild.TdSourceSnapshotFetchResult(
            api_fetch_result=td_orders_main.TdApiFetchResult(source_fetch_status=""),
            garments_rows=[],
            garment_order_snapshots=[],
            endpoint_health={},
            source_fetch_status="unknown",
            failure_class=None,
            source_fetch_error_class=None,
            request_metadata=[],
            endpoint_errors={},
            endpoint_error_diagnostics={},
            report_iframe_src="https://reports.quickdrycleaning.com/r",
        )

    async def fake_prepare_uc_api_page_for_store(**kwargs: Any) -> Any:
        uc_prepare_calls.append(kwargs)
        context = await kwargs["browser"].new_context(storage_state="prepared")
        page = await context.new_page()
        return uc_orders_main.UcApiPagePreparationResult(
            ok=True,
            message="ready",
            context=context,
            page=page,
            login_used=False,
            session_probe_result=True,
            fallback_login_attempted=False,
            fallback_login_result=None,
        )

    async def fake_collect_gst_orders_via_api(**kwargs: Any) -> Any:
        assert kwargs["page"] is uc_prepare_calls[0]["browser"].contexts[0].pages[0]
        return SimpleNamespace(
            gst_rows=[],
            base_rows=[],
            order_detail_rows=[],
            payment_detail_rows=[],
            order_detail_snapshot_rows=[],
            skipped_order_counters={},
            skipped_order_codes=[],
            booking_lookup_hits=0,
            booking_lookup_misses=0,
            delivered_rows_scanned=0,
            source_fetch_status="success",
            extractor_status="success",
        )

    monkeypatch.setattr(
        rebuild, "fetch_td_source_snapshot", fake_fetch_td_source_snapshot
    )
    monkeypatch.setattr(
        rebuild, "collect_gst_orders_via_api", fake_collect_gst_orders_via_api
    )
    monkeypatch.setattr(
        rebuild, "prepare_uc_api_page_for_store", fake_prepare_uc_api_page_for_store
    )

    snapshot = await rebuild.default_fetch_snapshot(
        source=source,
        store=rebuild.RebuildStore(
            source=source,
            store_code=f"{source.upper()}001",
            cost_center="CC01",
            raw_store=SimpleNamespace(
                home_url="https://example.test/home",
                storage_state_path=tmp_path / f"{source}.json",
            ),
        ),
        window=rebuild.RebuildWindow(date(2025, 1, 1), date(2025, 1, 2)),
        run_id=f"{source}-run",
        logger=logger,
    )

    expected_snapshot = rebuild.SourceSnapshot(line_item_rows=[], order_snapshots=[])
    if source == "uc":
        expected_snapshot.zero_snapshot_class = "confirmed_source_empty"
        expected_snapshot.uc_diagnostics = rebuild.UcSourceSnapshotDiagnostics(
            source_fetch_status="success",
            extractor_status="success",
        )
    assert snapshot == expected_snapshot
    if source == "td":
        assert launch_calls == []
        assert len(prepare_calls) == 1
        assert prepare_calls[0]["store"].storage_state_path == tmp_path / "td.json"
        assert (
            prepare_calls[0]["source_config"].context_source
            == "order_line_items_rebuild"
        )
        assert browser.closed is False
    else:
        assert launch_calls == [(playwright, logger)]
        assert prepare_calls == []
        assert len(uc_prepare_calls) == 1
        assert uc_prepare_calls[0]["source"] == "order_line_items_rebuild"
        assert uc_prepare_calls[0]["store"].storage_state_path == tmp_path / "uc.json"
        assert browser.closed is True


async def _fetch_uc_default_snapshot_with_extract(
    *,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    extract: Any,
    logger: Any | None = None,
) -> rebuild.SourceSnapshot:
    playwright = SimpleNamespace(name="uc-playwright")
    browser = _FakeBrowser()
    monkeypatch.setattr(
        "playwright.async_api.async_playwright",
        lambda: _FakeAsyncPlaywright(playwright),
    )

    async def fake_launch_browser(*, playwright: Any, logger: Any) -> _FakeBrowser:
        return browser

    async def fake_prepare_uc_api_page_for_store(**kwargs: Any) -> Any:
        context = await kwargs["browser"].new_context(storage_state="prepared")
        page = await context.new_page()
        return uc_orders_main.UcApiPagePreparationResult(
            ok=True,
            message="ready",
            context=context,
            page=page,
            login_used=False,
            session_probe_result=True,
            fallback_login_attempted=False,
            fallback_login_result=None,
        )

    async def fake_collect_gst_orders_via_api(**_kwargs: Any) -> Any:
        return extract

    monkeypatch.setattr(rebuild, "launch_browser", fake_launch_browser)
    monkeypatch.setattr(
        rebuild, "prepare_uc_api_page_for_store", fake_prepare_uc_api_page_for_store
    )
    monkeypatch.setattr(
        rebuild, "collect_gst_orders_via_api", fake_collect_gst_orders_via_api
    )

    return await rebuild.default_fetch_snapshot(
        source="uc",
        store=rebuild.RebuildStore(
            source="uc",
            store_code="UC001",
            cost_center="CC01",
            raw_store=SimpleNamespace(
                home_url="https://example.test/home",
                storage_state_path=tmp_path / "uc.json",
            ),
        ),
        window=rebuild.RebuildWindow(date(2025, 1, 1), date(2025, 1, 2)),
        run_id="uc-run",
        logger=logger or SimpleNamespace(name="uc-logger"),
    )


def _uc_extract(**overrides: Any) -> SimpleNamespace:
    values = {
        "gst_rows": [],
        "base_rows": [],
        "order_detail_rows": [],
        "payment_detail_rows": [],
        "order_detail_snapshot_rows": [],
        "skipped_order_counters": {},
        "skipped_order_codes": [],
        "booking_lookup_hits": 0,
        "booking_lookup_misses": 0,
        "delivered_rows_scanned": 0,
        "source_fetch_status": "success",
        "extractor_status": "success",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.mark.asyncio
async def test_uc_default_fetch_snapshot_empty_gst_api_response_confirmed_source_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    snapshot = await _fetch_uc_default_snapshot_with_extract(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        extract=_uc_extract(),
    )

    assert snapshot.zero_snapshot_class == "confirmed_source_empty"
    assert snapshot.uc_diagnostics == rebuild.UcSourceSnapshotDiagnostics(
        source_fetch_status="success", extractor_status="success"
    )


@pytest.mark.asyncio
async def test_uc_default_fetch_snapshot_gst_api_failed_is_not_confirmed_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    snapshot = await _fetch_uc_default_snapshot_with_extract(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        extract=_uc_extract(
            skipped_order_counters={"gst_api_failed": 1},
            skipped_order_codes=["store:UC001"],
            source_fetch_status="failed",
            extractor_status="failed",
        ),
    )

    assert snapshot.zero_snapshot_class == "source_fetch_auth_failure"
    assert snapshot.source_fetch_error_class == "source_fetch_auth_failure"
    assert snapshot.uc_diagnostics is not None
    assert snapshot.uc_diagnostics.skipped_order_counters == {"gst_api_failed": 1}


@pytest.mark.asyncio
async def test_uc_default_fetch_snapshot_gst_api_invalid_data_is_not_confirmed_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    snapshot = await _fetch_uc_default_snapshot_with_extract(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        extract=_uc_extract(
            skipped_order_counters={"gst_api_invalid_data": 1},
            skipped_order_codes=["store:UC001"],
            source_fetch_status="invalid_data",
            extractor_status="failed",
        ),
    )

    assert snapshot.zero_snapshot_class == "source_fetch_auth_failure"
    assert snapshot.source_fetch_error_class == "source_fetch_auth_failure"
    assert snapshot.uc_diagnostics is not None
    assert snapshot.uc_diagnostics.skipped_order_counters == {"gst_api_invalid_data": 1}


@pytest.mark.asyncio
async def test_uc_rebuild_non_empty_extract_inspects_orders_and_logs_diagnostics(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    logger = _InMemoryLogger("uc-non-empty-diagnostics")

    async def fetcher(**_kwargs: Any) -> rebuild.SourceSnapshot:
        return rebuild.SourceSnapshot(
            line_item_rows=[
                {
                    "order_number": "ORD-UC-1",
                    "line_hash": "hash-uc-1",
                    "item_name": "Shirt",
                    "service": "Wash",
                    "quantity": 1,
                }
            ],
            order_snapshots=[
                {
                    "order_number": "ORD-UC-1",
                    "snapshot_outcome": "complete_with_rows",
                    "detail_row_count": 1,
                }
            ],
            zero_snapshot_class="unknown_ambiguous_empty",
            uc_diagnostics=rebuild.UcSourceSnapshotDiagnostics(
                gst_rows_count=1,
                base_rows_count=1,
                order_detail_rows_count=1,
                order_detail_snapshot_rows_count=1,
                booking_lookup_hits=1,
                delivered_rows_scanned=2,
                source_fetch_status="success",
                extractor_status="success",
            ),
        )

    metrics = await rebuild.run_rebuild(
        source_selection="uc",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=True,
        run_id="uc-non-empty-diagnostics",
        logger=logger,
        fetch_snapshot=fetcher,
        skip_auth_preflight=True,
    )

    assert metrics[0].inspected_orders == 1
    assert metrics[0].zero_snapshot_class is None
    checkpoint_event = [
        event
        for event in logger.events
        if event.get("phase") == "order_line_items_rebuild_window"
    ][-1]
    assert checkpoint_event["uc_extraction_diagnostics"] == {
        "gst_rows_count": 1,
        "base_rows_count": 1,
        "order_detail_rows_count": 1,
        "payment_detail_rows_count": 0,
        "order_detail_snapshot_rows_count": 1,
        "skipped_order_counters": {},
        "skipped_order_codes": [],
        "booking_lookup_hits": 1,
        "booking_lookup_misses": 0,
        "delivered_rows_scanned": 2,
        "source_fetch_status": "success",
        "extractor_status": "success",
    }


@pytest.mark.asyncio
async def test_default_fetch_snapshot_raises_on_td_unauthorized_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger = SimpleNamespace(name="td-logger")

    async def fake_fetch_td_source_snapshot(**_kwargs: Any) -> Any:
        api_result = td_orders_main.TdApiFetchResult(
            endpoint_errors={"/garments/details": "http_401"},
            endpoint_health={
                "/garments/details": {
                    "success": False,
                    "final_error_class": "http_401",
                }
            },
            source_fetch_status="auth_failed",
            source_fetch_error_class="http_401",
            source_fetch_failed_endpoints=["/garments/details"],
        )
        return rebuild.TdSourceSnapshotFetchResult(
            api_fetch_result=api_result,
            garments_rows=[],
            garment_order_snapshots=[],
            endpoint_health=api_result.endpoint_health,
            source_fetch_status=api_result.source_fetch_status,
            failure_class="store_auth_failure",
            source_fetch_error_class=api_result.source_fetch_error_class,
            request_metadata=[],
            endpoint_errors=api_result.endpoint_errors,
            endpoint_error_diagnostics={},
        )

    monkeypatch.setattr(
        rebuild, "fetch_td_source_snapshot", fake_fetch_td_source_snapshot
    )

    with pytest.raises(rebuild.TdApiUnauthorizedError, match="TD API unauthorized"):
        await rebuild.default_fetch_snapshot(
            source="td",
            store=rebuild.RebuildStore(
                source="td",
                store_code="TD001",
                cost_center="CC01",
                raw_store=SimpleNamespace(storage_state_path=Path()),
            ),
            window=rebuild.RebuildWindow(date(2025, 1, 1), date(2025, 1, 1)),
            run_id="td-unauthorized",
            logger=logger,
        )


@pytest.mark.asyncio
async def test_prepare_td_api_context_invalid_storage_state_logs_in_and_refreshes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[dict[str, Any]] = []
    call_order: list[str] = []
    browser = _FakeBrowser()
    logger = SimpleNamespace(name="logger")

    monkeypatch.setattr(td_orders_main, "default_profiles_dir", lambda: tmp_path)
    monkeypatch.setattr(
        td_orders_main,
        "log_event",
        lambda **kwargs: events.append(kwargs),
    )

    store = td_orders_main.TdStore(
        store_code="TD001",
        store_name="TD001",
        cost_center="CC01",
        sync_config={},
    )
    _write_td_storage_state(store.storage_state_path)

    async def fake_probe(
        *_args: Any, **_kwargs: Any
    ) -> td_orders_main.SessionProbeResult:
        call_order.append("probe")
        return td_orders_main.SessionProbeResult(
            valid=False,
            final_url="https://subs.quickdrycleaning.com/td001/App/Login",
            reason="login_form_visible",
            verification_seen=False,
        )

    async def fake_login(*_args: Any, **_kwargs: Any) -> bool:
        call_order.append("login")
        return True

    async def fake_otp(*_args: Any, **_kwargs: Any) -> tuple[bool, bool]:
        call_order.append("otp")
        return True, False

    async def fake_home(*_args: Any, **_kwargs: Any) -> bool:
        call_order.append("home")
        return True

    async def fake_nav(*_args: Any, **_kwargs: Any) -> bool:
        call_order.append("navigate")
        return True

    async def fake_iframe(*_args: Any, **_kwargs: Any) -> tuple[object, str]:
        call_order.append("iframe")
        return object(), "https://reports.quickdrycleaning.com/orders"

    async def fake_resolve(*_args: Any, **_kwargs: Any) -> str:
        call_order.append("resolve")
        return "https://reports.quickdrycleaning.com/orders?auth=1"

    monkeypatch.setattr(td_orders_main, "_probe_session", fake_probe)
    monkeypatch.setattr(td_orders_main, "_perform_login", fake_login)
    monkeypatch.setattr(td_orders_main, "_wait_for_otp_verification", fake_otp)
    monkeypatch.setattr(td_orders_main, "_wait_for_home", fake_home)
    monkeypatch.setattr(td_orders_main, "_navigate_to_orders_container", fake_nav)
    monkeypatch.setattr(td_orders_main, "_wait_for_iframe", fake_iframe)
    monkeypatch.setattr(
        td_orders_main, "_resolve_report_iframe_auth_source_for_api", fake_resolve
    )

    api_context = await td_orders_main.prepare_td_api_context_for_store(
        browser=browser,
        store=store,
        logger=logger,
        run_id="invalid-storage-refresh",
        run_start_date=date(2025, 1, 1),
        run_end_date=date(2025, 1, 2),
        nav_timeout_ms=10,
    )

    assert call_order == [
        "probe",
        "login",
        "otp",
        "home",
        "navigate",
        "iframe",
        "resolve",
    ]
    assert api_context.login_performed is True
    assert (
        api_context.report_iframe_src
        == "https://reports.quickdrycleaning.com/orders?auth=1"
    )
    assert browser.contexts[0].kwargs["storage_state"] == str(store.storage_state_path)
    assert browser.contexts[0].storage_state_paths == [str(store.storage_state_path)]
    assert any(
        event.get("message") == "Storage state probe invalid; performing login"
        for event in events
    )


@pytest.mark.asyncio
async def test_default_fetch_snapshot_successful_td_garments_response_is_authoritative(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    logger = SimpleNamespace(name="td-logger")

    async def fake_fetch_td_source_snapshot(**_kwargs: Any) -> Any:
        api_result = td_orders_main.TdApiFetchResult(
            garments_rows=[{"order_number": "ORD-1", "garment_name": "Shirt"}],
            garment_order_snapshots=[
                {
                    "order_number": "ORD-1",
                    "garment_snapshot_outcome": "complete_with_rows",
                }
            ],
            endpoint_health={
                "/garments/details": {
                    "success": True,
                    "garments_fetch_completeness": "complete",
                }
            },
        )
        return rebuild.TdSourceSnapshotFetchResult(
            api_fetch_result=api_result,
            garments_rows=api_result.garments_rows,
            garment_order_snapshots=api_result.garment_order_snapshots,
            endpoint_health=api_result.endpoint_health,
            source_fetch_status=api_result.source_fetch_status,
            failure_class=None,
            source_fetch_error_class=None,
            request_metadata=[],
            endpoint_errors={},
            endpoint_error_diagnostics={},
            report_iframe_src="https://reports.quickdrycleaning.com/orders?auth=1",
        )

    monkeypatch.setattr(
        rebuild, "fetch_td_source_snapshot", fake_fetch_td_source_snapshot
    )

    snapshot = await rebuild.default_fetch_snapshot(
        source="td",
        store=rebuild.RebuildStore(
            source="td",
            store_code="TD001",
            cost_center="CC01",
            raw_store=SimpleNamespace(storage_state_path=tmp_path / "td.json"),
        ),
        window=rebuild.RebuildWindow(date(2025, 1, 1), date(2025, 1, 1)),
        run_id="td-garments-success",
        logger=logger,
    )

    assert len(snapshot.line_item_rows) == 1
    assert len(snapshot.order_snapshots) == 1
    assert (
        snapshot.order_snapshots[0]["garment_snapshot_outcome"] == "complete_with_rows"
    )


@pytest.mark.asyncio
async def test_td_unauthorized_does_not_create_successful_window_metrics(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    logger = _InMemoryLogger("unauthorized-window")

    async def fetcher(**kwargs):
        raise rebuild.TdApiUnauthorizedError(
            store_code=kwargs["store"].store_code,
            failed_endpoints=["/garments/details"],
            error_class="http_401",
        )

    with pytest.raises(rebuild.OrderLineItemsRebuildIncomplete) as exc_info:
        await rebuild.run_rebuild(
            source_selection="td",
            store_codes=None,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 1),
            window_size_days=1,
            dry_run=True,
            run_id="unauthorized-window",
            logger=logger,
            fetch_snapshot=fetcher,
        )

    assert exc_info.value.completed_window_count == 0
    assert exc_info.value.missing_windows == ("td:TD001:2025-01-01..2025-01-01",)
    failed_events = [
        event
        for event in logger.events
        if event.get("phase") == "order_line_items_rebuild_window"
        and event.get("window_start") == "2025-01-01"
    ]
    assert failed_events[-1]["status"] == "error"
    assert not any(event.get("status") == "ok" for event in failed_events)


@pytest.mark.asyncio
async def test_td_unauthorized_window_is_reported_missing(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    logger = _InMemoryLogger("unauthorized-missing")

    async def fetcher(**kwargs):
        raise rebuild.TdApiUnauthorizedError(
            store_code="TD001",
            failed_endpoints=["/reports/order-report", "/garments/details"],
            error_class="http_401",
        )

    with pytest.raises(rebuild.OrderLineItemsRebuildIncomplete):
        await rebuild.run_rebuild(
            source_selection="td",
            store_codes=None,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 1),
            window_size_days=1,
            dry_run=True,
            run_id="unauthorized-missing",
            logger=logger,
            fetch_snapshot=fetcher,
        )

    missing_event = [
        event
        for event in logger.events
        if event.get("phase") == "order_line_items_rebuild_missing_windows"
    ][-1]
    assert missing_event["status"] == "warning"
    assert missing_event["missing_window_count"] == 1
    assert missing_event["missing_windows"] == [
        {
            "source": "td",
            "store_code": "TD001",
            "window_start": "2025-01-01",
            "window_end": "2025-01-01",
        }
    ]



@pytest.mark.asyncio
async def test_run_rebuild_persists_summary_before_notifications_for_zero_snapshot_warning(
    patch_config_and_stores, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    calls: list[tuple[str, Any]] = []

    async def fake_insert_run_summary(database_url: str, summary: dict[str, Any]) -> None:
        calls.append(("summary", {"database_url": database_url, "summary": summary}))

    async def fake_send_notifications_for_run(pipeline_name: str, run_id: str) -> dict[str, Any]:
        calls.append(
            (
                "notification",
                {"pipeline_name": pipeline_name, "run_id": run_id},
            )
        )
        return {"sent": 1}

    async def fetcher(**_kwargs: Any) -> rebuild.SourceSnapshot:
        return rebuild.SourceSnapshot(line_item_rows=[], order_snapshots=[])

    monkeypatch.setattr(rebuild, "insert_run_summary", fake_insert_run_summary)
    monkeypatch.setattr(
        rebuild, "send_notifications_for_run", fake_send_notifications_for_run
    )

    metrics = await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=True,
        run_id="summary-before-notify",
        logger=_InMemoryLogger("summary-before-notify"),
        fetch_snapshot=fetcher,
    )

    assert len(metrics) == 1
    assert [name for name, _payload in calls] == ["summary", "notification"]
    summary = calls[0][1]["summary"]
    notification = calls[1][1]
    assert summary["pipeline_name"] == "order_line_items_rebuild"
    assert summary["run_id"] == "summary-before-notify"
    assert summary["overall_status"] in {"success", "warning"}
    assert summary["overall_status"] == "warning"
    assert "notification_payload" in summary["metrics_json"]
    assert summary["metrics_json"]["notification_payload"]["pipeline_name"] == (
        "order_line_items_rebuild"
    )
    assert summary["metrics_json"]["notification_payload"]["run_id"] == (
        "summary-before-notify"
    )
    assert summary["metrics_json"]["warnings"] == [
        "1 suspicious zero snapshot window(s) detected"
    ]
    payload = summary["metrics_json"]["notification_payload"]
    assert payload["warnings"] == ["1 suspicious zero snapshot window(s) detected"]
    assert payload["completed_windows"] == [
        {
            "source": "td",
            "store_code": "TD001",
            "cost_center": "CC01",
            "window_start": "2025-01-01",
            "window_end": "2025-01-01",
            "dry_run": True,
            "zero_snapshot_class": "unknown_ambiguous_empty",
            "inspected_orders": 0,
            "complete_with_rows_orders": 0,
            "complete_empty_orders": 0,
            "skipped_incomplete_orders": 0,
            "deleted_rows": 0,
            "inserted_rows": 0,
            "orphan_rows": 0,
            "window_status": "completed",
            "rows_rebuilt": 0,
        }
    ]
    assert payload["store_rows"] == [
        {
            "source": "td",
            "store_code": "TD001",
            "cost_center": "CC01",
            "window_count": 1,
            "rows_rebuilt": 0,
            "inserted_rows": 0,
            "deleted_rows": 0,
            "orphan_rows": 0,
            "inspected_orders": 0,
            "complete_with_rows_orders": 0,
            "complete_empty_orders": 0,
            "skipped_incomplete_orders": 0,
        }
    ]
    assert notification == {
        "pipeline_name": "order_line_items_rebuild",
        "run_id": "summary-before-notify",
    }


@pytest.mark.asyncio
async def test_run_rebuild_failure_persists_failed_summary_and_notifies_before_raising(
    patch_config_and_stores, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    calls: list[tuple[str, Any]] = []

    async def fake_insert_run_summary(database_url: str, summary: dict[str, Any]) -> None:
        calls.append(("summary", {"database_url": database_url, "summary": summary}))

    async def fake_send_notifications_for_run(pipeline_name: str, run_id: str) -> dict[str, Any]:
        calls.append(
            (
                "notification",
                {"pipeline_name": pipeline_name, "run_id": run_id},
            )
        )
        return {"sent": 1}

    async def fetcher(**_kwargs: Any) -> rebuild.SourceSnapshot:
        raise ValueError("deterministic fetch failure")

    monkeypatch.setattr(rebuild, "insert_run_summary", fake_insert_run_summary)
    monkeypatch.setattr(
        rebuild, "send_notifications_for_run", fake_send_notifications_for_run
    )

    with pytest.raises(rebuild.OrderLineItemsRebuildIncomplete):
        await rebuild.run_rebuild(
            source_selection="td",
            store_codes=None,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 1),
            window_size_days=1,
            dry_run=True,
            run_id="failure-summary-notify",
            logger=_InMemoryLogger("failure-summary-notify"),
            fetch_snapshot=fetcher,
        )

    assert [name for name, _payload in calls] == ["summary", "notification"]
    summary = calls[0][1]["summary"]
    assert summary["pipeline_name"] == "order_line_items_rebuild"
    assert summary["run_id"] == "failure-summary-notify"
    assert summary["overall_status"] == "failed"
    assert summary["metrics_json"]["missing_window_count"] == 1
    assert summary["metrics_json"]["notification_payload"]["overall_status"] == "failed"
    assert summary["metrics_json"]["warnings"]
    assert "OrderLineItemsRebuildIncomplete" in summary["metrics_json"]["warnings"][0]
    assert calls[1][1] == {
        "pipeline_name": "order_line_items_rebuild",
        "run_id": "failure-summary-notify",
    }


def test_rebuild_cli_preserves_nonzero_exit_after_notification_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    async def fake_insert_run_summary(database_url: str, summary: dict[str, Any]) -> None:
        calls.append("summary")

    async def fake_send_notifications_for_run(pipeline_name: str, run_id: str) -> dict[str, Any]:
        calls.append("notification")
        return {"sent": 1}

    async def fake_run_rebuild(**kwargs: Any) -> list[rebuild.WindowMetrics]:
        await rebuild._persist_rebuild_summary_and_notify(
            database_url="sqlite+aiosqlite:///:memory:",
            logger=_InMemoryLogger(str(kwargs.get("run_id"))),
            run_id=str(kwargs.get("run_id")),
            run_env="test",
            source_selection=str(kwargs["source_selection"]),
            sources=["td"],
            selected_stores=["TD001"],
            dry_run=bool(kwargs["dry_run"]),
            resume=bool(kwargs["resume"]),
            resume_run_id=kwargs.get("resume_run_id"),
            started_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
            report_date=date(2025, 1, 1),
            expected_windows={("td", "TD001", date(2025, 1, 1), date(2025, 1, 1))},
            successful_windows=set(),
            missing_windows=[("td", "TD001", date(2025, 1, 1), date(2025, 1, 1))],
            skipped_windows=[],
            metrics=[],
            final_status="failed",
            warnings=["synthetic CLI failure"],
        )
        raise rebuild.OrderLineItemsRebuildIncomplete(
            run_id=str(kwargs.get("run_id")),
            expected_window_count=1,
            completed_window_count=0,
            missing_window_count=1,
            missing_windows=("td:TD001:2025-01-01..2025-01-01",),
        )

    monkeypatch.setattr(rebuild, "insert_run_summary", fake_insert_run_summary)
    monkeypatch.setattr(
        rebuild, "send_notifications_for_run", fake_send_notifications_for_run
    )
    monkeypatch.setattr(rebuild, "run_rebuild", fake_run_rebuild)

    with pytest.raises(SystemExit) as exc_info:
        rebuild.run(
            [
                "--source",
                "td",
                "--start-date",
                "2025-01-01",
                "--end-date",
                "2025-01-01",
                "--window-size",
                "1",
                "--dry-run",
                "--run-id",
                "cli-notified-failure",
            ]
        )

    assert calls == ["summary", "notification"]
    assert exc_info.value.code == 1

def test_cli_exits_nonzero_when_td_garments_auth_failure_would_be_zero_row_window(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'cli-unauthorized.sqlite'}"
    asyncio.run(_create_common_tables(db_url))
    monkeypatch.setattr(rebuild, "config", SimpleNamespace(database_url=db_url))
    monkeypatch.setattr(
        "playwright.async_api.async_playwright",
        lambda: _FakeAsyncPlaywright(SimpleNamespace(name="td-playwright")),
    )
    emitted_loggers: list[_InMemoryLogger] = []

    def fake_get_logger(run_id: str | None = None) -> _InMemoryLogger:
        logger = _InMemoryLogger(str(run_id))
        emitted_loggers.append(logger)
        return logger

    async def load_stores(*, sources, store_codes, logger):
        return [
            rebuild.RebuildStore(
                source="td",
                store_code="TD001",
                cost_center="CC01",
                raw_store=SimpleNamespace(storage_state_path=Path()),
            )
        ]

    async def fake_launch_browser(*, playwright: Any, logger: Any) -> _FakeBrowser:
        return _FakeBrowser()

    class FakeTdApiClient:
        def __init__(self, **kwargs: Any) -> None:
            pass

        async def fetch_reports(self, **kwargs: Any) -> Any:
            # Regression shape from the operator log: the garments source endpoint
            # is unauthorized, but row arrays are empty. Rebuild must treat this
            # as a failed/missing window, not as a successful zero-row snapshot.
            return SimpleNamespace(
                garments_rows=[],
                garment_order_snapshots=[],
                endpoint_errors={"/garments/details": "http_401"},
                endpoint_health={
                    "/garments/details": {
                        "success": False,
                        "final_error_class": "http_401",
                        "attempts": 2,
                    }
                },
                source_fetch_status="auth_failed",
                source_fetch_error_class="http_401",
                source_fetch_failed_endpoints=["/garments/details"],
            )

    async def fake_prepare_td_api_context_for_store(**kwargs: Any) -> Any:
        context = await kwargs["browser"].new_context(storage_state=None)
        return SimpleNamespace(
            context=context,
            report_iframe_src="https://reports.quickdrycleaning.com/r",
        )

    monkeypatch.setattr(rebuild, "get_logger", fake_get_logger)
    monkeypatch.setattr(rebuild, "load_rebuild_stores", load_stores)
    monkeypatch.setattr(rebuild, "launch_browser", fake_launch_browser)
    monkeypatch.setattr(rebuild, "TdApiClient", FakeTdApiClient)
    monkeypatch.setattr(
        rebuild,
        "prepare_td_api_context_for_store",
        fake_prepare_td_api_context_for_store,
    )

    with pytest.raises(SystemExit) as exc_info:
        rebuild.run(
            [
                "--source",
                "td",
                "--start-date",
                "2025-01-01",
                "--end-date",
                "2025-01-01",
                "--window-size",
                "1",
                "--dry-run",
                "--skip-auth-preflight",
                "--run-id",
                "cli-unauthorized",
            ]
        )

    assert exc_info.value.code == 1
    assert emitted_loggers
    events = emitted_loggers[-1].events
    assert not any(
        event.get("phase") == "order_line_items_rebuild_window"
        and event.get("status") == "ok"
        and event.get("inspected_orders") == 0
        for event in events
    )
    missing_summary = [
        event
        for event in events
        if event.get("phase") == "order_line_items_rebuild_missing_windows"
    ][-1]
    assert missing_summary["missing_window_count"] > 0


def test_bounded_window_progression() -> None:
    windows = rebuild.iter_windows(date(2025, 1, 1), date(2025, 1, 10), 4)
    assert [(w.start, w.end) for w in windows] == [
        (date(2025, 1, 1), date(2025, 1, 4)),
        (date(2025, 1, 5), date(2025, 1, 8)),
        (date(2025, 1, 9), date(2025, 1, 10)),
    ]


@pytest.mark.asyncio
async def test_preflight_missing_store_master_start_date_fails_before_windows(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    async with session_scope(db_url) as session:
        await session.execute(sa.text("""
            CREATE TABLE store_master (
                store_code TEXT,
                sync_group TEXT,
                start_date TEXT
            )
        """))
        await session.execute(
            sa.text(
                "INSERT INTO store_master (store_code, sync_group, start_date) "
                "VALUES ('TD001', 'TD', NULL)"
            )
        )
        await session.commit()
    logger = _InMemoryLogger("preflight-missing-start")
    window_seen = False

    async def fetcher(**_kwargs):
        nonlocal window_seen
        window_seen = True
        return rebuild.SourceSnapshot()

    with pytest.raises(RuntimeError, match="store_master.start_date"):
        await rebuild.run_rebuild(
            source_selection="td",
            store_codes=None,
            start_date=None,
            end_date=date(2025, 1, 1),
            window_size_days=1,
            dry_run=True,
            run_id="preflight-missing-start",
            logger=logger,
            fetch_snapshot=fetcher,
        )

    assert window_seen is False
    preflight_errors = [
        event
        for event in logger.events
        if event.get("phase") == "order_line_items_rebuild_preflight"
        and event.get("status") == "error"
    ]
    assert preflight_errors
    assert preflight_errors[-1]["missing_start_dates"] == [
        {"source": "td", "store_code": "TD001", "cost_center": "CC01"}
    ]
    assert preflight_errors[-1]["errors"] == [
        "store_master.start_date is required for full-live rebuild when "
        "--start-date is omitted; missing start dates: "
        "td:TD001 cost_center=CC01"
    ]
    assert any(
        event.get("message") == "store_master.start_date is missing for selected stores"
        and event.get("missing_start_dates")
        == [{"source": "td", "store_code": "TD001", "cost_center": "CC01"}]
        for event in preflight_errors
    )


@pytest.mark.asyncio
async def test_preflight_browser_launch_contract_failure_fails_before_windows(
    patch_config_and_stores, monkeypatch: pytest.MonkeyPatch
) -> None:
    logger = _InMemoryLogger("preflight-browser-failure")
    window_seen = False

    async def failing_launch_browser(**_kwargs):
        raise RuntimeError("launch_browser contract broke")

    async def fetcher(**_kwargs):
        nonlocal window_seen
        window_seen = True
        return rebuild.SourceSnapshot()

    monkeypatch.setattr(rebuild, "launch_browser", failing_launch_browser)

    with pytest.raises(RuntimeError, match="browser launch contract failed"):
        await rebuild.run_rebuild(
            source_selection="td",
            store_codes=None,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 1),
            window_size_days=1,
            dry_run=True,
            run_id="preflight-browser-failure",
            logger=logger,
            fetch_snapshot=fetcher,
        )

    assert window_seen is False
    assert [
        event["status"]
        for event in logger.events
        if event.get("phase") == "order_line_items_rebuild_preflight"
    ][-1] == "error"


@pytest.mark.asyncio
async def test_valid_preflight_proceeds_to_window_processing(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    logger = _InMemoryLogger("preflight-valid")
    seen_windows: list[tuple[date, date]] = []

    async def fetcher(**kwargs):
        seen_windows.append((kwargs["window"].start, kwargs["window"].end))
        return rebuild.SourceSnapshot()

    metrics = await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=True,
        run_id="preflight-valid",
        logger=logger,
        fetch_snapshot=fetcher,
    )

    assert len(metrics) == 1
    assert seen_windows == [(date(2025, 1, 1), date(2025, 1, 1))]
    assert any(
        event.get("phase") == "order_line_items_rebuild_preflight"
        and event.get("message") == "order_line_items rebuild preflight completed"
        for event in logger.events
    )


@pytest.mark.asyncio
async def test_preflight_logs_warning_for_missing_storage_state(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'storage-warning.sqlite'}"
    await _create_common_tables(db_url)
    missing_state = tmp_path / "profiles" / "TD001_storage_state.json"
    monkeypatch.setattr(rebuild, "config", SimpleNamespace(database_url=db_url))

    async def load_stores(*, sources, store_codes, logger):
        return [
            rebuild.RebuildStore(
                source="td",
                store_code="TD001",
                cost_center="CC01",
                raw_store=SimpleNamespace(storage_state_path=missing_state),
            )
        ]

    monkeypatch.setattr(rebuild, "load_rebuild_stores", load_stores)
    logger = _InMemoryLogger("preflight-storage-warning")

    async def fetcher(**_kwargs):
        return rebuild.SourceSnapshot()

    await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=True,
        run_id="preflight-storage-warning",
        logger=logger,
        fetch_snapshot=fetcher,
        skip_auth_preflight=True,
    )

    warning_events = [
        event
        for event in logger.events
        if event.get("phase") == "order_line_items_rebuild_preflight"
        and event.get("status") == "warning"
    ]
    assert warning_events
    assert warning_events[0]["missing_storage_states"] == [
        {
            "source": "td",
            "store_code": "TD001",
            "storage_state": str(missing_state),
            "reason": "storage_state_file_missing",
        }
    ]


@pytest.mark.asyncio
async def test_td_expired_auth_fails_preflight_before_windows(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'td-expired.sqlite'}"
    await _create_common_tables(db_url)
    monkeypatch.setattr(rebuild, "config", SimpleNamespace(database_url=db_url))
    expired_state = _write_td_storage_state(
        tmp_path / "profiles" / "A817_storage_state.json", expires=1
    )

    async def load_stores(*, sources, store_codes, logger):
        return [
            rebuild.RebuildStore(
                source="td",
                store_code="A817",
                cost_center="CC01",
                raw_store=SimpleNamespace(storage_state_path=expired_state),
            )
        ]

    async def td_auth_not_ready(store, *, run_id, logger):
        return "unauthorized"

    monkeypatch.setattr(rebuild, "load_rebuild_stores", load_stores)
    monkeypatch.setattr(rebuild, "_td_api_auth_readiness_status", td_auth_not_ready)
    logger = _InMemoryLogger("td-expired-preflight")
    window_seen = False

    async def fetcher(**_kwargs):
        nonlocal window_seen
        window_seen = True
        return rebuild.SourceSnapshot()

    with pytest.raises(RuntimeError, match="source auth readiness failed"):
        await rebuild.run_rebuild(
            source_selection="td",
            store_codes=None,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 1),
            window_size_days=1,
            dry_run=True,
            run_id="td-expired-preflight",
            logger=logger,
            fetch_snapshot=fetcher,
        )

    assert window_seen is False
    error_event = [
        event
        for event in logger.events
        if event.get("phase") == "order_line_items_rebuild_preflight"
        and event.get("status") == "error"
    ][-1]
    assert error_event["auth_readiness"] == {"td:A817": "unauthorized"}


@pytest.mark.asyncio
async def test_td_stale_storage_state_can_pass_after_refresh(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'td-refresh.sqlite'}"
    await _create_common_tables(db_url)
    monkeypatch.setattr(rebuild, "config", SimpleNamespace(database_url=db_url))
    storage_state = _write_td_storage_state(
        tmp_path / "profiles" / "A817_storage_state.json"
    )

    async def load_stores(*, sources, store_codes, logger):
        return [
            rebuild.RebuildStore(
                source="td",
                store_code="A817",
                cost_center="CC01",
                raw_store=SimpleNamespace(storage_state_path=storage_state),
            )
        ]

    async def td_auth_ready(store, *, run_id, logger):
        return "login_refresh_required"

    monkeypatch.setattr(rebuild, "load_rebuild_stores", load_stores)
    monkeypatch.setattr(rebuild, "_td_api_auth_readiness_status", td_auth_ready)
    logger = _InMemoryLogger("td-refresh-preflight")
    seen_windows: list[tuple[date, date]] = []

    async def fetcher(**kwargs):
        seen_windows.append((kwargs["window"].start, kwargs["window"].end))
        return rebuild.SourceSnapshot()

    await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=True,
        run_id="td-refresh-preflight",
        logger=logger,
        fetch_snapshot=fetcher,
    )

    assert seen_windows == [(date(2025, 1, 1), date(2025, 1, 1))]
    completed = [
        event
        for event in logger.events
        if event.get("phase") == "order_line_items_rebuild_preflight"
        and event.get("message") == "order_line_items rebuild preflight completed"
    ][-1]
    assert completed["auth_readiness"] == {"td:A817": "login_refresh_required"}


@pytest.mark.asyncio
async def test_td_valid_session_passes_preflight(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'td-valid-session.sqlite'}"
    await _create_common_tables(db_url)
    monkeypatch.setattr(rebuild, "config", SimpleNamespace(database_url=db_url))
    storage_state = _write_td_storage_state(
        tmp_path / "profiles" / "A817_storage_state.json"
    )

    async def load_stores(*, sources, store_codes, logger):
        return [
            rebuild.RebuildStore(
                source="td",
                store_code="A817",
                cost_center="CC01",
                raw_store=SimpleNamespace(storage_state_path=storage_state),
            )
        ]

    async def td_auth_ready(store, *, run_id, logger):
        return "session_valid"

    monkeypatch.setattr(rebuild, "load_rebuild_stores", load_stores)
    monkeypatch.setattr(rebuild, "_td_api_auth_readiness_status", td_auth_ready)
    logger = _InMemoryLogger("td-valid-session-preflight")
    window_seen = False

    async def fetcher(**_kwargs):
        nonlocal window_seen
        window_seen = True
        return rebuild.SourceSnapshot()

    await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=True,
        run_id="td-valid-session-preflight",
        logger=logger,
        fetch_snapshot=fetcher,
    )

    assert window_seen is True
    completed = [
        event
        for event in logger.events
        if event.get("phase") == "order_line_items_rebuild_preflight"
        and event.get("message") == "order_line_items rebuild preflight completed"
    ][-1]
    assert completed["auth_readiness"] == {"td:A817": "session_valid"}


@pytest.mark.asyncio
async def test_td_missing_iframe_or_token_fails_preflight(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'td-missing-iframe.sqlite'}"
    await _create_common_tables(db_url)
    monkeypatch.setattr(rebuild, "config", SimpleNamespace(database_url=db_url))
    storage_state = _write_td_storage_state(
        tmp_path / "profiles" / "A817_storage_state.json"
    )

    async def load_stores(*, sources, store_codes, logger):
        return [
            rebuild.RebuildStore(
                source="td",
                store_code="A817",
                cost_center="CC01",
                raw_store=SimpleNamespace(storage_state_path=storage_state),
            )
        ]

    async def td_auth_not_ready(store, *, run_id, logger):
        return "unauthorized"

    monkeypatch.setattr(rebuild, "load_rebuild_stores", load_stores)
    monkeypatch.setattr(rebuild, "_td_api_auth_readiness_status", td_auth_not_ready)
    logger = _InMemoryLogger("td-missing-iframe-preflight")
    window_seen = False

    async def fetcher(**_kwargs):
        nonlocal window_seen
        window_seen = True
        return rebuild.SourceSnapshot()

    with pytest.raises(RuntimeError, match="source auth readiness failed"):
        await rebuild.run_rebuild(
            source_selection="td",
            store_codes=None,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 1),
            window_size_days=1,
            dry_run=True,
            run_id="td-missing-iframe-preflight",
            logger=logger,
            fetch_snapshot=fetcher,
        )

    assert window_seen is False
    error_event = [
        event
        for event in logger.events
        if event.get("phase") == "order_line_items_rebuild_preflight"
        and event.get("status") == "error"
    ][-1]
    assert error_event["auth_readiness"] == {"td:A817": "unauthorized"}


@pytest.mark.asyncio
async def test_preflight_failure_cli_exits_nonzero_before_windows(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'td-cli-fail.sqlite'}"
    await _create_common_tables(db_url)
    monkeypatch.setattr(rebuild, "config", SimpleNamespace(database_url=db_url))
    storage_state = _write_td_storage_state(
        tmp_path / "profiles" / "A817_storage_state.json"
    )

    async def load_stores(*, sources, store_codes, logger):
        return [
            rebuild.RebuildStore(
                source="td",
                store_code="A817",
                cost_center="CC01",
                raw_store=SimpleNamespace(storage_state_path=storage_state),
            )
        ]

    async def td_auth_not_ready(store, *, run_id, logger):
        return "unauthorized"

    monkeypatch.setattr(rebuild, "load_rebuild_stores", load_stores)
    monkeypatch.setattr(rebuild, "_td_api_auth_readiness_status", td_auth_not_ready)
    window_seen = False

    async def fetcher(**_kwargs):
        nonlocal window_seen
        window_seen = True
        return rebuild.SourceSnapshot()

    monkeypatch.setattr(rebuild, "default_fetch_snapshot", fetcher)

    with pytest.raises(SystemExit) as exc_info:
        await rebuild._async_entrypoint(
            [
                "--source",
                "td",
                "--start-date",
                "2025-01-01",
                "--end-date",
                "2025-01-01",
                "--window-size",
                "1",
                "--dry-run",
                "--run-id",
                "td-cli-fail",
            ]
        )

    assert exc_info.value.code == 1
    assert window_seen is False


@pytest.mark.asyncio
async def test_uc_missing_token_fails_preflight_before_windows(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'uc-missing-token.sqlite'}"
    await _create_common_tables(db_url)
    monkeypatch.setattr(rebuild, "config", SimpleNamespace(database_url=db_url))
    storage_state = _write_uc_storage_state(
        tmp_path / "profiles" / "UC567_storage_state.json", token=None
    )

    async def load_stores(*, sources, store_codes, logger):
        return [
            rebuild.RebuildStore(
                source="uc",
                store_code="UC567",
                cost_center="CC01",
                raw_store=SimpleNamespace(storage_state_path=storage_state),
            )
        ]

    monkeypatch.setattr(rebuild, "load_rebuild_stores", load_stores)
    logger = _InMemoryLogger("uc-missing-token-preflight")
    window_seen = False

    async def fetcher(**_kwargs):
        nonlocal window_seen
        window_seen = True
        return rebuild.SourceSnapshot()

    with pytest.raises(RuntimeError, match="source auth readiness failed"):
        await rebuild.run_rebuild(
            source_selection="uc",
            store_codes=None,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 1),
            window_size_days=1,
            dry_run=True,
            run_id="uc-missing-token-preflight",
            logger=logger,
            fetch_snapshot=fetcher,
        )

    assert window_seen is False
    error_event = [
        event
        for event in logger.events
        if event.get("phase") == "order_line_items_rebuild_preflight"
        and event.get("status") == "error"
    ][-1]
    assert error_event["auth_readiness"] == {"uc:UC567": "missing_token"}


@pytest.mark.asyncio
async def test_valid_storage_and_token_pass_preflight(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'valid-auth.sqlite'}"
    await _create_common_tables(db_url)
    monkeypatch.setattr(rebuild, "config", SimpleNamespace(database_url=db_url))
    td_state = _write_td_storage_state(
        tmp_path / "profiles" / "A668_storage_state.json"
    )
    uc_state = _write_uc_storage_state(
        tmp_path / "profiles" / "UC610_storage_state.json"
    )

    async def load_stores(*, sources, store_codes, logger):
        return [
            rebuild.RebuildStore(
                source="td",
                store_code="A668",
                cost_center="CC01",
                raw_store=SimpleNamespace(storage_state_path=td_state),
            ),
            rebuild.RebuildStore(
                source="uc",
                store_code="UC610",
                cost_center="CC02",
                raw_store=SimpleNamespace(storage_state_path=uc_state),
            ),
        ]

    async def td_auth_ready(store, *, run_id, logger):
        return "session_valid"

    monkeypatch.setattr(rebuild, "load_rebuild_stores", load_stores)
    monkeypatch.setattr(rebuild, "_td_api_auth_readiness_status", td_auth_ready)
    logger = _InMemoryLogger("valid-auth-preflight")
    seen_stores: list[str] = []

    async def fetcher(**kwargs):
        seen_stores.append(kwargs["store"].store_code)
        return rebuild.SourceSnapshot()

    await rebuild.run_rebuild(
        source_selection="both",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=True,
        run_id="valid-auth-preflight",
        logger=logger,
        fetch_snapshot=fetcher,
    )

    assert seen_stores == ["A668", "UC610"]
    completed = [
        event
        for event in logger.events
        if event.get("phase") == "order_line_items_rebuild_preflight"
        and event.get("message") == "order_line_items rebuild preflight completed"
    ][-1]
    assert completed["auth_readiness"] == {
        "td:A668": "session_valid",
        "uc:UC610": "token_detected",
    }


@pytest.mark.asyncio
async def test_run_rebuild_generates_default_run_id_without_type_error(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    seen_run_ids: list[str] = []

    async def fetcher(**kwargs):
        seen_run_ids.append(kwargs["run_id"])
        return rebuild.SourceSnapshot(line_item_rows=[], order_snapshots=[])

    try:
        metrics = await rebuild.run_rebuild(
            source_selection="td",
            store_codes=None,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 1),
            window_size_days=1,
            dry_run=True,
            fetch_snapshot=fetcher,
        )
    except TypeError as exc:  # pragma: no cover - assertion failure path
        pytest.fail(f"run_rebuild raised TypeError without run_id: {exc}")

    assert len(metrics) == 1
    assert len(seen_run_ids) == 1
    assert seen_run_ids[0]


@pytest.mark.asyncio
async def test_run_rebuild_default_run_id_is_used_for_logger_store_and_window_events(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'rebuild-run-id.sqlite'}"
    await _create_common_tables(db_url)
    monkeypatch.setattr(rebuild, "config", SimpleNamespace(database_url=db_url))
    monkeypatch.setattr(rebuild, "new_run_id", lambda: "generated-rebuild-run")
    loggers: list[_InMemoryLogger] = []

    def fake_get_logger(run_id: str | None = None) -> _InMemoryLogger:
        assert run_id is not None
        logger = _InMemoryLogger(run_id)
        logger.info(
            phase="logger",
            message="Initialized JSON logger",
            run_id=logger.run_id,
        )
        loggers.append(logger)
        return logger

    async def fake_load_td_order_stores(*, logger, store_codes=None):
        rebuild.log_event(
            logger=logger,
            phase="init",
            message="Loaded TD store rows",
            store_count=1,
            stores=["TD001"],
        )
        return [SimpleNamespace(store_code="TD001", cost_center="CC01", sync_config={})]

    async def fake_load_uc_order_stores(*, logger, store_codes=None):
        rebuild.log_event(
            logger=logger,
            phase="init",
            message="Loaded UC store rows",
            store_count=1,
            stores=["UC001"],
        )
        return [SimpleNamespace(store_code="UC001", cost_center="CC01", sync_config={})]

    async def fetcher(**kwargs):
        assert kwargs["run_id"] == "generated-rebuild-run"
        return rebuild.SourceSnapshot(line_item_rows=[], order_snapshots=[])

    monkeypatch.setattr(rebuild, "get_logger", fake_get_logger)
    monkeypatch.setattr(rebuild, "_load_td_order_stores", fake_load_td_order_stores)
    monkeypatch.setattr(rebuild, "_load_uc_order_stores", fake_load_uc_order_stores)

    await rebuild.run_rebuild(
        source_selection="both",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=True,
        fetch_snapshot=fetcher,
        skip_auth_preflight=True,
    )

    assert len(loggers) == 1
    expected_messages = {
        "Initialized JSON logger",
        "Loaded TD store rows",
        "Loaded UC store rows",
        "Starting order_line_items historical rebuild",
        "order_line_items historical rebuild window checkpoint",
        "No missing order_line_items rebuild windows detected",
        "Completed order_line_items historical rebuild",
    }
    assert (
        _event_run_ids(loggers[0].events, expected_messages)
        == ["generated-rebuild-run"] * 8
    )


@pytest.mark.asyncio
async def test_run_rebuild_explicit_run_id_is_used_for_logger_store_and_window_events(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'rebuild-explicit-run-id.sqlite'}"
    await _create_common_tables(db_url)
    monkeypatch.setattr(rebuild, "config", SimpleNamespace(database_url=db_url))
    monkeypatch.setattr(
        rebuild,
        "new_run_id",
        lambda: pytest.fail("new_run_id must not be called for explicit run_id"),
    )
    loggers: list[_InMemoryLogger] = []

    def fake_get_logger(run_id: str | None = None) -> _InMemoryLogger:
        logger = _InMemoryLogger(str(run_id))
        loggers.append(logger)
        return logger

    async def fake_load_td_order_stores(*, logger, store_codes=None):
        rebuild.log_event(
            logger=logger,
            phase="init",
            message="Loaded TD store rows",
            store_count=1,
            stores=["TD001"],
        )
        return [SimpleNamespace(store_code="TD001", cost_center="CC01", sync_config={})]

    async def fetcher(**kwargs):
        assert kwargs["run_id"] == "explicit-cli-run"
        return rebuild.SourceSnapshot(line_item_rows=[], order_snapshots=[])

    monkeypatch.setattr(rebuild, "get_logger", fake_get_logger)
    monkeypatch.setattr(rebuild, "_load_td_order_stores", fake_load_td_order_stores)

    await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=True,
        run_id="explicit-cli-run",
        fetch_snapshot=fetcher,
        skip_auth_preflight=True,
    )

    assert len(loggers) == 1
    expected_messages = {
        "Loaded TD store rows",
        "Starting order_line_items historical rebuild",
        "order_line_items historical rebuild window checkpoint",
        "No missing order_line_items rebuild windows detected",
        "Completed order_line_items historical rebuild",
    }
    assert (
        _event_run_ids(loggers[0].events, expected_messages) == ["explicit-cli-run"] * 5
    )


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
    await rebuild._ensure_progress_table(db_url)
    progress = await _rows(
        db_url,
        "SELECT run_id FROM order_line_items_rebuild_progress WHERE run_id='dry'",
    )
    assert progress == []


@pytest.mark.asyncio
async def test_live_resume_after_dry_run_still_processes_window(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                "INSERT INTO orders (id, cost_center, store_code, order_number) "
                "VALUES (1,'CC01','TD001','ORD-DRY-LIVE')"
            )
        )
        await session.execute(
            sa.text(
                "INSERT INTO order_line_items "
                "(run_id, cost_center, store_code, order_id, order_number, line_item_key, line_item_uid, garment_name, ingest_row_seq) "
                "VALUES ('old','CC01','TD001',1,'ORD-DRY-LIVE','old','old','Old',1)"
            )
        )
        await session.commit()

    seen: list[tuple[bool, date]] = []

    async def fetcher(**kwargs):
        seen.append((kwargs["run_id"] == "dry-before-live", kwargs["window"].start))
        return rebuild.SourceSnapshot(
            line_item_rows=[
                {
                    "order_number": "ORD-DRY-LIVE",
                    "line_item_key": "new",
                    "garment_name": "New Live",
                }
            ],
            order_snapshots=[
                {
                    "order_number": "ORD-DRY-LIVE",
                    "garment_snapshot_outcome": "complete_with_rows",
                }
            ],
        )

    await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=True,
        run_id="dry-before-live",
        fetch_snapshot=fetcher,
    )
    await rebuild._ensure_progress_table(db_url)
    dry_progress = await _rows(
        db_url,
        "SELECT run_id FROM order_line_items_rebuild_progress WHERE run_id='dry-before-live'",
    )
    assert dry_progress == []

    live_metrics = await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=False,
        resume=True,
        run_id="live-after-dry",
        fetch_snapshot=fetcher,
    )

    assert seen == [(True, date(2025, 1, 1)), (False, date(2025, 1, 1))]
    assert len(live_metrics) == 1
    assert live_metrics[0].inserted_rows == 1
    rows = await _rows(
        db_url,
        "SELECT garment_name, run_id FROM order_line_items WHERE order_number='ORD-DRY-LIVE'",
    )
    assert [(row.garment_name, row.run_id) for row in rows] == [
        ("New Live", "live-after-dry")
    ]


@pytest.mark.asyncio
async def test_live_resume_ignores_legacy_dry_run_progress_success(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    await rebuild._ensure_progress_table(db_url)
    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                "INSERT INTO order_line_items_rebuild_progress "
                "(source, store_code, cost_center, window_start, window_end, run_id, status, attempt_no, dry_run) "
                "VALUES ('td', 'TD001', 'CC01', '2025-01-01', '2025-01-01', 'legacy-dry', 'success', 1, TRUE)"
            )
        )
        await session.execute(
            sa.text(
                "INSERT INTO orders (id, cost_center, store_code, order_number) "
                "VALUES (1,'CC01','TD001','ORD-LEGACY-DRY')"
            )
        )
        await session.commit()
    seen: list[date] = []

    async def fetcher(**kwargs):
        seen.append(kwargs["window"].start)
        return rebuild.SourceSnapshot(
            line_item_rows=[
                {
                    "order_number": "ORD-LEGACY-DRY",
                    "line_item_key": "live",
                    "garment_name": "Live After Legacy Dry",
                }
            ],
            order_snapshots=[
                {
                    "order_number": "ORD-LEGACY-DRY",
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
        resume=True,
        run_id="live-after-legacy-dry",
        fetch_snapshot=fetcher,
    )

    assert seen == [date(2025, 1, 1)]
    assert len(metrics) == 1
    rows = await _rows(
        db_url,
        "SELECT garment_name FROM order_line_items WHERE order_number='ORD-LEGACY-DRY'",
    )
    assert [row.garment_name for row in rows] == ["Live After Legacy Dry"]
    progress = await _rows(
        db_url,
        "SELECT run_id, dry_run FROM order_line_items_rebuild_progress "
        "WHERE source='td' AND store_code='TD001' AND window_start='2025-01-01'",
    )
    assert [(row.run_id, bool(row.dry_run)) for row in progress] == [
        ("live-after-legacy-dry", False)
    ]


@pytest.mark.asyncio
async def test_td_complete_garments_fetch_allows_replacement(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                "INSERT INTO orders (id, cost_center, store_code, order_number) "
                "VALUES (1,'CC01','TD001','ORD-COMPLETE')"
            )
        )
        await session.execute(
            sa.text(
                "INSERT INTO order_line_items "
                "(run_id, cost_center, store_code, order_id, order_number, "
                "line_item_key, line_item_uid, garment_name, ingest_row_seq) "
                "VALUES ('old','CC01','TD001',1,'ORD-COMPLETE','old','old','Old',1)"
            )
        )
        await session.commit()

    async def fetcher(**kwargs):
        return rebuild.SourceSnapshot(
            line_item_rows=[
                {
                    "order_number": "ORD-COMPLETE",
                    "line_item_key": "new",
                    "garment_name": "Replacement",
                }
            ],
            order_snapshots=[
                {
                    "order_number": "ORD-COMPLETE",
                    "garment_snapshot_outcome": "complete_with_rows",
                }
            ],
            garments_fetch_completeness="complete",
            endpoint_health={"garments_fetch_completeness": "complete"},
        )

    metrics = await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=False,
        run_id="complete-authoritative",
        fetch_snapshot=fetcher,
    )

    assert metrics[0].deleted_rows == 1
    assert metrics[0].inserted_rows == 1
    rows = await _rows(
        db_url,
        "SELECT garment_name FROM order_line_items WHERE order_number='ORD-COMPLETE'",
    )
    assert [row.garment_name for row in rows] == ["Replacement"]


@pytest.mark.asyncio
async def test_td_incomplete_garments_fetch_preserves_existing_rows(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    logger = _InMemoryLogger("incomplete-authority")
    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                "INSERT INTO order_line_items "
                "(run_id, cost_center, store_code, order_number, line_item_key, "
                "line_item_uid, garment_name, ingest_row_seq) "
                "VALUES ('old','CC01','TD001','ORD-INCOMPLETE','old','old','Keep',1)"
            )
        )
        await session.commit()

    async def fetcher(**kwargs):
        return rebuild.SourceSnapshot(
            line_item_rows=[],
            order_snapshots=[
                {
                    "order_number": "ORD-INCOMPLETE",
                    "garment_snapshot_outcome": "complete_empty",
                }
            ],
            garments_fetch_completeness="incomplete",
            source_fetch_error_class="pagination_budget_exhausted",
            endpoint_health={
                "garments_fetch_completeness": "incomplete",
                "final_error_class": "pagination_budget_exhausted",
                "resume_from_page": 4,
            },
            endpoint_error_diagnostics={"reason": "pagination budget exhausted"},
        )

    with pytest.raises(rebuild.OrderLineItemsRebuildIncomplete):
        await rebuild.run_rebuild(
            source_selection="td",
            store_codes=None,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 1),
            window_size_days=1,
            dry_run=False,
            run_id="incomplete-authority",
            logger=logger,
            fetch_snapshot=fetcher,
        )

    rows = await _rows(db_url, "SELECT garment_name FROM order_line_items")
    assert [row.garment_name for row in rows] == ["Keep"]
    failed_events = [
        event
        for event in logger.events
        if event.get("phase") == "order_line_items_rebuild_window"
        and event.get("status") == "error"
    ]
    assert failed_events[-1]["garments_fetch_completeness"] == "incomplete"
    assert (
        failed_events[-1]["source_fetch_error_class"] == "pagination_budget_exhausted"
    )
    assert failed_events[-1]["endpoint_health"]["resume_from_page"] == 4


@pytest.mark.asyncio
async def test_td_auth_failed_garments_fetch_fails_window(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    logger = _InMemoryLogger("auth-failed-garments")

    async def fetcher(**kwargs):
        raise rebuild.TdApiUnauthorizedError(
            store_code="TD001",
            failed_endpoints=["/garments/details"],
            error_class="http_401",
        )

    with pytest.raises(rebuild.OrderLineItemsRebuildIncomplete) as exc_info:
        await rebuild.run_rebuild(
            source_selection="td",
            store_codes=None,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 1),
            window_size_days=1,
            dry_run=True,
            run_id="auth-failed-garments",
            logger=logger,
            fetch_snapshot=fetcher,
        )

    assert exc_info.value.completed_window_count == 0
    assert exc_info.value.missing_windows == ("td:TD001:2025-01-01..2025-01-01",)
    failed_events = [
        event
        for event in logger.events
        if event.get("phase") == "order_line_items_rebuild_window"
        and event.get("status") == "error"
    ]
    assert failed_events[-1]["failure_class"] == "store_specific_failure"
    assert "http_401" in failed_events[-1]["error_message"]


@pytest.mark.asyncio
async def test_td_timeout_incomplete_garments_fetch_is_retryable_and_resumable(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    logger = _InMemoryLogger("timeout-incomplete")
    attempts = 0

    async def fetcher(**kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return rebuild.SourceSnapshot(
                line_item_rows=[],
                order_snapshots=[],
                garments_fetch_completeness="incomplete",
                source_fetch_error_class="read_timeout",
                endpoint_health={
                    "garments_fetch_completeness": "incomplete",
                    "final_error_class": "read_timeout",
                    "resume_from_page": 7,
                    "timeout_count": 2,
                },
                endpoint_error_diagnostics={"timeout_ms": 35000},
            )
        return rebuild.SourceSnapshot(
            line_item_rows=[],
            order_snapshots=[],
            garments_fetch_completeness="complete",
            endpoint_health={"garments_fetch_completeness": "complete"},
        )

    metrics = await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=True,
        run_id="timeout-incomplete",
        logger=logger,
        fetch_snapshot=fetcher,
    )

    assert attempts == 2
    assert len(metrics) == 1
    retry_events = [
        event
        for event in logger.events
        if event.get("message")
        == "Retrying order_line_items rebuild window after retryable failure"
    ]
    assert retry_events[-1]["garments_fetch_completeness"] == "incomplete"
    assert retry_events[-1]["source_fetch_error_class"] == "read_timeout"
    assert retry_events[-1]["endpoint_health"]["resume_from_page"] == 7
    assert retry_events[-1]["retryable"] is True


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
        skip_auth_preflight=True,
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
        skip_auth_preflight=True,
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
        skip_auth_preflight=True,
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
        skip_auth_preflight=True,
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
        skip_auth_preflight=True,
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

    logger = _InMemoryLogger("resume-parent")
    metrics = await rebuild.run_rebuild(
        source_selection="uc",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 2),
        window_size_days=1,
        dry_run=False,
        resume=True,
        run_id="resume-parent",
        logger=logger,
        fetch_snapshot=fetcher,
        skip_auth_preflight=True,
    )

    assert seen == [date(2025, 1, 2)]
    skip_events = [
        event
        for event in logger.events
        if event.get("message")
        == "Skipping previously successful order_line_items rebuild window"
    ]
    assert skip_events[0]["prior_run_id"] == "old-parent"
    assert skip_events[0]["resume"] is True
    assert [
        event
        for event in logger.events
        if event.get("message")
        == "order_line_items rebuild resume matches prior progress by source/store/window, not the current run ID"
    ]
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
        skip_auth_preflight=True,
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
    await rebuild._ensure_progress_table(db_url)
    rows = await _rows(
        db_url,
        "SELECT status, attempt_no FROM order_line_items_rebuild_progress WHERE run_id='retry'",
    )
    assert rows == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exc",
    [
        TypeError("launch_browser() takes 0 positional arguments but 1 was given"),
        RuntimeError("missing cost_center"),
    ],
)
async def test_deterministic_setup_failures_are_not_retried(
    patch_config_and_stores, exc: Exception
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    attempts = 0

    async def fetcher(**kwargs):
        nonlocal attempts
        attempts += 1
        raise exc

    with pytest.raises(type(exc)):
        await rebuild.run_rebuild(
            source_selection="td",
            store_codes=None,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 1),
            window_size_days=1,
            dry_run=True,
            run_id="deterministic",
            fetch_snapshot=fetcher,
        )

    assert attempts == 1


@pytest.mark.asyncio
async def test_systemic_type_error_stops_after_first_window(
    patch_config_and_stores, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    attempts: list[tuple[str, date]] = []
    events: list[dict[str, Any]] = []

    def capture_log_event(**kwargs: Any) -> None:
        events.append(kwargs)

    monkeypatch.setattr(rebuild, "log_event", capture_log_event)

    async def fetcher(**kwargs):
        attempts.append((kwargs["store"].store_code, kwargs["window"].start))
        raise TypeError("launch_browser() takes 0 positional arguments but 1 was given")

    with pytest.raises(TypeError):
        await rebuild.run_rebuild(
            source_selection="td",
            store_codes=None,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 2),
            window_size_days=1,
            dry_run=True,
            run_id="systemic-type-error",
            fetch_snapshot=fetcher,
        )

    assert attempts == [("TD001", date(2025, 1, 1))]
    stop_events = [
        event
        for event in events
        if event.get("phase") == "order_line_items_rebuild"
        and event.get("status") == "error"
    ]
    assert len(stop_events) == 1
    assert stop_events[0]["failure_class"] == "systemic_setup_failure"
    assert stop_events[0]["source"] == "td"
    assert stop_events[0]["store_code"] == "TD001"
    assert stop_events[0]["window_start"] == "2025-01-01"
    assert "launch_browser" in stop_events[0]["error_message"]


@pytest.mark.asyncio
async def test_transient_timeout_retries_and_then_continues_to_next_window(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    attempts: list[date] = []

    async def fetcher(**kwargs):
        window_start = kwargs["window"].start
        attempts.append(window_start)
        if attempts.count(window_start) == 1 and window_start == date(2025, 1, 1):
            raise TimeoutError("Navigation timeout of 30000 ms exceeded")
        return rebuild.SourceSnapshot(line_item_rows=[], order_snapshots=[])

    metrics = await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 2),
        window_size_days=1,
        dry_run=True,
        run_id="transient-continues",
        fetch_snapshot=fetcher,
    )

    assert attempts == [date(2025, 1, 1), date(2025, 1, 1), date(2025, 1, 2)]
    assert [(metric.window_start, metric.window_end) for metric in metrics] == [
        (date(2025, 1, 1), date(2025, 1, 1)),
        (date(2025, 1, 2), date(2025, 1, 2)),
    ]


@pytest.mark.asyncio
async def test_store_specific_auth_failure_does_not_suppress_other_sources(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    seen: list[str] = []

    async def fetcher(**kwargs):
        source = kwargs["source"]
        seen.append(source)
        if source == "td":
            raise RuntimeError("401 unauthorized: store authentication failed")
        return rebuild.SourceSnapshot(line_item_rows=[], order_snapshots=[])

    with pytest.raises(rebuild.OrderLineItemsRebuildIncomplete) as exc_info:
        await rebuild.run_rebuild(
            source_selection="both",
            store_codes=None,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 1),
            window_size_days=1,
            dry_run=True,
            run_id="store-auth",
            fetch_snapshot=fetcher,
            skip_auth_preflight=True,
        )

    assert seen == ["td", "uc"]
    assert exc_info.value.completed_window_count == 1
    assert exc_info.value.missing_windows == ("td:TD001:2025-01-01..2025-01-01",)


@pytest.mark.asyncio
async def test_runtime_error_with_transient_navigation_token_is_retried(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    attempts = 0

    async def fetcher(**kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("session timeout while loading archive orders page")
        return rebuild.SourceSnapshot(line_item_rows=[], order_snapshots=[])

    metrics = await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=True,
        run_id="runtime-transient",
        fetch_snapshot=fetcher,
    )

    assert attempts == 2
    assert len(metrics) == 1


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
    with pytest.raises(rebuild.OrderLineItemsRebuildIncomplete) as exc_info:
        await rebuild.run_rebuild(
            source_selection="td",
            store_codes=None,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 2),
            window_size_days=1,
            dry_run=True,
            run_id="missing",
            fetch_snapshot=fetcher,
        )

    assert exc_info.value.run_id == "missing"
    assert exc_info.value.expected_window_count == 2
    assert exc_info.value.completed_window_count == 1
    assert exc_info.value.missing_window_count == 1
    assert exc_info.value.missing_windows == ("td:TD001:2025-01-02..2025-01-02",)
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
async def test_resume_skipped_window_log_includes_prior_progress_metadata(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    await rebuild._ensure_progress_table(db_url)
    async with session_scope(db_url) as session:
        await session.execute(sa.text("""
            INSERT INTO order_line_items_rebuild_progress
            (source, store_code, cost_center, window_start, window_end, run_id, status, attempt_no,
             complete_with_rows_orders, complete_empty_orders, skipped_incomplete_orders, deleted_rows, inserted_rows,
             orphan_rows, updated_at)
            VALUES ('td', 'TD001', 'CC01', '2025-01-01', '2025-01-01', 'prior-run', 'success_with_warnings', 2,
             3, 4, 5, 6, 7, 8, '2025-01-02 03:04:05')
        """))
        await session.commit()
    logger = _InMemoryLogger("current-run")

    async def fetcher(**kwargs):
        pytest.fail("successful resume progress should skip the window")

    await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=True,
        resume=True,
        run_id="current-run",
        logger=logger,
        fetch_snapshot=fetcher,
    )

    skip_event = next(
        event
        for event in logger.events
        if event.get("message")
        == "Skipping previously successful order_line_items rebuild window"
    )
    assert skip_event["prior_run_id"] == "prior-run"
    assert skip_event["prior_updated_at"] == "2025-01-02 03:04:05"
    assert skip_event["prior_status"] == "success_with_warnings"
    assert skip_event["prior_metrics_counts"] == {
        "complete_with_rows_orders": 3,
        "complete_empty_orders": 4,
        "skipped_incomplete_orders": 5,
        "deleted_rows": 6,
        "inserted_rows": 7,
        "orphan_rows": 8,
    }


@pytest.mark.asyncio
async def test_fresh_live_rebuild_without_resume_ignores_progress_and_processes_all_windows(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    await rebuild._ensure_progress_table(db_url)
    async with session_scope(db_url) as session:
        await session.execute(sa.text("""
            INSERT INTO order_line_items_rebuild_progress
            (source, store_code, cost_center, window_start, window_end, run_id, status, attempt_no)
            VALUES ('td', 'TD001', 'CC01', '2025-01-01', '2025-01-01', 'prior-success', 'success', 1)
        """))
        await session.commit()
    seen: list[date] = []

    async def fetcher(**kwargs):
        seen.append(kwargs["window"].start)
        return rebuild.SourceSnapshot(line_item_rows=[], order_snapshots=[])

    metrics = await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=False,
        resume=False,
        run_id="fresh-live",
        fetch_snapshot=fetcher,
    )

    assert seen == [date(2025, 1, 1)]
    assert len(metrics) == 1
    progress = await _rows(
        db_url,
        "SELECT run_id, status FROM order_line_items_rebuild_progress WHERE source='td' AND store_code='TD001'",
    )
    assert [(row.run_id, row.status) for row in progress] == [("fresh-live", "success")]


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
async def test_end_date_defaults_to_current_pipeline_date(
    patch_config_and_stores, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    monkeypatch.setattr(
        rebuild,
        "aware_now",
        lambda tz: datetime(2025, 2, 12, 8, 30, tzinfo=timezone.utc),
    )
    seen: list[tuple[date, date]] = []

    async def fetcher(**kwargs):
        seen.append((kwargs["window"].start, kwargs["window"].end))
        return rebuild.SourceSnapshot(line_item_rows=[], order_snapshots=[])

    await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 2, 12),
        end_date=None,
        window_size_days=1,
        dry_run=True,
        run_id="default-end",
        fetch_snapshot=fetcher,
    )

    assert seen == [(date(2025, 2, 12), date(2025, 2, 12))]


@pytest.mark.asyncio
async def test_omitted_dates_use_store_start_through_current_pipeline_date(
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
    monkeypatch.setattr(
        rebuild,
        "aware_now",
        lambda tz: datetime(2025, 2, 12, 8, 30, tzinfo=timezone.utc),
    )
    seen: list[tuple[date, date]] = []

    async def fetcher(**kwargs):
        seen.append((kwargs["window"].start, kwargs["window"].end))
        return rebuild.SourceSnapshot(line_item_rows=[], order_snapshots=[])

    await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=None,
        end_date=None,
        window_size_days=1,
        dry_run=True,
        run_id="default-both",
        fetch_snapshot=fetcher,
    )

    assert seen == [
        (date(2025, 2, 10), date(2025, 2, 10)),
        (date(2025, 2, 11), date(2025, 2, 11)),
        (date(2025, 2, 12), date(2025, 2, 12)),
    ]


@pytest.mark.asyncio
async def test_both_source_full_live_uses_each_store_master_start_date(
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
        await session.execute(sa.text("""
            INSERT INTO store_master (store_code, sync_group, start_date) VALUES
            ('TD001', 'td', '2025-02-10'),
            ('UC001', 'UC', '2025-02-11')
        """))
        await session.commit()
    monkeypatch.setattr(
        rebuild,
        "aware_now",
        lambda tz: datetime(2025, 2, 11, 8, 30, tzinfo=timezone.utc),
    )
    seen: list[tuple[str, str, date, date]] = []

    async def fetcher(**kwargs):
        seen.append(
            (
                kwargs["store"].source,
                kwargs["store"].store_code,
                kwargs["window"].start,
                kwargs["window"].end,
            )
        )
        return rebuild.SourceSnapshot(line_item_rows=[], order_snapshots=[])

    await rebuild.run_rebuild(
        source_selection="both",
        store_codes=None,
        start_date=None,
        end_date=None,
        window_size_days=1,
        dry_run=True,
        run_id="full-live-both",
        fetch_snapshot=fetcher,
        skip_auth_preflight=True,
    )

    assert seen == [
        ("td", "TD001", date(2025, 2, 10), date(2025, 2, 10)),
        ("td", "TD001", date(2025, 2, 11), date(2025, 2, 11)),
        ("uc", "UC001", date(2025, 2, 11), date(2025, 2, 11)),
    ]


@pytest.mark.asyncio
async def test_explicit_dates_override_store_and_current_defaults(
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
            sa.text("INSERT INTO store_master VALUES ('TD001', 'TD', '2025-02-01')")
        )
        await session.commit()
    monkeypatch.setattr(
        rebuild,
        "aware_now",
        lambda tz: datetime(2025, 2, 12, 8, 30, tzinfo=timezone.utc),
    )
    seen: list[tuple[date, date]] = []

    async def fetcher(**kwargs):
        seen.append((kwargs["window"].start, kwargs["window"].end))
        return rebuild.SourceSnapshot(line_item_rows=[], order_snapshots=[])

    await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 2, 5),
        end_date=date(2025, 2, 6),
        window_size_days=1,
        dry_run=True,
        run_id="explicit-dates",
        fetch_snapshot=fetcher,
    )

    assert seen == [
        (date(2025, 2, 5), date(2025, 2, 5)),
        (date(2025, 2, 6), date(2025, 2, 6)),
    ]


def test_module_parser_accepts_omitted_dates() -> None:
    args = rebuild._build_parser().parse_args(
        ["--source", "both", "--resume", "--run-id", "explicit-cli-run"]
    )

    assert args.source == "both"
    assert args.start_date is None
    assert args.end_date is None
    assert args.resume is True
    assert args.resume_run_id is None
    assert args.fresh is False
    assert args.run_id == "explicit-cli-run"

    scoped_args = rebuild._build_parser().parse_args(
        ["--source", "td", "--resume", "--resume-run-id", "prior-run"]
    )
    assert scoped_args.resume is True
    assert scoped_args.resume_run_id == "prior-run"

    fresh_args = rebuild._build_parser().parse_args(
        ["--source", "td", "--ignore-progress"]
    )
    assert fresh_args.fresh is True


def test_async_entrypoint_exits_zero_when_rebuild_completes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[dict[str, Any]] = []

    async def fake_run_rebuild(**kwargs):
        captured.append(kwargs)
        return []

    monkeypatch.setattr(rebuild, "run_rebuild", fake_run_rebuild)

    rebuild.run(
        [
            "--source",
            "td",
            "--start-date",
            "2025-01-01",
            "--end-date",
            "2025-01-01",
            "--run-id",
            "explicit-cli-run",
        ]
    )

    assert captured[0]["source_selection"] == "td"
    assert captured[0]["run_id"] == "explicit-cli-run"


def test_async_entrypoint_exits_nonzero_when_rebuild_is_incomplete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_run_rebuild(**kwargs):
        raise rebuild.OrderLineItemsRebuildIncomplete(
            run_id="cli-failed",
            expected_window_count=1,
            completed_window_count=0,
            missing_window_count=1,
            missing_windows=("td:TD001:2025-01-01..2025-01-01",),
        )

    monkeypatch.setattr(rebuild, "run_rebuild", fake_run_rebuild)

    with pytest.raises(SystemExit) as exc_info:
        rebuild.run(
            ["--source", "td", "--start-date", "2025-01-01", "--end-date", "2025-01-01"]
        )

    assert exc_info.value.code == 1


@pytest.mark.asyncio
async def test_smoke_run_with_expected_windows_but_zero_completed_windows_fails_process(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)

    async def fetcher(**kwargs):
        raise RuntimeError("crm snapshot unavailable")

    with pytest.raises(rebuild.OrderLineItemsRebuildIncomplete) as exc_info:
        await rebuild.run_rebuild(
            source_selection="td",
            store_codes=None,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 1),
            window_size_days=1,
            dry_run=True,
            run_id="smoke-missing",
            fetch_snapshot=fetcher,
        )

    assert exc_info.value.expected_window_count == 1
    assert exc_info.value.completed_window_count == 0
    assert exc_info.value.missing_window_count == 1
    assert exc_info.value.missing_windows == ("td:TD001:2025-01-01..2025-01-01",)


def test_top_level_canonical_and_alias_cli_accept_omitted_dates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[list[str] | None] = []

    def fake_runner(argv: list[str] | None = None) -> None:
        captured.append(argv)

    monkeypatch.setattr("app.crm_downloader.order_line_items_rebuild.run", fake_runner)

    canonical_exit = app_main.main(
        ["crm", "rebuild-order-line-items", "--source", "both", "--resume"]
    )
    alias_exit = app_main.main(
        ["crm", "order-line-items-rebuild", "--source", "both", "--resume"]
    )

    assert canonical_exit == 0
    assert alias_exit == 0
    assert captured == [
        ["--source", "both", "--resume"],
        ["--source", "both", "--resume"],
    ]


def test_top_level_crm_rebuild_order_line_items_dry_run_without_run_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[list[str] | None] = []

    def fake_runner(argv: list[str] | None = None) -> None:
        captured.append(argv)

    monkeypatch.setattr("app.crm_downloader.order_line_items_rebuild.run", fake_runner)

    exit_code = app_main.main(
        [
            "crm",
            "rebuild-order-line-items",
            "--source",
            "both",
            "--from-date",
            "2026-05-01",
            "--to-date",
            "2026-05-07",
            "--window-days",
            "7",
            "--dry-run",
        ]
    )

    assert exit_code == 0
    assert captured == [
        [
            "--source",
            "both",
            "--end-date",
            "2026-05-07",
            "--start-date",
            "2026-05-01",
            "--window-size",
            "7",
            "--dry-run",
        ]
    ]


@pytest.mark.asyncio
async def test_resume_skips_completed_windows_with_default_end_date(
    patch_config_and_stores, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    await rebuild._ensure_progress_table(db_url)
    async with session_scope(db_url) as session:
        await session.execute(sa.text("""
            INSERT INTO order_line_items_rebuild_progress
            (source, store_code, cost_center, window_start, window_end, run_id, status, attempt_no)
            VALUES ('td', 'TD001', 'CC01', '2025-02-10', '2025-02-10', 'old', 'success', 1)
        """))
        await session.commit()
    monkeypatch.setattr(
        rebuild,
        "aware_now",
        lambda tz: datetime(2025, 2, 11, 8, 30, tzinfo=timezone.utc),
    )
    seen: list[date] = []

    async def fetcher(**kwargs):
        seen.append(kwargs["window"].start)
        return rebuild.SourceSnapshot(line_item_rows=[], order_snapshots=[])

    await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 2, 10),
        end_date=None,
        window_size_days=1,
        dry_run=True,
        resume=True,
        run_id="resume-default-end",
        fetch_snapshot=fetcher,
    )

    assert seen == [date(2025, 2, 11)]


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
        skip_auth_preflight=True,
    )

    assert seen == [
        (date(2025, 1, 1), date(2025, 1, 10)),
        (date(2025, 1, 11), date(2025, 1, 20)),
        (date(2025, 1, 21), date(2025, 1, 25)),
    ]


@pytest.mark.asyncio
async def test_zero_snapshot_confirmed_source_empty_logs_ok_event(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    logger = _InMemoryLogger("zero-confirmed-empty")

    async def fetcher(**_kwargs):
        return rebuild.SourceSnapshot(
            line_item_rows=[],
            order_snapshots=[],
            zero_snapshot_class="confirmed_source_empty",
        )

    metrics = await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=True,
        run_id="zero-confirmed-empty",
        logger=logger,
        fetch_snapshot=fetcher,
    )

    assert len(metrics) == 1
    zero_events = [
        event
        for event in logger.events
        if event.get("message")
        == "zero_authoritative_orders_detected_confirmed_source_empty"
    ]
    assert zero_events
    assert zero_events[-1]["status"] == "ok"
    assert zero_events[-1]["source"] == "td"
    assert zero_events[-1]["store_code"] == "TD001"
    assert zero_events[-1]["cost_center"] == "CC01"
    assert zero_events[-1]["window_start"] == "2025-01-01"
    assert zero_events[-1]["window_end"] == "2025-01-01"
    assert zero_events[-1]["dry_run"] is True
    assert zero_events[-1]["zero_snapshot_class"] == "confirmed_source_empty"


@pytest.mark.asyncio
async def test_zero_snapshot_source_fetch_auth_failure_fails_in_strict_mode(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    logger = _InMemoryLogger("zero-auth-failure")

    async def fetcher(**_kwargs):
        return rebuild.SourceSnapshot(
            line_item_rows=[],
            order_snapshots=[],
            zero_snapshot_class="source_fetch_auth_failure",
        )

    with pytest.raises(rebuild.OrderLineItemsZeroSnapshotDetected) as exc_info:
        await rebuild.run_rebuild(
            source_selection="td",
            store_codes=None,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 1),
            window_size_days=1,
            dry_run=True,
            fail_on_zero_snapshot=True,
            run_id="zero-auth-failure",
            logger=logger,
            fetch_snapshot=fetcher,
        )

    assert exc_info.value.suspicious_window_count == 1
    zero_events = [
        event
        for event in logger.events
        if event.get("message")
        == "zero_authoritative_orders_detected_source_fetch_auth_failure"
    ]
    assert zero_events
    assert zero_events[-1]["status"] == "warning"
    summary_events = [
        event
        for event in logger.events
        if event.get("phase") == "order_line_items_rebuild_zero_snapshot_summary"
    ]
    assert summary_events[-1]["status"] == "error"
    assert summary_events[-1]["fail_on_zero_snapshot"] is True


@pytest.mark.asyncio
async def test_all_selected_windows_zero_in_dry_run_logs_prominent_warning(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    logger = _InMemoryLogger("zero-dry-run-summary")

    async def fetcher(**_kwargs):
        return rebuild.SourceSnapshot(line_item_rows=[], order_snapshots=[])

    metrics = await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 2),
        window_size_days=1,
        dry_run=True,
        run_id="zero-dry-run-summary",
        logger=logger,
        fetch_snapshot=fetcher,
    )

    assert len(metrics) == 2
    summary_events = [
        event
        for event in logger.events
        if event.get("message")
        == "all_selected_windows_zero_authoritative_orders_detected"
    ]
    assert summary_events
    assert summary_events[-1]["status"] == "warning"
    assert summary_events[-1]["dry_run"] is True
    assert summary_events[-1]["fail_on_zero_snapshot"] is False
    assert summary_events[-1]["expected_window_count"] == 2
    assert summary_events[-1]["zero_window_count"] == 2
    assert summary_events[-1]["suspicious_zero_window_count"] == 2
    assert [item["window_start"] for item in summary_events[-1]["zero_windows"]] == [
        "2025-01-01",
        "2025-01-02",
    ]


@pytest.mark.asyncio
async def test_uc_zero_snapshot_confirmed_empty_does_not_fail_by_default(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    logger = _InMemoryLogger("uc-zero-confirmed-empty")

    async def fetcher(**_kwargs):
        return rebuild.SourceSnapshot(
            line_item_rows=[],
            order_snapshots=[],
            zero_snapshot_class="confirmed_source_empty",
        )

    metrics = await rebuild.run_rebuild(
        source_selection="uc",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=True,
        run_id="uc-zero-confirmed-empty",
        logger=logger,
        fetch_snapshot=fetcher,
        skip_auth_preflight=True,
    )

    assert len(metrics) == 1
    final_events = [
        event
        for event in logger.events
        if event.get("phase") == "order_line_items_rebuild"
        and event.get("message") == "Completed order_line_items historical rebuild"
    ]
    assert final_events[-1]["status"] == "ok"
    assert final_events[-1]["zero_snapshot_count"] == 1
    assert final_events[-1]["confirmed_empty_snapshot_count"] == 1
    assert final_events[-1]["ambiguous_zero_snapshot_count"] == 0


@pytest.mark.asyncio
async def test_uc_zero_snapshot_ambiguous_causes_final_warning(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    logger = _InMemoryLogger("uc-zero-ambiguous")

    async def fetcher(**_kwargs):
        return rebuild.SourceSnapshot(line_item_rows=[], order_snapshots=[])

    metrics = await rebuild.run_rebuild(
        source_selection="uc",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=True,
        run_id="uc-zero-ambiguous",
        logger=logger,
        fetch_snapshot=fetcher,
        skip_auth_preflight=True,
    )

    assert len(metrics) == 1
    final_events = [
        event
        for event in logger.events
        if event.get("phase") == "order_line_items_rebuild"
        and event.get("message") == "Completed order_line_items historical rebuild"
    ]
    assert final_events[-1]["status"] == "warning"
    assert final_events[-1]["zero_snapshot_count"] == 1
    assert final_events[-1]["ambiguous_zero_snapshot_count"] == 1
    assert final_events[-1]["confirmed_empty_snapshot_count"] == 0


@pytest.mark.asyncio
async def test_uc_zero_snapshot_ambiguous_with_fail_on_zero_snapshot_fails(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)

    async def fetcher(**_kwargs):
        return rebuild.SourceSnapshot(line_item_rows=[], order_snapshots=[])

    with pytest.raises(rebuild.OrderLineItemsZeroSnapshotDetected) as exc_info:
        await rebuild.run_rebuild(
            source_selection="uc",
            store_codes=None,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 1),
            window_size_days=1,
            dry_run=True,
            fail_on_zero_snapshot=True,
            run_id="uc-zero-ambiguous-fail",
            fetch_snapshot=fetcher,
            skip_auth_preflight=True,
        )

    assert exc_info.value.suspicious_window_count == 1


@pytest.mark.asyncio
async def test_uc_nonzero_extract_keeps_final_status_ok(
    patch_config_and_stores,
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    logger = _InMemoryLogger("uc-nonzero-ok")

    async def fetcher(**_kwargs):
        return rebuild.SourceSnapshot(
            line_item_rows=[
                {
                    "order_number": "ORD-UC-OK",
                    "line_hash": "line-1",
                    "item_name": "Shirt",
                    "service": "Wash",
                    "quantity": 1,
                }
            ],
            order_snapshots=[
                {
                    "order_number": "ORD-UC-OK",
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
        dry_run=True,
        run_id="uc-nonzero-ok",
        logger=logger,
        fetch_snapshot=fetcher,
        skip_auth_preflight=True,
    )

    assert len(metrics) == 1
    final_events = [
        event
        for event in logger.events
        if event.get("phase") == "order_line_items_rebuild"
        and event.get("message") == "Completed order_line_items historical rebuild"
    ]
    assert final_events[-1]["status"] == "ok"
    assert final_events[-1]["zero_snapshot_count"] == 0
    assert final_events[-1]["ambiguous_zero_snapshot_count"] == 0
    assert final_events[-1]["confirmed_empty_snapshot_count"] == 0


def test_cli_parser_accepts_fail_on_zero_snapshot() -> None:
    args = rebuild._build_parser().parse_args(
        ["--source", "td", "--dry-run", "--fail-on-zero-snapshot"]
    )

    assert args.dry_run is True
    assert args.fail_on_zero_snapshot is True


def test_cli_fail_on_zero_snapshot_exits_nonzero_for_ambiguous_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_run_rebuild(**kwargs):
        assert kwargs["fail_on_zero_snapshot"] is True
        raise rebuild.OrderLineItemsZeroSnapshotDetected(
            run_id="cli-ambiguous-zero",
            zero_window_count=1,
            expected_window_count=1,
            suspicious_window_count=1,
            zero_windows=("uc:UC001:2025-01-01..2025-01-01",),
        )

    monkeypatch.setattr(rebuild, "run_rebuild", fake_run_rebuild)

    with pytest.raises(SystemExit) as exc_info:
        rebuild.run(
            [
                "--source",
                "uc",
                "--start-date",
                "2025-01-01",
                "--end-date",
                "2025-01-01",
                "--fail-on-zero-snapshot",
            ]
        )

    assert exc_info.value.code == 1


def test_td_orders_and_rebuild_import_same_source_snapshot_helper() -> None:
    from app.crm_downloader.td_orders_sync.source_snapshot import (
        fetch_td_source_snapshot,
    )

    assert rebuild.fetch_td_source_snapshot is fetch_td_source_snapshot
    assert td_orders_main.fetch_td_source_snapshot is fetch_td_source_snapshot


@pytest.mark.asyncio
async def test_td_orders_sync_api_primary_uses_shared_source_snapshot_helper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[dict[str, Any]] = []
    api_result = td_orders_main.TdApiFetchResult(
        orders_rows=[{"order_number": "ORD-1"}],
        sales_rows=[{"order_number": "ORD-1"}],
        garments_rows=[{"order_number": "ORD-1", "garment_name": "Shirt"}],
        garment_order_snapshots=[
            {"order_number": "ORD-1", "garment_snapshot_outcome": "complete_with_rows"}
        ],
        endpoint_health={
            "/garments/details": {"garments_fetch_completeness": "complete"}
        },
        request_metadata=[{"endpoint": "/garments/details"}],
    )

    async def fake_fetch_td_source_snapshot(**kwargs: Any) -> Any:
        calls.append(kwargs)
        return rebuild.TdSourceSnapshotFetchResult(
            api_fetch_result=api_result,
            garments_rows=api_result.garments_rows,
            garment_order_snapshots=api_result.garment_order_snapshots,
            endpoint_health=api_result.endpoint_health,
            source_fetch_status=api_result.source_fetch_status,
            failure_class=None,
            source_fetch_error_class=None,
            request_metadata=api_result.request_metadata,
            endpoint_errors={},
            endpoint_error_diagnostics={},
            report_iframe_src=kwargs.get("report_iframe_src"),
        )

    monkeypatch.setattr(
        td_orders_main, "fetch_td_source_snapshot", fake_fetch_td_source_snapshot
    )
    monkeypatch.setattr(
        td_orders_main,
        "persist_td_api_artifacts",
        lambda **_kwargs: SimpleNamespace(warnings=[]),
    )

    store = td_orders_main.TdStore(
        store_code="TD001", store_name="TD001", cost_center="CC01", sync_config={}
    )
    _, _, returned_result, request_metadata = (
        await td_orders_main._execute_api_primary_ingestion(
            context=SimpleNamespace(name="context"),
            store=store,
            logger=JsonLogger(stream=io.StringIO(), log_file_path=None),
            source_mode="api_shadow",
            run_id="shared-helper-run",
            run_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
            run_start_date=date(2026, 1, 1),
            run_end_date=date(2026, 1, 1),
            run_orders=True,
            run_sales=True,
            download_dir=tmp_path,
            summary=td_orders_main.TdOrdersDiscoverySummary(
                run_id="shared-helper-run",
                run_env="test",
                report_date=date(2026, 1, 1),
                report_end_date=date(2026, 1, 1),
            ),
            stored_state_path="/tmp/state.json",
            report_iframe_src="https://reports.quickdrycleaning.com/orders?auth=1",
            context_source="iframe",
        )
    )

    assert returned_result is api_result
    assert request_metadata == [{"endpoint": "/garments/details"}]
    assert len(calls) == 1
    assert calls[0]["store"] is store
    assert calls[0]["source_config"].source_mode == "api_shadow"
    assert calls[0]["source_config"].context_source == "iframe"
    assert (
        calls[0]["report_iframe_src"]
        == "https://reports.quickdrycleaning.com/orders?auth=1"
    )


@pytest.mark.asyncio
async def test_default_fetch_snapshot_uc_home_readiness_failure_is_hard_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    playwright = SimpleNamespace(name="uc-playwright")
    browser = _FakeBrowser()
    logger = SimpleNamespace(name="uc-logger")

    monkeypatch.setattr(
        "playwright.async_api.async_playwright",
        lambda: _FakeAsyncPlaywright(playwright),
    )

    async def fake_launch_browser(*, playwright: Any, logger: Any) -> _FakeBrowser:
        return browser

    async def fake_collect_gst_orders_via_api(**kwargs: Any) -> Any:
        pytest.fail(
            "collect_gst_orders_via_api must not run after UC readiness failure"
        )

    async def fake_prepare_uc_api_page_for_store(**kwargs: Any) -> Any:
        return uc_orders_main.UcApiPagePreparationResult(
            ok=False,
            message="Home page marker not detected",
            login_used=True,
            session_probe_result=False,
            fallback_login_attempted=True,
            fallback_login_result=False,
        )

    monkeypatch.setattr(rebuild, "launch_browser", fake_launch_browser)
    monkeypatch.setattr(
        rebuild, "prepare_uc_api_page_for_store", fake_prepare_uc_api_page_for_store
    )
    monkeypatch.setattr(
        rebuild, "collect_gst_orders_via_api", fake_collect_gst_orders_via_api
    )

    with pytest.raises(RuntimeError, match="Home page marker not detected"):
        await rebuild.default_fetch_snapshot(
            source="uc",
            store=rebuild.RebuildStore(
                source="uc",
                store_code="UC001",
                cost_center="CC01",
                raw_store=SimpleNamespace(
                    home_url="https://example.test/home",
                    orders_url="https://example.test/orders",
                    storage_state_path=tmp_path / "uc.json",
                ),
            ),
            window=rebuild.RebuildWindow(date(2025, 1, 1), date(2025, 1, 2)),
            run_id="uc-run",
            logger=logger,
        )

    assert browser.closed is True


@pytest.mark.asyncio
async def test_td_overdue_popup_preflight_is_resumable_not_auth_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'td-popup-preflight.sqlite'}"
    logger = _InMemoryLogger("td-popup-preflight")
    storage_state = _write_td_storage_state(tmp_path / "TD001.json")
    store = rebuild.RebuildStore(
        source="td",
        store_code="TD001",
        cost_center="CC01",
        raw_store=SimpleNamespace(storage_state_path=storage_state),
        start_date=date(2025, 1, 1),
    )

    async def td_popup_status(*_args: Any, **_kwargs: Any) -> str:
        return rebuild.TD_OVERDUE_POPUP_READINESS_STATUS

    async def no_browser_check(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(rebuild, "_td_api_auth_readiness_status", td_popup_status)
    monkeypatch.setattr(rebuild, "_check_browser_launch_contract", no_browser_check)

    preflight = await rebuild.preflight_rebuild(
        database_url=db_url,
        sources=["td"],
        stores=[store],
        start_date=date(2025, 1, 1),
        requested_window_size_days=1,
        run_id="td-popup-preflight",
        logger=logger,  # type: ignore[arg-type]
    )

    assert preflight.auth_readiness == {"td:TD001": "overdue_popup_blocked"}
    assert any(
        event.get("phase") == "order_line_items_rebuild_preflight"
        and event.get("status") == "ok"
        and event.get("auth_failures") == []
        for event in logger.events
    )


@pytest.mark.asyncio
async def test_td_overdue_popup_actual_run_skips_td_window_and_uc_continues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'td-popup-continues.sqlite'}"
    await _create_common_tables(db_url)
    monkeypatch.setattr(rebuild, "config", SimpleNamespace(database_url=db_url))
    logger = _InMemoryLogger("td-popup-continues")

    async def load_stores(*, sources: Any, store_codes: Any, logger: Any) -> list[Any]:
        return [
            rebuild.RebuildStore(
                source="td",
                store_code="TD001",
                cost_center="CC01",
                raw_store=SimpleNamespace(
                    storage_state_path=_write_td_storage_state(tmp_path / "TD001.json")
                ),
                start_date=date(2025, 1, 1),
            ),
            rebuild.RebuildStore(
                source="uc",
                store_code="UC001",
                cost_center="CC02",
                raw_store=SimpleNamespace(
                    storage_state_path=_write_uc_storage_state(tmp_path / "UC001.json")
                ),
                start_date=date(2025, 1, 1),
            ),
        ]

    async def no_browser_check(*_args: Any, **_kwargs: Any) -> None:
        return None

    async def td_auth_ready(*_args: Any, **_kwargs: Any) -> str:
        return "session_valid"

    async def fetch_snapshot(
        *, source: str, store: Any, window: Any, **_kwargs: Any
    ) -> rebuild.SourceSnapshot:
        if source == "td":
            raise td_orders_main.TdOrdersOverduePopupBlocked(
                store_code=store.store_code
            )
        return rebuild.SourceSnapshot(
            line_item_rows=[],
            order_snapshots=[
                {"order_number": "UC-1", "snapshot_outcome": "complete_empty"}
            ],
            zero_snapshot_class="confirmed_source_empty",
        )

    monkeypatch.setattr(rebuild, "load_rebuild_stores", load_stores)
    monkeypatch.setattr(rebuild, "_check_browser_launch_contract", no_browser_check)
    monkeypatch.setattr(rebuild, "_td_api_auth_readiness_status", td_auth_ready)

    metrics = await rebuild.run_rebuild(
        source_selection="both",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=True,
        run_id="td-popup-continues",
        logger=logger,  # type: ignore[arg-type]
        fetch_snapshot=fetch_snapshot,  # type: ignore[arg-type]
    )

    assert [(metric.source, metric.store_code) for metric in metrics] == [
        ("uc", "UC001")
    ]
    assert any(
        event.get("phase") == "order_line_items_rebuild_window"
        and event.get("source") == "td"
        and event.get("skip_status") == "resumable"
        and event.get("reason") == td_orders_main.ORDERS_OVERDUE_POPUP_BLOCKED_REASON
        for event in logger.events
    )
    assert any(
        event.get("phase") == "order_line_items_rebuild_window"
        and event.get("status") == "ok"
        and event.get("source") == "uc"
        for event in logger.events
    )
    missing_event = [
        event
        for event in logger.events
        if event.get("phase") == "order_line_items_rebuild_missing_windows"
    ][-1]
    assert missing_event["missing_window_count"] == 0
    assert missing_event["skipped_window_count"] == 1
    assert missing_event["resumable_skipped_window_count"] == 1
    assert missing_event["skipped_windows"] == [
        {
            "source": "td",
            "store_code": "TD001",
            "cost_center": "CC01",
            "window_start": "2025-01-01",
            "window_end": "2025-01-01",
            "reason": td_orders_main.ORDERS_OVERDUE_POPUP_BLOCKED_REASON,
            "skip_status": "resumable",
            "retryable": True,
        }
    ]
    final_event = [
        event
        for event in logger.events
        if event.get("phase") == "order_line_items_rebuild"
        and event.get("message")
        == "Completed order_line_items historical rebuild with resumable skipped windows"
    ][-1]
    assert final_event["status"] == "warning"
    assert final_event["missing_window_count"] == 0
    assert final_event["skipped_window_count"] == 1
    assert final_event["resumable_skipped_window_count"] == 1


@pytest.mark.asyncio
async def test_td_overdue_popup_does_not_mutate_rows_or_write_success_progress_and_resume_retries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'td-popup-resume.sqlite'}"
    await _create_common_tables(db_url)
    monkeypatch.setattr(rebuild, "config", SimpleNamespace(database_url=db_url))
    logger = _InMemoryLogger("td-popup-resume")
    td_store = rebuild.RebuildStore(
        source="td",
        store_code="TD001",
        cost_center="CC01",
        raw_store=SimpleNamespace(
            storage_state_path=_write_td_storage_state(tmp_path / "TD001.json")
        ),
        start_date=date(2025, 1, 1),
    )

    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                "INSERT INTO order_line_items "
                "(run_id, run_date, cost_center, store_code, order_number, line_sequence, line_item_key, line_item_uid, ingest_row_seq) "
                "VALUES ('seed', '2025-01-01', 'CC01', 'TD001', 'TD-OLD', 1, 'old-key', 'old-uid', 1)"
            )
        )
        await session.commit()

    async def load_stores(*_args: Any, **_kwargs: Any) -> list[Any]:
        return [td_store]

    async def no_browser_check(*_args: Any, **_kwargs: Any) -> None:
        return None

    async def td_auth_ready(*_args: Any, **_kwargs: Any) -> str:
        return "session_valid"

    fetch_calls: list[str] = []

    async def popup_fetch(*, store: Any, **_kwargs: Any) -> rebuild.SourceSnapshot:
        fetch_calls.append(store.store_code)
        raise td_orders_main.TdOrdersOverduePopupBlocked(store_code=store.store_code)

    monkeypatch.setattr(rebuild, "load_rebuild_stores", load_stores)
    monkeypatch.setattr(rebuild, "_check_browser_launch_contract", no_browser_check)
    monkeypatch.setattr(rebuild, "_td_api_auth_readiness_status", td_auth_ready)

    first_metrics = await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=False,
        run_id="td-popup-first",
        logger=logger,  # type: ignore[arg-type]
        fetch_snapshot=popup_fetch,  # type: ignore[arg-type]
    )

    assert first_metrics == []

    rows = await _rows(
        db_url,
        "SELECT run_id, order_number, line_item_uid FROM order_line_items ORDER BY id",
    )
    assert [(row.run_id, row.order_number, row.line_item_uid) for row in rows] == [
        ("seed", "TD-OLD", "old-uid")
    ]
    progress_rows = await _rows(
        db_url,
        "SELECT status FROM order_line_items_rebuild_progress",
    )
    assert progress_rows == []

    resume_metrics = await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=True,
        resume=True,
        run_id="td-popup-resume",
        logger=logger,  # type: ignore[arg-type]
        fetch_snapshot=popup_fetch,  # type: ignore[arg-type]
    )

    assert resume_metrics == []
    assert fetch_calls == ["TD001", "TD001"]
    final_events = [
        event
        for event in logger.events
        if event.get("phase") == "order_line_items_rebuild"
        and event.get("message")
        == "Completed order_line_items historical rebuild with resumable skipped windows"
    ]
    assert final_events[-1]["status"] == "warning"
    assert final_events[-1]["resume"] is True
    assert final_events[-1]["skipped_windows"][0]["reason"] == (
        td_orders_main.ORDERS_OVERDUE_POPUP_BLOCKED_REASON
    )


@pytest.mark.asyncio
async def test_run_rebuild_persists_summary_and_sends_notifications(
    patch_config_and_stores, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    summary_records: list[dict[str, Any]] = []
    notification_calls: list[tuple[str, str]] = []

    async def fake_insert_run_summary(
        database_url: str, record: dict[str, Any]
    ) -> None:
        assert database_url == db_url
        summary_records.append(record)

    async def fake_send_notifications_for_run(
        pipeline_name: str, run_id: str
    ) -> dict[str, Any]:
        notification_calls.append((pipeline_name, run_id))
        return {"emails_planned": 1, "emails_sent": 1, "errors": []}

    async def fetcher(**_kwargs):
        return rebuild.SourceSnapshot(
            line_item_rows=[{"order_number": "TD-1", "line_item_key": "k"}],
            order_snapshots=[
                {"order_number": "TD-1", "snapshot_outcome": "complete_with_rows"}
            ],
        )

    monkeypatch.setattr(rebuild, "insert_run_summary", fake_insert_run_summary)
    monkeypatch.setattr(
        rebuild, "send_notifications_for_run", fake_send_notifications_for_run
    )

    await rebuild.run_rebuild(
        source_selection="td",
        store_codes=None,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        window_size_days=1,
        dry_run=True,
        run_id="summary-success",
        fetch_snapshot=fetcher,
        skip_auth_preflight=True,
    )

    assert notification_calls == [(rebuild.PIPELINE_NAME, "summary-success")]
    assert len(summary_records) == 1
    record = summary_records[0]
    assert record["pipeline_name"] == rebuild.PIPELINE_NAME
    assert record["run_id"] == "summary-success"
    assert record["overall_status"] == "success"
    assert (
        record["metrics_json"]["notification_payload"]["run_id"] == "summary-success"
    )
    assert record["metrics_json"]["expected_window_count"] == 1
    assert record["metrics_json"]["completed_window_count"] == 1


@pytest.mark.asyncio
async def test_run_rebuild_failure_persists_failed_summary_before_reraising(
    patch_config_and_stores, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_url = patch_config_and_stores
    await _create_common_tables(db_url)
    summary_records: list[dict[str, Any]] = []
    notification_calls: list[tuple[str, str]] = []

    async def fake_insert_run_summary(
        _database_url: str, record: dict[str, Any]
    ) -> None:
        summary_records.append(record)

    async def fake_send_notifications_for_run(
        pipeline_name: str, run_id: str
    ) -> dict[str, Any]:
        notification_calls.append((pipeline_name, run_id))
        return {"emails_planned": 0, "emails_sent": 0, "errors": []}

    async def fetcher(**_kwargs):
        raise TypeError("systemic setup exploded")

    monkeypatch.setattr(rebuild, "insert_run_summary", fake_insert_run_summary)
    monkeypatch.setattr(
        rebuild, "send_notifications_for_run", fake_send_notifications_for_run
    )

    with pytest.raises(TypeError, match="systemic setup exploded"):
        await rebuild.run_rebuild(
            source_selection="td",
            store_codes=None,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 1),
            window_size_days=1,
            dry_run=True,
            run_id="summary-failed",
            fetch_snapshot=fetcher,
            skip_auth_preflight=True,
        )

    assert notification_calls == [(rebuild.PIPELINE_NAME, "summary-failed")]
    assert len(summary_records) == 1
    record = summary_records[0]
    assert record["overall_status"] == "failed"
    assert (
        record["metrics_json"]["notification_payload"]["overall_status"] == "failed"
    )
    assert "TypeError" in record["summary_text"]
