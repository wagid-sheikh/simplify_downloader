from __future__ import annotations

import asyncio
import importlib.util
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
        json.dumps({"accessToken": token})
        if token
        else json.dumps({"other": "value"})
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

    monkeypatch.setattr(rebuild, "load_rebuild_stores", load_stores)
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

    async def new_page(self) -> "_FakePage":
        page = _FakePage()
        self.pages.append(page)
        return page


class _FakePage:
    def __init__(self) -> None:
        self.goto_calls: list[tuple[str, str | None]] = []

    async def goto(self, url: str, *, wait_until: str | None = None) -> None:
        self.goto_calls.append((url, wait_until))


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["td", "uc"])
async def test_default_fetch_snapshot_launches_browser_with_keyword_arguments(
    monkeypatch: pytest.MonkeyPatch, source: rebuild.Source
) -> None:
    playwright = SimpleNamespace(name=f"{source}-playwright")
    browser = _FakeBrowser()
    logger = SimpleNamespace(name=f"{source}-logger")
    launch_calls: list[tuple[Any, Any]] = []

    monkeypatch.setattr(
        "playwright.async_api.async_playwright",
        lambda: _FakeAsyncPlaywright(playwright),
    )

    async def fake_launch_browser(*, playwright: Any, logger: Any) -> _FakeBrowser:
        launch_calls.append((playwright, logger))
        return browser

    monkeypatch.setattr(rebuild, "launch_browser", fake_launch_browser)

    class FakeTdApiClient:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs

        async def fetch_reports(self, **kwargs: Any) -> Any:
            return SimpleNamespace(garments_rows=[], garment_order_snapshots=[])

    async def fake_collect_gst_orders_via_api(**kwargs: Any) -> Any:
        return SimpleNamespace(order_detail_rows=[], order_detail_snapshot_rows=[])

    monkeypatch.setattr(rebuild, "TdApiClient", FakeTdApiClient)
    monkeypatch.setattr(
        rebuild, "collect_gst_orders_via_api", fake_collect_gst_orders_via_api
    )

    snapshot = await rebuild.default_fetch_snapshot(
        source=source,
        store=rebuild.RebuildStore(
            source=source,
            store_code=f"{source.upper()}001",
            cost_center="CC01",
            raw_store=SimpleNamespace(home_url="https://example.test/home"),
        ),
        window=rebuild.RebuildWindow(date(2025, 1, 1), date(2025, 1, 2)),
        run_id=f"{source}-run",
        logger=logger,
    )

    assert snapshot == rebuild.SourceSnapshot(line_item_rows=[], order_snapshots=[])
    assert launch_calls == [(playwright, logger)]
    assert browser.closed is True


@pytest.mark.asyncio
async def test_default_fetch_snapshot_raises_on_td_unauthorized_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    playwright = SimpleNamespace(name="td-playwright")
    browser = _FakeBrowser()
    logger = SimpleNamespace(name="td-logger")

    monkeypatch.setattr(
        "playwright.async_api.async_playwright",
        lambda: _FakeAsyncPlaywright(playwright),
    )

    async def fake_launch_browser(*, playwright: Any, logger: Any) -> _FakeBrowser:
        return browser

    class FakeTdApiClient:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs

        async def fetch_reports(self, **kwargs: Any) -> Any:
            return SimpleNamespace(
                garments_rows=[],
                garment_order_snapshots=[],
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

    monkeypatch.setattr(rebuild, "launch_browser", fake_launch_browser)
    monkeypatch.setattr(rebuild, "TdApiClient", FakeTdApiClient)

    with pytest.raises(rebuild.TdApiUnauthorizedError, match="TD API unauthorized"):
        await rebuild.default_fetch_snapshot(
            source="td",
            store=rebuild.RebuildStore(
                source="td",
                store_code="TD001",
                cost_center="CC01",
            ),
            window=rebuild.RebuildWindow(date(2025, 1, 1), date(2025, 1, 1)),
            run_id="td-unauthorized",
            logger=logger,
        )

    assert browser.closed is True


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


def test_cli_exits_nonzero_when_all_td_windows_are_unauthorized(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'cli-unauthorized.sqlite'}"
    asyncio.run(_create_common_tables(db_url))
    monkeypatch.setattr(rebuild, "config", SimpleNamespace(database_url=db_url))
    monkeypatch.setattr(
        "playwright.async_api.async_playwright",
        lambda: _FakeAsyncPlaywright(SimpleNamespace(name="td-playwright")),
    )

    async def load_stores(*, sources, store_codes, logger):
        return [
            rebuild.RebuildStore(source="td", store_code="TD001", cost_center="CC01")
        ]

    async def fake_launch_browser(*, playwright: Any, logger: Any) -> _FakeBrowser:
        return _FakeBrowser()

    class FakeTdApiClient:
        def __init__(self, **kwargs: Any) -> None:
            pass

        async def fetch_reports(self, **kwargs: Any) -> Any:
            return SimpleNamespace(
                garments_rows=[],
                garment_order_snapshots=[],
                endpoint_errors={
                    "/reports/order-report": "http_401",
                    "/sales-and-deliveries/sales": "http_401",
                    "/garments/details": "http_401",
                },
                endpoint_health={},
                source_fetch_status="auth_failed",
                source_fetch_error_class="http_401",
                source_fetch_failed_endpoints=[
                    "/reports/order-report",
                    "/sales-and-deliveries/sales",
                    "/garments/details",
                ],
            )

    monkeypatch.setattr(rebuild, "load_rebuild_stores", load_stores)
    monkeypatch.setattr(rebuild, "launch_browser", fake_launch_browser)
    monkeypatch.setattr(rebuild, "TdApiClient", FakeTdApiClient)

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

    monkeypatch.setattr(rebuild, "load_rebuild_stores", load_stores)
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

    monkeypatch.setattr(rebuild, "load_rebuild_stores", load_stores)
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
        "td:A668": "authorized",
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


def test_cli_parser_accepts_fail_on_zero_snapshot() -> None:
    args = rebuild._build_parser().parse_args(
        ["--source", "td", "--dry-run", "--fail-on-zero-snapshot"]
    )

    assert args.dry_run is True
    assert args.fail_on_zero_snapshot is True
