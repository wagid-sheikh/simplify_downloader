from __future__ import annotations

import io
import json
from dataclasses import replace
from types import SimpleNamespace
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from app.crm_downloader.td_orders_sync import main as td_orders_main
from app.dashboard_downloader.json_logger import JsonLogger


class _FakePage:
    url = "https://subs.quickdrycleaning.com/a817/App/home"


class _FakeContext:
    def __init__(self, page: _FakePage) -> None:
        self._page = page

    async def new_page(self) -> _FakePage:
        return self._page

    async def storage_state(self, *, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(
            '{"cookies": [{"name": "sid", "value": "1"}], "origins": []}',
            encoding="utf-8",
        )

    async def close(self) -> None:
        return None


class _FakeBrowser:
    def __init__(self, page: _FakePage) -> None:
        self._page = page

    async def new_context(self, **_kwargs: object) -> _FakeContext:
        return _FakeContext(self._page)


@pytest.mark.asyncio
async def test_api_only_modal_blocked_orders_navigation_is_warning_when_api_has_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(td_orders_main, "default_profiles_dir", lambda: tmp_path)
    monkeypatch.setattr(
        td_orders_main, "_resolve_td_api_artifact_dir", lambda: tmp_path
    )

    async def _insert_log(**_kwargs: object) -> int:
        return 1

    async def _noop_async(*_args: object, **_kwargs: object) -> None:
        return None

    async def _login(*_args: object, **_kwargs: object) -> bool:
        return True

    async def _home(*_args: object, **_kwargs: object) -> bool:
        return True

    async def _nav_blocked(*_args: object, **_kwargs: object) -> bool:
        return False

    async def _api_success(**_kwargs: object):
        orders_report = td_orders_main.StoreReport(
            status="ok",
            source_mode="api_only",
            rows_downloaded=2,
            rows_ingested=2,
            final_rows=2,
            compare_rows_orders=[
                {"order_number": "A817-1"},
                {"order_number": "A817-2"},
            ],
        )
        sales_report = td_orders_main.StoreReport(
            status="ok",
            source_mode="api_only",
            rows_downloaded=1,
            rows_ingested=1,
            final_rows=1,
            compare_rows_sales=[{"order_number": "A817-1"}],
        )
        fetch_result = td_orders_main.TdApiFetchResult(
            orders_rows=[{"order_number": "A817-1"}, {"order_number": "A817-2"}],
            sales_rows=[{"order_number": "A817-1"}],
            endpoint_health={
                "/garments/details": {"garments_fetch_completeness": "complete"}
            },
        )
        return orders_report, sales_report, fetch_result, []

    monkeypatch.setattr(td_orders_main, "_insert_orders_sync_log", _insert_log)
    monkeypatch.setattr(td_orders_main, "_update_orders_sync_log", _noop_async)
    monkeypatch.setattr(td_orders_main, "_perform_login", _login)
    monkeypatch.setattr(td_orders_main, "_wait_for_home", _home)
    monkeypatch.setattr(td_orders_main, "_navigate_to_orders_container", _nav_blocked)
    monkeypatch.setattr(td_orders_main, "_execute_api_primary_ingestion", _api_success)
    monkeypatch.setattr(td_orders_main, "_log_home_nav_diagnostics", _noop_async)

    async def _no_orphan_alert(*_args: object, **_kwargs: object) -> dict[str, object]:
        return {"triggered": False}

    monkeypatch.setattr(
        td_orders_main, "_evaluate_garment_orphan_alert", _no_orphan_alert
    )

    summary = td_orders_main.TdOrdersDiscoverySummary(
        run_id="run-modal",
        run_env="test",
        report_date=date(2026, 1, 1),
        report_end_date=date(2026, 1, 1),
    )
    store = td_orders_main.TdStore(
        store_code="A817", store_name="A817", cost_center="CC-A817", sync_config={}
    )

    payload = await td_orders_main._run_store_discovery(
        browser=_FakeBrowser(_FakePage()),
        store=store,
        logger=JsonLogger(stream=io.StringIO(), log_file_path=None),
        run_env="test",
        run_id="run-modal",
        run_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        run_start_date=date(2026, 1, 1),
        run_end_date=date(2026, 1, 1),
        nav_timeout_ms=10,
        summary=summary,
        run_orders=True,
        run_sales=True,
        source_mode="api_only",
    )
    summary.record_store(
        store.store_code,
        payload.outcome,
        orders_result=payload.orders_report,
        sales_result=payload.sales_report,
    )
    record = summary.build_record(finished_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
    store_summary = record["metrics_json"]["stores_summary"]["stores"]["A817"]

    assert payload.outcome is not None
    assert payload.outcome.status == "warning"
    assert payload.outcome.ingest_status == "success"
    assert payload.outcome.failure_stage is None
    assert payload.orders_report is not None
    assert td_orders_main.UI_ORDERS_NAVIGATION_WARNING in payload.orders_report.warnings
    assert store_summary["status"] == "warning"
    assert store_summary["data_ingest_status"] == "success"
    assert store_summary["failure_stage"] is None


@pytest.mark.asyncio
async def test_successful_garment_replacement_logs_ok_without_failure_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(td_orders_main, "default_profiles_dir", lambda: tmp_path)
    monkeypatch.setattr(td_orders_main, "_resolve_td_api_artifact_dir", lambda: tmp_path)
    monkeypatch.setattr(
        td_orders_main,
        "config",
        replace(
            td_orders_main.config, database_url="sqlite+aiosqlite:///garments-ok.db"
        ),
    )

    async def _insert_log(**_kwargs: object) -> int:
        return 1

    async def _noop_async(*_args: object, **_kwargs: object) -> None:
        return None

    async def _login(*_args: object, **_kwargs: object) -> bool:
        return True

    async def _home(*_args: object, **_kwargs: object) -> bool:
        return True

    async def _nav_blocked(*_args: object, **_kwargs: object) -> bool:
        return False

    async def _api_success(**_kwargs: object):
        orders_report = td_orders_main.StoreReport(
            status="ok",
            source_mode="api_only",
            rows_downloaded=1,
            rows_ingested=1,
            final_rows=1,
            compare_rows_orders=[{"order_number": "A817-1"}],
        )
        sales_report = td_orders_main.StoreReport(
            status="ok",
            source_mode="api_only",
            rows_downloaded=1,
            rows_ingested=1,
            final_rows=1,
            compare_rows_sales=[{"order_number": "A817-1"}],
        )
        fetch_result = td_orders_main.TdApiFetchResult(
            orders_rows=[{"order_number": "A817-1"}],
            sales_rows=[{"order_number": "A817-1"}],
            garments_rows=[{"order_number": "A817-1", "garment_name": "Shirt"}],
            garment_order_snapshots=[
                {
                    "order_number": "A817-1",
                    "garment_snapshot_outcome": "complete_with_rows",
                }
            ],
            endpoint_health={
                "/garments/details": {"garments_fetch_completeness": "complete"}
            },
        )
        return orders_report, sales_report, fetch_result, []

    async def _garment_ingest_success(
        **_kwargs: object,
    ) -> td_orders_main.TdGarmentIngestResult:
        return td_orders_main.TdGarmentIngestResult(
            staging_rows=1,
            staging_inserted=1,
            staging_updated=0,
            final_rows=1,
            final_inserted=1,
            final_updated=0,
            row_count=1,
            source_id_duplicate_rows=0,
            changed_rows=1,
            late_updates=0,
            orphan_rows=0,
            authoritative_orders_inspected=1,
            complete_with_rows_orders=1,
            complete_empty_orders=0,
            replacement_skipped_incomplete_orders=0,
            deleted_final_rows=0,
            inserted_final_rows=1,
        )

    async def _no_orphan_alert(*_args: object, **_kwargs: object) -> dict[str, object]:
        return {"triggered": False}

    monkeypatch.setattr(td_orders_main, "_insert_orders_sync_log", _insert_log)
    monkeypatch.setattr(td_orders_main, "_update_orders_sync_log", _noop_async)
    monkeypatch.setattr(td_orders_main, "_perform_login", _login)
    monkeypatch.setattr(td_orders_main, "_wait_for_home", _home)
    monkeypatch.setattr(td_orders_main, "_navigate_to_orders_container", _nav_blocked)
    monkeypatch.setattr(td_orders_main, "_execute_api_primary_ingestion", _api_success)
    monkeypatch.setattr(td_orders_main, "ingest_td_garment_rows", _garment_ingest_success)
    monkeypatch.setattr(td_orders_main, "_log_home_nav_diagnostics", _noop_async)
    monkeypatch.setattr(td_orders_main, "_evaluate_garment_orphan_alert", _no_orphan_alert)

    stream = io.StringIO()
    summary = td_orders_main.TdOrdersDiscoverySummary(
        run_id="run-garment-ok",
        run_env="test",
        report_date=date(2026, 1, 1),
        report_end_date=date(2026, 1, 1),
    )
    store = td_orders_main.TdStore(
        store_code="A817", store_name="A817", cost_center="CC-A817", sync_config={}
    )

    await td_orders_main._run_store_discovery(
        browser=_FakeBrowser(_FakePage()),
        store=store,
        logger=JsonLogger(stream=stream, log_file_path=None),
        run_env="test",
        run_id="run-garment-ok",
        run_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        run_start_date=date(2026, 1, 1),
        run_end_date=date(2026, 1, 1),
        nav_timeout_ms=10,
        summary=summary,
        run_orders=True,
        run_sales=True,
        source_mode="api_only",
    )

    events = [json.loads(line) for line in stream.getvalue().splitlines()]
    replacement_event = next(
        event
        for event in events
        if event.get("message") == "TD garment snapshot replacement completed"
    )

    assert replacement_event["status"] == "ok"
    assert not any(
        event.get("message") == "TD garment sync failed" for event in events
    )
    assert {event["status"] for event in events} <= {
        "debug",
        "info",
        "ok",
        "warning",
        "error",
    }


def test_a817_summary_distinguishes_api_ingest_ui_navigation_and_garment_warnings() -> (
    None
):
    summary = td_orders_main.TdOrdersDiscoverySummary(
        run_id="run-a817",
        run_env="test",
        report_date=date(2026, 1, 2),
        report_end_date=date(2026, 1, 2),
    )
    summary.record_store(
        "A817",
        td_orders_main.StoreOutcome(
            status="warning",
            message=td_orders_main.API_ONLY_NAVIGATION_WARNING_MESSAGE,
        ),
        orders_result=td_orders_main.StoreReport(
            status="warning",
            source_mode="api_only",
            rows_downloaded=3,
            rows_ingested=3,
            warnings=[
                td_orders_main.UI_ORDERS_NAVIGATION_WARNING,
                "DATA INCOMPLETE: TD garment details fetch incomplete; reason=pagination budget exhausted",
            ],
            garments_fetch_completeness="incomplete",
            garments_incomplete_reason={"message": "pagination budget exhausted"},
        ),
        sales_result=td_orders_main.StoreReport(
            status="ok", source_mode="api_only", rows_downloaded=2, rows_ingested=2
        ),
    )
    summary.record_store(
        "A818",
        td_orders_main.StoreOutcome(
            status="error", message="TD API ingestion failed", failure_stage="ingest"
        ),
        orders_result=td_orders_main.StoreReport(
            status="error",
            source_mode="api_only",
            rows_downloaded=2,
            error_message="schema mismatch",
        ),
        sales_result=td_orders_main.StoreReport(
            status="ok", source_mode="api_only", rows_downloaded=2
        ),
    )

    record = summary.build_record(finished_at=datetime(2026, 1, 2, tzinfo=timezone.utc))
    stores = record["metrics_json"]["stores_summary"]["stores"]

    assert stores["A817"]["data_ingest_status"] == "success"
    assert stores["A817"]["failure_stage"] is None
    assert (
        td_orders_main.UI_ORDERS_NAVIGATION_WARNING
        in stores["A817"]["observability_warnings"]
    )
    assert any(
        "garment details fetch incomplete" in warning
        for warning in stores["A817"]["observability_warnings"]
    )
    assert stores["A818"]["data_ingest_status"] == "failed"
    assert stores["A818"]["failure_stage"] == "ingest"


class _NavLocator:
    def __init__(self, page: "_OverduePopupPage", selector: str, *, visible: bool) -> None:
        self.page = page
        self.selector = selector
        self._visible = visible

    @property
    def first(self) -> "_NavLocator":
        return self

    async def count(self) -> int:
        return 1 if self._visible else 0

    async def is_visible(self) -> bool:
        return self._visible

    async def wait_for(self, *_args: object, **_kwargs: object) -> None:
        if not self._visible:
            raise TimeoutError("locator not visible")

    async def click(self, *_args: object, **_kwargs: object) -> None:
        self.page.clicks.append(self.selector)


class _OverduePopupPage:
    url = "https://subs.quickdrycleaning.com/a817/App/home"

    def __init__(self) -> None:
        self.clicks: list[str] = []
        self.reloads = 0

    def locator(self, selector: str) -> _NavLocator:
        return _NavLocator(
            self,
            selector,
            visible=selector == td_orders_main.OVERDUE_ORDERS_POPUP_SELECTOR,
        )

    def get_by_role(self, *_args: object, **_kwargs: object) -> _NavLocator:
        return _NavLocator(self, "role", visible=False)

    async def reload(self, *_args: object, **_kwargs: object) -> None:
        self.reloads += 1

    async def title(self) -> str:
        return "Home"


@pytest.mark.asyncio
async def test_visible_overdue_popup_skips_orders_navigation_without_dismissal_or_retry() -> None:
    stream = io.StringIO()
    page = _OverduePopupPage()
    store = td_orders_main.TdStore(
        store_code="A817", store_name="A817", cost_center="CC-A817", sync_config={}
    )

    result = await td_orders_main._navigate_to_orders_container(
        page,
        store=store,
        logger=JsonLogger(stream=stream, log_file_path=None),
        nav_selector="#achrOrderReport",
        nav_timeout_ms=10,
    )

    events = [json.loads(line) for line in stream.getvalue().splitlines()]
    warning = next(
        event
        for event in events
        if event.get("message") == td_orders_main.ORDERS_OVERDUE_POPUP_BLOCKED_MESSAGE
    )

    assert result.ready is False
    assert result.reason == td_orders_main.ORDERS_OVERDUE_POPUP_BLOCKED_REASON
    assert result.modal_selector == td_orders_main.OVERDUE_ORDERS_POPUP_SELECTOR
    assert page.clicks == []
    assert page.reloads == 0
    assert warning["phase"] == "orders"
    assert warning["status"] == "warning"
    assert warning["store_code"] == "A817"
    assert warning["modal_selector"] == td_orders_main.OVERDUE_ORDERS_POPUP_SELECTOR
    assert warning["reason"] == td_orders_main.ORDERS_OVERDUE_POPUP_BLOCKED_REASON
    assert not any(
        event.get("message") == "Overlay blocking Orders click; retrying"
        for event in events
    )


@pytest.mark.asyncio
async def test_overdue_popup_store_skip_does_not_stop_other_api_only_stores(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(td_orders_main, "default_profiles_dir", lambda: tmp_path)
    monkeypatch.setattr(td_orders_main, "_resolve_td_api_artifact_dir", lambda: tmp_path)

    async def _insert_log(**_kwargs: object) -> int:
        return 1

    async def _noop_async(*_args: object, **_kwargs: object) -> None:
        return None

    async def _login(*_args: object, **_kwargs: object) -> bool:
        return True

    async def _home(*_args: object, **_kwargs: object) -> bool:
        return True

    async def _nav_by_store(*_args: object, **kwargs: object):
        store = kwargs["store"]
        if store.store_code == "A817":
            return td_orders_main.OrdersNavigationResult(
                ready=False,
                reason=td_orders_main.ORDERS_OVERDUE_POPUP_BLOCKED_REASON,
                modal_selector=td_orders_main.OVERDUE_ORDERS_POPUP_SELECTOR,
            )
        return False

    api_calls: list[str] = []

    async def _api_success(**kwargs: object):
        store = kwargs["store"]
        api_calls.append(store.store_code)
        orders_report = td_orders_main.StoreReport(
            status="ok",
            source_mode="api_only",
            rows_downloaded=1,
            rows_ingested=1,
            final_rows=1,
            compare_rows_orders=[{"order_number": f"{store.store_code}-1"}],
        )
        sales_report = td_orders_main.StoreReport(
            status="ok",
            source_mode="api_only",
            rows_downloaded=1,
            rows_ingested=1,
            final_rows=1,
            compare_rows_sales=[{"order_number": f"{store.store_code}-1"}],
        )
        fetch_result = td_orders_main.TdApiFetchResult(
            orders_rows=[{"order_number": f"{store.store_code}-1"}],
            sales_rows=[{"order_number": f"{store.store_code}-1"}],
        )
        return orders_report, sales_report, fetch_result, []

    async def _no_orphan_alert(*_args: object, **_kwargs: object) -> dict[str, object]:
        return {"triggered": False}

    monkeypatch.setattr(td_orders_main, "_insert_orders_sync_log", _insert_log)
    monkeypatch.setattr(td_orders_main, "_update_orders_sync_log", _noop_async)
    monkeypatch.setattr(td_orders_main, "_perform_login", _login)
    monkeypatch.setattr(td_orders_main, "_wait_for_home", _home)
    monkeypatch.setattr(td_orders_main, "_navigate_to_orders_container", _nav_by_store)
    monkeypatch.setattr(td_orders_main, "_execute_api_primary_ingestion", _api_success)
    monkeypatch.setattr(td_orders_main, "_log_home_nav_diagnostics", _noop_async)
    monkeypatch.setattr(td_orders_main, "_evaluate_garment_orphan_alert", _no_orphan_alert)

    summary = td_orders_main.TdOrdersDiscoverySummary(
        run_id="run-overdue",
        run_env="test",
        report_date=date(2026, 1, 1),
        report_end_date=date(2026, 1, 1),
    )
    stores = [
        td_orders_main.TdStore(
            store_code="A817", store_name="A817", cost_center="CC-A817", sync_config={}
        ),
        td_orders_main.TdStore(
            store_code="A818", store_name="A818", cost_center="CC-A818", sync_config={}
        ),
    ]

    payloads = []
    for store in stores:
        payload = await td_orders_main._run_store_discovery(
            browser=_FakeBrowser(_FakePage()),
            store=store,
            logger=JsonLogger(stream=io.StringIO(), log_file_path=None),
            run_env="test",
            run_id="run-overdue",
            run_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
            run_start_date=date(2026, 1, 1),
            run_end_date=date(2026, 1, 1),
            nav_timeout_ms=10,
            summary=summary,
            run_orders=True,
            run_sales=True,
            source_mode="api_only",
        )
        payloads.append(payload)
        summary.record_store(
            store.store_code,
            payload.outcome,
            orders_result=payload.orders_report,
            sales_result=payload.sales_report,
        )

    record = summary.build_record(finished_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
    stores_summary = record["metrics_json"]["stores_summary"]["stores"]

    assert api_calls == ["A818"]
    assert payloads[0].outcome is not None
    assert payloads[0].outcome.failure_stage == td_orders_main.ORDERS_OVERDUE_POPUP_BLOCKED_REASON
    assert payloads[0].orders_report is not None
    assert payloads[0].orders_report.status == "skipped"
    assert payloads[1].outcome is not None
    assert payloads[1].outcome.status == "warning"
    assert stores_summary["A817"]["failure_stage"] == td_orders_main.ORDERS_OVERDUE_POPUP_BLOCKED_REASON
    assert stores_summary["A817"]["data_ingest_status"] == "skipped"
    assert stores_summary["A818"]["data_ingest_status"] == "success"


@pytest.mark.asyncio
async def test_summary_only_td_orders_api_response_is_clean_store_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[dict[str, object]] = []

    class _FakeTdApiClient:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def read_session_artifact(self) -> dict[str, object]:
            return {"cookies": [{"name": "sid"}], "origins": []}

        async def fetch_reports(self, **_kwargs: object) -> td_orders_main.TdApiFetchResult:
            return td_orders_main.TdApiFetchResult(
                raw_orders_payload={
                    "data": {
                        "rows": [
                            {
                                "order_number": "Total Order",
                                "order_amount": "0.00",
                                "row_type": "summary",
                            }
                        ],
                        "count": 1,
                    }
                },
                raw_sales_payload={"data": {"rows": [{"order_number": "A668-1"}], "count": 1}},
                raw_garments_payload={"data": {"rows": [], "count": 0}},
                orders_rows=[],
                orders_summary_rows_filtered=1,
                sales_rows=[{"order_number": "A668-1"}],
                garments_rows=[],
                endpoint_errors={},
                endpoint_health={
                    "/reports/order-report": {
                        "success": True,
                        "reported_total_rows": 1,
                        "parsed_row_count": 0,
                    },
                    "/sales-and-deliveries/sales": {
                        "success": True,
                        "reported_total_rows": 1,
                        "parsed_row_count": 1,
                    },
                    "/garments/details": {
                        "success": True,
                        "reported_total_rows": 0,
                        "parsed_row_count": 0,
                        "garments_fetch_completeness": "complete",
                        "garments_budget_state": "within_budget",
                        "garments_final_row_count": 0,
                    },
                },
            )

    async def _ingest_sales_rows(**kwargs: object) -> td_orders_main.TdSalesIngestResult:
        rows = list(kwargs["rows"])
        return td_orders_main.TdSalesIngestResult(
            staging_rows=len(rows),
            staging_inserted=len(rows),
            staging_updated=0,
            final_rows=len(rows),
            final_inserted=len(rows),
            final_updated=0,
            warnings=[],
            parsed_rows=rows,
        )

    monkeypatch.setattr(td_orders_main, "config", SimpleNamespace(database_url="sqlite+aiosqlite:///:memory:"))
    monkeypatch.setattr(td_orders_main, "TdApiClient", _FakeTdApiClient)
    monkeypatch.setattr(td_orders_main, "ingest_td_sales_rows", _ingest_sales_rows)
    monkeypatch.setattr(
        td_orders_main,
        "persist_td_api_artifacts",
        lambda **_kwargs: SimpleNamespace(warnings=[]),
    )
    monkeypatch.setattr(td_orders_main, "log_event", lambda **kwargs: events.append(kwargs))

    summary = td_orders_main.TdOrdersDiscoverySummary(
        run_id="run-valid-empty-a668",
        run_env="test",
        report_date=date(2026, 1, 1),
        report_end_date=date(2026, 1, 1),
    )
    store = td_orders_main.TdStore(
        store_code="A668", store_name="A668", cost_center="CC-A668", sync_config={}
    )

    orders_report, sales_report, fetch_result, _metadata = await td_orders_main._execute_api_primary_ingestion(
        context=_FakeContext(_FakePage()),
        store=store,
        logger=JsonLogger(stream=io.StringIO(), log_file_path=None),
        source_mode="api_only",
        run_id="run-valid-empty-a668",
        run_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        run_start_date=date(2026, 1, 1),
        run_end_date=date(2026, 1, 1),
        run_orders=True,
        run_sales=True,
        download_dir=tmp_path,
        summary=summary,
        stored_state_path=str(tmp_path / "state.json"),
    )

    summary.record_store(
        store.store_code,
        td_orders_main.StoreOutcome(status="ok", message="API primary path executed"),
        orders_result=orders_report,
        sales_result=sales_report,
    )
    record = summary.build_record(finished_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
    store_summary = record["metrics_json"]["stores_summary"]["stores"]["A668"]
    zero_row_events = [
        event for event in events if event.get("message") == "Classified TD API orders zero-row result"
    ]

    assert fetch_result.orders_rows == []
    assert fetch_result.orders_summary_rows_filtered == 1
    assert zero_row_events[-1]["status"] == "ok"
    assert zero_row_events[-1]["orders_status"] == "ok"
    assert zero_row_events[-1]["orders_zero_row_classification"] == "valid_empty_summary_only_endpoint"
    assert orders_report.status == "ok"
    assert orders_report.warnings == []
    assert orders_report.warning_rows == []
    assert sales_report.status == "ok"
    assert sales_report.warnings == []
    assert store_summary["status"] == "ok"
    assert store_summary["data_ingest_status"] == "success"
    assert store_summary["observability_warnings"] == []
    assert store_summary["orders"]["status"] == "ok"
    assert store_summary["sales"]["status"] == "ok"
    assert td_orders_main._resolve_sync_log_status(
        orders_report=orders_report,
        sales_report=sales_report,
        run_orders=True,
        run_sales=True,
    ) == "success"
    assert summary.overall_status() == "success"
    assert summary.orders_overall_status() == "success"
    assert summary.sales_overall_status() == "success"
    assert record["overall_status"] == "success"
    assert record["metrics_json"]["orders"]["overall_status"] == "success"
    assert record["metrics_json"]["sales"]["overall_status"] == "success"
    assert "success_with_warnings" not in {
        summary.overall_status(),
        summary.orders_overall_status(),
        summary.sales_overall_status(),
        record["overall_status"],
        record["metrics_json"]["orders"]["overall_status"],
        record["metrics_json"]["sales"]["overall_status"],
        td_orders_main._resolve_sync_log_status(
            orders_report=orders_report,
            sales_report=sales_report,
            run_orders=True,
            run_sales=True,
        ),
    }


def test_zero_orders_nonzero_sales_complete_empty_garments_compare_pass_is_success() -> None:
    summary = td_orders_main.TdOrdersDiscoverySummary(
        run_id="run-valid-empty",
        run_env="test",
        report_date=date(2026, 1, 1),
        report_end_date=date(2026, 1, 1),
    )
    orders_report = td_orders_main.StoreReport(
        status="ok",
        source_mode="api_only",
        rows_downloaded=0,
        rows_ingested=0,
        final_rows=0,
        compare_rows_orders=[],
        garment_reconciliation={
            "complete_empty_orders": 0,
            "final_row_count": 0,
        },
        garments_fetch_completeness="complete",
        garments_final_row_count=0,
        gate_verdict={"overall_pass": True},
    )
    sales_report = td_orders_main.StoreReport(
        status="ok",
        source_mode="api_only",
        rows_downloaded=2,
        rows_ingested=2,
        final_rows=2,
        compare_rows_sales=[{"order_number": "A817-1"}, {"order_number": "A817-2"}],
    )
    outcome = td_orders_main.StoreOutcome(status="ok", message="API primary path executed")

    summary.record_store(
        "A817",
        outcome,
        orders_result=orders_report,
        sales_result=sales_report,
    )
    record = summary.build_record(finished_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
    store_summary = record["metrics_json"]["stores_summary"]["stores"]["A817"]

    assert td_orders_main._resolve_sync_log_status(
        orders_report=orders_report,
        sales_report=sales_report,
        run_orders=True,
        run_sales=True,
    ) == "success"
    assert store_summary["status"] == "ok"
    assert store_summary["data_ingest_status"] == "success"
    assert store_summary["observability_warnings"] == []
    assert record["overall_status"] == "success"
