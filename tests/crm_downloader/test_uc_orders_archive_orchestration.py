from __future__ import annotations

import io
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import sqlalchemy as sa

from app.crm_downloader.uc_orders_sync import ingest as uc_ingest
from app.crm_downloader.uc_orders_sync import main as uc_main
from app.dashboard_downloader.db_tables import orders_sync_log
from app.dashboard_downloader.json_logger import JsonLogger


class _FakePage:
    def __init__(self) -> None:
        self.url = "about:blank"

    async def goto(self, url: str, wait_until: str = "domcontentloaded") -> None:
        self.url = url


class _FakeContext:
    def __init__(self) -> None:
        self._page = _FakePage()

    async def new_page(self) -> _FakePage:
        return self._page

    async def storage_state(self, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text("{}")

    async def close(self) -> None:
        return None


class _FakeBrowser:
    async def new_context(self, storage_state: str | None = None, **_kwargs) -> _FakeContext:
        return _FakeContext()


@pytest.mark.asyncio
async def test_archive_orchestration_uses_api_only_and_produces_archive_outputs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    logger = JsonLogger(stream=io.StringIO(), log_file_path=None)
    summary = uc_main.UcOrdersDiscoverySummary(
        run_id="run-api-1",
        run_env="test",
        report_date=date(2025, 1, 1),
        report_end_date=date(2025, 1, 1),
        started_at=datetime.now(timezone.utc),
        store_codes=["A100"],
    )
    store = uc_main.UcStore(
        store_code="A100",
        store_name="Store A",
        cost_center="CC01",
        sync_config={
            "urls": {
                "home": "https://example.com/home",
                "orders_link": "https://example.com/orders",
                "login": "https://example.com/login",
            },
            "username": "user",
            "password": "pass",
        },
    )

    (tmp_path / "A100_storage_state.json").write_text("{}")

    monkeypatch.setattr(
        uc_main,
        "config",
        type("_Cfg", (), {"database_url": "postgres://db", "pipeline_skip_dom_logging": True})(),
    )
    monkeypatch.setattr(uc_main, "default_profiles_dir", lambda: tmp_path)
    monkeypatch.setattr(uc_main, "_resolve_uc_download_dir", lambda *_: tmp_path)
    monkeypatch.setattr(uc_main, "_insert_orders_sync_log", AsyncMock(return_value=1))
    monkeypatch.setattr(uc_main, "_update_orders_sync_log", AsyncMock())
    monkeypatch.setattr(uc_main, "_assert_home_ready", AsyncMock(return_value=True))
    monkeypatch.setattr(uc_main, "_navigate_to_gst_reports", AsyncMock(return_value=False))
    monkeypatch.setattr(uc_main, "_try_direct_gst_reports", AsyncMock(return_value=(False, None)))
    monkeypatch.setattr(uc_main, "_navigate_to_archive_orders", AsyncMock(return_value=True))

    collect_gst_mock = AsyncMock(
        return_value=uc_main.GstApiExtract(
            gst_rows=[{"order_number": "UC-100"}],
            base_rows=[{"order_code": "UC-100"}],
            order_detail_rows=[],
            payment_detail_rows=[],
            order_detail_snapshot_rows=[{
                "store_code": "A100",
                "order_code": "UC-100",
                "snapshot_outcome": "complete_empty",
                "detail_row_count": 0,
            }],
        )
    )
    monkeypatch.setattr(uc_main, "collect_gst_orders_via_api", collect_gst_mock)


    ui_extract_mock = AsyncMock(side_effect=AssertionError("UI archive path must remain disabled"))
    monkeypatch.setattr(uc_main, "_collect_archive_orders", ui_extract_mock)

    monkeypatch.setattr(
        uc_main,
        "ingest_uc_archive_excels",
        AsyncMock(
            return_value=SimpleNamespace(
                files={
                    "base": SimpleNamespace(parsed=1, inserted=1, updated=0, rejected=0, warnings=0, reject_reasons={}),
                    "order_details": SimpleNamespace(parsed=1, inserted=1, updated=0, rejected=0, warnings=0, reject_reasons={}),
                    "payment_details": SimpleNamespace(parsed=1, inserted=1, updated=0, rejected=0, warnings=0, reject_reasons={}),
                },
                rejects=[],
            )
        ),
    )
    monkeypatch.setattr(
        uc_main,
        "publish_uc_gst_order_details_to_orders",
        AsyncMock(return_value=SimpleNamespace(inserted=1, updated=0, skipped=0, warnings=0, reason_codes=[])),
    )
    monkeypatch.setattr(
        uc_main,
        "publish_uc_gst_order_details_to_line_items",
        AsyncMock(
            return_value=SimpleNamespace(
                inserted=1,
                updated=0,
                deleted_final_rows=0,
                skipped=0,
                warnings=0,
                reason_codes=[],
                line_item_serial_validation={},
                invoices_inspected=1,
                complete_with_rows_invoices=0,
                complete_empty_invoices=1,
                replacement_skipped_incomplete_invoices=0,
                inserted_final_rows=0,
                orphan_rows=0,
                staging_rows_written=0,
            )
        ),
    )
    monkeypatch.setattr(
        uc_main,
        "publish_uc_gst_payments_to_sales",
        AsyncMock(
            return_value=SimpleNamespace(
                inserted=1,
                updated=0,
                skipped=0,
                warnings=0,
                reason_codes=[],
                publish_parent_match_rate=1.0,
                missing_parent_count=0,
                preflight_warning=False,
                preflight_diagnostics={},
                    post_publish_verification={},
            )
        ),
    )

    await uc_main._run_store_discovery(
        browser=_FakeBrowser(),
        store=store,
        logger=logger,
        run_env="test",
        run_id="run-api-1",
        run_date=datetime.now(timezone.utc),
        summary=summary,
        from_date=date(2025, 1, 1),
        to_date=date(2025, 1, 1),
        download_timeout_ms=1000,
    )

    assert collect_gst_mock.await_count == 1
    assert ui_extract_mock.await_count == 0

    expected_files = {
        "A100-uc_gst-base_order_info_20250101_20250101_run-api-1.xlsx",
        "A100-uc_gst-order_details_20250101_20250101_run-api-1.xlsx",
        "A100-uc_gst-payment_details_20250101_20250101_run-api-1.xlsx",
        "A100-uc_gst-order_detail_snapshots_20250101_20250101_run-api-1.xlsx",
    }
    assert expected_files.issubset({path.name for path in tmp_path.glob("*.xlsx")})

    outcome = summary.store_outcomes["A100"]
    assert outcome.stage_statuses["archive_ingest"] == "success"
    assert outcome.stage_statuses["gst_publish"] == "success"
    assert set(outcome.stage_metrics["archive_ingest"]["files"].keys()) == {
        "base",
        "order_details",
        "payment_details",
    }


@pytest.mark.asyncio
async def test_archive_orchestration_ingest_exception_sets_reason_codes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    logger = JsonLogger(stream=io.StringIO(), log_file_path=None)
    summary = uc_main.UcOrdersDiscoverySummary(
        run_id="run-api-2",
        run_env="test",
        report_date=date(2025, 1, 1),
        report_end_date=date(2025, 1, 1),
        started_at=datetime.now(timezone.utc),
        store_codes=["A101"],
    )
    store = uc_main.UcStore(
        store_code="A101",
        store_name="Store B",
        cost_center="CC02",
        sync_config={
            "urls": {
                "home": "https://example.com/home",
                "orders_link": "https://example.com/orders",
                "login": "https://example.com/login",
            },
            "username": "user",
            "password": "pass",
        },
    )

    (tmp_path / "A101_storage_state.json").write_text("{}")

    monkeypatch.setattr(
        uc_main,
        "config",
        type("_Cfg", (), {"database_url": "postgres://db", "pipeline_skip_dom_logging": True})(),
    )
    monkeypatch.setattr(uc_main, "default_profiles_dir", lambda: tmp_path)
    monkeypatch.setattr(uc_main, "_resolve_uc_download_dir", lambda *_: tmp_path)
    monkeypatch.setattr(uc_main, "_insert_orders_sync_log", AsyncMock(return_value=1))
    monkeypatch.setattr(uc_main, "_update_orders_sync_log", AsyncMock())
    monkeypatch.setattr(uc_main, "_assert_home_ready", AsyncMock(return_value=True))
    monkeypatch.setattr(uc_main, "_navigate_to_gst_reports", AsyncMock(return_value=False))
    monkeypatch.setattr(uc_main, "_try_direct_gst_reports", AsyncMock(return_value=(False, None)))
    monkeypatch.setattr(uc_main, "_navigate_to_archive_orders", AsyncMock(return_value=True))
    monkeypatch.setattr(
        uc_main,
        "collect_gst_orders_via_api",
        AsyncMock(
            return_value=uc_main.GstApiExtract(
                gst_rows=[{"order_number": "UC-101"}],
                base_rows=[{"order_code": "UC-101"}],
                order_detail_rows=[{"order_code": "UC-101"}],
                payment_detail_rows=[{"order_code": "UC-101"}],
            )
        ),
    )
    monkeypatch.setattr(
        uc_main,
        "ingest_uc_archive_excels",
        AsyncMock(side_effect=RuntimeError("forced ingest failure")),
    )

    publish_orders_mock = AsyncMock()
    publish_line_items_mock = AsyncMock()
    publish_sales_mock = AsyncMock()
    monkeypatch.setattr(uc_main, "publish_uc_gst_order_details_to_orders", publish_orders_mock)
    monkeypatch.setattr(uc_main, "publish_uc_gst_order_details_to_line_items", publish_line_items_mock)
    monkeypatch.setattr(uc_main, "publish_uc_gst_payments_to_sales", publish_sales_mock)

    await uc_main._run_store_discovery(
        browser=_FakeBrowser(),
        store=store,
        logger=logger,
        run_env="test",
        run_id="run-api-2",
        run_date=datetime.now(timezone.utc),
        summary=summary,
        from_date=date(2025, 1, 1),
        to_date=date(2025, 1, 1),
        download_timeout_ms=1000,
    )

    outcome = summary.store_outcomes["A101"]
    assert outcome.stage_statuses["archive_ingest"] == "failed"
    assert outcome.stage_statuses["gst_publish"] == "skipped"
    assert outcome.reason_codes
    assert uc_main.REASON_ARCHIVE_INGEST_FAILED in outcome.reason_codes
    assert uc_main.REASON_GST_PUBLISH_SKIPPED_DUE_INGEST_FAILURE in outcome.reason_codes

    window_result = summary.window_audit[-1]
    assert window_result["store_code"] == "A101"
    assert window_result["warning_count"] == outcome.warning_count
    assert window_result["reason_codes"]
    assert uc_main.REASON_ARCHIVE_INGEST_FAILED in window_result["reason_codes"]
    assert (
        uc_main.REASON_GST_PUBLISH_SKIPPED_DUE_INGEST_FAILURE
        in window_result["reason_codes"]
    )

    assert publish_orders_mock.await_count == 0
    assert publish_line_items_mock.await_count == 0
    assert publish_sales_mock.await_count == 0
    assert summary.overall_status() == "partial"



def test_resolve_uc_archive_extraction_mode_rejects_ui_flags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("UC_ARCHIVE_EXTRACTION_MODE", "ui")
    with pytest.raises(ValueError, match="no longer supports UI"):
        uc_main._resolve_uc_archive_extraction_mode()

    monkeypatch.setenv("UC_ARCHIVE_EXTRACTION_MODE", "")
    monkeypatch.setenv("UC_ARCHIVE_UI_ENABLED", "true")
    with pytest.raises(ValueError, match="no longer supported"):
        uc_main._resolve_uc_archive_extraction_mode()


@pytest.mark.asyncio
async def test_run_store_discovery_warning_path_does_not_raise_type_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    logger = JsonLogger(stream=io.StringIO(), log_file_path=None)
    summary = uc_main.UcOrdersDiscoverySummary(
        run_id="run-warning-1",
        run_env="test",
        report_date=date(2025, 1, 1),
        report_end_date=date(2025, 1, 1),
        started_at=datetime.now(timezone.utc),
        store_codes=["A102"],
    )
    store = uc_main.UcStore(
        store_code="A102",
        store_name="Store C",
        cost_center="CC03",
        sync_config={
            "urls": {
                "home": "https://example.com/home",
                "orders_link": "https://example.com/orders",
                "login": "https://example.com/login",
            },
            "username": "user",
            "password": "pass",
        },
    )

    (tmp_path / "A102_storage_state.json").write_text("{}")

    update_calls = {"count": 0}

    async def strict_update_orders_sync_log(
        *,
        logger: JsonLogger,
        log_id: int | None,
        status: str | None = None,
        orders_pulled_at: datetime | None = None,
        error_message: str | None = None,
        primary_metrics: dict[str, object] | None = None,
        secondary_metrics: dict[str, object] | None = None,
    ) -> None:
        del logger, log_id, status, orders_pulled_at, error_message, primary_metrics, secondary_metrics
        update_calls["count"] += 1

    monkeypatch.setattr(
        uc_main,
        "config",
        type("_Cfg", (), {"database_url": None, "pipeline_skip_dom_logging": True})(),
    )
    monkeypatch.setattr(uc_main, "default_profiles_dir", lambda: tmp_path)
    monkeypatch.setattr(uc_main, "_resolve_uc_download_dir", lambda *_: tmp_path)
    monkeypatch.setattr(uc_main, "_insert_orders_sync_log", AsyncMock(return_value=1))
    monkeypatch.setattr(uc_main, "_update_orders_sync_log", strict_update_orders_sync_log)
    monkeypatch.setattr(uc_main, "_assert_home_ready", AsyncMock(return_value=True))
    monkeypatch.setattr(uc_main, "_navigate_to_gst_reports", AsyncMock(return_value=False))
    monkeypatch.setattr(uc_main, "_try_direct_gst_reports", AsyncMock(return_value=(False, None)))
    monkeypatch.setattr(uc_main, "_navigate_to_archive_orders", AsyncMock(return_value=False))
    monkeypatch.setattr(
        uc_main,
        "collect_gst_orders_via_api",
        AsyncMock(
            return_value=uc_main.GstApiExtract(
                gst_rows=[{"order_number": "UC-102"}],
                base_rows=[{"order_code": "UC-102"}],
                order_detail_rows=[],
                payment_detail_rows=[],
            )
        ),
    )

    await uc_main._run_store_discovery(
        browser=_FakeBrowser(),
        store=store,
        logger=logger,
        run_env="test",
        run_id="run-warning-1",
        run_date=datetime.now(timezone.utc),
        summary=summary,
        from_date=date(2025, 1, 1),
        to_date=date(2025, 1, 1),
        download_timeout_ms=1000,
    )

    outcome = summary.store_outcomes["A102"]
    assert outcome.status == "warning"
    assert update_calls["count"] >= 1


def test_run_summary_final_status_not_success_when_windows_missing_or_errors() -> None:
    summary_missing = uc_main.UcOrdersDiscoverySummary(
        run_id="run-summary-1",
        run_env="test",
        report_date=date(2025, 1, 1),
        report_end_date=date(2025, 1, 1),
        started_at=datetime.now(timezone.utc),
        store_codes=["A1", "A2"],
    )
    summary_missing.record_store(
        "A1",
        uc_main.StoreOutcome(status="ok", message="done"),
    )

    summary_store_error = uc_main.UcOrdersDiscoverySummary(
        run_id="run-summary-2",
        run_env="test",
        report_date=date(2025, 1, 1),
        report_end_date=date(2025, 1, 1),
        started_at=datetime.now(timezone.utc),
        store_codes=["A1"],
    )
    summary_store_error.mark_phase("store", "error")
    summary_store_error.record_store(
        "A1",
        uc_main.StoreOutcome(status="ok", message="done"),
    )

    assert summary_missing.overall_status() == "failed"
    assert summary_store_error.overall_status() == "failed"


@pytest.mark.parametrize(
    ("classification", "expected_status"),
    [
        (uc_main.ZERO_ROW_EXPORT_VALID_EMPTY_WORKBOOK, "success"),
        (uc_main.ZERO_ROW_EXPORT_SOURCE_NO_DATA, "success"),
        (uc_ingest.ZERO_ROW_EXPORT_MALFORMED, "success_with_warnings"),
        (uc_main.ZERO_ROW_EXPORT_UNCONFIRMED, "success_with_warnings"),
        (uc_main.ZERO_ROW_EXPORT_DEGRADED_NAVIGATION, "success_with_warnings"),
    ],
)
def test_zero_row_export_classification_controls_sync_status(
    classification: str, expected_status: str
) -> None:
    outcome = uc_main.StoreOutcome(
        status="ok",
        message="empty export",
        download_path="/tmp/uc-empty.xlsx",
        rows_downloaded=0,
        staging_rows=0,
        final_rows=0,
        zero_row_export_classification=classification,
    )

    assert (
        uc_main._resolve_sync_log_status(
            outcome=outcome, download_succeeded=True, row_count=0
        )
        == expected_status
    )


def test_degraded_navigation_empty_export_is_in_run_summary_and_notification() -> None:
    summary = uc_main.UcOrdersDiscoverySummary(
        run_id="run-degraded-empty",
        run_env="test",
        report_date=date(2025, 1, 1),
        report_end_date=date(2025, 1, 1),
        started_at=datetime.now(timezone.utc),
        store_codes=["A103"],
    )
    outcome = uc_main.StoreOutcome(
        status="ok",
        message="empty export after degraded navigation",
        download_path="/tmp/uc-empty.xlsx",
        rows_downloaded=0,
        staging_rows=0,
        final_rows=0,
        zero_row_export_classification=uc_main.ZERO_ROW_EXPORT_DEGRADED_NAVIGATION,
    )
    summary.record_store("A103", outcome)
    summary.window_audit.append(
        {
            "store_code": "A103",
            "zero_row_export_classification": outcome.zero_row_export_classification,
        }
    )

    record = summary.build_record(finished_at=datetime.now(timezone.utc))
    metrics = record["metrics_json"]
    notification_store = metrics["notification_payload"]["stores"][0]

    assert record["overall_status"] == "success_with_warnings"
    assert metrics["window_audit"][0]["zero_row_export_classification"] == (
        uc_main.ZERO_ROW_EXPORT_DEGRADED_NAVIGATION
    )
    assert notification_store["zero_row_export_classification"] == (
        uc_main.ZERO_ROW_EXPORT_DEGRADED_NAVIGATION
    )
    assert metrics["notification_payload"]["warnings"] == [
        "UC_ZERO_ROW_EXPORT: A103 classified as downloaded_after_degraded_navigation"
    ]


def test_degraded_navigation_wins_over_source_no_data_classification() -> None:
    classification, reason_code = uc_main._classify_zero_row_export(
        base_count=0,
        gst_api_extract=uc_main.GstApiExtract(),
        ingest_classification=uc_main.ZERO_ROW_EXPORT_VALID_EMPTY_WORKBOOK,
        home_url_navigation_timed_out=True,
    )

    assert classification == uc_main.ZERO_ROW_EXPORT_DEGRADED_NAVIGATION
    assert reason_code == uc_main.REASON_ZERO_ROWS_AFTER_NAV_TIMEOUT


def test_unified_metrics_can_update_orders_sync_log_with_zero_row_classification() -> None:
    outcome = uc_main.StoreOutcome(
        status="ok",
        message="confirmed empty source export",
        rows_downloaded=0,
        staging_rows=0,
        final_rows=0,
        zero_row_export_classification=uc_main.ZERO_ROW_EXPORT_SOURCE_NO_DATA,
    )
    primary_metrics, secondary_metrics = uc_main._build_unified_metrics(outcome)
    values = {**primary_metrics, **secondary_metrics}

    engine = sa.create_engine("sqlite://")
    sync_log_table = orders_sync_log.to_metadata(sa.MetaData())
    sync_log_table.create(engine)
    with engine.begin() as connection:
        connection.execute(
            sync_log_table.insert().values(
                id=1,
                pipeline_id=1,
                run_id="run-zero-row",
                run_env="test",
                store_code="UC001",
                from_date=date(2025, 1, 1),
                to_date=date(2025, 1, 1),
                status="running",
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
        )
        connection.execute(
            sa.update(orders_sync_log)
            .where(orders_sync_log.c.id == 1)
            .values(**values)
        )
        classification = connection.scalar(
            sa.select(orders_sync_log.c.zero_row_export_classification).where(
                orders_sync_log.c.id == 1
            )
        )

    assert classification == uc_main.ZERO_ROW_EXPORT_SOURCE_NO_DATA
