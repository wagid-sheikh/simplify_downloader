from __future__ import annotations

import io
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.crm_downloader.uc_orders_sync import main as uc_main
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
    async def new_context(self, storage_state: str | None = None) -> _FakeContext:
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

    monkeypatch.setattr(
        uc_main,
        "collect_gst_orders_via_api",
        AsyncMock(
            return_value=uc_main.GstApiExtract(
                gst_rows=[{"order_number": "UC-100"}],
                base_rows=[{"order_code": "UC-100"}],
                order_detail_rows=[],
                payment_detail_rows=[],
            )
        ),
    )

    collect_archive_api_mock = AsyncMock(
        return_value=SimpleNamespace(
            base_rows=[{"store_code": "A100", "order_code": "UC-100"}],
            order_detail_rows=[{"store_code": "A100", "order_code": "UC-100"}],
            payment_detail_rows=[{"store_code": "A100", "order_code": "UC-100"}],
            skipped_order_codes=[],
            skipped_order_counters={},
            page_count=1,
            api_total=1,
            extractor_reason_codes=[],
            extractor_error_counters={},
        )
    )
    monkeypatch.setattr(uc_main, "collect_archive_orders_via_api", collect_archive_api_mock)

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
        AsyncMock(return_value=SimpleNamespace(inserted=1, updated=0, skipped=0, warnings=0, reason_codes=[])),
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

    assert collect_archive_api_mock.await_count == 1
    assert ui_extract_mock.await_count == 0

    expected_files = {
        "A100-uc_gst-base_order_info_20250101_20250101_run-api-1.xlsx",
        "A100-uc_gst-order_details_20250101_20250101_run-api-1.xlsx",
        "A100-uc_gst-payment_details_20250101_20250101_run-api-1.xlsx",
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
