from __future__ import annotations

from datetime import date
from itertools import count
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

import app.reports.daily_sales_report.pipeline as daily_pipeline
import app.reports.mtd_same_day_fulfillment.pipeline as mtd_pipeline
import app.reports.pending_deliveries.pipeline as pending_pipeline


REPORT_DATE = date(2026, 4, 29)


def _install_common_mocks(monkeypatch: pytest.MonkeyPatch, module, tmp_path: Path, emails_sent: int) -> tuple[list[Path], list[dict], list[dict]]:
    render_paths: list[Path] = []
    documents: list[dict] = []
    summaries: list[dict] = []
    run_counter = count(1)

    monkeypatch.setattr(
        module,
        "config",
        SimpleNamespace(database_url="sqlite+aiosqlite:///unused.db", pdf_render_timeout_seconds=30),
    )
    monkeypatch.setattr(module, "OUTPUT_ROOT", tmp_path)
    monkeypatch.setattr(module, "get_timezone", lambda: ZoneInfo("Asia/Kolkata"))
    monkeypatch.setattr(module, "resolve_run_env", lambda env: env or "test")
    monkeypatch.setattr(module, "new_run_id", lambda: f"run-{next(run_counter)}")

    async def _capture_persist_summary(_database_url: str, record: dict) -> None:
        summaries.append(dict(record))

    async def _capture_update_summary(_database_url: str, _run_id: str, record: dict) -> None:
        summaries.append(dict(record))

    async def _capture_document(**kwargs) -> None:
        documents.append(dict(kwargs))

    async def _fake_notify(*_args, **_kwargs) -> dict[str, object]:
        return {"emails_planned": 1, "emails_sent": emails_sent, "errors": []}

    monkeypatch.setattr(module, "persist_summary_record", _capture_persist_summary)
    monkeypatch.setattr(module, "update_summary_record", _capture_update_summary)
    monkeypatch.setattr(module, "_persist_document", _capture_document)
    monkeypatch.setattr(module, "send_notifications_for_run", _fake_notify)

    if hasattr(module, "render_pdf_with_configured_browser"):
        async def _fake_render_pdf(_html: str, output_path: Path, **_kwargs) -> None:
            render_paths.append(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"pdf")

        monkeypatch.setattr(module, "render_pdf_with_configured_browser", _fake_render_pdf)
    else:
        async def _fake_render_pdf(_html: str, output_path: Path, **_kwargs) -> None:
            render_paths.append(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"pdf")

        monkeypatch.setattr(module, "render_pdf", _fake_render_pdf)

    return render_paths, documents, summaries


def _daily_data() -> SimpleNamespace:
    return SimpleNamespace(
        report_date=REPORT_DATE,
        rows=[],
        totals=SimpleNamespace(),
        edited_orders=[],
        edited_orders_summary=SimpleNamespace(),
        edited_orders_totals=SimpleNamespace(),
        missed_leads=[],
        cancelled_leads=[],
        lead_performance_summary=SimpleNamespace(),
        to_be_recovered=[],
        to_be_compensated=[],
        to_be_recovered_total_order_value=0,
        to_be_compensated_total_order_value=0,
        auto_cleared_order_numbers_text="",
        same_day_fulfillment_rows=[],
        missing_payment_rows=[],
        short_payment_rows=[],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("emails_sent,expected_previous_status", [(1, "ok"), (0, "warning")])
async def test_daily_sales_report_regenerates_for_same_date_with_previous_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    emails_sent: int,
    expected_previous_status: str,
) -> None:
    render_paths, documents, summaries = _install_common_mocks(
        monkeypatch, daily_pipeline, tmp_path, emails_sent
    )

    async def _fake_daily_data(*_args, **_kwargs):
        return _daily_data()

    async def _fake_mtd_rows(*_args, **_kwargs):
        return []

    monkeypatch.setattr(daily_pipeline, "fetch_daily_sales_report", _fake_daily_data)
    monkeypatch.setattr(daily_pipeline, "fetch_mtd_same_day_fulfillment", _fake_mtd_rows)
    monkeypatch.setattr(daily_pipeline, "fetch_missing_payments_mtd", _fake_mtd_rows)
    monkeypatch.setattr(daily_pipeline, "_render_html", lambda *_args, **_kwargs: "html")
    monkeypatch.setattr(daily_pipeline, "render_mtd_same_day_html", lambda **_kwargs: "mtd-html")

    await daily_pipeline._run(report_date=REPORT_DATE, env="test", force=False)
    assert summaries[-1]["overall_status"] == expected_previous_status

    await daily_pipeline._run(report_date=REPORT_DATE, env="test", force=False)

    daily_pdf = tmp_path / f"{daily_pipeline.PIPELINE_NAME}_{REPORT_DATE.isoformat()}.pdf"
    assert render_paths.count(daily_pdf) == 2
    assert len(documents) == 8
    assert summaries[-1]["overall_status"] == expected_previous_status


@pytest.mark.asyncio
@pytest.mark.parametrize("emails_sent,expected_previous_status", [(1, "ok"), (0, "warning")])
async def test_mtd_same_day_report_regenerates_for_same_date_with_previous_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    emails_sent: int,
    expected_previous_status: str,
) -> None:
    render_paths, documents, summaries = _install_common_mocks(
        monkeypatch, mtd_pipeline, tmp_path, emails_sent
    )

    async def _fake_rows(*_args, **_kwargs):
        return []

    monkeypatch.setattr(mtd_pipeline, "fetch_mtd_same_day_fulfillment", _fake_rows)
    monkeypatch.setattr(mtd_pipeline, "fetch_missing_payments_mtd", _fake_rows)
    monkeypatch.setattr(mtd_pipeline, "render_html", lambda **_kwargs: "html")

    await mtd_pipeline._run(report_date=REPORT_DATE, env="test", force=False)
    assert summaries[-1]["overall_status"] == expected_previous_status

    await mtd_pipeline._run(report_date=REPORT_DATE, env="test", force=False)

    output_pdf = tmp_path / f"{mtd_pipeline.PIPELINE_NAME}_{REPORT_DATE.isoformat()}.pdf"
    assert render_paths == [output_pdf, output_pdf]
    assert len(documents) == 2
    assert summaries[-1]["overall_status"] == expected_previous_status


@pytest.mark.asyncio
@pytest.mark.parametrize("emails_sent,expected_previous_status", [(1, "ok"), (0, "warning")])
async def test_pending_deliveries_report_regenerates_for_same_date_with_previous_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    emails_sent: int,
    expected_previous_status: str,
) -> None:
    render_paths, documents, summaries = _install_common_mocks(
        monkeypatch, pending_pipeline, tmp_path, emails_sent
    )

    async def _fake_data(*_args, **_kwargs):
        return SimpleNamespace(
            report_date=REPORT_DATE,
            summary_sections=[],
            cost_center_sections=[],
            total_count=0,
            total_pending_amount=0,
            manual_recovery_rows=[],
            manual_recovery_total_amount_at_risk=0,
        )

    monkeypatch.setattr(pending_pipeline, "fetch_pending_deliveries_report", _fake_data)
    monkeypatch.setattr(pending_pipeline, "render_html", lambda _context: "html")

    await pending_pipeline._run(report_date=REPORT_DATE, env="test", force=False)
    assert summaries[-1]["overall_status"] == expected_previous_status

    await pending_pipeline._run(report_date=REPORT_DATE, env="test", force=False)

    output_pdf = tmp_path / f"{pending_pipeline.PIPELINE_NAME}_{REPORT_DATE.isoformat()}.pdf"
    assert render_paths == [output_pdf, output_pdf]
    assert len(documents) == 2
    assert summaries[-1]["overall_status"] == expected_previous_status
