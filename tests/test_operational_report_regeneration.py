from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from itertools import count
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

import app.reports.daily_sales_report.pipeline as daily_pipeline
import app.reports.mtd_same_day_fulfillment.pipeline as mtd_pipeline
import app.reports.pending_deliveries.pipeline as pending_pipeline
from app.reports.shared.short_payments import ShortPaymentRow


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
async def test_daily_sales_short_payments_refresh_when_existing_summary_and_write_off(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    render_paths, documents, summaries = _install_common_mocks(
        monkeypatch, daily_pipeline, tmp_path, emails_sent=1
    )
    rendered_html: dict[Path, str] = {}
    recovery_status = {"SHORT-1": None}

    def _short_rows() -> list[ShortPaymentRow]:
        if recovery_status["SHORT-1"] == "WRITE_OFF":
            return []
        return [
            ShortPaymentRow(
                cost_center="CC1",
                order_number="SHORT-1",
                order_date=datetime(2026, 4, 29, 9, 0),
                customer_name="Alice",
                mobile_number="9999999999",
                order_amount=Decimal("100"),
                paid_amount=Decimal("80"),
                shortage_amount=Decimal("20"),
            )
        ]

    async def _fake_daily_data(*_args, **_kwargs):
        data = _daily_data()
        data.short_payment_rows = _short_rows()
        return data

    async def _fake_mtd_rows(*_args, **_kwargs):
        return []

    def _fake_render_html(context, *args, **kwargs):
        template_name = kwargs.get("template_name")
        if template_name == daily_pipeline.SHORT_PAYMENTS_TEMPLATE_NAME:
            return "SHORT PAYMENTS: " + ",".join(
                row.order_number for row in context["short_payment_rows"]
            )
        return "html"

    async def _capture_render_pdf(html: str, output_path: Path, **_kwargs) -> None:
        render_paths.append(output_path)
        rendered_html[output_path] = html
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"pdf")

    monkeypatch.setattr(daily_pipeline, "fetch_daily_sales_report", _fake_daily_data)
    monkeypatch.setattr(daily_pipeline, "fetch_mtd_same_day_fulfillment", _fake_mtd_rows)
    monkeypatch.setattr(daily_pipeline, "fetch_missing_payments_mtd", _fake_mtd_rows)
    monkeypatch.setattr(daily_pipeline, "_render_html", _fake_render_html)
    monkeypatch.setattr(daily_pipeline, "render_mtd_same_day_html", lambda **_kwargs: "mtd-html")
    monkeypatch.setattr(
        daily_pipeline, "render_pdf_with_configured_browser", _capture_render_pdf
    )

    await daily_pipeline._run(report_date=REPORT_DATE, env="test", force=False)
    assert summaries[-1]["overall_status"] == "ok"

    recovery_status["SHORT-1"] = "WRITE_OFF"
    await daily_pipeline._run(report_date=REPORT_DATE, env="test", force=False)

    short_payments_pdf = daily_pipeline._short_payments_output_path(REPORT_DATE)
    assert render_paths.count(short_payments_pdf) == 2
    assert rendered_html[short_payments_pdf] == "SHORT PAYMENTS: "
    assert documents[-2]["doc_type"] == daily_pipeline.SHORT_PAYMENTS_DOCUMENT_TYPE
    assert documents[-2]["file_path"] == short_payments_pdf
    assert summaries[-1]["metrics_json"]["short_payment_rows"] == 0


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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("module", "fetch_attr", "fetch_result", "render_attr", "render_stub"),
    [
        (
            daily_pipeline,
            "fetch_daily_sales_report",
            _daily_data(),
            "_render_html",
            (lambda *_args, **_kwargs: "html"),
        ),
        (
            mtd_pipeline,
            "fetch_mtd_same_day_fulfillment",
            [],
            "render_html",
            (lambda **_kwargs: "html"),
        ),
        (
            pending_pipeline,
            "fetch_pending_deliveries_report",
            SimpleNamespace(
                report_date=REPORT_DATE,
                summary_sections=[],
                cost_center_sections=[],
                total_count=0,
                total_pending_amount=0,
            ),
            "render_html",
            (lambda _context: "html"),
        ),
    ],
)
async def test_zero_sent_notifications_log_warning_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    module,
    fetch_attr: str,
    fetch_result,
    render_attr: str,
    render_stub,
) -> None:
    _install_common_mocks(monkeypatch, module, tmp_path, emails_sent=0)

    async def _fake_fetch(*_args, **_kwargs):
        return fetch_result

    captured_send_email_events: list[dict] = []

    def _capture_log_event(*, logger, phase, message, **kwargs) -> None:
        if phase == "send_email" and "zero emails sent" in message:
            captured_send_email_events.append({"phase": phase, "message": message, **kwargs})

    monkeypatch.setattr(module, fetch_attr, _fake_fetch)
    monkeypatch.setattr(module, render_attr, render_stub)
    monkeypatch.setattr(module, "log_event", _capture_log_event)
    if module is daily_pipeline:
        async def _fake_mtd_rows(*_args, **_kwargs):
            return []

        monkeypatch.setattr(module, "fetch_mtd_same_day_fulfillment", _fake_mtd_rows)
        monkeypatch.setattr(module, "render_mtd_same_day_html", lambda **_kwargs: "mtd-html")

    await module._run(report_date=REPORT_DATE, env="test", force=False)

    assert captured_send_email_events
    assert captured_send_email_events[-1]["status"] == "warning"
