from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from app.dashboard_downloader.notifications import (
    PROFILER_HTML_TEMPLATE,
    _build_fact_rows,
    _build_profiler_context,
    _build_run_plan,
    _build_store_plans,
    _derive_duration_fields,
    _build_uc_orders_context,
    _format_fact_sections_text,
    _prepare_ingest_remarks,
    _render_template,
    _resolve_reporting_mode_suffix,
    _td_summary_text_from_payload,
    _uc_summary_text_from_payload,
)


def test_prepare_ingest_remarks_includes_all_rows() -> None:
    rows = [
        {"store_code": "a001", "order_number": "123", "ingest_remarks": "x" * 15},
        {"store_code": "a002", "order_number": "456", "ingest_remarks": "ok"},
    ]

    cleaned, truncated_rows, truncated_length, ingest_text = _prepare_ingest_remarks(rows)

    assert truncated_rows is False
    assert truncated_length is False
    assert cleaned == [
        {"store_code": "A001", "order_number": "123", "ingest_remarks": "x" * 15},
        {"store_code": "A002", "order_number": "456", "ingest_remarks": "ok"},
    ]
    assert "- A001 123: " + "x" * 15 in ingest_text
    assert "- A002 456: ok" in ingest_text
    assert "truncated" not in ingest_text.lower()


def test_uc_orders_warning_count_ignores_row_totals() -> None:
    run_data = {
        "metrics_json": {
            "notification_payload": {
                "stores": [
                    {
                        "store_code": "UC100",
                        "status": "ok",
                        "staging_rows": 250,
                        "final_rows": 250,
                        "staging_inserted": 250,
                        "final_inserted": 250,
                    }
                ]
            },
            "ingest_remarks": {"rows": []},
            "stores_summary": {"stores": {"UC100": {"warning_count": None}}},
            "window_audit": [],
        }
    }

    context = _build_uc_orders_context(run_data)

    assert context["stores"][0]["warning_count"] == 0


def test_fact_sections_include_placeholder_and_sorted_rows() -> None:
    rows = [
        {
            "store_code": "b2",
            "order_number": "",
            "order_date": "2024-01-02",
            "customer_name": "Bee",
            "mobile_number": "222",
            "ingest_remarks": "missing order number",
        },
        {
            "store_code": "a1",
            "order_number": "ORD-1",
            "order_date": "2024-01-01",
            "customer_name": "Ace",
            "mobile_number": "111",
            "ingest_remarks": "ok",
        },
    ]

    fact_rows = _build_fact_rows(rows, include_remarks=True)
    fact_text = _format_fact_sections_text(warning_rows=fact_rows)

    assert fact_rows[0]["store_code"] == "A1"
    assert fact_rows[1]["order_number"] == "<missing_order_number>"
    assert "Warning rows (2):" in fact_text
    assert "<missing_order_number>" in fact_text


def test_td_and_uc_summary_text_have_shared_deterministic_sections() -> None:
    td_run_data = {
        "run_id": "run-td-1",
        "run_env": "test",
        "report_date": "2024-01-05",
        "overall_status": "warning",
        "total_time_taken": "00:02:00",
        "started_at": "2024-01-05T05:00:00+00:00",
        "finished_at": "2024-01-05T05:02:00+00:00",
        "metrics_json": {
            "window_summary": {"completed_windows": 1, "expected_windows": 1, "missing_windows": 0},
            "notification_payload": {
                "overall_status": "warning",
                "orders_status": "ok",
                "sales_status": "warning",
                "stores": [
                    {
                        "store_code": "TD01",
                        "data_source_decision": "ui",
                        "ingest_status": "success",
                        "failure_stage": None,
                        "orders": {"status": "ok", "rows_downloaded": 10, "rows_ingested": 10},
                        "sales": {
                            "status": "warning",
                            "rows_downloaded": 10,
                            "rows_ingested": 9,
                            "warning_rows": [{"order_number": "S1"}],
                            "filenames": ["TD01_sales.xlsx"],
                        },
                    }
                ],
            },
        },
    }
    uc_run_data = {
        "run_id": "run-uc-1",
        "run_env": "test",
        "report_date": "2024-01-05",
        "overall_status": "success_with_warnings",
        "total_time_taken": "00:03:00",
        "started_at": "2024-01-05T05:00:00+00:00",
        "finished_at": "2024-01-05T05:03:00+00:00",
        "metrics_json": {
            "window_summary": {"completed_windows": 1, "expected_windows": 1, "missing_windows": 0},
            "window_audit": [],
            "notification_payload": {
                "overall_status": "warning",
                "stores": [
                    {
                        "store_code": "UC01",
                        "status": "warning",
                        "rows_downloaded": 10,
                        "rows_ingested": 9,
                        "staging_inserted": 8,
                        "staging_updated": 1,
                        "rows_skipped_invalid": 1,
                        "rows_skipped_invalid_reasons": {"invalid_gstin": 1},
                        "warning_rows": [{"order_number": "U1"}],
                        "filename": "UC01_uc.xlsx",
                    }
                ],
            },
        },
    }

    td_summary = _td_summary_text_from_payload(td_run_data)
    uc_summary = _uc_summary_text_from_payload(uc_run_data)

    for summary in (td_summary, uc_summary):
        assert "Header:" in summary
        assert "Window reconciliation summary:" in summary
        assert "Per-store metrics:" in summary
        assert "Warnings:" in summary
        assert "Row-level facts:" in summary
        assert "Filenames:" in summary

        assert summary.index("Header:") < summary.index("Window reconciliation summary:")
        assert summary.index("Window reconciliation summary:") < summary.index("Per-store metrics:")
        assert summary.index("Per-store metrics:") < summary.index("Warnings:")
        assert summary.index("Warnings:") < summary.index("Row-level facts:")
        assert summary.index("Row-level facts:") < summary.index("Filenames:")

    assert "overall_status: SUCCESS WITH WARNINGS" in td_summary
    assert "overall_status: SUCCESS WITH WARNINGS" in uc_summary
    assert "started_at: 05-Jan-2024 10:30:00 IST" in td_summary
    assert "finished_at: 05-Jan-2024 10:33:00 IST" in uc_summary


def test_unified_context_contract_for_uc_orders() -> None:
    run_data = {
        "run_id": "run-uc-contract-1",
        "run_env": "stage",
        "report_date": "2024-01-05",
        "overall_status": "success_with_warnings",
        "metrics_json": {
            "window_summary": {"completed_windows": 1, "expected_windows": 1, "missing_windows": 0},
            "window_audit": [],
            "notification_payload": {
                "overall_status": "warning",
                "started_at": "2024-01-05T05:00:00+00:00",
                "finished_at": "2024-01-05T05:03:00+00:00",
                "stores": [
                    {
                        "store_code": "UC01",
                        "status": "warning",
                        "warning_rows": [{"order_number": "U1", "ingest_remarks": "needs review"}],
                        "filename": "UC01_orders.xlsx",
                    }
                ],
            },
        },
    }

    context = _build_uc_orders_context(run_data)

    assert context["env_upper"] == "STAGE"
    assert context["overall_status_upper"] == "SUCCESS WITH WARNINGS"
    assert context["pipeline_display_name"] == "UC Orders Sync"
    assert context["store_code"] == "UC01"
    assert context["run_date_display"] == "05-Jan-2024"
    assert context["started_at_ist"] == "05-Jan-2024 10:30:00 IST"
    assert context["finished_at_ist"] == "05-Jan-2024 10:33:00 IST"
    assert context["store_processing_summary_block"].count("UC01") == 1
    assert context["files_processed_block"].count("UC01_orders.xlsx") == 1
    assert context["warnings_block"]
    assert context["optional_notes_block"]


def test_store_scope_td_uc_allows_global_recipient_without_documents() -> None:
    profile = {"code": "store_reports", "scope": "store", "attach_mode": "per_store_pdf"}
    template = {"subject_template": "{{ store_code }}", "body_template": "{{ store_name }}"}
    recipients = [{"store_code": None, "email_address": "ops@example.com", "send_as": "to"}]

    plans = _build_store_plans(
        pipeline_code="td_orders_sync",
        profile=profile,
        template=template,
        recipients=recipients,
        docs=[],
        context={"stores": [{"store_code": "TD001"}]},
        store_names={"TD001": "Store TD001"},
    )

    assert len(plans) == 1
    assert plans[0].store_code == "TD001"
    assert plans[0].to == ["ops@example.com"]
    assert plans[0].attachments == []


def test_td_leads_run_plan_sets_html_body_when_summary_html_is_available() -> None:
    plan = _build_run_plan(
        pipeline_code="td_crm_leads_sync",
        profile={"code": "run_summary", "scope": "run", "attach_mode": "none"},
        template={"subject_template": "TD Leads {{ run_id }}", "body_template": "{{ summary_html or summary_text }}"},
        recipients=[{"store_code": "ALL", "email_address": "ops@example.com", "display_name": None, "send_as": "to"}],
        docs=[],
        context={"run_id": "run-1", "summary_text": "plain summary", "summary_html": "<h3>HTML summary</h3>"},
    )

    assert plan is not None
    assert plan.body_html == "<h3>HTML summary</h3>"


def test_td_leads_run_plan_preserves_actionable_details_html() -> None:
    actionable_html = (
        "<h4>Lead Changes (Actionable Details)</h4>"
        "<table><tr><td>Nia</td><td>9000000000</td><td>created</td></tr></table>"
    )
    plan = _build_run_plan(
        pipeline_code="td_crm_leads_sync",
        profile={"code": "run_summary", "scope": "run", "attach_mode": "none"},
        template={"subject_template": "TD Leads {{ run_id }}", "body_template": "{{ summary_html }}"},
        recipients=[{"store_code": "ALL", "email_address": "ops@example.com", "display_name": None, "send_as": "to"}],
        docs=[],
        context={"run_id": "run-2", "summary_text": "plain summary", "summary_html": actionable_html},
    )

    assert plan is not None
    assert "Lead Changes (Actionable Details)" in (plan.body_html or "")
    assert "Nia" in (plan.body_html or "")


def test_resolve_reporting_mode_suffix_for_td_leads_pipeline() -> None:
    assert (
        _resolve_reporting_mode_suffix(
            pipeline_name="td_crm_leads_sync",
            metrics_payload={"reporting_mode": "day_end"},
        )
        == " [day_end]"
    )


def test_derive_duration_fields_prefers_summary_timestamps_when_metrics_missing() -> None:
    started = datetime(2026, 4, 22, 0, 0, tzinfo=timezone.utc)
    finished = datetime(2026, 4, 22, 0, 1, 7, tzinfo=timezone.utc)

    duration_seconds, duration_human = _derive_duration_fields(
        {"started_at": started, "finished_at": finished, "total_time_taken": ""},
        {},
    )

    assert duration_seconds == 67
    assert duration_human == "00:01:07"

from pathlib import Path
from app.dashboard_downloader.notifications import DocumentRecord


def test_run_plan_all_docs_for_run_includes_daily_and_mtd_documents(tmp_path) -> None:
    main_pdf = tmp_path / "daily.pdf"
    same_day_pdf = tmp_path / "mtd_same_day.pdf"
    short_payments_pdf = tmp_path / "short_payments.pdf"
    missing_payments_workbook = tmp_path / "actual_payments_not_found.xlsx"
    main_pdf.write_bytes(b"daily")
    same_day_pdf.write_bytes(b"same-day")
    short_payments_pdf.write_bytes(b"short-payments")
    missing_payments_workbook.write_bytes(b"xlsx")

    plan = _build_run_plan(
        pipeline_code="reports.daily_sales_report",
        profile={"code": "run_summary", "scope": "run", "attach_mode": "all_docs_for_run"},
        template={"subject_template": "Daily {{ report_date }}", "body_template": "Summary"},
        recipients=[{"store_code": "ALL", "email_address": "ops@example.com", "display_name": None, "send_as": "to"}],
        docs=[
            DocumentRecord(doc_type="daily_sales_report_pdf", store_code=None, path=Path(main_pdf)),
            DocumentRecord(doc_type="daily_sales_short_payments_pdf", store_code=None, path=Path(short_payments_pdf)),
            DocumentRecord(doc_type="daily_sales_actual_payments_not_found_xlsx", store_code=None, path=Path(missing_payments_workbook)),
            DocumentRecord(doc_type="mtd_same_day_fulfillment_pdf", store_code=None, path=Path(same_day_pdf)),
        ],
        context={"report_date": "2026-04-29"},
    )

    assert plan is not None
    assert plan.attachments == [main_pdf, short_payments_pdf, missing_payments_workbook, same_day_pdf]


def test_profiler_context_and_html_include_failed_window_reason() -> None:
    cert_error = "Page.goto: net::ERR_CERT_DATE_INVALID at https://example.test/orders\n    stack details"
    run_data = {
        "run_id": "profiler-run-1",
        "run_env": "test",
        "report_date": "2024-01-05",
        "overall_status": "failed",
        "summary_text": "Orders Sync Profiler Run Summary",
        "started_at": "2024-01-05T05:00:00+00:00",
        "finished_at": "2024-01-05T05:01:00+00:00",
        "metrics_json": {
            "notification_payload": {
                "overall_status": "failed",
                "window_summary": {"completed_windows": 0, "expected_windows": 1, "missing_windows": 0},
                "stores": [
                    {
                        "store_code": "UC01",
                        "pipeline_name": "uc_orders_sync",
                        "status": "failed",
                        "window_count": 1,
                        "primary_metrics": {},
                        "secondary_metrics": {},
                        "window_audit": [
                            {
                                "from_date": "2024-01-01",
                                "to_date": "2024-01-02",
                                "status": "failed",
                                "status_note": "window execution failed",
                                "error_message": cert_error,
                            }
                        ],
                    }
                ],
            }
        },
    }

    context = _build_profiler_context(run_data)
    html_context = dict(context)
    html_context.update({"run_id": "profiler-run-1", "run_env": "test", "report_date": "2024-01-05"})
    body_html = _render_template(PROFILER_HTML_TEMPLATE, html_context)

    assert context["stores"][0]["failed_windows"][0]["from_date"] == "2024-01-01"
    assert context["stores"][0]["failed_windows"][0]["to_date"] == "2024-01-02"
    assert context["stores"][0]["failed_windows"][0]["status"] == "failed"
    assert "Page.goto: net::ERR_CERT_DATE_INVALID" in context["stores"][0]["failed_windows_note"]
    assert "Page.goto: net::ERR_CERT_DATE_INVALID" in body_html
    assert "status_note=window execution failed" in body_html


def test_profiler_context_and_html_include_td_garment_warning_details() -> None:
    run_data = {
        "run_id": "profiler-run-td",
        "run_env": "test",
        "report_date": "2024-02-02",
        "overall_status": "success_with_warnings",
        "summary_text": "Orders Sync Profiler Run Summary",
        "started_at": "2024-02-02T05:00:00+00:00",
        "finished_at": "2024-02-02T05:01:00+00:00",
        "metrics_json": {
            "notification_payload": {
                "overall_status": "success_with_warnings",
                "warnings": ["TD_GARMENT_DATA_INCOMPLETE: TD01 had 1 incomplete garment window(s); garment-dependent downstream reports may be incomplete"],
                "window_summary": {"completed_windows": 1, "expected_windows": 1, "missing_windows": 0},
                "stores": [
                    {
                        "store_code": "TD01",
                        "pipeline_name": "td_orders_sync",
                        "status": "success_with_warnings",
                        "window_count": 1,
                        "td_garment_warning_count": 1,
                        "td_garment_incomplete_windows": [
                            {
                                "store_code": "TD01",
                                "from_date": "2024-02-01",
                                "to_date": "2024-02-02",
                                "garments_fetch_completeness": "incomplete",
                                "garments_final_row_count": 17,
                                "garments_budget_state": "near_limit",
                            }
                        ],
                        "primary_metrics": {},
                        "secondary_metrics": {},
                    }
                ],
            }
        },
    }

    context = _build_profiler_context(run_data)
    html_context = dict(context)
    html_context.update({"run_id": "profiler-run-td", "run_env": "test", "report_date": "2024-02-02"})
    body_html = _render_template(PROFILER_HTML_TEMPLATE, html_context)

    assert context["td_garment_warning_count"] == 1
    assert context["td_garment_warning_details"][0]["store_code"] == "TD01"
    assert context["td_garment_warning_details"][0]["garments_final_row_count"] == 17
    assert context["stores"][0]["td_garment_incomplete_windows"][0]["from_date"] == "2024-02-01"
    assert "DATA INCOMPLETE: TD garment details incomplete 2024-02-01 to 2024-02-02" in body_html
    assert "final garment rows=17" in body_html

def test_send_email_uses_bounded_smtp_timeout(monkeypatch) -> None:
    from app.dashboard_downloader import notifications
    from app.dashboard_downloader.notifications import EmailPlan, SmtpConfig, _send_email

    captured: dict[str, object] = {}

    class FakeSMTP:
        def __init__(self, host, port, timeout=None):
            captured["host"] = host
            captured["port"] = port
            captured["timeout"] = timeout

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def send_message(self, message, to_addrs):
            captured["to_addrs"] = to_addrs

    monkeypatch.setattr(notifications.smtplib, "SMTP", FakeSMTP)

    result = _send_email(
        SmtpConfig(
            host="smtp.example.test",
            port=587,
            sender="sender@example.test",
            username=None,
            password=None,
            use_tls=False,
        ),
        EmailPlan(
            profile_code="default",
            scope="pipeline",
            store_code=None,
            subject="Subject",
            body="Body",
            to=["ops@example.test"],
            cc=[],
            bcc=[],
            attachments=[],
        ),
    )

    assert result.sent is True
    assert result.failure is None
    assert captured["timeout"] == notifications.SMTP_CONNECT_TIMEOUT_SECONDS


def test_send_email_retries_connection_reset_during_starttls_then_succeeds(monkeypatch) -> None:
    from app.dashboard_downloader import notifications
    from app.dashboard_downloader.notifications import (
        EmailPlan,
        NotificationSendRetryConfig,
        SmtpConfig,
        _send_email,
    )

    attempts = {"starttls": 0, "send": 0}

    class FlakyStartTlsSMTP:
        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def starttls(self):
            attempts["starttls"] += 1
            if attempts["starttls"] == 1:
                raise ConnectionResetError("connection reset during TLS negotiation")

        def login(self, _username, _password):
            return None

        def send_message(self, _message, to_addrs):
            attempts["send"] += 1
            assert to_addrs == ["ops@example.test"]

    monkeypatch.setattr(notifications.smtplib, "SMTP", FlakyStartTlsSMTP)
    monkeypatch.setattr(
        notifications,
        "_load_notification_send_retry_config",
        lambda: NotificationSendRetryConfig(
            max_attempts=2,
            initial_delay_seconds=0,
            max_delay_seconds=0,
            transient_exception_types=(ConnectionResetError,),
        ),
    )

    result = _send_email(
        SmtpConfig(
            host="smtp.example.test",
            port=587,
            sender="sender@example.test",
            username="smtp-user",
            password="secret-token",
            use_tls=True,
        ),
        EmailPlan(
            profile_code="run_summary",
            scope="run",
            store_code=None,
            subject="Subject",
            body="Body",
            to=["ops@example.test"],
            cc=[],
            bcc=[],
            attachments=[],
        ),
    )

    assert attempts == {"starttls": 2, "send": 1}
    assert result.sent is True
    assert result.failure is None


@pytest.mark.asyncio
async def test_send_notifications_records_smtp_exception(monkeypatch) -> None:
    from app.dashboard_downloader import notifications
    from app.dashboard_downloader.notifications import (
        SmtpConfig,
        send_notifications_for_run,
    )

    async def fake_load_notification_resources(_pipeline_name: str, _run_id: str):
        return (
            {
                "pipeline": {"description": "Test pipeline"},
                "run": {
                    "run_env": "local",
                    "report_date": date(2026, 5, 29),
                    "overall_status": "ok",
                    "total_time_taken": "00:00:01",
                    "summary_text": "summary",
                    "metrics_json": {},
                    "started_at": datetime(2026, 5, 29, 0, 0, tzinfo=timezone.utc),
                    "finished_at": datetime(2026, 5, 29, 0, 0, 1, tzinfo=timezone.utc),
                },
                "docs": [],
                "profiles": [
                    {"id": 1, "code": "run_summary", "scope": "run", "attach_mode": "none"}
                ],
                "templates": {
                    1: {
                        "subject_template": "Run {{ run_id }}",
                        "body_template": "{{ summary_text }}",
                    }
                },
                "recipients": {
                    1: [
                        {
                            "store_code": "ALL",
                            "email_address": "ops@example.test",
                            "display_name": None,
                            "send_as": "to",
                        }
                    ]
                },
                "store_names": {},
                "profiler_missing_windows": {},
            },
            [],
        )

    class FailingSMTP:
        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def login(self, _username, _password):
            return None

        def send_message(self, _message, to_addrs):
            raise notifications.smtplib.SMTPException(
                f"failed for {to_addrs[0]} using secret-token"
            )

    monkeypatch.setattr(
        notifications, "_load_notification_resources", fake_load_notification_resources
    )
    monkeypatch.setattr(
        notifications,
        "_load_smtp_config",
        lambda: SmtpConfig(
            host="smtp.example.test",
            port=587,
            sender="sender@example.test",
            username="smtp-user",
            password="secret-token",
            use_tls=False,
        ),
    )
    monkeypatch.setattr(notifications.smtplib, "SMTP", FailingSMTP)

    result = await send_notifications_for_run("test_pipeline", "run-1")

    assert result["emails_planned"] == 1
    assert result["emails_sent"] == 0
    assert result["errors"]
    error = result["errors"][0]
    assert error["profile_code"] == "run_summary"
    assert error["store_code"] is None
    assert error["recipient_count"] == 1
    assert error["recipients"] == ["o***@example.test"]
    assert error["exception_type"] == "SMTPException"
    assert "ops@example.test" not in error["exception_summary"]
    assert "secret-token" not in error["exception_summary"]


@pytest.mark.asyncio
async def test_send_notifications_retries_transient_smtp_failure_then_succeeds(monkeypatch) -> None:
    from app.dashboard_downloader import notifications
    from app.dashboard_downloader.notifications import (
        NotificationSendRetryConfig,
        SmtpConfig,
        send_notifications_for_run,
    )

    attempts = {"count": 0}

    async def fake_load_notification_resources(_pipeline_name: str, _run_id: str):
        return (
            {
                "pipeline": {"description": "Test pipeline"},
                "run": {
                    "run_env": "local",
                    "report_date": date(2026, 5, 29),
                    "overall_status": "ok",
                    "total_time_taken": "00:00:01",
                    "summary_text": "summary",
                    "metrics_json": {},
                    "started_at": datetime(2026, 5, 29, 0, 0, tzinfo=timezone.utc),
                    "finished_at": datetime(2026, 5, 29, 0, 0, 1, tzinfo=timezone.utc),
                },
                "docs": [],
                "profiles": [
                    {"id": 1, "code": "run_summary", "scope": "run", "attach_mode": "none"}
                ],
                "templates": {
                    1: {
                        "subject_template": "Run {{ run_id }}",
                        "body_template": "{{ summary_text }}",
                    }
                },
                "recipients": {
                    1: [
                        {
                            "store_code": "ALL",
                            "email_address": "ops@example.test",
                            "display_name": None,
                            "send_as": "to",
                        }
                    ]
                },
                "store_names": {},
                "profiler_missing_windows": {},
            },
            [],
        )

    class FlakySMTP:
        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def login(self, _username, _password):
            return None

        def send_message(self, _message, to_addrs):
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise notifications.smtplib.SMTPServerDisconnected("temporary disconnect")
            assert to_addrs == ["ops@example.test"]

    monkeypatch.setattr(
        notifications, "_load_notification_resources", fake_load_notification_resources
    )
    monkeypatch.setattr(
        notifications,
        "_load_smtp_config",
        lambda: SmtpConfig(
            host="smtp.example.test",
            port=587,
            sender="sender@example.test",
            username="smtp-user",
            password="secret-token",
            use_tls=False,
        ),
    )
    monkeypatch.setattr(
        notifications,
        "_load_notification_send_retry_config",
        lambda: NotificationSendRetryConfig(
            max_attempts=3,
            initial_delay_seconds=0,
            max_delay_seconds=0,
            transient_exception_types=(notifications.smtplib.SMTPServerDisconnected,),
        ),
    )
    monkeypatch.setattr(notifications.smtplib, "SMTP", FlakySMTP)

    result = await send_notifications_for_run("test_pipeline", "run-1")

    assert attempts["count"] == 3
    assert result["emails_planned"] == 1
    assert result["emails_sent"] == 1
    assert result["errors"] == []


def test_resolve_transient_exception_types_includes_ssl_eof() -> None:
    import ssl

    from app.dashboard_downloader.notifications import _resolve_transient_exception_types

    assert _resolve_transient_exception_types(["ssl.SSLEOFError"]) == (ssl.SSLEOFError,)


@pytest.mark.asyncio
async def test_send_notifications_retries_ssl_eof_during_starttls_then_succeeds(monkeypatch) -> None:
    import ssl

    from app.dashboard_downloader import notifications
    from app.dashboard_downloader.notifications import (
        NotificationSendRetryConfig,
        SmtpConfig,
        send_notifications_for_run,
    )

    attempts = {"starttls": 0, "send_message": 0}

    async def fake_load_notification_resources(_pipeline_name: str, _run_id: str):
        return (
            {
                "pipeline": {"description": "Test pipeline"},
                "run": {
                    "run_env": "local",
                    "report_date": date(2026, 5, 29),
                    "overall_status": "ok",
                    "total_time_taken": "00:00:01",
                    "summary_text": "summary",
                    "metrics_json": {},
                    "started_at": datetime(2026, 5, 29, 0, 0, tzinfo=timezone.utc),
                    "finished_at": datetime(2026, 5, 29, 0, 0, 1, tzinfo=timezone.utc),
                },
                "docs": [],
                "profiles": [
                    {"id": 1, "code": "run_summary", "scope": "run", "attach_mode": "none"}
                ],
                "templates": {
                    1: {
                        "subject_template": "Run {{ run_id }}",
                        "body_template": "{{ summary_text }}",
                    }
                },
                "recipients": {
                    1: [
                        {
                            "store_code": "ALL",
                            "email_address": "ops@example.test",
                            "display_name": None,
                            "send_as": "to",
                        }
                    ]
                },
                "store_names": {},
                "profiler_missing_windows": {},
            },
            [],
        )

    class FlakyStarttlsSMTP:
        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def starttls(self):
            attempts["starttls"] += 1
            if attempts["starttls"] == 1:
                raise ssl.SSLEOFError("temporary TLS EOF")

        def login(self, _username, _password):
            return None

        def send_message(self, _message, to_addrs):
            attempts["send_message"] += 1
            assert to_addrs == ["ops@example.test"]

    monkeypatch.setattr(
        notifications, "_load_notification_resources", fake_load_notification_resources
    )
    monkeypatch.setattr(
        notifications,
        "_load_smtp_config",
        lambda: SmtpConfig(
            host="smtp.example.test",
            port=587,
            sender="sender@example.test",
            username="smtp-user",
            password="secret-token",
            use_tls=True,
        ),
    )
    monkeypatch.setattr(
        notifications,
        "_load_notification_send_retry_config",
        lambda: NotificationSendRetryConfig(
            max_attempts=2,
            initial_delay_seconds=0,
            max_delay_seconds=0,
            transient_exception_types=(notifications.ssl.SSLEOFError,),
        ),
    )
    monkeypatch.setattr(notifications.smtplib, "SMTP", FlakyStarttlsSMTP)

    result = await send_notifications_for_run("test_pipeline", "run-1")

    assert attempts == {"starttls": 2, "send_message": 1}
    assert result["emails_planned"] == 1
    assert result["emails_sent"] == 1
    assert result["errors"] == []


@pytest.mark.asyncio
async def test_send_notifications_does_not_retry_non_transient_configuration_failure(monkeypatch) -> None:
    from app.dashboard_downloader import notifications
    from app.dashboard_downloader.notifications import (
        NotificationSendRetryConfig,
        SmtpConfig,
        send_notifications_for_run,
    )

    attempts = {"count": 0}

    async def fake_load_notification_resources(_pipeline_name: str, _run_id: str):
        return (
            {
                "pipeline": {"description": "Test pipeline"},
                "run": {
                    "run_env": "local",
                    "report_date": date(2026, 5, 29),
                    "overall_status": "ok",
                    "total_time_taken": "00:00:01",
                    "summary_text": "summary",
                    "metrics_json": {},
                    "started_at": datetime(2026, 5, 29, 0, 0, tzinfo=timezone.utc),
                    "finished_at": datetime(2026, 5, 29, 0, 0, 1, tzinfo=timezone.utc),
                },
                "docs": [],
                "profiles": [
                    {"id": 1, "code": "run_summary", "scope": "run", "attach_mode": "none"}
                ],
                "templates": {
                    1: {
                        "subject_template": "Run {{ run_id }}",
                        "body_template": "{{ summary_text }}",
                    }
                },
                "recipients": {
                    1: [
                        {
                            "store_code": "ALL",
                            "email_address": "ops@example.test",
                            "display_name": None,
                            "send_as": "to",
                        }
                    ]
                },
                "store_names": {},
                "profiler_missing_windows": {},
            },
            [],
        )

    class AuthFailingSMTP:
        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def login(self, _username, _password):
            attempts["count"] += 1
            raise notifications.smtplib.SMTPAuthenticationError(535, b"bad credentials")

        def send_message(self, _message, to_addrs):
            raise AssertionError("send_message should not run after auth failure")

    monkeypatch.setattr(
        notifications, "_load_notification_resources", fake_load_notification_resources
    )
    monkeypatch.setattr(
        notifications,
        "_load_smtp_config",
        lambda: SmtpConfig(
            host="smtp.example.test",
            port=587,
            sender="sender@example.test",
            username="smtp-user",
            password="secret-token",
            use_tls=False,
        ),
    )
    monkeypatch.setattr(
        notifications,
        "_load_notification_send_retry_config",
        lambda: NotificationSendRetryConfig(
            max_attempts=4,
            initial_delay_seconds=0,
            max_delay_seconds=0,
            transient_exception_types=(notifications.smtplib.SMTPServerDisconnected,),
        ),
    )
    monkeypatch.setattr(notifications.smtplib, "SMTP", AuthFailingSMTP)

    result = await send_notifications_for_run("test_pipeline", "run-1")

    assert attempts["count"] == 1
    assert result["emails_planned"] == 1
    assert result["emails_sent"] == 0
    assert result["errors"]
    assert result["errors"][0]["exception_type"] == "SMTPAuthenticationError"
    assert result["errors"][0]["attempt_count"] == 1
    assert result["errors"][0]["final_exception_type"] == "SMTPAuthenticationError"


def test_send_email_does_not_retry_recipient_resolution_failure(monkeypatch) -> None:
    from app.dashboard_downloader import notifications
    from app.dashboard_downloader.notifications import EmailPlan, SmtpConfig, _send_email

    smtp_calls = {"count": 0}

    class UnexpectedSMTP:
        def __init__(self, *_args, **_kwargs):
            smtp_calls["count"] += 1

    monkeypatch.setattr(notifications.smtplib, "SMTP", UnexpectedSMTP)

    result = _send_email(
        SmtpConfig(
            host="smtp.example.test",
            port=587,
            sender="sender@example.test",
            username="smtp-user",
            password="secret-token",
            use_tls=False,
        ),
        EmailPlan(
            profile_code="run_summary",
            scope="run",
            store_code=None,
            subject="Subject",
            body="Body",
            to=[],
            cc=[],
            bcc=[],
            attachments=[],
        ),
    )

    assert smtp_calls["count"] == 0
    assert result.sent is False
    assert result.failure is not None
    assert result.failure.exception_type == "NoRecipients"
    assert result.failure.attempt_count == 1

def test_dashboard_notification_summary_includes_data_quality_threshold_text() -> None:
    from app.dashboard_downloader.notifications import _append_dashboard_data_quality_warnings

    summary = _append_dashboard_data_quality_warnings(
        "Pipeline summary",
        {
            "data_quality_warnings": {
                "breaches": [
                    {"code": "invalid_csv_downloads", "count": 1, "threshold": 1},
                    {"code": "skipped_required_rows", "count": 2, "threshold": 1},
                ]
            }
        },
    )

    assert "Pipeline summary" in summary
    assert "Dashboard data quality warnings:" in summary
    assert "invalid CSV downloads discarded: 1 observed (threshold 1)" in summary
    assert "rows skipped due to missing required fields: 2 observed (threshold 1)" in summary


def test_report_notification_context_includes_upstream_orders_status() -> None:
    from app.dashboard_downloader.notifications import (
        _append_orders_sync_degraded_warning,
        _orders_sync_upstream_context,
    )

    metrics = {
        "orders_sync_upstream": {
            "status": "failed",
            "run_id": "orders-run-1",
            "is_degraded": True,
        }
    }

    context = _orders_sync_upstream_context(metrics)
    summary = _append_orders_sync_degraded_warning("Report generated.", metrics)

    assert context["orders_sync_is_degraded"] is True
    assert context["orders_sync_upstream_status"] == "failed"
    assert context["orders_sync_upstream_run_id"] == "orders-run-1"
    assert "Orders sync was degraded before this report; data may be stale or incomplete." in summary
    assert "run_id=orders-run-1" in summary
