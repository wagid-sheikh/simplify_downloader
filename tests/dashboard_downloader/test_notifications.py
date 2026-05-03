from __future__ import annotations

from datetime import datetime, timezone

from app.dashboard_downloader.notifications import (
    _build_fact_rows,
    _build_run_plan,
    _derive_duration_fields,
    _build_store_plans,
    _build_uc_orders_context,
    _format_fact_sections_text,
    _prepare_ingest_remarks,
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
    main_pdf.write_bytes(b"daily")
    same_day_pdf.write_bytes(b"same-day")

    plan = _build_run_plan(
        pipeline_code="reports.daily_sales_report",
        profile={"code": "run_summary", "scope": "run", "attach_mode": "all_docs_for_run"},
        template={"subject_template": "Daily {{ report_date }}", "body_template": "Summary"},
        recipients=[{"store_code": "ALL", "email_address": "ops@example.com", "display_name": None, "send_as": "to"}],
        docs=[
            DocumentRecord(doc_type="daily_sales_report_pdf", store_code=None, path=Path(main_pdf)),
            DocumentRecord(doc_type="mtd_same_day_fulfillment_pdf", store_code=None, path=Path(same_day_pdf)),
        ],
        context={"report_date": "2026-04-29"},
    )

    assert plan is not None
    assert plan.attachments == [main_pdf, same_day_pdf]
