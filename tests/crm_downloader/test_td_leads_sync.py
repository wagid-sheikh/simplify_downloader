from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from app.crm_downloader.td_leads_sync.ingest import build_lead_uid
from app.crm_downloader.td_leads_sync import ingest as td_leads_ingest
from app.crm_downloader.td_leads_sync import main as td_leads_main
from app.crm_downloader.td_leads_sync.main import (
    LeadsRunSummary,
    StoreLeadResult,
    _collect_status_rows,
    _build_td_leads_summary_html,
    _build_td_leads_tables_html,
    _available_pager_args,
    _ensure_scheduler_page,
    _field_from_headers,
    _find_tz_aware_columns,
    _postback_page_arg,
    _sanitize_rows_for_xlsx_export,
    _scrape_grid_rows,
    _write_store_artifact,
)
from app.dashboard_downloader.notifications import send_notifications_for_run
from app.config import config as app_config


def test_build_lead_uid_is_stable_for_same_business_identity() -> None:
    base = {
        "store_code": "A668",
        "status_bucket": "pending",
        "pickup_id": "4434944",
        "pickup_no": "A668-3025",
        "mobile": "9599242207",
        "pickup_date": "22 Apr 2026",
        "pickup_time": "11:00 AM - 1:00 PM",
    }
    uid_a = build_lead_uid(base)
    uid_b = build_lead_uid(dict(base))
    assert uid_a == uid_b


def test_build_lead_uid_is_stable_when_status_changes() -> None:
    row = {
        "store_code": "A668",
        "status_bucket": "pending",
        "pickup_id": "4434944",
        "pickup_no": "A668-3025",
        "mobile": "9599242207",
        "pickup_date": "22 Apr 2026",
        "pickup_time": "11:00 AM - 1:00 PM",
    }
    pending_uid = build_lead_uid(row)
    cancelled_uid = build_lead_uid({**row, "status_bucket": "cancelled"})
    assert pending_uid == cancelled_uid


def test_build_lead_uid_uses_store_code_and_pickup_no_identity_only() -> None:
    row_with_mobile = {
        "store_code": "A668",
        "status_bucket": "pending",
        "pickup_id": "4434944",
        "pickup_no": "A668-3025",
        "mobile": "9599242207",
        "pickup_date": "21 Apr 2026",
        "pickup_created_date": "21 Apr 2026 3:03:39 PM",
        "pickup_time": "3:03:39 PM",
    }
    row_without_mobile = {
        **row_with_mobile,
        "mobile": "",
        "pickup_date": "22 Apr 2026",
        "pickup_created_date": "22 Apr 2026 8:03:39 AM",
    }

    assert build_lead_uid(row_with_mobile) == build_lead_uid(row_without_mobile)


@pytest.mark.parametrize(
    ("field_name", "expected"),
    [
        ("pickup_no", "A668-3025"),
        ("customer_name", "moni"),
        ("mobile", "9599242207"),
        ("pickup_date", "22 Apr 2026"),
        ("pickup_time", "11:00 AM - 1:00 PM"),
        ("special_instruction", "Leave at door"),
        ("status_text", "CANCELLED"),
        ("reason", "enquiry"),
        ("source", "Facebook"),
        ("user", "Super Admin"),
    ],
)
def test_field_from_headers_uses_header_name_mapping(field_name: str, expected: str) -> None:
    headers = [
        "S No.",
        "Pickup No.",
        "Customer Name",
        "Address",
        "Mobile",
        "Pickup Date",
        "Pickup Time",
        "Special Instruction",
        "Status",
        "Reason",
        "Source",
        "User",
    ]
    values = [
        "1",
        "A668-3025",
        "moni",
        "Address line",
        "9599242207",
        "22 Apr 2026",
        "11:00 AM - 1:00 PM",
        "Leave at door",
        "CANCELLED",
        "enquiry",
        "Facebook",
        "Super Admin",
    ]

    resolved = _field_from_headers(headers=headers, values=values, field_name=field_name)
    assert resolved == expected


def test_field_from_headers_returns_none_when_alias_not_present() -> None:
    assert _field_from_headers(headers=["Foo"], values=["Bar"], field_name="pickup_no") is None


def test_field_from_headers_maps_created_datetime_header_aliases() -> None:
    headers = ["Created Date / Time", "Created Datetime", "Created Date Time"]
    values = ["21 Apr 2026 3:03:39 PM", "21 Apr 2026 4:03:39 PM", "21 Apr 2026 5:03:39 PM"]

    resolved = _field_from_headers(headers=headers, values=values, field_name="pickup_created_at")

    assert resolved == "21 Apr 2026 3:03:39 PM"

def test_field_from_headers_does_not_treat_created_date_as_pickup_date() -> None:
    headers = ["Created Date", "Pickup No."]
    values = ["21 Apr 2026", "A668-3025"]

    resolved_pickup_date = _field_from_headers(headers=headers, values=values, field_name="pickup_date")
    resolved_created = _field_from_headers(headers=headers, values=values, field_name="pickup_created_at")

    assert resolved_pickup_date is None
    assert resolved_created == "21 Apr 2026"



def test_scraped_at_value_can_pass_through() -> None:
    now_utc = datetime.now(timezone.utc)
    row = {
        "store_code": "A668",
        "status_bucket": "pending",
        "pickup_id": "1",
        "pickup_no": "A668-1",
        "mobile": "9999999999",
        "pickup_date": "22 Apr 2026",
        "pickup_time": "11:00 AM - 1:00 PM",
        "scraped_at": now_utc,
    }
    assert build_lead_uid(row)


def test_coerce_pickup_created_at_accepts_date_only_created_date() -> None:
    coerced = td_leads_ingest._coerce_pickup_created_at(
        row={"pickup_created_at": None},
        normalized_created_date="21 Apr 2026",
    )

    assert coerced is not None


def test_coerce_pickup_created_at_converts_date_only_from_ist_midnight_to_utc() -> None:
    coerced = td_leads_ingest._coerce_pickup_created_at(
        row={"pickup_created_at": None},
        normalized_created_date="21 Apr 2026",
    )

    assert coerced == datetime(2026, 4, 20, 18, 30, tzinfo=timezone.utc)


def test_sanitize_rows_for_xlsx_export_converts_tz_aware_datetime_and_iso_strings() -> None:
    aware_value = datetime(2026, 4, 22, 6, 30, tzinfo=timezone.utc)
    rows = [
        {
            "pickup_date": "22 Apr 2026",
            "pickup_created_at": "2026-04-22T11:00:00+05:30",
            "scraped_at": aware_value,
            "mobile": "9999999999",
        }
    ]

    sanitized = _sanitize_rows_for_xlsx_export(rows=rows)

    assert sanitized[0]["pickup_date"] == "22 Apr 2026"
    assert sanitized[0]["pickup_created_at"] == datetime(2026, 4, 22, 11, 0)
    assert sanitized[0]["scraped_at"] == datetime(2026, 4, 22, 6, 30)
    assert sanitized[0]["scraped_at"].tzinfo is None
    assert rows[0]["scraped_at"] is aware_value
    assert rows[0]["pickup_date"] == "22 Apr 2026"
    assert rows[0]["pickup_created_at"] == "2026-04-22T11:00:00+05:30"


def test_find_tz_aware_columns_flags_remaining_tz_values() -> None:
    rows = [
        {
            "pickup_date": "22 Apr 2026",
            "scraped_at": datetime(2026, 4, 22, 6, 30, tzinfo=timezone.utc),
            "mobile": "9999999999",
        }
    ]

    tz_columns = _find_tz_aware_columns(rows=rows, columns=["pickup_date", "scraped_at", "mobile"])

    assert tz_columns == {"scraped_at"}


def test_run_summary_record_includes_duration_for_failed_store_runs() -> None:
    started_at = datetime(2026, 4, 22, 0, 0, tzinfo=timezone.utc)
    finished_at = datetime(2026, 4, 22, 0, 2, 30, tzinfo=timezone.utc)
    summary = LeadsRunSummary(
        run_id="run-1",
        run_env="local",
        report_date=started_at.date(),
        started_at=started_at,
        store_results={
            "A668": StoreLeadResult(store_code="A668", status="error", message="store timed out"),
        },
    )

    record = summary.build_record(finished_at=finished_at)

    assert record["overall_status"] == "failed"
    assert record["total_time_taken"] == "00:02:30"
    assert record["metrics_json"]["duration_seconds"] == 150
    assert record["metrics_json"]["duration_human"] == "00:02:30"


def test_td_leads_summary_html_renders_business_sections_and_footer_refs() -> None:
    summary = LeadsRunSummary(
        run_id="run-1",
        run_env="local",
        report_date=datetime(2026, 4, 22, tzinfo=timezone.utc).date(),
        store_results={
            "A817": StoreLeadResult(
                store_code="A817",
                rows=[
                    {
                        "status_bucket": "pending",
                        "customer_name": "Nia",
                        "mobile": "9000000000",
                        "pickup_id": "P-10",
                        "pickup_date": "2026-04-22T10:15:00+00:00",
                        "status_text": "Pending",
                    },
                    {
                        "status_bucket": "cancelled",
                        "customer_name": "Raj",
                        "mobile": "9111111111",
                        "pickup_no": "A817-2",
                        "pickup_id": "C-2",
                        "pickup_date": "2026-04-22T09:00:00+00:00",
                        "status_text": "Cancelled",
                        "reason": "No inventory",
                    },
                ],
                status_counts={"pending": 1, "completed": 0, "cancelled": 1},
                ingested_rows=2,
                artifact_path="app/crm_downloader/data/A817-crm_leads.xlsx",
                lead_change_details={
                    "created_by_bucket": [
                        {
                            "status_bucket": "pending",
                            "rows": [
                                {
                                    "customer_name": "Nia",
                                    "mobile": "9000000000",
                                    "action": "created",
                                    "current_status_bucket": "pending",
                                    "previous_status_bucket": None,
                                    "lead_identity": {"pickup_no": "A817-1"},
                                }
                            ],
                            "overflow_count": 2,
                        }
                    ],
                    "updated_by_bucket": [],
                    "transitions": [],
                },
            )
        },
    )

    summary_html = _build_td_leads_summary_html(summary=summary, duration_human="00:01:00")

    assert "Lead details by store" in summary_html
    assert "New Leads created" in summary_html
    assert "Leads Marked as Cancelled" in summary_html
    assert "Pending Leads" in summary_html
    assert "A817" in summary_html
    assert "Total stores processed:</strong> 1" in summary_html
    assert "Runtime duration:</strong> 00:01:00" in summary_html
    assert "Reference run_id: <code>run-1</code>" in summary_html
    assert "Completed</h5>" not in summary_html
    assert "Converted" not in summary_html


def test_build_lead_uid_ignores_status_bucket_for_transition_tracking() -> None:
    base_row = {
        "store_code": "A668",
        "pickup_id": "4434944",
        "pickup_no": "A668-3025",
        "mobile": "9599242207",
        "pickup_created_date": "21 Apr 2026 3:03:39 PM",
        "pickup_time": "11:00 AM - 1:00 PM",
    }
    pending_uid = build_lead_uid({**base_row, "status_bucket": "pending"})
    completed_uid = build_lead_uid({**base_row, "status_bucket": "completed"})

    assert pending_uid == completed_uid


def test_td_leads_run_summary_record_exposes_summary_html_in_metrics() -> None:
    started_at = datetime(2026, 4, 22, 0, 0, tzinfo=timezone.utc)
    finished_at = datetime(2026, 4, 22, 0, 1, tzinfo=timezone.utc)
    summary = LeadsRunSummary(
        run_id="run-4",
        run_env="local",
        report_date=started_at.date(),
        started_at=started_at,
        store_results={
            "A668": StoreLeadResult(
                store_code="A668",
                rows=[{"status_bucket": "pending", "customer_name": "Ada", "pickup_date": "2026-04-22"}],
            )
        },
    )

    record = summary.build_record(finished_at=finished_at)

    assert "Total Stores Processed: 1" in record["summary_text"]
    assert "status=pending, customer_name=Ada" not in record["summary_text"]
    assert "No new leads/status changed across all stores." in record["metrics_json"]["summary_html"]
    assert "No new leads/status changed across all stores." in record["metrics_json"]["lead_tables_html"]
    assert "A668" in record["summary_text"]
    assert "lead_change_details" in record["metrics_json"]["stores"][0]
    assert "lead_change_details" in record["metrics_json"]
    assert "rows" not in record["metrics_json"]["stores"][0]


def test_td_leads_tables_html_renders_three_business_sections_per_store() -> None:
    pending_rows = [
        {
            "status_bucket": "pending",
            "pickup_code": f"P-{index}",
            "customer_name": f"Pending {index}",
            "mobile": "9000000000",
            "address": "Area 1",
            "pickup_date": "2026-04-22 10:15",
            "pickup_time": "10:15 AM - 12:15 PM",
        }
        for index in range(1, 53)
    ]
    summary = LeadsRunSummary(
        run_id="run-html",
        run_env="local",
        report_date=datetime(2026, 4, 22, tzinfo=timezone.utc).date(),
        store_results={
            "A817": StoreLeadResult(
                store_code="A817",
                rows=[
                    *pending_rows,
                    {
                        "status_bucket": "cancelled",
                        "pickup_no": "A817-C2",
                        "customer_name": "Raj",
                        "mobile": "9111111111",
                        "reason": "No inventory",
                        "pickup_date": "2026-04-22 09:00",
                        "source": "Walk-in",
                    },
                ],
                status_transitions=[
                    {
                        "pickup_no": "A817-C2",
                        "from_status_bucket": "pending",
                        "to_status_bucket": "cancelled",
                    }
                ],
                lead_change_details={
                    "created_by_bucket": [
                        {
                            "status_bucket": "pending",
                            "rows": [
                                {
                                    "customer_name": "Pending 1",
                                    "mobile": "9000000000",
                                    "lead_identity": {"pickup_no": "P-1"},
                                }
                            ],
                            "overflow_count": 0,
                        }
                    ]
                },
            )
        },
    )

    tables_html = _build_td_leads_tables_html(summary=summary)

    assert "Store A817" in tables_html
    assert "<h5 style='margin:10px 0 6px 0;'>New Leads created (1)</h5>" in tables_html
    assert "<h5 style='margin:10px 0 6px 0;'>Leads Marked as Cancelled (1 transitions this run)</h5>" in tables_html
    assert "<h5 style='margin:10px 0 6px 0;'>Pending Leads (52)</h5>" in tables_html
    assert "Pending 1" in tables_html
    assert "Raj" in tables_html
    assert "Pending 52" in tables_html
    assert "<th align='left'>Customer Name</th><th align='left'>Mobile Number</th><th align='left'>Source</th>" in tables_html
    assert "<th align='left'>Customer Name</th><th align='left'>Mobile Number</th><th align='left'>Flag</th><th align='left'>Reason</th><th align='left'>Source</th>" in tables_html
    assert "<th align='left'>Customer Name</th><th align='left'>Mobile Number</th><th align='left'>Created Date/Time</th><th align='left'>Source</th>" in tables_html
    assert "Completed</h5>" not in tables_html
    assert "Converted" not in tables_html


def test_td_leads_tables_html_sorts_pending_and_cancelled_rows_by_created_datetime_desc() -> None:
    summary = LeadsRunSummary(
        run_id="run-html-sort",
        run_env="local",
        report_date=datetime(2026, 4, 22, tzinfo=timezone.utc).date(),
        store_results={
            "A817": StoreLeadResult(
                store_code="A817",
                rows=[
                    {
                        "status_bucket": "pending",
                        "pickup_code": "P-legacy-1",
                        "customer_name": "Legacy One",
                        "mobile": "9000000001",
                        "address": "Area 1",
                        "pickup_date": "not-a-date",
                        "pickup_time": "—",
                    },
                    {
                        "status_bucket": "pending",
                        "pickup_code": "P-2",
                        "customer_name": "Recent",
                        "mobile": "9000000002",
                        "address": "Area 2",
                        "pickup_date": "21 Apr 2026 3:03:39 PM",
                        "pickup_created_at": datetime(2026, 4, 21, 15, 3, 39),
                        "pickup_time": "3:03:39 PM",
                    },
                    {
                        "status_bucket": "pending",
                        "pickup_code": "P-3",
                        "customer_name": "Most Recent",
                        "mobile": "9000000003",
                        "address": "Area 3",
                        "pickup_date": "22 Apr 2026 9:03:39 PM",
                        "pickup_created_at": datetime(2026, 4, 22, 21, 3, 39),
                        "pickup_time": "9:03:39 PM",
                    },
                    {
                        "status_bucket": "pending",
                        "pickup_code": "P-legacy-2",
                        "customer_name": "Legacy Two",
                        "mobile": "9000000004",
                        "address": "Area 4",
                        "pickup_date": "older-text",
                        "pickup_time": "—",
                    },
                    {
                        "status_bucket": "cancelled",
                        "pickup_code": "X-1",
                        "pickup_no": "A817-C1",
                        "customer_name": "Cancelled Earlier",
                        "mobile": "9222222221",
                        "address": "Area 7",
                        "pickup_date": "19 Apr 2026 11:00:00 AM",
                        "pickup_created_at": datetime(2026, 4, 19, 11, 0, 0),
                        "reason": "No slot",
                    },
                    {
                        "status_bucket": "cancelled",
                        "pickup_code": "X-2",
                        "pickup_no": "A817-C2",
                        "customer_name": "Cancelled Latest",
                        "mobile": "9222222222",
                        "address": "Area 8",
                        "pickup_date": "22 Apr 2026 01:00:00 PM",
                        "pickup_created_at": datetime(2026, 4, 22, 13, 0, 0),
                        "reason": "No inventory",
                    },
                ],
                status_transitions=[
                    {"pickup_no": "A817-C1", "from_status_bucket": "pending", "to_status_bucket": "cancelled"},
                    {"pickup_no": "A817-C2", "from_status_bucket": "pending", "to_status_bucket": "cancelled"},
                ],
            )
        },
    )

    tables_html = _build_td_leads_tables_html(summary=summary)

    assert tables_html.index("Most Recent") < tables_html.index("Recent")
    assert tables_html.index("Recent") < tables_html.index("Legacy One")
    assert tables_html.index("Legacy One") < tables_html.index("Legacy Two")
    assert tables_html.index("Cancelled Latest") < tables_html.index("Cancelled Earlier")




def test_td_leads_tables_html_pending_prefers_pickup_created_text_then_falls_back_to_pickup_created_at() -> None:
    summary = LeadsRunSummary(
        run_id="run-pending-display",
        run_env="local",
        report_date=datetime(2026, 4, 22, tzinfo=timezone.utc).date(),
        store_results={
            "A817": StoreLeadResult(
                store_code="A817",
                rows=[
                    {
                        "status_bucket": "pending",
                        "customer_name": "Text First",
                        "mobile": "9000000001",
                        "pickup_created_text": "21 Apr 2026 3:03:39 PM",
                        "pickup_created_at": datetime(2026, 4, 21, 9, 33, 39, tzinfo=timezone.utc),
                        "pickup_no": "A817-1",
                    },
                    {
                        "status_bucket": "pending",
                        "customer_name": "Datetime Fallback",
                        "mobile": "9000000002",
                        "pickup_created_at": datetime(2026, 4, 21, 9, 33, 39, tzinfo=timezone.utc),
                        "pickup_no": "A817-2",
                    },
                ],
                lead_change_details={
                    "created_by_bucket": [
                        {
                            "rows": [
                                {"lead_identity": {"pickup_no": "A817-1"}},
                                {"lead_identity": {"pickup_no": "A817-2"}},
                            ]
                        }
                    ]
                },
            )
        },
    )

    tables_html = _build_td_leads_tables_html(summary=summary)

    assert "21 Apr 2026 3:03:39 PM" in tables_html
    assert "21 Apr 2026 09:33:39 AM UTC" in tables_html


def test_td_leads_tables_html_hides_customer_cancelled_rows_but_keeps_counts() -> None:
    summary = LeadsRunSummary(
        run_id="run-cancel-policy",
        run_env="local",
        report_date=datetime(2026, 4, 22, tzinfo=timezone.utc).date(),
        store_results={
            "A817": StoreLeadResult(
                store_code="A817",
                rows=[
                    {
                        "status_bucket": "cancelled",
                        "pickup_no": "A817-CC",
                        "customer_name": "Customer Cancelled",
                        "mobile": "9888888888",
                        "reason": "",
                        "pickup_date": "22 Apr 2026 09:00:00 AM",
                    },
                    {
                        "status_bucket": "cancelled",
                        "pickup_no": "A817-SC",
                        "customer_name": "Store Cancelled",
                        "mobile": "9777777777",
                        "reason": "No inventory",
                        "pickup_date": "22 Apr 2026 08:00:00 AM",
                    },
                ],
                status_transitions=[
                    {"pickup_no": "A817-CC", "from_status_bucket": "pending", "to_status_bucket": "cancelled"},
                    {"pickup_no": "A817-SC", "from_status_bucket": "pending", "to_status_bucket": "cancelled"},
                ],
            )
        },
    )

    tables_html = _build_td_leads_tables_html(summary=summary)

    assert "Leads Marked as Cancelled (2 transitions this run)" in tables_html
    assert "Store Cancelled" in tables_html
    assert "No inventory" in tables_html
    assert "Customer Cancelled" not in tables_html


def test_td_leads_tables_html_does_not_mark_unchanged_cancelled_leads_as_new_transitions() -> None:
    summary = LeadsRunSummary(
        run_id="run-cancelled-unchanged",
        run_env="local",
        report_date=datetime(2026, 4, 22, tzinfo=timezone.utc).date(),
        store_results={
            "A817": StoreLeadResult(
                store_code="A817",
                rows=[
                    {
                        "status_bucket": "cancelled",
                        "pickup_no": "A817-CX1",
                        "customer_name": "Existing Cancelled",
                        "mobile": "9666666666",
                        "reason": "No inventory",
                    }
                ],
                status_transitions=[],
                lead_change_details={"transitions": []},
            )
        },
    )

    tables_html = _build_td_leads_tables_html(summary=summary)

    assert "No new leads/status changed across all stores." in tables_html
    assert "Existing Cancelled" not in tables_html


def test_td_leads_tables_html_marks_only_new_cancelled_transitions() -> None:
    summary = LeadsRunSummary(
        run_id="run-cancelled-new-transitions",
        run_env="local",
        report_date=datetime(2026, 4, 22, tzinfo=timezone.utc).date(),
        store_results={
            "A817": StoreLeadResult(
                store_code="A817",
                rows=[
                    {
                        "status_bucket": "cancelled",
                        "pickup_no": "A817-CN1",
                        "customer_name": "Store Transition",
                        "mobile": "9555555555",
                        "reason": "No rider available",
                        "source": "App",
                    },
                    {
                        "status_bucket": "cancelled",
                        "pickup_no": "A817-CN2",
                        "customer_name": "Customer Transition",
                        "mobile": "9444444444",
                        "reason": "",
                        "source": "Call Center",
                    },
                ],
                lead_change_details={
                    "transitions": [
                        {
                            "to_status_bucket": "cancelled",
                            "rows": [
                                {
                                    "lead_identity": {"pickup_no": "A817-CN1"},
                                    "customer_name": "Store Transition",
                                    "mobile": "9555555555",
                                },
                                {
                                    "lead_identity": {"pickup_no": "A817-CN2"},
                                    "customer_name": "Customer Transition",
                                    "mobile": "9444444444",
                                },
                            ],
                        }
                    ]
                },
            )
        },
    )

    tables_html = _build_td_leads_tables_html(summary=summary)

    assert "Leads Marked as Cancelled (2 transitions this run)" in tables_html
    assert "Store Transition" in tables_html
    assert "No rider available" in tables_html
    assert "App" in tables_html
    assert "Customer Transition" not in tables_html


def test_td_leads_tables_html_unchanged_snapshot_run_has_no_cancelled_entries() -> None:
    summary = LeadsRunSummary(
        run_id="run-cancelled-unchanged-snapshot",
        run_env="local",
        report_date=datetime(2026, 4, 22, tzinfo=timezone.utc).date(),
        store_results={
            "A817": StoreLeadResult(
                store_code="A817",
                rows=[
                    {
                        "status_bucket": "cancelled",
                        "pickup_no": "A817-K1",
                        "customer_name": "Already Cancelled",
                        "mobile": "9333333333",
                        "reason": "No inventory",
                        "source": "Dashboard",
                    }
                ],
                status_transitions=[],
                lead_change_details={"transitions": []},
            )
        },
    )

    tables_html = _build_td_leads_tables_html(summary=summary)

    assert "No new leads/status changed across all stores." in tables_html
    assert "Leads Marked as Cancelled" not in tables_html
    assert "Already Cancelled" not in tables_html


def test_td_leads_tables_html_pending_to_cancelled_transition_is_listed_once() -> None:
    summary = LeadsRunSummary(
        run_id="run-cancelled-deduped-once",
        run_env="local",
        report_date=datetime(2026, 4, 22, tzinfo=timezone.utc).date(),
        store_results={
            "A817": StoreLeadResult(
                store_code="A817",
                rows=[
                    {
                        "status_bucket": "cancelled",
                        "pickup_no": "A817-K2",
                        "customer_name": "Resolved From Current Row",
                        "mobile": "9222222222",
                        "reason": "Inventory delayed",
                        "source": "App",
                    }
                ],
                status_transitions=[
                    {
                        "lead_uid": "uid-a817-k2",
                        "pickup_no": "A817-K2",
                        "from_status_bucket": "pending",
                        "to_status_bucket": "cancelled",
                        "customer_name": "Stale Transition Name",
                        "mobile": "9000000000",
                    }
                ],
                lead_change_details={
                    "transitions": [
                        {
                            "to_status_bucket": "cancelled",
                            "rows": [
                                {
                                    "lead_uid": "uid-a817-k2",
                                    "pickup_no": "A817-K2",
                                    "from_status_bucket": "pending",
                                    "customer_name": "Another Stale Name",
                                }
                            ],
                        }
                    ]
                },
            )
        },
    )

    tables_html = _build_td_leads_tables_html(summary=summary)

    assert "Leads Marked as Cancelled (1 transitions this run)" in tables_html
    assert tables_html.count("Resolved From Current Row") == 1
    assert "Stale Transition Name" not in tables_html
    assert "Inventory delayed" in tables_html
    assert "App" in tables_html


def test_is_customer_cancelled_td_lead_uses_helper_consistent_resolution() -> None:
    assert td_leads_main._is_customer_cancelled_td_lead({"reason": ""}) is True
    assert td_leads_main._is_customer_cancelled_td_lead({"reason": None}) is True
    assert td_leads_main._is_customer_cancelled_td_lead({"reason": "No inventory"}) is False
    assert td_leads_main._is_customer_cancelled_td_lead({"cancelled_flag": "customer", "reason": "No inventory"}) is True
    assert td_leads_main._is_customer_cancelled_td_lead({"cancelled_flag": "store", "reason": ""}) is False


def test_td_leads_tables_html_renders_compact_run_message_for_unchanged_empty_store() -> None:
    summary = LeadsRunSummary(
        run_id="run-empty-sections",
        run_env="local",
        report_date=datetime(2026, 4, 22, tzinfo=timezone.utc).date(),
        store_results={"A817": StoreLeadResult(store_code="A817", rows=[])},
    )

    tables_html = _build_td_leads_tables_html(summary=summary)

    assert "No new leads/status changed across all stores." in tables_html
    assert "<table" not in tables_html


def test_td_leads_tables_html_shows_compact_store_message_when_no_changes() -> None:
    summary = LeadsRunSummary(
        run_id="run-no-store-changes",
        run_env="local",
        report_date=datetime(2026, 4, 22, tzinfo=timezone.utc).date(),
        store_results={
            "A817": StoreLeadResult(
                store_code="A817",
                rows=[
                    {
                        "status_bucket": "pending",
                        "customer_name": "Existing Pending",
                        "pickup_no": "A817-1",
                    }
                ],
                lead_change_details={"created_by_bucket": [], "transitions": []},
                status_transitions=[],
            )
        },
    )

    tables_html = _build_td_leads_tables_html(summary=summary)

    assert "Store A817" not in tables_html
    assert "No new leads/status changed across all stores." in tables_html
    assert "<table" not in tables_html


def test_td_leads_tables_html_mixes_changed_and_unchanged_store_sections() -> None:
    summary = LeadsRunSummary(
        run_id="run-mixed-changes",
        run_env="local",
        report_date=datetime(2026, 4, 22, tzinfo=timezone.utc).date(),
        store_results={
            "A817": StoreLeadResult(
                store_code="A817",
                rows=[{"status_bucket": "pending", "customer_name": "Legacy", "pickup_no": "A817-1"}],
                lead_change_details={"created_by_bucket": [], "transitions": []},
                status_transitions=[],
            ),
            "A668": StoreLeadResult(
                store_code="A668",
                rows=[
                    {
                        "status_bucket": "pending",
                        "customer_name": "New Lead",
                        "mobile": "9000000000",
                        "pickup_no": "A668-1",
                    }
                ],
                lead_change_details={
                    "created_by_bucket": [
                        {
                            "rows": [
                                {
                                    "customer_name": "New Lead",
                                    "mobile": "9000000000",
                                    "lead_identity": {"pickup_no": "A668-1"},
                                }
                            ]
                        }
                    ],
                    "transitions": [],
                },
                status_transitions=[],
            ),
        },
    )

    tables_html = _build_td_leads_tables_html(summary=summary)

    assert "Lead details by store" in tables_html
    assert "Store A817" in tables_html
    assert "Store A668" in tables_html
    assert "No new leads/status changed." in tables_html
    assert "New Leads created (1)" in tables_html


def test_write_store_artifact_fails_when_tz_aware_values_remain(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    events: list[dict[str, object]] = []
    monkeypatch.setattr(
        "app.crm_downloader.td_leads_sync.main.log_event",
        lambda **kwargs: events.append(kwargs),
    )
    rows = [
        {
            "store_code": "A668",
            "status_bucket": "pending",
            "pickup_id": "1",
            "pickup_no": "A668-1",
            "customer_name": "Foo",
            "address": "Bar",
            "mobile": datetime(2026, 4, 22, 6, 30, tzinfo=timezone.utc),
            "pickup_date": "22 Apr 2026",
            "pickup_time": "11:00 AM - 1:00 PM",
            "special_instruction": "",
            "status_text": "PENDING",
            "reason": "",
            "source": "",
            "user": "",
            "scraped_at": datetime(2026, 4, 22, 6, 30, tzinfo=timezone.utc),
        }
    ]

    with pytest.raises(ValueError, match="timezone-aware datetime values in columns: mobile"):
        _write_store_artifact(
            store_code="A668",
            rows=rows,
            output_dir=tmp_path,
            logger=SimpleNamespace(),
        )

    assert events
    assert events[-1]["tz_aware_columns"] == ["mobile"]


def test_write_store_artifact_writes_temp_then_promotes(tmp_path) -> None:
    rows = [
        {
            "store_code": "A668",
            "status_bucket": "pending",
            "pickup_id": "1",
            "pickup_no": "A668-1",
            "customer_name": "Foo",
            "address": "Bar",
            "mobile": "9999999999",
            "pickup_date": "22 Apr 2026",
            "pickup_time": "11:00 AM - 1:00 PM",
            "special_instruction": "",
            "status_text": "PENDING",
            "reason": "",
            "source": "",
            "user": "",
            "scraped_at": datetime(2026, 4, 22, 6, 30),
        }
    ]

    output_path = _write_store_artifact(
        store_code="A668",
        rows=rows,
        output_dir=tmp_path,
        logger=SimpleNamespace(),
    )

    assert output_path == tmp_path / "A668-crm_leads.xlsx"
    assert output_path.exists()
    assert not (tmp_path / "A668-crm_leads.xlsx.tmp").exists()


def test_write_store_artifact_removes_temp_and_logs_failure(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    events: list[dict[str, object]] = []
    monkeypatch.setattr(
        "app.crm_downloader.td_leads_sync.main.log_event",
        lambda **kwargs: events.append(kwargs),
    )

    class _FakeSheet:
        def __init__(self) -> None:
            self.title = ""

        def append(self, _row) -> None:
            return None

    class _FailingWorkbook:
        def __init__(self) -> None:
            self.active = _FakeSheet()

        def save(self, _path) -> None:
            raise OSError("disk full")

        def close(self) -> None:
            return None

    monkeypatch.setattr(
        "app.crm_downloader.td_leads_sync.main.openpyxl.Workbook",
        lambda: _FailingWorkbook(),
    )

    with pytest.raises(OSError, match="disk full"):
        _write_store_artifact(
            store_code="A668",
            rows=[],
            output_dir=tmp_path,
            logger=SimpleNamespace(),
        )

    tmp_artifact = tmp_path / "A668-crm_leads.xlsx.tmp"
    assert not tmp_artifact.exists()
    assert events
    assert events[-1]["message"] == "artifact_write_failed"
    assert events[-1]["store_code"] == "A668"
    assert events[-1]["artifact_path"] == str(tmp_artifact)


class _FakeLocator:
    def __init__(self, count: int) -> None:
        self._count = count

    async def count(self) -> int:
        return self._count


class _FakeNavigationContext:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakePage:
    def __init__(self, *, selectors_present: set[str], url: str = "https://subs.quickdrycleaning.com/a668/App/home") -> None:
        self.selectors_present = selectors_present
        self.url = url
        self.waited_selectors: list[str] = []
        self.clicked: list[str] = []
        self.goto_urls: list[str] = []
        self.waited_url_patterns: list[object] = []
        self.expect_navigation_calls = 0
        self.title_text = "Pickup Scheduler"
        self.fail_ready = False

    async def wait_for_selector(self, selector: str, timeout: int | None = None) -> None:
        self.waited_selectors.append(selector)
        if self.fail_ready and selector in {"#drpStatus", "#grdEntry", "#grdCompleted", "#grdCanceled"}:
            raise TimeoutError("status selector timeout")

    def locator(self, selector: str) -> _FakeLocator:
        return _FakeLocator(1 if selector in self.selectors_present else 0)

    def expect_navigation(self, **kwargs) -> _FakeNavigationContext:
        self.expect_navigation_calls += 1
        return _FakeNavigationContext()

    async def click(self, selector: str) -> None:
        self.clicked.append(selector)
        self.url = "https://subs.quickdrycleaning.com/a668/App/New_Admin/frmHomePickUpScheduler.aspx"

    async def goto(self, url: str, **kwargs) -> None:
        self.goto_urls.append(url)
        self.url = "https://subs.quickdrycleaning.com/a668/App/New_Admin/frmHomePickUpScheduler.aspx"

    async def wait_for_url(self, pattern, **kwargs) -> None:
        self.waited_url_patterns.append(pattern)

    async def title(self) -> str:
        return self.title_text


@pytest.mark.asyncio
async def test_ensure_scheduler_page_prefers_pickup_alert_click(monkeypatch: pytest.MonkeyPatch) -> None:
    page = _FakePage(selectors_present={"#achrPickUp"})
    events: list[dict[str, object]] = []
    monkeypatch.setattr(
        "app.crm_downloader.td_leads_sync.main.log_event",
        lambda **kwargs: events.append(kwargs),
    )

    ok = await _ensure_scheduler_page(page, store=SimpleNamespace(store_code="A668"), logger=SimpleNamespace())

    assert ok is True
    assert page.clicked == ["#achrPickUp"]
    assert not page.goto_urls
    assert page.expect_navigation_calls == 1
    assert any(event.get("navigation_branch") == "home_alert_click" for event in events)


@pytest.mark.asyncio
async def test_ensure_scheduler_page_uses_fallback_click_when_alert_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    fallback = "a[href*='frmHomePickUpScheduler.aspx']"
    page = _FakePage(selectors_present={fallback})
    events: list[dict[str, object]] = []
    monkeypatch.setattr(
        "app.crm_downloader.td_leads_sync.main.log_event",
        lambda **kwargs: events.append(kwargs),
    )

    ok = await _ensure_scheduler_page(page, store=SimpleNamespace(store_code="A668"), logger=SimpleNamespace())

    assert ok is True
    assert page.clicked == [fallback]
    assert not page.goto_urls
    assert any(str(event.get("navigation_branch", "")).startswith("fallback_click:") for event in events)


@pytest.mark.asyncio
async def test_ensure_scheduler_page_timeout_logs_selector_and_final_url(monkeypatch: pytest.MonkeyPatch) -> None:
    page = _FakePage(selectors_present={"#achrPickUp"})
    page.fail_ready = True
    events: list[dict[str, object]] = []
    monkeypatch.setattr(
        "app.crm_downloader.td_leads_sync.main.log_event",
        lambda **kwargs: events.append(kwargs),
    )

    ok = await _ensure_scheduler_page(page, store=SimpleNamespace(store_code="A668"), logger=SimpleNamespace())

    assert ok is False
    error_events = [event for event in events if event.get("status") == "error"]
    assert error_events
    error_event = error_events[-1]
    assert error_event.get("awaited_selectors")
    assert str(error_event.get("final_url", "")).endswith("frmHomePickUpScheduler.aspx")


class _FakeEvaluatePage:
    def __init__(self, evaluate_result):
        self.evaluate_result = evaluate_result
        self.evaluate_calls: list[tuple[str, dict[str, str]]] = []

    async def evaluate(self, script: str, payload: dict[str, str]):
        self.evaluate_calls.append((script, payload))
        return self.evaluate_result


class _FakeLogger:
    def info(self, **kwargs) -> None:
        return None


@pytest.mark.asyncio
async def test_available_pager_args_uses_raw_regex_pattern_in_evaluate_script() -> None:
    page = _FakeEvaluatePage(["Page$1", "Page$2"])

    values = await _available_pager_args(page, grid_selector="#grdEntry")

    assert values == ["Page$1", "Page$2"]
    script, payload = page.evaluate_calls[0]
    assert payload == {"gridSelector": "#grdEntry"}
    assert r"href.match(/Page\$\d+/i)" in script


@pytest.mark.asyncio
async def test_postback_page_arg_uses_event_argument_payload() -> None:
    page = _FakeEvaluatePage(None)

    await _postback_page_arg(page, arg="Page$3")

    script, payload = page.evaluate_calls[0]
    assert payload == {"eventArgument": "Page$3"}
    assert "__EVENTARGUMENT" in script
    assert "__doPostBack('', eventArgument)" in script


@pytest.mark.asyncio
async def test_scrape_grid_rows_script_keeps_whitespace_regex_literal() -> None:
    page = _FakeEvaluatePage({"headers": [], "rows": []})

    headers, rows = await _scrape_grid_rows(page, grid_selector="#grdEntry")

    assert headers == []
    assert rows == []
    script, _payload = page.evaluate_calls[0]
    assert r"replace(/\s+/g, ' ')" in script


@pytest.mark.asyncio
async def test_collect_status_rows_maps_combined_created_datetime_to_pickup_created_at(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _noop_switch_status(*args, **kwargs) -> None:
        return None

    async def _fake_scrape_grid_rows(*args, **kwargs):
        return (
            ["Pickup No.", "Customer Name", "Created Date/Time", "Mobile", "Status"],
            [
                {
                    "pickup_id": "4434944",
                    "values": ["A668-3025", "Moni", "21 Apr 2026 3:03:39 PM", "9599242207", "PENDING"],
                }
            ],
        )

    async def _no_pages(*args, **kwargs):
        return []

    monkeypatch.setattr("app.crm_downloader.td_leads_sync.main._switch_status", _noop_switch_status)
    monkeypatch.setattr("app.crm_downloader.td_leads_sync.main._scrape_grid_rows", _fake_scrape_grid_rows)
    monkeypatch.setattr("app.crm_downloader.td_leads_sync.main._available_pager_args", _no_pages)

    rows, warnings = await _collect_status_rows(
        page=SimpleNamespace(),
        store_code="A668",
        status_bucket="pending",
        status_value="1",
        grid_selector="#grdEntry",
        logger=_FakeLogger(),
    )

    assert warnings == []
    assert rows[0]["pickup_date"] is None
    assert rows[0]["pickup_time"] is None
    assert rows[0]["pickup_created_text"] == "21 Apr 2026 3:03:39 PM"
    assert rows[0]["pickup_created_at"] == datetime(2026, 4, 21, 9, 33, 39, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_collect_status_rows_keeps_pickup_date_when_pickup_date_column_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _noop_switch_status(*args, **kwargs) -> None:
        return None

    async def _fake_scrape_grid_rows(*args, **kwargs):
        return (
            ["Pickup No.", "Pickup Date", "Created Date/Time", "Mobile", "Status"],
            [
                {
                    "pickup_id": "4434944",
                    "values": ["A668-3025", "22 Apr 2026", "21 Apr 2026 3:03:39 PM", "9599242207", "PENDING"],
                }
            ],
        )

    async def _no_pages(*args, **kwargs):
        return []

    monkeypatch.setattr("app.crm_downloader.td_leads_sync.main._switch_status", _noop_switch_status)
    monkeypatch.setattr("app.crm_downloader.td_leads_sync.main._scrape_grid_rows", _fake_scrape_grid_rows)
    monkeypatch.setattr("app.crm_downloader.td_leads_sync.main._available_pager_args", _no_pages)

    rows, warnings = await _collect_status_rows(
        page=SimpleNamespace(),
        store_code="A668",
        status_bucket="pending",
        status_value="1",
        grid_selector="#grdEntry",
        logger=_FakeLogger(),
    )

    assert warnings == []
    assert rows[0]["pickup_date"] == "22 Apr 2026"
    assert rows[0]["pickup_created_text"] == "21 Apr 2026 3:03:39 PM"
    assert rows[0]["pickup_created_at"] == datetime(2026, 4, 21, 9, 33, 39, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_ingest_store_path_uses_async_session_scope_without_greenlet_errors(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'td_leads.db'}"

    result = await td_leads_ingest.ingest_td_crm_leads_rows(
        rows=[
            {
                "store_code": "A668",
                "status_bucket": "pending",
                "pickup_id": "4434944",
                "pickup_no": "A668-3025",
                "mobile": "9599242207",
                "pickup_date": "22 Apr 2026",
                "pickup_time": "11:00 AM - 1:00 PM",
            }
        ],
        run_id="run-1",
        run_env="test",
        source_file="A668-crm_leads.xlsx",
        database_url=database_url,
    )

    assert result.rows_received == 1
    assert result.rows_upserted == 1

    engine = create_async_engine(database_url, future=True)
    try:
        async with engine.connect() as connection:
            count = await connection.scalar(sa.text("SELECT COUNT(*) FROM crm_leads_current"))
        assert count == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_combined_created_datetime_populates_ingest_payload_and_email_row_context(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'td_leads_created_datetime.db'}"
    row = {
        "store_code": "A668",
        "status_bucket": "pending",
        "pickup_id": "4434944",
        "pickup_no": "A668-3025",
        "customer_name": "Moni",
        "mobile": "9599242207",
        "pickup_date": "21 Apr 2026",
        "pickup_created_date": "21 Apr 2026 3:03:39 PM",
        "pickup_created_at": datetime(2026, 4, 21, 9, 33, 39, tzinfo=timezone.utc),
        "pickup_time": None,
    }

    ingest_result = await td_leads_ingest.ingest_td_crm_leads_rows(
        rows=[row],
        run_id="run-created-dt",
        run_env="test",
        source_file="A668-crm_leads.xlsx",
        database_url=database_url,
    )

    assert ingest_result.rows_upserted == 1

    engine = create_async_engine(database_url, future=True)
    try:
        async with engine.connect() as connection:
            stored = (
                await connection.execute(
                    sa.text(
                        """
                        SELECT pickup_date, pickup_created_at, pickup_time
                        FROM crm_leads_current
                        WHERE pickup_no = :pickup_no
                        """
                    ),
                    {"pickup_no": "A668-3025"},
                )
            ).mappings().one()
        assert stored["pickup_date"] == "21 Apr 2026"
        assert stored["pickup_created_at"] is not None
        assert stored["pickup_time"] is None
    finally:
        await engine.dispose()

    summary = LeadsRunSummary(
        run_id="run-created-dt",
        run_env="test",
        report_date=datetime(2026, 4, 22, tzinfo=timezone.utc).date(),
        store_results={
            "A668": StoreLeadResult(
                store_code="A668",
                rows=[
                    {
                        **row,
                        "pickup_created_text": row["pickup_created_date"],
                        "pickup_time": None,
                    }
                ],
                lead_change_details={
                    "created_by_bucket": [
                        {"rows": [{"lead_identity": {"pickup_no": "A668-3025"}}]}
                    ]
                },
            )
        },
    )
    lead_tables_html = _build_td_leads_tables_html(summary=summary)
    assert "21 Apr 2026 3:03:39 PM" in lead_tables_html
    assert "None" in lead_tables_html


@pytest.mark.asyncio
async def test_ingest_populates_pickup_created_at_for_all_status_buckets(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'td_leads_created_at_all_buckets.db'}"
    created_text = "21 Apr 2026 3:03:39 PM"
    rows = [
        {
            "store_code": "A668",
            "status_bucket": bucket,
            "pickup_id": f"4434944-{bucket}",
            "pickup_no": f"A668-3025-{bucket}",
            "customer_name": "Moni",
            "mobile": "9599242207",
            "pickup_created_date": created_text,
            "pickup_time": "11:00 AM - 1:00 PM",
        }
        for bucket in ("pending", "completed", "cancelled")
    ]

    ingest_result = await td_leads_ingest.ingest_td_crm_leads_rows(
        rows=rows,
        run_id="run-created-at-all",
        run_env="test",
        source_file="A668-crm_leads.xlsx",
        database_url=database_url,
    )

    assert ingest_result.rows_upserted == 3

    engine = create_async_engine(database_url, future=True)
    try:
        async with engine.connect() as connection:
            stored_rows = (
                await connection.execute(
                    sa.text(
                        """
                        SELECT status_bucket, pickup_created_at
                        FROM crm_leads_current
                        ORDER BY status_bucket
                        """
                    )
                )
            ).mappings().all()
    finally:
        await engine.dispose()

    assert [row["status_bucket"] for row in stored_rows] == ["cancelled", "completed", "pending"]
    assert all(row["pickup_created_at"] is not None for row in stored_rows)


@pytest.mark.asyncio
async def test_ingest_upsert_preserves_existing_pickup_created_at_when_new_value_is_unparseable(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'td_leads_pickup_created_at_upsert.db'}"
    base_row = {
        "store_code": "A668",
        "status_bucket": "pending",
        "pickup_id": "4434944",
        "pickup_no": "A668-3025",
        "customer_name": "Moni",
        "mobile": "9599242207",
        "pickup_time": "11:00 AM - 1:00 PM",
    }
    first_row = {
        **base_row,
        "pickup_date": "21 Apr 2026",
        "pickup_created_date": "21 Apr 2026",
        "pickup_created_at": "21 Apr 2026 3:03:39 PM",
    }
    second_row = {
        **base_row,
        "pickup_date": "21 Apr 2026",
        "pickup_created_date": "21 Apr 2026",
        "pickup_created_at": "not-a-date",
    }

    await td_leads_ingest.ingest_td_crm_leads_rows(
        rows=[first_row],
        run_id="run-pickup-created-at-initial",
        run_env="test",
        source_file="A668-crm_leads.xlsx",
        database_url=database_url,
    )

    await td_leads_ingest.ingest_td_crm_leads_rows(
        rows=[second_row],
        run_id="run-pickup-created-at-upsert",
        run_env="test",
        source_file="A668-crm_leads.xlsx",
        database_url=database_url,
    )

    engine = create_async_engine(database_url, future=True)
    try:
        async with engine.connect() as connection:
            stored = (
                await connection.execute(
                    sa.text(
                        """
                        SELECT pickup_date, pickup_created_at
                        FROM crm_leads_current
                        WHERE lead_uid = :lead_uid
                        """
                    ),
                        {"lead_uid": build_lead_uid(first_row)},
                )
            ).mappings().one()
    finally:
        await engine.dispose()

    assert stored["pickup_date"] == "21 Apr 2026"
    preserved_pickup_created_at = stored["pickup_created_at"]
    assert preserved_pickup_created_at is not None
    if isinstance(preserved_pickup_created_at, str):
        assert preserved_pickup_created_at.startswith("2026-04-21 09:33:39")
    else:
        assert preserved_pickup_created_at == datetime(2026, 4, 21, 9, 33, 39)


@pytest.mark.asyncio
async def test_ingest_captures_created_updated_counts_and_status_transitions(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'td_leads_transition_metrics.db'}"
    base_row = {
        "store_code": "A668",
        "pickup_id": "4434944",
        "pickup_no": "A668-3025",
        "customer_name": "Moni",
        "mobile": "9599242207",
        "pickup_created_date": "21 Apr 2026 3:03:39 PM",
        "pickup_time": "11:00 AM - 1:00 PM",
    }

    first_result = await td_leads_ingest.ingest_td_crm_leads_rows(
        rows=[{**base_row, "status_bucket": "pending"}],
        run_id="run-transition-1",
        run_env="test",
        source_file="A668-crm_leads.xlsx",
        database_url=database_url,
    )
    second_result = await td_leads_ingest.ingest_td_crm_leads_rows(
        rows=[{**base_row, "status_bucket": "completed"}],
        run_id="run-transition-2",
        run_env="test",
        source_file="A668-crm_leads.xlsx",
        database_url=database_url,
    )

    assert first_result.bucket_write_counts["pending"]["created"] == 1
    assert second_result.bucket_write_counts["completed"]["updated"] == 1
    assert len(second_result.status_transitions) == 1
    transition = second_result.status_transitions[0]
    assert transition["from_status_bucket"] == "pending"
    assert transition["to_status_bucket"] == "completed"
    assert second_result.task_stub["status"] == "open"
    transition_groups = second_result.lead_change_details["transitions"]
    assert transition_groups[0]["rows"][0]["customer_name"] == "Moni"
    assert transition_groups[0]["rows"][0]["previous_status_bucket"] == "pending"

    engine = create_async_engine(database_url, future=True)
    try:
        async with engine.connect() as connection:
            event_rows = (
                await connection.execute(
                    sa.text(
                        """
                        SELECT event_type, previous_status_bucket, status_bucket
                        FROM crm_leads_status_events
                        ORDER BY id
                        """
                    )
                )
            ).mappings().all()
    finally:
        await engine.dispose()

    assert [row["event_type"] for row in event_rows] == ["new_lead", "status_transition"]
    assert event_rows[1]["previous_status_bucket"] == "pending"
    assert event_rows[1]["status_bucket"] == "completed"


@pytest.mark.asyncio
async def test_ingest_first_run_emits_new_events_for_all_rows(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'td_leads_first_run_new.db'}"
    rows = [
        {"store_code": "A668", "status_bucket": "pending", "pickup_no": "A668-1", "pickup_date": "21 Apr 2026"},
        {"store_code": "A668", "status_bucket": "completed", "pickup_no": "A668-2", "pickup_date": "21 Apr 2026"},
    ]

    result = await td_leads_ingest.ingest_td_crm_leads_rows(
        rows=rows,
        run_id="run-first",
        run_env="test",
        source_file="A668-crm_leads.xlsx",
        database_url=database_url,
    )

    assert result.bucket_write_counts["pending"]["created"] == 1
    assert result.bucket_write_counts["completed"]["created"] == 1
    assert result.status_transitions == []

    engine = create_async_engine(database_url, future=True)
    try:
        async with engine.connect() as connection:
            event_count = await connection.scalar(sa.text("SELECT COUNT(*) FROM crm_leads_status_events"))
    finally:
        await engine.dispose()

    assert event_count == 2


@pytest.mark.asyncio
async def test_ingest_reopen_transition_records_event(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'td_leads_reopen.db'}"
    base_row = {"store_code": "A668", "pickup_no": "A668-3025", "pickup_date": "21 Apr 2026"}

    await td_leads_ingest.ingest_td_crm_leads_rows(
        rows=[{**base_row, "status_bucket": "pending"}],
        run_id="run-reopen-1",
        run_env="test",
        source_file="A668-crm_leads.xlsx",
        database_url=database_url,
    )
    await td_leads_ingest.ingest_td_crm_leads_rows(
        rows=[{**base_row, "status_bucket": "completed"}],
        run_id="run-reopen-2",
        run_env="test",
        source_file="A668-crm_leads.xlsx",
        database_url=database_url,
    )
    result = await td_leads_ingest.ingest_td_crm_leads_rows(
        rows=[{**base_row, "status_bucket": "pending"}],
        run_id="run-reopen-3",
        run_env="test",
        source_file="A668-crm_leads.xlsx",
        database_url=database_url,
    )

    assert len(result.status_transitions) == 1
    assert result.status_transitions[0]["from_status_bucket"] == "completed"
    assert result.status_transitions[0]["to_status_bucket"] == "pending"

    engine = create_async_engine(database_url, future=True)
    try:
        async with engine.connect() as connection:
            latest_event = (
                await connection.execute(
                    sa.text(
                        """
                        SELECT event_type, previous_status_bucket, status_bucket
                        FROM crm_leads_status_events
                        ORDER BY id DESC
                        LIMIT 1
                        """
                    )
                )
            ).mappings().one()
    finally:
        await engine.dispose()

    assert latest_event["event_type"] == "status_transition"
    assert latest_event["previous_status_bucket"] == "completed"
    assert latest_event["status_bucket"] == "pending"


@pytest.mark.asyncio
async def test_ingest_sets_cancelled_flag_from_reason(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'td_leads_cancelled_flag.db'}"
    rows = [
        {"store_code": "A668", "status_bucket": "cancelled", "pickup_no": "A668-10", "reason": "", "pickup_date": "22 Apr 2026"},
        {"store_code": "A668", "status_bucket": "cancelled", "pickup_no": "A668-11", "reason": "Customer unavailable", "pickup_date": "22 Apr 2026"},
        {"store_code": "A668", "status_bucket": "pending", "pickup_no": "A668-12", "reason": "", "pickup_date": "22 Apr 2026"},
    ]

    await td_leads_ingest.ingest_td_crm_leads_rows(
        rows=rows,
        run_id="run-cancelled-flag",
        run_env="test",
        source_file="A668-crm_leads.xlsx",
        database_url=database_url,
    )

    engine = create_async_engine(database_url, future=True)
    try:
        async with engine.connect() as connection:
            persisted = (
                await connection.execute(
                    sa.text(
                        """
                        SELECT pickup_no, cancelled_flag
                        FROM crm_leads_current
                        ORDER BY pickup_no
                        """
                    )
                )
            ).mappings().all()
    finally:
        await engine.dispose()

    assert persisted == [
        {"pickup_no": "A668-10", "cancelled_flag": "customer"},
        {"pickup_no": "A668-11", "cancelled_flag": "store"},
        {"pickup_no": "A668-12", "cancelled_flag": None},
    ]


@pytest.mark.asyncio
async def test_ingest_lead_change_details_dedupes_and_caps_rows(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'td_leads_dedup_cap.db'}"
    rows = []
    for index in range(25):
        rows.append(
            {
                "store_code": "A668",
                "status_bucket": "pending",
                "pickup_id": f"44349{index}",
                "pickup_no": f"A668-{index}",
                "customer_name": f"Lead {index}",
                "mobile": "9599242207",
                "pickup_created_date": "21 Apr 2026 3:03:39 PM",
            }
        )
    rows.append(dict(rows[0]))

    result = await td_leads_ingest.ingest_td_crm_leads_rows(
        rows=rows,
        run_id="run-dedupe-cap",
        run_env="test",
        source_file="A668-crm_leads.xlsx",
        database_url=database_url,
    )

    created_groups = result.lead_change_details["created_by_bucket"]
    assert created_groups[0]["status_bucket"] == "pending"
    assert len(created_groups[0]["rows"]) == 25
    assert created_groups[0]["overflow_count"] == 0


@pytest.mark.asyncio
async def test_td_leads_seeded_run_notification_plans_email(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'td_leads_notif.db'}"
    engine = create_async_engine(database_url, future=True)

    try:
        async with engine.begin() as connection:
            await connection.execute(
                sa.text(
                    """
                    CREATE TABLE pipelines (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        code TEXT NOT NULL,
                        description TEXT
                    )
                    """
                )
            )
            await connection.execute(
                sa.text(
                    """
                    CREATE TABLE pipeline_run_summaries (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        pipeline_name TEXT,
                        run_id TEXT,
                        run_env TEXT,
                        started_at DATETIME,
                        finished_at DATETIME,
                        total_time_taken TEXT,
                        report_date DATE,
                        overall_status TEXT,
                        summary_text TEXT,
                        phases_json JSON,
                        metrics_json JSON,
                        created_at DATETIME
                    )
                    """
                )
            )
            await connection.execute(
                sa.text(
                    """
                    CREATE TABLE documents (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        doc_type TEXT,
                        doc_subtype TEXT,
                        doc_date DATE,
                        reference_name_1 TEXT,
                        reference_id_1 TEXT,
                        reference_name_2 TEXT,
                        reference_id_2 TEXT,
                        reference_name_3 TEXT,
                        reference_id_3 TEXT,
                        file_name TEXT,
                        mime_type TEXT,
                        file_size_bytes INTEGER,
                        storage_backend TEXT,
                        file_path TEXT,
                        file_blob BLOB,
                        checksum TEXT,
                        status TEXT,
                        error_message TEXT,
                        created_at DATETIME,
                        created_by TEXT
                    )
                    """
                )
            )
            await connection.execute(
                sa.text(
                    """
                    CREATE TABLE notification_profiles (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        pipeline_id INTEGER,
                        code TEXT,
                        description TEXT,
                        env TEXT,
                        scope TEXT,
                        attach_mode TEXT,
                        is_active BOOLEAN
                    )
                    """
                )
            )
            await connection.execute(
                sa.text(
                    """
                    CREATE TABLE email_templates (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        profile_id INTEGER,
                        name TEXT,
                        subject_template TEXT,
                        body_template TEXT,
                        is_active BOOLEAN
                    )
                    """
                )
            )
            await connection.execute(
                sa.text(
                    """
                    CREATE TABLE notification_recipients (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        profile_id INTEGER,
                        store_code TEXT,
                        env TEXT,
                        email_address TEXT,
                        display_name TEXT,
                        send_as TEXT,
                        is_active BOOLEAN,
                        created_at DATETIME
                    )
                    """
                )
            )
            await connection.execute(
                sa.text("INSERT INTO pipelines (id, code, description) VALUES (1, 'td_crm_leads_sync', 'TD leads')")
            )
            await connection.execute(
                sa.text(
                    """
                    INSERT INTO pipeline_run_summaries (
                        pipeline_name, run_id, run_env, started_at, finished_at, report_date, overall_status,
                        total_time_taken, summary_text, metrics_json
                    ) VALUES (
                        'td_crm_leads_sync', 'run-1', 'local', '2026-04-22T00:00:00+00:00', '2026-04-22T00:01:00+00:00',
                        '2026-04-22', 'success', '00:01:00', 'ok',
                        :metrics_json
                    )
                    """
                ),
                {
                    "metrics_json": json.dumps({"summary_html": "<div>ok</div>"}),
                },
            )
            await connection.execute(
                sa.text(
                    """
                    INSERT INTO notification_profiles (
                        id, pipeline_id, code, description, env, scope, attach_mode, is_active
                    ) VALUES (
                        10, 1, 'run_summary', 'TD leads run summary', 'any', 'run', 'none', 1
                    )
                    """
                )
            )
            await connection.execute(
                sa.text(
                    """
                    INSERT INTO email_templates (
                        profile_id, name, subject_template, body_template, is_active
                    ) VALUES (
                        10, 'run_summary', 'TD Leads {{ run_id }}', 'Run {{ run_id }} complete in {{ duration_human }}', 1
                    )
                    """
                )
            )
            await connection.execute(
                sa.text(
                    """
                    INSERT INTO notification_recipients (
                        profile_id, store_code, env, email_address, send_as, is_active
                    ) VALUES (
                        10, 'ALL', 'any', 'ops@example.com', 'to', 1
                    )
                    """
                )
            )

        monkeypatch.setattr(
            "app.dashboard_downloader.notifications.config",
            SimpleNamespace(
                database_url=database_url,
                report_email_smtp_host=app_config.report_email_smtp_host,
                report_email_smtp_port=app_config.report_email_smtp_port,
                report_email_from=app_config.report_email_from,
                report_email_smtp_username=app_config.report_email_smtp_username,
                report_email_smtp_password=app_config.report_email_smtp_password,
                report_email_use_tls=app_config.report_email_use_tls,
            ),
        )
        sent_plans = []

        def _capture_send_email(_smtp_config, plan):
            sent_plans.append(plan)
            return True

        monkeypatch.setattr("app.dashboard_downloader.notifications._send_email", _capture_send_email)

        result = await send_notifications_for_run("td_crm_leads_sync", "run-1")

        assert result["emails_planned"] == 1
        assert result["emails_sent"] == 1
        assert len(sent_plans) == 1
        assert "00:01:00" in sent_plans[0].body
        assert "Run run-1 complete in 00:01:00" in sent_plans[0].body
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    ("env", "expected"),
    [
        ({}, (2, 2, True)),
        ({"TD_LEADS_MAX_WORKERS": "0"}, (1, 1, False)),
        ({"TD_LEADS_MAX_WORKERS": "5", "TD_LEADS_PARALLEL_ENABLED": "0"}, (5, 1, False)),
        ({"TD_LEADS_MAX_WORKERS": "4", "TD_LEADS_PARALLEL_ENABLED": "1"}, (4, 4, True)),
    ],
)
def test_resolve_td_leads_concurrency_settings(
    monkeypatch: pytest.MonkeyPatch, env: dict[str, str], expected: tuple[int, int, bool]
) -> None:
    monkeypatch.delenv("TD_LEADS_MAX_WORKERS", raising=False)
    monkeypatch.delenv("TD_LEADS_PARALLEL_ENABLED", raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    resolved = td_leads_main._resolve_td_leads_concurrency_settings()

    assert resolved == expected


@pytest.mark.asyncio
async def test_td_leads_main_parallel_worker_path_reduces_elapsed(monkeypatch: pytest.MonkeyPatch) -> None:
    stores = [SimpleNamespace(store_code="A"), SimpleNamespace(store_code="B"), SimpleNamespace(store_code="C")]
    persisted_summaries = []
    notifications: list[tuple[str, str]] = []

    class _FakePlaywrightContext:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _FakeBrowser:
        async def close(self) -> None:
            return None

    async def _fake_start_summary(*, logger, summary):
        return None

    async def _fake_persist_summary(*, logger, summary, finished_at):
        persisted_summaries.append(summary)
        return True

    async def _fake_notify(pipeline_name, run_id):
        notifications.append((pipeline_name, run_id))
        return {"emails_planned": 0, "emails_sent": 0, "errors": []}

    async def _fake_run_store(*, browser, store, run_id, run_env, logger):
        await asyncio.sleep(0.12)
        return td_leads_main.StoreLeadResult(store_code=store.store_code, status="ok", message="ok")

    monkeypatch.setattr(td_leads_main, "_load_td_order_stores", lambda logger, store_codes=None: asyncio.sleep(0, result=stores))
    monkeypatch.setattr(td_leads_main, "_start_run_summary", _fake_start_summary)
    monkeypatch.setattr(td_leads_main, "_persist_run_summary", _fake_persist_summary)
    monkeypatch.setattr(td_leads_main, "send_notifications_for_run", _fake_notify)
    monkeypatch.setattr(td_leads_main, "_resolve_td_leads_concurrency_settings", lambda: (2, 2, True))
    monkeypatch.setattr(td_leads_main, "_run_store", _fake_run_store)
    monkeypatch.setattr(td_leads_main, "async_playwright", lambda: _FakePlaywrightContext())
    monkeypatch.setattr(td_leads_main, "launch_browser", lambda playwright, logger: asyncio.sleep(0, result=_FakeBrowser()))

    started = time.perf_counter()
    await td_leads_main.main(run_env="test", run_id="run-parallel")
    elapsed = time.perf_counter() - started

    assert elapsed < 0.35
    assert len(persisted_summaries) == 1
    assert list(persisted_summaries[0].store_results.keys()) == ["A", "B", "C"]
    assert notifications == [("td_crm_leads_sync", "run-parallel")]


@pytest.mark.asyncio
async def test_td_leads_main_preserves_summary_store_order_when_workers_finish_out_of_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stores = [SimpleNamespace(store_code="S1"), SimpleNamespace(store_code="S2"), SimpleNamespace(store_code="S3")]
    persisted_summaries = []
    delays = {"S1": 0.1, "S2": 0.01, "S3": 0.05}

    class _FakePlaywrightContext:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _FakeBrowser:
        async def close(self) -> None:
            return None

    async def _fake_run_store(*, browser, store, run_id, run_env, logger):
        await asyncio.sleep(delays[store.store_code])
        return td_leads_main.StoreLeadResult(store_code=store.store_code, status="ok")

    monkeypatch.setattr(td_leads_main, "_load_td_order_stores", lambda logger, store_codes=None: asyncio.sleep(0, result=stores))
    monkeypatch.setattr(td_leads_main, "_start_run_summary", lambda logger, summary: asyncio.sleep(0))
    monkeypatch.setattr(
        td_leads_main,
        "_persist_run_summary",
        lambda logger, summary, finished_at: persisted_summaries.append(summary) or asyncio.sleep(0, result=False),
    )
    monkeypatch.setattr(td_leads_main, "_resolve_td_leads_concurrency_settings", lambda: (3, 3, True))
    monkeypatch.setattr(td_leads_main, "_run_store", _fake_run_store)
    monkeypatch.setattr(td_leads_main, "async_playwright", lambda: _FakePlaywrightContext())
    monkeypatch.setattr(td_leads_main, "launch_browser", lambda playwright, logger: asyncio.sleep(0, result=_FakeBrowser()))

    await td_leads_main.main(run_env="test", run_id="run-order")

    assert len(persisted_summaries) == 1
    assert list(persisted_summaries[0].store_results.keys()) == ["S1", "S2", "S3"]


@pytest.mark.asyncio
async def test_td_leads_main_normalizes_store_worker_exceptions_and_persists_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stores = [SimpleNamespace(store_code="A"), SimpleNamespace(store_code="B")]
    persisted_summaries = []
    notifications: list[tuple[str, str]] = []

    class _FakePlaywrightContext:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _FakeBrowser:
        async def close(self) -> None:
            return None

    async def _fake_run_store(*, browser, store, run_id, run_env, logger):
        if store.store_code == "B":
            raise RuntimeError("boom")
        return td_leads_main.StoreLeadResult(store_code=store.store_code, status="ok")

    async def _fake_notify(pipeline_name, run_id):
        notifications.append((pipeline_name, run_id))
        return {"emails_planned": 0, "emails_sent": 0, "errors": []}

    monkeypatch.setattr(td_leads_main, "_load_td_order_stores", lambda logger, store_codes=None: asyncio.sleep(0, result=stores))
    monkeypatch.setattr(td_leads_main, "_start_run_summary", lambda logger, summary: asyncio.sleep(0))
    monkeypatch.setattr(
        td_leads_main,
        "_persist_run_summary",
        lambda logger, summary, finished_at: persisted_summaries.append(summary) or asyncio.sleep(0, result=True),
    )
    monkeypatch.setattr(td_leads_main, "_resolve_td_leads_concurrency_settings", lambda: (2, 2, True))
    monkeypatch.setattr(td_leads_main, "_run_store", _fake_run_store)
    monkeypatch.setattr(td_leads_main, "send_notifications_for_run", _fake_notify)
    monkeypatch.setattr(td_leads_main, "async_playwright", lambda: _FakePlaywrightContext())
    monkeypatch.setattr(td_leads_main, "launch_browser", lambda playwright, logger: asyncio.sleep(0, result=_FakeBrowser()))

    await td_leads_main.main(run_env="test", run_id="run-errors")

    assert len(persisted_summaries) == 1
    assert persisted_summaries[0].store_results["A"].status == "ok"
    assert persisted_summaries[0].store_results["B"].status == "error"
    assert "failed" in persisted_summaries[0].store_results["B"].message.lower()
    assert notifications == [("td_crm_leads_sync", "run-errors")]
