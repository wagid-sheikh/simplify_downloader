from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
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
    _business_day_bounds_utc,
    _calculate_lead_age_days,
    _build_td_cancelled_lead_payload,
    _build_td_new_lead_payload,
    _collect_status_rows,
    _build_td_leads_summary_html,
    _build_td_leads_tables_html,
    _available_pager_args,
    _ensure_scheduler_page,
    _field_from_headers,
    _find_tz_aware_columns,
    _build_td_mobile_match_debug_diagnostics,
    _normalize_mobile_number,
    _postback_page_arg,
    _sanitize_rows_for_xlsx_export,
    _scrape_grid_rows,
    _write_store_artifact,
)
from app.dashboard_downloader.notifications import send_notifications_for_run
from app.config import config as app_config


@pytest.mark.parametrize(
    ("raw_mobile", "expected"),
    [
        pytest.param("9599242207", "9599242207", id="td-crm-plain-10-digit"),
        pytest.param(" 9599242207 ", "9599242207", id="td-crm-leading-trailing-whitespace"),
        pytest.param("95992 42207", "9599242207", id="td-crm-spaces"),
        pytest.param("+91 95992 42207", "9599242207", id="td-crm-country-code-spaces"),
        pytest.param("(+91) 95992 42207", "9599242207", id="vw-orders-parentheses-country-code"),
        pytest.param("+91-95992-42207", "9599242207", id="td-crm-hyphens-country-code"),
        pytest.param("9599242207.0", "9599242207", id="production-export-decimal-text"),
        pytest.param("9.599242207E9", "9599242207", id="production-export-scientific-text"),
        pytest.param("'9599242207", "9599242207", id="excel-text-apostrophe"),
        pytest.param(None, None, id="missing"),
        pytest.param("", None, id="blank"),
    ],
)
def test_normalize_mobile_number_matches_td_crm_and_vw_orders_exports(raw_mobile: object, expected: str | None) -> None:
    assert _normalize_mobile_number(raw_mobile) == expected


def test_build_td_mobile_match_debug_diagnostics_masks_originals_and_emits_normalized_last4() -> None:
    diagnostics = _build_td_mobile_match_debug_diagnostics(
        lead_mobile=" 9.599242207E9 ",
        order_mobile="(+91) 95992-42207",
    )

    assert diagnostics == {
        "original_lead_mobile_masked": "***2207",
        "normalized_lead_mobile_last4": "2207",
        "original_order_mobile_masked": "***2207",
        "normalized_order_mobile_last4": "2207",
    }

def test_business_day_bounds_utc_respects_pipeline_timezone() -> None:
    reference_ts = datetime(2026, 4, 22, 10, 15, tzinfo=timezone.utc)

    day_start_utc, day_end_utc = _business_day_bounds_utc(reference_ts=reference_ts)

    assert day_start_utc == datetime(2026, 4, 21, 18, 30, tzinfo=timezone.utc)
    assert day_end_utc == datetime(2026, 4, 22, 18, 29, 59, 999999, tzinfo=timezone.utc)


def test_calculate_lead_age_days_returns_whole_days_and_is_null_safe() -> None:
    report_generated_at = datetime(2026, 4, 24, 9, 0, tzinfo=timezone.utc)

    assert _calculate_lead_age_days(lead_created_at="2026-04-21 09:33:39", reference_ts=report_generated_at) == 2
    assert _calculate_lead_age_days(lead_created_at=None, reference_ts=report_generated_at) is None
    assert _calculate_lead_age_days(lead_created_at="invalid", reference_ts=report_generated_at) is None


@pytest.mark.asyncio
async def test_fetch_business_day_cancelled_td_leads_uses_events_window_and_normalizes_reason_flag(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'td_cancelled_window.db'}"
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                sa.text(
                    """
                    CREATE TABLE crm_leads_current (
                        lead_uid TEXT PRIMARY KEY,
                        store_code TEXT,
                        pickup_no TEXT,
                        customer_name TEXT,
                        mobile TEXT,
                        pickup_created_at TEXT,
                        reason TEXT,
                        cancelled_flag TEXT,
                        source TEXT,
                        customer_type TEXT,
                        status_bucket TEXT
                    )
                    """
                )
            )
            await connection.execute(
                sa.text(
                    """
                    CREATE TABLE crm_leads_status_events (
                        lead_uid TEXT,
                        status_bucket TEXT,
                        scraped_at TEXT,
                        created_at TEXT
                    )
                    """
                )
            )
            await connection.execute(
                sa.text(
                    """
                    CREATE TABLE vw_orders (
                        store_code TEXT,
                        mobile_number TEXT,
                        order_amount NUMERIC
                    )
                    """
                )
            )
            await connection.execute(
                sa.text(
                    """
                    INSERT INTO crm_leads_current (
                        lead_uid, store_code, pickup_no, customer_name, mobile, pickup_created_at, reason, cancelled_flag, source, customer_type
                    ) VALUES
                        ('L1', 'A001', 'A001-1', 'Alice', '9000000001', '2026-04-29 12:00:00+00:00', '', NULL, 'Meta', 'New'),
                        ('L2', 'A002', 'A002-2', 'Bob', '9000000002', '2026-04-20 12:00:00+00:00', 'No inventory', 'STORE', 'Web', 'Existing')
                    """
                )
            )
            await connection.execute(
                sa.text(
                    """
                    INSERT INTO crm_leads_status_events (lead_uid, status_bucket, scraped_at, created_at) VALUES
                        ('L1', 'cancelled', '2026-04-30 19:00:00+00:00', '2026-04-30 19:00:00+00:00'),
                        ('L2', 'cancelled', '2026-04-30 20:30:00+00:00', '2026-04-30 20:30:00+00:00'),
                        ('L2', 'cancelled', '2026-04-29 20:30:00+00:00', '2026-04-29 20:30:00+00:00')
                    """
                )
            )

        rows = await td_leads_main.fetch_business_day_cancelled_td_leads(
            database_url=database_url,
            reference_ts=datetime(2026, 5, 1, 1, 0, tzinfo=timezone.utc),
        )
    finally:
        await engine.dispose()

    assert [row["pickup_no"] for row in rows] == ["A001-1", "A002-2"]
    assert rows[0]["cancelled_flag"] == "customer"
    assert rows[0]["cancel_reason"] == ""
    assert rows[1]["cancelled_flag"] == "store"
    assert rows[1]["cancel_reason"] == "No inventory"
    assert rows[0]["lead_age_days_at_cancel"] == 1

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
        ("customer_type", "Existing"),
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
        "Customer Type",
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
        "Existing",
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


def test_coerce_pickup_created_at_normalizes_repeated_whitespace() -> None:
    coerced = td_leads_ingest._coerce_pickup_created_at(
        row={"pickup_created_at": None},
        normalized_created_date="21   Apr   2026   3:03:39    PM",
    )

    assert coerced == datetime(2026, 4, 21, 9, 33, 39, tzinfo=timezone.utc)


def test_normalized_pickup_created_text_falls_back_to_pickup_date_when_created_missing() -> None:
    normalized = td_leads_ingest._normalized_pickup_created_text(
        {
            "pickup_date": "21 Apr 2026",
            "pickup_created_at": None,
            "pickup_created_text": None,
            "pickup_created_date": None,
        }
    )

    assert normalized == "21 Apr 2026"


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
    assert record["metrics_json"]["has_new_leads"] is False
    assert "A668" in record["summary_text"]
    assert "lead_change_details" in record["metrics_json"]["stores"][0]
    assert "lead_change_details" in record["metrics_json"]
    assert "rows" not in record["metrics_json"]["stores"][0]




def test_td_leads_run_summary_record_includes_frozen_day_report_datasets_for_reporting_modes() -> None:
    started_at = datetime(2026, 4, 22, 0, 0, tzinfo=timezone.utc)
    finished_at = datetime(2026, 4, 22, 0, 1, tzinfo=timezone.utc)
    summary = LeadsRunSummary(
        run_id="run-freeze",
        run_env="local",
        report_date=started_at.date(),
        started_at=started_at,
        store_results={"A668": StoreLeadResult(store_code="A668")},
    )

    default_record = summary.build_record(finished_at=finished_at)
    meeting_record = summary.build_record(finished_at=finished_at, reporting_mode="meeting")

    assert default_record["metrics_json"]["frozen_day_report_datasets"] is None
    frozen = meeting_record["metrics_json"]["frozen_day_report_datasets"]
    assert frozen["reporting_mode"] == "meeting"
    assert frozen["report_date"] == "2026-04-22"
    assert "Reporting Mode: meeting" in meeting_record["summary_text"]

def test_td_leads_run_summary_record_marks_has_new_leads_true_when_created_events_exist() -> None:
    started_at = datetime(2026, 4, 22, 0, 0, tzinfo=timezone.utc)
    finished_at = datetime(2026, 4, 22, 0, 1, tzinfo=timezone.utc)
    summary = LeadsRunSummary(
        run_id="run-new",
        run_env="local",
        report_date=started_at.date(),
        started_at=started_at,
        store_results={
            "A668": StoreLeadResult(
                store_code="A668",
                lead_change_details={
                    "created_by_bucket": [
                        {
                            "status_bucket": "pending",
                            "rows": [{"lead_identity": {"lead_uid": "lead-1"}}],
                            "overflow_count": 0,
                        }
                    ]
                },
            )
        },
    )

    record = summary.build_record(finished_at=finished_at)

    assert record["metrics_json"]["has_new_leads"] is True


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
                                    "customer_type": "Retail",
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
    assert "<th align='left'>Lead Details</th>" in tables_html
    assert "<th align='left'>Lead Details</th><th align='left'>Copy</th>" not in tables_html
    assert "<th align='left'>Lead Details</th><th align='left'>Cancellation Context</th>" in tables_html
    assert "<th align='left'>Lead Details</th><th align='left'>Cancellation Context</th><th align='left'>Copy</th>" not in tables_html
    assert (
        "<th align='left'>Store Code</th><th align='left'>Pickup No</th><th align='left'>Customer Name</th>"
        "<th align='left'>Mobile</th><th align='left'>Customer Type</th><th align='left'>Number of Orders</th>"
        "<th align='left'>Average Order Value</th><th align='left'>Created Date/Time</th>"
    ) in tables_html

    new_lead_payload = "A817, Pending 1, 9000000000, None, Retail, None"
    cancelled_lead_payload = "A817, Raj, 9111111111, No inventory, 2026-04-22 09:00"
    assert tables_html.count(new_lead_payload) == 1
    assert tables_html.count(cancelled_lead_payload) == 1
    assert f"<tr><td>{new_lead_payload}</td></tr>" in tables_html
    assert f"<tr><td>{cancelled_lead_payload}</td><td>Store Cancelled | No inventory</td></tr>" in tables_html
    assert "Store Cancelled | No inventory" in tables_html
    assert "Copy" not in tables_html
    assert "📋 Copy" not in tables_html
    assert "onclick='var v=" not in tables_html
    assert "Completed</h5>" not in tables_html
    assert "Converted" not in tables_html


def test_build_td_leads_tables_html_includes_existing_customer_order_metrics_for_new_leads() -> None:
    summary = LeadsRunSummary(
        run_id="run-enriched-new-lead",
        run_env="test",
        report_date=datetime(2026, 5, 1, tzinfo=timezone.utc).date(),
        store_results={
            "A200": StoreLeadResult(
                store_code="A200",
                rows=[
                    {
                        "status_bucket": "pending",
                        "pickup_no": "A200-HIST",
                        "customer_name": "Historical Lead",
                        "mobile": "+91-90000 00003",
                        "source": "Meta",
                        "customer_type": "Existing",
                        "previous_number_of_orders": 2,
                        "average_order_amount": "1234.5",
                    }
                ],
                lead_change_details={
                    "created_by_bucket": [
                        {"rows": [{"lead_identity": {"pickup_no": "A200-HIST"}}]}
                    ]
                },
            )
        },
    )

    lead_tables_html = _build_td_leads_tables_html(summary=summary)

    assert "A200, Historical Lead, +91-90000 00003, Meta, Existing | Orders: 2 | Avg. Value: ₹1,234.50" in lead_tables_html
    assert "<!-- order_metrics_source store=A200 pickup_no=A200-HIST source=enriched_result_rows -->" in lead_tables_html


def test_build_td_leads_tables_html_uses_enriched_row_over_created_row_for_new_leads() -> None:
    summary = LeadsRunSummary(
        run_id="run-enriched-row-source-of-truth",
        run_env="test",
        report_date=datetime(2026, 5, 1, tzinfo=timezone.utc).date(),
        store_results={
            "A200": StoreLeadResult(
                store_code="A200",
                rows=[
                    {
                        "status_bucket": "pending",
                        "pickup_no": "A200-ENRICHED",
                        "customer_name": "Enriched Customer",
                        "mobile": "+91-90000 00999",
                        "source": "Enriched Meta",
                        "customer_type": "Existing",
                        "previous_number_of_orders": 4,
                        "average_order_amount": "1500.25",
                        "pickup_created_text": "01 May 2026 10:15:00 AM",
                    }
                ],
                lead_change_details={
                    "created_by_bucket": [
                        {
                            "rows": [
                                {
                                    "customer_name": "Stale Created Customer",
                                    "mobile": "+91-90000 00000",
                                    "source": "Stale Source",
                                    "customer_type": "Existing",
                                    "previous_number_of_orders": 0,
                                    "average_order_amount": "0",
                                    "pickup_created_text": "30 Apr 2026 09:00:00 AM",
                                    "lead_identity": {"pickup_no": "A200-ENRICHED"},
                                }
                            ]
                        }
                    ]
                },
            )
        },
    )

    lead_tables_html = _build_td_leads_tables_html(summary=summary)

    assert "A200, Enriched Customer, +91-90000 00999, Enriched Meta, Existing | Orders: 4 | Avg. Value: ₹1,500.25, 01 May 2026 10:15:00 AM" in lead_tables_html
    assert "Stale Created Customer" not in lead_tables_html
    assert "+91-90000 00000" not in lead_tables_html
    assert "Stale Source" not in lead_tables_html
    assert "Existing | Orders: 0" not in lead_tables_html
    assert "30 Apr 2026 09:00:00 AM" not in lead_tables_html
    assert "<!-- order_metrics_source store=A200 pickup_no=A200-ENRICHED source=enriched_result_rows -->" in lead_tables_html


def test_build_td_leads_tables_html_unmatched_created_row_with_zero_metrics_shows_unavailable() -> None:
    summary = LeadsRunSummary(
        run_id="run-created-row-zero-metrics-no-match",
        run_env="test",
        report_date=datetime(2026, 5, 1, tzinfo=timezone.utc).date(),
        store_results={
            "A200": StoreLeadResult(
                store_code="A200",
                rows=[],
                lead_change_details={
                    "created_by_bucket": [
                        {
                            "rows": [
                                {
                                    "customer_name": "Created Only Existing",
                                    "mobile": "9000000003",
                                    "source": "Meta",
                                    "customer_type": "Existing",
                                    "previous_number_of_orders": 0,
                                    "average_order_amount": "0",
                                    "lead_identity": {"pickup_no": "A200-NO-MATCH"},
                                }
                            ]
                        }
                    ]
                },
            )
        },
    )

    lead_tables_html = _build_td_leads_tables_html(summary=summary)

    assert "A200, Created Only Existing, 9000000003, Meta, Existing | Order history unavailable: lead row not matched" in lead_tables_html
    assert "Orders: 0" not in lead_tables_html
    assert "Avg. Value: ₹0.00" not in lead_tables_html


def test_build_td_leads_tables_html_marks_unmatched_existing_new_lead_history_unavailable() -> None:
    summary = LeadsRunSummary(
        run_id="run-unmatched-existing-new-lead",
        run_env="test",
        report_date=datetime(2026, 5, 1, tzinfo=timezone.utc).date(),
        store_results={
            "A200": StoreLeadResult(
                store_code="A200",
                rows=[
                    {
                        "status_bucket": "pending",
                        "pickup_no": "A200-OTHER",
                        "customer_name": "Other Lead",
                        "mobile": "+91-90000 00004",
                        "source": "Meta",
                        "customer_type": "Existing",
                        "previous_number_of_orders": 0,
                        "average_order_amount": "0",
                    }
                ],
                lead_change_details={
                    "created_by_bucket": [
                        {
                            "rows": [
                                {
                                    "customer_name": "Unmatched Historical Lead",
                                    "mobile": "+91-90000 00003",
                                    "source": "Meta",
                                    "customer_type": "Existing",
                                    "lead_identity": {"pickup_no": "A200-MISSING"},
                                }
                            ]
                        }
                    ]
                },
            )
        },
    )

    lead_tables_html = _build_td_leads_tables_html(summary=summary)

    assert "Existing | Order history unavailable: lead row not matched" in lead_tables_html
    assert "A200, Unmatched Historical Lead, +91-90000 00003, Meta, Existing | Order history unavailable: lead row not matched" in lead_tables_html
    assert "<!-- order_metrics_source store=A200 pickup_no=A200-MISSING source=no_match -->" in lead_tables_html
    assert "Orders: 0" not in lead_tables_html


def test_build_td_leads_tables_html_ignores_order_metrics_from_lead_change_details() -> None:
    summary = LeadsRunSummary(
        run_id="run-lead-change-order-metrics",
        run_env="test",
        report_date=datetime(2026, 5, 1, tzinfo=timezone.utc).date(),
        store_results={
            "A200": StoreLeadResult(
                store_code="A200",
                rows=[],
                lead_change_details={
                    "created_by_bucket": [
                        {
                            "rows": [
                                {
                                    "customer_name": "Changed Lead",
                                    "mobile": "+91-90000 00005",
                                    "source": "Meta",
                                    "customer_type": "Existing",
                                    "previous_number_of_orders": 3,
                                    "average_order_amount": "2000",
                                    "lead_identity": {"pickup_no": "A200-CHANGE"},
                                }
                            ]
                        }
                    ]
                },
            )
        },
    )

    lead_tables_html = _build_td_leads_tables_html(summary=summary)

    assert "<!-- order_metrics_source store=A200 pickup_no=A200-CHANGE source=no_match -->" in lead_tables_html
    assert "Orders: 3" not in lead_tables_html
    assert "Avg. Value: ₹2,000.00" not in lead_tables_html
    assert "Existing | Order history unavailable: lead row not matched" in lead_tables_html


async def _build_enriched_td_lead_summary_html(
    *,
    tmp_path,
    db_name: str,
    orders: list[tuple[str, str, object]],
    reporting_mode: str | None = None,
) -> str:
    database_url = f"sqlite+aiosqlite:///{tmp_path / db_name}"
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(sa.text("""
                CREATE TABLE vw_orders (
                    store_code TEXT,
                    mobile_number TEXT,
                    order_amount NUMERIC
                )
            """))
            await connection.execute(
                sa.text("""
                    INSERT INTO vw_orders (store_code, mobile_number, order_amount)
                    VALUES (:store_code, :mobile_number, :order_amount)
                """),
                [
                    {"store_code": store_code, "mobile_number": mobile_number, "order_amount": order_amount}
                    for store_code, mobile_number, order_amount in orders
                ],
            )

        summary = LeadsRunSummary(
            run_id="run-enriched-normal-email",
            run_env="test",
            report_date=datetime(2026, 5, 1, tzinfo=timezone.utc).date(),
            store_results={
                "A200": StoreLeadResult(
                    store_code="A200",
                    rows=[
                        {
                            "store_code": "A200",
                            "status_bucket": "pending",
                            "pickup_no": "A200-HIST",
                            "customer_name": "Historical Lead",
                            "mobile": "+91 90000 00003",
                            "source": "Meta",
                            "customer_type": "Existing",
                            "pickup_created_at": "2026-05-01 09:00:00+00:00",
                        }
                    ],
                    lead_change_details={
                        "created_by_bucket": [
                            {
                                "rows": [
                                    {
                                        "customer_name": "Historical Lead",
                                        "mobile": "9000000003",
                                        "source": "Meta",
                                        "customer_type": "Existing",
                                        "lead_identity": {"pickup_no": "A200-HIST"},
                                    }
                                ]
                            }
                        ],
                        "transitions": [],
                    },
                )
            },
        )
        await td_leads_main._enrich_td_summary_with_order_history(database_url=database_url, summary=summary)
        record = summary.build_record(
            finished_at=datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc),
            reporting_mode=reporting_mode,
        )
        return record["metrics_json"]["lead_tables_html"]
    finally:
        await engine.dispose()



@pytest.mark.asyncio
async def test_normal_run_summary_html_has_order_history_metrics_before_render(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'td_normal_run_summary_history.db'}"
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(sa.text("""
                CREATE TABLE vw_orders (
                    store_code TEXT,
                    mobile_number TEXT,
                    order_amount NUMERIC
                )
            """))
            await connection.execute(sa.text("""
                INSERT INTO vw_orders (store_code, mobile_number, order_amount)
                VALUES ('A200', '9000000003', 1234.5)
            """))

        store_result = StoreLeadResult(
            store_code="A200",
            rows=[
                {
                    "store_code": "A200",
                    "status_bucket": "pending",
                    "pickup_no": "A200-HIST",
                    "customer_name": "Historical Lead",
                    "mobile": "+91 90000 00003",
                    "source": "Meta",
                    "customer_type": "Existing",
                    "pickup_created_at": "2026-05-01 09:00:00+00:00",
                }
            ],
            lead_change_details={
                "created_by_bucket": [
                    {
                        "rows": [
                            {
                                "customer_name": "Historical Lead",
                                "mobile": "9000000003",
                                "source": "Meta",
                                "customer_type": "Existing",
                                "lead_identity": {"pickup_no": "A200-HIST"},
                            }
                        ]
                    }
                ],
                "transitions": [],
            },
        )
        summary = LeadsRunSummary(
            run_id="run-normal-email-history",
            run_env="test",
            report_date=datetime(2026, 5, 1, tzinfo=timezone.utc).date(),
            store_results={"A200": store_result},
        )

        await td_leads_main._enrich_td_lead_rows_with_order_history(database_url=database_url, rows=store_result.rows)
        record = summary.build_record(finished_at=datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc), reporting_mode=None)
        summary_html = record["metrics_json"]["summary_html"]
    finally:
        await engine.dispose()

    assert "Existing | Orders: 1" in summary_html
    assert "Avg. Value: ₹1,234.50" in summary_html
    assert "Order history unavailable: lead row not enriched" not in summary_html


@pytest.mark.asyncio
async def test_run_store_enriches_rows_with_order_history_before_returning(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'td_run_store_enriches.db'}"
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(sa.text("""
                CREATE TABLE vw_orders (
                    store_code TEXT,
                    mobile_number TEXT,
                    order_amount NUMERIC
                )
            """))
            await connection.execute(sa.text("""
                INSERT INTO vw_orders (store_code, mobile_number, order_amount)
                VALUES ('A200', '9000000003', 1234.5)
            """))

        class _FakeContext:
            async def new_page(self):
                return object()

            async def storage_state(self, *, path: str) -> None:
                return None

            async def close(self) -> None:
                return None

        class _FakeBrowser:
            async def new_context(self, **kwargs):
                return _FakeContext()

        row = {
            "store_code": "A200",
            "status_bucket": "pending",
            "pickup_no": "A200-HIST",
            "customer_name": "Historical Lead",
            "mobile": "+91 90000 00003",
            "source": "Meta",
            "customer_type": "Existing",
            "pickup_created_at": "2026-05-01 09:00:00+00:00",
        }

        async def _fake_collect_status_rows(page, *, store_code, status_bucket, status_value, grid_selector, logger):
            return ([dict(row)] if status_bucket == "pending" else []), []

        async def _fake_ingest_td_crm_leads_rows(**kwargs):
            return td_leads_main.TdLeadsIngestResult(
                rows_received=1,
                rows_upserted=1,
                bucket_write_counts={"pending": {"inserted": 1, "updated": 0}},
                pickup_created_at_null_count=0,
                pickup_created_at_null_counts_by_bucket={},
                status_transitions=[],
                lead_change_details={
                    "created_by_bucket": [
                        {
                            "rows": [
                                {
                                    "customer_name": "Historical Lead",
                                    "mobile": "9000000003",
                                    "source": "Meta",
                                    "customer_type": "Existing",
                                    "lead_identity": {"pickup_no": "A200-HIST"},
                                }
                            ]
                        }
                    ],
                    "transitions": [],
                },
                task_stub={},
            )

        monkeypatch.setattr(td_leads_main, "config", SimpleNamespace(database_url=database_url))
        monkeypatch.setattr(td_leads_main, "_perform_login", lambda *args, **kwargs: asyncio.sleep(0, result=True))
        monkeypatch.setattr(td_leads_main, "_wait_for_otp_verification", lambda *args, **kwargs: asyncio.sleep(0, result=(True, False)))
        monkeypatch.setattr(td_leads_main, "_wait_for_home", lambda *args, **kwargs: asyncio.sleep(0, result=True))
        monkeypatch.setattr(td_leads_main, "_ensure_scheduler_page", lambda *args, **kwargs: asyncio.sleep(0, result=True))
        monkeypatch.setattr(td_leads_main, "_collect_status_rows", _fake_collect_status_rows)
        monkeypatch.setattr(td_leads_main, "_write_store_artifact", lambda **kwargs: tmp_path / "td_leads.xlsx")
        monkeypatch.setattr(td_leads_main, "ingest_td_crm_leads_rows", _fake_ingest_td_crm_leads_rows)

        result = await td_leads_main._run_store(
            browser=_FakeBrowser(),
            store=SimpleNamespace(
                store_code="A200",
                storage_state_path=tmp_path / "storage" / "A200.json",
                reports_nav_selector="#nav",
            ),
            run_id="run-store-enriches",
            run_env="test",
            logger=SimpleNamespace(info=lambda **kwargs: None),
        )
    finally:
        await engine.dispose()

    assert result.status in {"ok", "error"}
    assert result.rows[0]["previous_number_of_orders"] == 1
    assert str(result.rows[0]["average_order_amount"]) == "1234.5"
    assert result.rows[0]["customer_type"] == "Existing"
    assert "order_history_warning_marker" not in result.rows[0]


@pytest.mark.asyncio
async def test_run_store_logs_benign_probe_reauth_as_info(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    events: list[dict[str, object]] = []

    class _FakeContext:
        async def new_page(self):
            return object()

        async def storage_state(self, *, path: str) -> None:
            return None

        async def close(self) -> None:
            return None

    class _FakeBrowser:
        async def new_context(self, **kwargs):
            return _FakeContext()

    async def _fake_collect_status_rows(page, *, store_code, status_bucket, status_value, grid_selector, logger):
        return ([], [])

    monkeypatch.setattr(td_leads_main, "config", SimpleNamespace(database_url=f"sqlite+aiosqlite:///{tmp_path / 'noop.db'}"))
    monkeypatch.setattr(
        td_leads_main,
        "_probe_session",
        lambda *args, **kwargs: asyncio.sleep(
            0,
            result=SimpleNamespace(valid=False, reason="login_form_visible", verification_seen=False, nav_visible=False, home_card_visible=False),
        ),
    )
    monkeypatch.setattr(td_leads_main, "_perform_login", lambda *args, **kwargs: asyncio.sleep(0, result=True))
    monkeypatch.setattr(td_leads_main, "_wait_for_otp_verification", lambda *args, **kwargs: asyncio.sleep(0, result=(True, False)))
    monkeypatch.setattr(td_leads_main, "_wait_for_home", lambda *args, **kwargs: asyncio.sleep(0, result=True))
    monkeypatch.setattr(td_leads_main, "_ensure_scheduler_page", lambda *args, **kwargs: asyncio.sleep(0, result=True))
    monkeypatch.setattr(td_leads_main, "_collect_status_rows", _fake_collect_status_rows)
    monkeypatch.setattr(td_leads_main, "_write_store_artifact", lambda **kwargs: tmp_path / "td_leads.xlsx")
    monkeypatch.setattr(td_leads_main, "ingest_td_crm_leads_rows", lambda **kwargs: asyncio.sleep(0, result=TdLeadsIngestResult()))
    monkeypatch.setattr("app.crm_downloader.td_leads_sync.main.log_event", lambda **kwargs: events.append(kwargs))

    storage_state_path = tmp_path / "storage" / "A200.json"
    storage_state_path.parent.mkdir(parents=True, exist_ok=True)
    storage_state_path.write_text("{}")
    result = await td_leads_main._run_store(
        browser=_FakeBrowser(),
        store=SimpleNamespace(store_code="A200", storage_state_path=storage_state_path, reports_nav_selector="#nav"),
        run_id="run-benign-probe",
        run_env="test",
        logger=SimpleNamespace(info=lambda **kwargs: None),
    )

    assert result.status in {"ok", "error"}
    started_event = next(event for event in events if event.get("message") == "Storage state probe invalid; performing leads login")
    assert started_event["status"] == "info"
    assert started_event["probe_result"] == "invalid"
    assert started_event["login_followup_status"] == "started"
    follow_up_event = next(event for event in events if event.get("message") == "Storage state probe follow-up leads login outcome")
    assert follow_up_event["status"] == "info"
    assert follow_up_event["login_followup_status"] == "success"


@pytest.mark.asyncio
async def test_run_store_logs_anomalous_probe_reauth_as_warning(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    events: list[dict[str, object]] = []

    class _FakeContext:
        async def new_page(self):
            return object()

        async def storage_state(self, *, path: str) -> None:
            return None

        async def close(self) -> None:
            return None

    class _FakeBrowser:
        async def new_context(self, **kwargs):
            return _FakeContext()

    async def _fake_collect_status_rows(page, *, store_code, status_bucket, status_value, grid_selector, logger):
        return ([], [])

    monkeypatch.setattr(td_leads_main, "config", SimpleNamespace(database_url=f"sqlite+aiosqlite:///{tmp_path / 'noop2.db'}"))
    monkeypatch.setattr(
        td_leads_main,
        "_probe_session",
        lambda *args, **kwargs: asyncio.sleep(
            0,
            result=SimpleNamespace(valid=False, reason="verification_redirect", verification_seen=True, nav_visible=False, home_card_visible=False),
        ),
    )
    monkeypatch.setattr(td_leads_main, "_perform_login", lambda *args, **kwargs: asyncio.sleep(0, result=True))
    monkeypatch.setattr(td_leads_main, "_wait_for_otp_verification", lambda *args, **kwargs: asyncio.sleep(0, result=(True, False)))
    monkeypatch.setattr(td_leads_main, "_wait_for_home", lambda *args, **kwargs: asyncio.sleep(0, result=True))
    monkeypatch.setattr(td_leads_main, "_ensure_scheduler_page", lambda *args, **kwargs: asyncio.sleep(0, result=True))
    monkeypatch.setattr(td_leads_main, "_collect_status_rows", _fake_collect_status_rows)
    monkeypatch.setattr(td_leads_main, "_write_store_artifact", lambda **kwargs: tmp_path / "td_leads.xlsx")
    monkeypatch.setattr(td_leads_main, "ingest_td_crm_leads_rows", lambda **kwargs: asyncio.sleep(0, result=TdLeadsIngestResult()))
    monkeypatch.setattr("app.crm_downloader.td_leads_sync.main.log_event", lambda **kwargs: events.append(kwargs))

    storage_state_path = tmp_path / "storage" / "A200.json"
    storage_state_path.parent.mkdir(parents=True, exist_ok=True)
    storage_state_path.write_text("{}")
    result = await td_leads_main._run_store(
        browser=_FakeBrowser(),
        store=SimpleNamespace(store_code="A200", storage_state_path=storage_state_path, reports_nav_selector="#nav"),
        run_id="run-anomalous-probe",
        run_env="test",
        logger=SimpleNamespace(info=lambda **kwargs: None),
    )

    assert result.status in {"ok", "error"}
    started_event = next(event for event in events if event.get("message") == "Storage state probe invalid; performing leads login")
    assert started_event["status"] == "warning"
    assert started_event["probe_result"] == "invalid"
    assert started_event["login_followup_status"] == "started"

@pytest.mark.asyncio
async def test_normal_mode_summary_html_renders_existing_lead_with_one_historical_paid_order(tmp_path) -> None:
    lead_tables_html = await _build_enriched_td_lead_summary_html(
        tmp_path=tmp_path,
        db_name="td_paid_order_history.db",
        orders=[("A200", "9000000003", 1234.50)],
    )

    assert "Existing | Orders: 1 | Avg. Value: ₹1,234.50" in lead_tables_html


@pytest.mark.asyncio
async def test_normal_mode_summary_html_renders_existing_lead_with_one_historical_zero_value_order(tmp_path) -> None:
    lead_tables_html = await _build_enriched_td_lead_summary_html(
        tmp_path=tmp_path,
        db_name="td_zero_order_history.db",
        orders=[("A200", "9000000003", 0)],
    )

    assert "Existing | Orders: 1 | Avg. Value: ₹0.00" in lead_tables_html


@pytest.mark.asyncio
async def test_normal_mode_summary_html_average_includes_paid_and_zero_value_orders(tmp_path) -> None:
    lead_tables_html = await _build_enriched_td_lead_summary_html(
        tmp_path=tmp_path,
        db_name="td_paid_and_zero_order_history.db",
        orders=[("A200", "+91-90000 00003", 1000), ("A200", "9000000003", 0)],
    )

    assert "Existing | Orders: 2 | Avg. Value: ₹500.00" in lead_tables_html


@pytest.mark.asyncio
@pytest.mark.parametrize("reporting_mode", [None, "meeting", "day_end"])
async def test_seeded_lead_order_metrics_are_identical_across_reporting_modes(tmp_path, reporting_mode: str | None) -> None:
    lead_tables_html = await _build_enriched_td_lead_summary_html(
        tmp_path=tmp_path,
        db_name=f"td_{reporting_mode or 'normal'}_mode_order_history.db",
        orders=[("A200", "9000000003", 1000), ("A200", "9000000003", 0)],
        reporting_mode=reporting_mode,
    )

    assert "Existing | Orders: 2 | Avg. Value: ₹500.00" in lead_tables_html


def test_lead_change_details_alone_cannot_render_misleading_existing_orders_zero() -> None:
    summary = LeadsRunSummary(
        run_id="run-lead-change-no-zero-fallback",
        run_env="test",
        report_date=datetime(2026, 5, 1, tzinfo=timezone.utc).date(),
        store_results={
            "A200": StoreLeadResult(
                store_code="A200",
                rows=[],
                lead_change_details={
                    "created_by_bucket": [
                        {
                            "rows": [
                                {
                                    "customer_name": "Unmatched Existing",
                                    "mobile": "9000000003",
                                    "source": "Meta",
                                    "customer_type": "Existing",
                                    "previous_number_of_orders": 0,
                                    "average_order_amount": "0",
                                    "lead_identity": {"pickup_no": "A200-CHANGE-ONLY"},
                                }
                            ]
                        }
                    ],
                    "transitions": [],
                },
            )
        },
    )

    lead_tables_html = _build_td_leads_tables_html(summary=summary)

    assert "Existing | Order history unavailable: lead row not matched" in lead_tables_html
    assert "Orders: 0" not in lead_tables_html
    assert "Avg. Value: ₹0.00" not in lead_tables_html


def test_build_td_new_lead_payload_formats_order_and_normalization() -> None:
    payload = _build_td_new_lead_payload(
        store_code=" A817 ",
        row={
            "customer_name": "  Alice  ",
            "mobile": " 9000000000 ",
            "source": " Walk-in ",
            "customer_type": " Retail ",
            "pickup_created_text": "22 Apr 2026 11:00:00 AM",
        },
    )

    assert payload == "A817, Alice, 9000000000, Walk-in, Retail, 22 Apr 2026 11:00:00 AM"


def test_td_leads_summary_email_includes_local_pickup_created_for_identified_leads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(td_leads_main, "get_timezone", lambda: ZoneInfo("Asia/Kolkata"))
    summary = LeadsRunSummary(
        run_id="run-local-created",
        run_env="test",
        report_date=datetime(2026, 4, 22, tzinfo=timezone.utc).date(),
        store_results={
            "A817": StoreLeadResult(
                store_code="A817",
                rows=[
                    {
                        "status_bucket": "pending",
                        "pickup_no": "A817-LOCAL",
                        "customer_name": "Local Timestamp",
                        "mobile": "9000000001",
                        "source": "Meta",
                        "customer_type": "New",
                        "pickup_created_at": datetime(2026, 4, 22, 9, 33, 39, tzinfo=timezone.utc),
                    }
                ],
                lead_change_details={
                    "created_by_bucket": [{"rows": [{"lead_identity": {"pickup_no": "A817-LOCAL"}}]}]
                },
            )
        },
    )

    summary_html = _build_td_leads_summary_html(summary=summary, duration_human="00:01:00")

    assert "22 Apr 2026 03:03:39 PM IST" in summary_html
    assert "UTC" not in summary_html


def test_build_td_new_lead_payload_uses_none_for_missing_values() -> None:
    payload = _build_td_new_lead_payload(
        store_code="A817",
        row={
            "customer_name": "  ",
            "mobile": None,
            "source": "",
            "customer_type": None,
            "pickup_created_text": None,
        },
    )

    assert payload == "A817, None, None, None, None, None"


def test_format_pickup_created_display_converts_aware_utc_to_configured_timezone(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(td_leads_main, "get_timezone", lambda: ZoneInfo("Asia/Kolkata"))

    rendered = td_leads_main._format_pickup_created_display(
        {"pickup_created_at": datetime(2026, 4, 22, 9, 33, 39, tzinfo=timezone.utc)}
    )

    assert rendered == "22 Apr 2026 03:03:39 PM IST"
    assert "UTC" not in rendered


def test_format_pickup_created_display_treats_naive_datetime_as_utc(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(td_leads_main, "get_timezone", lambda: ZoneInfo("Asia/Kolkata"))

    rendered = td_leads_main._format_pickup_created_display(
        {"pickup_created_at": datetime(2026, 4, 22, 9, 33, 39)}
    )

    assert rendered == "22 Apr 2026 03:03:39 PM IST"


def test_build_td_cancelled_lead_payload_formats_order_and_normalization() -> None:
    payload = _build_td_cancelled_lead_payload(
        store_code=" A817 ",
        row={
            "customer_name": "  Alice  ",
            "mobile": " 9000000000 ",
            "reason": " No inventory ",
        },
    )

    assert payload == "A817, Alice, 9000000000, No inventory, None"


def test_td_leads_tables_html_escapes_untrusted_payload_values_without_copy_control_html() -> None:
    summary = LeadsRunSummary(
        run_id="run-html-safe",
        run_env="local",
        report_date=datetime(2026, 4, 22, tzinfo=timezone.utc).date(),
        store_results={
            "A817": StoreLeadResult(
                store_code="A817",
                rows=[
                    {
                        "status_bucket": "pending",
                        "pickup_no": "A817-1",
                        "customer_name": "<script>alert(1)</script>",
                        "mobile": "9000000000",
                        "source": "Web & App",
                    }
                ],
                lead_change_details={
                    "created_by_bucket": [{"rows": [{"lead_identity": {"pickup_no": "A817-1"}}]}],
                },
            )
        },
    )

    tables_html = _build_td_leads_tables_html(summary=summary)

    escaped_payload = "A817, &lt;script&gt;alert(1)&lt;/script&gt;, 9000000000, Web &amp; App, None, None"
    assert tables_html.count(escaped_payload) == 1
    assert "<script>alert(1)</script>" not in tables_html
    assert "Copy" not in tables_html
    assert "📋 Copy" not in tables_html
    assert "href='javascript:void(0)'" not in tables_html


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




def test_td_leads_tables_html_pending_prefers_pickup_created_text_then_falls_back_to_effective_created() -> None:
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
                    {
                        "status_bucket": "pending",
                        "customer_name": "Pickup Date Fallback",
                        "mobile": "9000000003",
                        "pickup_date": "22 Apr 2026",
                        "pickup_no": "A817-3",
                    },
                ],
                lead_change_details={
                    "created_by_bucket": [
                        {
                            "rows": [
                                {"lead_identity": {"pickup_no": "A817-1"}},
                                {"lead_identity": {"pickup_no": "A817-2"}},
                                {"lead_identity": {"pickup_no": "A817-3"}},
                            ]
                        }
                    ]
                },
            )
        },
    )

    tables_html = _build_td_leads_tables_html(summary=summary)

    assert tables_html.count("21 Apr 2026 03:03:39 PM IST") == 4
    assert "UTC" not in tables_html
    assert "22 Apr 2026" in tables_html


def test_td_leads_tables_html_pending_renders_enriched_fields_and_customer_metrics_behavior() -> None:
    summary = LeadsRunSummary(
        run_id="run-pending-enriched-fields",
        run_env="local",
        report_date=datetime(2026, 4, 22, tzinfo=timezone.utc).date(),
        store_results={
            "A817": StoreLeadResult(
                store_code="A817",
                rows=[
                    {
                        "status_bucket": "pending",
                        "pickup_no": "A817-NEW",
                        "customer_name": "New Customer",
                        "mobile": "9000000001",
                        "customer_type": "New",
                        "previous_number_of_orders": 8,
                        "average_order_amount": "2500",
                        "pickup_created_text": "21 Apr 2026 3:03:39 PM",
                    },
                    {
                        "status_bucket": "pending",
                        "pickup_no": "A817-EXISTING",
                        "customer_name": "Existing Customer",
                        "mobile": "9000000002",
                        "customer_type": "Existing",
                        "previous_number_of_orders": 3,
                        "average_order_amount": "1234.5",
                        "pickup_created_at": datetime(2026, 4, 21, 9, 33, 39, tzinfo=timezone.utc),
                    },
                ],
                lead_change_details={
                    "created_by_bucket": [
                        {
                            "rows": [
                                {"lead_identity": {"pickup_no": "A817-NEW"}},
                                {"lead_identity": {"pickup_no": "A817-EXISTING"}},
                            ]
                        }
                    ]
                },
            )
        },
    )

    tables_html = _build_td_leads_tables_html(summary=summary)

    assert "A817</td><td>A817-EXISTING</td><td>Existing Customer</td><td>9000000002</td><td>Existing | Orders: 3 | Avg. Value: ₹1,234.50</td><td>3</td><td>₹1,234.50</td><td>21 Apr 2026 03:03:39 PM IST" in tables_html
    assert "A817</td><td>A817-NEW</td><td>New Customer</td><td>9000000001</td><td>New</td><td></td><td></td><td>21 Apr 2026 3:03:39 PM" in tables_html


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
    assert "Store Cancelled | No inventory" in tables_html
    assert "A817, Store Cancelled, 9777777777, No inventory" in tables_html
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
    assert "A817, Store Transition, 9555555555, No rider available" in tables_html
    assert "Store Cancelled | No rider available" in tables_html
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
    assert "A817, Resolved From Current Row, 9222222222, Inventory delayed" in tables_html
    assert "Stale Transition Name" not in tables_html
    assert "Store Cancelled | Inventory delayed" in tables_html


def test_is_customer_cancelled_td_lead_uses_helper_consistent_resolution() -> None:
    assert td_leads_main._is_customer_cancelled_td_lead({"reason": ""}) is True
    assert td_leads_main._is_customer_cancelled_td_lead({"reason": None}) is True
    assert td_leads_main._is_customer_cancelled_td_lead({"reason": "No inventory"}) is False
    assert td_leads_main._is_customer_cancelled_td_lead({"cancelled_flag": "customer", "reason": "No inventory"}) is True
    assert td_leads_main._is_customer_cancelled_td_lead({"cancelled_flag": "store", "reason": ""}) is False


def test_build_td_cancelled_context_combines_classification_and_reason_fallback() -> None:
    assert (
        td_leads_main._build_td_cancelled_context(row={"cancelled_flag": "store", "reason": "No inventory"})
        == "Store Cancelled | No inventory"
    )
    assert (
        td_leads_main._build_td_cancelled_context(row={"cancelled_flag": "store", "reason": ""})
        == "Store Cancelled | None"
    )


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
    def __init__(self, page: "_FakePage", selector: str) -> None:
        self.page = page
        self.selector = selector

    async def count(self) -> int:
        return 1 if self.selector in self.page.selectors_present else 0

    async def is_visible(self) -> bool:
        return self.selector in self.page.visible_selectors

    async def click(self) -> None:
        self.page.clicked.append(self.selector)
        if self.selector in td_leads_main.OVERDUE_ORDERS_MODAL_CLOSE_SELECTORS and self.page.modal_dismissal_succeeds:
            self.page.visible_selectors.discard(td_leads_main.OVERDUE_ORDERS_MODAL_SELECTOR)


class _FakeNavigationContext:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakeKeyboard:
    def __init__(self, page: "_FakePage") -> None:
        self.page = page

    async def press(self, key: str) -> None:
        self.page.pressed_keys.append(key)
        if key == "Escape" and self.page.modal_dismissal_succeeds:
            self.page.visible_selectors.discard(td_leads_main.OVERDUE_ORDERS_MODAL_SELECTOR)


class _FakePage:
    def __init__(self, *, selectors_present: set[str], url: str = "https://subs.quickdrycleaning.com/a668/App/home") -> None:
        self.selectors_present = selectors_present
        self.visible_selectors = set(selectors_present)
        self.url = url
        self.waited_selectors: list[str] = []
        self.clicked: list[str] = []
        self.goto_urls: list[str] = []
        self.waited_url_patterns: list[object] = []
        self.expect_navigation_calls = 0
        self.title_text = "Pickup Scheduler"
        self.fail_ready = False
        self.fail_click = False
        self.fail_goto = False
        self.modal_dismissal_succeeds = True
        self.pressed_keys: list[str] = []
        self.keyboard = _FakeKeyboard(self)

    async def wait_for_selector(self, selector: str, timeout: int | None = None) -> None:
        self.waited_selectors.append(selector)
        if self.fail_ready and selector in {"#drpStatus", "#grdEntry", "#grdCompleted", "#grdCanceled"}:
            raise TimeoutError("status selector timeout")

    def locator(self, selector: str) -> _FakeLocator:
        return _FakeLocator(self, selector)

    def expect_navigation(self, **kwargs) -> _FakeNavigationContext:
        self.expect_navigation_calls += 1
        return _FakeNavigationContext()

    async def click(self, selector: str) -> None:
        self.clicked.append(selector)
        if self.fail_click:
            raise TimeoutError("pointer interception timeout")
        self.url = "https://subs.quickdrycleaning.com/a668/App/New_Admin/frmHomePickUpScheduler.aspx"

    async def goto(self, url: str, **kwargs) -> None:
        self.goto_urls.append(url)
        if self.fail_goto:
            raise TimeoutError("direct scheduler URL timeout")
        self.url = "https://subs.quickdrycleaning.com/a668/App/New_Admin/frmHomePickUpScheduler.aspx"

    async def wait_for_url(self, pattern, **kwargs) -> None:
        self.waited_url_patterns.append(pattern)

    async def title(self) -> str:
        return self.title_text

    async def evaluate(self, script: str, selector: str) -> bool:
        if "modal.click()" in script and self.modal_dismissal_succeeds:
            self.visible_selectors.discard(selector)
            return True
        return "data-keyboard" in script


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
async def test_ensure_scheduler_page_dismisses_visible_overdue_orders_modal_before_click(monkeypatch: pytest.MonkeyPatch) -> None:
    close_selector = td_leads_main.OVERDUE_ORDERS_MODAL_CLOSE_SELECTORS[0]
    page = _FakePage(selectors_present={"#achrPickUp", td_leads_main.OVERDUE_ORDERS_MODAL_SELECTOR, close_selector})
    events: list[dict[str, object]] = []
    monkeypatch.setattr(
        "app.crm_downloader.td_leads_sync.main.log_event",
        lambda **kwargs: events.append(kwargs),
    )

    ok = await _ensure_scheduler_page(page, store=SimpleNamespace(store_code="A668"), logger=SimpleNamespace())

    assert ok is True
    assert page.clicked == [close_selector, "#achrPickUp"]
    assert not page.goto_urls


@pytest.mark.asyncio
async def test_ensure_scheduler_page_intercepted_click_uses_controlled_direct_url_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    page = _FakePage(selectors_present={"#achrPickUp"})
    page.fail_click = True
    events: list[dict[str, object]] = []
    monkeypatch.setattr(
        "app.crm_downloader.td_leads_sync.main.log_event",
        lambda **kwargs: events.append(kwargs),
    )

    ok = await _ensure_scheduler_page(page, store=SimpleNamespace(store_code="A668"), logger=SimpleNamespace())

    assert ok is True
    assert page.goto_urls == [td_leads_main._scheduler_url_for_store("A668")]
    assert page.waited_url_patterns
    warning = next(event for event in events if event.get("status") == "warning")
    assert warning["store_code"] == "A668"
    assert warning["modal_selector"] == td_leads_main.OVERDUE_ORDERS_MODAL_SELECTOR
    assert warning["attempted_dismissal_action"] == "not_visible"
    assert warning["fallback_action"] == td_leads_main.SCHEDULER_DIRECT_URL_FALLBACK_ACTION


@pytest.mark.asyncio
async def test_ensure_scheduler_page_modal_dismissal_and_direct_url_fallback_both_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    page = _FakePage(selectors_present={"#achrPickUp", td_leads_main.OVERDUE_ORDERS_MODAL_SELECTOR})
    page.modal_dismissal_succeeds = False
    page.fail_goto = True
    events: list[dict[str, object]] = []
    monkeypatch.setattr(
        "app.crm_downloader.td_leads_sync.main.log_event",
        lambda **kwargs: events.append(kwargs),
    )

    ok = await _ensure_scheduler_page(page, store=SimpleNamespace(store_code="A668"), logger=SimpleNamespace())

    assert ok is False
    assert page.clicked == []
    assert page.goto_urls == [td_leads_main._scheduler_url_for_store("A668")]
    warning = next(event for event in events if event.get("status") == "warning")
    assert warning["store_code"] == "A668"
    assert warning["modal_selector"] == td_leads_main.OVERDUE_ORDERS_MODAL_SELECTOR
    assert warning["attempted_dismissal_action"] == "escape_key"
    assert warning["fallback_action"] == td_leads_main.SCHEDULER_DIRECT_URL_FALLBACK_ACTION
    assert any(event.get("status") == "error" for event in events)


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
async def test_collect_status_rows_maps_created_date_header_to_created_fields_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _noop_switch_status(*args, **kwargs) -> None:
        return None

    async def _fake_scrape_grid_rows(*args, **kwargs):
        return (
            ["Pickup No.", "Customer Name", "Created Date", "Mobile", "Status"],
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
        "customer_type": "Existing",
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
                        SELECT pickup_date, pickup_created_at, pickup_time, customer_type
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
        assert stored["customer_type"] == "Existing"
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
    assert "21 Apr 2026 03:03:39 PM IST" in lead_tables_html
    assert "UTC" not in lead_tables_html
    assert "None" in lead_tables_html


@pytest.mark.asyncio
async def test_ingest_pickup_created_at_uses_fallback_without_overwriting_pickup_date(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'td_leads_created_at_all_buckets.db'}"
    rows = [
        {
            "store_code": "A668",
            "status_bucket": "pending",
            "pickup_id": "4434944-pending",
            "pickup_no": "A668-3025-pending",
            "customer_name": "Moni",
            "mobile": "9599242207",
            "pickup_date": "21 Apr 2026",
            "pickup_created_at": "21 Apr 2026 3:03:39 PM",
            "pickup_time": "11:00 AM - 1:00 PM",
        },
        {
            "store_code": "A668",
            "status_bucket": "completed",
            "pickup_id": "4434944-completed",
            "pickup_no": "A668-3025-completed",
            "customer_name": "Moni",
            "mobile": "9599242207",
            "pickup_date": "22 Apr 2026",
            "pickup_created_at": None,
            "pickup_time": "11:00 AM - 1:00 PM",
        },
        {
            "store_code": "A668",
            "status_bucket": "cancelled",
            "pickup_id": "4434944-cancelled",
            "pickup_no": "A668-3025-cancelled",
            "customer_name": "Moni",
            "mobile": "9599242207",
            "pickup_date": "23 Apr 2026",
            "pickup_created_at": None,
            "pickup_time": "11:00 AM - 1:00 PM",
        }
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
                        SELECT status_bucket, pickup_date, pickup_created_at
                        FROM crm_leads_current
                        ORDER BY status_bucket
                        """
                    )
                )
            ).mappings().all()
    finally:
        await engine.dispose()

    assert [row["status_bucket"] for row in stored_rows] == ["cancelled", "completed", "pending"]
    created_at_by_bucket = {row["status_bucket"]: row["pickup_created_at"] for row in stored_rows}
    pickup_date_by_bucket = {row["status_bucket"]: row["pickup_date"] for row in stored_rows}

    def _as_iso(value: object) -> str:
        if isinstance(value, datetime):
            return value.isoformat(sep=" ", timespec="seconds")
        return str(value)

    assert all(created_at_by_bucket[bucket] is not None for bucket in ("pending", "completed", "cancelled"))
    assert _as_iso(created_at_by_bucket["pending"]).startswith("2026-04-21 09:33:39")
    assert _as_iso(created_at_by_bucket["completed"]).startswith("2026-04-21 18:30:00")
    assert _as_iso(created_at_by_bucket["cancelled"]).startswith("2026-04-22 18:30:00")
    assert pickup_date_by_bucket == {
        "pending": "21 Apr 2026",
        "completed": "22 Apr 2026",
        "cancelled": "23 Apr 2026",
    }
    assert ingest_result.pickup_created_at_null_count == 0
    assert ingest_result.pickup_created_at_null_counts_by_bucket == {"pending": 0, "completed": 0, "cancelled": 0}


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
@pytest.mark.parametrize(
    ("has_new_leads", "has_cancelled_from_active", "reporting_mode", "run_overall_status", "expected_subject"),
    [
        (True, False, "meeting", "success", "NEW LEADS TD CRM Leads run-1 [meeting]"),
        (False, True, "day_end", "success", "NEW LEADS TD CRM Leads run-1 [day_end]"),
        (False, False, None, "failed", "TD CRM Leads run-1"),
    ],
)
async def test_td_leads_seeded_run_notification_plans_email(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    has_new_leads: bool,
    has_cancelled_from_active: bool,
    reporting_mode: str | None,
    run_overall_status: str,
    expected_subject: str,
) -> None:
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
                        '2026-04-22', :overall_status, '00:01:00', 'ok',
                        :metrics_json
                    )
                    """
                ),
                {
                    "overall_status": run_overall_status,
                    "metrics_json": json.dumps(
                        {
                            "summary_html": "<div>ok</div>",
                            "has_new_leads": has_new_leads,
                            "reporting_mode": reporting_mode,
                            "lead_change_details": {
                                "A817": {
                                    "transitions": (
                                        [
                                            {
                                                "from_status_bucket": "pending",
                                                "to_status_bucket": "cancelled",
                                                "rows": [{"pickup_no": "A817-1"}],
                                            }
                                        ]
                                        if has_cancelled_from_active
                                        else []
                                    )
                                }
                            },
                        }
                    ),
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
                        10, 'run_summary', '{{ subject_prefix }}TD CRM Leads {{ run_id }}{{ reporting_mode_suffix }}', 'Run {{ run_id }} status {{ overall_status_upper }} complete in {{ duration_human }}', 1
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
        assert sent_plans[0].subject == expected_subject
        if reporting_mode in {"day_end", "meeting"}:
            assert f"[{reporting_mode}]" in sent_plans[0].subject
        assert "00:01:00" in sent_plans[0].body
        assert "Run run-1 status" in sent_plans[0].body
        assert f"status {run_overall_status.upper()}" in sent_plans[0].body
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

    async def _fake_persist_summary(*, logger, summary, finished_at, reporting_mode=None, reporting_payload=None, reporting_schema_errors=None):
        persisted_summaries.append(summary)
        return True

    async def _fake_notify(pipeline_name, run_id):
        notifications.append((pipeline_name, run_id))
        return {"emails_planned": 0, "emails_sent": 0, "errors": []}

    async def _fake_run_store(*, browser, store, run_id, run_env, logger):
        await asyncio.sleep(0.12)
        return td_leads_main.StoreLeadResult(store_code=store.store_code, status="ok", message="ok")

    monkeypatch.setattr(
        td_leads_main,
        "_load_td_order_stores",
        lambda logger, store_codes=None: asyncio.sleep(0, result=stores),
    )
    monkeypatch.setattr(td_leads_main, "_start_run_summary", _fake_start_summary)
    monkeypatch.setattr(td_leads_main, "_persist_run_summary", _fake_persist_summary)
    monkeypatch.setattr(td_leads_main, "send_notifications_for_run", _fake_notify)
    monkeypatch.setattr(td_leads_main, "_resolve_td_leads_concurrency_settings", lambda: (2, 2, True))
    monkeypatch.setattr(td_leads_main, "_run_store", _fake_run_store)
    monkeypatch.setattr(td_leads_main, "async_playwright", lambda: _FakePlaywrightContext())
    monkeypatch.setattr(
        td_leads_main,
        "launch_browser",
        lambda playwright, logger: asyncio.sleep(0, result=_FakeBrowser()),
    )

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

    monkeypatch.setattr(
        td_leads_main,
        "_load_td_order_stores",
        lambda logger, store_codes=None: asyncio.sleep(0, result=stores),
    )
    monkeypatch.setattr(td_leads_main, "_start_run_summary", lambda logger, summary: asyncio.sleep(0))
    monkeypatch.setattr(
        td_leads_main,
        "_persist_run_summary",
        lambda logger, summary, finished_at, reporting_mode=None, reporting_payload=None, reporting_schema_errors=None: persisted_summaries.append(summary) or asyncio.sleep(0, result=False),
    )
    monkeypatch.setattr(td_leads_main, "_resolve_td_leads_concurrency_settings", lambda: (3, 3, True))
    monkeypatch.setattr(td_leads_main, "_run_store", _fake_run_store)
    monkeypatch.setattr(td_leads_main, "async_playwright", lambda: _FakePlaywrightContext())
    monkeypatch.setattr(
        td_leads_main,
        "launch_browser",
        lambda playwright, logger: asyncio.sleep(0, result=_FakeBrowser()),
    )

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

    async def _fake_run_store_worker(*, browser, store, logger, run_env, run_id, semaphore):
        if store.store_code == "B":
            raise RuntimeError("boom")
        return td_leads_main.TdLeadsStoreWorkerResult(
            store_code=store.store_code,
            result=td_leads_main.StoreLeadResult(store_code=store.store_code, status="ok"),
            queued_at=datetime.now(timezone.utc),
            queue_wait_ms=0,
            duration_ms=1,
        )

    async def _fake_notify(pipeline_name, run_id):
        notifications.append((pipeline_name, run_id))
        return {"emails_planned": 0, "emails_sent": 0, "errors": []}

    monkeypatch.setattr(
        td_leads_main,
        "_load_td_order_stores",
        lambda logger, store_codes=None: asyncio.sleep(0, result=stores),
    )
    monkeypatch.setattr(td_leads_main, "_start_run_summary", lambda logger, summary: asyncio.sleep(0))
    monkeypatch.setattr(
        td_leads_main,
        "_persist_run_summary",
        lambda logger, summary, finished_at, reporting_mode=None, reporting_payload=None, reporting_schema_errors=None: (
            persisted_summaries.append(summary) or asyncio.sleep(0, result=True)
        ),
    )
    monkeypatch.setattr(td_leads_main, "_resolve_td_leads_concurrency_settings", lambda: (2, 2, True))
    monkeypatch.setattr(td_leads_main, "_run_store_worker", _fake_run_store_worker)
    monkeypatch.setattr(td_leads_main, "send_notifications_for_run", _fake_notify)
    monkeypatch.setattr(td_leads_main, "async_playwright", lambda: _FakePlaywrightContext())
    monkeypatch.setattr(
        td_leads_main,
        "launch_browser",
        lambda playwright, logger: asyncio.sleep(0, result=_FakeBrowser()),
    )

    with pytest.raises(SystemExit) as exc_info:
        await td_leads_main.main(run_env="test", run_id="run-errors")

    assert exc_info.value.code == 1
    assert len(persisted_summaries) == 1
    assert persisted_summaries[0].store_results["A"].status == "ok"
    assert persisted_summaries[0].store_results["B"].status == "error"
    assert persisted_summaries[0].run_had_worker_exception is True
    assert persisted_summaries[0].overall_status() == "failed"
    record = persisted_summaries[0].build_record(finished_at=persisted_summaries[0].started_at)
    assert record["overall_status"] == "failed"
    assert record["metrics_json"]["run_had_worker_exception"] is True
    assert "did not return a result" in persisted_summaries[0].store_results["B"].message.lower()
    assert notifications == [("td_crm_leads_sync", "run-errors")]


@pytest.mark.asyncio
async def test_td_leads_main_exits_nonzero_when_all_store_workers_error(
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

    async def _fake_run_store_worker(*, browser, store, logger, run_env, run_id, semaphore):
        return td_leads_main.TdLeadsStoreWorkerResult(
            store_code=store.store_code,
            result=td_leads_main.StoreLeadResult(
                store_code=store.store_code,
                status="error",
                message="store failed",
            ),
            queued_at=datetime.now(timezone.utc),
            queue_wait_ms=0,
            duration_ms=1,
        )

    async def _fake_notify(pipeline_name, run_id):
        notifications.append((pipeline_name, run_id))
        return {"emails_planned": 0, "emails_sent": 0, "errors": []}

    monkeypatch.setattr(
        td_leads_main,
        "_load_td_order_stores",
        lambda logger, store_codes=None: asyncio.sleep(0, result=stores),
    )
    monkeypatch.setattr(td_leads_main, "_start_run_summary", lambda logger, summary: asyncio.sleep(0))
    monkeypatch.setattr(
        td_leads_main,
        "_persist_run_summary",
        lambda logger, summary, finished_at, reporting_mode=None, reporting_payload=None, reporting_schema_errors=None: (
            persisted_summaries.append(summary) or asyncio.sleep(0, result=True)
        ),
    )
    monkeypatch.setattr(td_leads_main, "_resolve_td_leads_concurrency_settings", lambda: (2, 2, True))
    monkeypatch.setattr(td_leads_main, "_run_store_worker", _fake_run_store_worker)
    monkeypatch.setattr(td_leads_main, "send_notifications_for_run", _fake_notify)
    monkeypatch.setattr(td_leads_main, "async_playwright", lambda: _FakePlaywrightContext())
    monkeypatch.setattr(
        td_leads_main,
        "launch_browser",
        lambda playwright, logger: asyncio.sleep(0, result=_FakeBrowser()),
    )

    with pytest.raises(SystemExit) as exc_info:
        await td_leads_main.main(run_env="test", run_id="run-all-errors")

    assert exc_info.value.code == 1
    assert len(persisted_summaries) == 1
    assert persisted_summaries[0].overall_status() == "failed"
    assert {result.status for result in persisted_summaries[0].store_results.values()} == {"error"}
    assert notifications == [("td_crm_leads_sync", "run-all-errors")]


def test_td_leads_parser_accepts_reporting_mode() -> None:
    parser = td_leads_main._build_parser()

    args = parser.parse_args(["--reporting-mode", "meeting", "--store-code", "A817"])

    assert args.reporting_mode == "meeting"
    assert args.store_codes == ["A817"]


def test_build_td_daily_reporting_and_action_required_cover_reconciliation_shapes(monkeypatch: pytest.MonkeyPatch) -> None:
    reference_now = datetime(2026, 4, 24, 9, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(td_leads_main, "aware_now", lambda tz: reference_now)
    monkeypatch.setattr(td_leads_main, "_resolve_td_open_lead_age_threshold_days", lambda: 2)

    summary = LeadsRunSummary(
        run_id="run-reporting",
        run_env="local",
        report_date=reference_now.date(),
        store_results={
            "A817": StoreLeadResult(
                store_code="A817",
                rows=[
                    {
                        "status_bucket": "pending",
                        "pickup_no": "A817-OPEN-1",
                        "customer_name": "Open Lead",
                        "mobile": "9000000001",
                        "pickup_created_at": "2026-04-21 09:33:39",
                        "source": "Meta",
                        "customer_type": "New",
                    },
                    {
                        "status_bucket": "completed",
                        "pickup_no": "A817-COMP-1",
                        "customer_name": "Matched Lead",
                        "mobile": "9000000002",
                        "pickup_created_at": "2026-04-24 07:00:00",
                        "order_number": "SO-123",
                    },
                    {
                        "status_bucket": "completed",
                        "pickup_no": "A817-COMP-ORDER-NO",
                        "customer_name": "Matched Legacy Order No Lead",
                        "mobile": "9000000004",
                        "pickup_created_at": "2026-04-24 07:00:00",
                        "order_no": "SO-124",
                    },
                    {
                        "status_bucket": "completed",
                        "pickup_no": "A817-COMP-MATCHED",
                        "customer_name": "Matched Legacy Matched Order Lead",
                        "mobile": "9000000005",
                        "pickup_created_at": "2026-04-24 07:00:00",
                        "matched_order_no": "SO-125",
                    },
                    {
                        "status_bucket": "completed",
                        "pickup_no": "A817-COMP-FLAG",
                        "customer_name": "Matched Flag Lead",
                        "mobile": "9000000006",
                        "pickup_created_at": "2026-04-24 07:00:00",
                        "order_match_found": True,
                    },
                    {
                        "status_bucket": "completed",
                        "pickup_no": "A817-COMP-HISTORY",
                        "customer_name": "Matched History Lead",
                        "mobile": "9000000007",
                        "pickup_created_at": "2026-04-24 07:00:00",
                        "previous_number_of_orders": 1,
                    },
                    {
                        "status_bucket": "completed",
                        "pickup_no": "A817-COMP-2",
                        "customer_name": "No Match Lead",
                        "mobile": "9000000003",
                        "pickup_created_at": "2026-04-24 07:00:00",
                        "source": "Walk-in",
                        "customer_type": "Existing",
                    },
                ],
            )
        },
    )

    daily_reporting = td_leads_main._build_td_daily_reporting(summary)

    assert daily_reporting["open_leads_high_age_threshold_days"] == 2
    assert daily_reporting["open_leads_high_age"][0].keys() == {
        "store_code",
        "pickup_no",
        "customer_name",
        "mobile",
        "lead_created_at",
        "lead_age_days",
        "source",
        "customer_type",
        "last_seen_status",
    }
    assert daily_reporting["open_leads_high_age"][0]["lead_age_days"] == 2

    assert len(daily_reporting["completed_leads_without_order_match"]) == 1
    assert daily_reporting["completed_leads_without_order_match"][0]["pickup_no"] == "A817-COMP-2"

    action_required_html = td_leads_main._build_td_action_required_html(daily_reporting=daily_reporting)
    assert "Action Required" in action_required_html
    assert "Open leads with high age (2+ days) (1)" in action_required_html
    assert "Completed leads without order match (1)" in action_required_html
    assert "21 Apr 2026 03:03:39 PM IST" in action_required_html
    assert "24 Apr 2026 12:30:00 PM IST" in action_required_html
    assert "UTC" not in action_required_html


def test_td_leads_default_summary_html_uses_order_history_for_completed_action_required() -> None:
    summary = LeadsRunSummary(
        run_id="run-history-reporting",
        run_env="local",
        report_date=datetime(2026, 4, 24, tzinfo=timezone.utc).date(),
        store_results={
            "A817": StoreLeadResult(
                store_code="A817",
                rows=[
                    {
                        "status_bucket": "completed",
                        "pickup_no": "A817-HIST-MATCH",
                        "customer_name": "Historical Match Lead",
                        "mobile": "9000000008",
                        "pickup_created_at": "2026-04-24 07:00:00",
                        "previous_number_of_orders": 1,
                    }
                ],
            )
        },
    )

    record = summary.build_record(finished_at=datetime(2026, 4, 24, 9, 0, tzinfo=timezone.utc), reporting_mode=None)
    summary_html = record["metrics_json"]["summary_html"]
    action_required_html = summary_html.split("<h4 style='margin:16px 0 8px 0;'>Action Required</h4>", 1)[1]

    assert "Completed leads without order match (0)" in action_required_html
    assert "A817-HIST-MATCH" not in action_required_html
    assert record["metrics_json"]["daily_reporting"]["completed_leads_without_order_match"] == []


def test_fetch_business_day_cancelled_td_leads_returns_expected_columns(tmp_path) -> None:
    async def _run() -> list[dict[str, object]]:
        database_url = f"sqlite+aiosqlite:///{tmp_path / 'td_cancelled_columns.db'}"
        engine = create_async_engine(database_url)
        try:
            async with engine.begin() as connection:
                await connection.execute(sa.text("""
                    CREATE TABLE crm_leads_current (
                        lead_uid TEXT PRIMARY KEY,
                        store_code TEXT,
                        pickup_no TEXT,
                        customer_name TEXT,
                        mobile TEXT,
                        pickup_created_at TEXT,
                        reason TEXT,
                        cancelled_flag TEXT,
                        source TEXT,
                        customer_type TEXT
                    )
                """))
                await connection.execute(sa.text("""
                    CREATE TABLE crm_leads_status_events (
                        lead_uid TEXT,
                        status_bucket TEXT,
                        scraped_at TEXT,
                        created_at TEXT
                    )
                """))
                await connection.execute(sa.text("""
                    CREATE TABLE vw_orders (
                        store_code TEXT,
                        mobile_number TEXT,
                        order_amount NUMERIC
                    )
                """))
                await connection.execute(sa.text("""
                    INSERT INTO crm_leads_current (
                        lead_uid, store_code, pickup_no, customer_name, mobile, pickup_created_at, reason, cancelled_flag, source, customer_type
                    ) VALUES ('L1', 'A001', 'A001-1', 'Alice', '9000000001', '2026-04-29 12:00:00+00:00', '', NULL, 'Meta', 'New')
                """))
                await connection.execute(sa.text("""
                    INSERT INTO crm_leads_status_events (lead_uid, status_bucket, scraped_at, created_at) VALUES
                    ('L1', 'cancelled', '2026-04-30 19:00:00+00:00', '2026-04-30 19:00:00+00:00')
                """))

            return await td_leads_main.fetch_business_day_cancelled_td_leads(
                database_url=database_url,
                reference_ts=datetime(2026, 5, 1, 1, 0, tzinfo=timezone.utc),
            )
        finally:
            await engine.dispose()

    rows = asyncio.run(_run())
    assert rows
    assert set(rows[0].keys()) >= {
        "store_code",
        "pickup_no",
        "customer_name",
        "mobile",
        "lead_created_at",
        "cancelled_at",
        "cancel_reason",
        "cancelled_flag",
        "source",
        "customer_type",
        "lead_age_days_at_cancel",
    }


@pytest.mark.asyncio
async def test_build_td_leads_reporting_payload_db_seeded_behavior_across_sections_and_modes(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'td_reporting_seeded.db'}"
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(sa.text("""
                CREATE TABLE crm_leads_current (
                    lead_uid TEXT PRIMARY KEY,
                    store_code TEXT,
                    pickup_no TEXT,
                    customer_name TEXT,
                    mobile TEXT,
                    pickup_created_at TEXT,
                    reason TEXT,
                    cancelled_flag TEXT,
                    source TEXT,
                    customer_type TEXT,
                    status_bucket TEXT
                )
            """))
            await connection.execute(sa.text("""
                CREATE TABLE crm_leads_status_events (
                    lead_uid TEXT,
                    status_bucket TEXT,
                    scraped_at TEXT,
                    created_at TEXT
                )
            """))
            await connection.execute(sa.text("""
                CREATE TABLE orders (
                    store_code TEXT,
                    mobile_number TEXT,
                    order_number TEXT,
                    order_date TEXT
                )
            """))
            await connection.execute(sa.text("""
                CREATE TABLE vw_orders (
                    store_code TEXT,
                    mobile_number TEXT,
                    order_number TEXT,
                    order_date TEXT,
                    order_amount NUMERIC
                )
            """))

            await connection.execute(sa.text("""
                INSERT INTO crm_leads_current
                (lead_uid, store_code, pickup_no, customer_name, mobile, pickup_created_at, reason, cancelled_flag, source, customer_type, status_bucket)
                VALUES
                ('L_OPEN', 'A100', 'A100-O1', 'Open Lead', '900 000 0001', '2026-04-28 10:00:00+00:00', NULL, NULL, 'Meta', 'New', 'pending'),
                ('L_DONE_MATCH', 'A100', 'A100-D1', 'Done Match', '9000000002', '2026-05-01 08:00:00+00:00', NULL, NULL, 'Web', 'Existing', 'completed'),
                ('L_DONE_NOMATCH', 'A100', 'A100-D2', 'Done No Match', '9000000999', '2026-05-01 07:30:00+00:00', NULL, NULL, 'Walk-in', 'New', 'completed'),
                ('L_DONE_MULTI', 'A100', 'A100-D3', 'Done Multi', '+91 90000 00003', '2026-04-30 08:00:00+00:00', NULL, NULL, 'Meta', 'Existing', 'completed'),
                ('L_CANCEL_TODAY', 'A100', 'A100-C1', 'Cancel Today', '9000000100', '2026-04-30 01:00:00+00:00', '', NULL, 'Meta', 'New', 'cancelled'),
                ('L_CANCEL_YDAY', 'A100', 'A100-C0', 'Cancel Yesterday', '9000000200', '2026-04-30 01:00:00+00:00', 'No stock', 'STORE', 'Meta', 'Existing', 'cancelled'),
                ('L_DONE_CAN', 'A100', 'A100-X1', 'Should Not Open', '9000000300', '2026-04-28 08:00:00+00:00', NULL, NULL, 'Meta', 'Existing', 'completed')
            """))

            await connection.execute(sa.text("""
                INSERT INTO crm_leads_status_events (lead_uid, status_bucket, scraped_at, created_at) VALUES
                ('L_OPEN', 'pending', '2026-05-01 03:00:00+00:00', '2026-05-01 03:00:00+00:00'),
                ('L_DONE_MATCH', 'completed', '2026-05-01 10:00:00+00:00', '2026-05-01 10:00:00+00:00'),
                ('L_DONE_NOMATCH', 'completed', '2026-05-01 11:00:00+00:00', '2026-05-01 11:00:00+00:00'),
                ('L_DONE_MULTI', 'completed', '2026-05-01 12:00:00+00:00', '2026-05-01 12:00:00+00:00'),
                ('L_CANCEL_TODAY', 'cancelled', '2026-05-01 09:00:00+00:00', '2026-05-01 09:00:00+00:00'),
                ('L_CANCEL_YDAY', 'cancelled', '2026-04-30 09:00:00+00:00', '2026-04-30 09:00:00+00:00'),
                ('L_DONE_CAN', 'completed', '2026-05-01 08:00:00+00:00', '2026-05-01 08:00:00+00:00')
            """))

            await connection.execute(sa.text("""
                INSERT INTO orders (store_code, mobile_number, order_number, order_date) VALUES
                ('A100', '9000000999', 'RAW-SHOULD-NOT-MATCH', '2026-05-01 11:30:00+00:00'),
                ('A100', '9000000002', 'RAW-SHOULD-NOT-BE-USED', '2026-05-01 10:30:00+00:00')
            """))
            await connection.execute(sa.text("""
                INSERT INTO vw_orders (store_code, mobile_number, order_number, order_date, order_amount) VALUES
                ('A100', '9000000002', 'SO-OLD', '2026-04-28 10:30:00+00:00', 100.00),
                ('A100', '9000000002', 'SO-100', '2026-05-01 10:30:00+00:00', 200.00),
                ('A100', '+91-90000 00003', 'SO-200', '2026-05-01 09:00:00+00:00', 300.00),
                ('A100', '9000000003', 'SO-150', '2026-05-01 08:30:00+00:00', 400.00)
            """))

        payload = await td_leads_main.build_td_leads_reporting_payload(
            database_url=database_url,
            reference_ts=datetime(2026, 5, 1, 15, 0, tzinfo=timezone.utc),
            open_leads_high_age_threshold_days=3,
        )
    finally:
        await engine.dispose()

    assert list(payload.keys()) == [
        "warning", "report_date", "reference_ts", "open_leads", "cancelled_leads_today", "completed_leads_today", "action_required"
    ]
    assert payload["warning"] is None

    assert [row["pickup_no"] for row in payload["open_leads"]] == ["A100-O1"]
    assert payload["open_leads"][0]["lead_age_days"] == 3

    assert [row["pickup_no"] for row in payload["cancelled_leads_today"]] == ["A100-C1"]
    cancelled_row = payload["cancelled_leads_today"][0]
    assert cancelled_row["lead_age_days_at_cancel"] == 1
    assert list(cancelled_row.keys()) == [
        "store_code", "pickup_no", "customer_name", "mobile", "lead_created_at", "cancelled_at", "lead_age_days_at_cancel", "cancel_reason", "cancelled_flag", "source", "customer_type", "previous_number_of_orders", "average_order_amount"
    ]

    by_pickup = {row["pickup_no"]: row for row in payload["completed_leads_today"]}
    assert by_pickup["A100-D1"]["order_match_found"] is True
    assert by_pickup["A100-D1"]["matched_order_count"] == 2
    assert by_pickup["A100-D1"]["matched_order_ids"] == ["SO-OLD", "SO-100"]
    assert "RAW-SHOULD-NOT-BE-USED" not in by_pickup["A100-D1"]["matched_order_ids"]

    assert by_pickup["A100-D2"]["order_match_found"] is False
    assert by_pickup["A100-D2"]["reconciliation_note"]

    assert by_pickup["A100-D3"]["matched_order_ids"] == ["SO-150", "SO-200"]
    assert by_pickup["A100-D3"]["first_order_date"] == "2026-05-01 08:30:00+00:00"
    assert by_pickup["A100-D3"]["last_order_date"] == "2026-05-01 09:00:00+00:00"
    assert list(by_pickup["A100-D1"].keys()) == [
        "store_code",
        "pickup_no",
        "customer_name",
        "mobile",
        "lead_created_at",
        "source",
        "customer_type",
        "last_seen_status",
        "completed_at",
        "lead_age_days_at_completion",
        "order_match_found",
        "matched_order_count",
        "matched_order_ids",
        "first_order_date",
        "last_order_date",
        "reconciliation_note",
        "previous_number_of_orders",
        "average_order_amount",
    ]

    action_required = payload["action_required"]
    assert list(action_required.keys()) == ["open_leads_high_age_threshold_days", "open_leads_high_age", "completed_without_order_match"]
    assert action_required["open_leads_high_age_threshold_days"] == 3
    assert [row["pickup_no"] for row in action_required["open_leads_high_age"]] == ["A100-O1"]
    assert [row["pickup_no"] for row in action_required["completed_without_order_match"]] == ["A100-D2", "A100-X1"]
    assert action_required["completed_without_order_match"][0]["source"] == "Walk-in"
    assert action_required["completed_without_order_match"][0]["customer_type"] == "New"
    assert action_required["completed_without_order_match"][0]["last_seen_status"] == "completed"
    assert td_leads_main._validate_td_reporting_payload_schema(payload) == []


@pytest.mark.asyncio
async def test_td_leads_reporting_enriches_customer_type_and_order_metrics_from_vw_orders(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'td_reporting_enrichment.db'}"
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(sa.text("""
                CREATE TABLE crm_leads_current (
                    lead_uid TEXT PRIMARY KEY,
                    store_code TEXT,
                    pickup_no TEXT,
                    customer_name TEXT,
                    mobile TEXT,
                    pickup_created_at TEXT,
                    reason TEXT,
                    cancelled_flag TEXT,
                    source TEXT,
                    customer_type TEXT,
                    status_bucket TEXT
                )
            """))
            await connection.execute(sa.text("""
                CREATE TABLE crm_leads_status_events (
                    lead_uid TEXT,
                    status_bucket TEXT,
                    scraped_at TEXT,
                    created_at TEXT
                )
            """))
            await connection.execute(sa.text("""
                CREATE TABLE orders (
                    store_code TEXT,
                    mobile_number TEXT,
                    order_number TEXT,
                    order_date TEXT
                )
            """))
            await connection.execute(sa.text("""
                CREATE TABLE vw_orders (
                    store_code TEXT,
                    mobile_number TEXT,
                    order_number TEXT,
                    order_date TEXT,
                    order_amount NUMERIC
                )
            """))
            await connection.execute(sa.text("""
                INSERT INTO crm_leads_current
                (lead_uid, store_code, pickup_no, customer_name, mobile, pickup_created_at, reason, cancelled_flag, source, customer_type, status_bucket)
                VALUES
                ('L_NULL_NO_ORDERS', 'A200', 'A200-NO', 'Null No Orders', '9000000001', '2026-04-28 10:00:00+00:00', NULL, NULL, 'Meta', NULL, 'pending'),
                ('L_NULL_MATCH', 'A200', 'A200-HIST', 'Null Historical', '+91-90000 00003', '2026-04-28 10:00:00+00:00', NULL, NULL, 'Meta', '', 'pending'),
                ('L_EXPLICIT_EXISTING', 'A200', 'A200-EX', 'Explicit Existing', '9000000004', '2026-05-01 08:00:00+00:00', NULL, NULL, 'Web', 'Existing', 'completed')
            """))
            await connection.execute(sa.text("""
                INSERT INTO crm_leads_status_events (lead_uid, status_bucket, scraped_at, created_at) VALUES
                ('L_NULL_NO_ORDERS', 'pending', '2026-05-01 03:00:00+00:00', '2026-05-01 03:00:00+00:00'),
                ('L_NULL_MATCH', 'pending', '2026-05-01 03:00:00+00:00', '2026-05-01 03:00:00+00:00'),
                ('L_EXPLICIT_EXISTING', 'completed', '2026-05-01 10:00:00+00:00', '2026-05-01 10:00:00+00:00')
            """))
            await connection.execute(sa.text("""
                INSERT INTO orders (store_code, mobile_number, order_number, order_date) VALUES
                ('A200', '9000000004', 'SO-EX', '2026-05-01 10:30:00+00:00')
            """))
            await connection.execute(sa.text("""
                INSERT INTO vw_orders (store_code, mobile_number, order_number, order_date, order_amount) VALUES
                ('A200', '9000000003', 'SO-HIST-1', '2026-04-01 10:00:00+00:00', 1000.00),
                ('A200', '+91-90000 00003', 'SO-HIST-2', '2026-04-02 10:00:00+00:00', 1469.00),
                ('A200', '9000000004', 'SO-EX-1', '2026-04-03 10:00:00+00:00', 1000.00),
                ('A200', '9000000004', 'SO-EX-2', '2026-05-01 10:30:00+00:00', 1469.00),
                ('A999', '9000000003', 'SO-OTHER-STORE', '2026-04-04 10:00:00+00:00', 99999.00)
            """))

        payload = await td_leads_main.build_td_leads_reporting_payload(
            database_url=database_url,
            reference_ts=datetime(2026, 5, 1, 15, 0, tzinfo=timezone.utc),
            open_leads_high_age_threshold_days=0,
        )

        async with engine.begin() as connection:
            columns = (await connection.execute(sa.text("PRAGMA table_info(crm_leads_current)"))).mappings().all()
    finally:
        await engine.dispose()

    open_by_pickup = {row["pickup_no"]: row for row in payload["open_leads"]}
    assert open_by_pickup["A200-NO"]["customer_type"] == "New"
    assert open_by_pickup["A200-NO"]["previous_number_of_orders"] == 0
    assert open_by_pickup["A200-HIST"]["customer_type"] == "Existing"
    assert open_by_pickup["A200-HIST"]["previous_number_of_orders"] == 2
    assert str(open_by_pickup["A200-HIST"]["average_order_amount"]) == "1234.5"

    completed_by_pickup = {row["pickup_no"]: row for row in payload["completed_leads_today"]}
    assert completed_by_pickup["A200-EX"]["customer_type"] == "Existing"
    assert completed_by_pickup["A200-EX"]["previous_number_of_orders"] == 2
    assert str(completed_by_pickup["A200-EX"]["average_order_amount"]) == "1234.5"

    action_required_html = td_leads_main._build_td_action_required_html(daily_reporting={
        "open_leads_high_age_threshold_days": 0,
        "open_leads_high_age": payload["action_required"]["open_leads_high_age"],
        "completed_leads_without_order_match": payload["action_required"]["completed_without_order_match"],
    })
    assert "Store Code</th><th align='left'>Pickup No</th><th align='left'>Customer Name</th><th align='left'>Mobile</th><th align='left'>Customer Type</th><th align='left'>Number of Orders</th><th align='left'>Average Order Value</th><th align='left'>Created Date/Time" in action_required_html
    assert "Lead Age (Days)" not in action_required_html
    assert "Last Seen Status" not in action_required_html
    assert "Source" not in action_required_html.split("Open leads with high age", 1)[1].split("Completed leads without order match", 1)[0]
    assert "A200-HIST</td>" in action_required_html
    assert "₹1,234.50" in action_required_html
    new_row_fragment = action_required_html.split("A200-NO", 1)[1].split("</tr>", 1)[0]
    assert "<td>New</td><td>None</td><td>None</td>" not in new_row_fragment
    assert "<td>New</td><td></td><td></td>" in new_row_fragment

    crm_current_columns = {column["name"] for column in columns}
    assert "previous_number_of_orders" not in crm_current_columns
    assert "average_order_amount" not in crm_current_columns


def test_td_leads_tables_html_highlights_cancelled_existing_rows_in_normal_run() -> None:
    summary = LeadsRunSummary(
        run_id="run-cancelled-existing-highlight",
        run_env="local",
        report_date=datetime(2026, 4, 24, tzinfo=timezone.utc).date(),
        store_results={
            "A817": StoreLeadResult(
                store_code="A817",
                rows=[
                    {
                        "status_bucket": "cancelled",
                        "pickup_no": "A817-CAN-EX",
                        "pickup_id": "CAN-EX",
                        "customer_name": "Existing Cancelled",
                        "mobile": "9000000100",
                        "reason": "No inventory",
                        "customer_type": "Existing",
                    }
                ],
                status_transitions=[{"pickup_no": "A817-CAN-EX", "to_status_bucket": "cancelled"}],
            )
        },
    )

    tables_html = _build_td_leads_tables_html(summary=summary)

    assert "Leads Marked as Cancelled (1 transitions this run)" in tables_html
    assert "<tr style='color:#b00020; font-weight:600;'>" in tables_html



@pytest.mark.asyncio
async def test_td_leads_order_history_enrichment_adds_safe_diagnostics_for_existing_zero_and_matches(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'td_order_history_diagnostics.db'}"
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(sa.text("""
                CREATE TABLE vw_orders (
                    store_code TEXT,
                    mobile_number TEXT,
                    order_amount NUMERIC
                )
            """))
            await connection.execute(sa.text("""
                INSERT INTO vw_orders (store_code, mobile_number, order_amount) VALUES
                ('A200', '+91 98765 43210', 1000.00),
                ('A200', '9876543210', 1500.00),
                ('A200', '9000000009', 999.00),
                ('A999', '9876543210', 99999.00)
            """))

        rows = [
            {
                "store_code": "A200",
                "pickup_no": "A200-MATCH",
                "mobile": "+91 98765 43210",
                "customer_type": None,
            },
            {
                "store_code": "A200",
                "pickup_no": "A200-ZERO",
                "mobile": "9000000002",
                "customer_type": "Existing",
            },
        ]

        await td_leads_main._enrich_td_lead_rows_with_order_history(database_url=database_url, rows=rows)
    finally:
        await engine.dispose()

    matched_row, zero_row = rows
    assert matched_row["previous_number_of_orders"] == 2
    assert matched_row["customer_type"] == "Existing"
    assert matched_row["order_history_lookup_store_code"] == "A200"
    assert matched_row["order_history_lookup_mobile_last4"] == "3210"
    assert matched_row["order_history_lookup_normalized_mobile_last4"] == "3210"
    assert matched_row["order_history_candidate_rows_for_store"] == 3
    assert matched_row["order_history_matched_rows_for_mobile"] == 2

    assert zero_row["customer_type"] == "Existing"
    assert zero_row["previous_number_of_orders"] == 0
    assert zero_row["order_history_warning_marker"] == "existing_customer_zero_order_history"
    assert "Existing | Order history not matched" in _build_td_new_lead_payload(store_code="A200", row=zero_row)
    assert "Orders: 0" not in _build_td_new_lead_payload(store_code="A200", row=zero_row)
    assert zero_row["order_history_candidate_rows_for_store"] == 3
    assert zero_row["order_history_matched_rows_for_mobile"] == 0

    diagnostic_fields = [
        "order_history_lookup_store_code",
        "order_history_lookup_mobile_last4",
        "order_history_lookup_normalized_mobile_last4",
        "order_history_candidate_rows_for_store",
        "order_history_matched_rows_for_mobile",
        "order_history_warning_marker",
    ]
    diagnostics_json = json.dumps(
        [{field: row.get(field) for field in diagnostic_fields if field in row} for row in rows],
        sort_keys=True,
    )
    assert "9876543210" not in diagnostics_json
    assert "9000000002" not in diagnostics_json
    assert "3210" in diagnostics_json
    assert "0002" in diagnostics_json

    summary = LeadsRunSummary(
        run_id="run-order-history-diagnostics",
        run_env="test",
        report_date=datetime(2026, 5, 1, tzinfo=timezone.utc).date(),
        store_results={"A200": StoreLeadResult(store_code="A200", rows=rows)},
    )
    record = summary.build_record(finished_at=datetime(2026, 5, 1, 1, 0, tzinfo=timezone.utc))
    warning_markers = record["metrics_json"]["order_history_warning_markers"]
    assert warning_markers == [
        {
            "marker": "existing_customer_zero_order_history",
            "store_code": "A200",
            "pickup_no": "A200-ZERO",
            "mobile_last4": "0002",
            "previous_number_of_orders": 0,
            "order_history_candidate_rows_for_store": 3,
            "order_history_matched_rows_for_mobile": 0,
        }
    ]
    assert "9000000002" not in json.dumps(warning_markers, sort_keys=True)


@pytest.mark.asyncio
async def test_td_leads_order_history_enrichment_keeps_null_customer_zero_matches_new_without_warning(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'td_order_history_null_customer_zero.db'}"
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(sa.text("""
                CREATE TABLE vw_orders (
                    store_code TEXT,
                    mobile_number TEXT,
                    order_amount NUMERIC
                )
            """))
            await connection.execute(sa.text("""
                INSERT INTO vw_orders (store_code, mobile_number, order_amount) VALUES
                ('A200', '9000000009', 999.00)
            """))

        rows = [
            {
                "store_code": "A200",
                "pickup_no": "A200-NEW-ZERO",
                "mobile": "9000000002",
                "customer_type": None,
            }
        ]

        await td_leads_main._enrich_td_lead_rows_with_order_history(database_url=database_url, rows=rows)
    finally:
        await engine.dispose()

    row = rows[0]
    assert row["customer_type"] == "New"
    assert row["previous_number_of_orders"] == 0
    assert "order_history_warning_marker" not in row
    assert row["order_history_candidate_rows_for_store"] == 1
    assert row["order_history_matched_rows_for_mobile"] == 0


@pytest.mark.asyncio
async def test_td_leads_order_history_enrichment_counts_zero_value_match_without_warning(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'td_order_history_zero_value_match.db'}"
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(sa.text("""
                CREATE TABLE vw_orders (
                    store_code TEXT,
                    mobile_number TEXT,
                    order_amount NUMERIC
                )
            """))
            await connection.execute(sa.text("""
                INSERT INTO vw_orders (store_code, mobile_number, order_amount) VALUES
                ('A200', '+91 98765 43210', 0.00)
            """))

        rows = [
            {
                "store_code": "A200",
                "pickup_no": "A200-ZERO-VALUE",
                "mobile": "9876543210",
                "customer_type": "Existing",
            }
        ]

        await td_leads_main._enrich_td_lead_rows_with_order_history(database_url=database_url, rows=rows)
    finally:
        await engine.dispose()

    row = rows[0]
    assert row["customer_type"] == "Existing"
    assert row["previous_number_of_orders"] == 1
    assert str(row["average_order_amount"]) == "0"
    assert "order_history_warning_marker" not in row
    assert "Existing | Orders: 1 | Avg. Value: ₹0.00" in _build_td_new_lead_payload(store_code="A200", row=row)


@pytest.mark.asyncio
async def test_td_leads_order_history_enrichment_raises_when_vw_orders_missing(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'td_missing_vw_orders.db'}"
    row = {
        "store_code": "A200",
        "pickup_no": "A200-MISSING-VW",
        "mobile": "9000000001",
        "customer_type": None,
    }

    with pytest.raises(sa.exc.SQLAlchemyError, match="vw_orders"):
        await td_leads_main._enrich_td_lead_rows_with_order_history(database_url=database_url, rows=[row])

    assert row["customer_type"] is None
    assert "previous_number_of_orders" not in row
    assert "average_order_amount" not in row


@pytest.mark.asyncio
async def test_td_leads_order_history_enrichment_raises_when_order_amount_missing(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'td_missing_order_amount.db'}"
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(sa.text("""
                CREATE TABLE vw_orders (
                    store_code TEXT,
                    mobile_number TEXT,
                    order_number TEXT,
                    order_date TEXT
                )
            """))
            await connection.execute(sa.text("""
                INSERT INTO vw_orders (store_code, mobile_number, order_number, order_date) VALUES
                ('A200', '9000000001', 'SO-1', '2026-05-01 10:30:00+00:00')
            """))

        row = {
            "store_code": "A200",
            "pickup_no": "A200-MISSING-AMOUNT",
            "mobile": "9000000001",
            "customer_type": None,
        }

        with pytest.raises(sa.exc.SQLAlchemyError, match="order_amount"):
            await td_leads_main._enrich_td_lead_rows_with_order_history(database_url=database_url, rows=[row])
    finally:
        await engine.dispose()

    assert row["customer_type"] is None
    assert "previous_number_of_orders" not in row
    assert "average_order_amount" not in row


@pytest.mark.asyncio
async def test_build_td_leads_reporting_payload_matches_completed_leads_from_vw_orders_not_orders(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'td_reporting_vw_orders_matching.db'}"
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(sa.text("""
                CREATE TABLE crm_leads_current (
                    lead_uid TEXT PRIMARY KEY,
                    store_code TEXT,
                    pickup_no TEXT,
                    customer_name TEXT,
                    mobile TEXT,
                    pickup_created_at TEXT,
                    reason TEXT,
                    cancelled_flag TEXT,
                    source TEXT,
                    customer_type TEXT,
                    status_bucket TEXT
                )
            """))
            await connection.execute(sa.text("""
                CREATE TABLE crm_leads_status_events (
                    lead_uid TEXT,
                    status_bucket TEXT,
                    scraped_at TEXT,
                    created_at TEXT
                )
            """))
            await connection.execute(sa.text("""
                CREATE TABLE orders (
                    store_code TEXT,
                    mobile_number TEXT,
                    order_number TEXT,
                    order_date TEXT
                )
            """))
            await connection.execute(sa.text("""
                CREATE TABLE vw_orders (
                    store_code TEXT,
                    mobile_number TEXT,
                    order_number TEXT,
                    order_date TEXT,
                    order_amount NUMERIC
                )
            """))
            await connection.execute(sa.text("""
                INSERT INTO crm_leads_current
                (lead_uid, store_code, pickup_no, customer_name, mobile, pickup_created_at, reason, cancelled_flag, source, customer_type, status_bucket)
                VALUES
                ('L_FORMATTED_MATCH', 'A100', 'A100-FMT', 'Formatted Match', '+91 98765-43210', '2026-05-01 08:00:00+00:00', NULL, NULL, 'Web', 'New', 'completed'),
                ('L_RAW_ONLY', 'A100', 'A100-RAW', 'Raw Only', '9000000999', '2026-05-01 08:00:00+00:00', NULL, NULL, 'Web', 'New', 'completed')
            """))
            await connection.execute(sa.text("""
                INSERT INTO crm_leads_status_events (lead_uid, status_bucket, scraped_at, created_at) VALUES
                ('L_FORMATTED_MATCH', 'completed', '2026-05-01 10:00:00+00:00', '2026-05-01 10:00:00+00:00'),
                ('L_RAW_ONLY', 'completed', '2026-05-01 10:05:00+00:00', '2026-05-01 10:05:00+00:00')
            """))
            await connection.execute(sa.text("""
                INSERT INTO orders (store_code, mobile_number, order_number, order_date) VALUES
                ('A100', '9000000999', 'RAW-ONLY-SHOULD-NOT-MATCH', '2026-05-01 10:30:00+00:00'),
                ('A100', '9876543210', 'RAW-FORMATTED-SHOULD-NOT-BE-USED', '2026-05-01 10:30:00+00:00')
            """))
            await connection.execute(sa.text("""
                INSERT INTO vw_orders (store_code, mobile_number, order_number, order_date, order_amount) VALUES
                ('A100', '(+91) 98765 43210', 'VW-FORMATTED-HISTORICAL', '2026-04-20 10:30:00+00:00', 100.00)
            """))

        payload = await td_leads_main.build_td_leads_reporting_payload(
            database_url=database_url,
            reference_ts=datetime(2026, 5, 1, 15, 0, tzinfo=timezone.utc),
        )
    finally:
        await engine.dispose()

    completed_by_pickup = {row["pickup_no"]: row for row in payload["completed_leads_today"]}
    formatted_match = completed_by_pickup["A100-FMT"]
    assert formatted_match["order_match_found"] is True
    assert formatted_match["matched_order_count"] == 1
    assert formatted_match["matched_order_ids"] == ["VW-FORMATTED-HISTORICAL"]
    assert formatted_match["first_order_date"] == "2026-04-20 10:30:00+00:00"
    assert formatted_match["last_order_date"] == "2026-04-20 10:30:00+00:00"

    raw_only = completed_by_pickup["A100-RAW"]
    assert raw_only["order_match_found"] is False
    assert raw_only["matched_order_count"] == 0
    assert raw_only["matched_order_ids"] == []
    assert [row["pickup_no"] for row in payload["action_required"]["completed_without_order_match"]] == ["A100-RAW"]


@pytest.mark.asyncio
async def test_build_td_leads_reporting_payload_matches_real_failing_mobile_formats(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'td_reporting_real_mobile_formats.db'}"
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(sa.text("""
                CREATE TABLE crm_leads_current (
                    lead_uid TEXT PRIMARY KEY,
                    store_code TEXT,
                    pickup_no TEXT,
                    customer_name TEXT,
                    mobile TEXT,
                    pickup_created_at TEXT,
                    reason TEXT,
                    cancelled_flag TEXT,
                    source TEXT,
                    customer_type TEXT,
                    status_bucket TEXT
                )
            """))
            await connection.execute(sa.text("""
                CREATE TABLE crm_leads_status_events (
                    lead_uid TEXT,
                    status_bucket TEXT,
                    scraped_at TEXT,
                    created_at TEXT
                )
            """))
            await connection.execute(sa.text("""
                CREATE TABLE vw_orders (
                    store_code TEXT,
                    mobile_number TEXT,
                    order_number TEXT,
                    order_date TEXT,
                    order_amount NUMERIC
                )
            """))
            await connection.execute(sa.text("""
                INSERT INTO crm_leads_current
                (lead_uid, store_code, pickup_no, customer_name, mobile, pickup_created_at, reason, cancelled_flag, source, customer_type, status_bucket)
                VALUES
                ('L_PROD_FORMAT', 'A668', 'A668-PROD-MOBILE', 'Production Format', ' 9.599242207E9 ', '2026-05-01 08:00:00+00:00', NULL, NULL, 'TD CRM', 'New', 'completed')
            """))
            await connection.execute(sa.text("""
                INSERT INTO crm_leads_status_events (lead_uid, status_bucket, scraped_at, created_at) VALUES
                ('L_PROD_FORMAT', 'completed', '2026-05-01 10:00:00+00:00', '2026-05-01 10:00:00+00:00')
            """))
            await connection.execute(sa.text("""
                INSERT INTO vw_orders (store_code, mobile_number, order_number, order_date, order_amount) VALUES
                ('A668', '9599242207.0', 'VW-PROD-MOBILE-MATCH', '2026-05-01 10:30:00+00:00', 100.00)
            """))

        payload = await td_leads_main.build_td_leads_reporting_payload(
            database_url=database_url,
            reference_ts=datetime(2026, 5, 1, 15, 0, tzinfo=timezone.utc),
        )
    finally:
        await engine.dispose()

    completed_row = payload["completed_leads_today"][0]
    assert completed_row["pickup_no"] == "A668-PROD-MOBILE"
    assert completed_row["order_match_found"] is True
    assert completed_row["matched_order_count"] == 1
    assert completed_row["matched_order_ids"] == ["VW-PROD-MOBILE-MATCH"]
    assert payload["action_required"]["completed_without_order_match"] == []

    diagnostics = _build_td_mobile_match_debug_diagnostics(
        lead_mobile=" 9.599242207E9 ",
        order_mobile="9599242207.0",
    )
    assert diagnostics["original_lead_mobile_masked"] == "***2207"
    assert diagnostics["normalized_lead_mobile_last4"] == "2207"
    assert diagnostics["original_order_mobile_masked"] == "***2207"
    assert diagnostics["normalized_order_mobile_last4"] == "2207"


@pytest.mark.parametrize("mode", ["meeting", "day_end"])
def test_td_leads_run_summary_record_includes_frozen_daily_reporting_for_reporting_modes(mode: str) -> None:
    summary = LeadsRunSummary(run_id="run-reports", run_env="local", report_date=datetime(2026, 5, 1, tzinfo=timezone.utc).date())

    record = summary.build_record(
        finished_at=datetime(2026, 5, 1, 16, 0, tzinfo=timezone.utc),
        reporting_mode=mode,
        reporting_payload={
            "cancelled_leads_today": [{"pickup_no": "A100-C1"}],
            "action_required": {
                "open_leads_high_age_threshold_days": 3,
                "open_leads_high_age": [],
                "completed_without_order_match": [],
            },
        },
    )
    frozen = record["metrics_json"]["frozen_day_report_datasets"]

    assert frozen["reporting_mode"] == mode
    assert frozen["daily_reporting"] is not None
    assert list(frozen["daily_reporting"].keys()) == [
        "open_leads_high_age_threshold_days", "open_leads_high_age", "cancelled_leads_today", "existing_customer_cancelled_current_state", "completed_leads_without_order_match"
    ]
    assert "action_required" in frozen and isinstance(frozen["action_required"], str)


def test_td_leads_run_summary_record_without_reporting_mode_keeps_backward_compatibility() -> None:
    summary = LeadsRunSummary(run_id="run-default", run_env="local", report_date=datetime(2026, 5, 1, tzinfo=timezone.utc).date())
    record = summary.build_record(finished_at=datetime(2026, 5, 1, 16, 0, tzinfo=timezone.utc), reporting_mode=None)

    assert record["metrics_json"]["frozen_day_report_datasets"] is None
    assert "summary_html" in record["metrics_json"]


@pytest.mark.asyncio
async def test_fetch_existing_customer_cancelled_current_state_td_leads_filters_and_ignores_business_day_window(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'td_existing_cancelled_current_state.db'}"
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(sa.text("""
                CREATE TABLE crm_leads_current (
                    store_code TEXT,
                    pickup_no TEXT,
                    customer_name TEXT,
                    mobile TEXT,
                    pickup_created_at TEXT,
                    reason TEXT,
                    cancelled_flag TEXT,
                    customer_type TEXT,
                    status_bucket TEXT
                )
            """))
            await connection.execute(sa.text("""
                CREATE TABLE vw_orders (
                    store_code TEXT,
                    mobile_number TEXT,
                    order_amount NUMERIC
                )
            """))
            await connection.execute(sa.text("""
                INSERT INTO crm_leads_current (store_code,pickup_no,customer_name,mobile,pickup_created_at,reason,cancelled_flag,customer_type,status_bucket) VALUES
                ('A001','A001-X1','Existing Cancelled','9000000001','2020-01-01 00:00:00+00:00','Customer dropped',NULL,'Existing','cancelled'),
                ('A001','A001-X2','New Cancelled','9000000002','2026-05-01 00:00:00+00:00','x',NULL,'New','cancelled'),
                ('A001','A001-X3','Existing Open','9000000003','2026-05-01 00:00:00+00:00','x',NULL,'Existing','pending')
            """))
            await connection.execute(sa.text("""
                INSERT INTO vw_orders (store_code,mobile_number,order_amount) VALUES
                ('A001','9000000001',100),('A001','9000000001',200)
            """))

        rows = await td_leads_main.fetch_existing_customer_cancelled_current_state_td_leads(database_url=database_url)
    finally:
        await engine.dispose()

    assert [row['pickup_no'] for row in rows] == ['A001-X1']
    assert rows[0]['previous_number_of_orders'] == 2
    assert rows[0]['average_order_amount'] == 150.0


def test_build_td_leads_summary_html_places_existing_cancelled_current_state_first_for_meeting_and_day_end() -> None:
    summary = LeadsRunSummary(run_id='run-1', run_env='test', report_date=datetime(2026, 5, 1, tzinfo=timezone.utc).date(), started_at=datetime(2026, 5, 1, 9, 0, tzinfo=timezone.utc))
    summary.store_results['A001'] = StoreLeadResult(store_code='A001', rows=[{'status_bucket':'pending','pickup_no':'A001-1','customer_name':'A','mobile':'9','pickup_created_at':'2026-05-01 09:00:00+00:00','source':'Meta'}], status_counts={'pending':1})

    reporting_payload = {
        'cancelled_leads_today': [{'store_code':'A001','pickup_no':'A001-C1'}],
        'existing_customer_cancelled_current_state': [
            {'store_code':'A001','pickup_no':'A001-EC1','customer_name':'Existing','mobile':'9000000001','cancel_reason':'Customer dropped','cancelled_flag':'customer','customer_type':'Existing','lead_created_at':'2020-01-01 00:00:00+00:00','previous_number_of_orders':0,'average_order_amount':None}
        ],
        'action_required': {'open_leads_high_age_threshold_days':2,'open_leads_high_age':[],'completed_without_order_match':[]}
    }

    for mode in ('meeting','day_end'):
        html = _build_td_leads_summary_html(summary=summary, duration_human='00:00:10', reporting_mode=mode, reporting_payload=reporting_payload)
        assert 'Existing Customer Leads Marked as Cancelled (All-time/current-state) (1)' in html
        assert html.index('Existing Customer Leads Marked as Cancelled (All-time/current-state) (1)') < html.index('Action Required')
