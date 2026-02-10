from __future__ import annotations

from datetime import date, datetime, timezone

from app.crm_downloader.uc_orders_sync.main import StoreOutcome, UcOrdersDiscoverySummary
from app.dashboard_downloader.notifications import (
    _build_fact_rows,
    _build_uc_orders_context,
    _format_fact_sections_text,
    _prepare_ingest_remarks,
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


def test_uc_orders_archive_warning_and_timeout_partial_render_in_summary_and_email() -> None:
    summary = UcOrdersDiscoverySummary(
        run_id="run-uc-regression",
        run_env="test",
        report_date=date(2024, 6, 10),
        report_end_date=date(2024, 6, 10),
        started_at=datetime(2024, 6, 11, tzinfo=timezone.utc),
    )
    summary.store_codes = ["UC567", "UC610"]

    summary.record_store(
        "UC567",
        StoreOutcome(
            status="warning",
            message="Publish completed with warnings",
            rows_downloaded=999,
            final_rows=7,
            base_rows_extracted=42,
            warning_count=3,
            warning_rows=[{"order_number": "O-1", "ingest_remarks": "publish warning"}],
            reason_codes=["preflight_parent_coverage_low"],
            stage_metrics={
                "archive_ingest": {
                    "files": {
                        "base": {"parsed": 10, "inserted": 9},
                        "order_details": {"parsed": 3, "inserted": 2},
                    }
                },
                "archive_publish": {
                    "payment_details_to_sales": {
                        "warnings": 1,
                        "reason_codes": {"preflight_parent_coverage_low": 1},
                        "preflight_warning": "parent coverage dropped",
                    }
                },
            },
        ),
    )
    summary.record_store(
        "UC610",
        StoreOutcome(
            status="warning",
            message="Timeout while loading Archive Orders page",
            skip_reason="timeout",
            warning_count=1,
            reason_codes=["timeout_window_skipped"],
        ),
    )

    record = summary.build_record(finished_at=datetime(2024, 6, 11, 0, 5, tzinfo=timezone.utc))
    payload_stores = (record["metrics_json"]["notification_payload"]["stores"])
    store_uc567 = next(store for store in payload_stores if store["store_code"] == "UC567")
    store_uc610 = next(store for store in payload_stores if store["store_code"] == "UC610")

    assert store_uc567["rows_downloaded"] == 42
    assert store_uc567["rows_ingested"] == 13
    assert store_uc610["status"] == "partial"

    context = _build_uc_orders_context(record)

    assert context["store_status_counts"]["partial"] == 1
    assert "preflight_parent_coverage_low" in context["warnings_text"]
    assert "timeout_window_skipped" in context["warnings_text"]
    assert "rows_downloaded: 42" in context["summary_text"]
    assert "rows_ingested: 13" in context["summary_text"]
