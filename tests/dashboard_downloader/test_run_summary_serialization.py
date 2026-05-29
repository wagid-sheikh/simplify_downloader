from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from app.dashboard_downloader.run_summary import RunAggregator, _normalize_json_for_db


def test_normalize_json_for_db_converts_nested_decimals() -> None:
    payload = {
        "metrics": {
            "amount": Decimal("12.34"),
            "stores": [
                {"code": "A1", "totals": {"tax": Decimal("1.23")}},
                Decimal("0.5"),
            ],
        }
    }

    normalized = _normalize_json_for_db(payload)

    assert normalized["metrics"]["amount"] == 12.34
    assert normalized["metrics"]["stores"][0]["totals"]["tax"] == 1.23
    assert normalized["metrics"]["stores"][1] == 0.5


def test_run_aggregator_surfaces_dashboard_data_quality_threshold_breaches() -> None:
    aggregator = RunAggregator(run_id="run-quality", run_env="test", store_codes=["A001"])

    for attempt in range(1, 4):
        aggregator.record_log_event(
            {
                "phase": "download",
                "status": "warning",
                "message": "navigation attempt failed",
                "store_code": "A001",
                "extras": {
                    "attempt": attempt,
                    "target_url": "https://example.test/dashboard",
                },
            }
        )
    aggregator.record_log_event(
        {
            "phase": "download",
            "status": "warning",
            "message": "discarding invalid CSV download for repeat_customers",
            "store_code": "A001",
            "extras": {"reason": "missing header row"},
        }
    )
    aggregator.record_log_event(
        {
            "phase": "ingest",
            "status": "warning",
            "message": (
                "rows skipped due to missing required values: "
                "repeat_customers/mobile_no/A001-2026-05-29-2"
            ),
            "bucket": "repeat_customers",
            "skipped_required_rows": {
                "total": 2,
                "details": [
                    {
                        "bucket": "repeat_customers",
                        "column": "mobile_no",
                        "store_code": "A001",
                        "report_date": "2026-05-29",
                        "count": 2,
                    }
                ],
            },
        }
    )
    aggregator.record_log_event(
        {
            "phase": "ingest",
            "status": "warning",
            "message": "failed to coerce csv row",
            "bucket": "nonpackage_all",
            "row_index": 7,
            "error": "Missing required values for: order_date",
        }
    )
    aggregator.record_log_event(
        {
            "phase": "ingest",
            "status": "warning",
            "message": "csv ingest summary",
            "bucket": "nonpackage_all",
            "counts": {
                "total_rows": 2,
                "coerced_rows": 1,
                "failed_rows": 1,
                "skipped_rows": 0,
            },
        }
    )

    record = aggregator.build_record(finished_at=datetime.now(timezone.utc))
    data_quality = record["metrics_json"]["data_quality_warnings"]

    assert data_quality["counts"] == {
        "navigation_failures": 3,
        "invalid_csv_downloads": 1,
        "skipped_required_rows": 2,
        "row_coercion_failures": 1,
    }
    assert {breach["code"] for breach in data_quality["breaches"]} == {
        "navigation_failures",
        "invalid_csv_downloads",
        "skipped_required_rows",
        "row_coercion_failures",
    }
    assert "Data quality warning thresholds:" in record["summary_text"]
    assert "rows skipped due to missing required fields: 2 observed" in record["summary_text"]
