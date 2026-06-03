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
        "row_coercion_failures": 1,
    }
    assert {breach["code"] for breach in data_quality["breaches"]} == {
        "navigation_failures",
        "invalid_csv_downloads",
        "row_coercion_failures",
    }
    assert "Data quality warning thresholds:" in record["summary_text"]


def _record_terminal_bootstrap_retry(aggregator: RunAggregator) -> None:
    aggregator.record_log_event(
        {
            "phase": "download",
            "status": "error",
            "message": "navigation attempt failed",
            "store_code": "A001",
            "extras": {
                "attempt": 3,
                "max_attempts": 3,
                "target_url": "https://example.test/home",
                "error_type": "TimeoutError",
                "error_message": "internet disconnected",
                "retry_context": "bootstrap_session_probe",
            },
        }
    )


def _record_successful_bootstrap_recovery(aggregator: RunAggregator) -> None:
    aggregator.record_log_event(
        {
            "phase": "download",
            "status": "warning",
            "message": "bootstrap connectivity recovered successfully",
            "store_code": "A001",
            "extras": {
                "recovered_retry_context": "bootstrap_session_probe",
                "recovery_strategy": "fresh_login",
            },
        }
    )


def test_run_aggregator_marks_recovered_bootstrap_disconnect_as_warning_after_complete_audit() -> None:
    aggregator = RunAggregator(run_id="run-recovered", run_env="test", store_codes=["A001"])

    _record_terminal_bootstrap_retry(aggregator)
    _record_successful_bootstrap_recovery(aggregator)
    aggregator.record_log_event(
        {
            "phase": "download",
            "status": "ok",
            "message": "store download completed",
            "store_code": "A001",
        }
    )
    aggregator.record_log_event({"phase": "ingest", "status": "ok", "message": "csv ingest summary"})
    aggregator.record_log_event({"phase": "audit", "status": "ok", "message": "audit complete"})

    record = aggregator.build_record(finished_at=datetime.now(timezone.utc))
    retry_telemetry = record["metrics_json"]["bootstrap_retry_telemetry"]

    assert record["overall_status"] == "warning"
    assert retry_telemetry["recovered_count"] == 1
    assert retry_telemetry["incidents"][0]["recovered"] is True
    assert retry_telemetry["incidents"][0]["retry"]["error_message"] == "internet disconnected"
    assert "Bootstrap connectivity recovered successfully after terminal retry exhaustion" in record["summary_text"]


def test_run_aggregator_preserves_error_when_bootstrap_retry_exhaustion_does_not_recover() -> None:
    aggregator = RunAggregator(run_id="run-exhausted", run_env="test", store_codes=["A001"])

    _record_terminal_bootstrap_retry(aggregator)

    record = aggregator.build_record(finished_at=datetime.now(timezone.utc))
    retry_telemetry = record["metrics_json"]["bootstrap_retry_telemetry"]

    assert record["overall_status"] == "error"
    assert retry_telemetry["recovered_count"] == 0
    assert retry_telemetry["incidents"][0]["recovered"] is False
    assert retry_telemetry["incidents"][0]["status"] == "error"


def test_run_aggregator_preserves_error_for_partial_bucket_failure_after_bootstrap_recovery() -> None:
    aggregator = RunAggregator(run_id="run-partial", run_env="test", store_codes=["A001"])

    _record_terminal_bootstrap_retry(aggregator)
    _record_successful_bootstrap_recovery(aggregator)
    aggregator.record_log_event(
        {
            "phase": "download",
            "status": "error",
            "message": "request failed for repeat_customers",
            "store_code": "A001",
            "bucket": "repeat_customers",
        }
    )

    record = aggregator.build_record(finished_at=datetime.now(timezone.utc))

    assert record["overall_status"] == "error"
    assert record["metrics_json"]["bootstrap_retry_telemetry"]["recovered_count"] == 1
    assert record["phases_json"]["download"]["error"] == 1


def test_repeat_customer_identity_exclusions_do_not_mark_run_as_warning() -> None:
    aggregator = RunAggregator(run_id="run-repeat-customers", run_env="test", store_codes=["A001"])

    aggregator.record_log_event(
        {
            "phase": "ingest",
            "status": "info",
            "message": "legacy skipped-required payload",
            "bucket": "repeat_customers",
            "skipped_required_rows": {
                "total": 2,
                "details": [
                    {
                        "bucket": "repeat_customers",
                        "column": "mobile_no",
                        "store_code": "A001",
                        "count": 2,
                        "mobile_no": "customer-sensitive-value",
                    }
                ],
            },
        }
    )
    aggregator.record_log_event(
        {
            "phase": "ingest",
            "status": "info",
            "message": "repeat-customer rows excluded by identity validation",
            "bucket": "repeat_customers",
            "skipped_repeat_customer_identity_rows": {
                "total": 2,
                "details": [{"store_code": "A001", "count": 2}],
            },
        }
    )

    record = aggregator.build_record(finished_at=datetime.now(timezone.utc))

    assert record["overall_status"] == "ok"
    assert record["metrics_json"]["data_quality_warnings"]["counts"] == {}
    assert "mobile" not in record["summary_text"].lower()
    assert "customer-sensitive-value" not in str(record)
