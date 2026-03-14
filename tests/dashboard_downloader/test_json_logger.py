from __future__ import annotations

import io
import json

from app.dashboard_downloader.json_logger import JsonLogger


def test_json_logger_truncates_oversized_payloads() -> None:
    stream = io.StringIO()
    logger = JsonLogger(run_id="run-1", stream=stream, log_file_path=None)
    logger.max_event_bytes = 220

    logger.info(
        phase="compare",
        message="payload",
        auth_diagnostics={"blob": "x" * 2000},
        api_request_metadata=[{"page_number": 1, "rows_in_page": 500, "cumulative_rows": 500}],
    )

    line = stream.getvalue().strip()
    payload = json.loads(line)
    assert payload["log_truncated"] is True
    assert payload["log_truncated_reason"] == "max_event_bytes_exceeded"


def test_json_logger_emits_full_payload_when_within_limit() -> None:
    stream = io.StringIO()
    logger = JsonLogger(run_id="run-2", stream=stream, log_file_path=None)
    logger.max_event_bytes = 10000

    logger.info(phase="api", message="ok", rows_total=12)
    payload = json.loads(stream.getvalue().strip())

    assert payload["rows_total"] == 12
    assert payload.get("log_truncated") is None


def test_json_logger_suppresses_status_normalized_warnings_only() -> None:
    stream = io.StringIO()
    logger = JsonLogger(run_id="run-3", stream=stream, log_file_path=None)

    logger.warn(
        phase="archive_ingest",
        message="row warning",
        ingest_remarks="status_normalized:UPI/Wallet App->UPI_WALLET_APP",
        source_file="base.xlsx",
    )
    logger.warn(
        phase="archive_ingest",
        message="row warning",
        ingest_remarks="phone_format_warning",
        source_file="base.xlsx",
    )
    logger.error(
        phase="archive_ingest",
        message="status_normalized:still visible on errors",
        source_file="base.xlsx",
    )

    events = [json.loads(line) for line in stream.getvalue().strip().splitlines()]
    assert len(events) == 2
    assert events[0]["status"] == "warning"
    assert events[0]["ingest_remarks"] == "phone_format_warning"
    assert events[1]["status"] == "error"


def test_json_logger_emits_suppressed_normalization_summary_per_file() -> None:
    stream = io.StringIO()
    logger = JsonLogger(run_id="run-4", stream=stream, log_file_path=None)

    logger.warn(
        phase="archive_ingest",
        message="row warning",
        ingest_remarks="status_normalized:UPI/Wallet App->UPI_WALLET_APP",
        source_file="base.xlsx",
    )
    logger.warn(
        phase="archive_ingest",
        message="row warning",
        ingest_remarks="status_normalized:Delivered Soon->DELIVERED_SOON",
        source_file="base.xlsx",
    )

    logger.emit_suppressed_normalization_summary(phase="archive_ingest", file_key="base.xlsx")

    events = [json.loads(line) for line in stream.getvalue().strip().splitlines()]
    assert len(events) == 1
    summary = events[0]
    assert summary["status"] == "info"
    assert summary["message"] == "normalization_events_suppressed"
    assert summary["source_file"] == "base.xlsx"
    assert summary["normalization_events_suppressed"] == 2
