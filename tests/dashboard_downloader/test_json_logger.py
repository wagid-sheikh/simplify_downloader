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
