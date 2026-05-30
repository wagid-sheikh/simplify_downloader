"""Dashboard data-quality warning thresholds and formatting helpers."""
from __future__ import annotations

from typing import Any, Mapping

NAVIGATION_FAILURES = "navigation_failures"
INVALID_CSV_DOWNLOADS = "invalid_csv_downloads"
SKIPPED_REQUIRED_ROWS = "skipped_required_rows"
ROW_COERCION_FAILURES = "row_coercion_failures"

DASHBOARD_DATA_QUALITY_WARNING_THRESHOLDS: dict[str, int] = {
    # A single transient navigation failure is noisy; three failed attempts in one run
    # means the download surface was unstable enough for operators to investigate.
    NAVIGATION_FAILURES: 3,
    # Any invalid CSV download means a bucket/store was silently omitted from merge.
    INVALID_CSV_DOWNLOADS: 1,
    # Bad source rows remain skipped, but required-field loss must be visible.
    SKIPPED_REQUIRED_ROWS: 1,
    # Row coercion failures are skipped by design; expose them once any are observed.
    ROW_COERCION_FAILURES: 1,
}

DATA_QUALITY_WARNING_LABELS: dict[str, str] = {
    NAVIGATION_FAILURES: "repeated navigation failures",
    INVALID_CSV_DOWNLOADS: "invalid CSV downloads discarded",
    SKIPPED_REQUIRED_ROWS: "rows skipped due to missing required fields",
    ROW_COERCION_FAILURES: "rows skipped due to CSV coercion failures",
}


def threshold_for(code: str) -> int:
    return DASHBOARD_DATA_QUALITY_WARNING_THRESHOLDS.get(code, 1)


def warning_label(code: str) -> str:
    return DATA_QUALITY_WARNING_LABELS.get(code, code.replace("_", " "))


def format_threshold_breach(breach: Mapping[str, Any]) -> str:
    code = str(breach.get("code") or "data_quality_warning")
    count = int(breach.get("count") or 0)
    threshold = int(breach.get("threshold") or threshold_for(code))
    return f"{warning_label(code)}: {count} observed (threshold {threshold})"
