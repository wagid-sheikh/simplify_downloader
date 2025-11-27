from __future__ import annotations

from typing import Dict

from app.dashboard_downloader.json_logger import JsonLogger, log_event


def audit_bucket(
    *,
    bucket: str,
    counts: Dict[str, int],
    logger: JsonLogger,
) -> Dict[str, int | str]:
    download_total = counts.get("download_total", 0)
    merged_rows = counts.get("merged_rows", 0)
    raw_merged_rows = counts.get("raw_merged_rows", merged_rows)
    ingested_rows = counts.get("ingested_rows", 0)

    status = "ok" if merged_rows == ingested_rows else "warn"
    message = "counts match" if status == "ok" else "ingest differs from merged"

    logged_counts = {
        "download_total": download_total,
        "merged_rows": merged_rows,
        "ingested_rows": ingested_rows,
    }
    if raw_merged_rows != merged_rows:
        logged_counts["raw_merged_rows"] = raw_merged_rows

    log_event(
        logger=logger,
        phase="audit",
        bucket=bucket,
        counts=logged_counts,
        status=status,
        message=message,
    )
    result = {
        "download_total": download_total,
        "merged_rows": merged_rows,
        "raw_merged_rows": raw_merged_rows,
        "ingested_rows": ingested_rows,
        "status": status,
    }
    return result
