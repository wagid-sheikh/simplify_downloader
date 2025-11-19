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
    ingested_rows = counts.get("ingested_rows", 0)

    status = "ok" if merged_rows == ingested_rows else "warn"
    message = "counts match" if status == "ok" else "ingest differs from merged"

    log_event(
        logger=logger,
        phase="audit",
        bucket=bucket,
        counts={
            "download_total": download_total,
            "merged_rows": merged_rows,
            "ingested_rows": ingested_rows,
        },
        status=status,
        message=message,
    )
    result = {
        "download_total": download_total,
        "merged_rows": merged_rows,
        "ingested_rows": ingested_rows,
        "status": status,
    }
    return result
