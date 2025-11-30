from __future__ import annotations

from pathlib import Path
from typing import Dict

from app.dashboard_downloader.json_logger import JsonLogger, log_event


def cleanup_bucket(
    *,
    bucket: str,
    download_info: Dict[str, Dict[str, object]],
    merged_path: Path,
    audit_status: str,
    logger: JsonLogger,
) -> None:
    if audit_status not in {"ok", "info"}:
        log_event(
            logger=logger,
            phase="cleanup",
            bucket=bucket,
            status="warn",
            merged_file=str(merged_path),
            message="skipping cleanup due to audit mismatch",
        )
        return

    for store_code, info in download_info.items():
        if store_code == "__merged__":
            continue
        path = info.get("path")
        if path and Path(path).exists():
            Path(path).unlink()
    if merged_path.exists():
        merged_path.unlink()
    log_event(
        logger=logger,
        phase="cleanup",
        bucket=bucket,
        merged_file=str(merged_path),
        status="ok",
        message="files removed after successful audit",
    )
