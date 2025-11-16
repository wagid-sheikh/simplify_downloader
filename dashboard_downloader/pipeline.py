from __future__ import annotations

from pathlib import Path

from dashboard_downloader.json_logger import JsonLogger, log_event
from dashboard_downloader.run_downloads import run_all_stores_single_session
from dashboard_downloader.run_store_reports import (
    resolve_report_date,
    run_store_reports_for_date,
)

from simplify_downloader.common.audit import audit_bucket
from simplify_downloader.common.cleanup import cleanup_bucket
from simplify_downloader.common.ingest.service import ingest_bucket

from .settings import PipelineSettings


async def run_pipeline(*, settings: PipelineSettings, logger: JsonLogger) -> None:
    log_event(logger=logger, phase="orchestrator", message="pipeline start")

    download_summary = await run_all_stores_single_session(
        settings=settings,
        logger=logger,
    )

    for bucket, store_info in download_summary.items():
        merged_meta = store_info.get("__merged__")
        if not merged_meta:
            continue
        merged_path = Path(merged_meta["path"])
        merged_rows = int(merged_meta.get("rows", 0))
        download_total = sum(
            int(info.get("rows", 0)) for key, info in store_info.items() if key != "__merged__"
        )
        counts = {
            "download_total": download_total,
            "merged_rows": merged_rows,
            "ingested_rows": 0,
        }

        if settings.dry_run or not settings.database_url:
            log_event(
                logger=logger,
                phase="ingest",
                bucket=bucket,
                merged_file=str(merged_path),
                status="warn",
                message="skipping ingestion (dry run or missing database)",
            )
        else:
            ingest_totals = await ingest_bucket(
                bucket=bucket,
                csv_path=merged_path,
                batch_size=settings.ingest_batch_size,
                database_url=settings.database_url,
                logger=logger,
            )
            counts["ingested_rows"] = ingest_totals["rows"]

        audit_result = audit_bucket(bucket=bucket, counts=counts, logger=logger)
        cleanup_bucket(
            bucket=bucket,
            download_info=store_info,
            merged_path=merged_path,
            audit_status=audit_result["status"],
            logger=logger,
        )

    log_event(logger=logger, phase="orchestrator", message="pipeline complete")

    await _run_reporting_tail_step(settings=settings, logger=logger)


async def _run_reporting_tail_step(*, settings: PipelineSettings, logger: JsonLogger) -> None:
    try:
        report_date = resolve_report_date()
    except Exception as exc:  # pragma: no cover - defensive
        log_event(
            logger=logger,
            phase="orchestrator",
            status="warning",
            message="reporting tail step skipped due to invalid report date",
            extras={"error": str(exc)},
        )
        return

    try:
        await run_store_reports_for_date(
            report_date,
            logger=logger,
            run_id=settings.run_id,
            database_url=settings.database_url,
        )
        log_event(
            logger=logger,
            phase="orchestrator",
            status="info",
            message="reporting tail step completed",
            extras={"report_date": report_date.isoformat()},
        )
    except Exception as exc:  # pragma: no cover - safeguard
        log_event(
            logger=logger,
            phase="orchestrator",
            status="warning",
            message="reporting tail step encountered an unexpected error",
            extras={"error": str(exc)},
        )
