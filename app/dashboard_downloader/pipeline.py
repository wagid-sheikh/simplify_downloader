from __future__ import annotations
from datetime import date, datetime, timezone
from pathlib import Path

from app.dashboard_downloader.json_logger import JsonLogger, log_event
from app.dashboard_downloader.run_downloads import run_all_stores_single_session
from app.dashboard_downloader.run_store_reports import (
    resolve_report_date,
    run_store_reports_for_date,
)
from app.dashboard_downloader.run_summary import (
    RunAggregator,
    insert_run_summary,
    update_run_summary,
)
from app.dashboard_downloader.notifications import send_notifications_for_run

from app.common.audit import audit_bucket
from app.common.cleanup import cleanup_bucket
from app.common.ingest.service import ingest_bucket

from .settings import PipelineSettings


async def run_pipeline(
    *, settings: PipelineSettings, logger: JsonLogger, aggregator: RunAggregator
) -> None:
    log_event(logger=logger, phase="orchestrator", message="pipeline start")

    run_date: date | None = None
    try:
        run_date = resolve_report_date()
    except Exception as exc:  # pragma: no cover - defensive
        log_event(
            logger=logger,
            phase="orchestrator",
            status="warning",
            message="unable to resolve report date; defaulting to today",
            extras={"error": str(exc)},
        )
        run_date = date.today()

    download_summary = await run_all_stores_single_session(
        settings=settings,
        logger=logger,
    )
    aggregator.record_download_summary(download_summary)

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
                run_id=settings.run_id,
                run_date=run_date,
            )
            counts["ingested_rows"] = ingest_totals["rows"]

            deduped_rows = ingest_totals.get("deduped_rows", merged_rows)
            if deduped_rows != merged_rows:
                counts["raw_merged_rows"] = merged_rows
                counts["merged_rows"] = deduped_rows

        audit_result = audit_bucket(
            bucket=bucket, counts=counts, logger=logger, single_session=True
        )
        aggregator.record_bucket_counts(bucket, counts)
        cleanup_bucket(
            bucket=bucket,
            download_info=store_info,
            merged_path=merged_path,
            audit_status=audit_result["status"],
            logger=logger,
        )

    log_event(logger=logger, phase="orchestrator", message="pipeline complete")

    report_date = await _run_reporting_tail_step(
        settings=settings, logger=logger, aggregator=aggregator, report_date=run_date
    )

    await _finalize_summary_and_email(
        settings=settings,
        logger=logger,
        aggregator=aggregator,
        report_date=report_date,
    )


async def _run_reporting_tail_step(
    *,
    settings: PipelineSettings,
    logger: JsonLogger,
    aggregator: RunAggregator,
    report_date: date | None,
) -> date | None:
    if report_date is None:
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
            return None

    try:
        aggregator.set_report_date(report_date)
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
        return report_date
    except Exception as exc:  # pragma: no cover - safeguard
        log_event(
            logger=logger,
            phase="orchestrator",
            status="warning",
            message="reporting tail step encountered an unexpected error",
            extras={"error": str(exc)},
        )
        return report_date


async def _finalize_summary_and_email(
    *,
    settings: PipelineSettings,
    logger: JsonLogger,
    aggregator: RunAggregator,
    report_date: date | None,
) -> None:
    database_url = settings.database_url
    attachment_count = len(getattr(aggregator, "pdf_records", []) or [])
    summary_only = attachment_count == 0
    plan_message = (
        "summary-only notification scheduled (no documents generated)"
        if summary_only
        else "notification dispatch scheduled"
    )
    aggregator.plan_email(
        recipients=[],
        attachment_count=attachment_count,
        message=plan_message,
    )
    if summary_only:
        log_event(
            logger=logger,
            phase="report_email",
            status="info",
            message="no documents generated; summary-only notification will be sent",
        )

    summary_inserted = False
    pre_finished_at = datetime.now(timezone.utc)
    if database_url:
        try:
            await insert_run_summary(
                database_url, aggregator.build_record(finished_at=pre_finished_at)
            )
            summary_inserted = True
            log_event(
                logger=logger,
                phase="run_summary",
                status="ok",
                message="run summary persisted",
                extras={
                    "pipeline_name": aggregator.pipeline_name,
                    "overall_status": aggregator.overall_status(),
                },
            )
        except Exception as exc:  # pragma: no cover - defensive
            log_event(
                logger=logger,
                phase="run_summary",
                status="error",
                message="failed to persist run summary",
                extras={"error": str(exc)},
            )
    else:
        log_event(
            logger=logger,
            phase="run_summary",
            status="info",
            message="run summary persistence skipped (no database)",
        )

    final_finished_at = datetime.now(timezone.utc)
    if not database_url:
        warning_message = "cannot send notifications without database access"
        status = "warning" if attachment_count else "info"
        aggregator.finalize_email(
            status=status,
            message=warning_message,
            recipients=[],
            attachment_count=attachment_count,
        )
        log_event(
            logger=logger,
            phase="report_email",
            status=status,
            message=warning_message,
            extras={"attachments": attachment_count},
        )
        return

    try:
        record = aggregator.build_record(finished_at=final_finished_at)
        if summary_inserted:
            await update_run_summary(database_url, aggregator.run_id, record)
            log_event(
                logger=logger,
                phase="run_summary",
                status="info",
                message="run summary updated after email",
                extras={"overall_status": aggregator.overall_status()},
            )
        else:
            await insert_run_summary(database_url, record)
            log_event(
                logger=logger,
                phase="run_summary",
                status="ok",
                message="run summary persisted",
                extras={
                    "pipeline_name": aggregator.pipeline_name,
                    "overall_status": aggregator.overall_status(),
                },
            )
    except Exception as exc:  # pragma: no cover - defensive
        log_event(
            logger=logger,
            phase="run_summary",
            status="error",
            message="failed to persist final run summary",
            extras={"error": str(exc)},
        )

    try:
        await send_notifications_for_run(aggregator.pipeline_name, aggregator.run_id)
        success_message = (
            "summary-only notification dispatched (no documents generated)"
            if summary_only
            else "notification dispatch completed"
        )
        log_event(
            logger=logger,
            phase="report_email",
            status="info",
            message=success_message,
            extras={"pipeline_name": aggregator.pipeline_name, "run_id": aggregator.run_id},
        )
        aggregator.finalize_email(
            status="info",
            message=success_message,
            recipients=[],
            attachment_count=attachment_count,
        )
    except Exception as exc:  # pragma: no cover - defensive
        aggregator.finalize_email(
            status="warning",
            message="notification dispatch failed",
            recipients=[],
            attachment_count=attachment_count,
        )
        log_event(
            logger=logger,
            phase="report_email",
            status="warning",
            message="notification dispatch failed",
            extras={"error": str(exc)},
        )


