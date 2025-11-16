from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone
from pathlib import Path

from dashboard_downloader.json_logger import JsonLogger, log_event
from dashboard_downloader.run_downloads import run_all_stores_single_session
from dashboard_downloader.run_store_reports import (
    build_email_message,
    load_email_settings,
    resolve_report_date,
    run_store_reports_for_date,
    send_email,
)
from dashboard_downloader.run_summary import (
    RunAggregator,
    fetch_report_documents,
    fetch_summary_for_run,
    insert_run_summary,
    update_run_summary,
)

from simplify_downloader.common.audit import audit_bucket
from simplify_downloader.common.cleanup import cleanup_bucket
from simplify_downloader.common.ingest.service import ingest_bucket

from .settings import PipelineSettings


async def run_pipeline(
    *, settings: PipelineSettings, logger: JsonLogger, aggregator: RunAggregator
) -> None:
    log_event(logger=logger, phase="orchestrator", message="pipeline start")

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
            )
            counts["ingested_rows"] = ingest_totals["rows"]

        audit_result = audit_bucket(bucket=bucket, counts=counts, logger=logger)
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
        settings=settings, logger=logger, aggregator=aggregator
    )

    await _finalize_summary_and_email(
        settings=settings,
        logger=logger,
        aggregator=aggregator,
        report_date=report_date,
    )


async def _run_reporting_tail_step(
    *, settings: PipelineSettings, logger: JsonLogger, aggregator: RunAggregator
) -> date | None:
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
    attachments_snapshot: list[dict] = []
    if database_url:
        try:
            attachments_snapshot = await fetch_report_documents(database_url, aggregator.run_id)
        except Exception as exc:  # pragma: no cover - defensive
            log_event(
                logger=logger,
                phase="run_summary",
                status="warning",
                message="failed to load document metadata for email plan",
                extras={"error": str(exc)},
            )
    email_settings = load_email_settings()
    recipients = email_settings.to if email_settings else []
    if not database_url:
        plan_message = "database unavailable"
    elif not attachments_snapshot:
        plan_message = "no reports available"
    elif not email_settings:
        plan_message = "email disabled"
    else:
        plan_message = "pending"
    aggregator.plan_email(
        recipients=recipients,
        attachment_count=len(attachments_snapshot),
        message=plan_message,
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

    await _handle_report_email(
        settings=settings,
        logger=logger,
        aggregator=aggregator,
        report_date=report_date,
    )

    final_finished_at = datetime.now(timezone.utc)
    if not database_url:
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


async def _handle_report_email(
    *,
    settings: PipelineSettings,
    logger: JsonLogger,
    aggregator: RunAggregator,
    report_date: date | None,
) -> None:
    database_url = settings.database_url
    if not database_url:
        log_event(
            logger=logger,
            phase="report_email",
            status="info",
            message="report_email: skipped (no database)",
        )
        aggregator.finalize_email(
            status="skipped",
            message="database unavailable",
            recipients=[],
            attachment_count=0,
        )
        return

    try:
        document_rows = await fetch_report_documents(database_url, aggregator.run_id)
    except Exception as exc:  # pragma: no cover - defensive
        log_event(
            logger=logger,
            phase="report_email",
            status="error",
            message="report_email: failed to load documents",
            extras={"error": str(exc)},
        )
        aggregator.finalize_email(
            status="error",
            message="failed to load documents",
            recipients=[],
            attachment_count=0,
        )
        return

    attachment_paths: list[tuple[str, Path]] = []
    for row in document_rows:
        raw_path = row.get("file_path")
        store_code = row.get("store_code") or (row.get("file_name") or "UNKNOWN")
        if isinstance(store_code, str) and "_" in store_code and not row.get("store_code"):
            store_code = store_code.split("_", 1)[0]
        if not raw_path:
            continue
        path = Path(raw_path)
        if path.exists():
            attachment_paths.append((store_code, path))

    if not attachment_paths:
        log_event(
            logger=logger,
            phase="report_email",
            status="info",
            message="no reports generated, skipping email",
        )
        aggregator.finalize_email(
            status="skipped",
            message="no reports generated, skipping email",
            recipients=[],
            attachment_count=0,
        )
        return

    settings_obj = load_email_settings()
    if not settings_obj:
        log_event(
            logger=logger,
            phase="report_email",
            status="info",
            message="report_email: disabled or not configured, skipping send",
        )
        aggregator.finalize_email(
            status="skipped",
            message="email disabled",
            recipients=[],
            attachment_count=len(attachment_paths),
        )
        return

    summary_row = await fetch_summary_for_run(database_url, aggregator.run_id)
    summary_text = summary_row.get("summary_text") if summary_row else None
    if not summary_text:
        log_event(
            logger=logger,
            phase="report_email",
            status="error",
            message="report_email: summary text missing",
        )
        aggregator.finalize_email(
            status="error",
            message="summary text missing",
            recipients=settings_obj.to,
            attachment_count=len(attachment_paths),
        )
        return

    message = build_email_message(
        settings_obj,
        report_date or date.today(),
        attachment_paths,
        body_text=summary_text,
    )
    try:
        await asyncio.to_thread(send_email, settings_obj, message)
    except Exception as exc:  # pragma: no cover - network issues
        log_event(
            logger=logger,
            phase="report_email",
            status="error",
            message="report_email: failed",
            extras={"error": str(exc)},
        )
        aggregator.finalize_email(
            status="error",
            message="report_email: failed",
            recipients=settings_obj.to,
            attachment_count=len(attachment_paths),
        )
        return

    log_event(
        logger=logger,
        phase="report_email",
        status="ok",
        message="report_email: ok",
        extras={
            "report_date": report_date.isoformat() if report_date else None,
            "store_codes": [code for code, _ in attachment_paths],
            "to": settings_obj.to,
        },
    )
    aggregator.finalize_email(
        status="ok",
        message="email sent",
        recipients=settings_obj.to,
        attachment_count=len(attachment_paths),
    )
