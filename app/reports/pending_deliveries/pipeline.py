from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone
from pathlib import Path

from app.common.date_utils import aware_now, get_timezone
from app.common.db import session_scope
from app.config import config
from app.dashboard_downloader.db_tables import documents
from app.dashboard_downloader.json_logger import get_logger, log_event, new_run_id
from app.dashboard_downloader.notifications import send_notifications_for_run
from app.dashboard_downloader.pipelines.base import (
    PipelinePhaseTracker,
    check_existing_run,
    persist_summary_record,
    resolve_run_env,
    update_summary_record,
)

from .data import PendingDeliveriesReportData, fetch_pending_deliveries_report
from .render import render_html, render_pdf

PIPELINE_NAME = "reports.pending_deliveries"
OUTPUT_ROOT = Path("app") / "reports" / "output_files"


def _build_context(
    data: PendingDeliveriesReportData, *, run_id: str, timezone_label: str
) -> dict[str, object]:
    report_date_display = data.report_date.strftime("%d-%b-%Y")
    summary_rows = [
        {
            "label": bucket.label,
            "count": bucket.total_count,
            "pending_amount": bucket.total_pending_amount,
        }
        for bucket in data.summary_buckets
    ]
    return {
        "report_date_display": report_date_display,
        "report_date": data.report_date.isoformat(),
        "run_id": run_id,
        "timezone": timezone_label,
        "summary_rows": summary_rows,
        "store_sections": data.store_sections,
        "total_count": data.total_count,
        "total_pending_amount": data.total_pending_amount,
    }


async def _persist_document(
    *,
    database_url: str,
    run_id: str,
    report_date: date,
    file_path: Path,
) -> None:
    async with session_scope(database_url) as session:
        await session.execute(
            documents.insert().values(
                doc_type="pending_deliveries_pdf",
                doc_subtype="pipeline_report",
                doc_date=report_date,
                reference_name_1="pipeline",
                reference_id_1=PIPELINE_NAME,
                reference_name_2="run_id",
                reference_id_2=run_id,
                reference_name_3="report_date",
                reference_id_3=report_date.isoformat(),
                file_name=file_path.name,
                mime_type="application/pdf",
                file_size_bytes=file_path.stat().st_size if file_path.exists() else None,
                storage_backend="fs",
                file_path=str(file_path),
                created_at=datetime.now(timezone.utc),
                created_by="pipeline",
            )
        )
        await session.commit()


async def _run(report_date: date | None, env: str | None, force: bool) -> None:
    run_env = resolve_run_env(env)
    run_id = new_run_id()
    logger = get_logger(run_id=run_id)
    tracker = PipelinePhaseTracker(pipeline_name=PIPELINE_NAME, env=run_env, run_id=run_id)
    database_url = config.database_url

    try:
        if not database_url:
            tracker.mark_phase("load_data", "error")
            tracker.add_summary("Database URL is missing; cannot generate pending deliveries report.")
            tracker.overall = "error"
            log_event(
                logger=logger,
                phase="load_data",
                status="error",
                message="Database URL is missing; cannot generate pending deliveries report.",
            )
            return

        tz = get_timezone()
        resolved_date = report_date or aware_now(tz).date()
        tracker.set_report_date(resolved_date)
        log_event(
            logger=logger,
            phase="orchestrator",
            message="starting pending deliveries report pipeline",
            report_date=resolved_date.isoformat(),
            run_env=run_env,
            force=force,
        )

        existing = await check_existing_run(database_url, PIPELINE_NAME, resolved_date)
        if not force and existing and existing.get("overall_status") in {"ok", "warning"}:
            log_event(
                logger=logger,
                phase="orchestrator",
                status="warn",
                message="pending deliveries report already generated; skipping",
                report_date=resolved_date.isoformat(),
                existing_status=existing.get("overall_status"),
            )
            return

        data = await fetch_pending_deliveries_report(
            database_url=database_url,
            report_date=resolved_date,
            skip_uc_pending_delivery=config.skip_uc_pending_delivery,
        )
        tracker.mark_phase("load_data", "ok")
        log_event(
            logger=logger,
            phase="load_data",
            message="pending deliveries report data loaded",
            report_date=resolved_date.isoformat(),
            rows=data.total_count,
        )

        context = _build_context(data, run_id=run_id, timezone_label=tz.key)
        html = render_html(context)
        tracker.mark_phase("render_html", "ok")
        log_event(
            logger=logger,
            phase="render_html",
            message="rendered pending deliveries report html",
            report_date=resolved_date.isoformat(),
        )

        OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
        output_path = OUTPUT_ROOT / f"{PIPELINE_NAME}_{resolved_date.isoformat()}.pdf"
        if output_path.exists():
            output_path.unlink()
        await render_pdf(html, output_path)
        tracker.mark_phase("render_pdf", "ok")
        log_event(
            logger=logger,
            phase="render_pdf",
            message="rendered pending deliveries report pdf",
            report_date=resolved_date.isoformat(),
            file_path=str(output_path),
        )

        await _persist_document(
            database_url=database_url,
            run_id=run_id,
            report_date=resolved_date,
            file_path=output_path,
        )
        tracker.mark_phase("persist_documents", "ok")
        log_event(
            logger=logger,
            phase="persist_documents",
            message="saved pending deliveries report document",
            report_date=resolved_date.isoformat(),
            file_path=str(output_path),
        )

        tracker.metrics = {
            "report_date": resolved_date.isoformat(),
            "rows": data.total_count,
        }
        tracker.add_summary(
            f"Pending deliveries report generated for {resolved_date.isoformat()} with "
            f"{data.total_count} pending orders."
        )

        pre_finished_at = datetime.now(timezone.utc)
        pre_record = tracker.build_record(pre_finished_at)
        await persist_summary_record(database_url, pre_record)

        try:
            await send_notifications_for_run(PIPELINE_NAME, run_id)
            tracker.mark_phase("send_email", "ok")
            log_event(
                logger=logger,
                phase="send_email",
                message="notification sent",
                report_date=resolved_date.isoformat(),
            )
        except Exception as exc:  # pragma: no cover - defensive guardrail
            tracker.mark_phase("send_email", "warning")
            tracker.add_summary(f"Notification dispatch failed; see logs for details ({exc}).")
            tracker.overall = "warning"
            log_event(
                logger=logger,
                phase="send_email",
                status="warn",
                message="notification dispatch failed",
                report_date=resolved_date.isoformat(),
                error=str(exc),
            )

        final_finished_at = datetime.now(timezone.utc)
        final_record = tracker.build_record(final_finished_at)
        await update_summary_record(database_url, run_id, final_record)
        log_event(
            logger=logger,
            phase="orchestrator",
            message="pending deliveries report pipeline complete",
            report_date=resolved_date.isoformat(),
            status=tracker.overall or "ok",
        )
    finally:
        logger.close()


def run_pipeline(
    report_date: date | None = None, env: str | None = None, force: bool = False
) -> None:
    asyncio.run(_run(report_date, env, force))


__all__ = ["run_pipeline"]
