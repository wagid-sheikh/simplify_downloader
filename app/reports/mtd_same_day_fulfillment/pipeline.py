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
from app.dashboard_downloader.report_generator import render_pdf_with_configured_browser

from .data import fetch_mtd_same_day_fulfillment
from .render import render_html

PIPELINE_NAME = "reports.mtd_same_day_fulfillment"
TEMPLATE_NAME = "report.html"
TEMPLATE_DIR = Path("app") / "reports" / "mtd_same_day_fulfillment" / "templates"
OUTPUT_ROOT = Path("app") / "reports" / "output_files"


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
                doc_type="mtd_same_day_fulfillment_pdf",
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
            tracker.add_summary("Database URL is missing; cannot generate MTD same-day fulfillment report.")
            tracker.overall = "error"
            log_event(
                logger=logger,
                phase="load_data",
                status="error",
                message="Database URL is missing; cannot generate MTD same-day fulfillment report.",
            )
            return

        tz = get_timezone()
        resolved_date = report_date or aware_now(tz).date()
        tracker.set_report_date(resolved_date)
        log_event(
            logger=logger,
            phase="orchestrator",
            message="starting MTD same-day fulfillment report pipeline",
            report_date=resolved_date.isoformat(),
            run_env=run_env,
            force=force,
        )

        existing = await check_existing_run(database_url, PIPELINE_NAME, resolved_date)
        if not force and existing and existing.get("overall_status") in {"ok", "warning"}:
            log_event(
                logger=logger,
                phase="orchestrator",
                status="warning",
                message="MTD same-day fulfillment report already generated; skipping",
                report_date=resolved_date.isoformat(),
                existing_status=existing.get("overall_status"),
            )
            return

        rows = await fetch_mtd_same_day_fulfillment(database_url=database_url, report_date=resolved_date)
        tracker.mark_phase("load_data", "ok")
        log_event(
            logger=logger,
            phase="load_data",
            message="MTD same-day fulfillment report data loaded",
            report_date=resolved_date.isoformat(),
            rows=len(rows),
        )

        html = render_html(rows=rows, report_date_display=resolved_date.strftime("%d-%b-%Y"))
        tracker.mark_phase("render_html", "ok")
        log_event(
            logger=logger,
            phase="render_html",
            message="rendered MTD same-day fulfillment report html",
            report_date=resolved_date.isoformat(),
            template_name=TEMPLATE_NAME,
            template_dir=str(TEMPLATE_DIR),
        )

        OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
        output_path = OUTPUT_ROOT / f"{PIPELINE_NAME}_{resolved_date.isoformat()}.pdf"
        if output_path.exists():
            output_path.unlink()
        try:
            await render_pdf_with_configured_browser(
                html,
                output_path,
                pdf_options={"format": "A4", "landscape": True},
                logger=logger,
            )
        except asyncio.TimeoutError as exc:
            tracker.mark_phase("render_pdf", "error")
            tracker.add_summary(f"PDF rendering timed out after {config.pdf_render_timeout_seconds}s.")
            tracker.overall = "error"
            log_event(
                logger=logger,
                phase="render_pdf",
                status="error",
                message="MTD same-day fulfillment report pdf render timed out",
                report_date=resolved_date.isoformat(),
                file_path=str(output_path),
                error=str(exc),
            )
            finished_at = datetime.now(timezone.utc)
            record = tracker.build_record(finished_at)
            await persist_summary_record(database_url, record)
            return

        tracker.mark_phase("render_pdf", "ok")
        log_event(
            logger=logger,
            phase="render_pdf",
            message="rendered MTD same-day fulfillment report pdf",
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
            message="saved MTD same-day fulfillment report document",
            report_date=resolved_date.isoformat(),
            file_path=str(output_path),
        )

        tracker.metrics = {"report_date": resolved_date.isoformat(), "rows": len(rows)}
        tracker.add_summary(
            f"MTD same-day fulfillment report generated for {resolved_date.isoformat()} with {len(rows)} rows."
        )

        pre_finished_at = datetime.now(timezone.utc)
        pre_record = tracker.build_record(pre_finished_at)
        await persist_summary_record(database_url, pre_record)

        try:
            notification_result = await send_notifications_for_run(PIPELINE_NAME, run_id)
            emails_planned = int(notification_result.get("emails_planned") or 0)
            emails_sent = int(notification_result.get("emails_sent") or 0)
            notification_errors = notification_result.get("errors") or []
            if emails_sent > 0:
                tracker.mark_phase("send_email", "ok")
                log_event(
                    logger=logger,
                    phase="send_email",
                    message="notification sent",
                    report_date=resolved_date.isoformat(),
                    emails_planned=emails_planned,
                    emails_sent=emails_sent,
                    notification_errors=notification_errors,
                )
            else:
                tracker.mark_phase("send_email", "warning")
                tracker.add_summary("Notification dispatch completed but no emails were sent; see logs.")
                tracker.overall = "warning"
                log_event(
                    logger=logger,
                    phase="send_email",
                    status="warn",
                    message="notification dispatch completed with zero emails sent",
                    report_date=resolved_date.isoformat(),
                    emails_planned=emails_planned,
                    emails_sent=emails_sent,
                    notification_errors=notification_errors,
                )
        except Exception as exc:  # pragma: no cover - defensive guardrail
            tracker.mark_phase("send_email", "warning")
            tracker.add_summary(f"Notification dispatch failed; see logs for details ({exc}).")
            tracker.overall = "warning"
            log_event(
                logger=logger,
                phase="send_email",
                status="warning",
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
            message="MTD same-day fulfillment report pipeline complete",
            report_date=resolved_date.isoformat(),
            status=tracker.overall or "ok",
        )
    finally:
        logger.close()


def run_pipeline(report_date: date | None = None, env: str | None = None, force: bool = False) -> None:
    asyncio.run(_run(report_date, env, force))


__all__ = ["PIPELINE_NAME", "run_pipeline"]
