from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Mapping

from jinja2 import Environment, FileSystemLoader, select_autoescape

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

from app.reports.mtd_same_day_fulfillment.data import (
    fetch_missing_payments_mtd,
    fetch_mtd_same_day_fulfillment,
)
from app.reports.mtd_same_day_fulfillment.render import (
    render_html as render_mtd_same_day_html,
)
from app.reports.shared.formatters import (
    format_amount,
    format_ddmmyyyy,
    format_hhmm_ampm,
)
from app.reports.shared.same_day_fulfillment import (
    build_store_summary,
    format_duration_hours,
    format_duration_minutes,
    group_rows_by_store,
)

from .data import DailySalesReportData, fetch_daily_sales_report
from .to_be_recovered import (
    DOCUMENT_TYPE as TO_BE_RECOVERED_DOCUMENT_TYPE,
    PIPELINE_OUTPUT_PREFIX as TO_BE_RECOVERED_OUTPUT_PREFIX,
    TEMPLATE_NAME as TO_BE_RECOVERED_TEMPLATE_NAME,
    build_context as build_to_be_recovered_context,
)

PIPELINE_NAME = "reports.daily_sales_report"
TEMPLATE_NAME = "daily_sales_report.html"
TEMPLATE_DIR = Path("app") / "reports" / "daily_sales_report" / "templates"
SHARED_TEMPLATE_DIR = Path("app") / "reports" / "shared" / "templates"
OUTPUT_ROOT = Path("app") / "reports" / "output_files"


def _format_amount(value: Decimal | int | float | None) -> str:
    return format_amount(value)


def _format_ddmmyyyy(value: object | None) -> str:
    return format_ddmmyyyy(value)


def _format_hhmm_ampm(value: object | None) -> str:
    return format_hhmm_ampm(value)


def _render_html(
    context: Mapping[str, object], template_name: str = TEMPLATE_NAME
) -> str:
    env = Environment(
        loader=FileSystemLoader([str(TEMPLATE_DIR), str(SHARED_TEMPLATE_DIR)]),
        autoescape=select_autoescape(["html", "xml"]),
    )
    env.filters["format_amount"] = _format_amount
    env.filters["format_ddmmyyyy"] = _format_ddmmyyyy
    env.filters["format_hhmm_ampm"] = _format_hhmm_ampm
    template = env.get_template(template_name)
    return template.render(**context)


async def _persist_document(
    *,
    database_url: str,
    run_id: str,
    report_date: date,
    file_path: Path,
    doc_type: str,
) -> None:
    async with session_scope(database_url) as session:
        await session.execute(
            documents.insert().values(
                doc_type=doc_type,
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
                file_size_bytes=file_path.stat().st_size
                if file_path.exists()
                else None,
                storage_backend="fs",
                file_path=str(file_path),
                created_at=datetime.now(timezone.utc),
                created_by="pipeline",
            )
        )
        await session.commit()


def _build_context(
    data: DailySalesReportData, run_environment: str
) -> dict[str, object]:
    report_date_display = data.report_date.strftime("%d-%b-%Y")
    return {
        "company_name": "The Shaw Ventures",
        "report_date_display": report_date_display,
        "run_environment": run_environment,
        "rows": data.rows,
        "totals": data.totals,
        "edited_orders": data.edited_orders,
        "edited_orders_summary": data.edited_orders_summary,
        "edited_orders_totals": data.edited_orders_totals,
        "missed_leads": data.missed_leads,
        "cancelled_leads": data.cancelled_leads,
        "lead_performance_summary": data.lead_performance_summary,
        "to_be_recovered": data.to_be_recovered,
        "to_be_compensated": data.to_be_compensated,
        "to_be_recovered_total_order_value": data.to_be_recovered_total_order_value,
        "to_be_compensated_total_order_value": data.to_be_compensated_total_order_value,
        "auto_cleared_order_numbers_text": getattr(
            data, "auto_cleared_order_numbers_text", ""
        ),
        "same_day_fulfillment_rows": data.same_day_fulfillment_rows,
        "missing_payment_rows": data.missing_payment_rows,
        "same_day_grouped_rows_by_store": group_rows_by_store(
            data.same_day_fulfillment_rows
        ),
        "same_day_store_summary_rows": build_store_summary(
            data.same_day_fulfillment_rows
        ),
        "format_duration_hours": format_duration_hours,
        "format_duration_minutes": format_duration_minutes,
    }


async def _run(report_date: date | None, env: str | None, force: bool) -> None:
    run_env = resolve_run_env(env)
    run_id = new_run_id()
    logger = get_logger(run_id=run_id)
    tracker = PipelinePhaseTracker(
        pipeline_name=PIPELINE_NAME, env=run_env, run_id=run_id
    )
    database_url = config.database_url

    try:
        if not database_url:
            tracker.mark_phase("load_data", "error")
            tracker.add_summary(
                "Database URL is missing; cannot generate daily sales report."
            )
            tracker.overall = "error"
            log_event(
                logger=logger,
                phase="load_data",
                status="error",
                message="Database URL is missing; cannot generate daily sales report.",
            )
            return

        tz = get_timezone()
        resolved_date = report_date or aware_now(tz).date()
        tracker.set_report_date(resolved_date)
        log_event(
            logger=logger,
            phase="orchestrator",
            message="starting daily sales report pipeline",
            report_date=resolved_date.isoformat(),
            run_env=run_env,
            force=force,
        )

        existing = await check_existing_run(database_url, PIPELINE_NAME, resolved_date)
        if (
            not force
            and existing
            and existing.get("overall_status") in {"ok", "warning"}
        ):
            log_event(
                logger=logger,
                phase="orchestrator",
                status="warning",
                message="daily sales report already generated; skipping",
                report_date=resolved_date.isoformat(),
                existing_status=existing.get("overall_status"),
            )
            return

        data = await fetch_daily_sales_report(
            database_url=database_url,
            report_date=resolved_date,
        )
        tracker.mark_phase("load_data", "ok")
        log_event(
            logger=logger,
            phase="load_data",
            message="daily sales report data loaded",
            report_date=resolved_date.isoformat(),
            rows=len(data.rows),
            edited_orders=len(data.edited_orders),
            missing_payment_rows=len(data.missing_payment_rows),
        )

        context = _build_context(data, run_env)
        html = _render_html(context)
        to_be_recovered_context = build_to_be_recovered_context(
            rows=data.to_be_recovered,
            report_date=resolved_date,
            run_environment=run_env,
            auto_cleared_order_numbers_text=getattr(
                data, "auto_cleared_order_numbers_text", ""
            ),
        )
        to_be_recovered_html = _render_html(
            to_be_recovered_context,
            template_name=TO_BE_RECOVERED_TEMPLATE_NAME,
        )
        mtd_attachment_generated = True
        mtd_rows = []
        mtd_missing_payment_rows = []
        same_day_html: str | None = None
        mtd_attachment_error: str | None = None
        try:
            mtd_rows = await fetch_mtd_same_day_fulfillment(
                database_url=database_url, report_date=resolved_date
            )
            mtd_missing_payment_rows = await fetch_missing_payments_mtd(
                database_url=database_url, report_date=resolved_date
            )
        except Exception as exc:
            mtd_attachment_generated = False
            mtd_attachment_error = str(exc)
            tracker.mark_phase("render_html", "warning")
            tracker.add_summary(
                f"MTD attachment generation failed during data load ({exc})."
            )
            tracker.overall = "warning"
            log_event(
                logger=logger,
                phase="render_html",
                status="warning",
                message="failed to fetch mtd same-day fulfillment data; continuing daily report generation",
                report_date=resolved_date.isoformat(),
                database_backend=database_url.split("://", 1)[0],
                function_name="fetch_mtd_same_day_fulfillment",
                error=str(exc),
            )
        mtd_start = resolved_date.replace(day=1)
        mtd_end = resolved_date
        if mtd_attachment_generated:
            same_day_html = render_mtd_same_day_html(
                rows=mtd_rows,
                report_date_display=resolved_date.strftime("%d-%b-%Y"),
                mtd_start_display=mtd_start.strftime("%d-%b-%Y"),
                mtd_end_display=mtd_end.strftime("%d-%b-%Y"),
                missing_payment_rows=mtd_missing_payment_rows,
            )
            tracker.mark_phase("render_html", "ok")
        log_event(
            logger=logger,
            phase="render_html",
            status="warning" if not mtd_attachment_generated else "ok",
            message=(
                "rendered daily sales report html; skipped MTD attachment"
                if not mtd_attachment_generated
                else "rendered daily sales report html"
            ),
            report_date=resolved_date.isoformat(),
            pipeline_name=PIPELINE_NAME,
            mtd_start=mtd_start.isoformat(),
            mtd_end=mtd_end.isoformat(),
            mtd_row_count=len(mtd_rows),
        )

        OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
        output_path = OUTPUT_ROOT / f"{PIPELINE_NAME}_{resolved_date.isoformat()}.pdf"
        same_day_output_path = (
            OUTPUT_ROOT
            / f"reports.mtd_same_day_fulfillment_{resolved_date.isoformat()}.pdf"
        )
        to_be_recovered_output_path = (
            OUTPUT_ROOT
            / f"{TO_BE_RECOVERED_OUTPUT_PREFIX}_{resolved_date.isoformat()}.pdf"
        )
        if output_path.exists():
            output_path.unlink()
        if same_day_output_path.exists():
            same_day_output_path.unlink()
        if to_be_recovered_output_path.exists():
            to_be_recovered_output_path.unlink()
        try:
            await render_pdf_with_configured_browser(
                html,
                output_path,
                pdf_options={"format": "A4", "landscape": True},
                logger=logger,
            )
            await render_pdf_with_configured_browser(
                to_be_recovered_html,
                to_be_recovered_output_path,
                pdf_options={"format": "A4", "landscape": True},
                logger=logger,
            )
            if mtd_attachment_generated and same_day_html is not None:
                await render_pdf_with_configured_browser(
                    same_day_html,
                    same_day_output_path,
                    pdf_options={"format": "A4", "landscape": True},
                    logger=logger,
                )
        except asyncio.TimeoutError as exc:
            tracker.mark_phase("render_pdf", "error")
            tracker.add_summary(
                f"PDF rendering timed out after {config.pdf_render_timeout_seconds}s."
            )
            tracker.overall = "error"
            log_event(
                logger=logger,
                phase="render_pdf",
                status="error",
                message="daily sales report pdf render timed out",
                report_date=resolved_date.isoformat(),
                file_path=str(output_path),
                same_day_file_path=str(same_day_output_path),
                to_be_recovered_file_path=str(to_be_recovered_output_path),
                pipeline_name=PIPELINE_NAME,
                mtd_start=mtd_start.isoformat(),
                mtd_end=mtd_end.isoformat(),
                mtd_row_count=len(mtd_rows),
                error=str(exc),
            )
            finished_at = datetime.now(timezone.utc)
            record = tracker.build_record(finished_at)
            await persist_summary_record(database_url, record)
            return
        tracker.mark_phase(
            "render_pdf", "warning" if not mtd_attachment_generated else "ok"
        )
        log_event(
            logger=logger,
            phase="render_pdf",
            status="warning" if not mtd_attachment_generated else "ok",
            message=(
                "rendered daily sales report pdf; skipped MTD attachment pdf"
                if not mtd_attachment_generated
                else "rendered daily sales report pdf"
            ),
            report_date=resolved_date.isoformat(),
            file_path=str(output_path),
            same_day_file_path=str(same_day_output_path),
            to_be_recovered_file_path=str(to_be_recovered_output_path),
            pipeline_name=PIPELINE_NAME,
            mtd_start=mtd_start.isoformat(),
            mtd_end=mtd_end.isoformat(),
            mtd_row_count=len(mtd_rows),
        )

        await _persist_document(
            database_url=database_url,
            run_id=run_id,
            report_date=resolved_date,
            file_path=output_path,
            doc_type="daily_sales_report_pdf",
        )
        await _persist_document(
            database_url=database_url,
            run_id=run_id,
            report_date=resolved_date,
            file_path=to_be_recovered_output_path,
            doc_type=TO_BE_RECOVERED_DOCUMENT_TYPE,
        )
        if mtd_attachment_generated:
            await _persist_document(
                database_url=database_url,
                run_id=run_id,
                report_date=resolved_date,
                file_path=same_day_output_path,
                doc_type="mtd_same_day_fulfillment_pdf",
            )
        tracker.mark_phase(
            "persist_documents", "warning" if not mtd_attachment_generated else "ok"
        )
        log_event(
            logger=logger,
            phase="persist_documents",
            status="warning" if not mtd_attachment_generated else "ok",
            message=(
                "saved daily sales report document; MTD attachment document skipped"
                if not mtd_attachment_generated
                else "saved daily sales report document"
            ),
            report_date=resolved_date.isoformat(),
            file_path=str(output_path),
            same_day_file_path=str(same_day_output_path),
            to_be_recovered_file_path=str(to_be_recovered_output_path),
            pipeline_name=PIPELINE_NAME,
            mtd_start=mtd_start.isoformat(),
            mtd_end=mtd_end.isoformat(),
            mtd_row_count=len(mtd_rows),
        )

        tracker.metrics = {
            "report_date": resolved_date.isoformat(),
            "rows": len(data.rows),
            "edited_orders": len(data.edited_orders),
            "mtd_attachment_generated": mtd_attachment_generated,
            "mtd_attachment_row_count": len(mtd_rows),
            "mtd_attachment_error": mtd_attachment_error,
            "to_be_recovered_orders": len(data.to_be_recovered),
            "to_be_recovered_total_order_value": str(
                data.to_be_recovered_total_order_value
            ),
        }
        tracker.add_summary(
            f"Daily sales report generated for {resolved_date.isoformat()} with "
            f"{len(data.rows)} cost centers and {len(data.edited_orders)} edited orders."
        )
        if not mtd_attachment_generated:
            tracker.add_summary(
                "MTD same-day fulfillment attachment was not generated; daily sales report PDF was still produced."
            )

        pre_finished_at = datetime.now(timezone.utc)
        pre_record = tracker.build_record(pre_finished_at)
        await persist_summary_record(database_url, pre_record)

        try:
            notification_result = await send_notifications_for_run(
                PIPELINE_NAME, run_id
            )
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
                tracker.add_summary(
                    "Notification dispatch completed but no emails were sent; see logs."
                )
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
            tracker.add_summary(
                f"Notification dispatch failed; see logs for details ({exc})."
            )
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
            message="daily sales report pipeline complete",
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
