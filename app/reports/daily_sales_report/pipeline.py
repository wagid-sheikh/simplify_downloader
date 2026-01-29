from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
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

from .data import DailySalesReportData, fetch_daily_sales_report

PIPELINE_NAME = "reports.daily_sales_report"
TEMPLATE_NAME = "daily_sales_report.html"
TEMPLATE_DIR = Path("app") / "reports" / "daily_sales_report" / "templates"
OUTPUT_ROOT = Path("app") / "reports" / "output_files"


def _format_amount(value: Decimal | int | float | None) -> str:
    if value is None:
        return "0"
    try:
        numeric = Decimal(str(value))
    except Exception:  # pragma: no cover - defensive
        return "0"
    rounded = int(numeric.to_integral_value(rounding=ROUND_HALF_UP))
    sign = "-" if rounded < 0 else ""
    return f"{sign}{_format_indian_number(abs(rounded))}"


def _format_indian_number(value: int) -> str:
    digits = str(value)
    if len(digits) <= 3:
        return digits
    last_three = digits[-3:]
    remaining = digits[:-3]
    chunks: list[str] = []
    while len(remaining) > 2:
        chunks.insert(0, remaining[-2:])
        remaining = remaining[:-2]
    if remaining:
        chunks.insert(0, remaining)
    return ",".join(chunks + [last_three])


def _render_html(context: Mapping[str, object]) -> str:
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    env.filters["format_amount"] = _format_amount
    template = env.get_template(TEMPLATE_NAME)
    return template.render(**context)


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
                doc_type="daily_sales_report_pdf",
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


def _build_context(data: DailySalesReportData) -> dict[str, object]:
    report_date_display = data.report_date.strftime("%d-%b-%Y")
    return {
        "company_name": "The Shaw Ventures",
        "report_date_display": report_date_display,
        "rows": data.rows,
        "totals": data.totals,
        "edited_orders": data.edited_orders,
        "edited_orders_summary": data.edited_orders_summary,
        "edited_orders_totals": data.edited_orders_totals,
        "missed_leads": data.missed_leads,
    }


async def _run(report_date: date | None, env: str | None, force: bool) -> None:
    run_env = resolve_run_env(env)
    run_id = new_run_id()
    logger = get_logger(run_id=run_id)
    tracker = PipelinePhaseTracker(pipeline_name=PIPELINE_NAME, env=run_env, run_id=run_id)
    database_url = config.database_url

    try:
        if not database_url:
            tracker.mark_phase("load_data", "error")
            tracker.add_summary("Database URL is missing; cannot generate daily sales report.")
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
        if not force and existing and existing.get("overall_status") in {"ok", "warning"}:
            log_event(
                logger=logger,
                phase="orchestrator",
                status="warn",
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
        )

        context = _build_context(data)
        html = _render_html(context)
        tracker.mark_phase("render_html", "ok")
        log_event(
            logger=logger,
            phase="render_html",
            message="rendered daily sales report html",
            report_date=resolved_date.isoformat(),
        )

        OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
        output_path = OUTPUT_ROOT / f"{PIPELINE_NAME}_{resolved_date.isoformat()}.pdf"
        if output_path.exists():
            output_path.unlink()
        await render_pdf_with_configured_browser(
            html,
            output_path,
            pdf_options={"format": "A4", "landscape": True},
            logger=logger,
        )
        tracker.mark_phase("render_pdf", "ok")
        log_event(
            logger=logger,
            phase="render_pdf",
            message="rendered daily sales report pdf",
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
            message="saved daily sales report document",
            report_date=resolved_date.isoformat(),
            file_path=str(output_path),
        )

        tracker.metrics = {
            "report_date": resolved_date.isoformat(),
            "rows": len(data.rows),
            "edited_orders": len(data.edited_orders),
        }
        tracker.add_summary(
            f"Daily sales report generated for {resolved_date.isoformat()} with "
            f"{len(data.rows)} cost centers and {len(data.edited_orders)} edited orders."
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
