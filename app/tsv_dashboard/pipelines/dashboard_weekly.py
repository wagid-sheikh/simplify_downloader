from __future__ import annotations

import asyncio
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List

from app.dashboard_downloader.notifications import send_notifications_for_run
from app.config import config

from .base import (
    PipelinePhaseTracker,
    check_existing_run,
    persist_summary_record,
    resolve_run_env,
    update_summary_record,
)
from .reporting import (
    PdfArtifact,
    fetch_store_period_rows,
    generate_combined_pdf,
    generate_period_pdfs,
    get_report_store_codes,
    record_documents,
)

PIPELINE_NAME = "simplify_dashboard_weekly"


async def _dispatch_notifications(run_id: str, tracker: PipelinePhaseTracker) -> None:
    try:
        await send_notifications_for_run(PIPELINE_NAME, run_id)
        tracker.mark_phase("send_email", "ok")
    except Exception as exc:  # pragma: no cover - defensive guardrail
        tracker.mark_phase("send_email", "warning")
        tracker.add_summary(
            f"Notification dispatch failed; see logs for details ({exc})."
        )
        tracker.overall = "warning"


def _compute_period(today: date) -> tuple[date, date]:
    delta = today.weekday() + 1
    period_end = today - timedelta(days=delta)
    period_start = period_end - timedelta(days=6)
    return period_start, period_end


def _build_period_label(start: date, end: date) -> str:
    return f"{start:%Y-%m-%d}_to_{end:%Y-%m-%d}"


async def _run(env: str | None = None) -> None:
    run_env = resolve_run_env(env)
    run_id = uuid.uuid4().hex
    tracker = PipelinePhaseTracker(pipeline_name=PIPELINE_NAME, env=run_env, run_id=run_id)
    database_url = config.database_url

    stores = get_report_store_codes()
    today = date.today()
    period_start, period_end = _compute_period(today)
    tracker.set_report_date(period_end)
    period_label = _build_period_label(period_start, period_end)

    existing = await check_existing_run(database_url, PIPELINE_NAME, period_end)
    if existing and existing.get("overall_status") in {"ok", "warning"}:
        print(
            f"Weekly report for {period_label} already generated with status {existing['overall_status']}; skipping."
        )
        return

    rows = await fetch_store_period_rows(
        database_url=database_url,
        store_codes=stores,
        period_start=period_start,
        period_end=period_end,
    )
    tracker.mark_phase("load_data", "ok")
    stats_by_store: Dict[str, Dict[str, float]] = {}
    for row in rows:
        stats_by_store[row["store_code"]] = {
            "row_count": int(row["row_count"] or 0),
            "pickup_total": float(row["pickup_total"] or 0),
            "avg_delivery_tat": float(row["avg_delivery_tat"] or 0)
            if row["avg_delivery_tat"] is not None
            else None,
            "avg_repeat_pct": float(row["avg_repeat_pct"] or 0)
            if row["avg_repeat_pct"] is not None
            else None,
            "avg_conversion": float(row["avg_conversion"] or 0)
            if row["avg_conversion"] is not None
            else None,
        }

    stores_with_data = sorted(stats_by_store.keys())
    stores_without_data = [code for code in stores if code not in stats_by_store]
    total_rows = sum(stats["row_count"] for stats in stats_by_store.values())

    tracker.metrics = {
        "period": {"start": period_start.isoformat(), "end": period_end.isoformat()},
        "stores_expected": stores,
        "stores_with_data": stores_with_data,
        "stores_without_data": stores_without_data,
        "rows_found": total_rows,
    }

    if total_rows == 0:
        tracker.add_summary(
            f"No dashboard data was available for the week {period_label}. Daily ingestion is required before weekly reporting."
        )
        tracker.mark_phase("render_pdfs", "warning")
        tracker.mark_phase("persist_documents", "warning")
        tracker.overall = "warning"
        finished_at = datetime.now(timezone.utc)
        record = tracker.build_record(finished_at)
        await persist_summary_record(database_url, record)
        await _dispatch_notifications(run_id, tracker)
        final_finished_at = datetime.now(timezone.utc)
        final_record = tracker.build_record(final_finished_at)
        await update_summary_record(database_url, run_id, final_record)
        return

    pdfs: List[PdfArtifact] = []
    per_store = await generate_period_pdfs(
        pipeline_name=PIPELINE_NAME,
        report_date=period_end,
        period_label=period_label,
        store_stats=stats_by_store,
        stores_without_data=stores_without_data,
        prefix="Weekly Store Performance",
        reference_key="week",
    )
    pdfs.extend(per_store)

    combined_totals = {
        "row_count": sum(stats["row_count"] for stats in stats_by_store.values()),
        "pickup_total": sum(stats["pickup_total"] for stats in stats_by_store.values()),
    }
    avg_fields = ["avg_delivery_tat", "avg_repeat_pct", "avg_conversion"]
    for key in avg_fields:
        values = [stats[key] for stats in stats_by_store.values() if stats[key] is not None]
        combined_totals[key] = sum(values) / len(values) if values else 0

    combined_pdf = await generate_combined_pdf(
        pipeline_name=PIPELINE_NAME,
        report_date=period_end,
        period_label=period_label,
        combined_stats=combined_totals,
        missing_stores=stores_without_data,
        prefix="Weekly Store Performance",
        reference_key="week",
    )
    pdfs.append(combined_pdf)
    tracker.metrics["pdfs_generated"] = len(pdfs)
    tracker.mark_phase("render_pdfs", "ok")

    if stores_without_data and stores_with_data:
        tracker.overall = "warning"

    tracker.add_summary(
        f"Weekly PDFs: {len(pdfs)} generated for period {period_label}. Stores with data: {len(stores_with_data)}; missing: {len(stores_without_data)}."
    )

    await record_documents(
        database_url=database_url,
        pipeline_name=PIPELINE_NAME,
        run_id=run_id,
        report_date=period_end,
        artifacts=pdfs,
        doc_type="store_weekly_pdf",
    )
    tracker.mark_phase("persist_documents", "ok")

    pre_finished_at = datetime.now(timezone.utc)
    pre_record = tracker.build_record(pre_finished_at)
    await persist_summary_record(database_url, pre_record)
    await _dispatch_notifications(run_id, tracker)

    final_finished_at = datetime.now(timezone.utc)
    final_record = tracker.build_record(final_finished_at)
    await update_summary_record(database_url, run_id, final_record)


def run_pipeline(env: str | None = None) -> None:
    asyncio.run(_run(env))


if __name__ == "__main__":
    run_pipeline()
