from __future__ import annotations

import asyncio
import os
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List

from .base import (
    PipelinePhaseTracker,
    check_existing_run,
    persist_summary_record,
    resolve_run_env,
    update_summary_record,
)
from .emailing import load_email_config, send_report_email
from .reporting import (
    PdfArtifact,
    fetch_store_period_rows,
    generate_combined_pdf,
    generate_period_pdfs,
    get_report_store_codes,
    record_documents,
)

PIPELINE_NAME = "dashboard_monthly"


def _compute_period(today: date) -> tuple[date, date]:
    first_of_month = today.replace(day=1)
    period_end = first_of_month - timedelta(days=1)
    period_start = period_end.replace(day=1)
    return period_start, period_end


def _build_period_label(start: date) -> str:
    return f"{start:%Y-%m}"


async def _run(env: str | None = None) -> None:
    run_env = resolve_run_env(env)
    run_id = uuid.uuid4().hex
    tracker = PipelinePhaseTracker(pipeline_name=PIPELINE_NAME, env=run_env, run_id=run_id)
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required for monthly pipeline")

    stores = get_report_store_codes()
    today = date.today()
    period_start, period_end = _compute_period(today)
    tracker.set_report_date(period_end)
    period_label = _build_period_label(period_start)

    existing = await check_existing_run(database_url, PIPELINE_NAME, period_end)
    if existing and existing.get("overall_status") in {"ok", "warning"}:
        print(
            f"Monthly report for {period_label} already generated with status {existing['overall_status']}; skipping."
        )
        return

    rows = await fetch_store_period_rows(
        database_url=database_url,
        store_codes=stores,
        period_start=period_start,
        period_end=period_end,
    )
    tracker.mark_phase("load_data", "ok")
    stats_by_store: Dict[str, Dict[str, float | None]] = {}
    for row in rows:
        stats_by_store[row["store_code"]] = {
            "row_count": int(row["row_count"] or 0),
            "pickup_total": float(row["pickup_total"] or 0),
            "avg_delivery_tat": float(row["avg_delivery_tat"] or 0) if row["avg_delivery_tat"] is not None else None,
            "avg_repeat_pct": float(row["avg_repeat_pct"] or 0) if row["avg_repeat_pct"] is not None else None,
            "avg_conversion": float(row["avg_conversion"] or 0) if row["avg_conversion"] is not None else None,
        }

    stores_with_data = sorted(stats_by_store.keys())
    stores_without_data = [code for code in stores if code not in stats_by_store]
    total_rows = sum(stats["row_count"] for stats in stats_by_store.values())

    tracker.metrics = {
        "period": {
            "label": period_label,
            "start": period_start.isoformat(),
            "end": period_end.isoformat(),
        },
        "stores_expected": stores,
        "stores_with_data": stores_with_data,
        "stores_without_data": stores_without_data,
        "rows_found": total_rows,
    }

    if total_rows == 0:
        tracker.add_summary(
            f"No dashboard data was found for {period_label}. Please ensure the daily pipeline ran successfully."
        )
        tracker.mark_phase("render_pdfs", "warning")
        tracker.mark_phase("persist_documents", "warning")
        tracker.mark_phase("send_email", "warning")
        tracker.overall = "warning"
        finished_at = datetime.now(timezone.utc)
        record = tracker.build_record(finished_at)
        await persist_summary_record(database_url, record)
        return

    pdfs: List[PdfArtifact] = []
    per_store = await generate_period_pdfs(
        pipeline_name=PIPELINE_NAME,
        report_date=period_end,
        period_label=period_label,
        store_stats=stats_by_store,
        stores_without_data=stores_without_data,
        prefix="Monthly Store Performance",
        reference_key="month",
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
        prefix="Monthly Store Performance",
        reference_key="month",
    )
    pdfs.append(combined_pdf)
    tracker.metrics["pdfs_generated"] = len(pdfs)
    tracker.mark_phase("render_pdfs", "ok")

    if stores_without_data and stores_with_data:
        tracker.overall = "warning"

    tracker.add_summary(
        f"Monthly PDFs: {len(pdfs)} generated for {period_label}. Stores with data: {len(stores_with_data)}; missing: {len(stores_without_data)}."
    )

    await record_documents(
        database_url=database_url,
        pipeline_name=PIPELINE_NAME,
        report_date=period_end,
        period_label=period_label,
        artifacts=pdfs,
        reference_key="report_month",
        doc_subtype="monthly_pdf",
    )
    tracker.mark_phase("persist_documents", "ok")

    attachments = [(artifact.store_code, artifact.file_path) for artifact in pdfs]
    pre_finished_at = datetime.now(timezone.utc)
    pre_record = tracker.build_record(pre_finished_at)
    await persist_summary_record(database_url, pre_record)
    summary_text = pre_record["summary_text"]

    email_config = load_email_config()
    if email_config and attachments:
        try:
            send_report_email(
                config=email_config,
                pipeline_name=PIPELINE_NAME,
                period_label=period_label,
                summary_text=summary_text,
                artifacts=attachments,
            )
            tracker.mark_phase("send_email", "ok")
        except Exception:
            tracker.mark_phase("send_email", "warning")
            tracker.add_summary("Email delivery failed; SMTP error encountered.")
            tracker.overall = "warning"
    else:
        tracker.mark_phase("send_email", "warning")
        tracker.add_summary("Email skipped because SMTP configuration or attachments were missing.")
        tracker.overall = "warning"

    final_finished_at = datetime.now(timezone.utc)
    final_record = tracker.build_record(final_finished_at)
    await update_summary_record(database_url, run_id, final_record)


def run_pipeline(env: str | None = None) -> None:
    asyncio.run(_run(env))


if __name__ == "__main__":
    run_pipeline()
