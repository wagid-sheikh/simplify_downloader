from __future__ import annotations

import asyncio
from argparse import ArgumentParser
from datetime import date, datetime, timezone

from app.common.date_utils import aware_now, get_timezone
from app.config import config
from app.dashboard_downloader.json_logger import get_logger, log_event, new_run_id
from app.dashboard_downloader.pipelines.base import (
    PipelinePhaseTracker,
    persist_summary_record,
    resolve_run_env,
)
from app.reports.pending_deliveries.data import (
    transition_aged_pending_deliveries_to_recovery_metrics,
)

PIPELINE_NAME = "recovery.mark_aged_pending_deliveries"


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid date format: {value}. Use YYYY-MM-DD.") from exc


async def _run(report_date: date | None, env: str | None) -> None:
    run_id = new_run_id()
    run_env = resolve_run_env(env)
    logger = get_logger(run_id=run_id)
    tracker = PipelinePhaseTracker(pipeline_name=PIPELINE_NAME, env=run_env, run_id=run_id)

    database_url = config.database_url
    if not database_url:
        tracker.mark_phase("mark_aged_pending_deliveries", "error")
        tracker.add_summary("Database URL is missing; cannot mark aged pending deliveries.")
        tracker.overall = "error"
        return

    resolved_report_date = report_date or aware_now(get_timezone()).date()
    tracker.set_report_date(resolved_report_date)

    metrics = await transition_aged_pending_deliveries_to_recovery_metrics(
        database_url=database_url,
        report_date=resolved_report_date,
    )
    tracker.mark_phase("mark_aged_pending_deliveries", "ok")
    tracker.metrics = {"report_date": resolved_report_date.isoformat(), **metrics.to_dict()}
    tracker.add_summary(
        f"Scanned {metrics.scanned_count} pending orders; transitioned {metrics.transitioned_count}."
    )

    log_event(
        logger=logger,
        phase="mark_aged_pending_deliveries",
        message="aged pending deliveries recovery marking completed",
        **tracker.metrics,
    )
    finished_at = datetime.now(timezone.utc)
    await persist_summary_record(database_url, tracker.build_record(finished_at))


def main(argv: Sequence[str] | None = None) -> None:
    parser = ArgumentParser(description="Mark aged pending deliveries to TO_BE_RECOVERED.")
    parser.add_argument("--report-date", type=_parse_date, help="Report date (YYYY-MM-DD).")
    parser.add_argument("--env", type=str, default=None, help="Override run environment.")
    args = parser.parse_args(list(argv) if argv is not None else None)
    asyncio.run(_run(report_date=args.report_date, env=args.env))


if __name__ == "__main__":
    main()
