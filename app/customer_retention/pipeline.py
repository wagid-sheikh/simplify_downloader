"""Orchestration entry point for the customer retention pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from app.common.db import session_scope
from app.config import config
from app.dashboard_downloader.json_logger import JsonLogger, get_logger, log_event, new_run_id

from .analytics import RunTiming, build_management_summary_payload
from .external_import import import_external_lead_file
from .input_discovery import archive_processed_file, discover_external_lead_files, discover_returned_workbooks, get_customer_followup_paths
from .notifications import NotificationResult, send_owner_summary
from .recovery_detection import detect_recoveries
from .retention_generation import generate_retention_leads_from_snapshot
from .snapshot import build_customer_retention_snapshot
from .source_adapters import import_td_leads
from .workbook_generator import WorkbookGenerationResult, default_customer_followup_output_root, generate_workbooks
from .workbook_ingestor import ingest_returned_workbook
from .workbook_selection import load_active_retention_stores, select_workbook_leads_for_active_stores

DEFAULT_BACKLOG_THRESHOLD = 50


@dataclass(frozen=True)
class CustomerRetentionRunResult:
    run_id: str
    run_date: date
    status: str
    counts: dict[str, int]
    warnings: list[str] = field(default_factory=list)
    generated_files: list[str] = field(default_factory=list)
    email_status: dict[str, Any] = field(default_factory=dict)
    summary_payload: dict[str, Any] = field(default_factory=dict)


async def run_customer_retention_pipeline(
    *,
    run_date: date | None = None,
    run_id: str | None = None,
    env: str | None = None,
    dry_run: bool = False,
    skip_email: bool = False,
    database_url: str | None = None,
    logger: JsonLogger | None = None,
) -> CustomerRetentionRunResult:
    started = datetime.now(timezone.utc)
    actual_run_id = run_id or new_run_id()
    actual_run_date = run_date or date.today()
    db_url = database_url or config.database_url
    log = logger or get_logger(run_id=actual_run_id)
    counts: dict[str, int] = {}
    warnings: list[str] = []
    generated_files: list[str] = []
    workbook_result: WorkbookGenerationResult | None = None
    notification = NotificationResult(planned=0, sent=0, skipped=True, reason="not_attempted")

    def count(name: str, value: int) -> None:
        counts[name] = counts.get(name, 0) + int(value)

    try:
        log_event(logger=log, phase="config", status="ok", message="customer_retention_pipeline_started", run_id=actual_run_id, extras={"run_date": actual_run_date.isoformat(), "dry_run": dry_run, "env": env})
        paths = get_customer_followup_paths()
        returned_files = discover_returned_workbooks(logger=log)
        external_files = discover_external_lead_files(logger=log)
        count("returned_files_discovered", len(returned_files))
        count("external_files_discovered", len(external_files))

        ingestion_results = []
        if not dry_run:
            for discovered in returned_files:
                result = await ingest_returned_workbook(database_url=db_url, path=discovered.path, pipeline_run_id=actual_run_id, logger=log)
                ingestion_results.append(result)
                count("workbook_rows_seen", result.rows_seen)
                count("workbook_history_inserted", result.history_inserted)
                warnings.extend(w.code for w in result.warnings)
                archive_processed_file(discovered.path, archive_dir=paths.archive_dir, run_id=actual_run_id, result_metadata={"rows_seen": result.rows_seen}, logger=log)
            for discovered in external_files:
                result = await import_external_lead_file(database_url=db_url, path=discovered.path, pipeline_run_id=actual_run_id, logger=log)
                count("external_rows_seen", result.rows_seen)
                count("external_leads_created", result.leads_created)
                warnings.extend(w.code for w in result.warnings)
                archive_processed_file(discovered.path, archive_dir=paths.archive_dir, run_id=actual_run_id, result_metadata={"rows_seen": result.rows_seen}, logger=log)
            td_result = await import_td_leads(database_url=db_url, pipeline_run_id=actual_run_id, logger=log)
            count("td_rows_seen", td_result.rows_seen)
            count("td_leads_created", td_result.leads_created)
            warnings.extend(w.code for w in td_result.warnings)
        else:
            log_event(logger=log, phase="ingest", status="info", message="customer_retention_dry_run_skips_mutating_ingest", run_id=actual_run_id)

        async with session_scope(db_url) as session:
            active_stores = await load_active_retention_stores(session, logger=log, run_id=actual_run_id, phase="store_load", pipeline="customer_retention_pipeline", run_date=actual_run_date)
            count("active_stores", len(active_stores))
            if not dry_run:
                recovery = await detect_recoveries(session, as_of_date=actual_run_date, pipeline_run_id=actual_run_id)
                count("leads_recovered", recovery.leads_recovered)
                count("leads_closed_by_recovery", recovery.leads_closed)
                await session.commit()
            snapshot = await build_customer_retention_snapshot(session, snapshot_date=actual_run_date)
            count("snapshot_rows", len(snapshot.rows))
            count("snapshot_invalid_mobile_rows", snapshot.rows_invalid_mobile)
            if not dry_run:
                retention_generation = await generate_retention_leads_from_snapshot(session, snapshot=snapshot, pipeline_run_id=actual_run_id, logger=log)
                count("retention_rows_seen", retention_generation.rows_seen)
                count("retention_leads_created", retention_generation.leads_created)
                count("retention_leads_reused", retention_generation.leads_reused)
                count("retention_rows_skipped", retention_generation.rows_skipped)
                warnings.extend(retention_generation.warnings)
                await session.flush()
            selections = await select_workbook_leads_for_active_stores(session, run_date=actual_run_date, backlog_threshold=DEFAULT_BACKLOG_THRESHOLD, logger=log, run_id=actual_run_id, phase="workbook", pipeline="customer_retention_pipeline")
            count("workbook_rows_selected", sum(len(s.rows) for s in selections))
            if not dry_run:
                workbook_result = generate_workbooks(selections=selections, active_cost_centers=active_stores, output_root=default_customer_followup_output_root(), generated_at=started, logger=log)
                generated_files = [str(output.output_path) for output in workbook_result.outputs]
            summary_payload = await build_management_summary_payload(session, run_id=actual_run_id, run_date=actual_run_date, timing=RunTiming(run_id=actual_run_id, started_at=started, ended_at=datetime.now(timezone.utc), execution_mode="dry_run" if dry_run else "manual", status="success", env=env), selections=selections, workbook_result=workbook_result, ingestion_results=ingestion_results, generated_files=generated_files)
            notification = await send_owner_summary(session, payload=summary_payload, env=env, skip_email=(skip_email or dry_run), logger=log)
            await session.commit()
        status = "success_with_warnings" if warnings else "success"
        log_event(logger=log, phase="email", status="ok", message="customer_retention_pipeline_completed", run_id=actual_run_id, extras={"status": status, "counts": counts})
        return CustomerRetentionRunResult(actual_run_id, actual_run_date, status, counts, warnings, generated_files, notification.__dict__, summary_payload)
    except Exception as exc:
        log_event(logger=log, phase="email", status="error", message="customer_retention_pipeline_failed", run_id=actual_run_id, error=str(exc))
        raise
