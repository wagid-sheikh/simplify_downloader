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
from .external_import import _import_external_lead_file
from .input_discovery import archive_processed_file, discover_external_lead_files, discover_returned_workbooks, get_customer_followup_paths
from .notifications import NotificationResult, send_owner_summary
from .recovery_detection import detect_recoveries
from .retention_generation import generate_retention_leads_from_snapshot
from .snapshot import build_customer_retention_snapshot
from .source_adapters import _import_td_leads
from .types import RowWarning
from .workbook_generator import WorkbookGenerationResult, generate_workbooks
from .workbook_ingestor import _ingest_returned_workbook
from .workbook_selection import load_active_retention_stores, select_workbook_leads_for_active_stores


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


class CustomerRetentionNotificationError(RuntimeError):
    """Raised when the SRS-required owner summary cannot be delivered.

    Business data is committed before delivery is attempted so that owner
    summaries never announce uncommitted work. The attached run_result preserves
    generated workbook and archive traces for operator recovery after the hard
    notification failure.
    """

    def __init__(self, message: str, *, run_result: CustomerRetentionRunResult):
        super().__init__(message)
        self.run_result = run_result


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
    """Run customer retention in the SRS Section 9 orchestration order.

    Dry-run mode is intentionally read-only: it may discover input files, build
    the current snapshot, select would-be workbook rows, and build the summary
    payload, but it must not ingest/import rows, detect and persist recoveries,
    generate retention leads, write XLSX workbooks, archive inputs, or send
    email. Dry-run counts are therefore reported as ``planned_*`` values for
    would-be side effects, while mutation counters such as ``*_created`` or
    ``*_inserted`` are only populated by real runs.
    """
    started = datetime.now(timezone.utc)

    # 1. Load config and logging. Keep all config reads centralized here so the
    # orchestration order is explicit and dry-runs can avoid mutating side effects.
    actual_run_id = run_id or new_run_id()
    actual_run_date = run_date or date.today()
    db_url = database_url or config.database_url
    log = logger or get_logger(run_id=actual_run_id)
    backlog_threshold = getattr(config, "customer_followup_backlog_warning_threshold", 20)
    output_root = Path(config.customer_followup_output_dir).expanduser()

    counts: dict[str, int] = {}
    warnings: list[str] = []
    row_warnings: list[RowWarning] = []
    generated_files: list[str] = []
    workbook_result: WorkbookGenerationResult | None = None
    notification = NotificationResult(planned=0, sent=0, skipped=True, reason="not_attempted")
    ingestion_results = []
    selections = []
    processed_files: list[tuple[Path, dict[str, Any]]] = []
    archived_files: list[str] = []

    def count(name: str, value: int) -> None:
        counts[name] = counts.get(name, 0) + int(value)

    def add_warnings(source_warnings: Any) -> None:
        """Preserve structured warnings and wrap code-only warnings.

        Some producers can only emit run-level warning codes because there is no
        row context. Convert those strings into RowWarning objects so management
        analytics receives one structured stream regardless of source.
        """
        for warning in source_warnings or ():
            if isinstance(warning, RowWarning):
                row_warning = warning
            elif hasattr(warning, "code"):
                code = str(warning.code)
                row_warning = RowWarning(code=code, message=str(getattr(warning, "message", code)))
            else:
                code = str(warning)
                row_warning = RowWarning(code=code, message=code)
            row_warnings.append(row_warning)
            warnings.append(row_warning.code)

    try:
        log_event(logger=log, phase="config", status="ok", message="customer_retention_pipeline_started", run_id=actual_run_id, extras={"run_date": actual_run_date.isoformat(), "dry_run": dry_run, "env": env})
        paths = get_customer_followup_paths()

        async with session_scope(db_url) as session:
            # 2. Fetch active stores.
            active_stores = await load_active_retention_stores(session, logger=log, run_id=actual_run_id, phase="store_load", pipeline="customer_retention_pipeline", run_date=actual_run_date)
            count("active_stores", len(active_stores))

            # 3. Discover/ingest returned workbooks.
            returned_files = discover_returned_workbooks(logger=log)
            count("returned_files_discovered", len(returned_files))
            if dry_run:
                count("planned_returned_workbooks_to_ingest", len(returned_files))
            if not dry_run:
                for discovered in returned_files:
                    result = await _ingest_returned_workbook(session, discovered.path, actual_run_id, run_date=actual_run_date, logger=log)
                    ingestion_results.append(result)
                    count("workbook_rows_seen", result.rows_seen)
                    count("workbook_history_inserted", result.history_inserted)
                    add_warnings(result.warnings)
                    processed_files.append((discovered.path, {"rows_seen": result.rows_seen}))
            else:
                log_event(
                    logger=log,
                    phase="ingest",
                    status="info",
                    message="customer_retention_dry_run_skips_mutating_ingest",
                    run_id=actual_run_id,
                    extras={"planned_returned_workbooks_to_ingest": len(returned_files)},
                )

            # 4. Returned workbook ingestion updates lifecycle/history/suppression
            # through the existing ingestion/lifecycle functions above.

            # 5. Detect recoveries.
            if not dry_run:
                recovery = await detect_recoveries(session, as_of_date=actual_run_date, pipeline_run_id=actual_run_id)
                count("leads_recovered", recovery.leads_recovered)
                count("leads_closed_by_recovery", recovery.leads_closed)
                await session.flush()

            # 6. Build retention snapshot.
            snapshot = await build_customer_retention_snapshot(session, snapshot_date=actual_run_date)
            count("snapshot_rows", len(snapshot.rows))
            count("snapshot_invalid_mobile_rows", snapshot.rows_invalid_mobile)

            # 7. Discover/import external lead files.
            external_files = discover_external_lead_files(logger=log)
            count("external_files_discovered", len(external_files))
            if dry_run:
                count("planned_external_files_to_import", len(external_files))
                count("planned_td_imports", 1)
                count("planned_retention_snapshot_generations", 1)
            if not dry_run:
                for discovered in external_files:
                    result = await _import_external_lead_file(session, discovered.path, actual_run_id, logger=log)
                    count("external_rows_seen", result.rows_seen)
                    count("external_leads_created", result.leads_created)
                    add_warnings(result.warnings)
                    processed_files.append((discovered.path, {"rows_seen": result.rows_seen}))

                # 8. Pull/convert TD leads.
                td_result = await _import_td_leads(session, actual_run_id, logger=log)
                count("td_rows_seen", td_result.rows_seen)
                count("td_leads_created", td_result.leads_created)
                add_warnings(td_result.warnings)

                # 9. Generate fresh retention leads.
                retention_generation = await generate_retention_leads_from_snapshot(session, snapshot=snapshot, pipeline_run_id=actual_run_id, logger=log)
                count("retention_rows_seen", retention_generation.rows_seen)
                count("retention_leads_created", retention_generation.leads_created)
                count("retention_leads_reused", retention_generation.leads_reused)
                count("retention_rows_skipped", retention_generation.rows_skipped)
                add_warnings(retention_generation.warnings)
                await session.flush()

            # 10. Select due follow-ups, carry-forward, TD, external, and fresh retention rows.
            selections = await select_workbook_leads_for_active_stores(session, run_date=actual_run_date, backlog_threshold=backlog_threshold, logger=log, run_id=actual_run_id, phase="workbook", pipeline="customer_retention_pipeline")
            selected_rows = sum(len(selection.rows) for selection in selections)
            for selection in selections:
                add_warnings(getattr(selection, "warnings", ()))
            if dry_run:
                count("planned_workbook_rows_selected", selected_rows)
                count("planned_workbooks_to_generate", len(selections))
                count("planned_files_to_archive", len(returned_files) + len(external_files))
                count("planned_summary_emails", 0 if skip_email else 1)
                count("dry_run_backlog_threshold", backlog_threshold)
            else:
                count("workbook_rows_selected", selected_rows)

            # 11. Generate workbooks.
            if not dry_run:
                workbook_result = generate_workbooks(
                    selections=selections,
                    active_cost_centers=active_stores,
                    output_root=output_root,
                    generated_at=started,
                    logger=log,
                )
                generated_files = [str(output.output_path) for output in workbook_result.outputs]

                # 12. Archive processed input/import files deterministically after
                # successful processing policy is satisfied (all processing through
                # workbook generation has completed without raising).
                for path, metadata in sorted(processed_files, key=lambda item: str(item[0])):
                    archived_path = archive_processed_file(path, archive_dir=paths.archive_dir, run_id=actual_run_id, result_metadata=metadata, logger=log)
                    archived_files.append(str(archived_path))
                count("files_archived", len(archived_files))

            # 13. Build/send summary email. Dry-runs build the payload but do not
            # call the sender so the run remains non-mutating and side-effect free.
            summary_payload = await build_management_summary_payload(session, run_id=actual_run_id, run_date=actual_run_date, timing=RunTiming(run_id=actual_run_id, started_at=started, ended_at=datetime.now(timezone.utc), execution_mode="dry_run" if dry_run else "manual", status="success", env=env), selections=selections, workbook_result=workbook_result, ingestion_results=ingestion_results, generated_files=generated_files, row_warnings=row_warnings)
            if not dry_run:
                # The management payload must see this transaction's uncommitted
                # lead/history/suppression changes, but success email must never
                # be sent for data that failed to commit. Commit first, then send.
                await session.commit()
                try:
                    notification = await send_owner_summary(session, payload=summary_payload, env=env, skip_email=skip_email, logger=log)
                except Exception as exc:
                    failure_code = "email_delivery_failed_after_commit"
                    notification = NotificationResult(planned=1, sent=0, skipped=False, reason=failure_code)
                    log_event(
                        logger=log,
                        phase="email",
                        status="error",
                        message="customer_retention_success_notification_failed_after_commit",
                        run_id=actual_run_id,
                        error=str(exc),
                        extras={"generated_files": generated_files, "archived_files": archived_files},
                    )
                    email_status = notification.__dict__ | {
                        "error": str(exc),
                        "committed": True,
                        "generated_files": generated_files,
                        "archived_files": archived_files,
                    }
                    failure_result = CustomerRetentionRunResult(
                        actual_run_id,
                        actual_run_date,
                        "failed",
                        counts,
                        [*warnings, failure_code],
                        generated_files,
                        email_status,
                        summary_payload,
                    )
                    raise CustomerRetentionNotificationError(
                        "customer retention owner summary email delivery failed after commit",
                        run_result=failure_result,
                    ) from exc
                else:
                    email_status = notification.__dict__ | {"committed": True}
            else:
                # Enforce dry-run read-only semantics at the transaction boundary.
                await session.rollback()
                email_status = notification.__dict__
        status = "success_with_warnings" if warnings else "success"
        log_event(logger=log, phase="email", status="ok", message="customer_retention_pipeline_completed", run_id=actual_run_id, extras={"status": status, "counts": counts})
        return CustomerRetentionRunResult(actual_run_id, actual_run_date, status, counts, warnings, generated_files, email_status, summary_payload)
    except Exception as exc:
        log_event(logger=log, phase="email", status="error", message="customer_retention_pipeline_failed", run_id=actual_run_id, error=str(exc))
        raise
