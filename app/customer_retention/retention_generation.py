"""Generate fresh RETENTION follow-up leads from retention snapshot rows."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Iterable, Mapping

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.dashboard_downloader.json_logger import JsonLogger, log_event

from .caps import resolve_active_cap
from .constants import CAP_WORK_SECTION_FRESH_RETENTION, LEAD_SOURCE_RETENTION, LEAD_STATUS_OPEN, LIFECYCLE_BUCKET_ACTIVE
from .db_tables import trx_customer_followup_leads
from .lifecycle import OPEN_LEAD_STATUSES
from .mobile import normalize_mobile
from .persistence import sqlite_next_id, stable_uuid
from .snapshot import SnapshotResult, CustomerRetentionSnapshotRow
from .workload import evaluate_retention_workload_freeze

RETENTION_SOURCE_SYSTEM = "CUSTOMER_RETENTION_PIPELINE"


@dataclass(frozen=True)
class RetentionLeadGenerationResult:
    rows_seen: int = 0
    leads_created: int = 0
    leads_reused: int = 0
    skipped_active: int = 0
    skipped_suppressed: int = 0
    skipped_invalid_mobile: int = 0
    skipped_existing_open: int = 0
    skipped_cap: int = 0
    skipped_frozen: int = 0
    skipped_inactive_store: int = 0
    warnings: tuple[str, ...] = field(default_factory=tuple)

    @property
    def rows_skipped(self) -> int:
        return self.skipped_active + self.skipped_suppressed + self.skipped_invalid_mobile + self.skipped_existing_open + self.skipped_cap + self.skipped_frozen + self.skipped_inactive_store

    @property
    def warning_count(self) -> int:
        return len(self.warnings)


async def generate_retention_leads_from_snapshot(
    session: AsyncSession,
    *,
    snapshot: SnapshotResult,
    pipeline_run_id: str,
    logger: JsonLogger | None = None,
    phase: str = "retention_generation",
    selected_rows_by_store: Mapping[str, Iterable[CustomerRetentionSnapshotRow]] | None = None,
) -> RetentionLeadGenerationResult:
    """Insert or reuse fresh RETENTION leads for eligible snapshot identities.

    The snapshot builder performs the primary eligibility filtering. This step
    still repeats the safety checks that protect the follow-up lead contract so
    callers can safely retry the generation phase within the same run.
    """

    rows = _flatten_selected_snapshot_rows(snapshot, selected_rows_by_store)
    created = reused = skipped_active = skipped_suppressed = skipped_invalid = skipped_open = 0
    warnings: list[str] = []

    for row in rows:
        mobile = normalize_mobile(row.normalized_mobile_number)
        if not mobile.is_valid:
            skipped_invalid += 1
            warnings.append("invalid_mobile_identity")
            continue
        if row.lifecycle_bucket == LIFECYCLE_BUCKET_ACTIVE or not row.eligible_for_retention:
            skipped_active += 1
            continue
        if row.suppression_status:
            skipped_suppressed += 1
            continue

        existing_same_run = await _find_existing_retention_run_lead(session, row=row, pipeline_run_id=pipeline_run_id)
        if existing_same_run is not None:
            reused += 1
            continue
        if await _has_open_identity(session, row=row):
            skipped_open += 1
            continue

        await _insert_retention_lead(session, row=row, pipeline_run_id=pipeline_run_id)
        created += 1

    result = RetentionLeadGenerationResult(
        rows_seen=len(rows),
        leads_created=created,
        leads_reused=reused,
        skipped_active=skipped_active,
        skipped_suppressed=skipped_suppressed,
        skipped_invalid_mobile=skipped_invalid,
        skipped_existing_open=skipped_open,
        warnings=tuple(warnings),
    )
    _log_generation_result(logger, result=result, pipeline_run_id=pipeline_run_id, phase=phase)
    return result


async def allocate_and_generate_retention_leads(
    session: AsyncSession,
    *,
    snapshot: SnapshotResult,
    active_stores: Iterable[str],
    run_date: date,
    backlog_threshold: int,
    pipeline_run_id: str,
    logger: JsonLogger | None = None,
    phase: str = "retention_allocation",
) -> RetentionLeadGenerationResult:
    """Allocate fresh RETENTION snapshot rows before inserting DB leads.

    Due follow-ups and pending carry-forward already exist in
    ``trx_customer_followup_leads`` and are intentionally outside this path; only
    same-day fresh RETENTION candidates are capped or frozen here.
    """

    active = {store.strip().upper() for store in active_stores}
    candidates_by_store: dict[str, list[CustomerRetentionSnapshotRow]] = {store: [] for store in active}
    skipped_inactive = 0
    for row in snapshot.rows:
        store = row.cost_center.strip().upper()
        if store not in active:
            skipped_inactive += 1
            continue
        candidates_by_store.setdefault(store, []).append(row)

    selected_by_store: dict[str, tuple[CustomerRetentionSnapshotRow, ...]] = {}
    skipped_cap = 0
    skipped_frozen = 0
    warnings: list[str] = []
    for store in sorted(active):
        store_rows = sorted(candidates_by_store.get(store, ()), key=lambda row: (-row.priority_score, row.normalized_mobile_number))
        if not store_rows:
            selected_by_store[store] = ()
            continue
        cap = await resolve_active_cap(
            session,
            lead_source_type=LEAD_SOURCE_RETENTION,
            work_section=CAP_WORK_SECTION_FRESH_RETENTION,
            cost_center=store,
            run_date=run_date,
        )
        workload = await evaluate_retention_workload_freeze(session, cost_center=store, run_date=run_date, threshold=backlog_threshold)
        if workload.frozen:
            skipped_frozen += len(store_rows)
            selected_by_store[store] = ()
            continue
        if cap.missing or not cap.valid:
            skipped_cap += len(store_rows)
            warnings.extend(warning.code for warning in cap.warnings)
            if cap.missing:
                warnings.append("missing_retention_cap")
            selected_by_store[store] = ()
            continue
        limit = None if cap.is_uncapped else cap.daily_cap
        selected = tuple(store_rows if limit is None else store_rows[: max(limit or 0, 0)])
        skipped_cap += max(len(store_rows) - len(selected), 0)
        selected_by_store[store] = selected

    result = await generate_retention_leads_from_snapshot(
        session,
        snapshot=snapshot,
        pipeline_run_id=pipeline_run_id,
        logger=logger,
        phase=phase,
        selected_rows_by_store=selected_by_store,
    )
    return RetentionLeadGenerationResult(
        rows_seen=result.rows_seen + skipped_cap + skipped_frozen + skipped_inactive,
        leads_created=result.leads_created,
        leads_reused=result.leads_reused,
        skipped_active=result.skipped_active,
        skipped_suppressed=result.skipped_suppressed,
        skipped_invalid_mobile=result.skipped_invalid_mobile,
        skipped_existing_open=result.skipped_existing_open,
        skipped_cap=skipped_cap,
        skipped_frozen=skipped_frozen,
        skipped_inactive_store=skipped_inactive,
        warnings=tuple([*result.warnings, *warnings]),
    )


def _flatten_selected_snapshot_rows(
    snapshot: SnapshotResult,
    selected_rows_by_store: Mapping[str, Iterable[CustomerRetentionSnapshotRow]] | None,
) -> tuple[CustomerRetentionSnapshotRow, ...]:
    if selected_rows_by_store is None:
        return snapshot.rows
    rows: list[CustomerRetentionSnapshotRow] = []
    for selected in selected_rows_by_store.values():
        rows.extend(selected)
    return tuple(rows)


async def _find_existing_retention_run_lead(session: AsyncSession, *, row: CustomerRetentionSnapshotRow, pipeline_run_id: str) -> int | None:
    existing = await session.execute(
        sa.select(trx_customer_followup_leads.c.lead_id)
        .where(
            trx_customer_followup_leads.c.lead_source_type == LEAD_SOURCE_RETENTION,
            trx_customer_followup_leads.c.cost_center == row.cost_center,
            trx_customer_followup_leads.c.normalized_mobile_number == row.normalized_mobile_number,
            trx_customer_followup_leads.c.lifecycle_bucket == row.lifecycle_bucket,
            trx_customer_followup_leads.c.created_by_pipeline_run_id == pipeline_run_id,
        )
        .limit(1)
    )
    lead_id = existing.scalar_one_or_none()
    return int(lead_id) if lead_id is not None else None


async def _has_open_identity(session: AsyncSession, *, row: CustomerRetentionSnapshotRow) -> bool:
    existing = await session.execute(
        sa.select(trx_customer_followup_leads.c.lead_id)
        .where(
            trx_customer_followup_leads.c.cost_center == row.cost_center,
            trx_customer_followup_leads.c.normalized_mobile_number == row.normalized_mobile_number,
            trx_customer_followup_leads.c.lead_status.in_(tuple(OPEN_LEAD_STATUSES)),
            trx_customer_followup_leads.c.is_closed.is_(False),
        )
        .limit(1)
    )
    return existing.scalar_one_or_none() is not None


async def _insert_retention_lead(session: AsyncSession, *, row: CustomerRetentionSnapshotRow, pipeline_run_id: str) -> int:
    now = datetime.now(timezone.utc)
    values: dict[str, Any] = {
        "lead_uuid": stable_uuid("retention", row.cost_center, row.normalized_mobile_number, row.lifecycle_bucket, pipeline_run_id),
        "lead_source_type": LEAD_SOURCE_RETENTION,
        "source_system": RETENTION_SOURCE_SYSTEM,
        "source_table_name": None,
        "source_record_id": None,
        "source_reference": None,
        "cost_center": row.cost_center,
        "customer_name": row.customer_name,
        "mobile_number": row.mobile_number,
        "normalized_mobile_number": row.normalized_mobile_number,
        "lead_date": row.snapshot_date,
        "lead_status": LEAD_STATUS_OPEN,
        "lifecycle_bucket": row.lifecycle_bucket,
        "last_order_date": row.last_order_date,
        "days_since_last_order": row.days_since_last_order,
        "total_orders": row.total_orders,
        "lifetime_spend": row.lifetime_spend,
        "average_order_value": row.average_order_value,
        "last_order_amount": row.last_order_amount,
        "priority_score": row.priority_score,
        "recommended_strategy": row.recommended_strategy,
        "assigned_store": row.cost_center,
        "created_by_pipeline_run_id": pipeline_run_id,
        "updated_by_pipeline_run_id": pipeline_run_id,
        "created_at": now,
        "updated_at": now,
    }
    next_id = await sqlite_next_id(session, trx_customer_followup_leads, "lead_id")
    if next_id is not None:
        values["lead_id"] = next_id
    result = await session.execute(trx_customer_followup_leads.insert().values(**values))
    return int(next_id or result.inserted_primary_key[0])


def _log_generation_result(logger: JsonLogger | None, *, result: RetentionLeadGenerationResult, pipeline_run_id: str, phase: str) -> None:
    if logger is None:
        return
    log_event(
        logger=logger,
        phase=phase,
        status="warning" if result.warning_count else "ok",
        message="retention_lead_generation_complete",
        run_id=pipeline_run_id,
        rows_seen=result.rows_seen,
        leads_created=result.leads_created,
        leads_reused=result.leads_reused,
        rows_skipped=result.rows_skipped,
        skipped_active=result.skipped_active,
        skipped_suppressed=result.skipped_suppressed,
        skipped_invalid_mobile=result.skipped_invalid_mobile,
        skipped_existing_open=result.skipped_existing_open,
        skipped_cap=result.skipped_cap,
        skipped_frozen=result.skipped_frozen,
        skipped_inactive_store=result.skipped_inactive_store,
        warning_count=result.warning_count,
        warning_codes=result.warnings,
    )
