"""Generate fresh RETENTION follow-up leads from retention snapshot rows."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.dashboard_downloader.json_logger import JsonLogger, log_event

from .constants import LEAD_SOURCE_RETENTION, LEAD_STATUS_OPEN, LIFECYCLE_BUCKET_ACTIVE
from .db_tables import trx_customer_followup_leads
from .lifecycle import OPEN_LEAD_STATUSES
from .mobile import normalize_mobile
from .persistence import sqlite_next_id, stable_uuid
from .snapshot import SnapshotResult, CustomerRetentionSnapshotRow

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
    warnings: tuple[str, ...] = field(default_factory=tuple)

    @property
    def rows_skipped(self) -> int:
        return self.skipped_active + self.skipped_suppressed + self.skipped_invalid_mobile + self.skipped_existing_open

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
) -> RetentionLeadGenerationResult:
    """Insert or reuse fresh RETENTION leads for eligible snapshot identities.

    The snapshot builder performs the primary eligibility filtering. This step
    still repeats the safety checks that protect the follow-up lead contract so
    callers can safely retry the generation phase within the same run.
    """

    created = reused = skipped_active = skipped_suppressed = skipped_invalid = skipped_open = 0
    warnings: list[str] = []

    for row in snapshot.rows:
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
        rows_seen=len(snapshot.rows),
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
        warning_count=result.warning_count,
        warning_codes=result.warnings,
    )
