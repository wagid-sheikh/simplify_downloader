"""Phase 3 suppression lookup, creation, and approval workflow.

All suppression identity is store scoped: ``(cost_center, normalized_mobile_number)``.
The functions accept an active SQLAlchemy async session so callers can commit
state changes and history rows in one transaction.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from .constants import (
    SUPPRESSION_STATE_ACTIVE,
    SUPPRESSION_STATE_PENDING_APPROVAL,
    SUPPRESSION_STATE_REJECTED,
    WORKBOOK_OUTCOME_LEAD_STALE,
    WORKBOOK_OUTCOME_NOT_INTERESTED,
)
from .db_tables import trx_customer_followup_leads, trx_customer_suppression
from .mobile import normalize_mobile
from .persistence import insert_history_once, sqlite_next_id

DEFAULT_TIME_BOUND_SUPPRESSION_DAYS = 90
TIME_BOUND_SUPPRESSION_DAYS_BY_REASON = {
    WORKBOOK_OUTCOME_NOT_INTERESTED: DEFAULT_TIME_BOUND_SUPPRESSION_DAYS,
    WORKBOOK_OUTCOME_LEAD_STALE: DEFAULT_TIME_BOUND_SUPPRESSION_DAYS,
}


@dataclass(frozen=True)
class SuppressionDecision:
    is_suppressed: bool
    suppression_id: int | None = None
    suppression_state: str | None = None
    suppression_reason: str | None = None
    suppression_until: date | None = None
    is_permanent: bool = False


@dataclass(frozen=True)
class SuppressionApprovalResult:
    suppression_id: int
    action: str
    changed: bool
    history_inserted: bool


def is_active_suppression_row(row: sa.RowMapping | dict[str, Any], *, as_of_date: date) -> bool:
    if row["suppression_state"] != SUPPRESSION_STATE_ACTIVE:
        return False
    if row["suppression_start_date"] and row["suppression_start_date"] > as_of_date:
        return False
    if row["is_permanent"]:
        return True
    return row["suppression_until"] is not None and row["suppression_until"] >= as_of_date


async def check_active_suppression(
    session: AsyncSession,
    *,
    cost_center: str,
    normalized_mobile_number: str | None,
    as_of_date: date,
) -> SuppressionDecision:
    if not normalized_mobile_number or not normalize_mobile(normalized_mobile_number).is_valid:
        return SuppressionDecision(False)
    rows = (
        await session.execute(
            sa.select(trx_customer_suppression)
            .where(
                trx_customer_suppression.c.cost_center == cost_center,
                trx_customer_suppression.c.normalized_mobile_number == normalized_mobile_number,
            )
            .order_by(trx_customer_suppression.c.suppression_id.desc())
        )
    ).mappings().all()
    for row in rows:
        if is_active_suppression_row(row, as_of_date=as_of_date):
            return SuppressionDecision(True, int(row["suppression_id"]), row["suppression_state"], row["suppression_reason"], row["suppression_until"], bool(row["is_permanent"]))
    return SuppressionDecision(False)


async def create_time_bound_suppression(
    session: AsyncSession,
    *,
    cost_center: str,
    normalized_mobile_number: str,
    reason: str,
    start_date: date,
    source_lead_id: int | None,
    pipeline_run_id: str | None,
    mobile_number: str | None = None,
) -> SuppressionDecision:
    if not normalize_mobile(normalized_mobile_number).is_valid:
        return SuppressionDecision(False)
    duration_days = TIME_BOUND_SUPPRESSION_DAYS_BY_REASON.get(reason, DEFAULT_TIME_BOUND_SUPPRESSION_DAYS)
    suppression_until = start_date + timedelta(days=duration_days)
    existing = (
        await session.execute(
            sa.select(trx_customer_suppression)
            .where(
                trx_customer_suppression.c.cost_center == cost_center,
                trx_customer_suppression.c.normalized_mobile_number == normalized_mobile_number,
                trx_customer_suppression.c.suppression_reason == reason,
                trx_customer_suppression.c.suppression_state == SUPPRESSION_STATE_ACTIVE,
                trx_customer_suppression.c.is_permanent.is_(False),
                trx_customer_suppression.c.suppression_start_date == start_date,
                trx_customer_suppression.c.suppression_until == suppression_until,
                trx_customer_suppression.c.source_lead_id == source_lead_id,
            )
            .limit(1)
        )
    ).mappings().first()
    if existing is not None:
        return SuppressionDecision(True, int(existing["suppression_id"]), SUPPRESSION_STATE_ACTIVE, reason, suppression_until, False)
    values = {
        "cost_center": cost_center,
        "mobile_number": mobile_number,
        "normalized_mobile_number": normalized_mobile_number,
        "suppression_reason": reason,
        "suppression_state": SUPPRESSION_STATE_ACTIVE,
        "suppression_start_date": start_date,
        "suppression_until": suppression_until,
        "is_permanent": False,
        "approval_required": False,
        "source_lead_id": source_lead_id,
        "created_at": datetime.now(timezone.utc),
        "created_by_pipeline_run_id": pipeline_run_id,
    }
    next_id = await sqlite_next_id(session, trx_customer_suppression, "suppression_id")
    if next_id is not None:
        values["suppression_id"] = next_id
    result = await session.execute(trx_customer_suppression.insert().values(**values))
    suppression_id = int(next_id or result.inserted_primary_key[0])
    if source_lead_id is not None:
        await insert_history_once(
            session,
            lead_id=source_lead_id,
            pipeline_run_id=pipeline_run_id,
            event_type=f"SUPPRESSION_TIME_BOUND:{suppression_id}",
            previous_status=None,
            new_status=None,
            raw_excel_value_json=None,
            normalized_value_json={"suppression_id": suppression_id, "reason": reason, "suppression_until": str(suppression_until)},
        )
    return SuppressionDecision(True, suppression_id, SUPPRESSION_STATE_ACTIVE, reason, suppression_until, False)


async def create_pending_permanent_suppression(
    session: AsyncSession,
    *,
    cost_center: str,
    normalized_mobile_number: str,
    reason: str,
    start_date: date,
    source_lead_id: int,
    pipeline_run_id: str | None,
    mobile_number: str | None = None,
) -> SuppressionDecision:
    if not normalize_mobile(normalized_mobile_number).is_valid:
        return SuppressionDecision(False)
    existing = (
        await session.execute(
            sa.select(trx_customer_suppression)
            .where(
                trx_customer_suppression.c.cost_center == cost_center,
                trx_customer_suppression.c.normalized_mobile_number == normalized_mobile_number,
                trx_customer_suppression.c.suppression_reason == reason,
                trx_customer_suppression.c.suppression_state == SUPPRESSION_STATE_PENDING_APPROVAL,
                trx_customer_suppression.c.is_permanent.is_(True),
                trx_customer_suppression.c.source_lead_id == source_lead_id,
            )
            .limit(1)
        )
    ).mappings().first()
    if existing is not None:
        return SuppressionDecision(False, int(existing["suppression_id"]), SUPPRESSION_STATE_PENDING_APPROVAL, reason, None, True)
    values = {
        "cost_center": cost_center,
        "mobile_number": mobile_number,
        "normalized_mobile_number": normalized_mobile_number,
        "suppression_reason": reason,
        "suppression_state": SUPPRESSION_STATE_PENDING_APPROVAL,
        "suppression_start_date": start_date,
        "suppression_until": None,
        "is_permanent": True,
        "approval_required": True,
        "source_lead_id": source_lead_id,
        "created_at": datetime.now(timezone.utc),
        "created_by_pipeline_run_id": pipeline_run_id,
    }
    next_id = await sqlite_next_id(session, trx_customer_suppression, "suppression_id")
    if next_id is not None:
        values["suppression_id"] = next_id
    result = await session.execute(trx_customer_suppression.insert().values(**values))
    suppression_id = int(next_id or result.inserted_primary_key[0])
    await insert_history_once(
        session,
        lead_id=source_lead_id,
        pipeline_run_id=pipeline_run_id,
        event_type=f"SUPPRESSION_PENDING_APPROVAL:{suppression_id}",
        previous_status=None,
        new_status=None,
        raw_excel_value_json=None,
        normalized_value_json={"suppression_id": suppression_id, "reason": reason},
    )
    return SuppressionDecision(False, suppression_id, SUPPRESSION_STATE_PENDING_APPROVAL, reason, None, True)


async def approve_suppression(
    session: AsyncSession,
    *,
    suppression_id: int,
    approved_by: str,
    pipeline_run_id: str | None,
    approval_remarks: str | None = None,
) -> SuppressionApprovalResult:
    return await _resolve_pending_suppression(session, suppression_id=suppression_id, action="APPROVED", resolved_by=approved_by, pipeline_run_id=pipeline_run_id, remarks=approval_remarks)


async def reject_suppression(
    session: AsyncSession,
    *,
    suppression_id: int,
    rejected_by: str,
    pipeline_run_id: str | None,
    rejection_remarks: str | None = None,
) -> SuppressionApprovalResult:
    return await _resolve_pending_suppression(session, suppression_id=suppression_id, action="REJECTED", resolved_by=rejected_by, pipeline_run_id=pipeline_run_id, remarks=rejection_remarks)


async def _resolve_pending_suppression(
    session: AsyncSession,
    *,
    suppression_id: int,
    action: str,
    resolved_by: str,
    pipeline_run_id: str | None,
    remarks: str | None,
) -> SuppressionApprovalResult:
    row = (await session.execute(sa.select(trx_customer_suppression).where(trx_customer_suppression.c.suppression_id == suppression_id))).mappings().first()
    if row is None:
        raise ValueError(f"Unknown suppression_id={suppression_id}")
    target_state = SUPPRESSION_STATE_ACTIVE if action == "APPROVED" else SUPPRESSION_STATE_REJECTED
    changed = row["suppression_state"] == SUPPRESSION_STATE_PENDING_APPROVAL
    if changed:
        await session.execute(
            trx_customer_suppression.update()
            .where(trx_customer_suppression.c.suppression_id == suppression_id)
            .values(
                suppression_state=target_state,
                approval_required=False,
                approved_at=datetime.now(timezone.utc),
                approved_by=resolved_by,
                approval_remarks=remarks,
            )
        )
    history_inserted = False
    if row["source_lead_id"] is not None:
        history_inserted = await insert_history_once(
            session,
            lead_id=int(row["source_lead_id"]),
            pipeline_run_id=pipeline_run_id,
            event_type=f"SUPPRESSION_{action}:{suppression_id}",
            previous_status=str(row["suppression_state"]),
            new_status=target_state,
            raw_excel_value_json=None,
            normalized_value_json={"suppression_id": suppression_id, "action": action, "resolved_by": resolved_by},
        )
    return SuppressionApprovalResult(suppression_id, action, changed, history_inserted)
