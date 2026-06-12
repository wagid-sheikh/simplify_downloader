"""Rolling workload analytics for fresh RETENTION freeze decisions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from .constants import LEAD_SOURCE_RETENTION, LEAD_STATUS_DUE_FOLLOWUP, LEAD_STATUS_OPEN, LEAD_STATUS_PENDING, LEAD_STATUS_WORKED
from .db_tables import trx_customer_followup_leads
from .mobile import normalize_mobile

INCOMPLETE_STATUSES = (LEAD_STATUS_OPEN, LEAD_STATUS_PENDING, LEAD_STATUS_DUE_FOLLOWUP, LEAD_STATUS_WORKED)
WORKLOAD_WINDOW_DAYS = 14


@dataclass(frozen=True)
class WorkloadFreezeResult:
    cost_center: str
    run_date: date
    rolling_window_start: date
    incomplete_recent_retention_count: int
    older_carry_forward_count: int
    threshold: int
    frozen: bool
    reason_code: str | None = None


async def evaluate_retention_workload_freeze(
    session: AsyncSession,
    *,
    cost_center: str,
    run_date: date,
    threshold: int,
) -> WorkloadFreezeResult:
    """Freeze fresh RETENTION only when recent incomplete workload exceeds threshold.

    Older carry-forward remains actionable but is counted separately so stagnant
    old rows do not permanently block top-of-funnel generation.
    """

    window_start = run_date - timedelta(days=WORKLOAD_WINDOW_DAYS - 1)
    base_filters = (
        trx_customer_followup_leads.c.cost_center == cost_center,
        trx_customer_followup_leads.c.lead_source_type == LEAD_SOURCE_RETENTION,
        trx_customer_followup_leads.c.lead_status.in_(INCOMPLETE_STATUSES),
        trx_customer_followup_leads.c.is_closed.is_(False),
        trx_customer_followup_leads.c.is_recovered.is_(False),
        trx_customer_followup_leads.c.normalized_mobile_number.is_not(None),
        trx_customer_followup_leads.c.normalized_mobile_number != "",
    )
    recent_rows = (
        await session.execute(
            sa.select(trx_customer_followup_leads.c.normalized_mobile_number).where(
                *base_filters,
                trx_customer_followup_leads.c.lead_date >= window_start,
                trx_customer_followup_leads.c.lead_date <= run_date,
            )
        )
    ).scalars().all()
    older_rows = (
        await session.execute(
            sa.select(trx_customer_followup_leads.c.normalized_mobile_number).where(
                *base_filters,
                trx_customer_followup_leads.c.lead_date < window_start,
            )
        )
    ).scalars().all()
    recent_count = sum(1 for value in recent_rows if normalize_mobile(value).is_valid)
    older_count = sum(1 for value in older_rows if normalize_mobile(value).is_valid)
    frozen = recent_count > threshold
    return WorkloadFreezeResult(
        cost_center=cost_center,
        run_date=run_date,
        rolling_window_start=window_start,
        incomplete_recent_retention_count=recent_count,
        older_carry_forward_count=older_count,
        threshold=threshold,
        frozen=frozen,
        reason_code="recent_retention_backlog_exceeded" if frozen else None,
    )
