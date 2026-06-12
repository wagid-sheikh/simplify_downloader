"""Select customer retention leads for per-store follow-up workbooks."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from .caps import CapResolutionResult, resolve_active_cap
from .constants import (
    CAP_WORK_SECTION_EXTERNAL_LEAD,
    CAP_WORK_SECTION_FRESH_RETENTION,
    CAP_WORK_SECTION_PENDING_CARRY_FORWARD,
    CAP_WORK_SECTION_TD_LEAD,
    CAP_WORK_SECTION_DUE_FOLLOWUP,
    LEAD_SOURCE_EXTERNAL,
    LEAD_SOURCE_RETENTION,
    LEAD_SOURCE_TD,
    LEAD_STATUS_DUE_FOLLOWUP,
    LEAD_STATUS_OPEN,
    LEAD_STATUS_PENDING,
    LEAD_STATUS_WORKED,
)
from .db_tables import trx_customer_followup_leads
from .mobile import normalize_mobile
from .persistence import fetch_active_cost_centers
from .suppression import check_active_suppression
from .types import RowWarning
from .workload import WorkloadFreezeResult, evaluate_retention_workload_freeze

ACTIONABLE_STATUSES = (LEAD_STATUS_OPEN, LEAD_STATUS_PENDING, LEAD_STATUS_DUE_FOLLOWUP, LEAD_STATUS_WORKED)
CATEGORY_ORDER = {
    CAP_WORK_SECTION_DUE_FOLLOWUP: 0,
    CAP_WORK_SECTION_PENDING_CARRY_FORWARD: 1,
    CAP_WORK_SECTION_TD_LEAD: 2,
    CAP_WORK_SECTION_EXTERNAL_LEAD: 3,
    CAP_WORK_SECTION_FRESH_RETENTION: 4,
}


@dataclass(frozen=True)
class WorkbookLeadRow:
    lead_id: int
    lead_source_type: str
    work_section: str
    cost_center: str
    customer_name: str | None
    mobile_number: str | None
    normalized_mobile_number: str
    lifecycle_bucket: str | None
    last_order_date: date | None
    days_since_last_order: int | None
    total_orders: int | None
    lifetime_spend: Decimal | None
    average_order_value: Decimal | None
    last_order_amount: Decimal | None
    priority_score: Decimal | None
    recommended_strategy: str | None
    lead_date: date
    next_followup_date: date | None
    generated_at: datetime | None


@dataclass(frozen=True)
class StoreWorkbookSelectionResult:
    cost_center: str
    run_date: date
    rows: tuple[WorkbookLeadRow, ...]
    counts_by_category: dict[str, int]
    warnings: tuple[RowWarning, ...] = field(default_factory=tuple)
    retention_cap: CapResolutionResult | None = None
    external_cap: CapResolutionResult | None = None
    workload: WorkloadFreezeResult | None = None


async def load_active_retention_stores(session: AsyncSession) -> tuple[str, ...]:
    return tuple(sorted(await fetch_active_cost_centers(session)))


async def select_workbook_leads_for_store(
    session: AsyncSession,
    *,
    cost_center: str,
    run_date: date,
    backlog_threshold: int,
) -> StoreWorkbookSelectionResult:
    retention_cap = await resolve_active_cap(
        session,
        lead_source_type=LEAD_SOURCE_RETENTION,
        work_section=CAP_WORK_SECTION_FRESH_RETENTION,
        cost_center=cost_center,
        run_date=run_date,
    )
    external_cap = await resolve_active_cap(
        session,
        lead_source_type=LEAD_SOURCE_EXTERNAL,
        work_section=CAP_WORK_SECTION_EXTERNAL_LEAD,
        cost_center=cost_center,
        run_date=run_date,
    )
    workload = await evaluate_retention_workload_freeze(session, cost_center=cost_center, run_date=run_date, threshold=backlog_threshold)

    lead_rows = await _fetch_actionable_rows(session, cost_center=cost_center)
    selected: list[WorkbookLeadRow] = []
    warnings: list[RowWarning] = []
    seen_lead_ids: set[int] = set()
    external_selected = 0
    retention_selected = 0
    external_limit = None if external_cap.missing or external_cap.is_uncapped else external_cap.daily_cap
    retention_limit = 0 if workload.frozen else (None if retention_cap.is_uncapped else retention_cap.daily_cap)
    if retention_cap.missing or not retention_cap.valid:
        retention_limit = 0
    if not external_cap.valid:
        external_limit = 0

    for row in sorted(lead_rows, key=lambda candidate: _selection_sort_key(candidate, run_date=run_date)):
        lead_id = int(row["lead_id"])
        if lead_id in seen_lead_ids:
            continue
        mobile = str(row.get("normalized_mobile_number") or "")
        if not normalize_mobile(mobile).is_valid:
            warnings.append(RowWarning("invalid_mobile_identity", "Lead has invalid normalized mobile identity and was excluded", lead_id=lead_id, cost_center=cost_center))
            continue
        suppression = await check_active_suppression(session, cost_center=cost_center, normalized_mobile_number=mobile, as_of_date=run_date)
        if suppression.is_suppressed:
            continue
        category = _categorize(row, run_date=run_date)
        if category is None:
            continue
        if category == CAP_WORK_SECTION_EXTERNAL_LEAD and external_limit is not None:
            if external_selected >= external_limit:
                continue
            external_selected += 1
        if category == CAP_WORK_SECTION_FRESH_RETENTION:
            if retention_limit is not None and retention_selected >= retention_limit:
                continue
            retention_selected += 1
        seen_lead_ids.add(lead_id)
        selected.append(_to_workbook_row(row, category))

    counts = dict(Counter(row.work_section for row in selected))
    selected.sort(key=lambda row: (CATEGORY_ORDER[row.work_section], -(row.priority_score or Decimal("0")), row.next_followup_date or row.lead_date, row.lead_date, row.lead_id))
    return StoreWorkbookSelectionResult(cost_center, run_date, tuple(selected), counts, tuple(warnings), retention_cap, external_cap, workload)


async def select_workbook_leads_for_active_stores(
    session: AsyncSession,
    *,
    run_date: date,
    backlog_threshold: int,
) -> tuple[StoreWorkbookSelectionResult, ...]:
    results = []
    for cost_center in await load_active_retention_stores(session):
        results.append(await select_workbook_leads_for_store(session, cost_center=cost_center, run_date=run_date, backlog_threshold=backlog_threshold))
    return tuple(results)


async def _fetch_actionable_rows(session: AsyncSession, *, cost_center: str) -> list[dict[str, Any]]:
    rows = (
        await session.execute(
            sa.select(trx_customer_followup_leads)
            .where(
                trx_customer_followup_leads.c.cost_center == cost_center,
                trx_customer_followup_leads.c.lead_status.in_(ACTIONABLE_STATUSES),
                trx_customer_followup_leads.c.is_closed.is_(False),
                trx_customer_followup_leads.c.is_recovered.is_(False),
                trx_customer_followup_leads.c.suppression_applied.is_(False),
            )
        )
    ).mappings().all()
    return [dict(row) for row in rows]


def _categorize(row: dict[str, Any], *, run_date: date) -> str | None:
    next_followup_date = row.get("next_followup_date")
    if isinstance(next_followup_date, datetime):
        next_followup_date = next_followup_date.date()
    lead_date = row["lead_date"].date() if isinstance(row["lead_date"], datetime) else row["lead_date"]
    if next_followup_date is not None and next_followup_date <= run_date:
        return CAP_WORK_SECTION_DUE_FOLLOWUP
    if lead_date < run_date:
        return CAP_WORK_SECTION_PENDING_CARRY_FORWARD
    source = row["lead_source_type"]
    if source == LEAD_SOURCE_TD:
        return CAP_WORK_SECTION_TD_LEAD
    if source == LEAD_SOURCE_EXTERNAL:
        return CAP_WORK_SECTION_EXTERNAL_LEAD
    if source == LEAD_SOURCE_RETENTION:
        return CAP_WORK_SECTION_FRESH_RETENTION
    return None


def _selection_sort_key(row: dict[str, Any], *, run_date: date) -> tuple[Any, ...]:
    category = _categorize(row, run_date=run_date) or "ZZZ"
    priority = row.get("priority_score") or Decimal("0")
    return (CATEGORY_ORDER.get(category, 99), -Decimal(priority), row.get("next_followup_date") or row.get("lead_date"), row.get("lead_date"), row.get("lead_id"))


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _to_workbook_row(row: dict[str, Any], work_section: str) -> WorkbookLeadRow:
    lead_date = row["lead_date"].date() if isinstance(row["lead_date"], datetime) else row["lead_date"]
    return WorkbookLeadRow(
        lead_id=int(row["lead_id"]),
        lead_source_type=str(row["lead_source_type"]),
        work_section=work_section,
        cost_center=str(row["cost_center"]),
        customer_name=row.get("customer_name"),
        mobile_number=row.get("mobile_number"),
        normalized_mobile_number=str(row["normalized_mobile_number"]),
        lifecycle_bucket=row.get("lifecycle_bucket"),
        last_order_date=row.get("last_order_date"),
        days_since_last_order=row.get("days_since_last_order"),
        total_orders=row.get("total_orders"),
        lifetime_spend=_to_decimal(row.get("lifetime_spend")),
        average_order_value=_to_decimal(row.get("average_order_value")),
        last_order_amount=_to_decimal(row.get("last_order_amount")),
        priority_score=_to_decimal(row.get("priority_score")),
        recommended_strategy=row.get("recommended_strategy"),
        lead_date=lead_date,
        next_followup_date=row.get("next_followup_date"),
        generated_at=row.get("created_at"),
    )
