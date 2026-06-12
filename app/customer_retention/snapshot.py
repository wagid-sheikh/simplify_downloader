"""Phase 3 customer retention snapshot generation from actual order history.

The snapshot manager returns typed rows for downstream lead generation without
performing Phase 4 cap allocation or workbook generation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from .db_tables import trx_customer_followup_leads
from .lifecycle import OPEN_LEAD_STATUSES, classify_lifecycle, compute_priority_decision
from .mobile import normalize_mobile
from .suppression import check_active_suppression

VW_ORDERS = sa.table(
    "vw_orders",
    sa.column("cost_center"),
    sa.column("order_number"),
    sa.column("order_date"),
    sa.column("customer_name"),
    sa.column("mobile_number"),
    sa.column("order_amount"),
)


@dataclass(frozen=True)
class CustomerRetentionSnapshotRow:
    snapshot_date: date
    cost_center: str
    customer_name: str | None
    mobile_number: str | None
    normalized_mobile_number: str
    last_order_date: date
    days_since_last_order: int
    lifecycle_bucket: str
    total_orders: int
    lifetime_spend: Decimal
    average_order_value: Decimal
    last_order_amount: Decimal
    last_followup_date: date | None
    last_followup_status: str | None
    suppression_status: str | None
    eligible_for_retention: bool
    priority_score: Decimal
    recommended_strategy: str


@dataclass(frozen=True)
class SnapshotResult:
    snapshot_date: date
    rows: tuple[CustomerRetentionSnapshotRow, ...]
    rows_seen: int
    rows_invalid_mobile: int
    rows_suppressed: int
    rows_existing_open_lead: int
    warnings: tuple[str, ...] = field(default_factory=tuple)


async def build_customer_retention_snapshot(
    session: AsyncSession,
    *,
    snapshot_date: date,
    cost_center: str | None = None,
) -> SnapshotResult:
    order_rows = await _fetch_orders(session, cost_center=cost_center)
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    invalid_mobile = 0
    for order in order_rows:
        mobile_result = normalize_mobile(order.get("mobile_number"))
        if not mobile_result.is_valid:
            invalid_mobile += 1
            continue
        identity = (str(order["cost_center"]), mobile_result.normalized_mobile or "")
        order_date = _as_date(order["order_date"])
        amount = Decimal(str(order.get("order_amount") or "0"))
        entry = grouped.setdefault(
            identity,
            {
                "cost_center": identity[0],
                "normalized_mobile_number": identity[1],
                "customer_name": order.get("customer_name"),
                "mobile_number": order.get("mobile_number"),
                "last_order_date": order_date,
                "total_orders": 0,
                "lifetime_spend": Decimal("0"),
                "last_order_amount": amount,
                "last_order_key": (order_date, str(order.get("order_number") or "")),
            },
        )
        entry["total_orders"] += 1
        entry["lifetime_spend"] += amount
        if (order_date, str(order.get("order_number") or "")) >= entry["last_order_key"]:
            entry.update({"last_order_date": order_date, "last_order_amount": amount, "customer_name": order.get("customer_name"), "mobile_number": order.get("mobile_number"), "last_order_key": (order_date, str(order.get("order_number") or ""))})
    rows: list[CustomerRetentionSnapshotRow] = []
    suppressed = 0
    existing_open = 0
    for entry in grouped.values():
        classification = classify_lifecycle(last_order_date=entry["last_order_date"], snapshot_date=snapshot_date, normalized_mobile_number=entry["normalized_mobile_number"])
        if not classification.lifecycle_bucket or classification.days_since_last_order is None:
            continue
        followup = await _fetch_last_followup(session, cost_center=entry["cost_center"], normalized_mobile_number=entry["normalized_mobile_number"])
        has_open = await _has_open_lead(session, cost_center=entry["cost_center"], normalized_mobile_number=entry["normalized_mobile_number"])
        suppression = await check_active_suppression(session, cost_center=entry["cost_center"], normalized_mobile_number=entry["normalized_mobile_number"], as_of_date=snapshot_date)
        eligible = classification.eligible_for_retention and not suppression.is_suppressed and not has_open
        suppressed += int(suppression.is_suppressed)
        existing_open += int(has_open)
        if not eligible:
            continue
        average = (entry["lifetime_spend"] / Decimal(entry["total_orders"])).quantize(Decimal("0.01"))
        priority = compute_priority_decision(
            lifecycle_bucket=classification.lifecycle_bucket,
            days_since_last_order=classification.days_since_last_order,
            total_orders=entry["total_orders"],
            lifetime_spend=entry["lifetime_spend"],
            average_order_value=average,
            last_order_amount=entry["last_order_amount"],
            previous_followup_outcome=followup.get("customer_response") if followup else None,
            suppressed=suppression.is_suppressed,
        )
        rows.append(
            CustomerRetentionSnapshotRow(
                snapshot_date=snapshot_date,
                cost_center=entry["cost_center"],
                customer_name=entry["customer_name"],
                mobile_number=entry["mobile_number"],
                normalized_mobile_number=entry["normalized_mobile_number"],
                last_order_date=entry["last_order_date"],
                days_since_last_order=classification.days_since_last_order,
                lifecycle_bucket=classification.lifecycle_bucket,
                total_orders=entry["total_orders"],
                lifetime_spend=entry["lifetime_spend"].quantize(Decimal("0.01")),
                average_order_value=average,
                last_order_amount=entry["last_order_amount"].quantize(Decimal("0.01")),
                last_followup_date=followup.get("lead_date") if followup else None,
                last_followup_status=followup.get("lead_status") if followup else None,
                suppression_status=suppression.suppression_state,
                eligible_for_retention=eligible,
                priority_score=priority.priority_score,
                recommended_strategy=priority.recommended_strategy,
            )
        )
    rows.sort(key=lambda row: (row.cost_center, -row.priority_score, row.normalized_mobile_number))
    return SnapshotResult(snapshot_date, tuple(rows), len(order_rows), invalid_mobile, suppressed, existing_open)


async def _fetch_orders(session: AsyncSession, *, cost_center: str | None) -> list[dict[str, Any]]:
    stmt = sa.select(VW_ORDERS.c.cost_center, VW_ORDERS.c.order_number, VW_ORDERS.c.order_date, VW_ORDERS.c.customer_name, VW_ORDERS.c.mobile_number, VW_ORDERS.c.order_amount)
    if cost_center:
        stmt = stmt.where(VW_ORDERS.c.cost_center == cost_center)
    return [dict(row) for row in (await session.execute(stmt)).mappings().all()]


async def _fetch_last_followup(session: AsyncSession, *, cost_center: str, normalized_mobile_number: str) -> dict[str, Any] | None:
    row = (
        await session.execute(
            sa.select(trx_customer_followup_leads)
            .where(
                trx_customer_followup_leads.c.cost_center == cost_center,
                trx_customer_followup_leads.c.normalized_mobile_number == normalized_mobile_number,
            )
            .order_by(trx_customer_followup_leads.c.lead_date.desc(), trx_customer_followup_leads.c.lead_id.desc())
            .limit(1)
        )
    ).mappings().first()
    return dict(row) if row else None


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


async def _has_open_lead(session: AsyncSession, *, cost_center: str, normalized_mobile_number: str) -> bool:
    existing = await session.execute(
        sa.select(trx_customer_followup_leads.c.lead_id)
        .where(
            trx_customer_followup_leads.c.cost_center == cost_center,
            trx_customer_followup_leads.c.normalized_mobile_number == normalized_mobile_number,
            trx_customer_followup_leads.c.lead_status.in_(tuple(OPEN_LEAD_STATUSES)),
            trx_customer_followup_leads.c.is_closed.is_(False),
        )
        .limit(1)
    )
    return existing.scalar_one_or_none() is not None
