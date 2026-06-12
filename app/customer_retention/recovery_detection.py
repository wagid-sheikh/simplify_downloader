"""Phase 3 recovery detection for customer follow-up leads.

Recovery evidence is read from ``vw_orders`` only. The module intentionally
normalizes ``vw_orders.mobile_number`` in Python before matching because the
project's mobile-normalization contract is richer than database string cleanup.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from .constants import LEAD_STATUS_CLOSED, LEAD_STATUS_DUE_FOLLOWUP, LEAD_STATUS_OPEN, LEAD_STATUS_PENDING, LEAD_STATUS_RECOVERED, LEAD_STATUS_WORKED
from .db_tables import trx_customer_followup_leads
from .lifecycle import OPEN_LEAD_STATUSES
from .mobile import normalize_mobile
from .persistence import insert_history_once


@dataclass(frozen=True)
class RecoveryMatch:
    lead_id: int
    recovered_order_id: str
    recovered_at: datetime
    recovered_amount: Decimal


@dataclass(frozen=True)
class RecoveryDetectionResult:
    leads_recovered: int = 0
    leads_closed: int = 0
    orders_seen: int = 0
    history_inserted: int = 0
    warnings: tuple[str, ...] = field(default_factory=tuple)
    matches: tuple[RecoveryMatch, ...] = field(default_factory=tuple)


VW_ORDERS = sa.table(
    "vw_orders",
    sa.column("cost_center"),
    sa.column("order_number"),
    sa.column("order_date"),
    sa.column("customer_name"),
    sa.column("mobile_number"),
    sa.column("order_amount"),
)


async def detect_recoveries(
    session: AsyncSession,
    *,
    as_of_date: date,
    pipeline_run_id: str | None,
    cost_center: str | None = None,
) -> RecoveryDetectionResult:
    leads = (
        await session.execute(
            sa.select(trx_customer_followup_leads)
            .where(
                trx_customer_followup_leads.c.is_closed.is_(False),
                trx_customer_followup_leads.c.is_recovered.is_(False),
                trx_customer_followup_leads.c.lead_status.in_(tuple(OPEN_LEAD_STATUSES)),
                trx_customer_followup_leads.c.normalized_mobile_number.is_not(None),
            )
            .where(trx_customer_followup_leads.c.cost_center == cost_center if cost_center else sa.true())
            .order_by(trx_customer_followup_leads.c.cost_center.asc(), trx_customer_followup_leads.c.normalized_mobile_number.asc(), trx_customer_followup_leads.c.lead_date.desc(), trx_customer_followup_leads.c.lead_id.desc())
        )
    ).mappings().all()
    if not leads:
        return RecoveryDetectionResult()

    identities = {(str(lead["cost_center"]), str(lead["normalized_mobile_number"])) for lead in leads if normalize_mobile(lead["normalized_mobile_number"]).is_valid}
    order_rows = await _fetch_candidate_orders(session, as_of_date=as_of_date, cost_center=cost_center)
    orders_seen = 0
    matches: list[RecoveryMatch] = []
    recovered = 0
    closed = 0
    history_count = 0
    changed_lead_ids: set[int] = set()
    # Candidate orders are already sorted by order date then order number. When
    # multiple orders qualify for the same lead in one run, the first ordered
    # order wins and the lead is excluded from later order matches below.
    for raw_order in order_rows:
        mobile_result = normalize_mobile(raw_order.get("mobile_number"))
        if not mobile_result.is_valid:
            continue
        identity = (str(raw_order["cost_center"]), mobile_result.normalized_mobile or "")
        if identity not in identities:
            continue
        orders_seen += 1
        order_date = _as_date(raw_order["order_date"])
        order_datetime = _as_datetime(raw_order["order_date"])
        candidates = [
            lead
            for lead in leads
            if int(lead["lead_id"]) not in changed_lead_ids
            and (str(lead["cost_center"]), str(lead["normalized_mobile_number"])) == identity
            and _trigger_date(lead) < order_date
        ]
        candidates = [lead for lead in candidates if not bool(lead["is_closed"]) and not bool(lead["is_recovered"])]
        if not candidates:
            continue
        # Deterministic conflict rule: the most recently triggered active lead
        # owns the recovery; older open leads for the same store/customer close.
        candidates.sort(key=lambda lead: (_trigger_date(lead), int(lead["lead_id"])), reverse=True)
        winner = candidates[0]
        order_id = str(raw_order.get("order_number") or "")
        event_type = f"RECOVERY_DETECTED:{order_id}"
        history_inserted = await insert_history_once(
            session,
            lead_id=int(winner["lead_id"]),
            pipeline_run_id=pipeline_run_id,
            event_type=event_type,
            previous_status=str(winner["lead_status"]),
            new_status=LEAD_STATUS_RECOVERED,
            raw_excel_value_json=None,
            normalized_value_json={"recovered_order_id": order_id, "cost_center": identity[0], "order_amount": str(raw_order.get("order_amount"))},
        )
        if not history_inserted:
            continue
        await session.execute(
            trx_customer_followup_leads.update()
            .where(trx_customer_followup_leads.c.lead_id == int(winner["lead_id"]))
            .values(
                lead_status=LEAD_STATUS_RECOVERED,
                is_closed=True,
                closed_at=order_datetime,
                closed_reason="RECOVERED",
                is_recovered=True,
                recovered_at=order_datetime,
                recovered_order_id=order_id,
                updated_at=datetime.now(timezone.utc),
                updated_by_pipeline_run_id=pipeline_run_id,
            )
        )
        recovered += 1
        history_count += 1
        changed_lead_ids.add(int(winner["lead_id"]))
        amount = Decimal(str(raw_order.get("order_amount") or "0"))
        matches.append(RecoveryMatch(int(winner["lead_id"]), order_id, order_datetime, amount))
        other_ids = [int(lead["lead_id"]) for lead in candidates[1:]]
        if other_ids:
            await session.execute(
                trx_customer_followup_leads.update()
                .where(trx_customer_followup_leads.c.lead_id.in_(other_ids))
                .values(
                    lead_status=LEAD_STATUS_CLOSED,
                    is_closed=True,
                    closed_at=order_datetime,
                    closed_reason="RECOVERED_BY_SAME_CUSTOMER_STORE",
                    updated_at=datetime.now(timezone.utc),
                    updated_by_pipeline_run_id=pipeline_run_id,
                )
            )
            for other_id in other_ids:
                inserted = await insert_history_once(
                    session,
                    lead_id=other_id,
                    pipeline_run_id=pipeline_run_id,
                    event_type=f"RECOVERY_CLOSED_BY_IDENTITY:{order_id}",
                    previous_status=None,
                    new_status=LEAD_STATUS_CLOSED,
                    raw_excel_value_json=None,
                    normalized_value_json={"recovered_by_lead_id": int(winner["lead_id"]), "recovered_order_id": order_id},
                )
                history_count += int(inserted)
            changed_lead_ids.update(other_ids)
            closed += len(other_ids)
    return RecoveryDetectionResult(recovered, closed, orders_seen, history_count, matches=tuple(matches))


async def _fetch_candidate_orders(session: AsyncSession, *, as_of_date: date, cost_center: str | None) -> list[dict[str, Any]]:
    stmt = sa.select(VW_ORDERS.c.cost_center, VW_ORDERS.c.order_number, VW_ORDERS.c.order_date, VW_ORDERS.c.mobile_number, VW_ORDERS.c.order_amount)
    if cost_center:
        stmt = stmt.where(VW_ORDERS.c.cost_center == cost_center)
    stmt = stmt.order_by(VW_ORDERS.c.order_date.asc(), VW_ORDERS.c.order_number.asc())
    return [dict(row) for row in (await session.execute(stmt)).mappings().all() if _as_date(row["order_date"]) <= as_of_date]


def _trigger_date(lead: sa.RowMapping) -> date:
    values = [lead.get("lead_date"), lead.get("next_followup_date")]
    dates = [_as_date(value) for value in values if value is not None]
    return max(dates)


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _as_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    parsed = datetime.fromisoformat(str(value))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
