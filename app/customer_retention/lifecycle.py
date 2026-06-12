"""Phase 3 lifecycle classification, scoring, strategy, and lead transitions.

Inputs are normalized customer identities and already-normalized workbook values.
Outputs are typed decision/result objects plus transactional lead/history updates.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from .constants import (
    LEAD_SOURCE_EXTERNAL,
    LEAD_SOURCE_TD,
    LEAD_STATUS_CLOSED,
    LEAD_STATUS_DUE_FOLLOWUP,
    LEAD_STATUS_OPEN,
    LEAD_STATUS_PENDING,
    LEAD_STATUS_RECOVERED,
    LEAD_STATUS_WORKED,
    LIFECYCLE_BUCKET_ACTIVE,
    LIFECYCLE_BUCKET_COLD,
    LIFECYCLE_BUCKET_COOLING,
    LIFECYCLE_BUCKET_DORMANT,
    LIFECYCLE_BUCKET_LOST,
    LIFECYCLE_BUCKET_WARM,
    PERMANENT_SUPPRESSION_WORKBOOK_OUTCOMES,
    TIME_BOUND_SUPPRESSION_WORKBOOK_OUTCOMES,
    WORKBOOK_OUTCOME_ALREADY_GIVEN_ORDER,
    WORKBOOK_OUTCOME_INTERESTED,
    WORKBOOK_OUTCOME_NO_RESPONSE,
    WORKBOOK_OUTCOME_PICKUP_REQUESTED,
    WORKBOOK_OUTCOME_WILL_GIVE_ORDER_LATER,
)
from .db_tables import trx_customer_followup_leads
from .mobile import normalize_mobile
from .persistence import insert_history_once

LIFECYCLE_DAY_RANGES: tuple[tuple[str, int, int | None], ...] = (
    (LIFECYCLE_BUCKET_ACTIVE, 0, 21),
    (LIFECYCLE_BUCKET_WARM, 22, 45),
    (LIFECYCLE_BUCKET_COOLING, 46, 75),
    (LIFECYCLE_BUCKET_DORMANT, 76, 120),
    (LIFECYCLE_BUCKET_COLD, 121, 180),
    (LIFECYCLE_BUCKET_LOST, 181, None),
)
RETENTION_ELIGIBLE_BUCKETS = {
    LIFECYCLE_BUCKET_WARM,
    LIFECYCLE_BUCKET_COOLING,
    LIFECYCLE_BUCKET_DORMANT,
    LIFECYCLE_BUCKET_COLD,
    LIFECYCLE_BUCKET_LOST,
}
OPEN_LEAD_STATUSES = {
    LEAD_STATUS_OPEN,
    LEAD_STATUS_PENDING,
    LEAD_STATUS_DUE_FOLLOWUP,
    LEAD_STATUS_WORKED,
}
STRATEGY_BY_BUCKET = {
    LIFECYCLE_BUCKET_WARM: "Gentle reminder call. Mention pickup convenience and ask if anything is pending.",
    LIFECYCLE_BUCKET_COOLING: "Call and check if service is required. If no answer, send WhatsApp reminder.",
    LIFECYCLE_BUCKET_DORMANT: "Call customer. Ask for feedback and reason for not ordering recently. Mention pickup convenience.",
    LIFECYCLE_BUCKET_COLD: "Call first. Ask if customer still uses dry-cleaning/laundry service. Offer pickup convenience if interested.",
    LIFECYCLE_BUCKET_LOST: "Call first. If no response, send WhatsApp comeback message. Ask why customer stopped using us. Offer pickup convenience. Escalate complaints immediately.",
    LEAD_SOURCE_TD: "Call promptly. This is an inbound/source lead. Confirm requirement, pickup address, and expected pickup time.",
    LEAD_SOURCE_EXTERNAL: "Call promptly. Mention campaign/source context if available. Confirm service requirement and pickup interest.",
}
_BUCKET_PRIORITY = {
    LIFECYCLE_BUCKET_WARM: Decimal("40"),
    LIFECYCLE_BUCKET_COOLING: Decimal("55"),
    LIFECYCLE_BUCKET_DORMANT: Decimal("70"),
    LIFECYCLE_BUCKET_COLD: Decimal("80"),
    LIFECYCLE_BUCKET_LOST: Decimal("90"),
}
POSITIVE_OUTCOMES = {WORKBOOK_OUTCOME_INTERESTED, WORKBOOK_OUTCOME_WILL_GIVE_ORDER_LATER, WORKBOOK_OUTCOME_PICKUP_REQUESTED}
LOW_SIGNAL_OUTCOMES = {WORKBOOK_OUTCOME_NO_RESPONSE}


@dataclass(frozen=True)
class LifecycleClassificationResult:
    lifecycle_bucket: str | None
    eligible_for_retention: bool
    days_since_last_order: int | None
    warning_code: str | None = None
    warning_message: str | None = None


@dataclass(frozen=True)
class PriorityDecision:
    priority_score: Decimal
    recommended_strategy: str


@dataclass(frozen=True)
class LifecycleTransitionResult:
    lead_id: int
    previous_status: str
    new_status: str
    history_inserted: bool
    suppression_id: int | None = None
    pending_approval_id: int | None = None
    warnings: tuple[str, ...] = field(default_factory=tuple)


def classify_lifecycle(*, last_order_date: date | None, snapshot_date: date, normalized_mobile_number: str | None) -> LifecycleClassificationResult:
    """Classify a customer using inclusive SRS day boundaries.

    Missing/invalid normalized mobile identity is explicitly ineligible; callers
    should treat the warning as row/customer-level audit data, not actionable work.
    """

    if not normalized_mobile_number or not normalize_mobile(normalized_mobile_number).is_valid:
        return LifecycleClassificationResult(None, False, None, "invalid_mobile_identity", "Normalized mobile number is missing or invalid")
    if last_order_date is None:
        return LifecycleClassificationResult(None, False, None, "missing_last_order_date", "Last order date is required")
    days_since = (snapshot_date - last_order_date).days
    if days_since < 0:
        return LifecycleClassificationResult(None, False, days_since, "future_last_order_date", "Last order date is after snapshot date")
    for bucket, lower, upper in LIFECYCLE_DAY_RANGES:
        if days_since >= lower and (upper is None or days_since <= upper):
            return LifecycleClassificationResult(bucket, bucket in RETENTION_ELIGIBLE_BUCKETS, days_since)
    return LifecycleClassificationResult(None, False, days_since, "unclassified_lifecycle", "Days since last order did not match a lifecycle bucket")


def recommended_strategy(*, lifecycle_bucket: str | None = None, lead_source_type: str | None = None) -> str:
    if lead_source_type in {LEAD_SOURCE_TD, LEAD_SOURCE_EXTERNAL}:
        return STRATEGY_BY_BUCKET[lead_source_type]
    if lifecycle_bucket in STRATEGY_BY_BUCKET:
        return STRATEGY_BY_BUCKET[lifecycle_bucket]
    return "Review customer context before follow-up."


def compute_priority_decision(
    *,
    lifecycle_bucket: str | None,
    lead_source_type: str | None = None,
    days_since_last_order: int | None = None,
    total_orders: int | None = None,
    lifetime_spend: Decimal | int | float | None = None,
    average_order_value: Decimal | int | float | None = None,
    last_order_amount: Decimal | int | float | None = None,
    previous_followup_outcome: str | None = None,
    had_recovery: bool = False,
    suppressed: bool = False,
) -> PriorityDecision:
    if suppressed or lifecycle_bucket == LIFECYCLE_BUCKET_ACTIVE:
        score = Decimal("0")
    elif lead_source_type == LEAD_SOURCE_TD:
        score = Decimal("95")
    elif lead_source_type == LEAD_SOURCE_EXTERNAL:
        score = Decimal("75")
    else:
        score = _BUCKET_PRIORITY.get(lifecycle_bucket or "", Decimal("10"))

    score += min(Decimal(total_orders or 0), Decimal("10"))
    score += min(_decimal(lifetime_spend) / Decimal("1000"), Decimal("15"))
    score += min(_decimal(average_order_value) / Decimal("500"), Decimal("10"))
    score += min(_decimal(last_order_amount) / Decimal("500"), Decimal("10"))
    if days_since_last_order is not None and lifecycle_bucket in {LIFECYCLE_BUCKET_COLD, LIFECYCLE_BUCKET_LOST}:
        score += min(Decimal(days_since_last_order - 120) / Decimal("30"), Decimal("10"))
    if previous_followup_outcome in POSITIVE_OUTCOMES:
        score += Decimal("12")
    elif previous_followup_outcome in LOW_SIGNAL_OUTCOMES:
        score -= Decimal("5")
    if had_recovery:
        score -= Decimal("10")
    score = max(Decimal("0"), min(score, Decimal("100"))).quantize(Decimal("0.01"))
    return PriorityDecision(score, recommended_strategy(lifecycle_bucket=lifecycle_bucket, lead_source_type=lead_source_type))


def _decimal(value: Decimal | int | float | None) -> Decimal:
    if value is None:
        return Decimal("0")
    return Decimal(str(value))


async def apply_lifecycle_transition(
    session: AsyncSession,
    *,
    lead_id: int,
    customer_response: str | None,
    contact_attempted: bool | None = None,
    contact_mode: str | None = None,
    order_expected: str | None = None,
    next_followup_date: date | None = None,
    complaint_flag: bool | None = None,
    do_not_contact_flag: bool | None = None,
    staff_remarks: str | None = None,
    handled_by: str | None = None,
    pipeline_run_id: str | None = None,
    event_key: str | None = None,
) -> LifecycleTransitionResult:
    lead = (await session.execute(sa.select(trx_customer_followup_leads).where(trx_customer_followup_leads.c.lead_id == lead_id))).mappings().first()
    if lead is None:
        raise ValueError(f"Unknown customer follow-up lead_id={lead_id}")
    previous_status = str(lead["lead_status"])
    if not customer_response or contact_attempted is None:
        history_inserted = await _write_transition_history(
            session,
            lead_id=lead_id,
            pipeline_run_id=pipeline_run_id,
            event_type=f"Pending_Not_Updated:{event_key or 'manual'}",
            previous_status=previous_status,
            new_status=previous_status,
            normalized_values={"customer_response": customer_response, "contact_attempted": contact_attempted},
        )
        return LifecycleTransitionResult(lead_id, previous_status, previous_status, history_inserted, warnings=("required_fields_missing",))
    if previous_status in {LEAD_STATUS_CLOSED, LEAD_STATUS_RECOVERED}:
        return LifecycleTransitionResult(lead_id, previous_status, previous_status, False, warnings=("lead_not_actionable",))

    from .suppression import create_pending_permanent_suppression, create_time_bound_suppression

    now = datetime.now(timezone.utc)
    update_values: dict[str, Any] = {
        "customer_response": customer_response,
        "contact_attempted": bool(contact_attempted),
        "contact_mode": contact_mode,
        "order_expected": order_expected,
        "next_followup_date": next_followup_date,
        "complaint_flag": bool(complaint_flag) if complaint_flag is not None else False,
        "do_not_contact_flag": bool(do_not_contact_flag) if do_not_contact_flag is not None else False,
        "staff_remarks": staff_remarks,
        "handled_by": handled_by,
        "updated_at": now,
        "updated_by_pipeline_run_id": pipeline_run_id,
    }
    new_status = LEAD_STATUS_WORKED
    closed_reason: str | None = None
    suppression_id: int | None = None
    pending_approval_id: int | None = None

    if next_followup_date is not None or customer_response in POSITIVE_OUTCOMES:
        new_status = LEAD_STATUS_DUE_FOLLOWUP
    if customer_response == WORKBOOK_OUTCOME_ALREADY_GIVEN_ORDER:
        new_status = LEAD_STATUS_WORKED
    if customer_response in TIME_BOUND_SUPPRESSION_WORKBOOK_OUTCOMES:
        new_status = LEAD_STATUS_CLOSED
        closed_reason = customer_response
        decision = await create_time_bound_suppression(
            session,
            cost_center=str(lead["cost_center"]),
            normalized_mobile_number=str(lead["normalized_mobile_number"]),
            reason=customer_response,
            start_date=date.today(),
            source_lead_id=lead_id,
            pipeline_run_id=pipeline_run_id,
        )
        suppression_id = decision.suppression_id
        update_values.update({"suppression_applied": True, "suppression_until": decision.suppression_until})
    elif customer_response in PERMANENT_SUPPRESSION_WORKBOOK_OUTCOMES:
        new_status = LEAD_STATUS_CLOSED
        closed_reason = customer_response
        approval = await create_pending_permanent_suppression(
            session,
            cost_center=str(lead["cost_center"]),
            normalized_mobile_number=str(lead["normalized_mobile_number"]),
            reason=customer_response,
            mobile_number=lead.get("mobile_number"),
            source_lead_id=lead_id,
            pipeline_run_id=pipeline_run_id,
        )
        pending_approval_id = approval.suppression_id

    if new_status == LEAD_STATUS_CLOSED:
        update_values.update({"is_closed": True, "closed_at": now, "closed_reason": closed_reason})
    update_values["lead_status"] = new_status

    history_event = f"LIFECYCLE_TRANSITION:{event_key or lead_id}:{customer_response}"
    history_inserted = await _write_transition_history(
        session,
        lead_id=lead_id,
        pipeline_run_id=pipeline_run_id,
        event_type=history_event,
        previous_status=previous_status,
        new_status=new_status,
        normalized_values={"customer_response": customer_response, "suppression_id": suppression_id, "pending_approval_id": pending_approval_id},
        handled_by=handled_by,
        contact_attempted=contact_attempted,
        contact_mode=contact_mode,
        customer_response=customer_response,
        order_expected=order_expected,
        next_followup_date=next_followup_date,
        complaint_flag=complaint_flag,
        do_not_contact_flag=do_not_contact_flag,
        staff_remarks=staff_remarks,
    )
    if history_inserted:
        await session.execute(trx_customer_followup_leads.update().where(trx_customer_followup_leads.c.lead_id == lead_id).values(**update_values))
    return LifecycleTransitionResult(lead_id, previous_status, new_status if history_inserted else previous_status, history_inserted, suppression_id, pending_approval_id)


async def _write_transition_history(
    session: AsyncSession,
    *,
    lead_id: int,
    pipeline_run_id: str | None,
    event_type: str,
    previous_status: str | None,
    new_status: str | None,
    normalized_values: dict[str, Any] | None,
    **history_fields: Any,
) -> bool:
    return await insert_history_once(
        session,
        lead_id=lead_id,
        pipeline_run_id=pipeline_run_id,
        event_type=event_type,
        raw_excel_value_json=None,
        normalized_value_json=normalized_values,
        previous_status=previous_status,
        new_status=new_status,
        **history_fields,
    )
