"""Persistence helpers shared by Phase 2 customer retention ingestion paths."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from .constants import (
    LEAD_SOURCE_EXTERNAL,
    LEAD_SOURCE_TD,
    LEAD_STATUS_DUE_FOLLOWUP,
    LEAD_STATUS_OPEN,
    LEAD_STATUS_PENDING,
    LEAD_STATUS_WORKED,
)
from .db_tables import trx_customer_followup_history, trx_customer_followup_leads


def stable_uuid(*parts: object) -> str:
    return str(uuid5(NAMESPACE_URL, "customer-retention|" + "|".join(str(part or "") for part in parts)))


async def sqlite_next_id(session: AsyncSession, table: sa.Table, column_name: str) -> int | None:
    if session.bind and session.bind.dialect.name == "sqlite":
        result = await session.execute(sa.select(sa.func.coalesce(sa.func.max(table.c[column_name]), 0) + 1))
        return int(result.scalar_one())
    return None


async def fetch_active_cost_centers(session: AsyncSession) -> set[str]:
    bind = session.bind
    if bind is None:
        return set()
    def _inspect(sync_conn: Any) -> tuple[bool, set[str]]:
        inspector = sa.inspect(sync_conn.connection())
        if not inspector.has_table("store_master"):
            return False, set()
        return True, {c["name"] for c in inspector.get_columns("store_master")}
    has_table, columns = await session.run_sync(_inspect)
    if not has_table or "cost_center" not in columns:
        return set()
    store_master = sa.table("store_master", sa.column("cost_center", sa.String()), sa.column("customer_retention_pipeline", sa.Boolean()))
    stmt = sa.select(store_master.c.cost_center).where(store_master.c.cost_center.is_not(None))
    if "customer_retention_pipeline" in columns:
        stmt = stmt.where(store_master.c.customer_retention_pipeline.is_(True))
    rows = (await session.execute(stmt)).scalars().all()
    return {str(row).strip().upper() for row in rows if str(row or "").strip()}


CUSTOMER_IDENTITY_DEDUPE_SOURCE_TYPES = {LEAD_SOURCE_TD, LEAD_SOURCE_EXTERNAL}
CUSTOMER_IDENTITY_ACTIONABLE_STATUSES = {
    LEAD_STATUS_OPEN,
    LEAD_STATUS_PENDING,
    LEAD_STATUS_DUE_FOLLOWUP,
    LEAD_STATUS_WORKED,
}


async def find_existing_open_customer_lead(
    session: AsyncSession,
    *,
    lead_source_type: str,
    cost_center: str,
    normalized_mobile_number: str,
) -> int | None:
    """Return an existing actionable customer lead for TD/EXTERNAL identity de-dupe.

    Customer identity matching is intentionally cross-source for TD and EXTERNAL
    leads: a customer should have one open operational follow-up lead per
    cost center and normalized mobile number, regardless of whether the new
    trigger came from TD or an external campaign file. Source-record
    idempotency is handled before this lookup so exact reprocessing still
    returns the source-matched lead first.
    """

    if lead_source_type not in CUSTOMER_IDENTITY_DEDUPE_SOURCE_TYPES or not normalized_mobile_number:
        return None

    existing = await session.execute(
        sa.select(trx_customer_followup_leads.c.lead_id)
        .where(
            trx_customer_followup_leads.c.lead_source_type.in_(tuple(CUSTOMER_IDENTITY_DEDUPE_SOURCE_TYPES)),
            trx_customer_followup_leads.c.cost_center == cost_center,
            trx_customer_followup_leads.c.normalized_mobile_number == normalized_mobile_number,
            trx_customer_followup_leads.c.lead_status.in_(tuple(CUSTOMER_IDENTITY_ACTIONABLE_STATUSES)),
            trx_customer_followup_leads.c.is_closed.is_(False),
        )
        .order_by(trx_customer_followup_leads.c.lead_id.asc())
        .limit(1)
    )
    existing_id = existing.scalar_one_or_none()
    return int(existing_id) if existing_id is not None else None


async def get_or_create_followup_lead(
    session: AsyncSession,
    *,
    lead_source_type: str,
    source_system: str,
    source_table_name: str,
    source_record_id: str,
    source_reference: str | None,
    cost_center: str,
    customer_name: str | None,
    mobile_number: str | None,
    normalized_mobile_number: str,
    lead_date: date,
    pipeline_run_id: str | None,
    lead_stage: str | None = None,
    assigned_store: str | None = None,
    dedupe_by_customer_identity: bool = False,
) -> tuple[int, bool]:
    existing = await session.execute(
        sa.select(trx_customer_followup_leads.c.lead_id).where(
            trx_customer_followup_leads.c.lead_source_type == lead_source_type,
            trx_customer_followup_leads.c.source_system == source_system,
            trx_customer_followup_leads.c.source_table_name == source_table_name,
            trx_customer_followup_leads.c.source_record_id == source_record_id,
        )
    )
    existing_id = existing.scalar_one_or_none()
    if existing_id is not None:
        return int(existing_id), False
    if dedupe_by_customer_identity:
        customer_lead_id = await find_existing_open_customer_lead(
            session,
            lead_source_type=lead_source_type,
            cost_center=cost_center,
            normalized_mobile_number=normalized_mobile_number,
        )
        if customer_lead_id is not None:
            return customer_lead_id, False
    values: dict[str, Any] = {
        "lead_uuid": stable_uuid("followup", lead_source_type, source_system, source_table_name, source_record_id),
        "lead_source_type": lead_source_type,
        "source_system": source_system,
        "source_table_name": source_table_name,
        "source_record_id": source_record_id,
        "source_reference": source_reference,
        "cost_center": cost_center,
        "customer_name": customer_name,
        "mobile_number": mobile_number,
        "normalized_mobile_number": normalized_mobile_number,
        "lead_date": lead_date,
        "lead_status": LEAD_STATUS_OPEN,
        "lead_stage": lead_stage,
        "assigned_store": assigned_store or cost_center,
        "created_by_pipeline_run_id": pipeline_run_id,
        "updated_by_pipeline_run_id": pipeline_run_id,
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    }
    next_id = await sqlite_next_id(session, trx_customer_followup_leads, "lead_id")
    if next_id is not None:
        values["lead_id"] = next_id
    result = await session.execute(trx_customer_followup_leads.insert().values(**values))
    return int(next_id or result.inserted_primary_key[0]), True


async def insert_history_once(
    session: AsyncSession,
    *,
    lead_id: int,
    pipeline_run_id: str | None,
    event_type: str,
    raw_excel_value_json: dict[str, Any] | None,
    normalized_value_json: dict[str, Any] | None,
    previous_status: str | None = None,
    new_status: str | None = None,
    handled_by: str | None = None,
    contact_attempted: bool | None = None,
    contact_mode: str | None = None,
    customer_response: str | None = None,
    order_expected: str | None = None,
    next_followup_date: date | None = None,
    complaint_flag: bool | None = None,
    do_not_contact_flag: bool | None = None,
    staff_remarks: str | None = None,
    target_cost_center: str | None = None,
) -> bool:
    existing = await session.execute(
        sa.select(trx_customer_followup_history.c.history_id).where(
            trx_customer_followup_history.c.lead_id == lead_id,
            trx_customer_followup_history.c.event_type == event_type,
        )
    )
    if existing.scalar_one_or_none() is not None:
        return False
    values: dict[str, Any] = {
        "lead_id": lead_id,
        "pipeline_run_id": pipeline_run_id,
        "event_type": event_type,
        "previous_status": previous_status,
        "new_status": new_status,
        "handled_by": handled_by,
        "contact_attempted": contact_attempted,
        "contact_mode": contact_mode,
        "customer_response": customer_response,
        "order_expected": order_expected,
        "next_followup_date": next_followup_date,
        "complaint_flag": complaint_flag,
        "do_not_contact_flag": do_not_contact_flag,
        "staff_remarks": staff_remarks,
        "target_cost_center": target_cost_center,
        "raw_excel_value_json": raw_excel_value_json,
        "normalized_value_json": normalized_value_json,
        "created_at": datetime.now(timezone.utc),
    }
    next_id = await sqlite_next_id(session, trx_customer_followup_history, "history_id")
    if next_id is not None:
        values["history_id"] = next_id
    await session.execute(trx_customer_followup_history.insert().values(**values))
    return True
