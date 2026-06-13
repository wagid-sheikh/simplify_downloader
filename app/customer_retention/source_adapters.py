"""Source adapters for Customer Retention Phase 2 lead conversion."""

from __future__ import annotations

from datetime import date, datetime, timezone
from dataclasses import dataclass
from typing import Any

import sqlalchemy as sa

from app.common.db import session_scope
from sqlalchemy.ext.asyncio import AsyncSession
from app.dashboard_downloader.json_logger import JsonLogger, log_event

from .constants import LEAD_SOURCE_TD
from .mobile import normalize_mobile
from .persistence import get_or_create_followup_lead
from .types import AdapterConversionResult, RowWarning

TD_ACTIONABLE_STATUS_BUCKETS = {"pending"}


@dataclass(frozen=True)
class TDStoreResolution:
    store_code: str
    cost_center: str | None
    is_active: bool


async def _resolve_td_retention_cost_centers(
    session: AsyncSession,
    rows: list[dict[str, Any]],
) -> dict[str, TDStoreResolution]:
    """Resolve TD raw store codes to active customer-retention cost centers."""

    store_codes = {str(row.get("store_code") or "").strip().upper() for row in rows if row.get("store_code")}
    if not store_codes:
        return {}

    store_master = sa.table(
        "store_master",
        sa.column("store_code", sa.String()),
        sa.column("cost_center", sa.String()),
        sa.column("sync_group", sa.String()),
        sa.column("customer_retention_pipeline", sa.Boolean()),
    )
    result = await session.execute(
        sa.select(
            store_master.c.store_code,
            store_master.c.cost_center,
            store_master.c.sync_group,
            store_master.c.customer_retention_pipeline,
        ).where(sa.func.upper(sa.func.trim(store_master.c.store_code)).in_(store_codes))
    )

    resolved: dict[str, TDStoreResolution] = {}
    for row in result.mappings():
        store_code = str(row.get("store_code") or "").strip().upper()
        cost_center = str(row.get("cost_center") or "").strip().upper()
        sync_group = str(row.get("sync_group") or "").strip().upper()
        is_active = bool(row.get("customer_retention_pipeline")) and sync_group == "TD"
        if store_code:
            resolved[store_code] = TDStoreResolution(store_code=store_code, cost_center=cost_center or None, is_active=is_active)
    return resolved


def crm_leads_current_table(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "crm_leads_current",
        metadata,
        sa.Column("id", sa.BigInteger),
        sa.Column("lead_uid", sa.String(length=128)),
        sa.Column("store_code", sa.String(length=8)),
        sa.Column("pickup_no", sa.String(length=64)),
        sa.Column("status_bucket", sa.String(length=16)),
        sa.Column("customer_name", sa.String(length=256)),
        sa.Column("mobile", sa.String(length=32)),
        sa.Column("pickup_date", sa.String(length=64)),
        sa.Column("pickup_created_at", sa.DateTime(timezone=True)),
        sa.Column("special_instruction", sa.Text),
        sa.Column("reason", sa.String(length=128)),
        sa.Column("source", sa.String(length=128)),
        sa.Column("customer_type", sa.String(length=64)),
        sa.Column("run_id", sa.String(length=64)),
        sa.Column("source_file", sa.Text),
        sa.Column("scraped_at", sa.DateTime(timezone=True)),
    )


def _lead_date(row: dict[str, Any]) -> date:
    value = row.get("pickup_created_at") or row.get("scraped_at")
    if isinstance(value, datetime):
        return value.date()
    return datetime.now(timezone.utc).date()


async def import_td_leads(*, database_url: str, pipeline_run_id: str, logger: JsonLogger | None = None) -> AdapterConversionResult:
    """Convert actionable TD raw leads into unified follow-up leads idempotently."""

    result = AdapterConversionResult()
    metadata = sa.MetaData()
    crm_leads = crm_leads_current_table(metadata)
    async with session_scope(database_url) as session:
        row_mappings = (await session.execute(sa.select(crm_leads).where(sa.func.lower(crm_leads.c.status_bucket).in_(tuple(TD_ACTIONABLE_STATUS_BUCKETS))))).mappings().all()
        rows = [dict(row_mapping) for row_mapping in row_mappings]
        store_resolutions = await _resolve_td_retention_cost_centers(session, rows)
        result.rows_seen = len(rows)
        for row in rows:
            mobile = normalize_mobile(row.get("mobile"))
            store_code = str(row.get("store_code") or "").strip().upper()
            resolution = store_resolutions.get(store_code)
            cost_center = resolution.cost_center if resolution else None
            lead_uid = str(row.get("lead_uid") or "").strip()
            if not store_code or not lead_uid:
                result.rows_skipped += 1
                result.warnings.append(RowWarning("td_missing_identity", "TD lead is missing store or source identity", None, row.get("source_file"), cost_center=cost_center or store_code or None))
                continue
            if resolution is None or not cost_center:
                result.rows_skipped += 1
                result.warnings.append(RowWarning("td_store_unmapped", "TD lead store_code is not mapped in store_master", None, row.get("source_file"), "store_code", cost_center=store_code))
                continue
            if not resolution.is_active:
                result.rows_skipped += 1
                result.warnings.append(RowWarning("td_store_inactive", "TD lead store is not active for customer retention", None, row.get("source_file"), "store_code", cost_center=cost_center))
                continue
            if not mobile.is_valid:
                result.rows_skipped += 1
                result.warnings.append(RowWarning(mobile.warning_code or "invalid_mobile", mobile.warning_message or "Invalid mobile number", None, row.get("source_file"), "mobile", cost_center=cost_center))
                continue
            lead_id, created = await get_or_create_followup_lead(
                session,
                lead_source_type=LEAD_SOURCE_TD,
                source_system="TD_CRM_LEADS_SYNC",
                source_table_name="crm_leads_current",
                source_record_id=lead_uid,
                source_reference=str(row.get("pickup_no") or "").strip() or None,
                cost_center=cost_center,
                customer_name=str(row.get("customer_name") or "").strip() or None,
                mobile_number=str(row.get("mobile") or "").strip() or None,
                normalized_mobile_number=mobile.normalized_mobile or "",
                lead_date=_lead_date(row),
                pipeline_run_id=pipeline_run_id,
                lead_stage=str(row.get("status_bucket") or "").strip() or None,
                assigned_store=cost_center,
                dedupe_by_customer_identity=True,
            )
            if created:
                result.leads_created += 1
            else:
                result.leads_existing += 1
        await session.commit()
    if logger:
        log_event(logger=logger, phase="td_generation", message="td_generation_complete", rows_seen=result.rows_seen, leads_created=result.leads_created, warnings=result.warning_count)
    return result
