"""Management analytics for the customer retention pipeline.

Builds the Section 34 owner-summary payload from Phase 1-4 tables/results.
Financial recovery values are intentionally read from ``vw_orders.order_amount``.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from .constants import (
    CAP_WORK_SECTION_DUE_FOLLOWUP,
    CAP_WORK_SECTION_EXTERNAL_LEAD,
    CAP_WORK_SECTION_FRESH_RETENTION,
    CAP_WORK_SECTION_PENDING_CARRY_FORWARD,
    CAP_WORK_SECTION_TD_LEAD,
    LEAD_SOURCE_EXTERNAL,
    LEAD_SOURCE_TYPES,
    LEAD_STATUS_CLOSED,
    LEAD_STATUS_DUE_FOLLOWUP,
    LEAD_STATUS_OPEN,
    LEAD_STATUS_PENDING,
    LEAD_STATUS_RECOVERED,
    LEAD_STATUS_WORKED,
    PERMANENT_SUPPRESSION_WORKBOOK_OUTCOMES,
    TIME_BOUND_SUPPRESSION_WORKBOOK_OUTCOMES,
)
from .db_tables import trx_customer_followup_history, trx_customer_followup_leads, trx_customer_suppression
from .types import RowWarning, WorkbookIngestionResult
from .workbook_generator import WorkbookGenerationResult
from .workbook_selection import StoreWorkbookSelectionResult

UNSPECIFIED_HANDLED_BY = "UNSPECIFIED"
INCOMPLETE_STATUSES = (LEAD_STATUS_OPEN, LEAD_STATUS_PENDING, LEAD_STATUS_DUE_FOLLOWUP, LEAD_STATUS_WORKED)
DEAD_END_OUTCOMES = set(PERMANENT_SUPPRESSION_WORKBOOK_OUTCOMES) | set(TIME_BOUND_SUPPRESSION_WORKBOOK_OUTCOMES)

VW_ORDERS = sa.table(
    "vw_orders",
    sa.column("cost_center"),
    sa.column("order_number"),
    sa.column("order_amount"),
)


@dataclass(frozen=True)
class RunTiming:
    run_id: str
    started_at: datetime
    ended_at: datetime | None = None
    execution_mode: str = "manual"
    status: str = "success"
    env: str | None = None


async def build_management_summary_payload(
    session: AsyncSession,
    *,
    run_id: str,
    run_date: date,
    timing: RunTiming,
    selections: Iterable[StoreWorkbookSelectionResult] = (),
    workbook_result: WorkbookGenerationResult | None = None,
    ingestion_results: Iterable[WorkbookIngestionResult] = (),
    row_warnings: Iterable[RowWarning] = (),
    generated_files: Iterable[str | Path] = (),
) -> dict[str, Any]:
    selection_list = list(selections)
    ingestion_list = list(ingestion_results)
    warnings = [*row_warnings, *(w for result in ingestion_list for w in result.warnings), *(w for selection in selection_list for w in selection.warnings)]
    workbook_paths = {out.cost_center: str(out.output_path) for out in (workbook_result.outputs if workbook_result else ())}
    for path in generated_files:
        # Preserve additional generated files even if callers cannot map them to a store.
        workbook_paths.setdefault("_additional", str(path))

    stores = sorted({selection.cost_center for selection in selection_list} | await _fetch_run_store_codes(session, run_id=run_id))
    source_summary = await _source_summary(session, run_id=run_id)
    recovered_revenue = await _recovered_revenue_by_lead(session, run_id=run_id)
    staff = await _staff_productivity(session, run_id=run_id)
    suppression = await _suppression_summary(session, run_id=run_id)
    shifted = await _shifted_destination_summary(session, run_id=run_id)
    aging = _aging_from_selections(selection_list)
    warning_summary = _warning_summary(warnings, ingestion_list, aging, staff)

    store_rows: list[dict[str, Any]] = []
    selection_by_store = {selection.cost_center: selection for selection in selection_list}
    staff_by_store = defaultdict(list)
    for row in staff:
        staff_by_store[row["cost_center"]].append(row)
    for store in stores:
        selection = selection_by_store.get(store)
        counts = selection.counts_by_category if selection else {}
        workload = selection.workload if selection else None
        store_recovered = await _store_recovered_counts(session, run_id=run_id, cost_center=store)
        store_rows.append(
            {
                "cost_center": store,
                "workbook_generated_path": workbook_paths.get(store),
                "due_followups_included": int(counts.get(CAP_WORK_SECTION_DUE_FOLLOWUP, 0)),
                "pending_carry_forward_included": int(counts.get(CAP_WORK_SECTION_PENDING_CARRY_FORWARD, 0)),
                "fresh_retention_leads_generated": int(counts.get(CAP_WORK_SECTION_FRESH_RETENTION, 0)),
                "fresh_retention_frozen": bool(workload.frozen) if workload else False,
                "td_leads_included": int(counts.get(CAP_WORK_SECTION_TD_LEAD, 0)),
                "external_leads_included": int(counts.get(CAP_WORK_SECTION_EXTERNAL_LEAD, 0)),
                "shifted_location_destination_leads": shifted.get(store, 0),
                "recovered_customers": store_recovered["count"],
                "recovered_revenue_value": str(store_recovered["revenue"]),
                "closed_leads": await _closed_count(session, run_id=run_id, cost_center=store),
                "suppression_additions_by_outcome": suppression["by_store"].get(store, {}),
                "pending_suppression_approval_count": suppression["pending_by_store"].get(store, 0),
                "complaints_raised": await _complaints_count(session, run_id=run_id, cost_center=store),
                "rows_with_warnings": sum(1 for warning in warnings if warning.cost_center == store),
                "aging_actionable_workload": aging.get(store, _empty_aging(store)),
                "staff_productivity": staff_by_store.get(store, []),
            }
        )

    ended_at = timing.ended_at or datetime.now(timezone.utc)
    return {
        "run_summary": {
            "pipeline_run_id": run_id,
            "run_date": run_date.isoformat(),
            "run_start_time": timing.started_at.isoformat(),
            "run_end_time": ended_at.isoformat(),
            "duration_seconds": round((ended_at - timing.started_at).total_seconds(), 3),
            "execution_mode": timing.execution_mode,
            "success_failure_status": timing.status,
            "env": timing.env,
        },
        "store_summary": store_rows,
        "aging_actionable_workload": [row["aging_actionable_workload"] for row in store_rows],
        "staff_productivity": staff,
        "source_wise_summary": source_summary,
        "warning_error_summary": warning_summary,
        "recovered_revenue_by_lead": {str(k): str(v) for k, v in recovered_revenue.items()},
    }


async def _fetch_run_store_codes(session: AsyncSession, *, run_id: str) -> set[str]:
    rows = (await session.execute(sa.select(trx_customer_followup_leads.c.cost_center).where(sa.or_(trx_customer_followup_leads.c.created_by_pipeline_run_id == run_id, trx_customer_followup_leads.c.updated_by_pipeline_run_id == run_id)))).scalars().all()
    return {str(row) for row in rows if row}


async def _source_summary(session: AsyncSession, *, run_id: str) -> list[dict[str, Any]]:
    revenue_by_lead = await _recovered_revenue_by_lead(session, run_id=run_id)
    rows = (await session.execute(sa.select(trx_customer_followup_leads).where(sa.or_(trx_customer_followup_leads.c.created_by_pipeline_run_id == run_id, trx_customer_followup_leads.c.updated_by_pipeline_run_id == run_id)))).mappings().all()
    by_source = {source: Counter() for source in LEAD_SOURCE_TYPES}
    for row in rows:
        source = row["lead_source_type"]
        counter = by_source.setdefault(source, Counter())
        counter["included"] += 1
        if row.get("lead_status") == LEAD_STATUS_WORKED or row.get("handled_by"):
            counter["worked"] += 1
        if row.get("lead_status") in INCOMPLETE_STATUSES and not row.get("is_closed"):
            counter["pending"] += 1
        if row.get("is_closed") or row.get("lead_status") in (LEAD_STATUS_CLOSED, LEAD_STATUS_RECOVERED):
            counter["closed"] += 1
        if row.get("is_recovered"):
            counter["recovered"] += 1
    out = []
    for source in LEAD_SOURCE_TYPES:
        lead_ids = [int(row["lead_id"]) for row in rows if row["lead_source_type"] == source]
        revenue = sum((revenue_by_lead.get(lead_id, Decimal("0")) for lead_id in lead_ids), Decimal("0"))
        out.append({"source": source, **{key: int(by_source[source][key]) for key in ("included", "worked", "pending", "closed", "recovered")}, "recovered_revenue_value": str(revenue)})
    return out


async def _recovered_revenue_by_lead(session: AsyncSession, *, run_id: str) -> dict[int, Decimal]:
    leads = (await session.execute(sa.select(trx_customer_followup_leads.c.lead_id, trx_customer_followup_leads.c.cost_center, trx_customer_followup_leads.c.recovered_order_id).where(trx_customer_followup_leads.c.updated_by_pipeline_run_id == run_id, trx_customer_followup_leads.c.is_recovered.is_(True), trx_customer_followup_leads.c.recovered_order_id.is_not(None)))).all()
    if not leads:
        return {}
    clauses = [sa.and_(VW_ORDERS.c.cost_center == cost_center, VW_ORDERS.c.order_number == order_id) for lead_id, cost_center, order_id in leads]
    order_rows = (await session.execute(sa.select(VW_ORDERS.c.cost_center, VW_ORDERS.c.order_number, VW_ORDERS.c.order_amount).where(sa.or_(*clauses)))).mappings().all()
    amount_by_key = {(str(row["cost_center"]), str(row["order_number"])): Decimal(str(row["order_amount"] or "0")) for row in order_rows}
    return {int(lead_id): amount_by_key.get((str(cost_center), str(order_id)), Decimal("0")) for lead_id, cost_center, order_id in leads}


async def _staff_productivity(session: AsyncSession, *, run_id: str) -> list[dict[str, Any]]:
    histories = (await session.execute(sa.select(trx_customer_followup_history, trx_customer_followup_leads.c.cost_center).join(trx_customer_followup_leads, trx_customer_followup_history.c.lead_id == trx_customer_followup_leads.c.lead_id).where(trx_customer_followup_history.c.pipeline_run_id == run_id))).mappings().all()
    grouped: dict[tuple[str, str], Counter] = defaultdict(Counter)
    for row in histories:
        handled_by = str(row.get("handled_by") or "").strip() or UNSPECIFIED_HANDLED_BY
        key = (str(row["cost_center"]), handled_by)
        grouped[key]["total_leads_assigned"] += 1
        response = row.get("customer_response")
        if response or row.get("contact_attempted") is not None:
            grouped[key]["worked"] += 1
        if response in DEAD_END_OUTCOMES:
            grouped[key]["dead_ends_logged"] += 1
    return [{"cost_center": cc, "handled_by": hb, **{k: int(v) for k, v in counter.items()}, "operational_warning": hb == UNSPECIFIED_HANDLED_BY} for (cc, hb), counter in sorted(grouped.items())]


def _aging_from_selections(selections: list[StoreWorkbookSelectionResult]) -> dict[str, dict[str, Any]]:
    output = {}
    for selection in selections:
        pending_rows = [row for row in selection.rows if row.work_section == CAP_WORK_SECTION_PENDING_CARRY_FORWARD]
        older3 = sum(1 for row in pending_rows if (selection.run_date - row.lead_date).days > 3)
        older7 = sum(1 for row in pending_rows if (selection.run_date - row.lead_date).days > 7)
        workload = selection.workload
        output[selection.cost_center] = {
            "cost_center": selection.cost_center,
            "pending_carry_forward": len(pending_rows),
            "rolling_14_day_backlog_count": int(workload.incomplete_recent_retention_count) if workload else 0,
            "unworked_gt_3_days": older3,
            "unworked_gt_7_days": older7,
            "backlog_threshold": int(workload.threshold) if workload else 0,
            "fresh_retention_frozen": bool(workload.frozen) if workload else False,
        }
    return output


def _empty_aging(cost_center: str) -> dict[str, Any]:
    return {"cost_center": cost_center, "pending_carry_forward": 0, "rolling_14_day_backlog_count": 0, "unworked_gt_3_days": 0, "unworked_gt_7_days": 0, "backlog_threshold": 0, "fresh_retention_frozen": False}


def _warning_summary(warnings: list[RowWarning], ingestion_results: list[WorkbookIngestionResult], aging: Mapping[str, Mapping[str, Any]], staff: list[dict[str, Any]]) -> dict[str, Any]:
    codes = Counter(w.code for w in warnings)
    return {
        "returned_files_processed": len(ingestion_results),
        "returned_files_failed": 0,
        "duplicate_uploads_ignored": sum(r.history_existing for r in ingestion_results) + codes["duplicate_upload"],
        "system_column_edits_ignored": sum(r.protected_edits_ignored for r in ingestion_results) + codes["protected_column_edit"],
        "invalid_dropdowns_normalized": codes["invalid_dropdown"] + codes["normalized_dropdown"],
        "invalid_mobiles": codes["invalid_mobile"] + codes["invalid_mobile_identity"],
        "rows_left_pending_due_to_missing_fields": codes["missing_field"] + sum(r.rows_pending_not_updated for r in ingestion_results),
        "rows_skipped_invalid_mobile": codes["invalid_mobile"] + codes["invalid_mobile_identity"],
        "target_cost_center_warnings": codes["invalid_target_cost_center"] + codes["same_store_target_cost_center"],
        "destination_external_leads_created": codes["shifted_destination_lead_created"],
        "pending_suppression_approval_count": codes["pending_suppression_approval"],
        "frozen_stores": [cc for cc, row in aging.items() if row.get("fresh_retention_frozen")],
        "unspecified_handled_by_warning_count": sum(1 for row in staff if row["handled_by"] == UNSPECIFIED_HANDLED_BY),
        "warnings_by_code": dict(codes),
    }


async def _suppression_summary(session: AsyncSession, *, run_id: str) -> dict[str, Any]:
    rows = (await session.execute(sa.select(trx_customer_suppression).where(trx_customer_suppression.c.created_by_pipeline_run_id == run_id))).mappings().all()
    by_store: dict[str, Counter] = defaultdict(Counter)
    pending_by_store: Counter = Counter()
    for row in rows:
        cc = str(row["cost_center"])
        reason = str(row["suppression_reason"])
        by_store[cc][reason] += 1
        if row["suppression_state"] == SUPPRESSION_STATE_PENDING_APPROVAL:
            pending_by_store[cc] += 1
    return {"by_store": {k: dict(v) for k, v in by_store.items()}, "pending_by_store": dict(pending_by_store)}


async def _shifted_destination_summary(session: AsyncSession, *, run_id: str) -> dict[str, int]:
    rows = (await session.execute(sa.select(trx_customer_followup_leads.c.cost_center, sa.func.count()).where(trx_customer_followup_leads.c.created_by_pipeline_run_id == run_id, trx_customer_followup_leads.c.lead_source_type == LEAD_SOURCE_EXTERNAL, trx_customer_followup_leads.c.shifted_from_lead_id.is_not(None)).group_by(trx_customer_followup_leads.c.cost_center))).all()
    return {str(cc): int(count) for cc, count in rows}


async def _store_recovered_counts(session: AsyncSession, *, run_id: str, cost_center: str) -> dict[str, Any]:
    revenue = await _recovered_revenue_by_lead(session, run_id=run_id)
    ids = (await session.execute(sa.select(trx_customer_followup_leads.c.lead_id).where(trx_customer_followup_leads.c.updated_by_pipeline_run_id == run_id, trx_customer_followup_leads.c.cost_center == cost_center, trx_customer_followup_leads.c.is_recovered.is_(True)))).scalars().all()
    return {"count": len(ids), "revenue": sum((revenue.get(int(i), Decimal("0")) for i in ids), Decimal("0"))}


async def _closed_count(session: AsyncSession, *, run_id: str, cost_center: str) -> int:
    return int((await session.execute(sa.select(sa.func.count()).select_from(trx_customer_followup_leads).where(trx_customer_followup_leads.c.updated_by_pipeline_run_id == run_id, trx_customer_followup_leads.c.cost_center == cost_center, trx_customer_followup_leads.c.is_closed.is_(True)))).scalar_one())


async def _complaints_count(session: AsyncSession, *, run_id: str, cost_center: str) -> int:
    return int((await session.execute(sa.select(sa.func.count()).select_from(trx_customer_followup_history).join(trx_customer_followup_leads, trx_customer_followup_history.c.lead_id == trx_customer_followup_leads.c.lead_id).where(trx_customer_followup_history.c.pipeline_run_id == run_id, trx_customer_followup_leads.c.cost_center == cost_center, trx_customer_followup_history.c.complaint_flag.is_(True)))).scalar_one())
