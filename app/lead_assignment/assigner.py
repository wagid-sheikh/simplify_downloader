from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Iterable

from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.dashboard_downloader.json_logger import get_logger, log_event


@dataclass
class _LeadRow:
    lead_id: int
    store_code: str
    store_name: str | None
    pickup_date: date | None
    pickup_created_date: date | None
    run_date: date | None
    customer_type: str | None
    mobile_number: str
    customer_name: str | None
    special_instruction: str | None
    final_source: str | None
    source: str | None
    agent_id: int
    agent_code: str
    max_existing_per_lot: int | None
    max_new_per_lot: int | None
    max_daily_leads: int | None


@dataclass
class _GroupState:
    page_group_code: str
    rowid: int = 0
    assigned_existing: int = 0
    assigned_new: int = 0
    assigned_total: int = 0
    today_existing: int = 0
    today_new: int = 0
    today_total: int = 0


async def run_leads_assignment(
    db_session: AsyncSession, triggered_by: str, run_id: str | None = None
) -> int:
    """Assign eligible missed leads to agents and return the batch id."""

    logger = get_logger(run_id=run_id)
    batch_date = date.today()
    log_event(
        logger=logger,
        phase="lead_assignment",
        message="starting leads assignment",
        extras={"triggered_by": triggered_by},
    )

    assignments: list[dict[str, object]] = []

    async with db_session.begin():
        batch_id = await _create_batch(db_session, batch_date, triggered_by, run_id)
        eligible_rows = await _fetch_eligible_leads(db_session)

        if not eligible_rows:
            log_event(
                logger=logger,
                phase="lead_assignment",
                status="warn",
                message="no eligible leads found",
                extras={"batch_id": batch_id},
            )
        else:
            today_counts = await _fetch_today_assignment_counts(db_session)
            assignments = _build_assignments(
                eligible_rows, batch_id, batch_date, today_counts
            )

            if not assignments:
                log_event(
                    logger=logger,
                    phase="lead_assignment",
                    status="warn",
                    message="eligible leads exceeded quota limits",
                    extras={"batch_id": batch_id},
                )
            else:
                inserted_count = await _insert_assignments(db_session, assignments)
                await _mark_leads_assigned(
                    db_session, [item["lead_id"] for item in assignments]
                )

                log_event(
                    logger=logger,
                    phase="lead_assignment",
                    status="ok",
                    message="processed lead assignments",
                    extras={
                        "batch_id": batch_id,
                        "attempted_count": len(assignments),
                        "inserted_count": inserted_count,
                        "skipped_count": len(assignments) - inserted_count,
                    },
                )

    logger.close()
    return batch_id


def _determine_lead_type(customer_type: str | None) -> str:
    normalized = (customer_type or "").strip().lower()
    if normalized.startswith("new"):
        return "N"
    return "E"


def _determine_lead_date(row: _LeadRow) -> date | None:
    return row.pickup_date or row.pickup_created_date or row.run_date


async def _create_batch(
    db_session: AsyncSession, batch_date: date, triggered_by: str, run_id: str | None
) -> int:
    result = await db_session.execute(
        text(
            """
            INSERT INTO lead_assignment_batches (batch_date, triggered_by, run_id)
            VALUES (:batch_date, :triggered_by, :run_id)
            RETURNING id
            """
        ),
        {"batch_date": batch_date, "triggered_by": triggered_by, "run_id": run_id},
    )
    return int(result.scalar_one())


async def _fetch_eligible_leads(db_session: AsyncSession) -> list[_LeadRow]:
    result = await db_session.execute(
        text(
            """
            SELECT
                ml.pickup_row_id AS lead_id,
                ml.store_code,
                ml.store_name,
                ml.pickup_date,
                ml.pickup_created_date,
                ml.run_date,
                ml.customer_type,
                ml.mobile_number,
                ml.customer_name,
                ml.special_instruction,
                ml.final_source,
                ml.source,
                slam.agent_id,
                am.agent_code,
                slam.max_existing_per_lot,
                slam.max_new_per_lot,
                slam.max_daily_leads
            FROM missed_leads ml
            JOIN store_master sm ON sm.store_code = ml.store_code
            JOIN store_lead_assignment_map slam
              ON slam.store_code = ml.store_code AND slam.is_enabled = true
            JOIN agents_master am ON am.id = slam.agent_id AND am.is_active = true
            WHERE sm.assign_leads = true
              AND ml.customer_type = 'New'
              AND ml.lead_assigned = false
              AND (ml.is_order_placed = false OR ml.is_order_placed IS NULL)
            ORDER BY ml.pickup_created_date DESC
            """
        )
    )

    return [
        _LeadRow(
            lead_id=row.lead_id,
            store_code=row.store_code,
            store_name=row.store_name,
            pickup_date=row.pickup_date,
            pickup_created_date=row.pickup_created_date,
            run_date=row.run_date,
            customer_type=row.customer_type,
            mobile_number=row.mobile_number,
            customer_name=row.customer_name,
            special_instruction=row.special_instruction,
            final_source=row.final_source,
            source=row.source,
            agent_id=row.agent_id,
            agent_code=row.agent_code,
            max_existing_per_lot=row.max_existing_per_lot,
            max_new_per_lot=row.max_new_per_lot,
            max_daily_leads=row.max_daily_leads,
        )
        for row in result
    ]


async def _fetch_today_assignment_counts(db_session: AsyncSession) -> dict[tuple[str, int], dict[str, int]]:
    result = await db_session.execute(
        text(
            """
            SELECT
                store_code,
                agent_id,
                SUM(CASE WHEN lead_type = 'E' THEN 1 ELSE 0 END) AS existing_count,
                SUM(CASE WHEN lead_type = 'N' THEN 1 ELSE 0 END) AS new_count,
                COUNT(*) AS total_count
            FROM lead_assignments
            WHERE (lead_date = CURRENT_DATE OR CAST(assigned_at AS DATE) = CURRENT_DATE)
            GROUP BY store_code, agent_id
            """
        )
    )

    return {
        (row.store_code, row.agent_id): {
            "existing": int(row.existing_count or 0),
            "new": int(row.new_count or 0),
            "total": int(row.total_count or 0),
        }
        for row in result
    }


def _build_assignments(
    leads: Iterable[_LeadRow],
    batch_id: int,
    batch_date: date,
    today_counts: dict[tuple[str, int], dict[str, int]],
) -> list[dict[str, object]]:
    assignments: list[dict[str, object]] = []
    group_state: dict[tuple[str, int], _GroupState] = {}
    today_keyed = defaultdict(lambda: {"existing": 0, "new": 0, "total": 0}, today_counts)

    for lead in leads:
        key = (lead.store_code, lead.agent_id)
        state = group_state.get(key)
        if state is None:
            today = today_keyed[key]
            page_group_code = f"L{batch_date.strftime('%y%m%d')}{lead.agent_code}"
            state = _GroupState(
                page_group_code=page_group_code,
                today_existing=today.get("existing", 0),
                today_new=today.get("new", 0),
                today_total=today.get("total", 0),
            )
            group_state[key] = state

        lead_type = _determine_lead_type(lead.customer_type)

        if lead_type == "E" and lead.max_existing_per_lot is not None:
            if state.assigned_existing + state.today_existing >= lead.max_existing_per_lot:
                continue

        if lead_type == "N" and lead.max_new_per_lot is not None:
            if state.assigned_new + state.today_new >= lead.max_new_per_lot:
                continue

        if lead.max_daily_leads is not None:
            if state.assigned_total + state.today_total >= lead.max_daily_leads:
                continue

        state.rowid += 1
        state.assigned_total += 1
        if lead_type == "E":
            state.assigned_existing += 1
        else:
            state.assigned_new += 1

        lead_date = _determine_lead_date(lead)
        lead_source = lead.final_source or lead.source

        assignments.append(
            {
                "assignment_batch_id": batch_id,
                "lead_id": lead.lead_id,
                "agent_id": lead.agent_id,
                "page_group_code": state.page_group_code,
                "rowid": state.rowid,
                "lead_assignment_code": f"{state.page_group_code}-{state.rowid:04d}",
                "store_code": lead.store_code,
                "store_name": lead.store_name,
                "lead_date": lead_date,
                "lead_type": lead_type,
                "mobile_number": lead.mobile_number,
                "cx_name": lead.customer_name,
                "address": lead.special_instruction,
                "lead_source": lead_source,
            }
        )

    return assignments


async def _insert_assignments(
    db_session: AsyncSession, assignments: list[dict[str, object]]
) -> int:
    if not assignments:
        return 0

    result = await db_session.execute(
        text(
            """
            INSERT INTO lead_assignments (
                assignment_batch_id,
                lead_id,
                agent_id,
                page_group_code,
                rowid,
                lead_assignment_code,
                store_code,
                store_name,
                lead_date,
                lead_type,
                mobile_number,
                cx_name,
                address,
                lead_source
            ) VALUES (
                :assignment_batch_id,
                :lead_id,
                :agent_id,
                :page_group_code,
                :rowid,
                :lead_assignment_code,
                :store_code,
                :store_name,
                :lead_date,
                :lead_type,
                :mobile_number,
                :cx_name,
                :address,
                :lead_source
            )
            ON CONFLICT DO NOTHING
            """
        ),
        assignments,
    )

    rowcount = result.rowcount if result.rowcount is not None else 0
    return max(rowcount, 0)


async def _mark_leads_assigned(db_session: AsyncSession, lead_ids: list[int]) -> None:
    if not lead_ids:
        return
    update_sql = text(
        """
        UPDATE missed_leads
        SET lead_assigned = true
        WHERE pickup_row_id IN :lead_ids
        """
    ).bindparams(bindparam("lead_ids", expanding=True))
    await db_session.execute(update_sql, {"lead_ids": lead_ids})
