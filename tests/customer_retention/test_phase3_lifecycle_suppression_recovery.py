from __future__ import annotations

from datetime import date, datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from app.common.db import session_scope
from app.customer_retention.constants import (
    LEAD_SOURCE_EXTERNAL,
    LEAD_SOURCE_RETENTION,
    LEAD_SOURCE_TD,
    LEAD_STATUS_CLOSED,
    LEAD_STATUS_DUE_FOLLOWUP,
    LEAD_STATUS_OPEN,
    LEAD_STATUS_RECOVERED,
    LEAD_STATUS_WORKED,
    LIFECYCLE_BUCKET_COLD,
    LIFECYCLE_BUCKET_COOLING,
    LIFECYCLE_BUCKET_DORMANT,
    LIFECYCLE_BUCKET_LOST,
    LIFECYCLE_BUCKET_WARM,
    SUPPRESSION_STATE_ACTIVE,
    SUPPRESSION_STATE_PENDING_APPROVAL,
    SUPPRESSION_STATE_REJECTED,
    WORKBOOK_OUTCOME_DO_NOT_CONTACT,
    WORKBOOK_OUTCOME_LEAD_STALE,
    WORKBOOK_OUTCOME_NOT_INTERESTED,
    WORKBOOK_OUTCOME_PICKUP_REQUESTED,
    WORKBOOK_OUTCOME_WRONG_NUMBER,
)
from app.customer_retention.db_tables import metadata, trx_customer_followup_history, trx_customer_followup_leads, trx_customer_suppression
from app.customer_retention.lifecycle import apply_lifecycle_transition, classify_lifecycle, compute_priority_decision, recommended_strategy
from app.customer_retention.recovery_detection import detect_recoveries
from app.customer_retention.snapshot import build_customer_retention_snapshot
from app.customer_retention.suppression import approve_suppression, check_active_suppression, create_pending_permanent_suppression, create_time_bound_suppression, reject_suppression


async def _prepare_db(tmp_path: Path) -> str:
    db = tmp_path / "phase3.db"
    url = f"sqlite+aiosqlite:///{db}"
    engine = create_async_engine(url, future=True)
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
        await conn.execute(sa.text("""
            CREATE TABLE vw_orders (
                cost_center TEXT NOT NULL,
                order_number TEXT NOT NULL,
                order_date TIMESTAMP NOT NULL,
                customer_name TEXT,
                mobile_number TEXT,
                order_amount NUMERIC(12, 2) NOT NULL
            )
        """))
    await engine.dispose()
    return url


async def _insert_lead(session: sa.ext.asyncio.AsyncSession, *, lead_id: int, cost_center: str = "A100", mobile: str = "9876543210", source: str = LEAD_SOURCE_EXTERNAL, lead_date: date = date(2026, 1, 1), status: str = LEAD_STATUS_OPEN, next_followup_date: date | None = None) -> int:
    await session.execute(trx_customer_followup_leads.insert().values(
        lead_id=lead_id,
        lead_uuid=f"lead-{lead_id}",
        lead_source_type=source,
        source_system="test",
        source_table_name="source" if source in {LEAD_SOURCE_TD, LEAD_SOURCE_EXTERNAL} else None,
        source_record_id=str(lead_id) if source in {LEAD_SOURCE_TD, LEAD_SOURCE_EXTERNAL} else None,
        source_reference=f"ref-{lead_id}",
        cost_center=cost_center,
        customer_name=f"Customer {lead_id}",
        mobile_number=mobile,
        normalized_mobile_number=mobile,
        lead_date=lead_date,
        lead_status=status,
        lifecycle_bucket=LIFECYCLE_BUCKET_WARM if source == LEAD_SOURCE_RETENTION else None,
        next_followup_date=next_followup_date,
        contact_attempted=False,
        complaint_flag=False,
        do_not_contact_flag=False,
        is_closed=status == LEAD_STATUS_CLOSED,
        is_recovered=False,
        suppression_applied=False,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        created_by_pipeline_run_id="run0" if source == LEAD_SOURCE_RETENTION else None,
    ))
    return lead_id


def test_lifecycle_bucket_classifier_boundaries_and_invalid_mobile() -> None:
    snapshot_date = date(2026, 6, 12)
    cases = [
        (22, LIFECYCLE_BUCKET_WARM, True),
        (45, LIFECYCLE_BUCKET_WARM, True),
        (46, LIFECYCLE_BUCKET_COOLING, True),
        (75, LIFECYCLE_BUCKET_COOLING, True),
        (76, LIFECYCLE_BUCKET_DORMANT, True),
        (120, LIFECYCLE_BUCKET_DORMANT, True),
        (121, LIFECYCLE_BUCKET_COLD, True),
        (180, LIFECYCLE_BUCKET_COLD, True),
        (181, LIFECYCLE_BUCKET_LOST, True),
    ]
    for days_since, bucket, eligible in cases:
        result = classify_lifecycle(last_order_date=snapshot_date - timedelta(days=days_since), snapshot_date=snapshot_date, normalized_mobile_number="9876543210")
        assert result.lifecycle_bucket == bucket
        assert result.days_since_last_order == days_since
        assert result.eligible_for_retention is eligible
    invalid = classify_lifecycle(last_order_date=snapshot_date - timedelta(days=30), snapshot_date=snapshot_date, normalized_mobile_number="1111111111")
    assert invalid.eligible_for_retention is False
    assert invalid.warning_code == "invalid_mobile_identity"


def test_priority_scoring_and_strategy_for_buckets_td_and_external() -> None:
    warm = compute_priority_decision(lifecycle_bucket=LIFECYCLE_BUCKET_WARM, days_since_last_order=30, total_orders=2, lifetime_spend=Decimal("1000"), average_order_value=Decimal("500"), last_order_amount=Decimal("500"))
    lost = compute_priority_decision(lifecycle_bucket=LIFECYCLE_BUCKET_LOST, days_since_last_order=220, total_orders=10, lifetime_spend=Decimal("10000"), average_order_value=Decimal("1000"), last_order_amount=Decimal("1200"), previous_followup_outcome=WORKBOOK_OUTCOME_PICKUP_REQUESTED)
    suppressed = compute_priority_decision(lifecycle_bucket=LIFECYCLE_BUCKET_LOST, suppressed=True)
    assert lost.priority_score > warm.priority_score
    assert suppressed.priority_score < warm.priority_score
    assert "comeback" in recommended_strategy(lifecycle_bucket=LIFECYCLE_BUCKET_LOST).lower()
    assert "inbound" in compute_priority_decision(lifecycle_bucket=None, lead_source_type=LEAD_SOURCE_TD).recommended_strategy
    assert "campaign" in compute_priority_decision(lifecycle_bucket=None, lead_source_type=LEAD_SOURCE_EXTERNAL).recommended_strategy


@pytest.mark.asyncio
async def test_suppression_lookup_expiry_pending_approval_approval_rejection_and_cross_store(tmp_path: Path) -> None:
    url = await _prepare_db(tmp_path)
    async with session_scope(url) as session:
        await _insert_lead(session, lead_id=1)
        await _insert_lead(session, lead_id=2, cost_center="B200")
        active = await create_time_bound_suppression(session, cost_center="A100", normalized_mobile_number="9876543210", reason=WORKBOOK_OUTCOME_NOT_INTERESTED, start_date=date(2026, 6, 1), source_lead_id=1, pipeline_run_id="run1")
        duplicate = await create_time_bound_suppression(session, cost_center="A100", normalized_mobile_number="9876543210", reason=WORKBOOK_OUTCOME_NOT_INTERESTED, start_date=date(2026, 6, 1), source_lead_id=1, pipeline_run_id="run1")
        pending = await create_pending_permanent_suppression(session, cost_center="A100", normalized_mobile_number="9876543210", reason=WORKBOOK_OUTCOME_WRONG_NUMBER, source_lead_id=1, pipeline_run_id="run1")
        await session.commit()
        assert duplicate.suppression_id == active.suppression_id
        assert pending.suppression_state == SUPPRESSION_STATE_PENDING_APPROVAL
        assert (await check_active_suppression(session, cost_center="A100", normalized_mobile_number="9876543210", as_of_date=date(2026, 6, 15))).is_suppressed is True
        assert (await check_active_suppression(session, cost_center="B200", normalized_mobile_number="9876543210", as_of_date=date(2026, 6, 15))).is_suppressed is False
        assert (await check_active_suppression(session, cost_center="A100", normalized_mobile_number="9876543210", as_of_date=date(2026, 9, 30))).is_suppressed is False
        approval = await approve_suppression(session, suppression_id=pending.suppression_id or 0, approved_by="manager", pipeline_run_id="run2")
        approval_again = await approve_suppression(session, suppression_id=pending.suppression_id or 0, approved_by="manager", pipeline_run_id="run2")
        await session.commit()
        assert approval.changed is True
        assert approval_again.changed is False
        assert (await check_active_suppression(session, cost_center="A100", normalized_mobile_number="9876543210", as_of_date=date(2026, 9, 30))).is_suppressed is True
        history_count = (await session.execute(sa.select(sa.func.count()).select_from(trx_customer_followup_history))).scalar_one()
        assert history_count == 3

    async with session_scope(url) as session:
        await _insert_lead(session, lead_id=3, mobile="9876543211")
        pending_reject = await create_pending_permanent_suppression(session, cost_center="A100", normalized_mobile_number="9876543211", reason=WORKBOOK_OUTCOME_DO_NOT_CONTACT, source_lead_id=3, pipeline_run_id="run3")
        rejected = await reject_suppression(session, suppression_id=pending_reject.suppression_id or 0, rejected_by="manager", pipeline_run_id="run3")
        await session.commit()
        assert rejected.changed is True
        assert (await check_active_suppression(session, cost_center="A100", normalized_mobile_number="9876543211", as_of_date=date(2026, 6, 15))).is_suppressed is False


@pytest.mark.asyncio
async def test_lifecycle_transitions_history_idempotency_and_suppression_rules(tmp_path: Path) -> None:
    url = await _prepare_db(tmp_path)
    async with session_scope(url) as session:
        await _insert_lead(session, lead_id=1)
        missing = await apply_lifecycle_transition(session, lead_id=1, customer_response=None, contact_attempted=None, pipeline_run_id="run1", event_key="row1")
        worked = await apply_lifecycle_transition(session, lead_id=1, customer_response=WORKBOOK_OUTCOME_PICKUP_REQUESTED, contact_attempted=True, next_followup_date=date(2026, 6, 20), pipeline_run_id="run1", event_key="row2")
        duplicate = await apply_lifecycle_transition(session, lead_id=1, customer_response=WORKBOOK_OUTCOME_PICKUP_REQUESTED, contact_attempted=True, next_followup_date=date(2026, 6, 20), pipeline_run_id="run1", event_key="row2")
        await _insert_lead(session, lead_id=2, mobile="9876543211")
        stale = await apply_lifecycle_transition(session, lead_id=2, customer_response=WORKBOOK_OUTCOME_LEAD_STALE, contact_attempted=True, pipeline_run_id="run1", event_key="row3")
        await _insert_lead(session, lead_id=3, mobile="9876543212")
        permanent = await apply_lifecycle_transition(session, lead_id=3, customer_response=WORKBOOK_OUTCOME_DO_NOT_CONTACT, contact_attempted=True, pipeline_run_id="run1", event_key="row4")
        await session.commit()
        assert missing.warnings == ("required_fields_missing",)
        assert worked.new_status == LEAD_STATUS_DUE_FOLLOWUP
        assert duplicate.history_inserted is False
        assert stale.new_status == LEAD_STATUS_CLOSED
        assert stale.suppression_id is not None
        assert permanent.pending_approval_id is not None
        active_permanent_count = (await session.execute(sa.select(sa.func.count()).select_from(trx_customer_suppression).where(trx_customer_suppression.c.is_permanent.is_(True), trx_customer_suppression.c.suppression_state == SUPPRESSION_STATE_ACTIVE))).scalar_one()
        assert active_permanent_count == 0
        lead3 = (await session.execute(sa.select(trx_customer_followup_leads.c.lead_status).where(trx_customer_followup_leads.c.lead_id == 3))).scalar_one()
        assert lead3 == LEAD_STATUS_CLOSED


@pytest.mark.asyncio
async def test_recovery_detection_idempotent_store_scoped_and_uses_vw_order_amount(tmp_path: Path) -> None:
    url = await _prepare_db(tmp_path)
    async with session_scope(url) as session:
        await _insert_lead(session, lead_id=1, cost_center="A100", mobile="9876543210", lead_date=date(2026, 6, 1))
        await _insert_lead(session, lead_id=2, cost_center="B200", mobile="9876543210", lead_date=date(2026, 6, 1))
        await _insert_lead(session, lead_id=3, cost_center="A100", mobile="9876543211", lead_date=date(2026, 6, 10))
        await _insert_lead(session, lead_id=4, cost_center="A100", mobile="9876543211", lead_date=date(2026, 6, 12))
        await _insert_lead(session, lead_id=5, cost_center="A100", mobile="9876543212", lead_date=date(2026, 6, 20))
        await session.execute(sa.text("""
            INSERT INTO vw_orders (cost_center, order_number, order_date, customer_name, mobile_number, order_amount) VALUES
            ('A100', 'OA1', '2026-06-05 10:00:00', 'A', '9876543210', 777.50),
            ('B200', 'OB1', '2026-06-05 10:00:00', 'B', '9876543210', 100.00),
            ('A100', 'OA2', '2026-06-15 10:00:00', 'C', '9876543211', 900.00),
            ('A100', 'OLD', '2026-06-01 10:00:00', 'D', '9876543212', 111.00),
            ('A100', 'BAD', '2026-06-25 10:00:00', 'Bad', '1111111111', 222.00)
        """))
        first = await detect_recoveries(session, as_of_date=date(2026, 6, 30), pipeline_run_id="run1")
        second = await detect_recoveries(session, as_of_date=date(2026, 6, 30), pipeline_run_id="run1")
        await session.commit()
        assert first.leads_recovered == 3
        assert second.leads_recovered == 0
        assert {match.lead_id: match.recovered_amount for match in first.matches}[1] == Decimal("777.50")
        statuses = (await session.execute(sa.select(trx_customer_followup_leads.c.lead_id, trx_customer_followup_leads.c.lead_status, trx_customer_followup_leads.c.recovered_order_id).order_by(trx_customer_followup_leads.c.lead_id))).all()
        assert (1, LEAD_STATUS_RECOVERED, "OA1") in statuses
        assert (2, LEAD_STATUS_RECOVERED, "OB1") in statuses
        assert (3, LEAD_STATUS_CLOSED, None) in statuses
        assert (4, LEAD_STATUS_RECOVERED, "OA2") in statuses
        assert (5, LEAD_STATUS_OPEN, None) in statuses


@pytest.mark.asyncio
async def test_recovery_detection_recovers_lead_once_when_multiple_orders_match(tmp_path: Path) -> None:
    url = await _prepare_db(tmp_path)
    async with session_scope(url) as session:
        await _insert_lead(session, lead_id=1, cost_center="A100", mobile="9876543210", lead_date=date(2026, 6, 1))
        await session.execute(sa.text("""
            INSERT INTO vw_orders (cost_center, order_number, order_date, customer_name, mobile_number, order_amount) VALUES
            ('A100', 'FIRST', '2026-06-05 10:00:00', 'A', '9876543210', 100.00),
            ('A100', 'SECOND', '2026-06-06 10:00:00', 'A', '9876543210', 200.00)
        """))

        result = await detect_recoveries(session, as_of_date=date(2026, 6, 30), pipeline_run_id="run1")
        await session.commit()

        history_count = (await session.execute(sa.select(sa.func.count()).select_from(trx_customer_followup_history))).scalar_one()
        recovered_order_id = (await session.execute(sa.select(trx_customer_followup_leads.c.recovered_order_id).where(trx_customer_followup_leads.c.lead_id == 1))).scalar_one()
        assert history_count == 1
        assert result.leads_recovered == 1
        assert len(result.matches) == 1
        assert result.matches[0].recovered_order_id == "FIRST"
        assert recovered_order_id == "FIRST"


@pytest.mark.asyncio
async def test_snapshot_excludes_suppressed_open_leads_and_invalid_mobile(tmp_path: Path) -> None:
    url = await _prepare_db(tmp_path)
    async with session_scope(url) as session:
        await session.execute(sa.text("""
            INSERT INTO vw_orders (cost_center, order_number, order_date, customer_name, mobile_number, order_amount) VALUES
            ('A100', 'KEEP1', '2026-05-01 10:00:00', 'Keep', '9876543210', 100.00),
            ('A100', 'KEEP2', '2026-05-20 10:00:00', 'Keep', '9876543210', 300.00),
            ('A100', 'SUP', '2026-04-01 10:00:00', 'Suppressed', '9876543211', 200.00),
            ('A100', 'OPEN', '2026-04-01 10:00:00', 'Open', '9876543212', 250.00),
            ('A100', 'BAD', '2026-04-01 10:00:00', 'Bad', '1111111111', 999.00)
        """))
        await _insert_lead(session, lead_id=1, mobile="9876543212")
        await create_time_bound_suppression(session, cost_center="A100", normalized_mobile_number="9876543211", reason=WORKBOOK_OUTCOME_NOT_INTERESTED, start_date=date(2026, 6, 1), source_lead_id=1, pipeline_run_id="run1")
        snapshot = await build_customer_retention_snapshot(session, snapshot_date=date(2026, 6, 12), cost_center="A100")
        await session.commit()
        assert [row.normalized_mobile_number for row in snapshot.rows] == ["9876543210"]
        row = snapshot.rows[0]
        assert row.lifecycle_bucket == LIFECYCLE_BUCKET_WARM
        assert row.total_orders == 2
        assert row.lifetime_spend == Decimal("400.00")
        assert row.average_order_value == Decimal("200.00")
        assert row.last_order_amount == Decimal("300.00")
        assert snapshot.rows_invalid_mobile == 1
        assert snapshot.rows_suppressed == 1
        assert snapshot.rows_existing_open_lead == 1


@pytest.mark.asyncio
async def test_lifecycle_suppression_and_recovery_changes_are_transactional(tmp_path: Path) -> None:
    url = await _prepare_db(tmp_path)
    async with session_scope(url) as session:
        await _insert_lead(session, lead_id=1)
        await session.commit()

    async with session_scope(url) as session:
        stale = await apply_lifecycle_transition(session, lead_id=1, customer_response=WORKBOOK_OUTCOME_LEAD_STALE, contact_attempted=True, pipeline_run_id="run1", event_key="rollback-stale")
        assert stale.suppression_id is not None
        await session.rollback()

    async with session_scope(url) as session:
        lead_status = (await session.execute(sa.select(trx_customer_followup_leads.c.lead_status).where(trx_customer_followup_leads.c.lead_id == 1))).scalar_one()
        suppression_count = (await session.execute(sa.select(sa.func.count()).select_from(trx_customer_suppression))).scalar_one()
        history_count = (await session.execute(sa.select(sa.func.count()).select_from(trx_customer_followup_history))).scalar_one()
        assert lead_status == LEAD_STATUS_OPEN
        assert suppression_count == 0
        assert history_count == 0
        await session.execute(sa.text("INSERT INTO vw_orders (cost_center, order_number, order_date, customer_name, mobile_number, order_amount) VALUES ('A100', 'RECOVER', '2026-06-20 10:00:00', 'A', '9876543210', 500.00)"))
        await session.commit()

    async with session_scope(url) as session:
        recovery = await detect_recoveries(session, as_of_date=date(2026, 6, 30), pipeline_run_id="run2")
        assert recovery.leads_recovered == 1
        await session.rollback()

    async with session_scope(url) as session:
        lead = (await session.execute(sa.select(trx_customer_followup_leads.c.lead_status, trx_customer_followup_leads.c.recovered_order_id).where(trx_customer_followup_leads.c.lead_id == 1))).one()
        history_count = (await session.execute(sa.select(sa.func.count()).select_from(trx_customer_followup_history))).scalar_one()
        assert lead == (LEAD_STATUS_OPEN, None)
        assert history_count == 0
