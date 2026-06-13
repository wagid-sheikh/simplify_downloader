from __future__ import annotations

from datetime import date, datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from app.customer_retention.analytics import RunTiming, UNSPECIFIED_HANDLED_BY, build_management_summary_payload
from app.customer_retention.db_tables import metadata, trx_customer_followup_history, trx_customer_followup_leads, trx_customer_suppression
from app.customer_retention.workload import WorkloadFreezeResult
from app.customer_retention.workbook_selection import StoreWorkbookSelectionResult, WorkbookLeadRow
from app.customer_retention.constants import CAP_WORK_SECTION_PENDING_CARRY_FORWARD, LEAD_SOURCE_RETENTION, LEAD_SOURCE_TD, LEAD_SOURCE_EXTERNAL, SUPPRESSION_STATE_PENDING_APPROVAL
from app.customer_retention.notifications import send_owner_summary
from app.customer_retention.pipeline import run_customer_retention_pipeline


@pytest.mark.asyncio
async def test_analytics_source_counts_revenue_and_unspecified_staff(tmp_path: Path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path/'t.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
        await conn.execute(sa.text("CREATE VIEW vw_orders AS SELECT 'S1' AS cost_center, 'O1' AS order_number, 123.45 AS order_amount"))
    Session = async_sessionmaker(engine, expire_on_commit=False)
    async with Session() as session:
        for row in [
            {"lead_id": 1, "lead_uuid": "u1", "lead_source_type": LEAD_SOURCE_RETENTION, "source_system": "x", "cost_center": "S1", "normalized_mobile_number": "919999999999", "lead_date": date(2026,6,13), "lead_status": "RECOVERED", "lifecycle_bucket": "WARM", "is_closed": True, "is_recovered": True, "recovered_order_id": "O1", "updated_by_pipeline_run_id": "r1", "created_by_pipeline_run_id": "r1", "created_at": datetime(2026,6,13,tzinfo=timezone.utc), "updated_at": datetime(2026,6,13,tzinfo=timezone.utc)},
            {"lead_id": 2, "lead_uuid": "u2", "lead_source_type": LEAD_SOURCE_TD, "source_system": "x", "source_table_name": "t", "source_record_id": "2", "cost_center": "S1", "normalized_mobile_number": "918888888888", "lead_date": date(2026,6,13), "lead_status": "PENDING", "is_closed": False, "is_recovered": False, "created_by_pipeline_run_id": "r1", "created_at": datetime(2026,6,13,tzinfo=timezone.utc), "updated_at": datetime(2026,6,13,tzinfo=timezone.utc)},
            {"lead_id": 3, "lead_uuid": "u3", "lead_source_type": LEAD_SOURCE_EXTERNAL, "source_system": "x", "source_table_name": "t", "source_record_id": "3", "cost_center": "S1", "normalized_mobile_number": "917777777777", "lead_date": date(2026,6,13), "lead_status": "WORKED", "handled_by": "Asha", "is_closed": False, "is_recovered": False, "created_by_pipeline_run_id": "r1", "created_at": datetime(2026,6,13,tzinfo=timezone.utc), "updated_at": datetime(2026,6,13,tzinfo=timezone.utc)},
        ]:
            await session.execute(trx_customer_followup_leads.insert().values(**row))
        await session.execute(trx_customer_followup_history.insert(), [{"history_id": 1, "lead_id": 2, "pipeline_run_id": "r1", "event_type": "workbook_ingested", "handled_by": None, "customer_response": "Wrong Number", "created_at": datetime(2026,6,13,tzinfo=timezone.utc)}])
        await session.commit()
        payload = await build_management_summary_payload(session, run_id="r1", run_date=date(2026,6,13), timing=RunTiming("r1", datetime(2026,6,13,tzinfo=timezone.utc)))
    retention = next(row for row in payload["source_wise_summary"] if row["source"] == LEAD_SOURCE_RETENTION)
    assert retention["recovered"] == 1
    assert retention["recovered_revenue_value"] == "123.45"
    staff = payload["staff_productivity"]
    assert staff[0]["handled_by"] == UNSPECIFIED_HANDLED_BY
    assert payload["warning_error_summary"]["unspecified_handled_by_warning_count"] == 1


@pytest.mark.asyncio
async def test_analytics_counts_pending_suppression_approvals(tmp_path: Path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path/'s.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
        await conn.execute(sa.text("CREATE VIEW vw_orders AS SELECT 'S1' AS cost_center, 'O1' AS order_number, 0 AS order_amount"))
    Session = async_sessionmaker(engine, expire_on_commit=False)
    async with Session() as session:
        await session.execute(trx_customer_followup_leads.insert().values(
            lead_id=1,
            lead_uuid="u1",
            lead_source_type=LEAD_SOURCE_RETENTION,
            source_system="x",
            cost_center="S1",
            normalized_mobile_number="919999999999",
            lead_date=date(2026, 6, 13),
            lead_status="PENDING",
            lifecycle_bucket="WARM",
            is_closed=False,
            is_recovered=False,
            created_by_pipeline_run_id="r1",
            updated_by_pipeline_run_id="r1",
            created_at=datetime(2026, 6, 13, tzinfo=timezone.utc),
            updated_at=datetime(2026, 6, 13, tzinfo=timezone.utc),
        ))
        await session.execute(trx_customer_suppression.insert().values(
            suppression_id=1,
            cost_center="S1",
            normalized_mobile_number="919999999999",
            suppression_reason="Wrong Number",
            suppression_state=SUPPRESSION_STATE_PENDING_APPROVAL,
            suppression_start_date=date(2026, 6, 13),
            is_permanent=True,
            approval_required=True,
            created_by_pipeline_run_id="r1",
            created_at=datetime(2026, 6, 13, tzinfo=timezone.utc),
        ))
        await session.commit()

        payload = await build_management_summary_payload(
            session,
            run_id="r1",
            run_date=date(2026, 6, 13),
            timing=RunTiming("r1", datetime(2026, 6, 13, tzinfo=timezone.utc)),
        )

    store = payload["store_summary"][0]
    assert store["pending_suppression_approval_count"] == 1
    assert store["suppression_additions_by_outcome"] == {"Wrong Number": 1}


def test_static_customer_retention_reporting_uses_vw_orders_only():
    for path in [Path("app/customer_retention/analytics.py"), Path("app/customer_retention/recovery_detection.py"), Path("app/customer_retention/snapshot.py")]:
        text = path.read_text()
        assert "vw_orders" in text
        assert "orders.net_amount" not in text
        assert "orders.gross_amount" not in text
        assert "orders.adjustment" not in text


@pytest.mark.asyncio
async def test_notification_payload_rendering_skip_email(tmp_path: Path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path/'n.db'}")
    async with engine.begin() as conn:
        await conn.execute(sa.text("CREATE TABLE pipelines (id INTEGER, code TEXT, description TEXT)"))
        await conn.execute(sa.text("CREATE TABLE notification_profiles (id INTEGER, pipeline_id INTEGER, code TEXT, description TEXT, env TEXT, scope TEXT, attach_mode TEXT, is_active BOOLEAN)"))
        await conn.execute(sa.text("CREATE TABLE email_templates (id INTEGER, profile_id INTEGER, name TEXT, subject_template TEXT, body_template TEXT, is_active BOOLEAN)"))
        await conn.execute(sa.text("CREATE TABLE notification_recipients (id INTEGER, profile_id INTEGER, store_code TEXT, env TEXT, email_address TEXT, display_name TEXT, send_as TEXT, is_active BOOLEAN, created_at DATETIME)"))
        await conn.execute(sa.text("INSERT INTO pipelines VALUES (1, 'customer_retention_pipeline', 'x')"))
        await conn.execute(sa.text("INSERT INTO notification_profiles VALUES (1, 1, 'owner_summary', 'x', NULL, 'run', 'none', 1)"))
        await conn.execute(sa.text("INSERT INTO email_templates VALUES (1, 1, 'summary', 'Subject {{ run_summary.pipeline_run_id }}', 'Stores {{ store_summary|length }}', 1)"))
    Session = async_sessionmaker(engine, expire_on_commit=False)
    async with Session() as session:
        result = await send_owner_summary(session, payload={"run_summary": {"pipeline_run_id": "r1"}, "store_summary": [{}]}, env=None, skip_email=True)
    assert result.skipped is True
    assert result.subject == "Subject r1"
    assert result.body == "Stores 1"


@pytest.mark.asyncio
async def test_pipeline_dry_run_orchestration(monkeypatch, tmp_path: Path):
    db = tmp_path / "p.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db}")
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
        await conn.execute(sa.text("CREATE TABLE store_master (cost_center TEXT, customer_retention_pipeline BOOLEAN)"))
        await conn.execute(sa.text("INSERT INTO store_master VALUES ('S1', 1)"))
        await conn.execute(sa.text("CREATE VIEW vw_orders AS SELECT 'S1' cost_center, 'O1' order_number, '2026-06-13' order_date, 'C' customer_name, '9999999999' mobile_number, 10 order_amount"))
    monkeypatch.setattr("app.customer_retention.pipeline.config", type("Cfg", (), {"database_url": f"sqlite+aiosqlite:///{db}"})())
    monkeypatch.setattr("app.customer_retention.pipeline.discover_returned_workbooks", lambda logger=None: [])
    monkeypatch.setattr("app.customer_retention.pipeline.discover_external_lead_files", lambda logger=None: [])
    result = await run_customer_retention_pipeline(run_date=date(2026,6,13), run_id="dry", dry_run=True, skip_email=True)
    assert result.status == "success"
    assert result.counts["active_stores"] == 1
