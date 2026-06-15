from __future__ import annotations

from datetime import date, datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import openpyxl
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from app.customer_retention.analytics import RunTiming, UNSPECIFIED_HANDLED_BY, _warning_summary, build_management_summary_payload
from app.customer_retention.db_tables import customer_followup_cap_config, metadata, trx_customer_followup_history, trx_customer_followup_leads, trx_customer_suppression, trx_external_leads
from app.customer_retention.workload import WorkloadFreezeResult
from app.customer_retention.workbook_ingestor import FOLLOWUP_SHEET
from app.customer_retention.workbook_selection import StoreWorkbookSelectionResult, WorkbookLeadRow
from app.customer_retention.constants import CAP_WORK_SECTION_PENDING_CARRY_FORWARD, LEAD_SOURCE_RETENTION, LEAD_SOURCE_TD, LEAD_SOURCE_EXTERNAL, SUPPRESSION_STATE_PENDING_APPROVAL
from app.customer_retention.notifications import send_owner_summary
from app.customer_retention.pipeline import CustomerRetentionNotificationError, run_customer_retention_pipeline

from app.customer_retention.types import RowWarning


async def _create_notification_tables(conn) -> None:
    await conn.execute(sa.text("CREATE TABLE pipelines (id INTEGER, code TEXT, description TEXT)"))
    await conn.execute(sa.text("CREATE TABLE notification_profiles (id INTEGER, pipeline_id INTEGER, code TEXT, description TEXT, env TEXT, scope TEXT, attach_mode TEXT, is_active BOOLEAN)"))
    await conn.execute(sa.text("CREATE TABLE email_templates (id INTEGER, profile_id INTEGER, name TEXT, subject_template TEXT, body_template TEXT, is_active BOOLEAN)"))
    await conn.execute(sa.text("CREATE TABLE notification_recipients (id INTEGER, profile_id INTEGER, store_code TEXT, env TEXT, email_address TEXT, display_name TEXT, send_as TEXT, is_active BOOLEAN, created_at DATETIME)"))


def _notification_payload(run_id: str = "r1") -> dict:
    return {
        "run_summary": {
            "pipeline_run_id": run_id,
            "run_date": "2026-06-13",
            "success_failure_status": "success",
            "duration_seconds": 3,
        },
        "store_summary": [{"cost_center": "S1"}],
        "aging_actionable_workload": [],
        "staff_productivity": [],
        "source_wise_summary": [],
        "warning_error_summary": {},
    }


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
        await session.execute(trx_customer_followup_history.insert(), [
            {"history_id": 1, "lead_id": 2, "pipeline_run_id": "r1", "event_type": "workbook_ingested", "handled_by": None, "customer_response": "Wrong Number", "created_at": datetime(2026,6,13,tzinfo=timezone.utc)},
            {"history_id": 2, "lead_id": 3, "pipeline_run_id": "r1", "event_type": "workbook_ingested", "handled_by": "Asha", "customer_response": "No Response", "created_at": datetime(2026,6,13,tzinfo=timezone.utc)},
        ])
        await session.commit()
        payload = await build_management_summary_payload(session, run_id="r1", run_date=date(2026,6,13), timing=RunTiming("r1", datetime(2026,6,13,tzinfo=timezone.utc)))
    retention = next(row for row in payload["source_wise_summary"] if row["source"] == LEAD_SOURCE_RETENTION)
    assert retention["recovered"] == 1
    assert retention["recovered_revenue_value"] == "123.45"
    staff = payload["staff_productivity"]
    staff_by_handler = {row["handled_by"]: row for row in staff}
    assert UNSPECIFIED_HANDLED_BY in staff_by_handler
    assert staff_by_handler[UNSPECIFIED_HANDLED_BY]["operational_warning"] is True
    assert staff_by_handler["Asha"]["operational_warning"] is False
    assert payload["warning_error_summary"]["unspecified_handled_by_warning_count"] == 1


def test_warning_summary_counts_all_target_cost_center_warning_codes():
    warnings = [
        RowWarning("target_cost_center_blank", "missing target"),
        RowWarning("target_cost_center_invalid", "inactive target"),
        RowWarning("target_cost_center_same_store", "same source and target"),
        RowWarning("target_cost_center_ignored", "target ignored for non-shift response"),
    ]

    summary = _warning_summary(warnings, ingestion_results=[], aging={}, staff=[])

    assert summary["target_cost_center_warnings"] == 4
    assert summary["warnings_by_code"] == {
        "target_cost_center_blank": 1,
        "target_cost_center_invalid": 1,
        "target_cost_center_same_store": 1,
        "target_cost_center_ignored": 1,
    }


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
    assert payload["warning_error_summary"]["pending_suppression_approval_count"] == 1


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
        await _create_notification_tables(conn)
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
async def test_notification_missing_tables_uses_fallback_template_skip_email(tmp_path: Path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path/'missing_tables.db'}")
    Session = async_sessionmaker(engine, expire_on_commit=False)
    async with Session() as session:
        result = await send_owner_summary(session, payload=_notification_payload("no-tables"), env=None, skip_email=True)

    assert result.skipped is True
    assert result.reason == "skip_email"
    assert result.subject == "Customer Retention Summary 2026-06-13 (success)"
    assert "Customer Retention Pipeline Run no-tables" in result.body


@pytest.mark.asyncio
async def test_notification_missing_profile_uses_fallback_template_skip_email(tmp_path: Path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path/'missing_profile.db'}")
    async with engine.begin() as conn:
        await _create_notification_tables(conn)
        await conn.execute(sa.text("INSERT INTO pipelines VALUES (1, 'customer_retention_pipeline', 'x')"))
    Session = async_sessionmaker(engine, expire_on_commit=False)
    async with Session() as session:
        result = await send_owner_summary(session, payload=_notification_payload("no-profile"), env=None, skip_email=True)

    assert result.skipped is True
    assert result.subject == "Customer Retention Summary 2026-06-13 (success)"
    assert "Customer Retention Pipeline Run no-profile" in result.body


@pytest.mark.asyncio
async def test_notification_missing_recipients_is_successful_skip_with_warning_status(tmp_path: Path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path/'missing_recipients.db'}")
    async with engine.begin() as conn:
        await _create_notification_tables(conn)
        await conn.execute(sa.text("INSERT INTO pipelines VALUES (1, 'customer_retention_pipeline', 'x')"))
        await conn.execute(sa.text("INSERT INTO notification_profiles VALUES (1, 1, 'owner_summary', 'x', NULL, 'run', 'none', 1)"))
        await conn.execute(sa.text("INSERT INTO email_templates VALUES (1, 1, 'summary', 'Subject {{ run_summary.pipeline_run_id }}', 'Body {{ store_summary|length }}', 1)"))
    Session = async_sessionmaker(engine, expire_on_commit=False)
    async with Session() as session:
        result = await send_owner_summary(session, payload=_notification_payload("no-recipients"), env=None, skip_email=False)

    assert result.planned == 0
    assert result.sent == 0
    assert result.skipped is True
    assert result.reason == "no_recipients"
    assert result.subject == "Subject no-recipients"
    assert result.body == "Body 1"


@pytest.mark.asyncio
async def test_notification_valid_db_template_renders_and_sends_to_active_recipients(monkeypatch, tmp_path: Path):
    captured = {}

    def fake_send_email(config, plan):
        captured["plan"] = plan
        return SimpleNamespace(sent=True)

    monkeypatch.setattr("app.customer_retention.notifications._send_email", fake_send_email)
    monkeypatch.setattr("app.customer_retention.notifications._load_smtp_config", lambda: SimpleNamespace())

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path/'valid_notification.db'}")
    async with engine.begin() as conn:
        await _create_notification_tables(conn)
        await conn.execute(sa.text("INSERT INTO pipelines VALUES (1, 'customer_retention_pipeline', 'x')"))
        await conn.execute(sa.text("INSERT INTO notification_profiles VALUES (1, 1, 'owner_summary', 'x', NULL, 'run', 'none', 1)"))
        await conn.execute(sa.text("INSERT INTO email_templates VALUES (1, 1, 'summary', 'Subject {{ run_summary.pipeline_run_id }}', 'Stores {{ store_summary|length }}', 1)"))
        await conn.execute(sa.text("INSERT INTO notification_recipients VALUES (1, 1, NULL, NULL, 'owner@example.com', 'Owner', 'to', 1, '2026-06-13 00:00:00')"))
    Session = async_sessionmaker(engine, expire_on_commit=False)
    async with Session() as session:
        result = await send_owner_summary(session, payload=_notification_payload("db-backed"), env=None, skip_email=False)

    assert result.planned == 1
    assert result.sent == 1
    assert result.skipped is False
    assert result.subject == "Subject db-backed"
    assert result.body == "Stores 1"
    assert captured["plan"].to == ["Owner <owner@example.com>"]


@pytest.mark.asyncio
async def test_pipeline_dry_run_orchestration(monkeypatch, tmp_path: Path):
    db = tmp_path / "p.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db}")
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
        await conn.execute(sa.text("CREATE TABLE store_master (cost_center TEXT, customer_retention_pipeline BOOLEAN)"))
        await conn.execute(sa.text("INSERT INTO store_master VALUES ('S1', 1)"))
        await conn.execute(sa.text("CREATE VIEW vw_orders AS SELECT 'S1' cost_center, 'O1' order_number, '2026-06-13' order_date, 'C' customer_name, '9999999999' mobile_number, 10 order_amount"))
    monkeypatch.setattr(
        "app.customer_retention.pipeline.config",
        type(
            "Cfg",
            (),
            {
                "database_url": f"sqlite+aiosqlite:///{db}",
                "customer_followup_output_dir": str(tmp_path / "outputs"),
            },
        )(),
    )
    monkeypatch.setattr("app.customer_retention.pipeline.discover_returned_workbooks", lambda logger=None: [])
    monkeypatch.setattr("app.customer_retention.pipeline.discover_external_lead_files", lambda logger=None: [])
    result = await run_customer_retention_pipeline(run_date=date(2026,6,13), run_id="dry", dry_run=True, skip_email=True)
    assert result.status == "success"
    assert result.counts["active_stores"] == 1


@pytest.mark.asyncio
async def test_pipeline_generates_fresh_retention_leads_from_vw_orders(monkeypatch, tmp_path: Path):
    db = tmp_path / "fresh_retention.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db}")
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
        await conn.execute(sa.text("CREATE TABLE store_master (cost_center TEXT PRIMARY KEY, customer_retention_pipeline BOOLEAN NOT NULL DEFAULT 1)"))
        await conn.execute(sa.text("INSERT INTO store_master VALUES ('S1', 1)"))
        await conn.execute(sa.text(
            """
            CREATE VIEW vw_orders AS
            SELECT 'S1' AS cost_center, 'O1' AS order_number, '2026-05-10' AS order_date,
                   'Fresh Customer' AS customer_name, '9876543210' AS mobile_number, 1200.50 AS order_amount
            """
        ))
    monkeypatch.setattr(
        "app.customer_retention.pipeline.config",
        type(
            "Cfg",
            (),
            {
                "database_url": f"sqlite+aiosqlite:///{db}",
                "customer_followup_output_dir": str(tmp_path / "outputs"),
            },
        )(),
    )
    monkeypatch.setattr("app.customer_retention.pipeline.discover_returned_workbooks", lambda logger=None: [])
    monkeypatch.setattr("app.customer_retention.pipeline.discover_external_lead_files", lambda logger=None: [])

    async def no_td_leads(session, pipeline_run_id, logger=None):
        return type("TDResult", (), {"rows_seen": 0, "leads_created": 0, "warnings": ()})()

    monkeypatch.setattr("app.customer_retention.pipeline._import_td_leads", no_td_leads)

    Session = async_sessionmaker(engine, expire_on_commit=False)
    async with Session() as session:
        await session.execute(customer_followup_cap_config.insert().values(
            cap_config_id=1,
            cost_center=None,
            lead_source_type=LEAD_SOURCE_RETENTION,
            work_section="FRESH_RETENTION",
            daily_cap=13,
            is_uncapped=False,
            enabled=True,
            effective_from=date(2026, 1, 1),
            created_at=datetime(2026, 6, 13, tzinfo=timezone.utc),
            updated_at=datetime(2026, 6, 13, tzinfo=timezone.utc),
        ))
        await session.commit()

    result = await run_customer_retention_pipeline(run_date=date(2026, 6, 13), run_id="retention-run", dry_run=False, skip_email=True)

    assert result.status == "success"
    assert result.counts["snapshot_rows"] == 1
    assert result.counts["retention_leads_created"] == 1
    assert result.counts["workbook_rows_selected"] == 1
    assert len(result.generated_files) == 1

    async with Session() as session:
        lead = (await session.execute(sa.select(trx_customer_followup_leads).where(trx_customer_followup_leads.c.lead_source_type == LEAD_SOURCE_RETENTION))).mappings().one()
    assert lead["source_system"] == "CUSTOMER_RETENTION_PIPELINE"
    assert lead["source_table_name"] is None
    assert lead["source_record_id"] is None
    assert lead["cost_center"] == "S1"
    assert lead["normalized_mobile_number"] == "9876543210"
    assert lead["lifecycle_bucket"] == "WARM"
    assert lead["created_by_pipeline_run_id"] == "retention-run"

    workbook = openpyxl.load_workbook(result.generated_files[0])
    sheet = workbook[FOLLOWUP_SHEET]
    headers = [cell.value for cell in sheet[1]]
    row = dict(zip(headers, [cell.value for cell in sheet[2]]))
    assert row["lead_source_type"] == LEAD_SOURCE_RETENTION
    assert row["work_section"] == "FRESH_RETENTION"
    assert row["normalized_mobile_number"] == "9876543210"

    await engine.dispose()

@pytest.mark.asyncio
async def test_pipeline_srs_section9_orchestration_order(monkeypatch, tmp_path: Path):
    from types import SimpleNamespace

    calls: list[str] = []
    db = tmp_path / "order.db"
    config_obj = type(
        "Cfg",
        (),
        {
            "database_url": f"sqlite+aiosqlite:///{db}",
            "customer_followup_output_dir": str(tmp_path / "outputs"),
            "customer_followup_backlog_warning_threshold": 7,
        },
    )()
    monkeypatch.setattr("app.customer_retention.pipeline.config", config_obj)
    monkeypatch.setattr("app.customer_retention.pipeline.get_customer_followup_paths", lambda: SimpleNamespace(archive_dir=tmp_path / "archive"))

    async def load_stores(session, **kwargs):
        calls.append("fetch_active_stores")
        return ["S1"]

    def discover_workbooks(logger=None):
        calls.append("discover_returned_workbooks")
        return [SimpleNamespace(path=tmp_path / "returned.xlsx")]

    async def ingest_workbook(*args, **kwargs):
        calls.append("ingest_returned_workbook")
        return SimpleNamespace(rows_seen=2, history_inserted=1, warnings=[])

    async def detect(session, **kwargs):
        calls.append("detect_recoveries")
        return SimpleNamespace(leads_recovered=1, leads_closed=1)

    async def snapshot(session, **kwargs):
        calls.append("build_snapshot")
        return SimpleNamespace(rows=[object()], rows_invalid_mobile=0)

    def discover_external(logger=None):
        calls.append("discover_external_lead_files")
        return [SimpleNamespace(path=tmp_path / "external.csv")]

    async def import_external(*args, **kwargs):
        calls.append("import_external_lead_file")
        return SimpleNamespace(rows_seen=3, leads_created=2, warnings=[])

    async def import_td(*args, **kwargs):
        calls.append("import_td_leads")
        return SimpleNamespace(rows_seen=4, leads_created=3, warnings=[])

    async def generate_retention(session, **kwargs):
        calls.append("generate_retention_leads")
        return SimpleNamespace(rows_seen=5, leads_created=4, leads_reused=1, rows_skipped=0, warnings=[])

    async def select(session, **kwargs):
        calls.append("select_workbook_leads")
        return [SimpleNamespace(rows=[object(), object()])]

    def generate(**kwargs):
        calls.append("generate_workbooks")
        return SimpleNamespace(outputs=[SimpleNamespace(output_path=tmp_path / "outputs" / "S1.xlsx")])

    def archive(path, **kwargs):
        calls.append(f"archive:{Path(path).name}")

    async def summary(session, **kwargs):
        calls.append("build_summary")
        return {"run_summary": {"pipeline_run_id": "order"}}

    async def send(session, **kwargs):
        calls.append("send_email")
        return SimpleNamespace(planned=1, sent=1, skipped=False, reason=None)

    monkeypatch.setattr("app.customer_retention.pipeline.load_active_retention_stores", load_stores)
    monkeypatch.setattr("app.customer_retention.pipeline.discover_returned_workbooks", discover_workbooks)
    monkeypatch.setattr("app.customer_retention.pipeline._ingest_returned_workbook", ingest_workbook)
    monkeypatch.setattr("app.customer_retention.pipeline.detect_recoveries", detect)
    monkeypatch.setattr("app.customer_retention.pipeline.build_customer_retention_snapshot", snapshot)
    monkeypatch.setattr("app.customer_retention.pipeline.discover_external_lead_files", discover_external)
    monkeypatch.setattr("app.customer_retention.pipeline._import_external_lead_file", import_external)
    monkeypatch.setattr("app.customer_retention.pipeline._import_td_leads", import_td)
    monkeypatch.setattr("app.customer_retention.pipeline.allocate_and_generate_retention_leads", generate_retention)
    monkeypatch.setattr("app.customer_retention.pipeline.select_workbook_leads_for_active_stores", select)
    monkeypatch.setattr("app.customer_retention.pipeline.generate_workbooks", generate)
    monkeypatch.setattr("app.customer_retention.pipeline.archive_processed_file", archive)
    monkeypatch.setattr("app.customer_retention.pipeline.build_management_summary_payload", summary)
    monkeypatch.setattr("app.customer_retention.pipeline.send_owner_summary", send)

    result = await run_customer_retention_pipeline(run_date=date(2026, 6, 13), run_id="order", dry_run=False, skip_email=False)

    assert result.status == "success"
    assert calls == [
        "fetch_active_stores",
        "discover_returned_workbooks",
        "ingest_returned_workbook",
        "detect_recoveries",
        "build_snapshot",
        "discover_external_lead_files",
        "import_external_lead_file",
        "import_td_leads",
        "generate_retention_leads",
        "select_workbook_leads",
        "generate_workbooks",
        "archive:external.csv",
        "archive:returned.xlsx",
        "build_summary",
        "send_email",
    ]


@pytest.mark.asyncio
async def test_pipeline_dry_run_skips_mutating_import_archive_generate_and_email(monkeypatch, tmp_path: Path):
    from types import SimpleNamespace

    calls: list[str] = []
    forbidden_calls: list[str] = []
    seen_backlog_thresholds: list[int] = []
    db = tmp_path / "dry_order.db"
    monkeypatch.setattr(
        "app.customer_retention.pipeline.config",
        type(
            "Cfg",
            (),
            {
                "database_url": f"sqlite+aiosqlite:///{db}",
                "customer_followup_output_dir": str(tmp_path / "outputs"),
                "customer_followup_backlog_warning_threshold": 7,
            },
        )(),
    )
    monkeypatch.setattr("app.customer_retention.pipeline.get_customer_followup_paths", lambda: SimpleNamespace(archive_dir=tmp_path / "archive"))
    monkeypatch.setattr("app.customer_retention.pipeline.discover_returned_workbooks", lambda logger=None: calls.append("discover_returned_workbooks") or [SimpleNamespace(path=tmp_path / "returned.xlsx")])
    monkeypatch.setattr("app.customer_retention.pipeline.discover_external_lead_files", lambda logger=None: calls.append("discover_external_lead_files") or [SimpleNamespace(path=tmp_path / "external.csv")])

    async def load_stores(session, **kwargs):
        calls.append("fetch_active_stores")
        return ["S1"]

    async def snapshot(session, **kwargs):
        calls.append("build_snapshot")
        return SimpleNamespace(rows=[], rows_invalid_mobile=0)

    async def select(session, **kwargs):
        calls.append("select_workbook_leads")
        seen_backlog_thresholds.append(kwargs["backlog_threshold"])
        return [SimpleNamespace(rows=[object(), object()], warnings=[SimpleNamespace(code="dry_warning")])]

    async def summary(session, **kwargs):
        calls.append("build_summary")
        return {"run_summary": {"pipeline_run_id": "dry-order"}}

    async def forbidden_async(*args, **kwargs):
        forbidden_calls.append(kwargs.get("path") or kwargs.get("payload") or "async")
        raise AssertionError("dry-run called a mutating/import/send function")

    def forbidden_sync(*args, **kwargs):
        forbidden_calls.append(kwargs.get("path") or "sync")
        raise AssertionError("dry-run called an archive/generate function")

    monkeypatch.setattr("app.customer_retention.pipeline.load_active_retention_stores", load_stores)
    monkeypatch.setattr("app.customer_retention.pipeline.build_customer_retention_snapshot", snapshot)
    monkeypatch.setattr("app.customer_retention.pipeline.select_workbook_leads_for_active_stores", select)
    monkeypatch.setattr("app.customer_retention.pipeline.build_management_summary_payload", summary)
    monkeypatch.setattr("app.customer_retention.pipeline._ingest_returned_workbook", forbidden_async)
    monkeypatch.setattr("app.customer_retention.pipeline.detect_recoveries", forbidden_async)
    monkeypatch.setattr("app.customer_retention.pipeline._import_external_lead_file", forbidden_async)
    monkeypatch.setattr("app.customer_retention.pipeline._import_td_leads", forbidden_async)
    monkeypatch.setattr("app.customer_retention.pipeline.allocate_and_generate_retention_leads", forbidden_async)
    monkeypatch.setattr("app.customer_retention.pipeline.send_owner_summary", forbidden_async)
    monkeypatch.setattr("app.customer_retention.pipeline.generate_workbooks", forbidden_sync)
    monkeypatch.setattr("app.customer_retention.pipeline.archive_processed_file", forbidden_sync)

    result = await run_customer_retention_pipeline(run_date=date(2026, 6, 13), run_id="dry-order", dry_run=True, skip_email=False)

    assert result.status == "success_with_warnings"
    assert result.generated_files == []
    assert forbidden_calls == []
    assert seen_backlog_thresholds == [7]
    assert result.counts["planned_returned_workbooks_to_ingest"] == 1
    assert result.counts["planned_external_files_to_import"] == 1
    assert result.counts["planned_workbook_rows_selected"] == 2
    assert result.counts["planned_workbooks_to_generate"] == 1
    assert result.counts["planned_files_to_archive"] == 2
    assert result.counts["planned_summary_emails"] == 1
    assert result.counts["dry_run_backlog_threshold"] == 7
    assert "workbook_history_inserted" not in result.counts
    assert "external_leads_created" not in result.counts
    assert "td_leads_created" not in result.counts
    assert "retention_leads_created" not in result.counts
    assert "workbook_rows_selected" not in result.counts
    assert result.warnings == ["dry_warning"]
    assert result.email_status["skipped"] is True
    assert result.email_status["reason"] == "not_attempted"
    assert calls == [
        "fetch_active_stores",
        "discover_returned_workbooks",
        "build_snapshot",
        "discover_external_lead_files",
        "select_workbook_leads",
        "build_summary",
    ]


@pytest.mark.asyncio
async def test_pipeline_rolls_back_workbook_external_td_and_retention_mutations_on_later_failure(monkeypatch, tmp_path: Path):
    db = tmp_path / "rollback_all.db"
    db_url = f"sqlite+aiosqlite:///{db}"
    engine = create_async_engine(db_url)
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
        await conn.execute(sa.text("CREATE TABLE store_master (cost_center TEXT PRIMARY KEY, customer_retention_pipeline BOOLEAN NOT NULL DEFAULT 1)"))
        await conn.execute(sa.text("INSERT INTO store_master VALUES ('S1', 1)"))

    monkeypatch.setattr(
        "app.customer_retention.pipeline.config",
        type(
            "Cfg",
            (),
            {
                "database_url": db_url,
                "customer_followup_output_dir": str(tmp_path / "outputs"),
                "customer_followup_backlog_warning_threshold": 7,
            },
        )(),
    )
    monkeypatch.setattr("app.customer_retention.pipeline.get_customer_followup_paths", lambda: SimpleNamespace(archive_dir=tmp_path / "archive"))
    monkeypatch.setattr("app.customer_retention.pipeline.discover_returned_workbooks", lambda logger=None: [SimpleNamespace(path=tmp_path / "returned.xlsx")])
    monkeypatch.setattr("app.customer_retention.pipeline.discover_external_lead_files", lambda logger=None: [SimpleNamespace(path=tmp_path / "external.csv")])

    async def load_stores(session, **kwargs):
        return ["S1"]

    async def ingest_workbook(session, path, pipeline_run_id, **kwargs):
        await session.execute(
            trx_customer_followup_leads.insert().values(
                lead_id=101,
                lead_uuid="00000000-0000-0000-0000-000000000101",
                lead_source_type="RETENTION",
                source_system="TEST_WORKBOOK",
                cost_center="S1",
                normalized_mobile_number="9000000101",
                lead_date=date(2026, 6, 13),
                lead_status="OPEN",
                lifecycle_bucket="WARM",
                created_by_pipeline_run_id=pipeline_run_id,
                created_at=datetime(2026, 6, 13, tzinfo=timezone.utc),
                updated_at=datetime(2026, 6, 13, tzinfo=timezone.utc),
            )
        )
        await session.execute(
            trx_customer_followup_history.insert().values(
                history_id=201,
                lead_id=101,
                pipeline_run_id=pipeline_run_id,
                event_type="Workbook_Test",
                created_at=datetime(2026, 6, 13, tzinfo=timezone.utc),
            )
        )
        return SimpleNamespace(rows_seen=1, history_inserted=1, warnings=[])

    async def detect(session, **kwargs):
        return SimpleNamespace(leads_recovered=0, leads_closed=0)

    async def snapshot(session, **kwargs):
        return SimpleNamespace(rows=[], rows_invalid_mobile=0)

    async def import_external(session, path, pipeline_run_id, **kwargs):
        await session.execute(
            trx_external_leads.insert().values(
                external_lead_id=301,
                external_lead_uuid="00000000-0000-0000-0000-000000000301",
                lead_source="campaign",
                cost_center="S1",
                normalized_mobile_number="9000000301",
                lead_date=date(2026, 6, 13),
                lead_status="OPEN",
                created_at=datetime(2026, 6, 13, tzinfo=timezone.utc),
                updated_at=datetime(2026, 6, 13, tzinfo=timezone.utc),
            )
        )
        return SimpleNamespace(rows_seen=1, leads_created=0, warnings=[])

    async def import_td(session, pipeline_run_id, **kwargs):
        await session.execute(
            trx_customer_followup_leads.insert().values(
                lead_id=401,
                lead_uuid="00000000-0000-0000-0000-000000000401",
                lead_source_type="TD",
                source_system="TEST_TD",
                source_table_name="crm_leads_current",
                source_record_id="td-401",
                cost_center="S1",
                normalized_mobile_number="9000000401",
                lead_date=date(2026, 6, 13),
                lead_status="OPEN",
                created_at=datetime(2026, 6, 13, tzinfo=timezone.utc),
                updated_at=datetime(2026, 6, 13, tzinfo=timezone.utc),
            )
        )
        return SimpleNamespace(rows_seen=1, leads_created=1, warnings=[])

    async def generate_retention(session, **kwargs):
        await session.execute(
            trx_customer_followup_leads.insert().values(
                lead_id=501,
                lead_uuid="00000000-0000-0000-0000-000000000501",
                lead_source_type="RETENTION",
                source_system="CUSTOMER_RETENTION_PIPELINE",
                cost_center="S1",
                normalized_mobile_number="9000000501",
                lead_date=date(2026, 6, 13),
                lead_status="OPEN",
                lifecycle_bucket="WARM",
                created_by_pipeline_run_id="rollback-run",
                created_at=datetime(2026, 6, 13, tzinfo=timezone.utc),
                updated_at=datetime(2026, 6, 13, tzinfo=timezone.utc),
            )
        )
        return SimpleNamespace(rows_seen=1, leads_created=1, leads_reused=0, rows_skipped=0, warnings=[])

    async def select_raises(session, **kwargs):
        raise RuntimeError("later workbook selection failure")

    monkeypatch.setattr("app.customer_retention.pipeline.load_active_retention_stores", load_stores)
    monkeypatch.setattr("app.customer_retention.pipeline._ingest_returned_workbook", ingest_workbook)
    monkeypatch.setattr("app.customer_retention.pipeline.detect_recoveries", detect)
    monkeypatch.setattr("app.customer_retention.pipeline.build_customer_retention_snapshot", snapshot)
    monkeypatch.setattr("app.customer_retention.pipeline._import_external_lead_file", import_external)
    monkeypatch.setattr("app.customer_retention.pipeline._import_td_leads", import_td)
    monkeypatch.setattr("app.customer_retention.pipeline.allocate_and_generate_retention_leads", generate_retention)
    monkeypatch.setattr("app.customer_retention.pipeline.select_workbook_leads_for_active_stores", select_raises)

    with pytest.raises(RuntimeError, match="later workbook selection failure"):
        await run_customer_retention_pipeline(run_date=date(2026, 6, 13), run_id="rollback-run", dry_run=False, skip_email=True)

    Session = async_sessionmaker(engine, expire_on_commit=False)
    async with Session() as session:
        lead_count = await session.scalar(sa.select(sa.func.count()).select_from(trx_customer_followup_leads))
        history_count = await session.scalar(sa.select(sa.func.count()).select_from(trx_customer_followup_history))
        external_count = await session.scalar(sa.select(sa.func.count()).select_from(trx_external_leads))
    assert lead_count == 0
    assert history_count == 0
    assert external_count == 0
    await engine.dispose()

@pytest.mark.asyncio
async def test_pipeline_summary_payload_includes_external_td_and_retention_generation_warning_codes(monkeypatch, tmp_path: Path):
    db = tmp_path / "warning_propagation.db"
    monkeypatch.setattr(
        "app.customer_retention.pipeline.config",
        type(
            "Cfg",
            (),
            {
                "database_url": f"sqlite+aiosqlite:///{db}",
                "customer_followup_output_dir": str(tmp_path / "outputs"),
                "customer_followup_backlog_warning_threshold": 7,
            },
        )(),
    )
    monkeypatch.setattr("app.customer_retention.pipeline.get_customer_followup_paths", lambda: SimpleNamespace(archive_dir=tmp_path / "archive"))
    monkeypatch.setattr("app.customer_retention.pipeline.discover_returned_workbooks", lambda logger=None: [])
    monkeypatch.setattr("app.customer_retention.pipeline.discover_external_lead_files", lambda logger=None: [SimpleNamespace(path=tmp_path / "external.csv")])

    async def load_stores(session, **kwargs):
        return ["S1"]

    async def detect(session, **kwargs):
        return SimpleNamespace(leads_recovered=0, leads_closed=0)

    async def snapshot(session, **kwargs):
        return SimpleNamespace(rows=[], rows_invalid_mobile=0)

    async def import_external(session, path, pipeline_run_id, **kwargs):
        return SimpleNamespace(
            rows_seen=1,
            leads_created=0,
            warnings=[RowWarning("invalid_cost_center", "External lead row has missing or inactive cost center", row_number=2, source_file="external.csv", field_name="cost_center")],
        )

    async def import_td(session, pipeline_run_id, **kwargs):
        return SimpleNamespace(rows_seen=1, leads_created=0, warnings=[RowWarning("td_store_unmapped", "TD lead store_code is not mapped", cost_center="BAD")])

    async def generate_retention(session, **kwargs):
        return SimpleNamespace(rows_seen=1, leads_created=0, leads_reused=0, rows_skipped=1, warnings=["invalid_mobile_identity"])

    async def select(session, **kwargs):
        return []

    def generate(**kwargs):
        return SimpleNamespace(outputs=[])

    async def summary(session, **kwargs):
        warning_summary = _warning_summary(list(kwargs["row_warnings"]), ingestion_results=[], aging={}, staff=[])
        return {"warning_error_summary": warning_summary}

    async def send(session, **kwargs):
        return SimpleNamespace(planned=0, sent=0, skipped=True, reason="skip_email")

    monkeypatch.setattr("app.customer_retention.pipeline.load_active_retention_stores", load_stores)
    monkeypatch.setattr("app.customer_retention.pipeline.detect_recoveries", detect)
    monkeypatch.setattr("app.customer_retention.pipeline.build_customer_retention_snapshot", snapshot)
    monkeypatch.setattr("app.customer_retention.pipeline._import_external_lead_file", import_external)
    monkeypatch.setattr("app.customer_retention.pipeline._import_td_leads", import_td)
    monkeypatch.setattr("app.customer_retention.pipeline.allocate_and_generate_retention_leads", generate_retention)
    monkeypatch.setattr("app.customer_retention.pipeline.select_workbook_leads_for_active_stores", select)
    monkeypatch.setattr("app.customer_retention.pipeline.generate_workbooks", generate)
    monkeypatch.setattr("app.customer_retention.pipeline.archive_processed_file", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.customer_retention.pipeline.build_management_summary_payload", summary)
    monkeypatch.setattr("app.customer_retention.pipeline.send_owner_summary", send)

    result = await run_customer_retention_pipeline(run_date=date(2026, 6, 13), run_id="warning-run", dry_run=False, skip_email=True)

    assert result.status == "success_with_warnings"
    assert result.warnings == ["invalid_cost_center", "td_store_unmapped", "invalid_mobile_identity"]
    assert result.summary_payload["warning_error_summary"]["warnings_by_code"] == {
        "invalid_cost_center": 1,
        "td_store_unmapped": 1,
        "invalid_mobile_identity": 1,
    }
    assert result.summary_payload["warning_error_summary"]["invalid_mobiles"] == 1

@pytest.mark.asyncio
async def test_pipeline_commit_failure_prevents_success_email_dispatch(monkeypatch, tmp_path: Path):
    from contextlib import asynccontextmanager
    from app.customer_retention import pipeline

    calls: list[str] = []

    class CommitFailureSession:
        async def flush(self):
            return None

        async def commit(self):
            calls.append("commit")
            raise RuntimeError("database commit failed")

        async def rollback(self):
            calls.append("rollback")

    @asynccontextmanager
    async def fake_session_scope(_database_url):
        session = CommitFailureSession()
        try:
            yield session
        except Exception:
            await session.rollback()
            raise

    async def load_stores(*args, **kwargs):
        return ["S1"]

    async def detect(*args, **kwargs):
        return SimpleNamespace(leads_recovered=0, leads_closed=0)

    async def snapshot(*args, **kwargs):
        return SimpleNamespace(rows=[], rows_invalid_mobile=0)

    async def import_td(*args, **kwargs):
        return SimpleNamespace(rows_seen=0, leads_created=0, warnings=[])

    async def generate_retention(*args, **kwargs):
        return SimpleNamespace(rows_seen=0, leads_created=0, leads_reused=0, rows_skipped=0, warnings=[])

    async def select(*args, **kwargs):
        return []

    async def summary(*args, **kwargs):
        calls.append("build_summary")
        return {"run_summary": {"pipeline_run_id": "commit-fails"}}

    async def forbidden_send(*args, **kwargs):
        calls.append("send_email")
        raise AssertionError("email must not be sent when commit fails")

    monkeypatch.setattr(pipeline, "config", SimpleNamespace(database_url="sqlite+aiosqlite:///unused.db", customer_followup_output_dir=str(tmp_path / "outputs"), customer_followup_backlog_warning_threshold=7))
    monkeypatch.setattr(pipeline, "session_scope", fake_session_scope)
    monkeypatch.setattr(pipeline, "get_customer_followup_paths", lambda: SimpleNamespace(archive_dir=tmp_path / "archive"))
    monkeypatch.setattr(pipeline, "discover_returned_workbooks", lambda logger=None: [])
    monkeypatch.setattr(pipeline, "discover_external_lead_files", lambda logger=None: [])
    monkeypatch.setattr(pipeline, "load_active_retention_stores", load_stores)
    monkeypatch.setattr(pipeline, "detect_recoveries", detect)
    monkeypatch.setattr(pipeline, "build_customer_retention_snapshot", snapshot)
    monkeypatch.setattr(pipeline, "_import_td_leads", import_td)
    monkeypatch.setattr(pipeline, "allocate_and_generate_retention_leads", generate_retention)
    monkeypatch.setattr(pipeline, "select_workbook_leads_for_active_stores", select)
    monkeypatch.setattr(pipeline, "generate_workbooks", lambda **kwargs: SimpleNamespace(outputs=[]))
    monkeypatch.setattr(pipeline, "build_management_summary_payload", summary)
    monkeypatch.setattr(pipeline, "send_owner_summary", forbidden_send)

    with pytest.raises(RuntimeError, match="database commit failed"):
        await pipeline.run_customer_retention_pipeline(run_date=date(2026, 6, 13), run_id="commit-fails", dry_run=False, skip_email=False)

    assert calls == ["build_summary", "commit", "rollback"]


@pytest.mark.asyncio
async def test_pipeline_committed_run_reports_success_when_owner_email_sent(monkeypatch, tmp_path: Path):
    db = tmp_path / "email_success_after_commit.db"
    db_url = f"sqlite+aiosqlite:///{db}"
    engine = create_async_engine(db_url)
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
        await conn.execute(sa.text("CREATE TABLE store_master (cost_center TEXT PRIMARY KEY, customer_retention_pipeline BOOLEAN NOT NULL DEFAULT 1)"))
        await conn.execute(sa.text("INSERT INTO store_master VALUES ('S1', 1)"))

    sent = []
    monkeypatch.setattr("app.customer_retention.pipeline.config", SimpleNamespace(database_url=db_url, customer_followup_output_dir=str(tmp_path / "outputs"), customer_followup_backlog_warning_threshold=7))
    monkeypatch.setattr("app.customer_retention.pipeline.get_customer_followup_paths", lambda: SimpleNamespace(archive_dir=tmp_path / "archive"))
    monkeypatch.setattr("app.customer_retention.pipeline.discover_returned_workbooks", lambda logger=None: [])
    monkeypatch.setattr("app.customer_retention.pipeline.discover_external_lead_files", lambda logger=None: [])
    async def load_stores(*args, **kwargs):
        return ["S1"]

    monkeypatch.setattr("app.customer_retention.pipeline.load_active_retention_stores", load_stores)

    async def detect(*args, **kwargs):
        return SimpleNamespace(leads_recovered=0, leads_closed=0)

    async def snapshot(*args, **kwargs):
        return SimpleNamespace(rows=[], rows_invalid_mobile=0)

    async def import_td(*args, **kwargs):
        return SimpleNamespace(rows_seen=0, leads_created=0, warnings=[])

    async def generate_retention(*args, **kwargs):
        return SimpleNamespace(rows_seen=0, leads_created=0, leads_reused=0, rows_skipped=0, warnings=[])

    async def select(*args, **kwargs):
        return []

    async def summary(*args, **kwargs):
        return {"run_summary": {"pipeline_run_id": "email-sent"}}

    async def send(session, **kwargs):
        sent.append(kwargs["payload"]["run_summary"]["pipeline_run_id"])
        return SimpleNamespace(planned=1, sent=1, skipped=False, reason=None, subject="ok", body="ok")

    monkeypatch.setattr("app.customer_retention.pipeline.detect_recoveries", detect)
    monkeypatch.setattr("app.customer_retention.pipeline.build_customer_retention_snapshot", snapshot)
    monkeypatch.setattr("app.customer_retention.pipeline._import_td_leads", import_td)
    monkeypatch.setattr("app.customer_retention.pipeline.allocate_and_generate_retention_leads", generate_retention)
    monkeypatch.setattr("app.customer_retention.pipeline.select_workbook_leads_for_active_stores", select)
    monkeypatch.setattr("app.customer_retention.pipeline.generate_workbooks", lambda **kwargs: SimpleNamespace(outputs=[]))
    monkeypatch.setattr("app.customer_retention.pipeline.build_management_summary_payload", summary)
    monkeypatch.setattr("app.customer_retention.pipeline.send_owner_summary", send)

    result = await run_customer_retention_pipeline(run_date=date(2026, 6, 13), run_id="email-sent", dry_run=False, skip_email=False)

    assert result.status == "success"
    assert result.email_status["sent"] == 1
    assert result.email_status["committed"] is True
    assert sent == ["email-sent"]
    await engine.dispose()


@pytest.mark.asyncio
async def test_pipeline_email_failure_after_commit_is_hard_failure_and_preserves_trace_paths(monkeypatch, tmp_path: Path):
    db = tmp_path / "email_failure_after_commit.db"
    db_url = f"sqlite+aiosqlite:///{db}"
    engine = create_async_engine(db_url)
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
        await conn.execute(sa.text("CREATE TABLE store_master (cost_center TEXT PRIMARY KEY, customer_retention_pipeline BOOLEAN NOT NULL DEFAULT 1)"))
        await conn.execute(sa.text("INSERT INTO store_master VALUES ('S1', 1)"))

    monkeypatch.setattr("app.customer_retention.pipeline.config", SimpleNamespace(database_url=db_url, customer_followup_output_dir=str(tmp_path / "outputs"), customer_followup_backlog_warning_threshold=7))
    monkeypatch.setattr("app.customer_retention.pipeline.get_customer_followup_paths", lambda: SimpleNamespace(archive_dir=tmp_path / "archive"))
    returned_file = tmp_path / "returned.xlsx"
    returned_file.write_text("returned", encoding="utf-8")
    monkeypatch.setattr("app.customer_retention.pipeline.discover_returned_workbooks", lambda logger=None: [SimpleNamespace(path=returned_file)])
    monkeypatch.setattr("app.customer_retention.pipeline.discover_external_lead_files", lambda logger=None: [])
    async def load_stores(*args, **kwargs):
        return ["S1"]

    monkeypatch.setattr("app.customer_retention.pipeline.load_active_retention_stores", load_stores)

    async def detect(session, **kwargs):
        return SimpleNamespace(leads_recovered=0, leads_closed=0)

    async def snapshot(session, **kwargs):
        return SimpleNamespace(rows=[], rows_invalid_mobile=0)

    async def import_td(session, pipeline_run_id, **kwargs):
        await session.execute(trx_customer_followup_leads.insert().values(
            lead_id=901,
            lead_uuid="00000000-0000-0000-0000-000000000901",
            lead_source_type="TD",
            source_system="TEST_TD",
            source_table_name="crm_leads_current",
            source_record_id="td-901",
            cost_center="S1",
            normalized_mobile_number="9000000901",
            lead_date=date(2026, 6, 13),
            lead_status="OPEN",
            created_at=datetime(2026, 6, 13, tzinfo=timezone.utc),
            updated_at=datetime(2026, 6, 13, tzinfo=timezone.utc),
        ))
        await session.execute(trx_customer_followup_history.insert().values(
            history_id=902,
            lead_id=901,
            pipeline_run_id=pipeline_run_id,
            event_type="Email_Failure_Regression",
            created_at=datetime(2026, 6, 13, tzinfo=timezone.utc),
        ))
        await session.execute(trx_customer_suppression.insert().values(
            suppression_id=903,
            cost_center="S1",
            normalized_mobile_number="9000000901",
            suppression_reason="Wrong Number",
            suppression_state="ACTIVE",
            suppression_start_date=date(2026, 6, 13),
            is_permanent=True,
            approval_required=False,
            source_lead_id=901,
            created_by_pipeline_run_id=pipeline_run_id,
            created_at=datetime(2026, 6, 13, tzinfo=timezone.utc),
        ))
        return SimpleNamespace(rows_seen=1, leads_created=1, warnings=[])

    async def ingest_workbook(session, path, pipeline_run_id, **kwargs):
        return SimpleNamespace(rows_seen=1, history_inserted=0, warnings=[])

    async def generate_retention(session, **kwargs):
        return SimpleNamespace(rows_seen=0, leads_created=0, leads_reused=0, rows_skipped=0, warnings=[])

    async def select(session, **kwargs):
        return []

    async def summary(session, **kwargs):
        lead_count = await session.scalar(sa.select(sa.func.count()).select_from(trx_customer_followup_leads))
        return {"run_summary": {"pipeline_run_id": "email-fails"}, "uncommitted_lead_count": lead_count}

    async def send_raises(*args, **kwargs):
        raise RuntimeError("SMTP outage after commit")

    monkeypatch.setattr("app.customer_retention.pipeline._ingest_returned_workbook", ingest_workbook)
    monkeypatch.setattr("app.customer_retention.pipeline.detect_recoveries", detect)
    monkeypatch.setattr("app.customer_retention.pipeline.build_customer_retention_snapshot", snapshot)
    monkeypatch.setattr("app.customer_retention.pipeline._import_td_leads", import_td)
    monkeypatch.setattr("app.customer_retention.pipeline.allocate_and_generate_retention_leads", generate_retention)
    monkeypatch.setattr("app.customer_retention.pipeline.select_workbook_leads_for_active_stores", select)
    monkeypatch.setattr("app.customer_retention.pipeline.generate_workbooks", lambda **kwargs: SimpleNamespace(outputs=[]))
    monkeypatch.setattr("app.customer_retention.pipeline.build_management_summary_payload", summary)
    monkeypatch.setattr("app.customer_retention.pipeline.send_owner_summary", send_raises)

    with pytest.raises(CustomerRetentionNotificationError) as exc_info:
        await run_customer_retention_pipeline(run_date=date(2026, 6, 13), run_id="email-fails", dry_run=False, skip_email=False)

    result = exc_info.value.run_result
    assert result.status == "failed"
    assert "email_delivery_failed_after_commit" in result.warnings
    assert result.email_status["reason"] == "email_delivery_failed_after_commit"
    assert result.email_status["committed"] is True
    assert "SMTP outage after commit" in result.email_status["error"]
    assert result.email_status["generated_files"] == result.generated_files
    assert len(result.email_status["archived_files"]) == 1
    assert Path(result.email_status["archived_files"][0]).exists()
    assert result.counts["files_archived"] == 1

    # The failed email changes only the operator-visible run result; the
    # already-committed business transaction remains durable for recovery.
    assert result.summary_payload["uncommitted_lead_count"] == 1

    Session = async_sessionmaker(engine, expire_on_commit=False)
    async with Session() as session:
        lead_count = await session.scalar(sa.select(sa.func.count()).select_from(trx_customer_followup_leads))
        history_count = await session.scalar(sa.select(sa.func.count()).select_from(trx_customer_followup_history))
        suppression_count = await session.scalar(sa.select(sa.func.count()).select_from(trx_customer_suppression))
    assert lead_count == 1
    assert history_count == 1
    assert suppression_count == 1
    await engine.dispose()
