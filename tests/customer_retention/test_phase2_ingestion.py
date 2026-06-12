from __future__ import annotations

import csv
from datetime import date, datetime, timezone
from pathlib import Path

import openpyxl
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from app.common.db import session_scope
from app.customer_retention.db_tables import metadata, trx_customer_followup_history, trx_customer_followup_leads, trx_external_leads
from app.customer_retention.external_import import import_external_lead_file, parse_external_lead_file
from app.customer_retention.input_discovery import archive_processed_file, discover_external_lead_files, discover_returned_workbooks
from app.customer_retention.persistence import get_or_create_followup_lead
from app.customer_retention.mobile import MobileNormalizationStatus, normalize_mobile
from app.customer_retention.normalization import ValueNormalizer, normalize_value
from app.customer_retention.source_adapters import import_td_leads
from app.customer_retention.workbook_ingestor import ingest_returned_workbook


async def _prepare_db(tmp_path: Path) -> str:
    db = tmp_path / "phase2.db"
    url = f"sqlite+aiosqlite:///{db}"
    engine = create_async_engine(url, future=True)
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
        await conn.execute(sa.text("CREATE TABLE store_master (cost_center TEXT PRIMARY KEY, customer_retention_pipeline BOOLEAN NOT NULL DEFAULT 1)"))
        await conn.execute(sa.text("INSERT INTO store_master (cost_center, customer_retention_pipeline) VALUES ('A100', 1), ('B200', 0)"))
        await conn.execute(sa.text("""
            CREATE TABLE crm_leads_current (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_uid TEXT NOT NULL UNIQUE,
                store_code TEXT NOT NULL,
                pickup_no TEXT NOT NULL,
                status_bucket TEXT NOT NULL,
                customer_name TEXT,
                mobile TEXT,
                pickup_date TEXT,
                pickup_created_at TIMESTAMP,
                special_instruction TEXT,
                reason TEXT,
                source TEXT,
                customer_type TEXT,
                run_id TEXT,
                source_file TEXT,
                scraped_at TIMESTAMP
            )
        """))
    await engine.dispose()
    return url


def _write_external_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["cost_center", "customer_name", "mobile_number", "lead_source", "campaign_name", "lead_date", "remarks"])
        writer.writeheader()
        writer.writerows(rows)


def _write_workbook(path: Path, rows: list[dict[str, object]]) -> None:
    wb = openpyxl.Workbook()
    readme = wb.active
    readme.title = "READ_ME"
    readme.append(["Instructions"])
    ws = wb.create_sheet("FOLLOWUP_LEADS")
    headers = ["lead_id", "lead_source_type", "cost_center", "customer_name", "mobile_number", "Contact Attempted", "Customer Response", "Complaint", "Do Not Contact", "Handled By"]
    ws.append(headers)
    for row in rows:
        ws.append([row.get(h) for h in headers])
    wb.save(path)


def test_mobile_normalization_statuses_and_variants() -> None:
    assert normalize_mobile("98765 43210").normalized_mobile == "9876543210"
    assert normalize_mobile("+91-98765-43210").normalized_mobile == "9876543210"
    assert normalize_mobile("09876543210").normalized_mobile == "9876543210"
    assert normalize_mobile(" ").status == MobileNormalizationStatus.BLANK
    assert normalize_mobile("abc987").status == MobileNormalizationStatus.MALFORMED
    assert normalize_mobile("1111111111").status == MobileNormalizationStatus.UNNORMALIZABLE


def test_value_normalization_examples_and_unknowns() -> None:
    assert normalize_value(" whatsapp ").normalized_value == "WhatsApp Sent"
    assert normalize_value("watsapp").normalized_value == "WhatsApp Sent"
    assert normalize_value("WA sent").normalized_value == "WhatsApp Sent"
    assert normalize_value("no resp").normalized_value == "No Response"
    assert normalize_value("not interested").normalized_value == "Not Interested"
    assert normalize_value("dnd").normalized_value == "Do Not Contact"
    assert normalize_value("wrong no").normalized_value == "Wrong Number"
    assert normalize_value("invalid").normalized_value == "Invalid Number"
    assert normalize_value("pickup").normalized_value == "Pickup Requested"
    unknown = ValueNormalizer().normalize("mystery", allowed_values={"No Response"}, field_name="Customer Response")
    assert unknown.invalid is True
    assert unknown.warning_code == "value_unrecognized"


def test_external_xlsx_parser_reads_supported_phase2_columns(tmp_path: Path) -> None:
    path = tmp_path / "external.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["cost_center", "customer_name", "mobile_number", "lead_source", "campaign_name", "lead_date", "remarks"])
    ws.append(["A100", "Ada", "9876543210", "Meta", "June", "2026-06-01", "ok"])
    wb.save(path)
    rows = parse_external_lead_file(path)
    assert rows == [(2, {"cost_center": "A100", "customer_name": "Ada", "mobile_number": "9876543210", "lead_source": "Meta", "campaign_name": "June", "lead_date": "2026-06-01", "remarks": "ok"})]


@pytest.mark.asyncio
async def test_external_csv_import_idempotency_and_warnings(tmp_path: Path) -> None:
    url = await _prepare_db(tmp_path)
    csv_path = tmp_path / "leads.csv"
    _write_external_csv(csv_path, [
        {"cost_center": "A100", "customer_name": "Ada", "mobile_number": "9876543210", "lead_source": "Meta", "campaign_name": "June", "lead_date": "2026-06-01", "remarks": "ok"},
        {"cost_center": "A100", "customer_name": "Bad", "mobile_number": "abc", "lead_source": "Meta", "campaign_name": "June", "lead_date": "2026-06-01", "remarks": "bad"},
        {"cost_center": "", "customer_name": "No Store", "mobile_number": "9876543211", "lead_source": "Meta", "campaign_name": "June", "lead_date": "2026-06-01", "remarks": "bad"},
    ])
    first = await import_external_lead_file(database_url=url, path=csv_path, pipeline_run_id="run1")
    second = await import_external_lead_file(database_url=url, path=csv_path, pipeline_run_id="run2")
    assert first.raw_rows_inserted == 1
    assert first.leads_created == 1
    assert {w.code for w in first.warnings} == {"mobile_malformed", "invalid_cost_center"}
    assert second.raw_rows_existing == 1
    assert second.leads_existing == 1
    async with session_scope(url) as session:
        assert (await session.execute(sa.select(sa.func.count()).select_from(trx_external_leads))).scalar_one() == 1
        assert (await session.execute(sa.select(sa.func.count()).select_from(trx_customer_followup_leads))).scalar_one() == 1


@pytest.mark.asyncio
async def test_td_pending_rows_same_store_and_mobile_share_unified_lead(tmp_path: Path) -> None:
    url = await _prepare_db(tmp_path)
    async with session_scope(url) as session:
        await session.execute(sa.text("""
            INSERT INTO crm_leads_current (lead_uid, store_code, pickup_no, status_bucket, customer_name, mobile, pickup_created_at, run_id, scraped_at)
            VALUES ('TD-DUPE-1', 'A100', 'P1', 'pending', 'Ada One', '9876543210', '2026-06-01 00:00:00+00:00', 'r1', '2026-06-01 00:00:00+00:00'),
                   ('TD-DUPE-2', 'A100', 'P2', 'pending', 'Ada Two', '+91-98765-43210', '2026-06-01 00:00:00+00:00', 'r1', '2026-06-01 00:00:00+00:00')
        """))
        await session.commit()

    result = await import_td_leads(database_url=url, pipeline_run_id="run1")

    assert result.rows_seen == 2
    assert result.leads_created == 1
    assert result.leads_existing == 1
    async with session_scope(url) as session:
        assert (await session.execute(sa.select(sa.func.count()).select_from(trx_customer_followup_leads))).scalar_one() == 1


@pytest.mark.asyncio
async def test_external_import_batches_same_cost_center_and_mobile_share_unified_lead(tmp_path: Path) -> None:
    url = await _prepare_db(tmp_path)
    first_path = tmp_path / "leads-first.csv"
    second_path = tmp_path / "leads-second.csv"
    _write_external_csv(first_path, [
        {"cost_center": "A100", "customer_name": "Ada One", "mobile_number": "9876543210", "lead_source": "Meta", "campaign_name": "June", "lead_date": "2026-06-01", "remarks": "first"},
    ])
    _write_external_csv(second_path, [
        {"cost_center": "A100", "customer_name": "Ada Two", "mobile_number": "+91-98765-43210", "lead_source": "Google", "campaign_name": "July", "lead_date": "2026-06-02", "remarks": "second"},
    ])

    first = await import_external_lead_file(database_url=url, path=first_path, pipeline_run_id="run1")
    second = await import_external_lead_file(database_url=url, path=second_path, pipeline_run_id="run2")

    assert first.raw_rows_inserted == 1
    assert first.leads_created == 1
    assert second.raw_rows_inserted == 1
    assert second.leads_existing == 1
    async with session_scope(url) as session:
        assert (await session.execute(sa.select(sa.func.count()).select_from(trx_external_leads))).scalar_one() == 2
        assert (await session.execute(sa.select(sa.func.count()).select_from(trx_customer_followup_leads))).scalar_one() == 1
        converted_ids = (await session.execute(sa.select(trx_external_leads.c.converted_followup_lead_id).order_by(trx_external_leads.c.external_lead_id))).scalars().all()
        assert converted_ids == [1, 1]


@pytest.mark.asyncio
async def test_source_record_match_wins_before_customer_identity_dedupe(tmp_path: Path) -> None:
    url = await _prepare_db(tmp_path)
    async with session_scope(url) as session:
        await session.execute(trx_customer_followup_leads.insert().values(
            lead_id=1,
            lead_uuid="lead-source-exact",
            lead_source_type="TD",
            source_system="TD_CRM_LEADS_SYNC",
            source_table_name="crm_leads_current",
            source_record_id="TD-SAME",
            cost_center="A100",
            customer_name="Closed Source Lead",
            mobile_number="9876543210",
            normalized_mobile_number="9876543210",
            lead_date=date(2026, 6, 1),
            lead_status="CLOSED",
            is_closed=True,
            contact_attempted=False,
            complaint_flag=False,
            do_not_contact_flag=False,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        ))
        await session.execute(trx_customer_followup_leads.insert().values(
            lead_id=2,
            lead_uuid="lead-open-customer",
            lead_source_type="EXTERNAL",
            source_system="CUSTOMER_FOLLOWUP_EXTERNAL_IMPORT",
            source_table_name="trx_external_leads",
            source_record_id="external-other",
            cost_center="A100",
            customer_name="Open Customer Lead",
            mobile_number="9876543210",
            normalized_mobile_number="9876543210",
            lead_date=date(2026, 6, 2),
            lead_status="OPEN",
            is_closed=False,
            contact_attempted=False,
            complaint_flag=False,
            do_not_contact_flag=False,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        ))
        lead_id, created = await get_or_create_followup_lead(
            session,
            lead_source_type="TD",
            source_system="TD_CRM_LEADS_SYNC",
            source_table_name="crm_leads_current",
            source_record_id="TD-SAME",
            source_reference="P1",
            cost_center="A100",
            customer_name="Reprocessed",
            mobile_number="9876543210",
            normalized_mobile_number="9876543210",
            lead_date=date(2026, 6, 3),
            pipeline_run_id="run2",
            dedupe_by_customer_identity=True,
        )

    assert lead_id == 1
    assert created is False


@pytest.mark.asyncio
async def test_td_adapter_import_idempotency_and_invalid_mobile_warning(tmp_path: Path) -> None:
    url = await _prepare_db(tmp_path)
    async with session_scope(url) as session:
        await session.execute(sa.text("""
            INSERT INTO crm_leads_current (lead_uid, store_code, pickup_no, status_bucket, customer_name, mobile, pickup_created_at, run_id, scraped_at)
            VALUES ('TD1', 'A100', 'P1', 'pending', 'Ada', '9876543210', '2026-06-01 00:00:00+00:00', 'r1', '2026-06-01 00:00:00+00:00'),
                   ('TD2', 'A100', 'P2', 'pending', 'Bad', 'abc', '2026-06-01 00:00:00+00:00', 'r1', '2026-06-01 00:00:00+00:00'),
                   ('TD3', 'A100', 'P3', 'completed', 'Done', '9876543211', '2026-06-01 00:00:00+00:00', 'r1', '2026-06-01 00:00:00+00:00')
        """))
        await session.commit()
    first = await import_td_leads(database_url=url, pipeline_run_id="run1")
    second = await import_td_leads(database_url=url, pipeline_run_id="run2")
    assert first.rows_seen == 2
    assert first.leads_created == 1
    assert [w.code for w in first.warnings] == ["mobile_malformed"]
    assert second.leads_existing == 1
    async with session_scope(url) as session:
        assert (await session.execute(sa.select(sa.func.count()).select_from(trx_customer_followup_leads))).scalar_one() == 1


@pytest.mark.asyncio
async def test_workbook_duplicate_upload_protected_edits_invalid_mobile_and_required_blank(tmp_path: Path) -> None:
    url = await _prepare_db(tmp_path)
    async with session_scope(url) as session:
        await session.execute(trx_customer_followup_leads.insert().values(
            lead_id=1,
            lead_uuid="lead-1",
            lead_source_type="EXTERNAL",
            source_system="test",
            source_table_name="source",
            source_record_id="1",
            cost_center="A100",
            customer_name="Ada",
            mobile_number="9876543210",
            normalized_mobile_number="9876543210",
            lead_date=date(2026, 6, 1),
            lead_status="OPEN",
            contact_attempted=False,
            complaint_flag=False,
            do_not_contact_flag=False,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        ))
        await session.commit()
    wb_path = tmp_path / "returned.xlsx"
    _write_workbook(wb_path, [
        {"lead_id": 1, "lead_source_type": "TD", "cost_center": "A100", "customer_name": "Edited", "mobile_number": "9876543210", "Contact Attempted": "Yes", "Customer Response": "wa sent", "Complaint": "No", "Do Not Contact": "No", "Handled By": "Staff"},
        {"lead_id": 1, "lead_source_type": "EXTERNAL", "cost_center": "A100", "customer_name": "Ada", "mobile_number": "1111111111", "Contact Attempted": "Yes", "Customer Response": "No Response", "Complaint": "No", "Do Not Contact": "No"},
        {"lead_id": 1, "lead_source_type": "EXTERNAL", "cost_center": "A100", "customer_name": "Ada", "mobile_number": "9876543210", "Contact Attempted": "", "Customer Response": "", "Complaint": "No", "Do Not Contact": "No"},
    ])
    first = await ingest_returned_workbook(database_url=url, path=wb_path, pipeline_run_id="run1")
    second = await ingest_returned_workbook(database_url=url, path=wb_path, pipeline_run_id="run2")
    assert first.history_inserted == 2
    assert first.protected_edits_ignored >= 1
    assert "mobile_unnormalizable" in {w.code for w in first.warnings}
    assert first.rows_pending_not_updated == 1
    assert second.history_existing == 2
    async with session_scope(url) as session:
        assert (await session.execute(sa.select(sa.func.count()).select_from(trx_customer_followup_history))).scalar_one() == 2
        name = (await session.execute(sa.select(trx_customer_followup_leads.c.customer_name).where(trx_customer_followup_leads.c.lead_id == 1))).scalar_one()
        assert name == "Ada"


def test_input_discovery_and_archive_are_deterministic(tmp_path: Path) -> None:
    input_dir = tmp_path / "inputs" / "customer_followup"
    external_dir = input_dir / "external_leads"
    external_dir.mkdir(parents=True)
    (input_dir / ".hidden.xlsx").write_text("x")
    (input_dir / "~$temp.xlsx").write_text("x")
    workbook = input_dir / "returned.xlsx"
    workbook.write_text("workbook")
    external = external_dir / "leads.csv"
    external.write_text("cost_center,customer_name,mobile_number,lead_source,campaign_name,lead_date,remarks\n")
    assert [f.file_name for f in discover_returned_workbooks(input_dir=input_dir)] == ["returned.xlsx"]
    assert [f.file_name for f in discover_external_lead_files(external_input_dir=external_dir)] == ["leads.csv"]
    archive1 = archive_processed_file(external, archive_dir=tmp_path / "archive" / "customer_followup", run_id="run1", result_metadata={"status": "ok"})
    archive2 = archive_processed_file(external, archive_dir=tmp_path / "archive" / "customer_followup", run_id="run1", result_metadata={"status": "ok"})
    assert archive1 == archive2
    external.write_text("changed")
    archive3 = archive_processed_file(external, archive_dir=tmp_path / "archive" / "customer_followup", run_id="run1")
    assert archive3 != archive1
