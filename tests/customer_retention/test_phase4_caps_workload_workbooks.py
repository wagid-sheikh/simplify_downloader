from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import openpyxl
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from app.common.db import session_scope
from app.customer_retention.caps import resolve_active_cap
from app.customer_retention.constants import (
    CAP_WORK_SECTION_EXTERNAL_LEAD,
    CAP_WORK_SECTION_FRESH_RETENTION,
    CAP_WORK_SECTION_PENDING_CARRY_FORWARD,
    CAP_WORK_SECTION_TD_LEAD,
    CAP_WORK_SECTION_DUE_FOLLOWUP,
    LEAD_SOURCE_EXTERNAL,
    LEAD_SOURCE_RETENTION,
    LEAD_SOURCE_TD,
    LEAD_STATUS_CLOSED,
    LEAD_STATUS_OPEN,
    LEAD_STATUS_PENDING,
    LEAD_STATUS_RECOVERED,
    SUPPRESSION_STATE_ACTIVE,
    WORKBOOK_OUTCOME_LABELS,
)
from app.customer_retention.db_tables import customer_followup_cap_config, metadata, trx_customer_followup_leads, trx_customer_suppression
from app.customer_retention.workbook_generator import DROPDOWN_VALUES_BY_HEADER, FOLLOWUP_HEADERS, generate_store_workbook
from app.customer_retention.workbook_ingestor import EDITABLE_COLUMNS, FOLLOWUP_SHEET, READ_ME_SHEET, ingest_returned_workbook
from app.customer_retention.workbook_selection import load_active_retention_stores, select_workbook_leads_for_store
from app.customer_retention.workload import evaluate_retention_workload_freeze


async def _prepare_db(tmp_path: Path, *, relaxed_caps: bool = False) -> str:
    url = f"sqlite+aiosqlite:///{tmp_path / ('phase4_relaxed.db' if relaxed_caps else 'phase4.db')}"
    engine = create_async_engine(url, future=True)
    async with engine.begin() as conn:
        if relaxed_caps:
            await conn.execute(sa.text("""
                CREATE TABLE customer_followup_cap_config (
                    cap_config_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cost_center TEXT NULL,
                    lead_source_type TEXT NOT NULL,
                    work_section TEXT NOT NULL,
                    daily_cap INTEGER NULL,
                    is_uncapped BOOLEAN NOT NULL DEFAULT 0,
                    enabled BOOLEAN NOT NULL DEFAULT 1,
                    effective_from DATE NOT NULL,
                    effective_until DATE NULL,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                )
            """))
        else:
            await conn.run_sync(metadata.create_all)
            await conn.execute(sa.text("CREATE TABLE store_master (cost_center TEXT PRIMARY KEY, customer_retention_pipeline BOOLEAN NOT NULL DEFAULT 1)"))
            await conn.execute(sa.text("INSERT INTO store_master (cost_center, customer_retention_pipeline) VALUES ('A100', 1), ('B200', 0), ('C300', 1), ('D400', 1)"))
    await engine.dispose()
    return url


async def _insert_cap(session, *, source=LEAD_SOURCE_RETENTION, section=CAP_WORK_SECTION_FRESH_RETENTION, cost_center=None, daily_cap=13, uncapped=False, enabled=True, effective_from=date(2026, 1, 1), effective_until=None):
    next_id = int((await session.execute(sa.select(sa.func.coalesce(sa.func.max(customer_followup_cap_config.c.cap_config_id), 0) + 1))).scalar_one())
    await session.execute(customer_followup_cap_config.insert().values(
        cap_config_id=next_id,
        cost_center=cost_center,
        lead_source_type=source,
        work_section=section,
        daily_cap=daily_cap,
        is_uncapped=uncapped,
        enabled=enabled,
        effective_from=effective_from,
        effective_until=effective_until,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    ))


async def _insert_lead(session, *, lead_id: int, cost_center="A100", source=LEAD_SOURCE_RETENTION, status=LEAD_STATUS_OPEN, lead_date=date(2026, 6, 12), next_followup_date=None, mobile=None, priority=Decimal("10"), closed=False, recovered=False, suppression=False):
    normalized = mobile if mobile is not None else f"98765432{lead_id:02d}"[-10:]
    await session.execute(trx_customer_followup_leads.insert().values(
        lead_id=lead_id,
        lead_uuid=f"lead-{lead_id}",
        lead_source_type=source,
        source_system="test",
        source_table_name="source" if source in {LEAD_SOURCE_TD, LEAD_SOURCE_EXTERNAL} else None,
        source_record_id=str(lead_id) if source in {LEAD_SOURCE_TD, LEAD_SOURCE_EXTERNAL} else None,
        cost_center=cost_center,
        customer_name=f"Customer {lead_id}",
        mobile_number=normalized,
        normalized_mobile_number=normalized,
        lead_date=lead_date,
        lead_status=status,
        lifecycle_bucket="WARM" if source == LEAD_SOURCE_RETENTION else None,
        last_order_date=lead_date - timedelta(days=35),
        days_since_last_order=35,
        total_orders=3,
        lifetime_spend=Decimal("900.00"),
        average_order_value=Decimal("300.00"),
        last_order_amount=Decimal("300.00"),
        priority_score=priority,
        recommended_strategy="Call promptly" if source != LEAD_SOURCE_RETENTION else "Gentle reminder call",
        next_followup_date=next_followup_date,
        contact_attempted=False,
        complaint_flag=False,
        do_not_contact_flag=False,
        is_closed=closed or status == LEAD_STATUS_CLOSED,
        is_recovered=recovered or status == LEAD_STATUS_RECOVERED,
        suppression_applied=suppression,
        created_at=datetime.now(timezone.utc),
        created_by_pipeline_run_id="run-retention" if source == LEAD_SOURCE_RETENTION else None,
        updated_by_pipeline_run_id="run-retention" if source == LEAD_SOURCE_RETENTION else None,
        updated_at=datetime.now(timezone.utc),
    ))


@pytest.mark.asyncio
async def test_cap_resolver_selection_rules_and_missing_disabled_uncapped(tmp_path: Path) -> None:
    url = await _prepare_db(tmp_path)
    async with session_scope(url) as session:
        await _insert_cap(session, daily_cap=13, effective_from=date(2026, 1, 1))
        await _insert_cap(session, cost_center="A100", daily_cap=7, effective_from=date(2026, 1, 1))
        await _insert_cap(session, cost_center="A100", daily_cap=5, enabled=False, effective_from=date(2026, 6, 1))
        await _insert_cap(session, cost_center="C300", daily_cap=9, effective_from=date(2026, 1, 1), effective_until=date(2026, 5, 31))
        await _insert_cap(session, cost_center="D400", daily_cap=8, effective_from=date(2026, 1, 1))
        await _insert_cap(session, cost_center="D400", daily_cap=6, effective_from=date(2026, 6, 1))
        await _insert_cap(session, source=LEAD_SOURCE_TD, section=CAP_WORK_SECTION_TD_LEAD, daily_cap=None, uncapped=True)
        await session.commit()

        assert (await resolve_active_cap(session, lead_source_type=LEAD_SOURCE_RETENTION, work_section=CAP_WORK_SECTION_FRESH_RETENTION, cost_center="Z999", run_date=date(2026, 6, 12))).daily_cap == 13
        assert (await resolve_active_cap(session, lead_source_type=LEAD_SOURCE_RETENTION, work_section=CAP_WORK_SECTION_FRESH_RETENTION, cost_center="A100", run_date=date(2026, 6, 12))).daily_cap == 7
        assert (await resolve_active_cap(session, lead_source_type=LEAD_SOURCE_RETENTION, work_section=CAP_WORK_SECTION_FRESH_RETENTION, cost_center="C300", run_date=date(2026, 6, 12))).daily_cap == 13
        assert (await resolve_active_cap(session, lead_source_type=LEAD_SOURCE_RETENTION, work_section=CAP_WORK_SECTION_FRESH_RETENTION, cost_center="D400", run_date=date(2026, 6, 12))).daily_cap == 6
        assert (await resolve_active_cap(session, lead_source_type=LEAD_SOURCE_TD, work_section=CAP_WORK_SECTION_TD_LEAD, cost_center="A100", run_date=date(2026, 6, 12))).is_uncapped is True
        assert (await resolve_active_cap(session, lead_source_type=LEAD_SOURCE_EXTERNAL, work_section=CAP_WORK_SECTION_EXTERNAL_LEAD, cost_center="A100", run_date=date(2026, 6, 12))).missing is True


@pytest.mark.asyncio
async def test_cap_resolver_detects_invalid_td_and_ambiguous_overlap(tmp_path: Path) -> None:
    url = await _prepare_db(tmp_path, relaxed_caps=True)
    async with session_scope(url) as session:
        await _insert_cap(session, source=LEAD_SOURCE_TD, section=CAP_WORK_SECTION_TD_LEAD, daily_cap=1, uncapped=False)
        invalid = await resolve_active_cap(session, lead_source_type=LEAD_SOURCE_TD, work_section=CAP_WORK_SECTION_TD_LEAD, cost_center="A100", run_date=date(2026, 6, 12))
        assert invalid.valid is False
        assert invalid.warnings[0].code == "td_cap_contract_violation"
        await _insert_cap(session, daily_cap=13, effective_from=date(2026, 1, 1))
        await _insert_cap(session, daily_cap=12, effective_from=date(2026, 1, 1))
        ambiguous = await resolve_active_cap(session, lead_source_type=LEAD_SOURCE_RETENTION, work_section=CAP_WORK_SECTION_FRESH_RETENTION, cost_center="A100", run_date=date(2026, 6, 12))
        assert ambiguous.valid is False
        assert ambiguous.warnings[0].code == "ambiguous_active_cap"


@pytest.mark.asyncio
async def test_workload_freeze_threshold_and_old_carry_forward_exclusion(tmp_path: Path) -> None:
    url = await _prepare_db(tmp_path)
    run_date = date(2026, 6, 12)
    async with session_scope(url) as session:
        for i in range(1, 4):
            await _insert_lead(session, lead_id=i, lead_date=run_date - timedelta(days=i))
        await _insert_lead(session, lead_id=99, lead_date=run_date - timedelta(days=30))
        await session.commit()
        below = await evaluate_retention_workload_freeze(session, cost_center="A100", run_date=run_date, threshold=4)
        at_threshold_allowed = await evaluate_retention_workload_freeze(session, cost_center="A100", run_date=run_date, threshold=3)
        above = await evaluate_retention_workload_freeze(session, cost_center="A100", run_date=run_date, threshold=2)
        assert below.frozen is False
        assert at_threshold_allowed.frozen is False
        assert above.frozen is True
        assert above.incomplete_recent_retention_count == 3
        assert above.older_carry_forward_count == 1


@pytest.mark.asyncio
async def test_workbook_selection_categories_caps_freeze_suppression_and_ordering(tmp_path: Path) -> None:
    url = await _prepare_db(tmp_path)
    run_date = date(2026, 6, 12)
    async with session_scope(url) as session:
        await _insert_cap(session, daily_cap=1)
        await _insert_cap(session, source=LEAD_SOURCE_EXTERNAL, section=CAP_WORK_SECTION_EXTERNAL_LEAD, daily_cap=1)
        await _insert_lead(session, lead_id=1, next_followup_date=run_date, priority=Decimal("1"))
        await _insert_lead(session, lead_id=2, lead_date=run_date - timedelta(days=1), priority=Decimal("100"))
        await _insert_lead(session, lead_id=3, source=LEAD_SOURCE_TD, priority=Decimal("100"))
        await _insert_lead(session, lead_id=4, source=LEAD_SOURCE_TD, priority=Decimal("90"))
        await _insert_lead(session, lead_id=5, source=LEAD_SOURCE_EXTERNAL, priority=Decimal("80"))
        await _insert_lead(session, lead_id=6, source=LEAD_SOURCE_EXTERNAL, priority=Decimal("70"))
        await _insert_lead(session, lead_id=7, source=LEAD_SOURCE_RETENTION, priority=Decimal("60"))
        await _insert_lead(session, lead_id=8, source=LEAD_SOURCE_RETENTION, priority=Decimal("50"))
        await _insert_lead(session, lead_id=9, source=LEAD_SOURCE_RETENTION, mobile="1111111111")
        await _insert_lead(session, lead_id=10, source=LEAD_SOURCE_RETENTION, closed=True, status=LEAD_STATUS_CLOSED)
        await _insert_lead(session, lead_id=11, source=LEAD_SOURCE_RETENTION, recovered=True, status=LEAD_STATUS_RECOVERED)
        await _insert_lead(session, lead_id=12, source=LEAD_SOURCE_EXTERNAL, mobile="9876543299")
        await session.execute(trx_customer_suppression.insert().values(suppression_id=1, cost_center="A100", mobile_number="9876543299", normalized_mobile_number="9876543299", suppression_reason="Not Interested", suppression_state=SUPPRESSION_STATE_ACTIVE, suppression_start_date=run_date, suppression_until=run_date + timedelta(days=90), is_permanent=False, approval_required=False, source_lead_id=12, created_at=datetime.now(timezone.utc)))
        await session.commit()

        assert await load_active_retention_stores(session) == ("A100", "C300", "D400")
        result = await select_workbook_leads_for_store(session, cost_center="A100", run_date=run_date, backlog_threshold=99)
        assert [row.work_section for row in result.rows] == [CAP_WORK_SECTION_DUE_FOLLOWUP, CAP_WORK_SECTION_PENDING_CARRY_FORWARD, CAP_WORK_SECTION_TD_LEAD, CAP_WORK_SECTION_TD_LEAD, CAP_WORK_SECTION_EXTERNAL_LEAD, CAP_WORK_SECTION_FRESH_RETENTION]
        assert [row.lead_id for row in result.rows] == [1, 2, 3, 4, 5, 7]
        assert result.counts_by_category[CAP_WORK_SECTION_TD_LEAD] == 2
        frozen = await select_workbook_leads_for_store(session, cost_center="A100", run_date=run_date, backlog_threshold=0)
        assert CAP_WORK_SECTION_FRESH_RETENTION not in {row.work_section for row in frozen.rows}
        assert {CAP_WORK_SECTION_DUE_FOLLOWUP, CAP_WORK_SECTION_PENDING_CARRY_FORWARD, CAP_WORK_SECTION_TD_LEAD, CAP_WORK_SECTION_EXTERNAL_LEAD}.issubset({row.work_section for row in frozen.rows})


@pytest.mark.asyncio
async def test_workbook_generation_structure_validation_and_ingestion_identity(tmp_path: Path) -> None:
    url = await _prepare_db(tmp_path)
    run_date = date(2026, 6, 12)
    async with session_scope(url) as session:
        await _insert_cap(session, daily_cap=2)
        await _insert_lead(session, lead_id=1, priority=Decimal("20"))
        await session.commit()
        selection = await select_workbook_leads_for_store(session, cost_center="A100", run_date=run_date, backlog_threshold=99)

    output = generate_store_workbook(selection=selection, active_cost_centers=("A100", "C300", "D400"), output_root=tmp_path / "outputs" / "customer_followup", generated_at=datetime(2026, 6, 12, 8, 0, tzinfo=timezone.utc))
    assert output.output_path == tmp_path / "outputs" / "customer_followup" / "2026-06" / "customer_followup_A100_2026-06-12.xlsx"

    workbook = openpyxl.load_workbook(output.output_path)
    assert workbook.sheetnames == [READ_ME_SHEET, FOLLOWUP_SHEET]
    readme_text = "\n".join(str(workbook[READ_ME_SHEET].cell(row=i, column=1).value or "") for i in range(1, 7))
    assert "Only edit allowed columns" in readme_text
    assert "Target Cost Center" in readme_text
    assert "invalid entries become warnings" in readme_text
    sheet = workbook[FOLLOWUP_SHEET]
    headers = [cell.value for cell in sheet[1]]
    assert headers == FOLLOWUP_HEADERS
    assert sheet.max_row == 2
    assert sheet.cell(row=2, column=headers.index("lead_source_type") + 1).value == LEAD_SOURCE_RETENTION
    assert sheet.cell(row=2, column=headers.index("recommended_strategy") + 1).value == "Gentle reminder call"
    assert sheet.cell(row=2, column=headers.index("lead_id") + 1).value == 1
    assert sheet.cell(row=2, column=headers.index("normalized_mobile_number") + 1).value
    assert sheet.cell(row=2, column=headers.index("lead_id") + 1).protection.locked is True
    assert sheet.cell(row=2, column=headers.index("Contact Attempted") + 1).protection.locked is False

    validations = list(sheet.data_validations.dataValidation)
    validation_by_range = {str(rng): dv for dv in validations for rng in dv.cells.ranges}
    for header in DROPDOWN_VALUES_BY_HEADER:
        col = openpyxl.utils.get_column_letter(headers.index(header) + 1)
        assert col + "2" in validation_by_range
    target_col = openpyxl.utils.get_column_letter(headers.index("Target Cost Center") + 1)
    target_validation = validation_by_range[f"{target_col}2"]
    assert target_validation.type == "list"
    assert target_validation.allow_blank is True
    assert "A100,C300,D400" in target_validation.formula1
    assert target_validation.showErrorMessage is True
    response_col = openpyxl.utils.get_column_letter(headers.index("Customer Response") + 1)
    assert all(value in validation_by_range[f"{response_col}2"].formula1 for value in WORKBOOK_OUTCOME_LABELS)

    # The generated row can be returned through Phase 2 ingestion using its stable
    # lead_id + normalized mobile identity without creating an identity mismatch.
    sheet.cell(row=2, column=headers.index("Contact Attempted") + 1, value="Yes")
    sheet.cell(row=2, column=headers.index("Customer Response") + 1, value="Interested")
    sheet.cell(row=2, column=headers.index("Complaint") + 1, value="No")
    sheet.cell(row=2, column=headers.index("Do Not Contact") + 1, value="No")
    sheet.cell(row=2, column=headers.index("Handled By") + 1, value="Staff A")
    workbook.save(output.output_path)

    ingest_result = await ingest_returned_workbook(database_url=url, path=output.output_path, pipeline_run_id="run-phase4")
    assert ingest_result.rows_seen == 1
    assert ingest_result.history_inserted == 1
    assert ingest_result.warning_count == 0
