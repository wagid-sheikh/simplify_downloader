"""External CSV/XLSX lead import for Customer Retention Phase 2."""

from __future__ import annotations

import csv
import hashlib
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

import openpyxl
import sqlalchemy as sa

from app.common.db import session_scope
from app.dashboard_downloader.json_logger import JsonLogger, log_event

from .constants import CAP_WORK_SECTION_EXTERNAL_LEAD, LEAD_SOURCE_EXTERNAL, LEAD_STATUS_ERROR, LEAD_STATUS_OPEN
from .db_tables import trx_customer_followup_leads, trx_external_leads
from .mobile import normalize_mobile
from .caps import resolve_active_cap
from .persistence import fetch_active_cost_centers, get_or_create_followup_lead, sqlite_next_id, stable_uuid
from .types import ImportBatchResult, RowWarning

REQUIRED_EXTERNAL_COLUMNS = ("cost_center", "customer_name", "mobile_number", "lead_source", "campaign_name", "lead_date", "remarks")
BLOCKING_REQUIRED_EXTERNAL_FIELDS = set(REQUIRED_EXTERNAL_COLUMNS)
WARNING_ONLY_REQUIRED_EXTERNAL_FIELDS: tuple[str, ...] = ()


def _header_key(value: Any) -> str:
    return "_".join(str(value or "").strip().lower().split())


def _parse_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def parse_external_lead_file(path: Path) -> list[tuple[int, dict[str, Any]]]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            return [(idx, {_header_key(k): v for k, v in row.items()}) for idx, row in enumerate(reader, start=2)]
    if suffix == ".xlsx":
        workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
        sheet = workbook.active
        rows = list(sheet.iter_rows(values_only=True))
        if not rows:
            return []
        headers = [_header_key(cell) for cell in rows[0]]
        parsed: list[tuple[int, dict[str, Any]]] = []
        for offset, row in enumerate(rows[1:], start=2):
            parsed.append((offset, {headers[i]: row[i] if i < len(row) else None for i in range(len(headers))}))
        return parsed
    raise ValueError(f"Unsupported external lead file type: {path.suffix}")


def _row_fingerprint(row: dict[str, Any]) -> str:
    material = "|".join(str(row.get(col) or "").strip() for col in REQUIRED_EXTERNAL_COLUMNS)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


async def _import_external_lead_file(session: AsyncSession, path: Path, pipeline_run_id: str, logger: JsonLogger | None = None) -> ImportBatchResult:
    """Persist external lead file rows without converting them to unified leads."""

    result = ImportBatchResult(source_file=path.name)
    file_digest = hashlib.sha256(path.read_bytes()).hexdigest()
    result.import_batch_id = hashlib.sha256(f"{path.name}:{file_digest}".encode("utf-8")).hexdigest()[:32]
    rows = parse_external_lead_file(path)
    result.rows_seen = len(rows)
    active_cost_centers = await fetch_active_cost_centers(session)
    for row_number, row in rows:
        missing = [col for col in REQUIRED_EXTERNAL_COLUMNS if col not in row]
        if missing:
            result.rows_skipped += 1
            result.warnings.append(RowWarning("missing_required_columns", "External lead row is missing required columns", row_number, path.name))
            continue
        blank_required_fields = [col for col in REQUIRED_EXTERNAL_COLUMNS if not str(row.get(col) or "").strip()]
        for field_name in blank_required_fields:
            result.warnings.append(
                RowWarning(
                    "missing_required_field",
                    "External lead row has a blank required field",
                    row_number,
                    path.name,
                    field_name,
                )
            )
        blank_blocking_fields = [field_name for field_name in blank_required_fields if field_name in BLOCKING_REQUIRED_EXTERNAL_FIELDS]
        cost_center = str(row.get("cost_center") or "").strip().upper()
        mobile = normalize_mobile(row.get("mobile_number"))
        lead_date = _parse_date(row.get("lead_date"))
        if lead_date is None:
            result.rows_skipped += 1
            result.warnings.append(RowWarning("invalid_lead_date", "External lead row has invalid lead date", row_number, path.name, "lead_date", cost_center=cost_center or None))
            continue

        has_valid_cost_center = bool(cost_center) and (not active_cost_centers or cost_center in active_cost_centers)
        has_blank_blocking_field = bool(blank_blocking_fields)
        is_actionable = has_valid_cost_center and mobile.is_valid and not has_blank_blocking_field
        row_hash = _row_fingerprint(row)
        external_uuid = stable_uuid("external", result.import_batch_id, row_hash)
        existing_external = (await session.execute(sa.select(trx_external_leads.c.external_lead_id).where(trx_external_leads.c.external_lead_uuid == external_uuid))).first()
        if existing_external:
            result.raw_rows_existing += 1
        else:
            external_id = await sqlite_next_id(session, trx_external_leads, "external_lead_id")
            values = {
                "external_lead_uuid": external_uuid,
                "lead_source": str(row.get("lead_source") or "").strip(),
                "campaign_name": str(row.get("campaign_name") or "").strip() or None,
                "campaign_reference": result.import_batch_id,
                "cost_center": cost_center,
                "customer_name": str(row.get("customer_name") or "").strip() or None,
                "mobile_number": str(row.get("mobile_number") or "").strip() or None,
                "normalized_mobile_number": mobile.normalized_mobile or "",
                "lead_date": lead_date,
                "lead_status": LEAD_STATUS_OPEN if is_actionable else LEAD_STATUS_ERROR,
                "remarks": str(row.get("remarks") or "").strip() or None,
                "import_batch_id": result.import_batch_id,
                "raw_payload_json": {key: (str(value) if value is not None else None) for key, value in row.items()},
                "converted_to_followup_lead": False,
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            }
            if external_id is not None:
                values["external_lead_id"] = external_id
            await session.execute(trx_external_leads.insert().values(**values))
            result.raw_rows_inserted += 1
        if not has_valid_cost_center:
            result.rows_skipped += 1
            if "cost_center" not in blank_blocking_fields:
                result.warnings.append(RowWarning("invalid_cost_center", "External lead row has missing or inactive cost center", row_number, path.name, "cost_center"))
        elif not mobile.is_valid:
            result.rows_skipped += 1
            if "mobile_number" not in blank_blocking_fields:
                result.warnings.append(RowWarning(mobile.warning_code or "invalid_mobile", mobile.warning_message or "Invalid mobile number", row_number, path.name, "mobile_number", cost_center=cost_center))
        elif has_blank_blocking_field:
            result.rows_skipped += 1
    if logger:
        log_event(logger=logger, phase="external_import", message="external_raw_import_complete", source_file=path.name, rows_seen=result.rows_seen, raw_rows_inserted=result.raw_rows_inserted, warnings=result.warning_count)
    return result


async def convert_capped_external_leads_for_store(
    session: AsyncSession,
    *,
    cost_center: str,
    run_date: date,
    pipeline_run_id: str,
    logger: JsonLogger | None = None,
) -> ImportBatchResult:
    """Convert pending valid external raw rows for one store up to the active cap."""

    normalized_cost_center = cost_center.strip().upper()
    result = ImportBatchResult(source_file=None)
    cap = await resolve_active_cap(
        session,
        lead_source_type=LEAD_SOURCE_EXTERNAL,
        work_section=CAP_WORK_SECTION_EXTERNAL_LEAD,
        cost_center=normalized_cost_center,
        run_date=run_date,
    )
    if not cap.valid or cap.missing:
        result.warnings.extend(RowWarning(w.code, w.message, cost_center=normalized_cost_center) for w in cap.warnings)
        return result

    already_converted_today = int((await session.execute(
        sa.select(sa.func.count())
        .select_from(trx_external_leads.join(trx_customer_followup_leads, trx_external_leads.c.converted_followup_lead_id == trx_customer_followup_leads.c.lead_id))
        .where(
            trx_external_leads.c.cost_center == normalized_cost_center,
            trx_external_leads.c.converted_to_followup_lead.is_(True),
            trx_customer_followup_leads.c.lead_source_type == LEAD_SOURCE_EXTERNAL,
            sa.func.date(trx_external_leads.c.updated_at) == run_date.isoformat(),
        )
    )).scalar_one())
    remaining = None if cap.is_uncapped else max((cap.daily_cap or 0) - already_converted_today, 0)
    if remaining == 0:
        return result

    query = (
        sa.select(trx_external_leads)
        .where(
            trx_external_leads.c.cost_center == normalized_cost_center,
            trx_external_leads.c.lead_status == LEAD_STATUS_OPEN,
            trx_external_leads.c.converted_to_followup_lead.is_(False),
        )
        .order_by(trx_external_leads.c.lead_date, trx_external_leads.c.external_lead_id)
    )
    if remaining is not None:
        query = query.limit(remaining)
    rows = (await session.execute(query)).mappings().all()
    result.rows_seen = len(rows)
    converted_at = datetime.combine(run_date, time.min, tzinfo=timezone.utc)
    for row in rows:
        mobile = normalize_mobile(row["normalized_mobile_number"])
        if not mobile.is_valid:
            # Defensive guard: invalid raw rows should already be non-actionable.
            result.rows_skipped += 1
            continue
        lead_id, created = await get_or_create_followup_lead(
            session,
            lead_source_type=LEAD_SOURCE_EXTERNAL,
            source_system="CUSTOMER_FOLLOWUP_EXTERNAL_IMPORT",
            source_table_name="trx_external_leads",
            source_record_id=row["external_lead_uuid"],
            source_reference=row["import_batch_id"],
            cost_center=normalized_cost_center,
            customer_name=row["customer_name"],
            mobile_number=row["mobile_number"],
            normalized_mobile_number=row["normalized_mobile_number"],
            lead_date=row["lead_date"],
            pipeline_run_id=pipeline_run_id,
            lead_stage=row["campaign_name"],
            assigned_store=normalized_cost_center,
            dedupe_by_customer_identity=True,
        )
        if created:
            result.leads_created += 1
        else:
            result.leads_existing += 1
        await session.execute(
            trx_external_leads.update()
            .where(trx_external_leads.c.external_lead_id == row["external_lead_id"])
            .values(converted_to_followup_lead=True, converted_followup_lead_id=lead_id, updated_at=converted_at)
        )
    if logger:
        log_event(logger=logger, phase="external_conversion", message="external_conversion_complete", cost_center=normalized_cost_center, rows_seen=result.rows_seen, leads_created=result.leads_created, leads_existing=result.leads_existing)
    return result

async def import_external_lead_file(*, database_url: str, path: Path, pipeline_run_id: str, logger: JsonLogger | None = None) -> ImportBatchResult:
    """Standalone wrapper that owns its transaction for external lead import."""

    async with session_scope(database_url) as session:
        result = await _import_external_lead_file(session, path, pipeline_run_id, logger=logger)
        await session.commit()
        return result
