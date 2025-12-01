from __future__ import annotations

import argparse
import asyncio
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Sequence

from PyPDF2 import PdfReader
from sqlalchemy import text

from app.common.db import session_scope
from app.config import config
from app.dashboard_downloader.json_logger import get_logger, log_event, timed_event


@dataclass
class OutcomeRow:
    rowid: int
    converted_flag: bool | None
    order_number: str | None
    order_date: date | None
    order_value: Decimal | None
    payment_mode: str | None
    payment_amount: Decimal | None
    remarks: str | None


class OutcomeParseError(RuntimeError):
    """Raised when the ingestor cannot extract data from the PDF."""


def _clean(cell: str | None) -> str:
    return (cell or "").strip()


def _parse_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    lowered = value.strip().lstrip("/").lower()
    if not lowered:
        return None
    if lowered in {"y", "yes", "1", "on", "true"}:
        return True
    if lowered in {"n", "no", "0", "off", "false"}:
        return False
    raise OutcomeParseError(f"Invalid converted_flag value: {value!r}")


def _parse_decimal(value: str | None) -> Decimal | None:
    cleaned = _clean(value).replace(",", "")
    if not cleaned:
        return None
    try:
        return Decimal(cleaned)
    except Exception as exc:  # pragma: no cover - defensive
        raise OutcomeParseError(f"Invalid decimal value: {value!r}") from exc


def _parse_date(value: str | None) -> date | None:
    cleaned = _clean(value)
    if not cleaned:
        return None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    raise OutcomeParseError(f"Invalid date value: {value!r}")


def _parse_rowid_from_field(name: str) -> int | None:
    match = re.search(r"_(\d+)$", name)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _normalize_field_value(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    try:
        return str(value)
    except Exception:
        return None


def _parse_header_metadata(pdf_path: Path) -> tuple[date, str]:
    reader = PdfReader(str(pdf_path))
    if not reader.pages:
        raise OutcomeParseError("PDF has no pages")
    text = reader.pages[0].extract_text() or ""
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if len(lines) < 3:
        raise OutcomeParseError("PDF header is incomplete")

    agent_line = lines[1]
    if " - " not in agent_line:
        raise OutcomeParseError("Agent line missing expected separator")
    agent_code = agent_line.split(" - ", 1)[0].strip().upper()

    batch_line = lines[2]
    if not batch_line.lower().startswith("batch date:"):
        raise OutcomeParseError("Batch date line missing")
    batch_str = batch_line.split(":", 1)[1].strip()
    try:
        batch_date = datetime.strptime(batch_str, "%d-%m-%Y").date()
    except ValueError as exc:
        raise OutcomeParseError(f"Invalid batch date: {batch_str!r}") from exc

    return batch_date, agent_code


def _parse_pdf(pdf_path: Path) -> list[OutcomeRow]:
    reader = PdfReader(str(pdf_path))
    fields = reader.get_fields()
    if fields is None:
        raise OutcomeParseError("No form fields found in PDF")

    raw_fields: dict[str, str | None] = {}
    for name, field in fields.items():
        value = None
        if isinstance(field, dict):
            value = field.get("/V")
        else:
            value = getattr(field, "value", None)
        raw_fields[name] = _normalize_field_value(value)

    grouped: dict[int, dict[str, str | None]] = {}
    for field_name, value in raw_fields.items():
        rowid = _parse_rowid_from_field(field_name)
        if rowid is None:
            continue
        prefix = field_name.rsplit("_", 1)[0]
        grouped.setdefault(rowid, {})[prefix] = value

    rows: list[OutcomeRow] = []
    for rowid, group_fields in grouped.items():
        converted_flag = _parse_bool(group_fields.get("conv"))
        order_number = _clean(group_fields.get("order_no")) or None
        order_date = _parse_date(group_fields.get("order_date"))
        order_value = _parse_decimal(group_fields.get("value"))
        payment_mode = _clean(group_fields.get("payment_mode")) or None
        payment_amount = _parse_decimal(group_fields.get("payment_amt"))
        remarks = _clean(group_fields.get("remarks")) or None

        rows.append(
            OutcomeRow(
                rowid=rowid,
                converted_flag=converted_flag,
                order_number=order_number,
                order_date=order_date,
                order_value=order_value,
                payment_mode=payment_mode,
                payment_amount=payment_amount,
                remarks=remarks,
            )
        )

    if not rows:
        raise OutcomeParseError("No outcome rows found in PDF")
    return rows


async def _resolve_assignment_map(db_session, batch_date: date, agent_code: str) -> tuple[int, dict[int, int]]:
    batch_rows = await db_session.execute(
        text("SELECT id FROM lead_assignment_batches WHERE batch_date = :batch_date"),
        {"batch_date": batch_date},
    )
    batch_ids = [row.id for row in batch_rows]
    if len(batch_ids) != 1:
        raise OutcomeParseError(f"Expected exactly one batch for {batch_date}, found {len(batch_ids)}")
    batch_id = int(batch_ids[0])

    agent_row = (
        await db_session.execute(
            text("SELECT id FROM agents_master WHERE upper(agent_code) = :code LIMIT 1"),
            {"code": agent_code},
        )
    ).mappings().first()
    if not agent_row:
        raise OutcomeParseError(f"Unknown agent code: {agent_code}")
    agent_id = int(agent_row["id"])

    assignment_rows = await db_session.execute(
        text(
            """
            SELECT id, rowid
            FROM lead_assignments
            WHERE assignment_batch_id = :batch_id
              AND agent_id = :agent_id
            """
        ),
        {"batch_id": batch_id, "agent_id": agent_id},
    )
    mapping = {int(row.rowid): int(row.id) for row in assignment_rows}
    if not mapping:
        raise OutcomeParseError(f"No lead assignments found for batch {batch_id}, agent {agent_code}")
    return batch_id, mapping


async def _upsert_outcomes(db_session, outcomes: Iterable[OutcomeRow], rowid_map: dict[int, int]) -> int:
    updated = 0
    for outcome in outcomes:
        lead_assignment_id = rowid_map.get(outcome.rowid)
        if not lead_assignment_id:
            continue
        await db_session.execute(
            text(
                """
                INSERT INTO lead_assignment_outcomes (
                    lead_assignment_id,
                    converted_flag,
                    order_number,
                    order_date,
                    order_value,
                    payment_mode,
                    payment_amount,
                    remarks,
                    created_at,
                    updated_at
                ) VALUES (
                    :lead_assignment_id,
                    :converted_flag,
                    :order_number,
                    :order_date,
                    :order_value,
                    :payment_mode,
                    :payment_amount,
                    :remarks,
                    :now_ts,
                    :now_ts
                )
                ON CONFLICT (lead_assignment_id) DO UPDATE SET
                    converted_flag = EXCLUDED.converted_flag,
                    order_number = EXCLUDED.order_number,
                    order_date = EXCLUDED.order_date,
                    order_value = EXCLUDED.order_value,
                    payment_mode = EXCLUDED.payment_mode,
                    payment_amount = EXCLUDED.payment_amount,
                    remarks = EXCLUDED.remarks,
                    updated_at = EXCLUDED.updated_at
                """
            ),
            {
                "lead_assignment_id": lead_assignment_id,
                "converted_flag": outcome.converted_flag,
                "order_number": outcome.order_number,
                "order_date": outcome.order_date,
                "order_value": outcome.order_value,
                "payment_mode": outcome.payment_mode,
                "payment_amount": outcome.payment_amount,
                "remarks": outcome.remarks,
                "now_ts": datetime.now(timezone.utc),
            },
        )
        updated += 1
    await db_session.commit()
    return updated


async def ingest_pdf(pdf_path: Path) -> None:
    logger = get_logger()
    log_event(logger=logger, phase="ingest_pdf", message="starting", file=str(pdf_path))

    if not pdf_path.is_absolute():
        candidate = Path(config.reports_root).joinpath(pdf_path)
        pdf_path = candidate

    if not pdf_path.exists():
        raise OutcomeParseError(f"PDF not found at: {pdf_path}")

    batch_date, agent_code = _parse_header_metadata(pdf_path)
    with timed_event(logger=logger, phase="parse_pdf", message="extracting outcomes"):
        outcomes = _parse_pdf(pdf_path)

    async with session_scope(config.database_url) as db_session:
        batch_id, rowid_map = await _resolve_assignment_map(db_session, batch_date, agent_code)
        missing_rowids = [row.rowid for row in outcomes if row.rowid not in rowid_map]
        if missing_rowids:
            log_event(
                logger=logger,
                phase="ingest_pdf",
                status="warn",
                message="rowids missing from assignments",
                missing_rowids=missing_rowids,
                batch_id=batch_id,
                agent_code=agent_code,
            )
        with timed_event(logger=logger, phase="upsert_outcomes", message="persisting outcomes"):
            count = await _upsert_outcomes(db_session, outcomes, rowid_map)

    log_event(
        logger=logger,
        phase="ingest_pdf",
        message="completed",
        status="ok",
        file=str(pdf_path),
        outcomes=count,
        batch_id=batch_id,
        agent_code=agent_code,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest leads assignment outcomes from a PDF")
    parser.add_argument("pdf_path", type=str, help="Path to the leads assignment PDF file")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        asyncio.run(ingest_pdf(Path(args.pdf_path)))
    except OutcomeParseError as exc:
        logger = get_logger()
        log_event(logger=logger, phase="ingest_pdf", status="error", message=str(exc))
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
