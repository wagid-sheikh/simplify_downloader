from __future__ import annotations

import argparse
import asyncio
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Sequence

import pdfplumber
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


_FILENAME_PATTERN = re.compile(
    r"^leads_assignment_(?P<date>\d{4}-\d{2}-\d{2})_(?P<store>[A-Za-z0-9]+)_(?P<agent>[A-Za-z0-9]+)\.pdf$"
)


class OutcomeParseError(RuntimeError):
    """Raised when the ingestor cannot extract data from the PDF."""


def _clean(cell: str | None) -> str:
    return (cell or "").strip()


def _parse_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    lowered = value.strip().lower()
    if not lowered:
        return None
    if lowered in {"y", "yes", "1"}:
        return True
    if lowered in {"n", "no", "0"}:
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
    try:
        return date.fromisoformat(cleaned)
    except ValueError as exc:
        raise OutcomeParseError(f"Invalid date value: {value!r}") from exc


def _expect_int(value: str | None) -> int:
    cleaned = _clean(value)
    try:
        return int(cleaned)
    except ValueError as exc:
        raise OutcomeParseError(f"Invalid rowid: {value!r}") from exc


def _extract_outcome_rows(tables: Iterable[Sequence[Sequence[str | None]]]) -> list[OutcomeRow]:
    rows: list[OutcomeRow] = []
    for table in tables:
        if not table:
            continue
        header = [_clean(cell) for cell in table[0]]
        try:
            rowid_idx = header.index("RowID")
            conv_idx = header.index("Conv (Y/N)")
            order_no_idx = header.index("Order No")
            order_date_idx = header.index("Order Date")
            value_idx = header.index("Value")
            pay_mode_idx = header.index("Payment Mode")
            pay_amt_idx = header.index("Payment Amt")
            remarks_idx = header.index("Remarks")
        except ValueError:
            continue

        body = table[1:]
        # rows come in pairs: snapshot row then empty input row
        for i in range(0, len(body), 2):
            if i + 1 >= len(body):
                break
            snapshot = body[i]
            inputs = body[i + 1]
            rowid = _expect_int(snapshot[rowid_idx] if rowid_idx < len(snapshot) else None)
            converted_flag = _parse_bool(inputs[conv_idx] if conv_idx < len(inputs) else None)
            order_number = _clean(inputs[order_no_idx] if order_no_idx < len(inputs) else None) or None
            parsed_order_date = _parse_date(inputs[order_date_idx] if order_date_idx < len(inputs) else None)
            order_value = _parse_decimal(inputs[value_idx] if value_idx < len(inputs) else None)
            payment_mode = _clean(inputs[pay_mode_idx] if pay_mode_idx < len(inputs) else None) or None
            payment_amount = _parse_decimal(inputs[pay_amt_idx] if pay_amt_idx < len(inputs) else None)
            remarks = _clean(inputs[remarks_idx] if remarks_idx < len(inputs) else None) or None

            rows.append(
                OutcomeRow(
                    rowid=rowid,
                    converted_flag=converted_flag,
                    order_number=order_number,
                    order_date=parsed_order_date,
                    order_value=order_value,
                    payment_mode=payment_mode,
                    payment_amount=payment_amount,
                    remarks=remarks,
                )
            )
    if not rows:
        raise OutcomeParseError("No outcome rows found in PDF")
    return rows


def _parse_pdf(pdf_path: Path) -> list[OutcomeRow]:
    with pdfplumber.open(pdf_path) as pdf:
        if not pdf.pages:
            raise OutcomeParseError("PDF has no pages")
        collected: list[OutcomeRow] = []
        for page in pdf.pages:
            tables = page.extract_tables()
            collected.extend(_extract_outcome_rows(tables))
    if not collected:
        raise OutcomeParseError("No tables with outcomes found")
    return collected


def _parse_metadata_from_name(pdf_path: Path) -> tuple[date, str, str]:
    match = _FILENAME_PATTERN.match(pdf_path.name)
    if not match:
        raise OutcomeParseError(
            "Filename must follow leads_assignment_YYYY-MM-DD_STORECODE_AGENTCODE.pdf"
        )
    batch_date = date.fromisoformat(match.group("date"))
    store_code = match.group("store").upper()
    agent_code = match.group("agent").upper()
    return batch_date, store_code, agent_code


async def _resolve_assignment_map(db_session, batch_date: date, store_code: str, agent_code: str) -> tuple[int, dict[int, int]]:
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
              AND store_code = :store_code
              AND agent_id = :agent_id
            """
        ),
        {"batch_id": batch_id, "store_code": store_code, "agent_id": agent_id},
    )
    mapping = {int(row.rowid): int(row.id) for row in assignment_rows}
    if not mapping:
        raise OutcomeParseError(
            f"No lead assignments found for batch {batch_id}, store {store_code}, agent {agent_code}"
        )
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

    batch_date, store_code, agent_code = _parse_metadata_from_name(pdf_path)
    with timed_event(logger=logger, phase="parse_pdf", message="extracting outcomes"):
        outcomes = _parse_pdf(pdf_path)

    async with session_scope(config.database_url) as db_session:
        batch_id, rowid_map = await _resolve_assignment_map(db_session, batch_date, store_code, agent_code)
        missing_rowids = [row.rowid for row in outcomes if row.rowid not in rowid_map]
        if missing_rowids:
            log_event(
                logger=logger,
                phase="ingest_pdf",
                status="warn",
                message="rowids missing from assignments",
                missing_rowids=missing_rowids,
                batch_id=batch_id,
                store_code=store_code,
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
        store_code=store_code,
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
