from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

import openpyxl
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.common.date_utils import get_timezone
from app.common.db import session_scope
from app.dashboard_downloader.json_logger import JsonLogger, log_event

MOBILE_FALLBACK_NUMBER = "8888999762"

BASE_HEADERS = {"store_code", "order_code", "pickup", "delivery", "customer_name", "customer_phone", "address", "payment_text", "instructions", "customer_source", "status", "status_date"}
DETAIL_HEADERS = {"store_code", "order_code", "order_mode", "order_datetime", "pickup_datetime", "delivery_datetime", "service", "hsn_sac", "item_name", "rate", "quantity", "weight", "addons", "amount"}
PAYMENT_HEADERS = {"store_code", "order_code", "payment_mode", "amount", "payment_date", "transaction_id"}


@dataclass
class ArchiveFileCounters:
    inserted: int = 0
    updated: int = 0
    rejected: int = 0
    warnings: int = 0
    rejected_reasons: dict[str, int] = field(default_factory=dict)


@dataclass
class ArchiveFileResult:
    path: str
    table: str
    counters: ArchiveFileCounters = field(default_factory=ArchiveFileCounters)
    warning_messages: list[str] = field(default_factory=list)


@dataclass
class ArchiveIngestResult:
    files: list[ArchiveFileResult] = field(default_factory=list)


def _stg_uc_archive_orders_base_table(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "stg_uc_archive_orders_base",
        metadata,
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.Text()),
        sa.Column("run_date", sa.DateTime(timezone=True)),
        sa.Column("cost_center", sa.String(length=8)),
        sa.Column("store_code", sa.String(length=8)),
        sa.Column("ingest_remarks", sa.Text()),
        sa.Column("order_code", sa.String(length=24)),
        sa.Column("pickup_raw", sa.Text()),
        sa.Column("delivery_raw", sa.Text()),
        sa.Column("customer_name", sa.String(length=128)),
        sa.Column("customer_phone", sa.String(length=24)),
        sa.Column("address", sa.Text()),
        sa.Column("payment_text", sa.Text()),
        sa.Column("instructions", sa.Text()),
        sa.Column("customer_source", sa.String(length=64)),
        sa.Column("status", sa.String(length=32)),
        sa.Column("status_date_raw", sa.Text()),
        sa.Column("source_file", sa.Text()),
    )


def _stg_uc_archive_order_details_table(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "stg_uc_archive_order_details",
        metadata,
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.Text()),
        sa.Column("run_date", sa.DateTime(timezone=True)),
        sa.Column("cost_center", sa.String(length=8)),
        sa.Column("store_code", sa.String(length=8)),
        sa.Column("ingest_remarks", sa.Text()),
        sa.Column("order_code", sa.String(length=24)),
        sa.Column("order_mode", sa.String(length=64)),
        sa.Column("order_datetime_raw", sa.Text()),
        sa.Column("pickup_datetime_raw", sa.Text()),
        sa.Column("delivery_datetime_raw", sa.Text()),
        sa.Column("service", sa.Text()),
        sa.Column("hsn_sac", sa.String(length=32)),
        sa.Column("item_name", sa.Text()),
        sa.Column("rate", sa.Numeric(12, 2)),
        sa.Column("quantity", sa.Numeric(12, 2)),
        sa.Column("weight", sa.Numeric(12, 3)),
        sa.Column("addons", sa.Text()),
        sa.Column("amount", sa.Numeric(12, 2)),
        sa.Column("line_hash", sa.String(length=64)),
        sa.Column("source_file", sa.Text()),
    )


def _stg_uc_archive_payment_details_table(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "stg_uc_archive_payment_details",
        metadata,
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.Text()),
        sa.Column("run_date", sa.DateTime(timezone=True)),
        sa.Column("cost_center", sa.String(length=8)),
        sa.Column("store_code", sa.String(length=8)),
        sa.Column("ingest_remarks", sa.Text()),
        sa.Column("order_code", sa.String(length=24)),
        sa.Column("payment_mode", sa.String(length=32)),
        sa.Column("amount", sa.Numeric(12, 2)),
        sa.Column("payment_date_raw", sa.Text()),
        sa.Column("transaction_id", sa.String(length=128)),
        sa.Column("source_file", sa.Text()),
    )


def _normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_store_or_order(value: Any) -> str | None:
    text = _normalize_text(value)
    return re.sub(r"\s+", " ", text).upper() if text else None


def _normalize_raw(value: Any) -> str | None:
    text = _normalize_text(value)
    return None if not text or text == "-" else text


def _normalize_phone(value: Any, row_remarks: list[str]) -> str | None:
    text = _normalize_text(value)
    if not text:
        return None
    digits = re.sub(r"\D", "", re.sub(r"^\+?91", "", text))
    if not digits:
        return None
    if len(digits) == 10:
        return digits
    if len(digits) > 10:
        row_remarks.append("phone_format_warning")
        return digits
    row_remarks.append("MOBILE_FALLBACK_APPLIED")
    return MOBILE_FALLBACK_NUMBER


def _parse_numeric(value: Any, *, reason: str, row_remarks: list[str], scale: str = "0.01") -> Decimal | None:
    text = _normalize_text(value)
    if text is None or text == "-":
        row_remarks.append(reason)
        return None
    cleaned = re.sub(r"(?i)inr", "", text).replace("₹", "").replace(",", "").strip()
    cleaned = re.sub(r"\s*(kg|kgs|g)$", "", cleaned, flags=re.IGNORECASE)
    try:
        return Decimal(cleaned).quantize(Decimal(scale))
    except (InvalidOperation, ValueError):
        row_remarks.append(reason)
        return None


def _build_line_hash(row: Mapping[str, Any]) -> str:
    keys = ("store_code", "order_code", "service", "hsn_sac", "item_name", "rate", "quantity", "weight", "addons", "amount")
    payload = "|".join(str(row.get(key) or "") for key in keys)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _normalize_payment_mode(value: Any, remarks: list[str]) -> str:
    text = _normalize_text(value)
    if not text:
        remarks.append("missing_payment_mode")
        return "UNKNOWN"
    canonical = re.sub(r"[^A-Z0-9]+", "_", text.upper()).strip("_")
    return "UPI_WALLET" if canonical in {"UPI_WALLET", "UPI_WALLET_"} else (canonical or "UNKNOWN")


def _load_rows(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
    sheet = workbook.active
    rows = list(sheet.iter_rows(values_only=True))
    if not rows:
        return [], []
    headers = [str(v).strip() if v is not None else "" for v in rows[0]]
    return ([{headers[i]: row[i] if i < len(row) else None for i in range(len(headers))} for row in rows[1:]], headers)


async def _fetch_store_mapping(database_url: str) -> dict[str, str | None]:
    async with session_scope(database_url) as session:
        table = sa.table(
            "store_master",
            sa.column("store_code", sa.String()),
            sa.column("cost_center", sa.String()),
            sa.column("sync_group", sa.String()),
        )
        try:
            records = await session.execute(
                sa.select(table.c.store_code, table.c.cost_center).where(sa.func.upper(sa.func.coalesce(table.c.sync_group, "")) == "UC")
            )
        except Exception:
            return {}
        return {
            _normalize_store_or_order(row.store_code): _normalize_text(row.cost_center)
            for row in records
            if _normalize_store_or_order(row.store_code)
        }


def _reject(counters: ArchiveFileCounters, reasons: list[str]) -> None:
    counters.rejected += 1
    for reason in reasons:
        counters.rejected_reasons[reason] = counters.rejected_reasons.get(reason, 0) + 1


async def _existing_keys(session: sa.ext.asyncio.AsyncSession, cols: list[sa.Column], rows: list[dict[str, Any]]) -> set[tuple[Any, ...]]:
    keys = [tuple(row[col.name] for col in cols) for row in rows]
    if not keys:
        return set()
    stmt = sa.select(*cols).where(sa.tuple_(*cols).in_(keys)) if len(cols) > 1 else sa.select(*cols).where(cols[0].in_([k[0] for k in keys]))
    result = await session.execute(stmt)
    return {tuple(record) for record in result.fetchall()}


async def _upsert_rows(
    *,
    database_url: str,
    table: sa.Table,
    rows: list[dict[str, Any]],
    key_cols: list[sa.Column],
) -> tuple[int, int]:
    if not rows:
        return 0, 0
    async with session_scope(database_url) as session:
        existing = await _existing_keys(session, key_cols, rows)
        excluded_cols = {"id", *(col.name for col in key_cols)}
        dialect = session.bind.dialect.name  # type: ignore[union-attr]
        if dialect == "postgresql":
            ins = pg_insert(table).values(rows)
            stmt = ins.on_conflict_do_update(
                index_elements=key_cols,
                set_={col.name: getattr(ins.excluded, col.name) for col in table.columns if col.name not in excluded_cols},
            )
        else:
            ins = sqlite_insert(table).values(rows)
            stmt = ins.on_conflict_do_update(
                index_elements=[col.name for col in key_cols],
                set_={col.name: getattr(ins.excluded, col.name) for col in table.columns if col.name not in excluded_cols},
            )
        await session.execute(stmt)
        await session.commit()
    keys = [tuple(row[col.name] for col in key_cols) for row in rows]
    updated = sum(1 for key in keys if key in existing)
    return len(rows) - updated, updated


async def ingest_uc_archive_excels(
    *,
    folder: Path,
    run_id: str,
    run_date: datetime,
    database_url: str,
    logger: JsonLogger | None = None,
) -> ArchiveIngestResult:
    if run_date.tzinfo is None:
        run_date = run_date.replace(tzinfo=get_timezone())

    metadata = sa.MetaData()
    base_table = _stg_uc_archive_orders_base_table(metadata)
    details_table = _stg_uc_archive_order_details_table(metadata)
    payment_table = _stg_uc_archive_payment_details_table(metadata)
    store_map = await _fetch_store_mapping(database_url)
    result = ArchiveIngestResult()

    for path in sorted(folder.glob("*.xlsx")):
        name = path.name.lower()
        file_type, table, expected = (
            ("base", base_table, BASE_HEADERS)
            if "base_order_info" in name
            else ("details", details_table, DETAIL_HEADERS)
            if "order_details" in name
            else ("payment", payment_table, PAYMENT_HEADERS)
            if "payment_details" in name
            else ("", base_table, set())
        )
        if not file_type:
            continue

        loaded_rows, headers = _load_rows(path)
        file_result = ArchiveFileResult(path=str(path), table=table.name)
        extras = [header for header in headers if header and header not in expected]
        if extras:
            file_result.counters.warnings += 1
            file_result.warning_messages.append(f"unexpected_headers:{','.join(sorted(extras))}")

        upsert_rows: list[dict[str, Any]] = []
        for loaded in loaded_rows:
            remarks: list[str] = []
            store_code = _normalize_store_or_order(loaded.get("store_code"))
            order_code = _normalize_store_or_order(loaded.get("order_code"))
            rejects: list[str] = []
            if not store_code or store_code not in store_map:
                rejects.append("missing_store_code")
            if not order_code:
                rejects.append("missing_order_code")
            if rejects:
                _reject(file_result.counters, rejects)
                continue

            cost_center = store_map.get(store_code)
            if not cost_center:
                remarks.append("missing_cost_center_mapping")
            row: dict[str, Any] = {
                "run_id": run_id,
                "run_date": run_date,
                "cost_center": cost_center,
                "store_code": store_code,
                "order_code": order_code,
                "source_file": path.name,
            }

            if file_type == "base":
                row.update(
                    {
                        "pickup_raw": _normalize_raw(loaded.get("pickup")),
                        "delivery_raw": _normalize_raw(loaded.get("delivery")),
                        "customer_name": _normalize_text(loaded.get("customer_name")),
                        "customer_phone": _normalize_phone(loaded.get("customer_phone"), remarks),
                        "address": _normalize_text(loaded.get("address")),
                        "payment_text": _normalize_text(loaded.get("payment_text")),
                        "instructions": _normalize_text(loaded.get("instructions")),
                        "customer_source": (_normalize_text(loaded.get("customer_source")) or "").upper() or None,
                        "status": (_normalize_text(loaded.get("status")) or "").title() or None,
                        "status_date_raw": _normalize_raw(loaded.get("status_date")),
                    }
                )
            elif file_type == "details":
                row.update(
                    {
                        "order_mode": (_normalize_text(loaded.get("order_mode")) or "").upper() or None,
                        "order_datetime_raw": _normalize_raw(loaded.get("order_datetime")),
                        "pickup_datetime_raw": _normalize_raw(loaded.get("pickup_datetime")),
                        "delivery_datetime_raw": _normalize_raw(loaded.get("delivery_datetime")),
                        "service": _normalize_text(loaded.get("service")),
                        "hsn_sac": _normalize_text(loaded.get("hsn_sac")),
                        "item_name": _normalize_text(loaded.get("item_name")),
                        "rate": _parse_numeric(loaded.get("rate"), reason="invalid_rate", row_remarks=remarks),
                        "quantity": _parse_numeric(loaded.get("quantity"), reason="invalid_quantity", row_remarks=remarks),
                        "weight": _parse_numeric(loaded.get("weight"), reason="invalid_weight", row_remarks=remarks, scale="0.001"),
                        "addons": _normalize_raw(loaded.get("addons")),
                        "amount": _parse_numeric(loaded.get("amount"), reason="invalid_amount", row_remarks=remarks),
                    }
                )
                row["line_hash"] = _build_line_hash(row)
            else:
                payment_date_raw = _normalize_raw(loaded.get("payment_date"))
                if payment_date_raw is None:
                    remarks.append("missing_payment_date")
                row.update(
                    {
                        "payment_mode": _normalize_payment_mode(loaded.get("payment_mode"), remarks),
                        "amount": _parse_numeric(loaded.get("amount"), reason="invalid_payment_amount", row_remarks=remarks),
                        "payment_date_raw": payment_date_raw,
                        "transaction_id": _normalize_text(loaded.get("transaction_id")) or "",
                    }
                )

            row["ingest_remarks"] = ",".join(sorted(set(remarks))) if remarks else None
            if remarks:
                file_result.counters.warnings += 1
            upsert_rows.append(row)

        key_cols = (
            [table.c.store_code, table.c.order_code]
            if file_type == "base"
            else [table.c.store_code, table.c.order_code, table.c.line_hash]
            if file_type == "details"
            else [table.c.store_code, table.c.order_code, table.c.payment_date_raw, table.c.payment_mode, table.c.amount, table.c.transaction_id]
        )
        inserted, updated = await _upsert_rows(database_url=database_url, table=table, rows=upsert_rows, key_cols=key_cols)
        file_result.counters.inserted = inserted
        file_result.counters.updated = updated
        result.files.append(file_result)

        if logger:
            log_event(
                logger,
                "info",
                "UC archive file ingested",
                source_file=path.name,
                table=table.name,
                inserted=inserted,
                updated=updated,
                rejected=file_result.counters.rejected,
                warnings=file_result.counters.warnings,
            )

    return result
