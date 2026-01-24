from __future__ import annotations

import csv
from datetime import date
from pathlib import Path
from collections import defaultdict
from typing import Any, Dict, Iterable, List

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.dashboard_downloader.json_logger import JsonLogger, log_event

from ..db import session_scope
from .models import BUCKET_MODEL_MAP
from .schemas import MERGE_BUCKET_DB_SPECS, SkipRow, coerce_csv_row, normalize_headers


def _batched(iterable: Iterable[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    batch: List[Dict[str, Any]] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def _recency_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _read_text_sample(csv_path: Path, *, limit: int = 2048) -> str:
    try:
        with csv_path.open("r", encoding="utf-8", errors="ignore") as handle:
            return handle.read(limit)
    except OSError:
        return ""


def _looks_like_html(csv_path: Path) -> bool:
    """Return True when the file appears to contain an HTML document."""

    sample = _read_text_sample(csv_path)
    if not sample:
        return False

    lowered = sample.lower()
    return "<html" in lowered[:512] or "<!doctype html" in lowered[:512]


MAX_FAILURE_LOGS = 5
MAX_VALUE_LENGTH = 128
BULK_INSERT_BATCH_SIZE = 500


def _compact_row(raw_row: Dict[str, Any]) -> Dict[str, Any]:
    compact: Dict[str, Any] = {}
    for key, value in raw_row.items():
        text = "" if value is None else str(value)
        if len(text) > MAX_VALUE_LENGTH:
            compact[key] = text[: MAX_VALUE_LENGTH - 3] + "..."
        else:
            compact[key] = text
    return compact


def _load_csv_rows(
    bucket: str,
    csv_path: Path,
    logger: JsonLogger | None = None,
    *,
    row_context: Dict[str, Any] | None = None,
    skip_counters: Dict[tuple[str, str], int] | None = None,
) -> Iterable[Dict[str, Any]]:
    total_rows = 0
    coerced_rows = 0
    failed_rows = 0
    skipped_rows = 0
    suppressed_failures = 0
    failure_logs_emitted = 0

    def emit_summary(message: str = "csv ingest summary", *, status: str | None = None) -> None:
        if not logger:
            return
            log_event(
                logger=logger,
                phase="ingest",
                status=status or ("warn" if failed_rows else "info"),
                bucket=bucket,
                merged_file=str(csv_path),
                counts={
                    "total_rows": total_rows,
                    "coerced_rows": coerced_rows,
                    "failed_rows": failed_rows,
                    "skipped_rows": skipped_rows,
                },
                message=message,
            )

    if not csv_path.exists():
        emit_summary("csv file not found")
        return []

    if _looks_like_html(csv_path):
        if logger:
            sample = _read_text_sample(csv_path, limit=512)
            log_event(
                logger=logger,
                phase="ingest",
                status="warn",
                bucket=bucket,
                merged_file=str(csv_path),
                message="merged file is not a valid CSV (HTML content detected)",
                sample=sample,
            )
        emit_summary("csv file appears to contain HTML", status="warn")
        return []

    def iterator() -> Iterable[Dict[str, Any]]:
        nonlocal total_rows, coerced_rows, failed_rows, skipped_rows, suppressed_failures, failure_logs_emitted
        with csv_path.open("r", newline="", encoding="utf-8", errors="ignore") as handle:
            try:
                reader = csv.DictReader(handle)
            except csv.Error as exc:
                if logger:
                    sample = _read_text_sample(csv_path, limit=512)
                    log_event(
                        logger=logger,
                        phase="ingest",
                        status="warn",
                        bucket=bucket,
                        merged_file=str(csv_path),
                        message="failed to parse csv file",
                        error=str(exc),
                        sample=sample,
                    )
                emit_summary("failed to parse csv file", status="warn")
                return

            if not reader.fieldnames:
                if logger:
                    sample = _read_text_sample(csv_path, limit=512)
                    log_event(
                        logger=logger,
                        phase="ingest",
                        status="warn",
                        bucket=bucket,
                        merged_file=str(csv_path),
                        message="csv file missing header row",
                        sample=sample,
                    )
                emit_summary("csv file missing header row", status="warn")
                return
            header_map = normalize_headers(reader.fieldnames)
            try:
                for row_index, raw_row in enumerate(reader, start=1):
                    total_rows += 1
                    try:
                        coerced_row = coerce_csv_row(
                            bucket, raw_row, header_map, extra_fields=row_context
                        )
                    except SkipRow as exc:
                        skipped_rows += 1
                        if skip_counters is not None:
                            store_code = (exc.store_code or "").strip() or "unknown"
                            report_date = exc.report_date
                            if isinstance(report_date, date):
                                report_date = report_date.isoformat()
                            report_date_text = str(report_date or "")
                            skip_counters[(store_code, report_date_text)] += 1
                        continue
                    except ValueError as exc:
                        failed_rows += 1
                        if logger:
                            if failure_logs_emitted < MAX_FAILURE_LOGS:
                                failure_logs_emitted += 1
                                log_event(
                                    logger=logger,
                                    phase="ingest",
                                    status="warn",
                                    bucket=bucket,
                                    merged_file=str(csv_path),
                                    message="failed to coerce csv row",
                                    row_index=row_index,
                                    error=str(exc),
                                    raw_row=_compact_row(raw_row),
                                )
                            else:
                                suppressed_failures += 1
                        continue
                    coerced_rows += 1
                    yield coerced_row
            except csv.Error as exc:
                if logger:
                    sample = _read_text_sample(csv_path, limit=512)
                    log_event(
                        logger=logger,
                        phase="ingest",
                        status="warn",
                        bucket=bucket,
                        merged_file=str(csv_path),
                        message="csv parsing stopped due to error",
                        error=str(exc),
                        processed_rows=total_rows,
                        sample=sample,
                    )
                emit_summary("csv parsing stopped due to error", status="warn")
                return

        if logger and suppressed_failures > 0:
            log_event(
                logger=logger,
                phase="ingest",
                status="warn",
                bucket=bucket,
                merged_file=str(csv_path),
                message=(
                    f"{suppressed_failures} additional rows failed coercion; further failures suppressed"
                ),
            )
        if logger and total_rows > 0 and coerced_rows == 0:
            log_event(
                logger=logger,
                phase="ingest",
                status="warn",
                bucket=bucket,
                merged_file=str(csv_path),
                message="all csv rows failed coercion",
                counts={
                    "total_rows": total_rows,
                    "failed_rows": failed_rows,
                },
            )
        emit_summary()

    return iterator()


async def _upsert_batch(
    session: AsyncSession,
    bucket: str,
    rows: List[Dict[str, Any]],
    *,
    store_counts: Dict[str, int] | None = None,
) -> Dict[str, int]:
    """Upsert a batch of rows and return affected and deduped counts."""
    totals = {"affected_rows": 0, "deduped_rows": 0}
    if not rows:
        return totals

    for chunk in _batched(rows, BULK_INSERT_BATCH_SIZE):
        if store_counts is not None:
            spec = MERGE_BUCKET_DB_SPECS[bucket]
            deduped_rows = _dedupe_rows(bucket, spec, chunk)
            for row in deduped_rows:
                store_code = row.get("store_code")
                if store_code:
                    store_counts[store_code] += 1
        result = await _upsert_rows(session, bucket, chunk)
        totals["affected_rows"] += result["affected_rows"]
        totals["deduped_rows"] += result["deduped_rows"]

    return totals


def _dedupe_rows(bucket: str, spec: dict, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    dedupe_keys: list[str] = spec.get("dedupe_keys", [])
    if not dedupe_keys:
        return rows

    recency_fields_by_bucket = {
        "missed_leads": ["pickup_created_date", "pickup_created_time"],
        "repeat_customers": ["run_date"],
        "nonpackage_all": [
            "run_date",
            "order_date",
            "expected_delivery_date",
            "actual_delivery_date",
        ],
        "undelivered_all": [
            "run_date",
            "order_date",
            "expected_deliver_on",
            "actual_deliver_on",
        ],
    }

    recency_fields = recency_fields_by_bucket.get(bucket) or ["run_date"]

    def key(row: Dict[str, Any]) -> tuple[Any, ...]:
        return tuple(row[field] for field in dedupe_keys)

    def recency_key(row: Dict[str, Any]) -> tuple[str, ...]:
        return tuple(_recency_value(row.get(field)) for field in recency_fields)

    by_key: Dict[tuple[Any, ...], Dict[str, Any]] = {}
    for row in rows:
        k = key(row)
        if k not in by_key:
            by_key[k] = row
            continue

        existing_row = by_key[k]
        if bucket == "undelivered_all":
            incoming_has_actual = row.get("actual_deliver_on") is not None
            existing_has_actual = existing_row.get("actual_deliver_on") is not None

            if incoming_has_actual and not existing_has_actual:
                by_key[k] = row
                continue
            if existing_has_actual and not incoming_has_actual:
                continue

        if bucket == "missed_leads":
            incoming_order_placed = bool(row.get("is_order_placed"))
            existing_order_placed = bool(existing_row.get("is_order_placed"))

            if incoming_order_placed and not existing_order_placed:
                by_key[k] = row
                continue
            if existing_order_placed and not incoming_order_placed:
                continue

        if recency_key(row) >= recency_key(existing_row):
            by_key[k] = row

    return list(by_key.values())


async def _upsert_rows(
    session: AsyncSession,
    bucket: str,
    rows: List[Dict[str, Any]],
) -> Dict[str, int]:
    if not rows:
        return {"affected_rows": 0, "deduped_rows": 0}

    model = BUCKET_MODEL_MAP[bucket]
    spec = MERGE_BUCKET_DB_SPECS[bucket]

    deduped_rows = _dedupe_rows(bucket, spec, rows)

    insert_stmt = insert(model).values(deduped_rows)
    dedupe_keys = spec["dedupe_keys"]
    timestamp_update = {}
    if "updated_at" in model.__table__.c:
        timestamp_update["updated_at"] = func.now()

    if spec.get("insert_only"):
        stmt = insert_stmt.on_conflict_do_nothing(index_elements=dedupe_keys)
    elif bucket == "missed_leads":
        stmt = insert_stmt.on_conflict_do_update(
            index_elements=dedupe_keys,
            set_={
                "is_order_placed": insert_stmt.excluded["is_order_placed"],
                **timestamp_update,
            },
            where=model.is_order_placed.is_distinct_from(
                insert_stmt.excluded["is_order_placed"]
            ),
        )
    elif bucket == "undelivered_all":
        stmt = insert_stmt.on_conflict_do_update(
            index_elements=dedupe_keys,
            set_={
                "actual_deliver_on": insert_stmt.excluded["actual_deliver_on"],
                **timestamp_update,
            },
            where=model.actual_deliver_on.is_distinct_from(
                insert_stmt.excluded["actual_deliver_on"]
            ),
        )
    else:
        update_cols = {
            col: insert_stmt.excluded[col]
            for col in deduped_rows[0].keys()
            if col not in dedupe_keys
        }
        update_cols.update(timestamp_update)
        stmt = insert_stmt.on_conflict_do_update(
            index_elements=dedupe_keys,
            set_=update_cols,
        )

    result = await session.execute(stmt)

    rowcount = getattr(result, "rowcount", None)
    if not rowcount:
        # SQLAlchemy may report ``rowcount`` as ``None`` (or ``0`` on some drivers)
        # for PostgreSQL upserts, so fall back to the number of attempted rows.
        rowcount = len(deduped_rows)

    return {"affected_rows": rowcount, "deduped_rows": len(deduped_rows)}


async def ingest_bucket(
    *,
    bucket: str,
    csv_path: Path,
    batch_size: int,
    database_url: str,
    logger: JsonLogger,
    run_id: str,
    run_date: date,
) -> Dict[str, Any]:
    totals = {"rows": 0, "deduped_rows": 0}
    row_context = {"run_id": run_id, "run_date": run_date}
    skipped_missing_mobile: Dict[tuple[str, str], int] = defaultdict(int)
    store_counts: Dict[str, int] | None = None
    if bucket == "missed_leads":
        store_counts = defaultdict(int)
    async with session_scope(database_url) as session:
        async with session.begin():
            for batch in _batched(
                _load_csv_rows(
                    bucket,
                    csv_path,
                    logger,
                    row_context=row_context,
                    skip_counters=skipped_missing_mobile,
                ),
                batch_size,
            ):
                batch_totals = await _upsert_batch(
                    session, bucket, batch, store_counts=store_counts
                )
                totals["rows"] += batch_totals["affected_rows"]
                totals["deduped_rows"] += batch_totals["deduped_rows"]

    file_size = 0
    if csv_path.exists():
        try:
            file_size = csv_path.stat().st_size
        except OSError:
            file_size = 0

    if file_size > 0 and totals["rows"] == 0:
        log_event(
            logger=logger,
            phase="ingest",
            bucket=bucket,
            merged_file=str(csv_path),
            status="warn",
            message="non-empty merged file produced zero ingested rows",
        )
    log_event(
        logger=logger,
        phase="ingest",
        bucket=bucket,
        merged_file=str(csv_path),
        counts={"ingested_rows": totals["rows"], "deduped_rows": totals["deduped_rows"]},
        message="ingestion complete",
    )

    if bucket == "missed_leads" and skipped_missing_mobile:
        entries = []
        details = []
        for (store_code, report_date), count in sorted(skipped_missing_mobile.items()):
            entries.append(f"{store_code}-{report_date}-{count}")
            details.append(
                {"store_code": store_code, "report_date": report_date, "count": count}
            )

        log_event(
            logger=logger,
            phase="ingest",
            bucket=bucket,
            merged_file=str(csv_path),
            status="info",
            message=(
                "missed_leads rows skipped due to missing mobile_number: "
                + ", ".join(entries)
            ),
            skipped_missing_mobile=details,
        )
    if store_counts is not None:
        totals["store_rows"] = dict(store_counts)
    return totals
