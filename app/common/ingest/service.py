from __future__ import annotations

import csv
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.dashboard_downloader.json_logger import JsonLogger, log_event

from ..db import session_scope
from .models import BUCKET_MODEL_MAP
from .schemas import MERGE_BUCKET_DB_SPECS, coerce_csv_row, normalize_headers


def _batched(iterable: Iterable[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    batch: List[Dict[str, Any]] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


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
) -> Iterable[Dict[str, Any]]:
    total_rows = 0
    coerced_rows = 0
    failed_rows = 0
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
        nonlocal total_rows, coerced_rows, failed_rows, suppressed_failures, failure_logs_emitted
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
) -> int:
    """Upsert a batch of rows and return the number of affected rows."""
    if not rows:
        return 0

    affected_rows = 0
    for chunk in _batched(rows, BULK_INSERT_BATCH_SIZE):
        affected_rows += await _upsert_rows(session, bucket, chunk)

    return affected_rows


async def _upsert_rows(
    session: AsyncSession,
    bucket: str,
    rows: List[Dict[str, Any]],
) -> int:
    if not rows:
        return 0

    model = BUCKET_MODEL_MAP[bucket]
    spec = MERGE_BUCKET_DB_SPECS[bucket]

    deduped_rows = rows
    if bucket == "missed_leads":
        def key(r):
            return (r["store_code"], r["mobile_number"])

        def sort_key(r):
            return (r["pickup_created_date"], r["pickup_created_time"])

        by_key = {}
        for row in rows:
            k = key(row)
            if k not in by_key:
                by_key[k] = row
            else:
                if sort_key(row) > sort_key(by_key[k]):
                    by_key[k] = row

        deduped_rows = list(by_key.values())

    if bucket in {"repeat_customers", "nonpackage_all"}:
        def dedupe(rows: List[Dict[str, Any]], key_fields: tuple[str, str], recency_fields: list[str]):
            def key(row: Dict[str, Any]):
                return tuple(row[field] for field in key_fields)

            def recency_key(row: Dict[str, Any]):
                return tuple(row.get(field) or date.min for field in recency_fields)

            by_key: Dict[tuple[str, ...], Dict[str, Any]] = {}
            for row in rows:
                k = key(row)
                if k not in by_key or recency_key(row) >= recency_key(by_key[k]):
                    by_key[k] = row
            return list(by_key.values())

        if bucket == "repeat_customers":
            deduped_rows = dedupe(rows, ("store_code", "mobile_no"), ["run_date"])
        else:
            deduped_rows = dedupe(
                rows,
                ("store_code", "mobile_no"),
                ["run_date", "order_date", "expected_delivery_date", "actual_delivery_date"],
            )

    stmt = insert(model).values(deduped_rows)

    dedupe_keys = spec["dedupe_keys"]
    update_cols = {
        col: stmt.excluded[col]
        for col in deduped_rows[0].keys()
        if col not in dedupe_keys
    }

    stmt = stmt.on_conflict_do_update(
        index_elements=dedupe_keys,
        set_=update_cols,
    )

    result = await session.execute(stmt)

    rowcount = getattr(result, "rowcount", None)
    if not rowcount:
        # SQLAlchemy may report ``rowcount`` as ``None`` (or ``0`` on some drivers)
        # for PostgreSQL upserts, so fall back to the number of attempted rows.
        return len(deduped_rows)
    return rowcount


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
    totals = {"rows": 0}
    row_context = {"run_id": run_id, "run_date": run_date}
    async with session_scope(database_url) as session:
        async with session.begin():
            for batch in _batched(
                _load_csv_rows(bucket, csv_path, logger, row_context=row_context),
                batch_size,
            ):
                affected = await _upsert_batch(session, bucket, batch)
                totals["rows"] += affected

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
        counts={"ingested_rows": totals["rows"]},
        message="ingestion complete",
    )
    return totals
