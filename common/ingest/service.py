from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Dict, Iterable, List

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from simplify_downloader.dashboard_downloader.json_logger import JsonLogger, log_event

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


def _looks_like_html(csv_path: Path) -> bool:
    """Return True when the file appears to contain an HTML document."""

    try:
        with csv_path.open("r", encoding="utf-8", errors="ignore") as handle:
            sample = handle.read(2048)
    except OSError:
        return False

    lowered = sample.lower()
    return "<html" in lowered[:512] or "<!doctype html" in lowered[:512]


def _load_csv_rows(bucket: str, csv_path: Path) -> Iterable[Dict[str, Any]]:
    if not csv_path.exists():
        return []

    if _looks_like_html(csv_path):
        return []

    with csv_path.open("r", newline="", encoding="utf-8", errors="ignore") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return []
        header_map = normalize_headers(reader.fieldnames)
        for raw_row in reader:
            try:
                yield coerce_csv_row(bucket, raw_row, header_map)
            except ValueError:
                continue


async def _upsert_batch(
    session: AsyncSession,
    bucket: str,
    rows: List[Dict[str, Any]],
) -> int:
    """Upsert a batch of rows and return the number of affected rows."""
    if not rows:
        return 0

    model = BUCKET_MODEL_MAP[bucket]
    spec = MERGE_BUCKET_DB_SPECS[bucket]

    stmt = insert(model).values(rows)

    dedupe_keys = spec["dedupe_keys"]
    update_cols = {
        col: stmt.excluded[col]
        for col in rows[0].keys()
        if col not in dedupe_keys
    }

    stmt = stmt.on_conflict_do_update(
        index_elements=dedupe_keys,
        set_=update_cols,
    )

    result = await session.execute(stmt)

    rowcount = getattr(result, "rowcount", None)
    return rowcount or 0


async def ingest_bucket(
    *,
    bucket: str,
    csv_path: Path,
    batch_size: int,
    database_url: str,
    logger: JsonLogger,
) -> Dict[str, Any]:
    totals = {"rows": 0}
    async with session_scope(database_url) as session:
        async with session.begin():
            for batch in _batched(_load_csv_rows(bucket, csv_path), batch_size):
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
