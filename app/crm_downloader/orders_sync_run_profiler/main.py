from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Sequence

import sqlalchemy as sa

from app.common.date_utils import aware_now, get_timezone, normalize_store_codes
from app.common.db import session_scope
from app.config import config
from app.crm_downloader.config import default_download_dir
from app.crm_downloader.td_orders_sync.main import main as td_orders_sync_main
from app.crm_downloader.uc_orders_sync.main import main as uc_orders_sync_main
from app.dashboard_downloader.db_tables import orders_sync_log, pipelines
from app.dashboard_downloader.json_logger import JsonLogger, get_logger, log_event, new_run_id
from app.dashboard_downloader.run_summary import fetch_summary_for_run, insert_run_summary

PIPELINE_BY_GROUP = {
    "TD": ("td_orders_sync", td_orders_sync_main),
    "UC": ("uc_orders_sync", uc_orders_sync_main),
}

DEFAULT_BACKFILL_DAYS = 90
DEFAULT_WINDOW_DAYS = 90
DEFAULT_OVERLAP_DAYS = 2
DEFAULT_MAX_WORKERS = 4


@dataclass(frozen=True)
class StoreProfile:
    store_code: str
    store_name: str | None
    cost_center: str | None
    sync_config: Mapping[str, Any]
    start_date: date | None


@dataclass(frozen=True)
class StoreRunResult:
    store_code: str
    pipeline_group: str
    pipeline_name: str
    overall_status: str
    window_count: int
    status_counts: dict[str, int]


@asynccontextmanager
async def store_lock(store_code: str) -> Iterable[None]:
    lock_dir = default_download_dir() / "orders_sync_run_profiler_locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"{store_code}.lock"
    handle = open(lock_path, "w", encoding="utf-8")
    try:
        await asyncio.to_thread(fcntl.flock, handle, fcntl.LOCK_EX)
        yield
    finally:
        try:
            await asyncio.to_thread(fcntl.flock, handle, fcntl.LOCK_UN)
        finally:
            handle.close()


def _coerce_dict(raw: Any) -> Mapping[str, Any]:
    if isinstance(raw, Mapping):
        return raw
    if isinstance(raw, str):
        stripped = raw.strip()
        if not stripped:
            return {}
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, Mapping) else {}
    return {}


def _coerce_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}; expected YYYY-MM-DD") from exc


def _normalize_sync_group(value: str) -> str:
    normalized = value.strip().upper()
    if normalized == "ALL":
        return normalized
    if normalized not in PIPELINE_BY_GROUP:
        raise argparse.ArgumentTypeError("sync_group must be TD, UC, or ALL")
    return normalized


async def _load_store_profiles(
    *, logger: JsonLogger, sync_group: str, store_codes: Sequence[str] | None
) -> list[StoreProfile]:
    if not config.database_url:
        log_event(
            logger=logger,
            phase="init",
            status="error",
            message="database_url missing; cannot load store rows",
        )
        return []
    normalized_codes = normalize_store_codes(store_codes or [])
    query_text = """
        SELECT store_code, store_name, cost_center, sync_config, start_date
        FROM store_master
        WHERE sync_orders_flag = TRUE
          AND (is_active IS NULL OR is_active = TRUE)
    """
    if sync_group != "ALL":
        query_text += " AND sync_group = :sync_group"
    if normalized_codes:
        query_text += " AND UPPER(store_code) IN :store_codes"
    query = sa.text(query_text)
    if normalized_codes:
        query = query.bindparams(sa.bindparam("store_codes", expanding=True))
    async with session_scope(config.database_url) as session:
        params: dict[str, Any] = {}
        if sync_group != "ALL":
            params["sync_group"] = sync_group
        if normalized_codes:
            params["store_codes"] = normalized_codes
        result = await session.execute(query, params)
        stores: list[StoreProfile] = []
        for row in result.mappings():
            raw_code = (row.get("store_code") or "").strip()
            if not raw_code:
                log_event(
                    logger=logger,
                    phase="init",
                    status="warn",
                    message="Skipping store with missing store_code",
                    raw_row=dict(row),
                )
                continue
            stores.append(
                StoreProfile(
                    store_code=raw_code.upper(),
                    store_name=row.get("store_name"),
                    cost_center=row.get("cost_center"),
                    sync_config=_coerce_dict(row.get("sync_config")),
                    start_date=_coerce_date(row.get("start_date")),
                )
            )
    log_event(
        logger=logger,
        phase="init",
        message="Loaded store rows",
        sync_group=sync_group,
        store_count=len(stores),
        stores=[store.store_code for store in stores],
    )
    return stores


async def _fetch_pipeline_id(
    *, logger: JsonLogger, database_url: str, pipeline_name: str
) -> int | None:
    try:
        async with session_scope(database_url) as session:
            pipeline_id = (
                await session.execute(sa.select(pipelines.c.id).where(pipelines.c.code == pipeline_name))
            ).scalar_one_or_none()
    except Exception as exc:  # pragma: no cover - defensive
        log_event(
            logger=logger,
            phase="pipeline",
            status="warn",
            message="Failed to fetch pipeline id",
            pipeline_name=pipeline_name,
            error=str(exc),
        )
        return None
    if not pipeline_id:
        log_event(
            logger=logger,
            phase="pipeline",
            status="warn",
            message="Pipeline id not found",
            pipeline_name=pipeline_name,
        )
    return pipeline_id


async def _fetch_last_success_date(
    *, database_url: str, pipeline_id: int, store_code: str
) -> date | None:
    async with session_scope(database_url) as session:
        stmt = (
            sa.select(sa.func.max(orders_sync_log.c.to_date))
            .where(orders_sync_log.c.pipeline_id == pipeline_id)
            .where(orders_sync_log.c.store_code == store_code)
            .where(orders_sync_log.c.status == "success")
        )
        return (await session.execute(stmt)).scalar_one_or_none()


async def _fetch_latest_log_status(
    *, database_url: str, pipeline_id: int, store_code: str, run_id: str
) -> tuple[str | None, str | None]:
    async with session_scope(database_url) as session:
        stmt = (
            sa.select(orders_sync_log.c.status, orders_sync_log.c.error_message)
            .where(orders_sync_log.c.pipeline_id == pipeline_id)
            .where(orders_sync_log.c.store_code == store_code)
            .where(orders_sync_log.c.run_id == run_id)
            .order_by(orders_sync_log.c.id.desc())
            .limit(1)
        )
        row = (await session.execute(stmt)).first()
    if not row:
        return None, None
    status = row[0]
    error_message = row[1]
    status_value = str(status) if status else None
    error_value = str(error_message) if error_message else None
    return status_value, error_value


def _normalize_run_summary_status(summary: Mapping[str, Any] | None) -> str | None:
    if not summary:
        return None
    raw_status = str(summary.get("overall_status") or "").lower()
    if raw_status in {"ok", "success"}:
        return "success"
    if raw_status in {"warning", "warn", "partial"}:
        return "partial"
    if raw_status in {"error", "failed", "fail"}:
        return "failed"
    return None


def _extract_window_outcome_metadata(
    summary: Mapping[str, Any] | None, *, store_code: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not summary:
        return {}, {}
    metrics = _coerce_dict(summary.get("metrics_json"))
    if not metrics:
        return {}, {}
    normalized_code = store_code.upper()
    download_paths: dict[str, Any] = {}
    ingestion_counts: dict[str, Any] = {}

    def _record_counts(label: str, payload: Mapping[str, Any]) -> None:
        counts = {
            "rows_downloaded": payload.get("rows_downloaded"),
            "rows_ingested": payload.get("rows_ingested"),
            "staging_rows": payload.get("staging_rows"),
            "final_rows": payload.get("final_rows"),
        }
        if any(value is not None for value in counts.values()):
            ingestion_counts[label] = counts

    def _record_paths(label: str, payload: Mapping[str, Any]) -> None:
        download_path = payload.get("downloaded_path") or payload.get("download_path")
        filenames = payload.get("filenames") or payload.get("filename")
        if download_path or filenames:
            download_paths[label] = {
                "download_path": download_path,
                "filenames": filenames,
            }

    orders_snapshot = _coerce_dict(metrics.get("orders"))
    orders_store = _coerce_dict(_coerce_dict(orders_snapshot.get("stores")).get(normalized_code))
    if orders_store:
        _record_paths("orders", orders_store)
        _record_counts("orders", orders_store)

    sales_snapshot = _coerce_dict(metrics.get("sales"))
    sales_store = _coerce_dict(_coerce_dict(sales_snapshot.get("stores")).get(normalized_code))
    if sales_store:
        _record_paths("sales", sales_store)
        _record_counts("sales", sales_store)

    stores_summary = _coerce_dict(metrics.get("stores_summary"))
    summary_store = _coerce_dict(_coerce_dict(stores_summary.get("stores")).get(normalized_code))
    if summary_store:
        _record_paths("gst", summary_store)
        row_counts = _coerce_dict(summary_store.get("row_counts"))
        if row_counts:
            uc_counts = {
                "staging_rows": row_counts.get("staging_rows"),
                "final_rows": row_counts.get("final_rows"),
                "staging_inserted": row_counts.get("staging_inserted"),
                "staging_updated": row_counts.get("staging_updated"),
                "final_inserted": row_counts.get("final_inserted"),
                "final_updated": row_counts.get("final_updated"),
            }
            if any(value is not None for value in uc_counts.values()):
                ingestion_counts["gst"] = uc_counts

    return download_paths, ingestion_counts


def _is_uc_date_picker_failure(error_message: str | None) -> bool:
    if not error_message:
        return False
    normalized = error_message.lower()
    return any(
        token in normalized
        for token in (
            "date picker",
            "date-picker",
            "date range",
            "calendar",
        )
    )


def _normalize_window_status(
    *, pipeline_name: str, status: str, error_message: str | None
) -> tuple[str, str]:
    if pipeline_name != "uc_orders_sync":
        return status, ""
    if status == "failed" and _is_uc_date_picker_failure(error_message):
        return "partial", " (date picker failure mapped to partial)"
    return status, ""


def _window_settings(
    *, store: StoreProfile, backfill_days: int | None, window_days: int | None, overlap_days: int | None
) -> tuple[int, int, int]:
    sync_config = store.sync_config
    resolved_backfill = int(
        backfill_days
        or sync_config.get("orders_sync_backfill_days")
        or sync_config.get("sync_backfill_days")
        or DEFAULT_BACKFILL_DAYS
    )
    resolved_window = int(
        window_days
        or sync_config.get("orders_sync_window_days")
        or sync_config.get("sync_window_days")
        or DEFAULT_WINDOW_DAYS
    )
    resolved_overlap = int(
        overlap_days
        or sync_config.get("orders_sync_overlap_days")
        or sync_config.get("sync_overlap_days")
        or DEFAULT_OVERLAP_DAYS
    )
    resolved_backfill = max(1, resolved_backfill)
    resolved_window = max(1, resolved_window)
    resolved_overlap = max(0, min(resolved_overlap, resolved_window - 1))
    return resolved_backfill, resolved_window, resolved_overlap


def _build_windows(
    *, start_date: date, end_date: date, window_days: int, overlap_days: int
) -> list[tuple[date, date]]:
    if start_date > end_date:
        return []
    step_days = max(1, window_days - overlap_days)
    windows: list[tuple[date, date]] = []
    current = start_date
    while current <= end_date:
        window_end = min(current + timedelta(days=window_days - 1), end_date)
        windows.append((current, window_end))
        current = current + timedelta(days=step_days)
    return windows


def _resolve_start_date(
    *,
    end_date: date,
    last_success: date | None,
    overlap_days: int,
    backfill_days: int,
    window_days: int,
    from_date: date | None,
    store_start_date: date | None,
) -> date:
    if from_date:
        return from_date
    if last_success:
        overlap_offset = max(0, overlap_days - 1)
        candidate = last_success - timedelta(days=overlap_offset)
        if store_start_date:
            return max(store_start_date, candidate)
        return candidate
    if store_start_date:
        return store_start_date
    desired_window = max(backfill_days, window_days)
    return end_date - timedelta(days=desired_window - 1)


def _summary_text(
    *, store_code: str, status: str, windows: Sequence[tuple[date, date]], detail_lines: Sequence[str]
) -> str:
    window_lines = [f"- {start.isoformat()} → {end.isoformat()}" for start, end in windows]
    lines = [
        f"Store: {store_code}",
        f"Overall Status: {status}",
        f"Window Count: {len(windows)}",
        "Windows:",
    ]
    lines.extend(window_lines or ["- none"])
    if detail_lines:
        lines.append("")
        lines.append("Details:")
        lines.extend(f"- {line}" for line in detail_lines)
    return "\n".join(lines)


def _init_status_counts() -> dict[str, int]:
    return {status: 0 for status in ("success", "partial", "failed", "skipped")}


def _merge_status_counts(target: dict[str, int], source: Mapping[str, int]) -> None:
    for status, count in source.items():
        target[status] = target.get(status, 0) + int(count)


def _rollup_overall_status(status_counts: Mapping[str, int]) -> str:
    if status_counts.get("failed", 0) > 0:
        return "failed"
    if status_counts.get("partial", 0) > 0:
        return "partial"
    if status_counts.get("success", 0) > 0:
        return "success"
    if status_counts.get("skipped", 0) > 0:
        return "skipped"
    return "success"


async def _run_store_windows(
    *,
    logger: JsonLogger,
    store: StoreProfile,
    pipeline_name: str,
    pipeline_id: int,
    pipeline_fn: Any,
    run_env: str,
    run_id: str,
    backfill_days: int | None,
    window_days: int | None,
    overlap_days: int | None,
    from_date: date | None,
    to_date: date | None,
) -> tuple[str, list[tuple[date, date]], list[str], dict[str, int]]:
    started_at = datetime.now(timezone.utc)
    detail_lines: list[str] = []
    status_counts = _init_status_counts()
    backfill, window_size, overlap = _window_settings(
        store=store,
        backfill_days=backfill_days,
        window_days=window_days,
        overlap_days=overlap_days,
    )
    end_date = to_date or aware_now(get_timezone()).date()
    last_success = await _fetch_last_success_date(
        database_url=config.database_url, pipeline_id=pipeline_id, store_code=store.store_code
    )
    start_date = _resolve_start_date(
        end_date=end_date,
        last_success=last_success,
        overlap_days=overlap,
        backfill_days=backfill,
        window_days=window_size,
        from_date=from_date,
        store_start_date=store.start_date,
    )
    windows = _build_windows(
        start_date=start_date,
        end_date=end_date,
        window_days=window_size,
        overlap_days=overlap,
    )
    if not windows:
        detail_lines.append("No windows to process (start_date after end_date).")
        return "success", windows, detail_lines, status_counts
    log_event(
        logger=logger,
        phase="store",
        message="Computed window plan",
        store_code=store.store_code,
        start_date=start_date,
        end_date=end_date,
        window_days=window_size,
        overlap_days=overlap,
        window_count=len(windows),
    )
    overall_status = "success"
    for index, (window_start, window_end) in enumerate(windows, start=1):
        window_run_id = f"{run_id}_{store.store_code}_{index:03d}"
        status = "skipped"
        status_note = ""
        error_message: str | None = None
        download_paths: dict[str, Any] = {}
        ingestion_counts: dict[str, Any] = {}
        attempts = 0
        for attempt in range(2):
            attempts = attempt + 1
            log_event(
                logger=logger,
                phase="window",
                message="Running orders sync window",
                store_code=store.store_code,
                from_date=window_start,
                to_date=window_end,
                window_index=index,
                window_attempt=attempts,
            )
            await pipeline_fn(
                run_env=run_env,
                run_id=window_run_id,
                from_date=window_start,
                to_date=window_end,
                store_codes=[store.store_code],
                run_orders=True,
                run_sales=True,
            )
            summary = await fetch_summary_for_run(config.database_url, window_run_id)
            summary_status = _normalize_run_summary_status(summary)
            fetched_status, error_message = await _fetch_latest_log_status(
                database_url=config.database_url,
                pipeline_id=pipeline_id,
                store_code=store.store_code,
                run_id=window_run_id,
            )
            status_note = ""
            if not fetched_status:
                if summary_status:
                    fetched_status = summary_status
                    status_note = " (from pipeline run summary)"
                    log_event(
                        logger=logger,
                        phase="window",
                        status="warn",
                        message="orders_sync_log row missing; using pipeline run summary status",
                        store_code=store.store_code,
                        run_id=window_run_id,
                        window_index=index,
                        window_status=summary_status,
                    )
                else:
                    fetched_status = "skipped"
                    status_note = " (missing orders_sync_log row)"
                    log_event(
                        logger=logger,
                        phase="window",
                        status="warn",
                        message="orders_sync_log row missing; continuing without status",
                        store_code=store.store_code,
                        run_id=window_run_id,
                        window_index=index,
                    )
            status = (fetched_status or "skipped").lower()
            if status not in {"success", "partial", "failed", "skipped"}:
                status = "failed"
            status, mapped_note = _normalize_window_status(
                pipeline_name=pipeline_name, status=status, error_message=error_message
            )
            status_note += mapped_note
            download_paths, ingestion_counts = _extract_window_outcome_metadata(
                summary, store_code=store.store_code
            )
            if status in {"failed", "partial"} and attempt == 0:
                log_event(
                    logger=logger,
                    phase="window",
                    status="warn",
                    message="Retrying window after non-success status",
                    store_code=store.store_code,
                    window_index=index,
                    window_status=status,
                    window_attempt=attempts,
                )
                continue
            if attempt > 0:
                status_note += " (after retry)"
            break
        log_event(
            logger=logger,
            phase="window_result",
            message="Window completed",
            store_code=store.store_code,
            pipeline_name=pipeline_name,
            run_id=run_id,
            window_run_id=window_run_id,
            window_index=index,
            from_date=window_start,
            to_date=window_end,
            window_status=status,
            status_note=status_note or None,
            error_message=error_message,
            download_paths=download_paths or None,
            ingestion_counts=ingestion_counts or None,
        )
        detail_lines.append(
            f"{window_start.isoformat()} → {window_end.isoformat()}: {status}{status_note}"
        )
        status_counts[status] = status_counts.get(status, 0) + 1
        if status in {"failed", "partial"}:
            overall_status = status
            log_event(
                logger=logger,
                phase="window",
                status="warn" if status == "partial" else "error",
                message="Stopping further windows after non-success status",
                store_code=store.store_code,
                window_index=index,
                window_status=status,
                window_attempts=attempts,
            )
            break
    finished_at = datetime.now(timezone.utc)
    total_seconds = int((finished_at - started_at).total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    metrics = {
        "store_code": store.store_code,
        "window_count": len(windows),
        "window_days": window_size,
        "overlap_days": overlap,
        "backfill_days": backfill,
        "elapsed_seconds": total_seconds,
        "windows": [{"from": s.isoformat(), "to": e.isoformat()} for s, e in windows],
    }
    phases = {
        "window": {
            "ok": sum("success" in line for line in detail_lines),
            "warning": sum("partial" in line for line in detail_lines),
            "error": sum("failed" in line for line in detail_lines),
        }
    }
    summary_record = {
        "pipeline_name": pipeline_name,
        "run_id": f"{run_id}_{store.store_code}",
        "run_env": run_env,
        "started_at": started_at,
        "finished_at": finished_at,
        "total_time_taken": f"{hours:02d}:{minutes:02d}:{seconds:02d}",
        "report_date": windows[0][0] if windows else None,
        "overall_status": overall_status,
        "summary_text": _summary_text(
            store_code=store.store_code, status=overall_status, windows=windows, detail_lines=detail_lines
        ),
        "phases_json": phases,
        "metrics_json": metrics,
    }
    await insert_run_summary(config.database_url, summary_record)
    return overall_status, windows, detail_lines, status_counts


async def _process_store(
    *,
    logger: JsonLogger,
    store: StoreProfile,
    pipeline_group: str,
    pipeline_name: str,
    pipeline_id: int,
    pipeline_fn: Any,
    run_env: str,
    run_id: str,
    backfill_days: int | None,
    window_days: int | None,
    overlap_days: int | None,
    from_date: date | None,
    to_date: date | None,
) -> StoreRunResult:
    async with store_lock(store.store_code):
        overall_status, windows, _detail_lines, status_counts = await _run_store_windows(
            logger=logger,
            store=store,
            pipeline_name=pipeline_name,
            pipeline_id=pipeline_id,
            pipeline_fn=pipeline_fn,
            run_env=run_env,
            run_id=run_id,
            backfill_days=backfill_days,
            window_days=window_days,
            overlap_days=overlap_days,
            from_date=from_date,
            to_date=to_date,
        )
    return StoreRunResult(
        store_code=store.store_code,
        pipeline_group=pipeline_group,
        pipeline_name=pipeline_name,
        overall_status=overall_status,
        window_count=len(windows),
        status_counts=status_counts,
    )


async def main(
    *,
    sync_group: str | None,
    store_codes: Sequence[str] | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
    max_workers: int = DEFAULT_MAX_WORKERS,
    backfill_days: int | None = None,
    window_days: int | None = None,
    overlap_days: int | None = None,
    run_env: str | None = None,
    run_id: str | None = None,
) -> None:
    resolved_env = run_env or config.run_env
    resolved_run_id = run_id or new_run_id()
    logger = get_logger(run_id=resolved_run_id)
    resolved_sync_group = _normalize_sync_group(sync_group or "ALL")
    if not config.database_url:
        log_event(
            logger=logger,
            phase="init",
            status="error",
            message="database_url missing; exiting",
        )
        return
    group_items = (
        PIPELINE_BY_GROUP.items()
        if resolved_sync_group == "ALL"
        else [(resolved_sync_group, PIPELINE_BY_GROUP[resolved_sync_group])]
    )

    async def _process_group(
        group: str, pipeline_name: str, pipeline_fn: Any
    ) -> list[StoreRunResult]:
        pipeline_id = await _fetch_pipeline_id(
            logger=logger, database_url=config.database_url, pipeline_name=pipeline_name
        )
        if not pipeline_id:
            return []
        stores = await _load_store_profiles(
            logger=logger, sync_group=group, store_codes=store_codes
        )
        if not stores:
            log_event(
                logger=logger,
                phase="init",
                status="warn",
                message="No stores found for sync_group",
                sync_group=group,
            )
            return []
        group_max_workers = 1 if group == "UC" else max_workers
        semaphore = asyncio.Semaphore(max(1, group_max_workers))

        async def _guarded(store: StoreProfile) -> StoreRunResult:
            async with semaphore:
                return await _process_store(
                    logger=logger,
                    store=store,
                    pipeline_group=group,
                    pipeline_name=pipeline_name,
                    pipeline_id=pipeline_id,
                    pipeline_fn=pipeline_fn,
                    run_env=resolved_env,
                    run_id=resolved_run_id,
                    backfill_days=backfill_days,
                    window_days=window_days,
                    overlap_days=overlap_days,
                    from_date=from_date,
                    to_date=to_date,
                )

        return await asyncio.gather(*[_guarded(store) for store in stores])

    group_results = await asyncio.gather(
        *[
            _process_group(group, pipeline_name, pipeline_fn)
            for group, (pipeline_name, pipeline_fn) in group_items
        ]
    )
    all_results = [result for group in group_results for result in group]
    total_status_counts = _init_status_counts()
    pipeline_totals: dict[str, dict[str, Any]] = {}
    store_totals: dict[str, dict[str, Any]] = {}
    total_windows = 0
    for result in all_results:
        total_windows += result.window_count
        _merge_status_counts(total_status_counts, result.status_counts)
        pipeline_entry = pipeline_totals.setdefault(
            result.pipeline_group,
            {"window_count": 0, "status_counts": _init_status_counts()},
        )
        pipeline_entry["window_count"] += result.window_count
        _merge_status_counts(pipeline_entry["status_counts"], result.status_counts)
        store_totals[result.store_code] = {
            "pipeline_group": result.pipeline_group,
            "pipeline_name": result.pipeline_name,
            "overall_status": result.overall_status,
            "window_count": result.window_count,
            "status_counts": result.status_counts,
        }
    overall_status = _rollup_overall_status(total_status_counts)
    log_event(
        logger=logger,
        phase="summary",
        message="Orders sync profiler summary",
        run_id=resolved_run_id,
        run_env=resolved_env,
        total_windows=total_windows,
        status_counts=total_status_counts,
        pipeline_totals=pipeline_totals,
        store_totals=store_totals,
        overall_status=overall_status,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run orders sync profiler with windowed backfill.")
    parser.add_argument("--sync-group", default="ALL", type=_normalize_sync_group)
    parser.add_argument("--store-code", action="append", dest="store_codes")
    parser.add_argument("--from-date", type=_parse_date)
    parser.add_argument("--to-date", type=_parse_date)
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS)
    parser.add_argument("--backfill-days", type=int)
    parser.add_argument("--window-days", type=int)
    parser.add_argument("--overlap-days", type=int)
    parser.add_argument("--run-env")
    parser.add_argument("--run-id")
    return parser


def _main() -> None:
    args = _build_parser().parse_args()
    asyncio.run(
        main(
            sync_group=args.sync_group,
            store_codes=args.store_codes,
            from_date=args.from_date,
            to_date=args.to_date,
            max_workers=args.max_workers,
            backfill_days=args.backfill_days,
            window_days=args.window_days,
            overlap_days=args.overlap_days,
            run_env=args.run_env,
            run_id=args.run_id,
        )
    )


if __name__ == "__main__":
    _main()
