from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError

from app.common.date_utils import aware_now, get_timezone, normalize_store_codes
from app.common.db import session_scope
from app.config import config
from app.crm_downloader.config import default_download_dir
from app.crm_downloader.orders_sync_window import (
    fetch_last_success_window_end,
    resolve_orders_sync_start_date,
    resolve_window_settings,
)
from app.crm_downloader.td_orders_sync.main import main as td_orders_sync_main
from app.crm_downloader.uc_orders_sync.main import main as uc_orders_sync_main
from app.dashboard_downloader.db_tables import orders_sync_log, pipelines
from app.dashboard_downloader.json_logger import JsonLogger, get_logger, log_event, new_run_id
from app.dashboard_downloader.notifications import send_notifications_for_run
from app.dashboard_downloader.run_summary import fetch_summary_for_run, insert_run_summary

PIPELINE_NAME = "orders_sync_run_profiler"

PIPELINE_BY_GROUP = {
    "TD": ("td_orders_sync", td_orders_sync_main),
    "UC": ("uc_orders_sync", uc_orders_sync_main),
}

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
    cost_center: str | None
    overall_status: str
    window_count: int
    windows: list[tuple[date, date]]
    status_counts: dict[str, int]
    window_audit: list[dict[str, Any]]
    ingestion_totals: dict[str, int]
    row_facts: dict[str, list[dict[str, Any]]]


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


def _parse_window_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
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


def _env_flag(name: str) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return False
    return str(raw).strip().lower() not in {"", "0", "false", "no"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


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


async def _fetch_latest_log_row(
    *, database_url: str, pipeline_id: int, store_code: str, run_id: str
) -> Mapping[str, Any] | None:
    async with session_scope(database_url) as session:
        stmt = (
            sa.select(orders_sync_log)
            .where(orders_sync_log.c.pipeline_id == pipeline_id)
            .where(orders_sync_log.c.store_code == store_code)
            .where(orders_sync_log.c.run_id == run_id)
            .order_by(orders_sync_log.c.id.desc())
            .limit(1)
        )
        row = (await session.execute(stmt)).mappings().first()
    return dict(row) if row else None


def _init_row_facts() -> dict[str, list[dict[str, Any]]]:
    return {
        "warning_rows": [],
        "dropped_rows": [],
        "edited_rows": [],
        "error_rows": [],
    }


def _merge_row_facts(
    target: dict[str, list[dict[str, Any]]], source: Mapping[str, Sequence[Mapping[str, Any]]]
) -> None:
    for key in ("warning_rows", "dropped_rows", "edited_rows", "error_rows"):
        rows = source.get(key) or []
        if isinstance(rows, Sequence):
            target[key].extend(list(rows))


def _rows_with_store_metadata(
    rows: Iterable[Mapping[str, Any]] | None,
    *,
    store_code: str | None,
) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []
    for row in rows or []:
        if not isinstance(row, Mapping):
            continue
        data = dict(row)
        if store_code and not data.get("store_code"):
            data["store_code"] = store_code
        order_number = data.get("order_number")
        if not order_number:
            values = data.get("values") or {}
            if isinstance(values, Mapping):
                for key in ("order_number", "Order Number", "Order No.", "Booking ID"):
                    if values.get(key):
                        order_number = values.get(key)
                        break
        if order_number not in (None, "") and not data.get("order_number"):
            data["order_number"] = str(order_number)
        prepared.append(data)
    return prepared


def _extract_row_facts_from_summary(summary: Mapping[str, Any] | None) -> dict[str, list[dict[str, Any]]]:
    if not summary:
        return _init_row_facts()
    metrics = _coerce_dict(summary.get("metrics_json"))
    payload = _coerce_dict(metrics.get("notification_payload"))
    stores = payload.get("stores") or []
    orders_payload = _coerce_dict(metrics.get("orders"))
    sales_payload = _coerce_dict(metrics.get("sales"))
    orders_stores = _coerce_dict(orders_payload.get("stores"))
    sales_stores = _coerce_dict(sales_payload.get("stores"))
    stores_payload = _coerce_dict(metrics.get("stores"))
    outcome_stores = _coerce_dict(stores_payload.get("outcomes"))
    extracted = _init_row_facts()
    row_keys = ("warning_rows", "dropped_rows", "edited_rows", "error_rows")
    payload_rows_by_store: dict[str, dict[str, bool]] = {}

    def _is_row_list(value: Any) -> bool:
        return isinstance(value, Sequence) and not isinstance(value, (str, bytes, Mapping))

    def _record_payload_rows(store_code: str | None, key: str, rows: Sequence[Mapping[str, Any]] | None) -> None:
        if not store_code:
            return
        store_map = payload_rows_by_store.setdefault(store_code, {row_key: False for row_key in row_keys})
        if rows and _is_row_list(rows):
            store_map[key] = True

    def _add_rows(
        key: str,
        rows: Sequence[Mapping[str, Any]] | None,
        *,
        store_code: str | None,
        track_payload: bool = False,
    ) -> None:
        prepared = _rows_with_store_metadata(rows, store_code=store_code)
        if prepared:
            extracted[key].extend(prepared)
        if track_payload:
            _record_payload_rows(store_code, key, rows)

    def _should_fallback(store_code: str | None, key: str) -> bool:
        if not store_code:
            return True
        store_map = payload_rows_by_store.get(store_code)
        if not store_map:
            return True
        return not store_map.get(key)

    for store in stores:
        if not isinstance(store, Mapping):
            continue
        store_code = store.get("store_code")
        orders = store.get("orders")
        sales = store.get("sales")
        if orders is not None or sales is not None:
            orders = _coerce_dict(orders)
            sales = _coerce_dict(sales)
            _add_rows("warning_rows", orders.get("warning_rows"), store_code=store_code, track_payload=True)
            _add_rows("warning_rows", sales.get("warning_rows"), store_code=store_code, track_payload=True)
            _add_rows("dropped_rows", orders.get("dropped_rows"), store_code=store_code, track_payload=True)
            _add_rows("dropped_rows", sales.get("dropped_rows"), store_code=store_code, track_payload=True)
            _add_rows("edited_rows", orders.get("edited_rows"), store_code=store_code, track_payload=True)
            _add_rows("edited_rows", sales.get("edited_rows"), store_code=store_code, track_payload=True)
            _add_rows("error_rows", orders.get("error_rows"), store_code=store_code, track_payload=True)
            _add_rows("error_rows", sales.get("error_rows"), store_code=store_code, track_payload=True)
        else:
            _add_rows("warning_rows", store.get("warning_rows"), store_code=store_code, track_payload=True)
            _add_rows("dropped_rows", store.get("dropped_rows"), store_code=store_code, track_payload=True)
            _add_rows("edited_rows", store.get("edited_rows"), store_code=store_code, track_payload=True)
            _add_rows("error_rows", store.get("error_rows"), store_code=store_code, track_payload=True)

    for store_code, store_report in orders_stores.items():
        store_code = str(store_code)
        if not isinstance(store_report, Mapping):
            continue
        if not _should_fallback(store_code, "warning_rows"):
            warning_rows = None
        else:
            warning_rows = store_report.get("warning_rows")
        if not _should_fallback(store_code, "dropped_rows"):
            dropped_rows = None
        else:
            dropped_rows = store_report.get("dropped_rows")
        if not _should_fallback(store_code, "edited_rows"):
            edited_rows = None
        else:
            edited_rows = store_report.get("edited_rows")
        if not _should_fallback(store_code, "error_rows"):
            error_rows = None
        else:
            error_rows = store_report.get("error_rows")
        _add_rows("warning_rows", warning_rows, store_code=store_code)
        _add_rows("dropped_rows", dropped_rows, store_code=store_code)
        _add_rows("edited_rows", edited_rows, store_code=store_code)
        _add_rows("error_rows", error_rows, store_code=store_code)

    for store_code, store_report in sales_stores.items():
        store_code = str(store_code)
        if not isinstance(store_report, Mapping):
            continue
        if not _should_fallback(store_code, "warning_rows"):
            warning_rows = None
        else:
            warning_rows = store_report.get("warning_rows")
        if not _should_fallback(store_code, "dropped_rows"):
            dropped_rows = None
        else:
            dropped_rows = store_report.get("dropped_rows")
        if not _should_fallback(store_code, "edited_rows"):
            edited_rows = None
        else:
            edited_rows = store_report.get("edited_rows")
        if not _should_fallback(store_code, "error_rows"):
            error_rows = None
        else:
            error_rows = store_report.get("error_rows")
        _add_rows("warning_rows", warning_rows, store_code=store_code)
        _add_rows("dropped_rows", dropped_rows, store_code=store_code)
        _add_rows("edited_rows", edited_rows, store_code=store_code)
        _add_rows("error_rows", error_rows, store_code=store_code)

    for store_code, outcome in outcome_stores.items():
        store_code = str(store_code)
        if not isinstance(outcome, Mapping):
            continue
        if not _should_fallback(store_code, "warning_rows"):
            warning_rows = None
        else:
            warning_rows = outcome.get("warning_rows")
        if not _should_fallback(store_code, "dropped_rows"):
            dropped_rows = None
        else:
            dropped_rows = outcome.get("dropped_rows")
        if not _should_fallback(store_code, "edited_rows"):
            edited_rows = None
        else:
            edited_rows = outcome.get("edited_rows")
        if not _should_fallback(store_code, "error_rows"):
            error_rows = None
        else:
            error_rows = outcome.get("error_rows")
        _add_rows("warning_rows", warning_rows, store_code=store_code)
        _add_rows("dropped_rows", dropped_rows, store_code=store_code)
        _add_rows("edited_rows", edited_rows, store_code=store_code)
        _add_rows("error_rows", error_rows, store_code=store_code)

    return extracted


def _resolve_missing_window_status(store_summary: Mapping[str, Any]) -> str:
    status = str(store_summary.get("overall_status") or "").lower()
    if status in {"error", "failed", "failure"}:
        return "failed"
    return "skipped"


def _is_foreign_key_violation(exc: Exception) -> bool:
    if not isinstance(exc, IntegrityError):
        return False
    orig = exc.orig
    if getattr(orig, "pgcode", None) == "23503":
        return True
    return "foreign key" in str(exc).lower()


async def _persist_missing_windows_log_rows(
    *,
    logger: JsonLogger,
    run_id: str,
    fallback_run_id: str | None,
    run_env: str,
    missing_windows: Mapping[str, Sequence[Mapping[str, str]]],
    store_totals: Mapping[str, Mapping[str, Any]],
) -> None:
    if not config.database_url or not missing_windows:
        return
    pipeline_ids: dict[str, int] = {}
    pending_rows: list[dict[str, Any]] = []
    for store_code, windows in missing_windows.items():
        if not windows:
            continue
        store_summary = store_totals.get(store_code) or {}
        pipeline_name = store_summary.get("pipeline_name")
        if not pipeline_name:
            log_event(
                logger=logger,
                phase="summary",
                status="warn",
                message="Missing pipeline name for missing window log insert",
                store_code=store_code,
            )
            continue
        pipeline_id = pipeline_ids.get(pipeline_name)
        if pipeline_id is None:
            pipeline_id = await _fetch_pipeline_id(
                logger=logger, database_url=config.database_url, pipeline_name=pipeline_name
            )
            if not pipeline_id:
                continue
            pipeline_ids[pipeline_name] = pipeline_id
        status = _resolve_missing_window_status(store_summary)
        error_message = (
            "Missing window detected by orders sync profiler (failed)"
            if status == "failed"
            else "Missing window detected by orders sync profiler"
        )
        cost_center = store_summary.get("cost_center")
        window_run_id = store_summary.get("run_id") or run_id
        for window in windows:
            from_date = _parse_window_date(window.get("from_date"))
            to_date = _parse_window_date(window.get("to_date"))
            if not from_date or not to_date:
                log_event(
                    logger=logger,
                    phase="summary",
                    status="warn",
                    message="Skipping missing window log insert due to invalid date range",
                    store_code=store_code,
                    from_date=window.get("from_date"),
                    to_date=window.get("to_date"),
                )
                continue
            pending_rows.append(
                {
                    "pipeline_id": pipeline_id,
                    "run_id": window_run_id,
                    "run_env": run_env,
                    "cost_center": cost_center,
                    "store_code": store_code,
                    "from_date": from_date,
                    "to_date": to_date,
                    "status": status,
                    "attempt_no": 1,
                    "error_message": error_message,
                    "created_at": sa.func.now(),
                    "updated_at": sa.func.now(),
                }
            )
    if not pending_rows:
        return
    try:
        async with session_scope(config.database_url) as session:
            for row in pending_rows:
                await session.execute(
                    pg_insert(orders_sync_log)
                    .values(**row)
                    .on_conflict_do_update(
                        index_elements=(
                            "pipeline_id",
                            "store_code",
                            "from_date",
                            "to_date",
                            "run_id",
                        ),
                        set_={
                            "status": row["status"],
                            "error_message": row.get("error_message"),
                            "updated_at": sa.func.now(),
                            "run_env": run_env,
                            "cost_center": row.get("cost_center"),
                        },
                    )
                )
            await session.commit()
        log_event(
            logger=logger,
            phase="summary",
            status="info",
            message="Inserted missing window log rows",
            run_id=run_id,
            total=len(pending_rows),
        )
    except IntegrityError as exc:
        if fallback_run_id and _is_foreign_key_violation(exc):
            should_retry = any(row.get("run_id") != fallback_run_id for row in pending_rows)
            if not should_retry:
                log_event(
                    logger=logger,
                    phase="summary",
                    status="error",
                    message="Foreign key violation inserting missing window log rows",
                    run_id=run_id,
                    error=str(exc),
                )
                return
            fallback_rows = [{**row, "run_id": fallback_run_id} for row in pending_rows]
            log_event(
                logger=logger,
                phase="summary",
                status="warn",
                message="Foreign key violation inserting missing windows; retrying with fallback run_id",
                run_id=run_id,
                fallback_run_id=fallback_run_id,
                missing_windows=missing_windows,
            )
            try:
                async with session_scope(config.database_url) as session:
                    for row in fallback_rows:
                        await session.execute(
                            pg_insert(orders_sync_log)
                            .values(**row)
                            .on_conflict_do_update(
                                index_elements=(
                                    "pipeline_id",
                                    "store_code",
                                    "from_date",
                                    "to_date",
                                    "run_id",
                                ),
                                set_={
                                    "status": row["status"],
                                    "error_message": row.get("error_message"),
                                    "updated_at": sa.func.now(),
                                    "run_env": run_env,
                                    "cost_center": row.get("cost_center"),
                                },
                            )
                        )
                    await session.commit()
                log_event(
                    logger=logger,
                    phase="summary",
                    status="info",
                    message="Inserted missing window log rows with fallback run_id",
                    run_id=fallback_run_id,
                    total=len(fallback_rows),
                )
                return
            except Exception as retry_exc:  # pragma: no cover - defensive
                log_event(
                    logger=logger,
                    phase="summary",
                    status="error",
                    message="Failed to insert missing window log rows with fallback run_id",
                    run_id=fallback_run_id,
                    error=str(retry_exc),
                )
                return
        log_event(
            logger=logger,
            phase="summary",
            status="error",
            message="Failed to insert missing window log rows",
            run_id=run_id,
            error=str(exc),
        )
    except Exception as exc:  # pragma: no cover - defensive
        log_event(
            logger=logger,
            phase="summary",
            status="error",
            message="Failed to insert missing window log rows",
            run_id=run_id,
            error=str(exc),
        )


def _extract_window_download_paths(
    summary: Mapping[str, Any] | None, *, store_code: str
) -> dict[str, Any]:
    if not summary:
        return {}
    metrics = _coerce_dict(summary.get("metrics_json"))
    if not metrics:
        return {}
    normalized_code = store_code.upper()
    download_paths: dict[str, Any] = {}

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

    sales_snapshot = _coerce_dict(metrics.get("sales"))
    sales_store = _coerce_dict(_coerce_dict(sales_snapshot.get("stores")).get(normalized_code))
    if sales_store:
        _record_paths("sales", sales_store)

    stores_summary = _coerce_dict(metrics.get("stores_summary"))
    summary_store = _coerce_dict(_coerce_dict(stores_summary.get("stores")).get(normalized_code))
    if summary_store:
        _record_paths("gst", summary_store)

    return download_paths


def _first_mapping(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    for value in payload.values():
        if isinstance(value, Mapping):
            return value
    return {}


def _build_uc_window_log(
    *,
    download_paths: Mapping[str, Any],
    ingestion_counts: Mapping[str, Any],
    error_message: str | None,
) -> dict[str, Any]:
    path_payload = _coerce_dict(download_paths.get("gst"))
    if not path_payload:
        path_payload = _first_mapping(download_paths)
    download_path = path_payload.get("download_path")

    counts_payload = _coerce_dict(ingestion_counts.get("primary"))
    if not counts_payload:
        counts_payload = _coerce_dict(ingestion_counts.get("gst"))
    if not counts_payload:
        counts_payload = _first_mapping(ingestion_counts)
    staging_rows = counts_payload.get("staging_rows")
    final_rows = counts_payload.get("final_rows")
    if final_rows is None:
        final_rows = counts_payload.get("rows_ingested")

    ingest_occurred = staging_rows is not None or final_rows is not None
    ingest_success = bool(ingest_occurred)
    failure_reason = None
    if not ingest_occurred:
        if not download_path:
            failure_reason = error_message or "Download did not complete"
        else:
            failure_reason = error_message or "Ingestion did not run"

    return {
        "download_path": download_path,
        "staging_rows": staging_rows,
        "final_rows": final_rows,
        "ingest_success": ingest_success,
        "ingest_failure_reason": failure_reason,
    }


def _extract_ingestion_counts_from_log(
    log_row: Mapping[str, Any], *, pipeline_name: str
) -> dict[str, Any]:
    def _extract_metrics(prefix: str) -> dict[str, Any]:
        return {
            "rows_downloaded": log_row.get(f"{prefix}_rows_downloaded"),
            "rows_ingested": log_row.get(f"{prefix}_rows_ingested"),
            "staging_rows": log_row.get(f"{prefix}_staging_rows"),
            "staging_inserted": log_row.get(f"{prefix}_staging_inserted"),
            "staging_updated": log_row.get(f"{prefix}_staging_updated"),
            "final_inserted": log_row.get(f"{prefix}_final_inserted"),
            "final_updated": log_row.get(f"{prefix}_final_updated"),
        }

    primary = _extract_metrics("primary")
    secondary = _extract_metrics("secondary")
    if pipeline_name == "uc_orders_sync":
        secondary = {**{key: None for key in primary}, "label": "not applicable"}
    return {"primary": primary, "secondary": secondary}


def _normalize_ingestion_metrics(metrics: Mapping[str, Any] | None) -> dict[str, Any]:
    normalized = {
        "rows_downloaded": None,
        "rows_ingested": None,
        "staging_rows": None,
        "staging_inserted": None,
        "staging_updated": None,
        "final_inserted": None,
        "final_updated": None,
    }
    if metrics:
        normalized.update(metrics)
    return normalized


def _prefix_metrics(prefix: str, metrics: Mapping[str, Any]) -> dict[str, Any]:
    return {f"{prefix}{key}": value for key, value in metrics.items()}


def _has_positive_metric(metrics: Mapping[str, Any]) -> bool:
    for key in (
        "rows_downloaded",
        "rows_ingested",
        "staging_rows",
        "staging_inserted",
        "staging_updated",
        "final_inserted",
        "final_updated",
    ):
        value = _coerce_int(metrics.get(key))
        if value is not None and value > 0:
            return True
    return False


def _has_positive_ingestion_rows(ingestion_counts: Mapping[str, Any]) -> bool:
    for key in ("primary", "secondary"):
        payload = _coerce_dict(ingestion_counts.get(key))
        if payload and _has_positive_metric(payload):
            return True
    return False


def _normalize_window_status(
    *, pipeline_name: str, status: str, error_message: str | None
) -> tuple[str, str]:
    if status == "success_with_warnings":
        return "success_with_warnings", " (success with warnings)"
    return status, ""


def _has_explicit_stop_condition(*messages: str | None) -> bool:
    tokens = ("stop requested", "explicit stop", "stop signal")
    combined = " ".join(message for message in messages if message).lower()
    return any(token in combined for token in tokens)


def _should_stop_after_window(
    *, status: str, error_message: str | None, status_note: str
) -> bool:
    if status == "failed":
        return True
    return _has_explicit_stop_condition(error_message, status_note)


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
    return {status: 0 for status in ("success", "success_with_warnings", "partial", "failed", "skipped")}


def _merge_status_counts(target: dict[str, int], source: Mapping[str, int]) -> None:
    for status, count in source.items():
        target[status] = target.get(status, 0) + int(count)


def _window_warning_entries(store_code: str, status_counts: Mapping[str, int]) -> list[str]:
    warning_windows = int(status_counts.get("success_with_warnings", 0) or 0)
    if warning_windows <= 0:
        return []
    return [
        f"WINDOW_WARNINGS: {store_code} had {warning_windows} window(s) completed with warnings"
    ]


def _init_ingestion_totals() -> dict[str, int]:
    return {
        "rows_downloaded": 0,
        "rows_ingested": 0,
        "staging_rows": 0,
        "final_rows": 0,
        "staging_inserted": 0,
        "staging_updated": 0,
        "final_inserted": 0,
        "final_updated": 0,
    }


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _accumulate_ingestion_totals(
    target: dict[str, int], ingestion_counts: Mapping[str, Any]
) -> dict[str, int]:
    totals = _init_ingestion_totals()
    for payload in ingestion_counts.values():
        if not isinstance(payload, Mapping):
            continue
        for key in totals:
            value = _coerce_int(payload.get(key))
            if value is not None:
                totals[key] += value
    for key, value in totals.items():
        target[key] = target.get(key, 0) + value
    return totals


def _rollup_overall_status(status_counts: Mapping[str, int]) -> str:
    if status_counts.get("failed", 0) > 0:
        return "failed"
    if status_counts.get("partial", 0) > 0:
        return "partial"
    if status_counts.get("success_with_warnings", 0) > 0:
        return "success_with_warnings"
    if status_counts.get("success", 0) > 0:
        return "success"
    if status_counts.get("skipped", 0) > 0:
        return "skipped"
    return "success"


UNIFIED_METRIC_FIELDS = (
    "rows_downloaded",
    "rows_ingested",
    "staging_rows",
    "staging_inserted",
    "staging_updated",
    "final_inserted",
    "final_updated",
)


def _build_unified_metrics(metrics: Mapping[str, Any] | None) -> dict[str, Any]:
    if not metrics:
        return {field: None for field in UNIFIED_METRIC_FIELDS}
    payload = {field: _coerce_int(metrics.get(field)) for field in UNIFIED_METRIC_FIELDS}
    if "label" in metrics:
        payload["label"] = metrics.get("label")
    if payload.get("rows_ingested") is None:
        for candidate in ("final_rows", "staging_rows"):
            fallback = _coerce_int(metrics.get(candidate))
            if fallback is not None:
                payload["rows_ingested"] = fallback
                break
    return payload


def _sum_unified_metrics(totals: dict[str, int], metrics: Mapping[str, Any]) -> None:
    for field in UNIFIED_METRIC_FIELDS:
        value = _coerce_int(metrics.get(field))
        if value is not None:
            totals[field] = totals.get(field, 0) + value


def _build_window_summary(
    total_windows: int, missing_windows: Mapping[str, Sequence[Mapping[str, str]]]
) -> dict[str, Any]:
    missing_count = sum(len(entries) for entries in missing_windows.values())
    return {
        "expected_windows": total_windows,
        "completed_windows": max(0, total_windows - missing_count),
        "missing_windows": missing_count,
        "missing_store_codes": sorted(missing_windows.keys()),
    }


def _format_unified_metrics(metrics: Mapping[str, Any]) -> str:
    inserted = metrics.get("final_inserted")
    if inserted is None:
        inserted = metrics.get("staging_inserted")
    updated = metrics.get("final_updated")
    if updated is None:
        updated = metrics.get("staging_updated")
    parts = [
        f"rows_downloaded={metrics.get('rows_downloaded')}",
        f"rows_ingested={metrics.get('rows_ingested')}",
        f"inserted={inserted}",
        f"updated={updated}",
    ]
    label = metrics.get("label")
    if label:
        parts.append(f"label={label}")
    return ", ".join(parts)


def _build_profiler_summary_text(
    *,
    run_id: str,
    run_env: str,
    started_at: datetime,
    finished_at: datetime,
    overall_status: str,
    store_entries: Sequence[Mapping[str, Any]],
    window_summary: Mapping[str, Any],
    warnings: Sequence[str],
) -> str:
    total_seconds = max(0, int((finished_at - started_at).total_seconds()))
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    lines = [
        "Orders Sync Profiler Run Summary",
        f"Run ID: {run_id}",
        f"Env: {run_env}",
        f"Started: {started_at.isoformat()}",
        f"Finished: {finished_at.isoformat()}",
        f"Total Duration: {hours:02d}:{minutes:02d}:{seconds:02d}",
        f"Overall Status: {overall_status}",
        (
            "Windows Completed: "
            f"{window_summary.get('completed_windows', 0)} / {window_summary.get('expected_windows', 0)}"
        ),
        f"Missing Windows: {window_summary.get('missing_windows', 0)}",
        "",
        "Per Store Summary:",
    ]
    if not store_entries:
        lines.append("- (none)")
    for entry in store_entries:
        store_code = entry.get("store_code") or "UNKNOWN"
        status = entry.get("status") or "unknown"
        window_count = entry.get("window_count") or 0
        primary_metrics = entry.get("primary_metrics") or {}
        secondary_metrics = entry.get("secondary_metrics") or {}
        lines.append(f"- {store_code} ({entry.get('pipeline_name')}) — {status}")
        lines.append(f"  window_count: {window_count}")
        lines.append(f"  primary_metrics: {_format_unified_metrics(primary_metrics)}")
        lines.append(f"  secondary_metrics: {_format_unified_metrics(secondary_metrics)}")
        if entry.get("status_conflict_count"):
            lines.append(
                f"  warning: {entry['status_conflict_count']} window(s) skipped but rows present"
            )
    lines.append("")
    lines.append("Warnings:")
    if warnings:
        lines.extend(f"- {warning}" for warning in warnings)
    else:
        lines.append("- None.")
    return "\n".join(lines)


def _build_profiler_notification_payload(
    *,
    run_id: str,
    run_env: str,
    started_at: datetime,
    finished_at: datetime,
    overall_status: str,
    store_entries: Sequence[Mapping[str, Any]],
    window_summary: Mapping[str, Any],
    warnings: Sequence[str],
    total_time_taken: str,
    row_facts: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "run_env": run_env,
        "overall_status": overall_status,
        "stores": list(store_entries),
        "window_summary": dict(window_summary),
        "warnings": list(warnings),
        "row_facts": dict(row_facts),
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "total_time_taken": total_time_taken,
    }


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
) -> tuple[
    str,
    list[tuple[date, date]],
    list[str],
    dict[str, int],
    list[dict[str, Any]],
    dict[str, int],
    dict[str, list[dict[str, Any]]],
]:
    started_at = datetime.now(timezone.utc)
    detail_lines: list[str] = []
    status_counts = _init_status_counts()
    window_audit: list[dict[str, Any]] = []
    ingestion_totals = _init_ingestion_totals()
    row_facts = _init_row_facts()
    backfill, window_size, overlap = resolve_window_settings(
        sync_config=store.sync_config,
        backfill_days=backfill_days,
        window_days=window_days,
        overlap_days=overlap_days,
    )
    end_date = to_date or aware_now(get_timezone()).date()
    last_success = await fetch_last_success_window_end(
        database_url=config.database_url, pipeline_id=pipeline_id, store_code=store.store_code
    )
    start_date = resolve_orders_sync_start_date(
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
        return "success", windows, detail_lines, status_counts, window_audit, ingestion_totals
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
        uc_payload: dict[str, Any] | None = None
        status_conflict = False
        attempts = 0
        attempt_run_id = window_run_id
        attempt_audit: list[dict[str, Any]] = []
        for attempt in range(2):
            attempts = attempt + 1
            attempt_run_id = (
                f"{window_run_id}_attempt{attempts}"
                if pipeline_name == "uc_orders_sync"
                else window_run_id
            )
            log_event(
                logger=logger,
                phase="window",
                message="Running orders sync window",
                store_code=store.store_code,
                from_date=window_start,
                to_date=window_end,
                window_index=index,
                window_attempt=attempts,
                window_run_id=attempt_run_id,
            )
            await pipeline_fn(
                run_env=run_env,
                run_id=attempt_run_id,
                from_date=window_start,
                to_date=window_end,
                store_codes=[store.store_code],
                run_orders=True,
                run_sales=True,
            )
            summary = await fetch_summary_for_run(config.database_url, attempt_run_id)
            attempt_row_facts = _extract_row_facts_from_summary(summary)
            log_row = await _fetch_latest_log_row(
                database_url=config.database_url,
                pipeline_id=pipeline_id,
                store_code=store.store_code,
                run_id=attempt_run_id,
            )
            fetched_status = None
            error_message = None
            if log_row:
                status_value = log_row.get("status")
                error_value = log_row.get("error_message")
                fetched_status = str(status_value) if status_value else None
                error_message = str(error_value) if error_value else None
            status_note = ""
            if not fetched_status:
                status = "failed"
                status_note = " (missing orders_sync_log row)"
                if not error_message:
                    error_message = "orders_sync_log row missing for window"
                log_event(
                    logger=logger,
                    phase="window",
                    status="error",
                    message="orders_sync_log row missing; marking window failed",
                    store_code=store.store_code,
                    run_id=attempt_run_id,
                    window_index=index,
                    window_attempt=attempts,
                )
            else:
                status = fetched_status.lower()
            if status not in {"success", "success_with_warnings", "partial", "failed", "skipped"}:
                status = "failed"
            if fetched_status:
                status, mapped_note = _normalize_window_status(
                    pipeline_name=pipeline_name, status=status, error_message=error_message
                )
                status_note += mapped_note
            download_paths = _extract_window_download_paths(summary, store_code=store.store_code)
            if log_row:
                ingestion_counts = _extract_ingestion_counts_from_log(
                    log_row, pipeline_name=pipeline_name
                )
            else:
                ingestion_counts = {}
            status_conflict = status == "skipped" and _has_positive_ingestion_rows(ingestion_counts)
            if status_conflict:
                status_note += " (status skipped but rows present)"
                log_event(
                    logger=logger,
                    phase="window",
                    status="warn",
                    message="orders_sync_log status skipped but ingestion rows present",
                    store_code=store.store_code,
                    pipeline_name=pipeline_name,
                    run_id=attempt_run_id,
                    window_index=index,
                    window_attempt=attempts,
                    ingestion_counts=ingestion_counts,
                )
            if pipeline_name == "uc_orders_sync":
                uc_payload = _build_uc_window_log(
                    download_paths=download_paths,
                    ingestion_counts=ingestion_counts,
                    error_message=error_message,
                )
            note_for_attempt = status_note
            if attempt > 0:
                note_for_attempt += " (after retry)"
            attempt_audit.append(
                {
                    "attempt_no": attempts,
                    "attempt_run_id": attempt_run_id,
                    "status": status,
                    "status_note": note_for_attempt or None,
                    "status_conflict": status_conflict,
                    "error_message": error_message,
                    "download_paths": download_paths,
                    "ingestion_counts": ingestion_counts,
                    "orders_sync_log_id": log_row.get("id") if log_row else None,
                }
            )
            if not fetched_status:
                break
            if status == "failed" and attempt == 0:
                log_event(
                    logger=logger,
                    phase="window",
                    status="warn",
                    message="Retrying window after non-success status",
                    store_code=store.store_code,
                    window_index=index,
                    window_status=status,
                    window_attempt=attempts,
                    window_run_id=attempt_run_id,
                )
                continue
            if attempt_row_facts:
                _merge_row_facts(row_facts, attempt_row_facts)
            if attempt > 0:
                status_note += " (after retry)"
            break
        primary_metrics = _prefix_metrics(
            "primary_", _normalize_ingestion_metrics(_coerce_dict(ingestion_counts.get("primary")))
        )
        secondary_metrics = _prefix_metrics(
            "secondary_", _normalize_ingestion_metrics(_coerce_dict(ingestion_counts.get("secondary")))
        )
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
            primary_metrics=primary_metrics,
            secondary_metrics=secondary_metrics,
        )
        window_totals = _accumulate_ingestion_totals(ingestion_totals, ingestion_counts)
        window_audit.append(
            {
                "window_index": index,
                "from_date": window_start.isoformat(),
                "to_date": window_end.isoformat(),
                "status": status,
                "status_note": status_note or None,
                "status_conflict": status_conflict,
                "error_message": error_message,
                "download_paths": download_paths,
                "ingestion_counts": ingestion_counts,
                "ingestion_totals": window_totals,
                "attempt_no": attempts,
                "attempt_run_id": attempt_run_id,
                "orders_sync_log_id": log_row.get("id") if log_row else None,
                "attempts": attempt_audit,
            }
        )
        if pipeline_name == "uc_orders_sync":
            uc_payload = uc_payload or _build_uc_window_log(
                download_paths=download_paths,
                ingestion_counts=ingestion_counts,
                error_message=error_message,
            )
            uc_status = "ok" if uc_payload["ingest_success"] else ("error" if status == "failed" else "warn")
            log_event(
                logger=logger,
                phase="uc_window_log",
                status=uc_status,
                message="UC window ingestion snapshot",
                store_code=store.store_code,
                pipeline_name=pipeline_name,
                run_id=run_id,
                window_run_id=window_run_id,
                window_index=index,
                from_date=window_start,
                to_date=window_end,
                window_status=status,
                download_path=uc_payload["download_path"],
                staging_rows=uc_payload["staging_rows"],
                final_rows=uc_payload["final_rows"],
                ingest_success=uc_payload["ingest_success"],
                ingest_failure_reason=uc_payload["ingest_failure_reason"],
                attempt_no=attempts,
                orders_sync_log_id=log_row.get("id") if log_row else None,
            )
        detail_lines.append(
            f"{window_start.isoformat()} → {window_end.isoformat()}: {status}{status_note}"
        )
        status_counts[status] = status_counts.get(status, 0) + 1
        if status == "partial":
            overall_status = "partial"
        elif status == "success_with_warnings" and overall_status == "success":
            overall_status = "success_with_warnings"
        stop_after_window = _should_stop_after_window(
            status=status, error_message=error_message, status_note=status_note
        )
        if stop_after_window:
            overall_status = status
            log_event(
                logger=logger,
                phase="window",
                status="error" if status == "failed" else "warn",
                message="Stopping further windows after failure or explicit stop condition",
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
            "ok": status_counts.get("success", 0) + status_counts.get("skipped", 0),
            "warning": status_counts.get("partial", 0) + status_counts.get("success_with_warnings", 0),
            "error": status_counts.get("failed", 0),
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
    return overall_status, windows, detail_lines, status_counts, window_audit, ingestion_totals, row_facts


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
        (
            overall_status,
            windows,
            _detail_lines,
            status_counts,
            window_audit,
            ingestion_totals,
            row_facts,
        ) = await _run_store_windows(
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
        cost_center=store.cost_center,
        overall_status=overall_status,
        window_count=len(windows),
        windows=windows,
        status_counts=status_counts,
        window_audit=window_audit,
        ingestion_totals=ingestion_totals,
        row_facts=row_facts,
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
    uc_only: bool = False,
) -> None:
    resolved_env = run_env or config.run_env
    resolved_run_id = run_id or new_run_id()
    logger = get_logger(run_id=resolved_run_id)
    started_at = datetime.now(timezone.utc)
    resolved_sync_group = _normalize_sync_group(sync_group or "ALL")
    if uc_only:
        # Temporary UC-stabilization switch: ensure TD pipelines are excluded before planning windows.
        resolved_sync_group = "UC"
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
        uc_max_workers = _env_int("UC_MAX_WORKERS", max_workers)
        if group == "UC":
            group_max_workers = max(1, min(max_workers, uc_max_workers))
        else:
            group_max_workers = max_workers
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
    store_window_counts: dict[str, int] = {}
    missing_windows: dict[str, list[dict[str, str]]] = {}
    total_windows = 0
    grand_ingestion_totals = _init_ingestion_totals()
    store_entries: list[dict[str, Any]] = []
    primary_totals: dict[str, int] = {field: 0 for field in UNIFIED_METRIC_FIELDS}
    secondary_totals: dict[str, int] = {field: 0 for field in UNIFIED_METRIC_FIELDS}
    warning_messages: list[str] = []
    row_facts = _init_row_facts()
    for result in all_results:
        total_windows += result.window_count
        store_window_counts[result.store_code] = result.window_count
        missing = result.windows[len(result.window_audit) :]
        if missing:
            missing_windows[result.store_code] = [
                {"from_date": window_start.isoformat(), "to_date": window_end.isoformat()}
                for window_start, window_end in missing
            ]
        _merge_status_counts(total_status_counts, result.status_counts)
        pipeline_entry = pipeline_totals.setdefault(
            result.pipeline_group,
            {
                "window_count": 0,
                "status_counts": _init_status_counts(),
                "ingestion_totals": _init_ingestion_totals(),
            },
        )
        pipeline_entry["window_count"] += result.window_count
        _merge_status_counts(pipeline_entry["status_counts"], result.status_counts)
        _accumulate_ingestion_totals(pipeline_entry["ingestion_totals"], {"total": result.ingestion_totals})
        store_totals[result.store_code] = {
            "pipeline_group": result.pipeline_group,
            "pipeline_name": result.pipeline_name,
            "cost_center": result.cost_center,
            "overall_status": result.overall_status,
            "run_id": f"{resolved_run_id}_{result.store_code}",
            "window_count": result.window_count,
            "status_counts": result.status_counts,
            "window_audit": result.window_audit,
            "ingestion_totals": result.ingestion_totals,
        }
        status_conflicts = [
            window
            for window in result.window_audit
            if window.get("status_conflict")
        ]
        if status_conflicts:
            warning_messages.append(
                f"{result.store_code}: {len(status_conflicts)} window(s) skipped but rows present"
            )
        warning_messages.extend(
            _window_warning_entries(result.store_code, result.status_counts)
        )
        primary_metrics = {field: 0 for field in UNIFIED_METRIC_FIELDS}
        secondary_metrics = {field: 0 for field in UNIFIED_METRIC_FIELDS}
        secondary_label = None
        for window in result.window_audit:
            ingestion_counts = _coerce_dict(window.get("ingestion_counts"))
            primary = _build_unified_metrics(_coerce_dict(ingestion_counts.get("primary")))
            secondary = _build_unified_metrics(_coerce_dict(ingestion_counts.get("secondary")))
            if secondary.get("label"):
                secondary_label = secondary.get("label")
            _sum_unified_metrics(primary_metrics, primary)
            _sum_unified_metrics(secondary_metrics, secondary)
        if secondary_label:
            secondary_metrics["label"] = secondary_label
        store_status = _rollup_overall_status(result.status_counts)
        store_entries.append(
            {
                "store_code": result.store_code,
                "pipeline_group": result.pipeline_group,
                "pipeline_name": result.pipeline_name,
                "status": store_status,
                "window_count": result.window_count,
                "status_counts": result.status_counts,
                "window_audit": result.window_audit,
                "status_conflict_count": len(status_conflicts),
                "primary_metrics": primary_metrics,
                "secondary_metrics": secondary_metrics,
            }
        )
        _sum_unified_metrics(primary_totals, primary_metrics)
        _sum_unified_metrics(secondary_totals, secondary_metrics)
        _accumulate_ingestion_totals(grand_ingestion_totals, {"total": result.ingestion_totals})
        _merge_row_facts(row_facts, result.row_facts)
    overall_status = _rollup_overall_status(total_status_counts)
    warning_windows_total = int(total_status_counts.get("success_with_warnings", 0) or 0)
    if warning_windows_total > 0 and not any(
        entry.startswith("WINDOW_WARNINGS:") for entry in warning_messages
    ):
        warning_messages.append(
            f"WINDOW_WARNINGS: {warning_windows_total} window(s) completed with warnings"
        )
    if row_facts["warning_rows"] and not any(
        entry.startswith("ROW_WARNINGS:") for entry in warning_messages
    ):
        warning_messages.append(f"ROW_WARNINGS: {len(row_facts['warning_rows'])} row(s) with warnings")
    finished_at = datetime.now(timezone.utc)
    window_summary = _build_window_summary(total_windows, missing_windows)
    allow_missing_windows = _env_flag("ORDERS_SYNC_ALLOW_MISSING_WINDOWS")
    if (
        window_summary.get("completed_windows") != window_summary.get("expected_windows")
        and not allow_missing_windows
    ):
        overall_status = "failed"
        missing_summary = (
            f"Missing windows detected: {window_summary.get('missing_windows', 0)} missing "
            f"of {window_summary.get('expected_windows', 0)}."
        )
        warning_messages.append(missing_summary)
        log_event(
            logger=logger,
            phase="summary",
            status="error",
            message="Missing windows detected; marking run failed",
            missing_windows=missing_windows or None,
            window_summary=window_summary,
        )
    total_seconds = max(0, int((finished_at - started_at).total_seconds()))
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    total_time_taken = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    summary_text = _build_profiler_summary_text(
        run_id=resolved_run_id,
        run_env=resolved_env,
        started_at=started_at,
        finished_at=finished_at,
        overall_status=overall_status,
        store_entries=store_entries,
        window_summary=window_summary,
        warnings=warning_messages,
    )
    summary_record = {
        "pipeline_name": PIPELINE_NAME,
        "run_id": resolved_run_id,
        "run_env": resolved_env,
        "started_at": started_at,
        "finished_at": finished_at,
        "total_time_taken": total_time_taken,
        "report_date": started_at.date(),
        "overall_status": overall_status,
        "summary_text": summary_text,
        "phases_json": {
            "window": {
                "ok": total_status_counts.get("success", 0)
                + total_status_counts.get("skipped", 0),
                "warning": total_status_counts.get("partial", 0)
                + total_status_counts.get("success_with_warnings", 0),
                "error": total_status_counts.get("failed", 0),
            }
        },
        "metrics_json": {
            "status_counts": total_status_counts,
            "pipeline_totals": pipeline_totals,
            "store_totals": store_totals,
            "store_window_counts": store_window_counts,
            "missing_windows": missing_windows or None,
            "window_summary": window_summary,
            "ingestion_grand_totals": grand_ingestion_totals,
            "primary_totals": primary_totals,
            "secondary_totals": secondary_totals,
            "row_facts": row_facts,
            "notification_payload": _build_profiler_notification_payload(
                run_id=resolved_run_id,
                run_env=resolved_env,
                started_at=started_at,
                finished_at=finished_at,
                overall_status=overall_status,
                store_entries=store_entries,
                window_summary=window_summary,
                warnings=warning_messages,
                total_time_taken=total_time_taken,
                row_facts=row_facts,
            ),
        },
    }
    await insert_run_summary(config.database_url, summary_record)
    await _persist_missing_windows_log_rows(
        logger=logger,
        run_id=resolved_run_id,
        fallback_run_id=resolved_run_id,
        run_env=resolved_env,
        missing_windows=missing_windows,
        store_totals=store_totals,
    )
    await send_notifications_for_run(PIPELINE_NAME, resolved_run_id)
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
        store_window_counts=store_window_counts,
        missing_windows=missing_windows or None,
        ingestion_grand_totals=grand_ingestion_totals,
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
    parser.add_argument(
        "--uc-only",
        action="store_true",
        default=_env_flag("UC_ONLY"),
        help="Temporary UC-stabilization switch to skip TD pipelines.",
    )
    return parser


def _main() -> None:
    args = _build_parser().parse_args()
    resolved_run_id = args.run_id or new_run_id()
    logger = get_logger(run_id=resolved_run_id)

    async def _runner() -> None:
        await main(
            sync_group=args.sync_group,
            store_codes=args.store_codes,
            from_date=args.from_date,
            to_date=args.to_date,
            max_workers=args.max_workers,
            backfill_days=args.backfill_days,
            window_days=args.window_days,
            overlap_days=args.overlap_days,
            run_env=args.run_env,
            run_id=resolved_run_id,
            uc_only=args.uc_only,
        )

    loop = asyncio.new_event_loop()
    exit_code = 0
    try:
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_runner())
    except KeyboardInterrupt:
        exit_code = 130
        log_event(
            logger=logger,
            phase="shutdown",
            status="warn",
            message="Orders sync profiler interrupted",
        )
    except Exception as exc:
        exit_code = 1
        log_event(
            logger=logger,
            phase="fatal",
            status="error",
            message="Unhandled exception in orders sync profiler",
            error=str(exc),
            error_type=type(exc).__name__,
        )
    finally:
        try:
            pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
            if pending:
                for task in pending:
                    task.cancel()
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.run_until_complete(loop.shutdown_default_executor())
        finally:
            asyncio.set_event_loop(None)
            loop.close()
            logger.close()
    if exit_code:
        raise SystemExit(exit_code)


if __name__ == "__main__":
    _main()
