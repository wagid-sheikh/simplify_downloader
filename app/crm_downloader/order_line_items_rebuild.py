from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal, Mapping, Protocol, Sequence

import sqlalchemy as sa

from app.common.date_utils import aware_now, get_timezone, normalize_store_codes
from app.common.db import session_scope
from app.config import config
from app.crm_downloader.orders_sync_window import (
    build_non_overlapping_windows,
    resolve_crm_source_window_days,
    should_retry_exception,
    should_retry_window_status,
)
from app.crm_downloader.browser import launch_browser
from app.crm_downloader.td_orders_sync.garment_ingest import (
    TdGarmentIngestResult,
    ingest_td_garment_rows,
    order_line_items_table,
)
from app.crm_downloader.td_orders_sync.main import (
    _load_td_order_stores,
    prepare_td_api_context_for_store,
)
from app.crm_downloader.td_orders_sync.td_api_client import (
    TdApiClient,
    TdApiUnauthorizedError,
    td_api_fetch_auth_failure_endpoints,
)
from app.crm_downloader.uc_orders_sync.gst_api_extract import (
    collect_gst_orders_via_api,
    detect_archive_bearer_token_in_storage_state,
)
from app.crm_downloader.uc_orders_sync.gst_publish import (
    publish_uc_gst_order_details_to_line_items,
)
from app.crm_downloader.uc_orders_sync.main import _load_uc_order_stores
from app.dashboard_downloader.json_logger import (
    JsonLogger,
    get_logger,
    log_event,
    new_run_id,
)

Source = Literal["td", "uc"]
SnapshotOutcome = Literal[
    "complete_with_rows", "complete_empty", "incomplete_or_failed"
]
ZeroSnapshotClass = Literal[
    "confirmed_source_empty",
    "source_fetch_auth_failure",
    "unknown_ambiguous_empty",
]

FailureClass = Literal[
    "retryable_transient_failure",
    "systemic_setup_failure",
    "store_specific_failure",
    "window_data_failure",
]


SYSTEMIC_SETUP_ERROR_TOKENS = (
    "browser executable",
    "executable_path",
    "executable doesn't exist",
    "executable does not exist",
    "playwright install",
    "playwright dependencies",
    "missing dependencies",
    "host system is missing dependencies",
    "browsertype.launch",
    "browser_type.launch",
    "database_url is required",
    "missing required",
    "mandatory config",
    "invalid mandatory config",
    "missing cost_center",
    "cost_center is required",
    "store_master.start_date is not set",
    "no such table",
    "no such column",
    "undefined table",
    "undefined column",
    "relation does not exist",
    "column does not exist",
)

STORE_SPECIFIC_ERROR_TOKENS = (
    "401",
    "403",
    "auth failed",
    "authentication failed",
    "authorization failed",
    "unauthorized",
    "forbidden",
    "login required",
    "not authenticated",
    "storage state",
    "storage_state",
    "session expired",
)


def classify_rebuild_failure(exc: BaseException) -> FailureClass:
    """Classify rebuild failures before deciding whether to retry or stop.

    Systemic setup failures are deterministic environment/code/config/schema
    problems. Continuing across every store/window just repeats the same broken
    setup, so the rebuild must stop immediately. Source/store/data failures can
    leave other windows recoverable and remain isolated to window-level handling.
    """
    if should_retry_exception(exc):
        return "retryable_transient_failure"

    message = f"{type(exc).__module__}.{type(exc).__name__} {exc}".lower()
    if isinstance(exc, TypeError):
        return "systemic_setup_failure"
    if isinstance(exc, (ModuleNotFoundError, ImportError)) and "playwright" in message:
        return "systemic_setup_failure"
    if isinstance(exc, (sa.exc.ProgrammingError, sa.exc.OperationalError)) and any(
        token in message for token in SYSTEMIC_SETUP_ERROR_TOKENS
    ):
        return "systemic_setup_failure"
    if isinstance(exc, (AttributeError, KeyError, ValueError)) and any(
        token in message for token in SYSTEMIC_SETUP_ERROR_TOKENS
    ):
        return "systemic_setup_failure"
    if isinstance(exc, RuntimeError) and any(
        token in message for token in SYSTEMIC_SETUP_ERROR_TOKENS
    ):
        return "systemic_setup_failure"
    if any(token in message for token in STORE_SPECIFIC_ERROR_TOKENS):
        return "store_specific_failure"
    return "window_data_failure"


@dataclass(frozen=True)
class RebuildWindow:
    start: date
    end: date


@dataclass(frozen=True)
class RebuildStore:
    source: Source
    store_code: str
    cost_center: str
    raw_store: Any | None = None
    sync_config: Mapping[str, Any] = field(default_factory=dict)
    start_date: date | None = None


@dataclass(frozen=True)
class RebuildPreflightResult:
    stores: list[RebuildStore]
    source_window_days_by_store: dict[tuple[Source, str], int]
    missing_storage_states: list[dict[str, str]]
    auth_readiness: dict[str, str]


@dataclass
class SourceSnapshot:
    line_item_rows: list[Mapping[str, Any]] = field(default_factory=list)
    order_snapshots: list[Mapping[str, Any]] = field(default_factory=list)
    zero_snapshot_class: ZeroSnapshotClass | None = None
    garments_fetch_completeness: str | None = None
    source_fetch_error_class: str | None = None
    endpoint_health: Mapping[str, Any] = field(default_factory=dict)
    endpoint_error_diagnostics: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TdGarmentsFetchIncomplete(RuntimeError):
    store_code: str
    window_start: date
    window_end: date
    garments_fetch_completeness: str
    source_fetch_error_class: str | None = None
    endpoint_health: Mapping[str, Any] = field(default_factory=dict)
    endpoint_error_diagnostics: Mapping[str, Any] = field(default_factory=dict)

    @property
    def retryable(self) -> bool:
        error_class = str(self.source_fetch_error_class or "").lower()
        health_text = json.dumps(dict(self.endpoint_health or {}), default=str).lower()
        return any(
            token in f"{error_class} {health_text}"
            for token in (
                "timeout",
                "budget",
                "pagination_budget",
                "wall_time",
                "total_timeout",
                "read_timeout",
                "connect_timeout",
            )
        )

    def __str__(self) -> str:
        retry_hint = "; timeout retryable" if self.retryable else ""
        return (
            "TD garments fetch incomplete for store "
            f"{self.store_code.upper().strip()} window "
            f"{self.window_start.isoformat()}..{self.window_end.isoformat()}; "
            f"garments_fetch_completeness={self.garments_fetch_completeness}; "
            f"source_fetch_error_class={self.source_fetch_error_class or 'unknown'}"
            f"{retry_hint}"
        )


@dataclass(frozen=True)
class OrderLineItemsRebuildIncomplete(Exception):
    run_id: str
    expected_window_count: int
    completed_window_count: int
    missing_window_count: int
    missing_windows: tuple[str, ...]

    def __str__(self) -> str:
        preview = ", ".join(self.missing_windows[:5])
        suffix = "" if len(self.missing_windows) <= 5 else ", ..."
        return (
            "order_line_items rebuild incomplete "
            f"run_id={self.run_id} "
            f"completed={self.completed_window_count}/"
            f"{self.expected_window_count} "
            f"missing={self.missing_window_count}"
            f" missing_windows=[{preview}{suffix}]"
        )


@dataclass(frozen=True)
class OrderLineItemsZeroSnapshotDetected(Exception):
    run_id: str
    zero_window_count: int
    expected_window_count: int
    suspicious_window_count: int
    zero_windows: tuple[str, ...]

    def __str__(self) -> str:
        preview = ", ".join(self.zero_windows[:5])
        suffix = "" if len(self.zero_windows) <= 5 else ", ..."
        return (
            "order_line_items rebuild zero authoritative-order snapshot detected "
            f"run_id={self.run_id} zero_windows={self.zero_window_count}/"
            f"{self.expected_window_count} suspicious={self.suspicious_window_count}"
            f" windows=[{preview}{suffix}]"
        )


@dataclass
class WindowMetrics:
    source: Source
    store_code: str
    cost_center: str
    window_start: date
    window_end: date
    uc_child_run_id: str | None = None
    inspected_orders: int = 0
    complete_with_rows_orders: int = 0
    complete_empty_orders: int = 0
    skipped_incomplete_orders: int = 0
    deleted_rows: int = 0
    inserted_rows: int = 0
    orphan_rows: int = 0
    dry_run: bool = False
    zero_snapshot_class: ZeroSnapshotClass | None = None

    def checkpoint(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "source": self.source,
            "store_code": self.store_code,
            "cost_center": self.cost_center,
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "dry_run": self.dry_run,
            "zero_snapshot_class": self.zero_snapshot_class,
            "inspected_orders": self.inspected_orders,
            "complete_with_rows_orders": self.complete_with_rows_orders,
            "complete_empty_orders": self.complete_empty_orders,
            "skipped_incomplete_orders": self.skipped_incomplete_orders,
            "deleted_rows": self.deleted_rows,
            "inserted_rows": self.inserted_rows,
            "orphan_rows": self.orphan_rows,
        }
        if self.uc_child_run_id:
            payload["uc_child_run_id"] = self.uc_child_run_id
        return payload


def _is_zero_authoritative_snapshot(metrics: WindowMetrics) -> bool:
    return (
        metrics.inspected_orders == 0
        and metrics.complete_with_rows_orders == 0
        and metrics.complete_empty_orders == 0
        and metrics.skipped_incomplete_orders == 0
    )


def _zero_snapshot_class(snapshot: SourceSnapshot) -> ZeroSnapshotClass:
    return snapshot.zero_snapshot_class or "unknown_ambiguous_empty"


def _zero_snapshot_status(zero_snapshot_class: ZeroSnapshotClass) -> str:
    return "ok" if zero_snapshot_class == "confirmed_source_empty" else "warning"


def _zero_snapshot_message(zero_snapshot_class: ZeroSnapshotClass) -> str:
    if zero_snapshot_class == "confirmed_source_empty":
        return "zero_authoritative_orders_detected_confirmed_source_empty"
    if zero_snapshot_class == "source_fetch_auth_failure":
        return "zero_authoritative_orders_detected_source_fetch_auth_failure"
    return "zero_authoritative_orders_detected_unknown_ambiguous_empty"


def _garments_health_from_result(result: Any) -> Mapping[str, Any]:
    endpoint_health = getattr(result, "endpoint_health", None) or {}
    health = (
        endpoint_health.get("/garments/details")
        if isinstance(endpoint_health, Mapping)
        else None
    )
    return health if isinstance(health, Mapping) else {}


def _garments_endpoint_diagnostics_from_result(result: Any) -> Mapping[str, Any]:
    diagnostics = getattr(result, "endpoint_error_diagnostics", None) or {}
    endpoint_diagnostics = (
        diagnostics.get("/garments/details")
        if isinstance(diagnostics, Mapping)
        else None
    )
    return endpoint_diagnostics if isinstance(endpoint_diagnostics, Mapping) else {}


def _source_fetch_error_class(result: Any, health: Mapping[str, Any]) -> str | None:
    endpoint_errors = getattr(result, "endpoint_errors", None) or {}
    endpoint_error = (
        endpoint_errors.get("/garments/details")
        if isinstance(endpoint_errors, Mapping)
        else None
    )
    return (
        getattr(result, "source_fetch_error_class", None)
        or health.get("final_error_class")
        or endpoint_error
    )


def _td_garments_incomplete_log_fields(exc: BaseException) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for attr in (
        "garments_fetch_completeness",
        "source_fetch_error_class",
        "endpoint_health",
        "endpoint_error_diagnostics",
    ):
        if hasattr(exc, attr):
            fields[attr] = getattr(exc, attr)
    if hasattr(exc, "retryable"):
        fields["retryable"] = bool(getattr(exc, "retryable"))
    return fields


class SnapshotFetcher(Protocol):
    async def __call__(
        self,
        *,
        source: Source,
        store: RebuildStore,
        window: RebuildWindow,
        run_id: str,
        logger: JsonLogger,
    ) -> SourceSnapshot: ...


def iter_windows(
    start_date: date, end_date: date, window_size_days: int
) -> list[RebuildWindow]:
    if end_date < start_date:
        raise ValueError("end date must be on or after start date")
    return [
        RebuildWindow(start=window_start, end=window_end)
        for window_start, window_end in build_non_overlapping_windows(
            start_date=start_date, end_date=end_date, window_days=window_size_days
        )
    ]


def _uc_child_run_id(*, run_id: str, store: RebuildStore, window: RebuildWindow) -> str:
    return (
        f"{run_id}:uc:{store.store_code}:{window.start.isoformat()}:"
        f"{window.end.isoformat()}"
    )


def _compact_window_identifier(
    source: Source, store_code: str, window_start: date, window_end: date
) -> str:
    return (
        f"{source}:{store_code}:{window_start.isoformat()}.."
        f"{window_end.isoformat()}"
    )


def _order_number(row: Mapping[str, Any]) -> str:
    return str(
        row.get("order_number")
        or row.get("order_no")
        or row.get("orderNumber")
        or row.get("order_code")
        or row.get("normalized_order_number")
        or ""
    ).strip()


def _outcome(row: Mapping[str, Any]) -> SnapshotOutcome:
    raw = (
        str(
            row.get("garment_snapshot_outcome")
            or row.get("snapshot_outcome")
            or row.get("outcome")
            or ""
        )
        .strip()
        .lower()
    )
    if raw in {"complete_with_rows", "complete_empty", "incomplete_or_failed"}:
        return raw  # type: ignore[return-value]
    return "incomplete_or_failed"


async def _count_existing_rows(
    *, database_url: str, cost_center: str, order_numbers: Sequence[str]
) -> int:
    if not order_numbers:
        return 0
    metadata = sa.MetaData()
    line_items = order_line_items_table(metadata)
    async with session_scope(database_url) as session:
        stmt = (
            sa.select(sa.func.count())
            .select_from(line_items)
            .where(
                sa.and_(
                    line_items.c.cost_center == cost_center,
                    line_items.c.order_number.in_(list(order_numbers)),
                )
            )
        )
        return int((await session.execute(stmt)).scalar_one() or 0)


async def _count_orphans(
    *,
    database_url: str,
    cost_center: str,
    order_numbers: Sequence[str],
    inserted_rows_by_order: Mapping[str, int],
) -> int:
    if not order_numbers:
        return 0
    async with session_scope(database_url) as session:
        rows = (
            (
                await session.execute(
                    sa.text(
                        "SELECT order_number FROM orders WHERE cost_center = :cost_center AND order_number IN :order_numbers"
                    ).bindparams(sa.bindparam("order_numbers", expanding=True)),
                    {"cost_center": cost_center, "order_numbers": list(order_numbers)},
                )
            )
            .scalars()
            .all()
        )
    existing = {str(row) for row in rows}
    return sum(
        count
        for order_number, count in inserted_rows_by_order.items()
        if order_number not in existing
    )


async def _dry_run_metrics(
    *,
    source: Source,
    store: RebuildStore,
    window: RebuildWindow,
    snapshot: SourceSnapshot,
    database_url: str,
    zero_snapshot_class: ZeroSnapshotClass | None = None,
) -> WindowMetrics:
    snapshots = list(snapshot.order_snapshots)
    complete_with_rows = [
        _order_number(row)
        for row in snapshots
        if _outcome(row) == "complete_with_rows" and _order_number(row)
    ]
    complete_empty = [
        _order_number(row)
        for row in snapshots
        if _outcome(row) == "complete_empty" and _order_number(row)
    ]
    replaceable = [*complete_with_rows, *complete_empty]
    rows_by_order: dict[str, int] = {}
    for row in snapshot.line_item_rows:
        order_number = _order_number(row)
        if order_number:
            rows_by_order[order_number] = rows_by_order.get(order_number, 0) + 1
    return WindowMetrics(
        source=source,
        store_code=store.store_code,
        cost_center=store.cost_center,
        window_start=window.start,
        window_end=window.end,
        inspected_orders=len(snapshots),
        complete_with_rows_orders=len(complete_with_rows),
        complete_empty_orders=len(complete_empty),
        skipped_incomplete_orders=sum(
            1 for row in snapshots if _outcome(row) == "incomplete_or_failed"
        ),
        deleted_rows=await _count_existing_rows(
            database_url=database_url,
            cost_center=store.cost_center,
            order_numbers=replaceable,
        ),
        inserted_rows=sum(
            rows_by_order.get(order_number, 0) for order_number in complete_with_rows
        ),
        orphan_rows=await _count_orphans(
            database_url=database_url,
            cost_center=store.cost_center,
            order_numbers=complete_with_rows,
            inserted_rows_by_order=rows_by_order,
        ),
        dry_run=True,
        zero_snapshot_class=zero_snapshot_class,
    )


async def _stage_uc_snapshot(
    *,
    database_url: str,
    run_id: str,
    store: RebuildStore,
    snapshot: SourceSnapshot,
    run_date: datetime,
) -> None:
    async with session_scope(database_url) as session:
        for seq, row in enumerate(snapshot.line_item_rows, start=1):
            await session.execute(
                sa.text("""
                INSERT INTO stg_uc_archive_order_details
                (run_id, run_date, cost_center, store_code, order_code, service, item_name, rate, quantity, weight, amount, order_datetime_raw, line_hash, ingest_row_seq, ingest_remarks)
                VALUES (:run_id, :run_date, :cost_center, :store_code, :order_code, :service, :item_name, :rate, :quantity, :weight, :amount, :order_datetime_raw, :line_hash, :ingest_row_seq, :ingest_remarks)
            """),
                {
                    "run_id": run_id,
                    "run_date": run_date,
                    "cost_center": row.get("cost_center") or store.cost_center,
                    "store_code": row.get("store_code") or store.store_code,
                    "order_code": _order_number(row),
                    "service": row.get("service") or row.get("service_name"),
                    "item_name": row.get("item_name") or row.get("garment_name"),
                    "rate": row.get("rate"),
                    "quantity": row.get("quantity"),
                    "weight": row.get("weight"),
                    "amount": row.get("amount"),
                    "order_datetime_raw": row.get("order_datetime_raw"),
                    "line_hash": row.get("line_hash") or row.get("line_item_key"),
                    "ingest_row_seq": row.get("ingest_row_seq") or seq,
                    "ingest_remarks": row.get("ingest_remarks"),
                },
            )
        for row in snapshot.order_snapshots:
            order_number = _order_number(row)
            if not order_number:
                continue
            await session.execute(
                sa.text("""
                INSERT INTO stg_uc_order_detail_snapshots
                (run_id, run_date, cost_center, store_code, order_code, normalized_order_number, snapshot_outcome, detail_row_count, ingest_remarks)
                VALUES (:run_id, :run_date, :cost_center, :store_code, :order_code, :normalized_order_number, :snapshot_outcome, :detail_row_count, :ingest_remarks)
            """),
                {
                    "run_id": run_id,
                    "run_date": run_date,
                    "cost_center": row.get("cost_center") or store.cost_center,
                    "store_code": row.get("store_code") or store.store_code,
                    "order_code": order_number,
                    "normalized_order_number": order_number,
                    "snapshot_outcome": _outcome(row),
                    "detail_row_count": row.get("detail_row_count")
                    or row.get("garment_row_count")
                    or 0,
                    "ingest_remarks": row.get("ingest_remarks"),
                },
            )
        await session.commit()


def _td_metrics_from_result(
    *,
    store: RebuildStore,
    window: RebuildWindow,
    result: TdGarmentIngestResult,
    dry_run: bool,
    zero_snapshot_class: ZeroSnapshotClass | None = None,
) -> WindowMetrics:
    return WindowMetrics(
        source="td",
        store_code=store.store_code,
        cost_center=store.cost_center,
        window_start=window.start,
        window_end=window.end,
        inspected_orders=result.authoritative_orders_inspected,
        complete_with_rows_orders=result.complete_with_rows_orders,
        complete_empty_orders=result.complete_empty_orders,
        skipped_incomplete_orders=result.replacement_skipped_incomplete_orders,
        deleted_rows=result.deleted_final_rows,
        inserted_rows=result.inserted_final_rows,
        orphan_rows=result.orphan_rows,
        dry_run=dry_run,
        zero_snapshot_class=zero_snapshot_class,
    )


async def rebuild_window(
    *,
    source: Source,
    store: RebuildStore,
    window: RebuildWindow,
    run_id: str,
    run_date: datetime,
    database_url: str,
    dry_run: bool,
    logger: JsonLogger,
    fetch_snapshot: SnapshotFetcher,
) -> WindowMetrics:
    uc_child_run_id = (
        _uc_child_run_id(run_id=run_id, store=store, window=window)
        if source == "uc"
        else None
    )
    snapshot = await fetch_snapshot(
        source=source, store=store, window=window, run_id=run_id, logger=logger
    )
    td_replacement_allowed = True
    if source == "td":
        td_completeness = (snapshot.garments_fetch_completeness or "complete").lower()
        td_replacement_allowed = td_completeness == "complete"
        if not td_replacement_allowed:
            exc = TdGarmentsFetchIncomplete(
                store_code=store.store_code,
                window_start=window.start,
                window_end=window.end,
                garments_fetch_completeness=td_completeness,
                source_fetch_error_class=snapshot.source_fetch_error_class,
                endpoint_health=snapshot.endpoint_health,
                endpoint_error_diagnostics=snapshot.endpoint_error_diagnostics,
            )
            log_event(
                logger=logger,
                phase="order_line_items_rebuild_td_source_snapshot",
                status="error",
                message="TD garments source snapshot is not authoritative",
                run_id=run_id,
                source="td",
                store_code=store.store_code,
                cost_center=store.cost_center,
                window_start=window.start.isoformat(),
                window_end=window.end.isoformat(),
                dry_run=dry_run,
                garments_fetch_completeness=td_completeness,
                source_fetch_error_class=snapshot.source_fetch_error_class,
                endpoint_health=snapshot.endpoint_health,
                endpoint_error_diagnostics=snapshot.endpoint_error_diagnostics,
                retryable=exc.retryable,
            )
            raise exc
    if dry_run:
        metrics = await _dry_run_metrics(
            source=source,
            store=store,
            window=window,
            snapshot=snapshot,
            database_url=database_url,
            zero_snapshot_class=_zero_snapshot_class(snapshot),
        )
    elif source == "td":
        result = await ingest_td_garment_rows(
            rows=snapshot.line_item_rows,
            authoritative_order_scope=snapshot.order_snapshots,
            replacement_allowed=td_replacement_allowed,
            store_code=store.store_code,
            cost_center=store.cost_center,
            run_id=run_id,
            run_date=run_date,
            window_from_date=window.start,
            window_to_date=window.end,
            database_url=database_url,
        )
        metrics = _td_metrics_from_result(
            store=store,
            window=window,
            result=result,
            dry_run=False,
            zero_snapshot_class=_zero_snapshot_class(snapshot),
        )
    else:
        assert uc_child_run_id is not None
        await _stage_uc_snapshot(
            database_url=database_url,
            run_id=uc_child_run_id,
            store=store,
            snapshot=snapshot,
            run_date=run_date,
        )
        result = await publish_uc_gst_order_details_to_line_items(
            database_url=database_url,
            run_id=uc_child_run_id,
            store_code=store.store_code,
        )
        metrics = WindowMetrics(
            source="uc",
            store_code=store.store_code,
            cost_center=store.cost_center,
            window_start=window.start,
            window_end=window.end,
            uc_child_run_id=uc_child_run_id,
            inspected_orders=result.invoices_inspected,
            complete_with_rows_orders=result.complete_with_rows_invoices,
            complete_empty_orders=result.complete_empty_invoices,
            skipped_incomplete_orders=result.replacement_skipped_incomplete_invoices,
            deleted_rows=result.deleted_final_rows,
            inserted_rows=result.inserted_final_rows,
            orphan_rows=result.orphan_rows,
            dry_run=False,
            zero_snapshot_class=_zero_snapshot_class(snapshot),
        )

    if uc_child_run_id and metrics.uc_child_run_id is None:
        metrics.uc_child_run_id = uc_child_run_id

    if _is_zero_authoritative_snapshot(metrics):
        metrics.zero_snapshot_class = metrics.zero_snapshot_class or (
            "unknown_ambiguous_empty"
        )
        log_event(
            logger=logger,
            phase="order_line_items_rebuild_zero_snapshot",
            status=_zero_snapshot_status(metrics.zero_snapshot_class),
            message=_zero_snapshot_message(metrics.zero_snapshot_class),
            run_id=run_id,
            source=metrics.source,
            store_code=metrics.store_code,
            cost_center=metrics.cost_center,
            window_start=metrics.window_start.isoformat(),
            window_end=metrics.window_end.isoformat(),
            dry_run=metrics.dry_run,
            zero_snapshot_class=metrics.zero_snapshot_class,
            inspected_orders=metrics.inspected_orders,
            complete_with_rows_orders=metrics.complete_with_rows_orders,
            complete_empty_orders=metrics.complete_empty_orders,
            skipped_incomplete_orders=metrics.skipped_incomplete_orders,
            uc_child_run_id=metrics.uc_child_run_id,
        )
    else:
        metrics.zero_snapshot_class = None

    log_event(
        logger=logger,
        phase="order_line_items_rebuild_window",
        status="ok",
        message="order_line_items historical rebuild window checkpoint",
        run_id=run_id,
        source=metrics.source,
        store_code=metrics.store_code,
        cost_center=metrics.cost_center,
        window_start=metrics.window_start.isoformat(),
        window_end=metrics.window_end.isoformat(),
        inspected_orders=metrics.inspected_orders,
        complete_with_rows_orders=metrics.complete_with_rows_orders,
        complete_empty_orders=metrics.complete_empty_orders,
        skipped_incomplete_orders=metrics.skipped_incomplete_orders,
        deleted_rows=metrics.deleted_rows,
        inserted_rows=metrics.inserted_rows,
        orphan_rows=metrics.orphan_rows,
        dry_run=metrics.dry_run,
        uc_child_run_id=metrics.uc_child_run_id,
        checkpoint=metrics.checkpoint(),
    )
    return metrics


def _storage_state_path(store: RebuildStore) -> Path | None:
    raw_path = getattr(store.raw_store, "storage_state_path", None)
    return Path(raw_path) if raw_path else None


async def default_fetch_snapshot(
    *,
    source: Source,
    store: RebuildStore,
    window: RebuildWindow,
    run_id: str,
    logger: JsonLogger,
) -> SourceSnapshot:
    storage_state_path = _storage_state_path(store)
    storage_state = (
        str(storage_state_path)
        if storage_state_path and storage_state_path.exists()
        else None
    )

    if source == "td":
        from playwright.async_api import async_playwright

        td_store = store.raw_store
        if td_store is None:
            raise ValueError(
                "TD rebuild stores must include the raw TdStore auth config"
            )

        async with async_playwright() as playwright:
            browser = await launch_browser(playwright=playwright, logger=logger)
            try:
                api_context = await prepare_td_api_context_for_store(
                    browser=browser,
                    store=td_store,
                    logger=logger,
                    run_id=run_id,
                    run_start_date=window.start,
                    run_end_date=window.end,
                )
                client = TdApiClient(
                    store_code=store.store_code,
                    context=api_context.context,
                    storage_state_path=storage_state_path or Path(),
                    run_id=run_id,
                    structured_logger=logger,
                    report_iframe_src=api_context.report_iframe_src,
                )
                result = await client.fetch_reports(
                    from_date=window.start, to_date=window.end
                )
                garments_health = _garments_health_from_result(result)
                raw_completeness = garments_health.get("garments_fetch_completeness")
                assume_complete_for_legacy_result = raw_completeness is None and not (
                    getattr(result, "endpoint_errors", None)
                    or getattr(result, "source_fetch_status", None)
                )
                effective_completeness = (
                    "complete"
                    if assume_complete_for_legacy_result
                    else raw_completeness
                )
                garments_fetch_completeness = (
                    str(effective_completeness or "unknown").strip().lower()
                )
                source_fetch_error_class = _source_fetch_error_class(
                    result, garments_health
                )
                endpoint_diagnostics = _garments_endpoint_diagnostics_from_result(
                    result
                )
                auth_failed_endpoints = td_api_fetch_auth_failure_endpoints(result)
                if auth_failed_endpoints:
                    if hasattr(logger, "info"):
                        log_event(
                            logger=logger,
                            phase="order_line_items_rebuild_td_source_snapshot",
                            status="error",
                            message="TD garments source snapshot is not authoritative",
                            run_id=run_id,
                            source="td",
                            store_code=store.store_code,
                            cost_center=store.cost_center,
                            window_start=window.start.isoformat(),
                            window_end=window.end.isoformat(),
                            garments_fetch_completeness=garments_fetch_completeness,
                            source_fetch_error_class=source_fetch_error_class,
                            endpoint_health=garments_health,
                            endpoint_error_diagnostics=endpoint_diagnostics,
                        )
                    raise TdApiUnauthorizedError(
                        store_code=store.store_code,
                        failed_endpoints=auth_failed_endpoints,
                        error_class=source_fetch_error_class,
                    )
                if garments_fetch_completeness != "complete":
                    exc = TdGarmentsFetchIncomplete(
                        store_code=store.store_code,
                        window_start=window.start,
                        window_end=window.end,
                        garments_fetch_completeness=garments_fetch_completeness,
                        source_fetch_error_class=source_fetch_error_class,
                        endpoint_health=garments_health,
                        endpoint_error_diagnostics=endpoint_diagnostics,
                    )
                    if hasattr(logger, "info"):
                        log_event(
                            logger=logger,
                            phase="order_line_items_rebuild_td_source_snapshot",
                            status="error",
                            message="TD garments source snapshot is not authoritative",
                            run_id=run_id,
                            source="td",
                            store_code=store.store_code,
                            cost_center=store.cost_center,
                            window_start=window.start.isoformat(),
                            window_end=window.end.isoformat(),
                            garments_fetch_completeness=garments_fetch_completeness,
                            source_fetch_error_class=source_fetch_error_class,
                            endpoint_health=garments_health,
                            endpoint_error_diagnostics=endpoint_diagnostics,
                            retryable=exc.retryable,
                        )
                    raise exc
                return SourceSnapshot(
                    line_item_rows=result.garments_rows,
                    order_snapshots=result.garment_order_snapshots,
                    garments_fetch_completeness=(
                        None
                        if assume_complete_for_legacy_result
                        else garments_fetch_completeness
                    ),
                    source_fetch_error_class=source_fetch_error_class,
                    endpoint_health=garments_health,
                    endpoint_error_diagnostics=endpoint_diagnostics,
                )
            finally:
                await browser.close()

    from playwright.async_api import async_playwright

    async with async_playwright() as playwright:
        browser = await launch_browser(playwright=playwright, logger=logger)
        try:
            context = await browser.new_context(storage_state=storage_state)
            page = await context.new_page()
            home_url = getattr(store.raw_store, "home_url", None) or getattr(
                store.raw_store, "orders_url", None
            )
            if home_url:
                await page.goto(home_url, wait_until="domcontentloaded")
            extract = await collect_gst_orders_via_api(
                page=page,
                store_code=store.store_code,
                logger=logger,
                from_date=window.start,
                to_date=window.end,
            )
            return SourceSnapshot(
                line_item_rows=extract.order_detail_rows,
                order_snapshots=extract.order_detail_snapshot_rows,
            )
        finally:
            await browser.close()


def _coerce_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _store_sync_config(store: Any) -> Mapping[str, Any]:
    raw = getattr(store, "sync_config", None)
    return raw if isinstance(raw, Mapping) else {}


async def _load_store_start_dates(
    *, database_url: str, stores: Sequence[RebuildStore]
) -> dict[tuple[Source, str], date]:
    if not stores:
        return {}
    clauses: list[str] = []
    params: dict[str, Any] = {}
    for index, store in enumerate(stores):
        clauses.append(
            f"(UPPER(store_code) = :store_code_{index} "
            f"AND UPPER(sync_group) = :sync_group_{index})"
        )
        params[f"store_code_{index}"] = store.store_code.upper()
        # Keep the SQL bind value aligned to the DB contract (TD/UC) while
        # comparing case-insensitively in SQL for older lowercase rows.
        params[f"sync_group_{index}"] = store.source.upper()
    async with session_scope(database_url) as session:
        result = await session.execute(
            sa.text(
                "SELECT UPPER(store_code) AS store_code, "
                "UPPER(sync_group) AS sync_group, start_date "
                f"FROM store_master WHERE {' OR '.join(clauses)}"
            ),
            params,
        )
        return {
            (str(row.sync_group).lower(), str(row.store_code).upper()): parsed
            for row in result
            if (parsed := _coerce_date(row.start_date)) is not None
        }


async def _ensure_progress_table(database_url: str) -> None:
    """Verify the Alembic-managed rebuild progress table exists.

    Runtime code intentionally does not create this table. Keeping schema
    ownership in Alembic prevents production drift between application fallback
    DDL and the migration chain.
    """
    async with session_scope(database_url) as session:
        connection = await session.connection()
        has_table = await connection.run_sync(
            lambda sync_connection: sa.inspect(sync_connection).has_table(
                "order_line_items_rebuild_progress"
            )
        )
    if not has_table:
        raise RuntimeError(
            "order_line_items_rebuild_progress table is missing; run Alembic "
            "migrations before starting the order_line_items rebuild "
            "(for example: poetry run python -m app db upgrade)."
        )


async def _fetch_progress_rows(
    database_url: str,
    *,
    resume_run_id: str | None = None,
) -> dict[tuple[Source, str, date, date], Mapping[str, Any]]:
    await _ensure_progress_table(database_url)
    async with session_scope(database_url) as session:
        # Resume progress is a live-run contract. Legacy dry-run rows must not
        # cause a mutating rebuild to skip a window that was only simulated.
        progress_sql = (
            "SELECT * FROM order_line_items_rebuild_progress " "WHERE dry_run = FALSE"
        )
        params: dict[str, Any] = {}
        if resume_run_id:
            progress_sql += " AND run_id = :resume_run_id"
            params["resume_run_id"] = resume_run_id
        result = await session.execute(sa.text(progress_sql), params)
        rows: dict[tuple[Source, str, date, date], Mapping[str, Any]] = {}
        for row in result.mappings():
            window_start = _coerce_date(row.get("window_start"))
            window_end = _coerce_date(row.get("window_end"))
            if not window_start or not window_end:
                continue
            rows[
                (
                    str(row.get("source")).lower(),
                    str(row.get("store_code")).upper(),
                    window_start,
                    window_end,
                )
            ] = dict(row)
        return rows


async def _write_progress(
    *,
    database_url: str,
    store: RebuildStore,
    window: RebuildWindow,
    run_id: str,
    status: str,
    attempt_no: int,
    metrics: WindowMetrics | None = None,
    error_message: str | None = None,
    dry_run: bool = False,
) -> None:
    await _ensure_progress_table(database_url)
    values = {
        "source": store.source,
        "store_code": store.store_code,
        "cost_center": store.cost_center,
        "window_start": window.start,
        "window_end": window.end,
        "run_id": run_id,
        "status": status,
        "attempt_no": attempt_no,
        "error_message": error_message,
        "complete_with_rows_orders": (
            metrics.complete_with_rows_orders if metrics else 0
        ),
        "complete_empty_orders": metrics.complete_empty_orders if metrics else 0,
        "skipped_incomplete_orders": (
            metrics.skipped_incomplete_orders if metrics else 0
        ),
        "deleted_rows": metrics.deleted_rows if metrics else 0,
        "inserted_rows": metrics.inserted_rows if metrics else 0,
        "orphan_rows": metrics.orphan_rows if metrics else 0,
        "dry_run": metrics.dry_run if metrics else dry_run,
    }
    async with session_scope(database_url) as session:
        await session.execute(
            sa.text(
                "DELETE FROM order_line_items_rebuild_progress "
                "WHERE source = :source AND store_code = :store_code "
                "AND window_start = :window_start AND window_end = :window_end"
            ),
            values,
        )
        await session.execute(
            sa.text("""
            INSERT INTO order_line_items_rebuild_progress
            (source, store_code, cost_center, window_start, window_end, run_id, status, attempt_no, error_message,
             complete_with_rows_orders, complete_empty_orders, skipped_incomplete_orders, deleted_rows, inserted_rows,
             orphan_rows, dry_run, updated_at)
            VALUES (:source, :store_code, :cost_center, :window_start, :window_end, :run_id, :status, :attempt_no, :error_message,
             :complete_with_rows_orders, :complete_empty_orders, :skipped_incomplete_orders, :deleted_rows, :inserted_rows,
             :orphan_rows, :dry_run, CURRENT_TIMESTAMP)
        """),
            values,
        )
        await session.commit()


def _prior_progress_metadata(row: Mapping[str, Any] | None) -> dict[str, Any]:
    if not row:
        return {}
    updated_at = row.get("updated_at")
    if isinstance(updated_at, datetime):
        prior_updated_at: str | None = updated_at.isoformat()
    elif updated_at is None:
        prior_updated_at = None
    else:
        prior_updated_at = str(updated_at)
    prior_metrics_counts = {
        "complete_with_rows_orders": int(row.get("complete_with_rows_orders") or 0),
        "complete_empty_orders": int(row.get("complete_empty_orders") or 0),
        "skipped_incomplete_orders": int(row.get("skipped_incomplete_orders") or 0),
        "deleted_rows": int(row.get("deleted_rows") or 0),
        "inserted_rows": int(row.get("inserted_rows") or 0),
        "orphan_rows": int(row.get("orphan_rows") or 0),
    }
    return {
        "prior_run_id": row.get("run_id"),
        "prior_updated_at": prior_updated_at,
        "prior_status": row.get("status"),
        "prior_metrics_counts": prior_metrics_counts,
    }


def _is_success_status(status: Any) -> bool:
    return str(status or "").strip().lower() in {"success", "success_with_warnings"}


def _window_status(row: Mapping[str, Any] | None) -> str | None:
    return str(row.get("status") or "").strip().lower() if row else None


def _browser_backend() -> str:
    return str(
        getattr(config, "pdf_render_backend", None) or "bundled_chromium"
    ).lower()


class _PreflightChromiumProbe:
    async def launch(self, **_kwargs: Any) -> "_PreflightBrowserProbe":
        return _PreflightBrowserProbe()


class _PreflightBrowserProbe:
    async def close(self) -> None:
        return None


async def _check_browser_launch_contract(*, logger: JsonLogger) -> None:
    """Validate the rebuild's browser-launch calling contract without opening Chrome.

    The historical rebuild fetchers only depend on the shared ``launch_browser``
    keyword-only API and a browser object that can be closed.  A lightweight
    probe catches contract drift before the first window starts while keeping
    tests and dry-run preflight independent from the local Playwright install.
    """
    browser = await launch_browser(
        playwright=SimpleNamespace(chromium=_PreflightChromiumProbe()),
        logger=logger,
    )
    close = getattr(browser, "close", None)
    if close is not None:
        result = close()
        if hasattr(result, "__await__"):
            await result


def _read_storage_state(path: Path | None) -> Mapping[str, Any] | None:
    if path is None or not path.exists():
        return None
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return parsed if isinstance(parsed, Mapping) else None


def _cookie_is_unexpired(cookie: Mapping[str, Any], *, now_timestamp: float) -> bool:
    expires = cookie.get("expires")
    if expires in (None, ""):
        return True
    try:
        expires_value = float(expires)
    except (TypeError, ValueError):
        return False
    # Playwright uses -1 for session cookies; any positive timestamp must be in
    # the future to prove the persisted source session is still usable.
    return expires_value < 0 or expires_value > now_timestamp


def _td_storage_state_auth_status(store: RebuildStore) -> str:
    path = _storage_state_path(store)
    state = _read_storage_state(path)
    if not state:
        return "unauthorized"
    cookies = state.get("cookies")
    now_timestamp = aware_now(get_timezone()).timestamp()
    has_unexpired_cookie = isinstance(cookies, list) and any(
        isinstance(cookie, Mapping)
        and _cookie_is_unexpired(cookie, now_timestamp=now_timestamp)
        for cookie in cookies
    )
    if has_unexpired_cookie:
        # This only proves a persisted cookie is present. TD rebuild preflight
        # performs the real API-path probe/login/iframe validation in
        # ``_td_api_auth_readiness_status`` before treating the store as ready.
        return "storage_cookie_unexpired"
    return "unauthorized"


async def _td_api_auth_readiness_status(
    store: RebuildStore,
    *,
    run_id: str,
    logger: JsonLogger,
) -> str:
    """Validate TD auth using the same browser/API handoff as source fetches.

    A storage-state file can exist while the real TD session is stale. The
    rebuild must therefore exercise the mature TD API path: probe reusable
    session, refresh/login when necessary, resolve the reports iframe auth
    source, and validate the endpoint-specific API auth context.
    """
    td_store = store.raw_store
    storage_state_path = _storage_state_path(store)
    if td_store is None or storage_state_path is None:
        return "unauthorized"

    from playwright.async_api import async_playwright

    browser = None
    api_context = None
    try:
        async with async_playwright() as playwright:
            browser = await launch_browser(playwright=playwright, logger=logger)
            api_context = await prepare_td_api_context_for_store(
                browser=browser,
                store=td_store,
                logger=logger,
                run_id=run_id,
                run_start_date=aware_now(get_timezone()).date(),
                run_end_date=aware_now(get_timezone()).date(),
                navigate_to_report_iframe=True,
            )
            client = TdApiClient(
                store_code=store.store_code,
                context=api_context.context,
                storage_state_path=storage_state_path,
                run_id=run_id,
                structured_logger=logger,
                report_iframe_src=api_context.report_iframe_src,
            )
            auth_preparation = await client.prepare_auth_context()
            if api_context.probe_result and api_context.probe_result.valid:
                return "session_valid"
            if api_context.login_performed and api_context.session_reused:
                return "login_refresh_required"
            if api_context.report_iframe_src and auth_preparation.ready:
                return "report_iframe_auth_source_resolved"
            if api_context.report_iframe_src:
                return "report_iframe_auth_source_resolved"
            if auth_preparation.ready:
                return "session_valid"
            return "unauthorized"
    except Exception as exc:
        log_event(
            logger=logger,
            phase="order_line_items_rebuild_preflight",
            status="warning",
            message="TD auth readiness check failed",
            run_id=run_id,
            source="td",
            store_code=store.store_code,
            auth_readiness="unauthorized",
            error=str(exc),
        )
        return "unauthorized"
    finally:
        if api_context is not None:
            with contextlib.suppress(Exception):
                await api_context.context.close()
        if browser is not None:
            with contextlib.suppress(Exception):
                await browser.close()


def _uc_storage_state_auth_status(store: RebuildStore) -> str:
    state = _read_storage_state(_storage_state_path(store))
    if not state:
        return "missing_token"
    token = detect_archive_bearer_token_in_storage_state(state)
    return "token_detected" if token else "missing_token"


async def _source_auth_readiness(
    stores: Sequence[RebuildStore],
    *,
    run_id: str,
    logger: JsonLogger,
    skip_auth_preflight: bool = False,
) -> tuple[dict[str, str], list[dict[str, str]]]:
    readiness: dict[str, str] = {}
    failures: list[dict[str, str]] = []
    for store in stores:
        key = f"{store.source}:{store.store_code.upper()}"
        if skip_auth_preflight:
            status = "auth_preflight_skipped"
        elif store.source == "td":
            status = await _td_api_auth_readiness_status(
                store, run_id=run_id, logger=logger
            )
        else:
            status = _uc_storage_state_auth_status(store)
        readiness[key] = status
        if status in {"unauthorized", "missing_token"}:
            failures.append(
                {
                    "source": store.source,
                    "store_code": store.store_code,
                    "status": status,
                }
            )
    return readiness, failures


def _storage_state_concerns(stores: Sequence[RebuildStore]) -> list[dict[str, str]]:
    concerns: list[dict[str, str]] = []
    for store in stores:
        path = _storage_state_path(store)
        if path is None:
            concerns.append(
                {
                    "source": store.source,
                    "store_code": store.store_code,
                    "reason": "storage_state_path_missing",
                }
            )
            continue
        if not path.exists():
            concerns.append(
                {
                    "source": store.source,
                    "store_code": store.store_code,
                    "storage_state": str(path),
                    "reason": "storage_state_file_missing",
                }
            )
    return concerns


async def preflight_rebuild(
    *,
    database_url: str | None,
    sources: Sequence[Source],
    stores: Sequence[RebuildStore],
    start_date: date | None,
    requested_window_size_days: int | None,
    run_id: str,
    logger: JsonLogger,
    skip_auth_preflight: bool = False,
) -> RebuildPreflightResult:
    selected_stores = list(stores)
    selected_store_codes = [store.store_code for store in selected_stores]
    source_window_days_by_store: dict[tuple[Source, str], int] = {}
    missing_start_dates: list[dict[str, str]] = []
    missing_cost_centers = [
        {"source": store.source, "store_code": store.store_code}
        for store in selected_stores
        if not str(store.cost_center or "").strip()
    ]
    errors: list[str] = []
    auth_readiness: dict[str, str] = {}
    auth_failures: list[dict[str, str]] = []

    if not database_url:
        errors.append("database_url is required")
    if not selected_stores:
        errors.append("at least one selected store is required")
    if missing_cost_centers:
        errors.append("cost_center is required for every selected store")

    hydrated_stores = selected_stores
    if database_url and selected_stores and start_date is None:
        start_dates = await _load_store_start_dates(
            database_url=database_url, stores=selected_stores
        )
        hydrated_stores = [
            RebuildStore(
                source=store.source,
                store_code=store.store_code,
                cost_center=store.cost_center,
                raw_store=store.raw_store,
                sync_config=store.sync_config,
                start_date=start_dates.get((store.source, store.store_code.upper()))
                or store.start_date,
            )
            for store in selected_stores
        ]
        missing_start_dates = [
            {
                "source": store.source,
                "store_code": store.store_code,
                "cost_center": store.cost_center,
            }
            for store in hydrated_stores
            if store.start_date is None
        ]
        if missing_start_dates:
            missing_start_date_refs = ", ".join(
                f"{item['source']}:{item['store_code']} "
                f"cost_center={item['cost_center'] or '<missing>'}"
                for item in missing_start_dates
            )
            errors.append(
                "store_master.start_date is required for full-live rebuild when "
                "--start-date is omitted; missing start dates: "
                f"{missing_start_date_refs}"
            )
            log_event(
                logger=logger,
                phase="order_line_items_rebuild_preflight",
                status="error",
                message="store_master.start_date is missing for selected stores",
                run_id=run_id,
                sources=list(sources),
                stores=selected_store_codes,
                missing_start_dates=missing_start_dates,
                browser_backend=_browser_backend(),
            )

    for store in hydrated_stores:
        try:
            resolved_days = resolve_crm_source_window_days(
                sync_config=store.sync_config,
                source=store.source,
                requested_window_days=requested_window_size_days,
            )
        except Exception as exc:
            errors.append(
                f"source window size could not be resolved for "
                f"{store.source}:{store.store_code}: {exc}"
            )
            continue
        source_window_days_by_store[(store.source, store.store_code.upper())] = (
            resolved_days
        )
        if resolved_days < 1 or resolved_days > 30:
            errors.append(
                f"source window size for {store.source}:{store.store_code} must be "
                "between 1 and 30 days"
            )

    missing_storage_states = _storage_state_concerns(hydrated_stores)
    auth_readiness, auth_failures = await _source_auth_readiness(
        hydrated_stores,
        run_id=run_id,
        logger=logger,
        skip_auth_preflight=skip_auth_preflight,
    )
    if auth_failures and not skip_auth_preflight:
        auth_refs = ", ".join(
            f"{item['source']}:{item['store_code']}={item['status']}"
            for item in auth_failures
        )
        errors.append(
            "source auth readiness failed for selected stores; use "
            "--skip-auth-preflight only for an intentional operator override: "
            f"{auth_refs}"
        )
    if missing_storage_states:
        log_event(
            logger=logger,
            phase="order_line_items_rebuild_preflight",
            status="warning",
            message=(
                "Storage state is missing for one or more selected stores; "
                "CRM sync will follow the existing login/session refresh path"
            ),
            run_id=run_id,
            sources=list(sources),
            stores=selected_store_codes,
            missing_start_dates=missing_start_dates,
            missing_storage_states=missing_storage_states,
            auth_readiness=auth_readiness,
            skip_auth_preflight=skip_auth_preflight,
            browser_backend=_browser_backend(),
        )

    if not errors:
        try:
            await _check_browser_launch_contract(logger=logger)
        except Exception as exc:
            errors.append(f"browser launch contract failed: {exc}")

    if errors:
        log_event(
            logger=logger,
            phase="order_line_items_rebuild_preflight",
            status="error",
            message="order_line_items rebuild preflight failed",
            run_id=run_id,
            sources=list(sources),
            stores=selected_store_codes,
            missing_start_dates=missing_start_dates,
            missing_storage_states=missing_storage_states,
            auth_readiness=auth_readiness,
            auth_failures=auth_failures,
            skip_auth_preflight=skip_auth_preflight,
            missing_cost_centers=missing_cost_centers,
            browser_backend=_browser_backend(),
            errors=errors,
        )
        raise RuntimeError("; ".join(errors))

    log_event(
        logger=logger,
        phase="order_line_items_rebuild_preflight",
        status="warning" if missing_storage_states else "ok",
        message="order_line_items rebuild preflight completed",
        run_id=run_id,
        sources=list(sources),
        stores=selected_store_codes,
        missing_start_dates=missing_start_dates,
        missing_storage_states=missing_storage_states,
        auth_readiness=auth_readiness,
        auth_failures=auth_failures,
        skip_auth_preflight=skip_auth_preflight,
        source_window_days={
            f"{source}:{store_code}": days
            for (source, store_code), days in source_window_days_by_store.items()
        },
        browser_backend=_browser_backend(),
    )
    return RebuildPreflightResult(
        stores=hydrated_stores,
        source_window_days_by_store=source_window_days_by_store,
        missing_storage_states=missing_storage_states,
        auth_readiness=auth_readiness,
    )


async def load_rebuild_stores(
    *, sources: Sequence[Source], store_codes: Sequence[str] | None, logger: JsonLogger
) -> list[RebuildStore]:
    stores: list[RebuildStore] = []
    if "td" in sources:
        for store in await _load_td_order_stores(
            logger=logger, store_codes=store_codes
        ):
            stores.append(
                RebuildStore(
                    source="td",
                    store_code=store.store_code,
                    cost_center=store.cost_center,
                    raw_store=store,
                    sync_config=_store_sync_config(store),
                )
            )
    if "uc" in sources:
        for store in await _load_uc_order_stores(
            logger=logger, store_codes=store_codes
        ):
            stores.append(
                RebuildStore(
                    source="uc",
                    store_code=store.store_code,
                    cost_center=store.cost_center,
                    raw_store=store,
                    sync_config=_store_sync_config(store),
                )
            )
    return stores


async def run_rebuild(
    *,
    source_selection: Literal["td", "uc", "both"],
    store_codes: Sequence[str] | None,
    start_date: date | None,
    end_date: date | None,
    window_size_days: int | None,
    dry_run: bool,
    fail_on_zero_snapshot: bool = False,
    resume: bool = False,
    resume_run_id: str | None = None,
    run_id: str | None = None,
    logger: JsonLogger | None = None,
    fetch_snapshot: SnapshotFetcher = default_fetch_snapshot,
    skip_auth_preflight: bool = False,
) -> list[WindowMetrics]:
    if resume_run_id and not resume:
        raise ValueError("resume_run_id requires resume=True")
    run_id = run_id or new_run_id()
    logger = logger or get_logger(run_id)
    database_url = getattr(config, "database_url", None)
    end_date = end_date or aware_now(get_timezone()).date()
    run_date = aware_now(get_timezone())
    sources: list[Source] = (
        ["td", "uc"] if source_selection == "both" else [source_selection]
    )
    if not database_url:
        log_event(
            logger=logger,
            phase="order_line_items_rebuild_preflight",
            status="error",
            message="order_line_items rebuild preflight failed",
            run_id=run_id,
            sources=sources,
            stores=[],
            missing_start_dates=[],
            missing_storage_states=[],
            browser_backend=_browser_backend(),
            errors=["database_url is required"],
        )
        raise RuntimeError(
            "database_url is required for order_line_items historical rebuild"
        )
    stores = await load_rebuild_stores(
        sources=sources, store_codes=store_codes, logger=logger
    )
    preflight = await preflight_rebuild(
        database_url=database_url,
        sources=sources,
        stores=stores,
        start_date=start_date,
        requested_window_size_days=window_size_days,
        run_id=run_id,
        logger=logger,
        skip_auth_preflight=skip_auth_preflight,
    )
    stores = preflight.stores
    progress_rows = (
        await _fetch_progress_rows(database_url, resume_run_id=resume_run_id)
        if resume
        else {}
    )
    metrics: list[WindowMetrics] = []
    expected_windows: set[tuple[Source, str, date, date]] = set()
    successful_windows: set[tuple[Source, str, date, date]] = set()
    log_event(
        logger=logger,
        phase="order_line_items_rebuild",
        status="info",
        message="Starting order_line_items historical rebuild",
        run_id=run_id,
        sources=sources,
        stores=[s.store_code for s in stores],
        start_date=start_date.isoformat() if start_date else None,
        end_date=end_date.isoformat(),
        requested_window_size_days=window_size_days,
        max_source_window_days=30,
        dry_run=dry_run,
        fail_on_zero_snapshot=fail_on_zero_snapshot,
        resume=resume,
        resume_scope="source_store_window" if resume else None,
        resume_run_id_filter=resume_run_id,
        resume_ignores_current_run_id=bool(resume and resume_run_id is None),
    )
    if resume:
        log_event(
            logger=logger,
            phase="order_line_items_rebuild_resume_scope",
            status="info",
            message=(
                "order_line_items rebuild resume matches prior progress by "
                "source/store/window, not the current run ID"
            ),
            run_id=run_id,
            resume_scope="source_store_window",
            resume_run_id_filter=resume_run_id,
            resume_ignores_current_run_id=resume_run_id is None,
        )
    for store in stores:
        store_start_date = start_date or store.start_date
        if store_start_date is None:
            raise RuntimeError(
                f"start_date is required for {store.source}:{store.store_code} "
                "because store_master.start_date is not set"
            )
        source_window_days = preflight.source_window_days_by_store[
            (store.source, store.store_code.upper())
        ]
        windows = iter_windows(store_start_date, end_date, source_window_days)
        for window in windows:
            key = (store.source, store.store_code.upper(), window.start, window.end)
            uc_child_run_id = (
                _uc_child_run_id(run_id=run_id, store=store, window=window)
                if store.source == "uc"
                else None
            )
            expected_windows.add(key)
            existing = progress_rows.get(key)
            existing_status = _window_status(existing)
            if resume and _is_success_status(existing_status):
                successful_windows.add(key)
                log_event(
                    logger=logger,
                    phase="order_line_items_rebuild_window",
                    status="info",
                    message="Skipping previously successful order_line_items rebuild window",
                    run_id=run_id,
                    source=store.source,
                    store_code=store.store_code,
                    cost_center=store.cost_center,
                    window_start=window.start.isoformat(),
                    window_end=window.end.isoformat(),
                    uc_child_run_id=uc_child_run_id,
                    dry_run=dry_run,
                    resume=resume,
                    **_prior_progress_metadata(existing),
                )
                continue
            if (
                resume
                and existing_status
                and not should_retry_window_status(
                    status=existing_status,
                    error_message=(
                        str(existing.get("error_message") or "") if existing else None
                    ),
                    status_note=None,
                )
            ):
                log_event(
                    logger=logger,
                    phase="order_line_items_rebuild_window",
                    status="info",
                    message="Skipping non-retryable prior order_line_items rebuild window status",
                    run_id=run_id,
                    source=store.source,
                    store_code=store.store_code,
                    cost_center=store.cost_center,
                    window_start=window.start.isoformat(),
                    window_end=window.end.isoformat(),
                    uc_child_run_id=uc_child_run_id,
                    dry_run=dry_run,
                    resume=resume,
                    **_prior_progress_metadata(existing),
                )
                continue
            max_attempts = 2 if existing is None or existing_status else 1
            for attempt_no in range(1, max_attempts + 1):
                try:
                    metric = await rebuild_window(
                        source=store.source,
                        store=store,
                        window=window,
                        run_id=run_id,
                        run_date=run_date,
                        database_url=database_url,
                        dry_run=dry_run,
                        logger=logger,
                        fetch_snapshot=fetch_snapshot,
                    )
                except Exception as exc:
                    failure_class = classify_rebuild_failure(exc)
                    source_fetch_fields = _td_garments_incomplete_log_fields(exc)
                    if not dry_run:
                        await _write_progress(
                            database_url=database_url,
                            store=store,
                            window=window,
                            run_id=run_id,
                            status="failed",
                            attempt_no=attempt_no,
                            error_message=str(exc),
                            dry_run=False,
                        )
                    if (
                        failure_class == "retryable_transient_failure"
                        and attempt_no < max_attempts
                    ):
                        log_event(
                            logger=logger,
                            phase="order_line_items_rebuild_window",
                            status="warning",
                            message="Retrying order_line_items rebuild window after retryable failure",
                            run_id=run_id,
                            source=store.source,
                            store_code=store.store_code,
                            cost_center=store.cost_center,
                            window_start=window.start.isoformat(),
                            window_end=window.end.isoformat(),
                            uc_child_run_id=uc_child_run_id,
                            attempt_no=attempt_no,
                            failure_class=failure_class,
                            error_message=str(exc),
                            **source_fetch_fields,
                            dry_run=dry_run,
                        )
                        continue
                    if failure_class == "systemic_setup_failure":
                        log_event(
                            logger=logger,
                            phase="order_line_items_rebuild",
                            status="error",
                            message="Stopping order_line_items rebuild after systemic setup failure",
                            run_id=run_id,
                            source=store.source,
                            store_code=store.store_code,
                            cost_center=store.cost_center,
                            window_start=window.start.isoformat(),
                            window_end=window.end.isoformat(),
                            uc_child_run_id=uc_child_run_id,
                            attempt_no=attempt_no,
                            failure_class=failure_class,
                            error_message=str(exc),
                            **source_fetch_fields,
                            dry_run=dry_run,
                        )
                        raise
                    log_event(
                        logger=logger,
                        phase="order_line_items_rebuild_window",
                        status="error",
                        message="order_line_items rebuild window failed",
                        run_id=run_id,
                        source=store.source,
                        store_code=store.store_code,
                        cost_center=store.cost_center,
                        window_start=window.start.isoformat(),
                        window_end=window.end.isoformat(),
                        uc_child_run_id=uc_child_run_id,
                        attempt_no=attempt_no,
                        failure_class=failure_class,
                        error_message=str(exc),
                        **source_fetch_fields,
                        dry_run=dry_run,
                    )
                    break
                else:
                    metrics.append(metric)
                    successful_windows.add(key)
                    if not dry_run:
                        await _write_progress(
                            database_url=database_url,
                            store=store,
                            window=window,
                            run_id=run_id,
                            status="success",
                            attempt_no=attempt_no,
                            metrics=metric,
                            dry_run=False,
                        )
                    break
    missing_windows = sorted(
        expected_windows - successful_windows,
        key=lambda item: (item[0], item[1], item[2], item[3]),
    )
    log_event(
        logger=logger,
        phase="order_line_items_rebuild_missing_windows",
        status="warning" if missing_windows else "ok",
        message=(
            "Detected missing order_line_items rebuild windows"
            if missing_windows
            else "No missing order_line_items rebuild windows detected"
        ),
        run_id=run_id,
        missing_window_count=len(missing_windows),
        missing_windows=[
            {
                "source": source,
                "store_code": store_code,
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
            }
            for source, store_code, window_start, window_end in missing_windows
        ],
        dry_run=dry_run,
    )
    log_event(
        logger=logger,
        phase="order_line_items_rebuild",
        status="ok" if not missing_windows else "warning",
        message="Completed order_line_items historical rebuild",
        run_id=run_id,
        window_count=len(metrics),
        expected_window_count=len(expected_windows),
        missing_window_count=len(missing_windows),
        dry_run=dry_run,
        resume=resume,
    )
    if missing_windows:
        compact_missing_windows = tuple(
            _compact_window_identifier(source, store_code, window_start, window_end)
            for source, store_code, window_start, window_end in missing_windows
        )
        raise OrderLineItemsRebuildIncomplete(
            run_id=run_id,
            expected_window_count=len(expected_windows),
            completed_window_count=len(successful_windows),
            missing_window_count=len(missing_windows),
            missing_windows=compact_missing_windows,
        )

    zero_metrics = [
        metric for metric in metrics if _is_zero_authoritative_snapshot(metric)
    ]
    suspicious_zero_metrics = [
        metric
        for metric in zero_metrics
        if metric.zero_snapshot_class != "confirmed_source_empty"
    ]
    all_selected_windows_zero = bool(metrics) and len(zero_metrics) == len(metrics)
    if dry_run and all_selected_windows_zero:
        zero_windows = tuple(
            _compact_window_identifier(
                metric.source,
                metric.store_code,
                metric.window_start,
                metric.window_end,
            )
            for metric in zero_metrics
        )
        should_fail_zero_snapshot = fail_on_zero_snapshot and bool(
            suspicious_zero_metrics
        )
        log_event(
            logger=logger,
            phase="order_line_items_rebuild_zero_snapshot_summary",
            status="error" if should_fail_zero_snapshot else "warning",
            message=("all_selected_windows_zero_authoritative_orders_detected"),
            run_id=run_id,
            dry_run=dry_run,
            fail_on_zero_snapshot=fail_on_zero_snapshot,
            expected_window_count=len(expected_windows),
            zero_window_count=len(zero_metrics),
            suspicious_zero_window_count=len(suspicious_zero_metrics),
            zero_windows=[
                {
                    "source": metric.source,
                    "store_code": metric.store_code,
                    "cost_center": metric.cost_center,
                    "window_start": metric.window_start.isoformat(),
                    "window_end": metric.window_end.isoformat(),
                    "zero_snapshot_class": metric.zero_snapshot_class,
                }
                for metric in zero_metrics
            ],
        )
        if should_fail_zero_snapshot:
            raise OrderLineItemsZeroSnapshotDetected(
                run_id=run_id,
                zero_window_count=len(zero_metrics),
                expected_window_count=len(expected_windows),
                suspicious_window_count=len(suspicious_zero_metrics),
                zero_windows=zero_windows,
            )
    elif fail_on_zero_snapshot and suspicious_zero_metrics:
        zero_windows = tuple(
            _compact_window_identifier(
                metric.source,
                metric.store_code,
                metric.window_start,
                metric.window_end,
            )
            for metric in suspicious_zero_metrics
        )
        log_event(
            logger=logger,
            phase="order_line_items_rebuild_zero_snapshot_summary",
            status="error",
            message="suspicious_zero_authoritative_orders_detected",
            run_id=run_id,
            dry_run=dry_run,
            fail_on_zero_snapshot=fail_on_zero_snapshot,
            expected_window_count=len(expected_windows),
            zero_window_count=len(zero_metrics),
            suspicious_zero_window_count=len(suspicious_zero_metrics),
            zero_windows=[
                {
                    "source": metric.source,
                    "store_code": metric.store_code,
                    "cost_center": metric.cost_center,
                    "window_start": metric.window_start.isoformat(),
                    "window_end": metric.window_end.isoformat(),
                    "zero_snapshot_class": metric.zero_snapshot_class,
                }
                for metric in suspicious_zero_metrics
            ],
        )
        raise OrderLineItemsZeroSnapshotDetected(
            run_id=run_id,
            zero_window_count=len(zero_metrics),
            expected_window_count=len(expected_windows),
            suspicious_window_count=len(suspicious_zero_metrics),
            zero_windows=zero_windows,
        )
    return metrics


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Rebuild order_line_items from authoritative CRM snapshots"
    )
    parser.add_argument("--source", choices=("td", "uc", "both"), required=True)
    parser.add_argument(
        "--stores",
        nargs="*",
        default=None,
        help="Optional store codes; defaults to all sync_orders_flag stores for the selected source(s)",
    )
    parser.add_argument(
        "--start-date", "--from-date", dest="start_date", type=_parse_date, default=None
    )
    parser.add_argument(
        "--end-date",
        "--to-date",
        dest="end_date",
        type=_parse_date,
        default=None,
        help="End date (YYYY-MM-DD); defaults to the current pipeline date",
    )
    parser.add_argument(
        "--window-size",
        "--window-days",
        dest="window_size",
        type=int,
        default=None,
        help="Requested CRM source window size in days; source fetches are capped at 30 days",
    )
    parser.add_argument(
        "--fail-on-zero-snapshot",
        action="store_true",
        help=(
            "Fail when a completed window returns a suspicious zero authoritative-order snapshot; "
            "confirmed source-empty windows remain informational."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Report planned replacements without mutating order_line_items "
            "or writing live resume progress"
        ),
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help=(
            "Skip successful live windows recorded in "
            "order_line_items_rebuild_progress by source/store/window and retry "
            "retryable failures. This is not tied to the current --run-id."
        ),
    )
    parser.add_argument(
        "--resume-run-id",
        default=None,
        help=(
            "Only consider live progress rows from this prior run ID when --resume "
            "is used; useful when source/store/window resume is too broad."
        ),
    )
    parser.add_argument(
        "--fresh",
        "--ignore-progress",
        dest="fresh",
        action="store_true",
        help="Explicitly ignore order_line_items_rebuild_progress (default without --resume).",
    )
    parser.add_argument(
        "--skip-auth-preflight",
        action="store_true",
        help=(
            "Operator override: continue even when selected source/store "
            "sessions fail TD/UC auth-readiness checks. Use only after "
            "independently verifying source sessions."
        ),
    )
    parser.add_argument("--run-id", default=None)
    return parser


async def _async_entrypoint(argv: Sequence[str] | None = None) -> None:
    args = _build_parser().parse_args(list(argv) if argv is not None else None)
    if args.fresh and args.resume:
        raise SystemExit("--fresh/--ignore-progress cannot be combined with --resume")
    if args.resume_run_id and not args.resume:
        raise SystemExit("--resume-run-id requires --resume")
    try:
        await run_rebuild(
            source_selection=args.source,
            store_codes=normalize_store_codes(args.stores or []),
            start_date=args.start_date,
            end_date=args.end_date,
            window_size_days=args.window_size,
            dry_run=args.dry_run,
            fail_on_zero_snapshot=args.fail_on_zero_snapshot,
            resume=args.resume,
            resume_run_id=args.resume_run_id,
            run_id=args.run_id,
            skip_auth_preflight=args.skip_auth_preflight,
        )
    except (
        OrderLineItemsRebuildIncomplete,
        OrderLineItemsZeroSnapshotDetected,
        RuntimeError,
    ) as exc:
        raise SystemExit(1) from exc


def run(argv: Sequence[str] | None = None) -> None:
    asyncio.run(_async_entrypoint(argv))


if __name__ == "__main__":
    run()
