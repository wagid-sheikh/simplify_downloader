from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal, Mapping, Protocol, Sequence

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from app.common.db import session_scope
from app.config import config
from app.dashboard_downloader.json_logger import JsonLogger, get_logger, log_event, new_run_id
from app.crm_downloader.td_orders_sync.garment_ingest import ingest_td_garment_rows
from app.crm_downloader.td_orders_sync.main import _load_td_order_stores
from app.crm_downloader.uc_orders_sync.gst_publish import publish_uc_gst_order_details_to_line_items
from app.crm_downloader.uc_orders_sync.main import _load_uc_order_stores

RebuildSource = Literal["TD", "UC"]
SnapshotOutcome = Literal["complete_with_rows", "complete_empty", "incomplete_or_failed"]


@dataclass(frozen=True)
class RebuildStore:
    source: RebuildSource
    store_code: str
    cost_center: str


@dataclass(frozen=True)
class RebuildWindow:
    from_date: date
    to_date: date


@dataclass
class SourceSnapshot:
    """Authoritative source snapshot for one store/window.

    TD callers provide garment rows and authoritative order outcomes. UC callers
    use the normal GST archive extraction/ingest path to stage detail/snapshot
    rows for ``run_id`` before returning; the UC replacement publisher then reads
    those staged rows exactly as routine synchronization does.
    """

    rows: list[Mapping[str, Any]] = field(default_factory=list)
    authoritative_order_scope: list[Mapping[str, Any]] = field(default_factory=list)
    replacement_allowed: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class RebuildCounts:
    inspected_orders: int = 0
    complete_empty_orders: int = 0
    incomplete_orders: int = 0
    rows_scheduled_for_deletion: int = 0
    rows_scheduled_for_insertion: int = 0
    orphan_rows: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "inspected_orders": self.inspected_orders,
            "complete_empty_orders": self.complete_empty_orders,
            "incomplete_orders": self.incomplete_orders,
            "rows_scheduled_for_deletion": self.rows_scheduled_for_deletion,
            "rows_scheduled_for_insertion": self.rows_scheduled_for_insertion,
            "orphan_rows": self.orphan_rows,
        }


@dataclass
class RebuildWindowResult:
    source: RebuildSource
    store_code: str
    window: RebuildWindow
    status: str
    counts: RebuildCounts


class SnapshotFetcher(Protocol):
    async def __call__(
        self,
        *,
        source: RebuildSource,
        store: RebuildStore,
        window: RebuildWindow,
        run_id: str,
        logger: JsonLogger,
    ) -> SourceSnapshot: ...


class ReplacementService(Protocol):
    async def __call__(
        self,
        *,
        source: RebuildSource,
        store: RebuildStore,
        window: RebuildWindow,
        snapshot: SourceSnapshot,
        run_id: str,
        run_date: datetime,
        dry_run: bool,
        database_url: str,
    ) -> RebuildCounts: ...


def rebuild_progress_table(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "order_line_item_rebuild_progress",
        metadata,
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), primary_key=True, autoincrement=True),
        sa.Column("source_system", sa.String(8), nullable=False),
        sa.Column("store_code", sa.String(16), nullable=False),
        sa.Column("window_from_date", sa.Date(), nullable=False),
        sa.Column("window_to_date", sa.Date(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("run_id", sa.Text(), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("metrics", sa.JSON()),
        sqlite_autoincrement=True,
    )


async def ensure_rebuild_progress_table(database_url: str) -> None:
    metadata = sa.MetaData()
    rebuild_progress_table(metadata)
    async with session_scope(database_url) as session:
        bind = session.bind
        if isinstance(bind, AsyncEngine):
            async with bind.begin() as conn:
                await conn.run_sync(metadata.create_all)
        elif isinstance(bind, AsyncConnection):
            await bind.run_sync(metadata.create_all)


def bounded_windows(from_date: date, to_date: date, *, window_days: int) -> list[RebuildWindow]:
    if window_days < 1:
        raise ValueError("window_days must be >= 1")
    if from_date > to_date:
        raise ValueError("from_date must be on or before to_date")
    windows: list[RebuildWindow] = []
    current = from_date
    while current <= to_date:
        end = min(current + timedelta(days=window_days - 1), to_date)
        windows.append(RebuildWindow(from_date=current, to_date=end))
        current = end + timedelta(days=1)
    return windows


def _batched(values: Sequence[Any], batch_size: int) -> list[Sequence[Any]]:
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1")
    return [values[index : index + batch_size] for index in range(0, len(values), batch_size)]


async def _is_completed(*, database_url: str, source: RebuildSource, store_code: str, window: RebuildWindow) -> bool:
    metadata = sa.MetaData()
    progress = rebuild_progress_table(metadata)
    async with session_scope(database_url) as session:
        count = await session.scalar(
            sa.select(sa.func.count()).select_from(progress).where(
                sa.and_(
                    progress.c.source_system == source,
                    progress.c.store_code == store_code,
                    progress.c.window_from_date == window.from_date,
                    progress.c.window_to_date == window.to_date,
                    progress.c.status == "completed",
                )
            )
        )
    return bool(count)


async def _record_progress(
    *,
    database_url: str,
    source: RebuildSource,
    store_code: str,
    window: RebuildWindow,
    status: str,
    run_id: str,
    metrics: Mapping[str, Any] | None = None,
) -> None:
    metadata = sa.MetaData()
    progress = rebuild_progress_table(metadata)
    async with session_scope(database_url) as session:
        await session.execute(
            sa.insert(progress).values(
                source_system=source,
                store_code=store_code,
                window_from_date=window.from_date,
                window_to_date=window.to_date,
                status=status,
                run_id=run_id,
                completed_at=datetime.now(timezone.utc) if status == "completed" else None,
                metrics=dict(metrics or {}),
            )
        )
        await session.commit()


async def default_replacement_service(
    *,
    source: RebuildSource,
    store: RebuildStore,
    window: RebuildWindow,
    snapshot: SourceSnapshot,
    run_id: str,
    run_date: datetime,
    dry_run: bool,
    database_url: str,
) -> RebuildCounts:
    if source == "TD":
        result = await ingest_td_garment_rows(
            rows=snapshot.rows,
            authoritative_order_scope=snapshot.authoritative_order_scope,
            replacement_allowed=snapshot.replacement_allowed,
            store_code=store.store_code,
            cost_center=store.cost_center,
            run_id=run_id,
            run_date=run_date,
            window_from_date=window.from_date,
            window_to_date=window.to_date,
            database_url=database_url,
            dry_run=dry_run,
        )
        return RebuildCounts(
            inspected_orders=result.authoritative_orders_inspected,
            complete_empty_orders=result.complete_empty_orders,
            incomplete_orders=result.replacement_skipped_incomplete_orders,
            rows_scheduled_for_deletion=result.deleted_final_rows,
            rows_scheduled_for_insertion=result.inserted_final_rows,
            orphan_rows=result.orphan_rows,
        )

    result = await publish_uc_gst_order_details_to_line_items(
        database_url=database_url,
        run_id=run_id,
        store_code=store.store_code,
        dry_run=dry_run,
    )
    return RebuildCounts(
        inspected_orders=result.invoices_inspected,
        complete_empty_orders=result.complete_empty_invoices,
        incomplete_orders=result.replacement_skipped_incomplete_invoices,
        rows_scheduled_for_deletion=result.deleted_final_rows,
        rows_scheduled_for_insertion=result.inserted_final_rows,
        orphan_rows=result.orphan_rows,
    )


async def unavailable_snapshot_fetcher(**_: Any) -> SourceSnapshot:
    raise RuntimeError(
        "Live rebuild extraction must be wired to the existing TD/UC browser session in the caller. "
        "Use the routine source-specific sync if you need live extraction from the CLI."
    )


async def load_rebuild_stores(
    *, source: RebuildSource, store_codes: Sequence[str] | None, logger: JsonLogger
) -> list[RebuildStore]:
    if source == "TD":
        td_stores = await _load_td_order_stores(logger=logger, store_codes=store_codes)
        return [
            RebuildStore(source="TD", store_code=store.store_code, cost_center=str(store.cost_center or ""))
            for store in td_stores
            if store.cost_center
        ]
    uc_stores = await _load_uc_order_stores(logger=logger, store_codes=store_codes)
    return [
        RebuildStore(source="UC", store_code=store.store_code, cost_center=str(store.cost_center or ""))
        for store in uc_stores
        if store.cost_center
    ]


async def run_rebuild(
    *,
    source: RebuildSource,
    from_date: date,
    to_date: date,
    stores: Sequence[RebuildStore],
    run_id: str | None = None,
    dry_run: bool = False,
    window_days: int = 7,
    store_batch_size: int = 1,
    resume: bool = True,
    database_url: str | None = None,
    logger: JsonLogger | None = None,
    snapshot_fetcher: SnapshotFetcher = unavailable_snapshot_fetcher,
    replacement_service: ReplacementService = default_replacement_service,
) -> list[RebuildWindowResult]:
    resolved_database_url = database_url or config.database_url
    if not resolved_database_url:
        raise ValueError("database_url is required for line-item rebuild")
    resolved_run_id = run_id or new_run_id()
    run_date = datetime.now(timezone.utc)
    resolved_logger = logger or get_logger(run_id=resolved_run_id)
    windows = bounded_windows(from_date, to_date, window_days=window_days)
    results: list[RebuildWindowResult] = []

    if not dry_run:
        await ensure_rebuild_progress_table(resolved_database_url)

    log_event(
        logger=resolved_logger,
        phase="line_item_rebuild_start",
        status="info",
        message="Order line-item rebuild started",
        run_id=resolved_run_id,
        source=source,
        from_date=from_date.isoformat(),
        to_date=to_date.isoformat(),
        dry_run=dry_run,
        window_days=window_days,
        store_batch_size=store_batch_size,
    )

    for store_batch in _batched(list(stores), store_batch_size):
        for store in store_batch:
            for window in windows:
                if resume and not dry_run and await _is_completed(
                    database_url=resolved_database_url, source=source, store_code=store.store_code, window=window
                ):
                    counts = RebuildCounts()
                    results.append(RebuildWindowResult(source, store.store_code, window, "skipped_completed", counts))
                    log_event(
                        logger=resolved_logger,
                        phase="line_item_rebuild_window",
                        status="info",
                        message="Skipping completed rebuild window",
                        run_id=resolved_run_id,
                        store_code=store.store_code,
                        window_from_date=window.from_date.isoformat(),
                        window_to_date=window.to_date.isoformat(),
                    )
                    continue

                log_event(
                    logger=resolved_logger,
                    phase="line_item_rebuild_fetch",
                    status="info",
                    message="Fetching authoritative line-item snapshot",
                    run_id=resolved_run_id,
                    store_code=store.store_code,
                    window_from_date=window.from_date.isoformat(),
                    window_to_date=window.to_date.isoformat(),
                )
                snapshot = await snapshot_fetcher(
                    source=source, store=store, window=window, run_id=resolved_run_id, logger=resolved_logger
                )
                counts = await replacement_service(
                    source=source,
                    store=store,
                    window=window,
                    snapshot=snapshot,
                    run_id=resolved_run_id,
                    run_date=run_date,
                    dry_run=dry_run,
                    database_url=resolved_database_url,
                )
                status = "dry_run" if dry_run else "completed"
                if not dry_run:
                    await _record_progress(
                        database_url=resolved_database_url,
                        source=source,
                        store_code=store.store_code,
                        window=window,
                        status="completed",
                        run_id=resolved_run_id,
                        metrics=counts.as_dict(),
                    )
                results.append(RebuildWindowResult(source, store.store_code, window, status, counts))
                log_event(
                    logger=resolved_logger,
                    phase="line_item_rebuild_replace",
                    status="ok",
                    message="Line-item rebuild window completed",
                    run_id=resolved_run_id,
                    store_code=store.store_code,
                    window_from_date=window.from_date.isoformat(),
                    window_to_date=window.to_date.isoformat(),
                    dry_run=dry_run,
                    **counts.as_dict(),
                )

    total = RebuildCounts()
    for result in results:
        total.inspected_orders += result.counts.inspected_orders
        total.complete_empty_orders += result.counts.complete_empty_orders
        total.incomplete_orders += result.counts.incomplete_orders
        total.rows_scheduled_for_deletion += result.counts.rows_scheduled_for_deletion
        total.rows_scheduled_for_insertion += result.counts.rows_scheduled_for_insertion
        total.orphan_rows += result.counts.orphan_rows
    log_event(
        logger=resolved_logger,
        phase="line_item_rebuild_summary",
        status="ok",
        message="Order line-item rebuild finished",
        run_id=resolved_run_id,
        source=source,
        dry_run=dry_run,
        windows_processed=len(results),
        **total.as_dict(),
    )
    return results


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Rebuild TD/UC order line items from authoritative CRM snapshots.")
    parser.add_argument("--source", choices=["TD", "UC"], required=True)
    parser.add_argument("--from-date", type=_parse_date, required=True)
    parser.add_argument("--to-date", type=_parse_date, required=True)
    parser.add_argument("--store-code", action="append", dest="store_codes")
    parser.add_argument("--window-days", type=int, default=7)
    parser.add_argument("--store-batch-size", type=int, default=1)
    parser.add_argument("--run-id")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-resume", action="store_true")
    return parser


async def async_main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_id = args.run_id or new_run_id()
    logger = get_logger(run_id=run_id)
    stores = await load_rebuild_stores(source=args.source, store_codes=args.store_codes, logger=logger)
    await run_rebuild(
        source=args.source,
        from_date=args.from_date,
        to_date=args.to_date,
        stores=stores,
        run_id=run_id,
        dry_run=args.dry_run,
        window_days=args.window_days,
        store_batch_size=args.store_batch_size,
        resume=not args.no_resume,
        logger=logger,
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(async_main(argv))


__all__ = [
    "RebuildCounts",
    "RebuildStore",
    "RebuildWindow",
    "SourceSnapshot",
    "bounded_windows",
    "ensure_rebuild_progress_table",
    "run_rebuild",
]
