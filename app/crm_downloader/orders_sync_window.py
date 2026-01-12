from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Mapping

import sqlalchemy as sa

from app.common.db import session_scope
from app.dashboard_downloader.db_tables import orders_sync_log

DEFAULT_BACKFILL_DAYS = 90
DEFAULT_WINDOW_DAYS = 90
DEFAULT_OVERLAP_DAYS = 2


def resolve_window_settings(
    *,
    sync_config: Mapping[str, Any],
    backfill_days: int | None = None,
    window_days: int | None = None,
    overlap_days: int | None = None,
) -> tuple[int, int, int]:
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


async def fetch_last_success_window_end(
    *,
    database_url: str,
    pipeline_id: int,
    store_code: str,
) -> date | None:
    """Return the SSOT-backed last successful window end for a store.

    Selection criteria:
    - orders_sync_log rows scoped to the pipeline_id + store_code.
    - status filter: success only (partial/failed/skipped are ignored).
    - run_id is intentionally not filtered so the latest successful window
      across runs is used as the SSOT source-of-truth.
    - max(to_date) is used as the authoritative window end; overlap handling
      is applied when computing the next start date.
    """
    async with session_scope(database_url) as session:
        stmt = (
            sa.select(sa.func.max(orders_sync_log.c.to_date))
            .where(orders_sync_log.c.pipeline_id == pipeline_id)
            .where(orders_sync_log.c.store_code == store_code)
            .where(orders_sync_log.c.status == "success")
        )
        return (await session.execute(stmt)).scalar_one_or_none()


def resolve_orders_sync_start_date(
    *,
    end_date: date,
    last_success: date | None,
    overlap_days: int,
    backfill_days: int,
    window_days: int,
    from_date: date | None,
    store_start_date: date | None,
) -> date:
    """Resolve the start date for a sync run.

    Window overlap handling: if a prior successful window exists, the next
    window starts overlap_days - 1 days before the last successful end date,
    ensuring at least one day of overlap without duplicating the full window.
    """
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
