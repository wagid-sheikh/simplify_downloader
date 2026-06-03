from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Mapping

import sqlalchemy as sa

from app.common.db import session_scope
from app.dashboard_downloader.db_tables import orders_sync_log

DEFAULT_BACKFILL_DAYS = 90
DEFAULT_WINDOW_DAYS = 90
DEFAULT_OVERLAP_DAYS = 2
CRM_SOURCE_MAX_WINDOW_DAYS = 30

TIMEOUT_NAVIGATION_RETRY_TOKENS = (
    "timeout",
    "timed out",
    "navigation failed",
    "session load",
    "session-load",
    "session timeout",
    "archive orders navigation failed",
    "loading archive orders page",
    "page.goto",
    "net::err",
)


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
    - status filter: success + success_with_warnings (partial/failed/skipped
      are ignored).
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
            .where(orders_sync_log.c.status.in_(["success", "success_with_warnings"]))
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


def resolve_crm_source_window_days(
    *,
    sync_config: Mapping[str, Any],
    source: str | None = None,
    requested_window_days: int | None = None,
    default_window_days: int = CRM_SOURCE_MAX_WINDOW_DAYS,
) -> int:
    """Resolve a CRM-source-safe window size.

    CRM source fetches must not exceed 30 days. Operators may request a
    smaller window, and store/source config may set a smaller limit. Configured
    values above the hard cap are intentionally capped instead of trusted.
    """
    _, configured_window, _ = resolve_window_settings(
        sync_config=sync_config,
        window_days=requested_window_days or default_window_days,
    )
    candidates = [configured_window, CRM_SOURCE_MAX_WINDOW_DAYS]
    if source:
        normalized = source.strip().lower()
        for key in (
            f"{normalized}_order_line_items_rebuild_window_days",
            f"{normalized}_crm_source_window_days",
            f"{normalized}_source_window_days",
        ):
            raw_value = sync_config.get(key)
            if raw_value is not None:
                try:
                    candidates.append(int(raw_value))
                except (TypeError, ValueError):
                    continue
    for key in (
        "order_line_items_rebuild_window_days",
        "crm_source_window_days",
        "source_window_days",
    ):
        raw_value = sync_config.get(key)
        if raw_value is not None:
            try:
                candidates.append(int(raw_value))
            except (TypeError, ValueError):
                continue
    return max(1, min(value for value in candidates if value and value > 0))


def build_non_overlapping_windows(
    *, start_date: date, end_date: date, window_days: int
) -> list[tuple[date, date]]:
    if window_days < 1:
        raise ValueError("window_days must be at least 1")
    if start_date > end_date:
        return []
    windows: list[tuple[date, date]] = []
    current = start_date
    while current <= end_date:
        window_end = min(current + timedelta(days=window_days - 1), end_date)
        windows.append((current, window_end))
        current = window_end + timedelta(days=1)
    return windows


def has_timeout_navigation_failure(*messages: str | None) -> bool:
    combined = " ".join(message for message in messages if message).lower()
    return any(token in combined for token in TIMEOUT_NAVIGATION_RETRY_TOKENS)


def should_retry_window_status(
    *,
    status: str,
    error_message: str | None,
    status_note: str | None,
    skip_reason: str | None = None,
) -> bool:
    if status == "failed":
        return True
    if status not in {"skipped", "partial"}:
        return False
    return has_timeout_navigation_failure(error_message, status_note, skip_reason)
