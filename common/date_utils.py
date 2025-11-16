"""Shared helpers for timezone-aware report date calculations."""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from typing import Iterable, List, Tuple
from zoneinfo import ZoneInfo

DEFAULT_TIMEZONE = "Asia/Kolkata"


def get_timezone() -> ZoneInfo:
    """Return the configured pipeline timezone.

    The timezone can be overridden via the ``PIPELINE_TIMEZONE`` environment
    variable.  All pipelines (daily, weekly, monthly) rely on this helper so
    that date boundaries remain consistent regardless of the machine locale.
    """

    name = os.getenv("PIPELINE_TIMEZONE", DEFAULT_TIMEZONE)
    return ZoneInfo(name)


def aware_now(tz: ZoneInfo | None = None) -> datetime:
    """Return ``datetime.now`` in the configured timezone."""

    timezone = tz or get_timezone()
    return datetime.now(timezone)


def get_daily_report_date(reference: datetime | None = None, tz: ZoneInfo | None = None) -> date:
    """Return the standard daily report date (T-1 in the configured timezone)."""

    current = reference or aware_now(tz)
    return (current.date() - timedelta(days=1))


def get_latest_completed_week(
    reference: datetime | None = None, tz: ZoneInfo | None = None
) -> Tuple[date, date]:
    """Return the most recent fully completed Monday–Sunday window."""

    current = reference or aware_now(tz)
    # Only consider windows that fully ended before "today".
    anchor = current.date() - timedelta(days=1)
    # ``weekday`` uses Monday=0.
    start = anchor - timedelta(days=anchor.weekday())
    end = start + timedelta(days=6)
    return start, end


def get_latest_completed_month(
    reference: datetime | None = None, tz: ZoneInfo | None = None
) -> Tuple[date, date]:
    """Return the most recent fully completed calendar month."""

    current = reference or aware_now(tz)
    today = current.date()
    first_of_current = today.replace(day=1)
    last_of_previous = first_of_current - timedelta(days=1)
    start = last_of_previous.replace(day=1)
    return start, last_of_previous


def normalize_store_codes(values: Iterable[str]) -> List[str]:
    """Uppercase and de-duplicate store codes for validation helpers."""

    normalized = {value.strip().upper() for value in values if value and value.strip()}
    return sorted(normalized)
