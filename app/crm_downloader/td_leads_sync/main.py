from __future__ import annotations

import argparse
import asyncio
import contextlib
import html
import json
import os
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

import openpyxl
import sqlalchemy as sa
from playwright.async_api import Browser, BrowserContext, Page, async_playwright

from app.common.db import session_scope
from app.common.date_utils import aware_now, get_timezone
from app.common.lead_rules import is_customer_cancelled, resolve_cancelled_flag
from app.config import config
from app.crm_downloader.browser import launch_browser
from app.crm_downloader.config import default_download_dir
from app.crm_downloader.td_leads_sync.ingest import TdLeadsIngestResult, ingest_td_crm_leads_rows
from app.crm_downloader.td_orders_sync.main import (
    TdStore,
    _load_td_order_stores,
    _normalize_json_safe,
    _perform_login,
    _probe_session,
    _wait_for_home,
    _wait_for_otp_verification,
)
from app.dashboard_downloader.json_logger import JsonLogger, get_logger, log_event, new_run_id
from app.dashboard_downloader.notifications import send_notifications_for_run
from app.dashboard_downloader.run_summary import (
    fetch_summary_for_run,
    insert_run_summary,
    missing_required_run_summary_columns,
    update_run_summary,
)

PIPELINE_NAME = "td_crm_leads_sync"
SCHEDULER_PATH = "/App/New_Admin/frmHomePickUpScheduler"
NAV_TIMEOUT_MS = 90_000
SCHEDULER_HOME_ALERT_SELECTOR = "#achrPickUp"
SCHEDULER_FALLBACK_SELECTORS: tuple[str, ...] = (
    "a[href*='frmHomePickUpScheduler.aspx']",
    "a[href*='frmHomePickUpScheduler']",
    "a[title*='PickUp']",
    "a:has-text('PickUp Scheduler')",
)
SCHEDULER_READY_SELECTORS: tuple[str, ...] = (
    "#drpStatus",
    "#grdEntry",
    "#grdCompleted",
    "#grdCanceled",
)

STATUS_CONFIG: tuple[tuple[str, str, str], ...] = (
    ("pending", "1", "#grdEntry"),
    ("completed", "2", "#grdCompleted"),
    ("cancelled", "3", "#grdCanceled"),
)

FIELD_ALIASES: Mapping[str, tuple[str, ...]] = {
    "pickup_code": ("pickup code", "pickup no.", "pickup no", "pickupno"),
    "pickup_no": ("pickup no.", "pickup no", "pickupno"),
    "customer_name": ("customer name", "name"),
    "address": ("address",),
    "mobile": ("mobile", "mobile no", "mobile no."),
    "pickup_date": ("pickup date",),
    "pickup_time": ("pickup time", "time"),
    "pickup_created_at": (
        "created date",
        "pickup created date",
        "created date/time",
        "created date time",
        "created datetime",
        "pickup created date/time",
        "pickup created datetime",
    ),
    "special_instruction": ("special instruction", "instruction"),
    "status_text": ("status",),
    "reason": ("reason",),
    "source": ("source",),
    "customer_type": ("customer type", "customer type."),
    "user": ("user",),
}

OUTPUT_COLUMNS = [
    "store_code",
    "status_bucket",
    "pickup_id",
    "pickup_no",
    "customer_name",
    "address",
    "mobile",
    "pickup_date",
    "pickup_created_text",
    "pickup_created_at",
    "pickup_time",
    "special_instruction",
    "status_text",
    "reason",
    "source",
    "customer_type",
    "user",
    "scraped_at",
]

DATETIME_LIKE_OUTPUT_COLUMNS = frozenset({"pickup_created_at", "scraped_at", "started_at", "finished_at", "created_at"})
_DB_MODE_LOGGED_RUN_IDS: set[str] = set()
_DB_MODE_LOGGED_RUN_IDS_LOCK = asyncio.Lock()
_IST = ZoneInfo("Asia/Kolkata")
_UTC = ZoneInfo("UTC")
TD_LEADS_MAX_WORKERS_DEFAULT = 2
TD_LEADS_MAX_WORKERS_MIN = 1
OPEN_STATUS_CANDIDATES: tuple[str, ...] = ("pending", "open", "new", "in_progress")
OPEN_LEADS_OUTPUT_COLUMNS: tuple[str, ...] = (
    "store_code",
    "pickup_no",
    "customer_name",
    "mobile",
    "lead_created_at",
    "lead_age_days",
    "source",
    "customer_type",
    "last_seen_status",
)
BUSINESS_DAY_CANCELLED_OUTPUT_COLUMNS: tuple[str, ...] = (
    "store_code",
    "pickup_no",
    "customer_name",
    "mobile",
    "lead_created_at",
    "cancelled_at",
    "lead_age_days_at_cancel",
    "cancel_reason",
    "cancelled_flag",
    "source",
    "customer_type",
)
COMPLETED_LEADS_TODAY_OUTPUT_COLUMNS: tuple[str, ...] = (
    "store_code",
    "pickup_no",
    "customer_name",
    "mobile",
    "lead_created_at",
    "completed_at",
    "lead_age_days_at_completion",
    "order_match_found",
    "matched_order_count",
    "matched_order_ids",
    "first_order_date",
    "last_order_date",
    "reconciliation_note",
)



TD_OPEN_LEADS_AGE_THRESHOLD_DAYS_DEFAULT = 2
TD_LEADS_ORDER_MATCH_LOOKBACK_DAYS_DEFAULT = 1
TD_LEADS_ORDER_MATCH_GRACE_DAYS_DEFAULT = 1

ACTION_REQUIRED_HIGH_AGE_OUTPUT_COLUMNS: tuple[str, ...] = (
    "store_code",
    "pickup_no",
    "customer_name",
    "mobile",
    "lead_created_at",
    "lead_age_days",
    "source",
    "customer_type",
    "last_seen_status",
)
ACTION_REQUIRED_COMPLETED_WITHOUT_ORDER_OUTPUT_COLUMNS: tuple[str, ...] = (
    "store_code",
    "pickup_no",
    "customer_name",
    "mobile",
    "lead_created_at",
    "source",
    "customer_type",
    "last_seen_status",
)


def _validate_td_reporting_payload_schema(payload: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    required_sections: dict[str, tuple[str, ...]] = {
        "open_leads": OPEN_LEADS_OUTPUT_COLUMNS,
        "cancelled_leads_today": BUSINESS_DAY_CANCELLED_OUTPUT_COLUMNS,
        "completed_leads_today": COMPLETED_LEADS_TODAY_OUTPUT_COLUMNS,
    }

    for section_name, required_columns in required_sections.items():
        section = payload.get(section_name)
        if not isinstance(section, list):
            errors.append(f"missing_or_invalid_section:{section_name}")
            continue
        for idx, row in enumerate(section):
            if not isinstance(row, Mapping):
                errors.append(f"invalid_row_type:{section_name}[{idx}]")
                continue
            missing = [column for column in required_columns if column not in row]
            if missing:
                errors.append(f"missing_columns:{section_name}[{idx}]:{','.join(missing)}")

    action_required = payload.get("action_required")
    if not isinstance(action_required, Mapping):
        errors.append("missing_or_invalid_section:action_required")
    else:
        for key, columns in (
            ("open_leads_high_age", ACTION_REQUIRED_HIGH_AGE_OUTPUT_COLUMNS),
            ("completed_without_order_match", ACTION_REQUIRED_COMPLETED_WITHOUT_ORDER_OUTPUT_COLUMNS),
        ):
            section = action_required.get(key)
            if not isinstance(section, list):
                errors.append(f"missing_or_invalid_section:action_required.{key}")
                continue
            for idx, row in enumerate(section):
                if not isinstance(row, Mapping):
                    errors.append(f"invalid_row_type:action_required.{key}[{idx}]")
                    continue
                missing = [column for column in columns if column not in row]
                if missing:
                    errors.append(f"missing_columns:action_required.{key}[{idx}]:{','.join(missing)}")
    return errors


def _resolved_open_status_buckets() -> tuple[str, ...]:
    mapped_statuses = {status.strip().lower() for status, _, _ in STATUS_CONFIG if status and status.strip()}
    return tuple(status for status in OPEN_STATUS_CANDIDATES if status in mapped_statuses)


async def fetch_open_current_td_leads(*, database_url: str, reference_ts: datetime | None = None) -> list[dict[str, Any]]:
    open_statuses = _resolved_open_status_buckets()
    if not open_statuses:
        return []

    crm_leads_current = sa.table(
        "crm_leads_current",
        sa.column("store_code"),
        sa.column("pickup_no"),
        sa.column("customer_name"),
        sa.column("mobile"),
        sa.column("pickup_created_at"),
        sa.column("source"),
        sa.column("customer_type"),
        sa.column("status_bucket"),
    )
    normalized_status_expr = sa.func.lower(sa.func.trim(crm_leads_current.c.status_bucket))

    query = (
        sa.select(
            crm_leads_current.c.store_code.label("store_code"),
            crm_leads_current.c.pickup_no.label("pickup_no"),
            crm_leads_current.c.customer_name.label("customer_name"),
            crm_leads_current.c.mobile.label("mobile"),
            crm_leads_current.c.pickup_created_at.label("lead_created_at"),
            crm_leads_current.c.source.label("source"),
            crm_leads_current.c.customer_type.label("customer_type"),
            crm_leads_current.c.status_bucket.label("last_seen_status"),
        )
        .where(normalized_status_expr.in_(open_statuses))
        .order_by(crm_leads_current.c.store_code.asc(), crm_leads_current.c.pickup_no.asc())
    )

    rows: list[dict[str, Any]] = []
    async with session_scope(database_url) as session:
        result = await session.execute(query)
        for record in result.mappings():
            payload = {column: record.get(column) for column in OPEN_LEADS_OUTPUT_COLUMNS}
            payload["lead_age_days"] = _calculate_lead_age_days(
                lead_created_at=payload.get("lead_created_at"),
                reference_ts=reference_ts or aware_now(_UTC),
            )
            rows.append(payload)
    return rows


async def fetch_business_day_cancelled_td_leads(
    *,
    database_url: str,
    reference_ts: datetime | None = None,
) -> list[dict[str, Any]]:
    day_start_utc, day_end_utc = _business_day_bounds_utc(reference_ts=reference_ts)

    crm_leads_current = sa.table(
        "crm_leads_current",
        sa.column("lead_uid"),
        sa.column("store_code"),
        sa.column("pickup_no"),
        sa.column("customer_name"),
        sa.column("mobile"),
        sa.column("pickup_created_at"),
        sa.column("reason"),
        sa.column("cancelled_flag"),
        sa.column("source"),
        sa.column("customer_type"),
    )
    crm_leads_status_events = sa.table(
        "crm_leads_status_events",
        sa.column("lead_uid"),
        sa.column("status_bucket"),
        sa.column("scraped_at"),
        sa.column("created_at"),
    )
    normalized_status_expr = sa.func.lower(sa.func.trim(crm_leads_status_events.c.status_bucket))

    cancelled_events = (
        sa.select(
            crm_leads_status_events.c.lead_uid.label("lead_uid"),
            sa.func.max(crm_leads_status_events.c.scraped_at).label("cancelled_at"),
        )
        .where(normalized_status_expr == "cancelled")
        .where(crm_leads_status_events.c.scraped_at >= day_start_utc)
        .where(crm_leads_status_events.c.scraped_at <= day_end_utc)
        .group_by(crm_leads_status_events.c.lead_uid)
        .subquery()
    )

    query = (
        sa.select(
            crm_leads_current.c.store_code.label("store_code"),
            crm_leads_current.c.pickup_no.label("pickup_no"),
            crm_leads_current.c.customer_name.label("customer_name"),
            crm_leads_current.c.mobile.label("mobile"),
            crm_leads_current.c.pickup_created_at.label("lead_created_at"),
            cancelled_events.c.cancelled_at.label("cancelled_at"),
            crm_leads_current.c.reason.label("cancel_reason"),
            crm_leads_current.c.cancelled_flag.label("cancelled_flag"),
            crm_leads_current.c.source.label("source"),
            crm_leads_current.c.customer_type.label("customer_type"),
        )
        .select_from(cancelled_events.join(crm_leads_current, crm_leads_current.c.lead_uid == cancelled_events.c.lead_uid))
        .order_by(crm_leads_current.c.store_code.asc(), crm_leads_current.c.pickup_no.asc())
    )

    rows: list[dict[str, Any]] = []
    async with session_scope(database_url) as session:
        result = await session.execute(query)
        for record in result.mappings():
            payload = {column: record.get(column) for column in BUSINESS_DAY_CANCELLED_OUTPUT_COLUMNS}
            cancelled_at = _parse_td_leads_created_datetime(payload.get("cancelled_at"))
            payload["cancelled_at"] = cancelled_at or payload.get("cancelled_at")
            payload["lead_age_days_at_cancel"] = _calculate_lead_age_days(lead_created_at=payload.get("lead_created_at"), reference_ts=cancelled_at)
            payload["cancelled_flag"] = resolve_cancelled_flag(
                cancelled_flag=payload.get("cancelled_flag"),
                reason=payload.get("cancel_reason"),
            )
            rows.append(payload)
    return rows


def _normalize_mobile_number(value: Any) -> str | None:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if not digits:
        return None
    return digits[-10:] if len(digits) >= 10 else digits


async def build_td_leads_reporting_payload(
    *,
    database_url: str | None,
    report_date: date | None = None,
    reference_ts: datetime | None = None,
    open_leads_high_age_threshold_days: int | None = None,
) -> dict[str, Any]:
    if not database_url:
        return {
            "warning": "database_url_missing",
            "open_leads": [],
            "cancelled_leads_today": [],
            "completed_leads_today": [],
            "action_required": {
                "open_leads_high_age_threshold_days": open_leads_high_age_threshold_days
                if open_leads_high_age_threshold_days is not None
                else TD_OPEN_LEADS_AGE_THRESHOLD_DAYS_DEFAULT,
                "open_leads_high_age": [],
                "completed_without_order_match": [],
            },
        }

    timezone_local = get_timezone()
    anchor_ts = reference_ts.astimezone(timezone_local) if reference_ts is not None else aware_now(timezone_local)
    if report_date is not None:
        anchor_ts = datetime.combine(report_date, datetime.min.time(), tzinfo=timezone_local).replace(
            hour=23, minute=59, second=59, microsecond=999999
        )
    day_start_local = anchor_ts.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end_local = anchor_ts.replace(hour=23, minute=59, second=59, microsecond=999999)
    day_start_utc = day_start_local.astimezone(_UTC)
    day_end_utc = day_end_local.astimezone(_UTC)

    open_leads = await fetch_open_current_td_leads(database_url=database_url, reference_ts=anchor_ts.astimezone(_UTC))
    cancelled_leads_today = await fetch_business_day_cancelled_td_leads(database_url=database_url, reference_ts=anchor_ts)

    crm_leads_current = sa.table(
        "crm_leads_current",
        sa.column("lead_uid"),
        sa.column("store_code"),
        sa.column("pickup_no"),
        sa.column("customer_name"),
        sa.column("mobile"),
        sa.column("pickup_created_at"),
    )
    crm_leads_status_events = sa.table(
        "crm_leads_status_events",
        sa.column("lead_uid"),
        sa.column("status_bucket"),
        sa.column("scraped_at"),
    )
    orders = sa.table(
        "orders",
        sa.column("store_code"),
        sa.column("mobile_number"),
        sa.column("order_number"),
        sa.column("order_date"),
    )

    completed_events = (
        sa.select(
            crm_leads_status_events.c.lead_uid.label("lead_uid"),
            sa.func.max(crm_leads_status_events.c.scraped_at).label("completed_at"),
        )
        .where(sa.func.lower(sa.func.trim(crm_leads_status_events.c.status_bucket)) == "completed")
        .where(crm_leads_status_events.c.scraped_at >= day_start_utc)
        .where(crm_leads_status_events.c.scraped_at <= day_end_utc)
        .group_by(crm_leads_status_events.c.lead_uid)
        .subquery()
    )
    completed_query = (
        sa.select(
            crm_leads_current.c.store_code,
            crm_leads_current.c.pickup_no,
            crm_leads_current.c.customer_name,
            crm_leads_current.c.mobile,
            crm_leads_current.c.pickup_created_at.label("lead_created_at"),
            completed_events.c.completed_at,
        )
        .select_from(completed_events.join(crm_leads_current, crm_leads_current.c.lead_uid == completed_events.c.lead_uid))
        .order_by(
            crm_leads_current.c.store_code.asc(),
            crm_leads_current.c.pickup_no.asc(),
            completed_events.c.completed_at.asc(),
        )
    )

    completed_leads_today: list[dict[str, Any]] = []
    async with session_scope(database_url) as session:
        completed_rows = (await session.execute(completed_query)).mappings().all()
        lookback_days = _resolve_td_order_match_lookback_days()
        grace_days = _resolve_td_order_match_grace_days()
        for row in completed_rows:
            completed_at = _parse_td_leads_created_datetime(row.get("completed_at"))
            lead_created_at = _parse_td_leads_created_datetime(row.get("lead_created_at"))
            lead_mobile = _normalize_mobile_number(row.get("mobile"))
            order_rows: list[Mapping[str, Any]] = []
            reconciliation_note: str | None = None
            if lead_mobile:
                order_query = (
                    sa.select(orders.c.order_number, orders.c.order_date)
                    .where(orders.c.store_code == row.get("store_code"))
                    .where(orders.c.mobile_number == lead_mobile)
                )

                date_predicates_applied = False
                if lead_created_at is not None:
                    lower_bound = (lead_created_at - timedelta(days=lookback_days)).astimezone(_UTC).isoformat(sep=" ")
                    order_query = order_query.where(orders.c.order_date >= lower_bound)
                    date_predicates_applied = True
                else:
                    reconciliation_note = "Date-window degraded: invalid lead_created_at; lower bound not applied"

                if completed_at is not None:
                    upper_bound = (completed_at + timedelta(days=grace_days)).astimezone(_UTC).isoformat(sep=" ")
                    order_query = order_query.where(orders.c.order_date <= upper_bound)
                    date_predicates_applied = True
                elif reconciliation_note is None:
                    reconciliation_note = "Date-window degraded: invalid completed_at; upper bound not applied"

                if reconciliation_note is None and date_predicates_applied:
                    reconciliation_note = f"Matched within date window (lookback_days={lookback_days}, grace_days={grace_days})"

                order_query = order_query.order_by(orders.c.order_date.asc(), orders.c.order_number.asc())
                order_rows = (await session.execute(order_query)).mappings().all()
            matched_order_ids = [str(order.get("order_number") or "").strip() for order in order_rows if order.get("order_number")]
            first_order_date = order_rows[0].get("order_date") if order_rows else None
            last_order_date = order_rows[-1].get("order_date") if order_rows else None
            match_found = bool(matched_order_ids)
            if not match_found and reconciliation_note is None:
                reconciliation_note = "No matching orders by store_code+mobile within date window"
            completed_leads_today.append(
                {
                    "store_code": row.get("store_code"),
                    "pickup_no": row.get("pickup_no"),
                    "customer_name": row.get("customer_name"),
                    "mobile": row.get("mobile"),
                    "lead_created_at": row.get("lead_created_at"),
                    "completed_at": completed_at or row.get("completed_at"),
                    "lead_age_days_at_completion": _calculate_lead_age_days(
                        lead_created_at=row.get("lead_created_at"),
                        reference_ts=completed_at,
                    ),
                    "order_match_found": match_found,
                    "matched_order_count": len(matched_order_ids),
                    "matched_order_ids": matched_order_ids,
                    "first_order_date": first_order_date,
                    "last_order_date": last_order_date,
                    "reconciliation_note": None if match_found and reconciliation_note and reconciliation_note.startswith("Matched within") else reconciliation_note,
                }
            )

    threshold_days = max(0, open_leads_high_age_threshold_days or TD_OPEN_LEADS_AGE_THRESHOLD_DAYS_DEFAULT)
    action_required = {
        "open_leads_high_age_threshold_days": threshold_days,
        "open_leads_high_age": [
            row for row in open_leads if isinstance(row.get("lead_age_days"), int) and int(row["lead_age_days"]) >= threshold_days
        ],
        "completed_without_order_match": [row for row in completed_leads_today if not row.get("order_match_found")],
    }

    return {
        "warning": None,
        "report_date": day_start_local.date().isoformat(),
        "reference_ts": anchor_ts.isoformat(),
        "open_leads": sorted(open_leads, key=lambda row: (str(row.get("store_code") or ""), str(row.get("pickup_no") or ""))),
        "cancelled_leads_today": sorted(
            cancelled_leads_today,
            key=lambda row: (str(row.get("store_code") or ""), str(row.get("pickup_no") or ""), str(row.get("cancelled_at") or "")),
        ),
        "completed_leads_today": completed_leads_today,
        "action_required": action_required,
    }


def _business_day_bounds_local(*, reference_ts: datetime | None = None) -> tuple[datetime, datetime]:
    timezone_local = get_timezone()
    anchor = reference_ts.astimezone(timezone_local) if reference_ts is not None else aware_now(timezone_local)
    start = anchor.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start.replace(hour=23, minute=59, second=59, microsecond=999999)
    return start, end


def _business_day_bounds_utc(*, reference_ts: datetime | None = None) -> tuple[datetime, datetime]:
    start_local, end_local = _business_day_bounds_local(reference_ts=reference_ts)
    return start_local.astimezone(_UTC), end_local.astimezone(_UTC)


def _resolve_canonical_lead_created_at(row: Mapping[str, Any]) -> datetime | None:
    canonical_created_at = _parse_td_leads_created_datetime(row.get("pickup_created_at"))
    if canonical_created_at is not None:
        return canonical_created_at

    fallback_created_at = _parse_td_leads_created_datetime(row.get("pickup_created_text"))
    if fallback_created_at is not None:
        return fallback_created_at

    return _parse_td_leads_created_datetime(row.get("pickup_date"))


def _calculate_lead_age_days(*, lead_created_at: Any, reference_ts: datetime | None) -> int | None:
    created_at = _parse_td_leads_created_datetime(lead_created_at)
    if created_at is None or reference_ts is None:
        return None

    if created_at.tzinfo is None or created_at.utcoffset() is None:
        created_at = created_at.replace(tzinfo=_UTC)
    if reference_ts.tzinfo is None or reference_ts.utcoffset() is None:
        reference_ts = reference_ts.replace(tzinfo=_UTC)

    age_days = (reference_ts.astimezone(_UTC) - created_at.astimezone(_UTC)).days
    return max(age_days, 0)


def _td_leads_bucket_rows(result: "StoreLeadResult", status_bucket: str) -> list[dict[str, Any]]:
    bucket_rows = [row for row in result.rows if str(row.get("status_bucket") or "").strip().lower() == status_bucket]
    return _sort_td_leads_bucket_rows(bucket_rows)


def _parse_td_leads_created_datetime(value: Any) -> datetime | None:
    raw_value = str(value or "").strip()
    if not raw_value:
        return None

    normalized = raw_value.replace("T", " ")
    with contextlib.suppress(ValueError):
        return datetime.fromisoformat(normalized)

    formats = (
        "%d %b %Y %I:%M:%S %p",
        "%d %b %Y %I:%M %p",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    )
    for pattern in formats:
        with contextlib.suppress(ValueError):
            return datetime.strptime(raw_value, pattern)
    return None


def _sort_td_leads_bucket_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    epoch = datetime(1970, 1, 1)

    def _sort_seconds(value: datetime) -> float:
        comparable_value = value
        if comparable_value.tzinfo is not None and comparable_value.utcoffset() is not None:
            comparable_value = comparable_value.astimezone(timezone.utc).replace(tzinfo=None)
        return (comparable_value - epoch).total_seconds()

    def _resolve_created_at(row: Mapping[str, Any]) -> datetime | None:
        created_value = row.get("pickup_created_at")
        if isinstance(created_value, datetime):
            return created_value
        created_dt = _parse_td_leads_created_datetime(created_value)
        if created_dt is not None:
            return created_dt
        created_dt = _parse_td_leads_created_datetime(row.get("pickup_created_text"))
        if created_dt is not None:
            return created_dt
        return _parse_td_leads_created_datetime(row.get("pickup_date"))

    keyed_rows: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    for idx, row in enumerate(rows):
        resolved_created_at = _resolve_created_at(row)
        canonical_text = str(
            row.get("pickup_created_text") or row.get("pickup_created_at") or row.get("pickup_date") or ""
        ).strip()
        pickup_code = str(row.get("pickup_code") or row.get("pickup_no") or row.get("pickup_id") or "").strip()
        sort_key = (
            0 if resolved_created_at is not None else 1,
            -_sort_seconds(resolved_created_at) if resolved_created_at is not None else 0.0,
            canonical_text.lower(),
            pickup_code.lower(),
            idx,
        )
        keyed_rows.append((sort_key, dict(row)))

    keyed_rows.sort(key=lambda item: item[0])
    return [item[1] for item in keyed_rows]


def _build_td_leads_section_table_html(*, section_label: str, headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    return _build_td_leads_section_table_html_with_rich_cells(
        section_label=section_label,
        headers=headers,
        rows=rows,
        rich_html_columns=None,
    )


def _build_td_leads_section_table_html_with_rich_cells(
    *,
    section_label: str,
    headers: Sequence[str],
    rows: Sequence[Sequence[Any]],
    rich_html_columns: set[int] | None,
) -> str:
    header_html = "".join(f"<th align='left'>{html.escape(column)}</th>" for column in headers)
    blocks = [
        f"<h5 style='margin:10px 0 6px 0;'>{html.escape(section_label)}</h5>",
        "<table border='1' cellpadding='6' cellspacing='0' style='border-collapse:collapse; width:100%; margin-bottom:8px;'>",
        f"<thead><tr>{header_html}</tr></thead>",
        "<tbody>",
    ]
    if not rows:
        blocks.append(f"<tr><td colspan='{len(headers)}'><em>None</em></td></tr>")
    else:
        for row in rows:
            rendered_cells: list[str] = []
            for cell_index, value in enumerate(row):
                if rich_html_columns and cell_index in rich_html_columns:
                    rendered_cells.append(f"<td>{str(value or 'None')}</td>")
                else:
                    rendered_cells.append(f"<td>{html.escape(str(value or 'None'))}</td>")
            blocks.append("<tr>" + "".join(rendered_cells) + "</tr>")
    blocks.extend(["</tbody>", "</table>"])
    return "".join(blocks)


def _is_customer_cancelled_td_lead(row: Mapping[str, Any]) -> bool:
    return is_customer_cancelled(cancelled_flag=row.get("cancelled_flag"), reason=row.get("reason"))


def _format_pickup_created_display(row: Mapping[str, Any]) -> str:
    created_text = str(row.get("pickup_created_text") or "").strip()
    if created_text:
        return created_text

    created_at = row.get("pickup_created_at")
    if isinstance(created_at, datetime):
        normalized = created_at
        if normalized.tzinfo is None or normalized.utcoffset() is None:
            normalized = normalized.replace(tzinfo=timezone.utc)
        return normalized.astimezone(timezone.utc).strftime("%d %b %Y %I:%M:%S %p UTC")

    created_at_text = str(created_at or "").strip()
    if created_at_text:
        return created_at_text

    pickup_date_text = str(row.get("pickup_date") or "").strip()
    if pickup_date_text:
        return pickup_date_text

    return "None"




def _count_td_leads_created_events(result: "StoreLeadResult") -> int:
    lead_change_details = result.lead_change_details if isinstance(result.lead_change_details, Mapping) else {}
    created_groups = lead_change_details.get("created_by_bucket") if isinstance(lead_change_details.get("created_by_bucket"), list) else []
    created_count = 0
    for group in created_groups:
        if not isinstance(group, Mapping):
            continue
        group_rows = group.get("rows") if isinstance(group.get("rows"), list) else []
        created_count += len(group_rows)
    return created_count


def _count_td_leads_status_transitions(result: "StoreLeadResult") -> int:
    lead_change_details = result.lead_change_details if isinstance(result.lead_change_details, Mapping) else {}
    transition_groups = lead_change_details.get("transitions") if isinstance(lead_change_details.get("transitions"), list) else []
    grouped_count = 0
    for group in transition_groups:
        if not isinstance(group, Mapping):
            continue
        group_rows = group.get("rows") if isinstance(group.get("rows"), list) else []
        grouped_count += len(group_rows)

    flat_count = len([item for item in result.status_transitions if isinstance(item, Mapping)])
    return max(grouped_count, flat_count)


def _td_leads_store_has_changes(result: "StoreLeadResult") -> bool:
    created_count = _count_td_leads_created_events(result)
    transition_count = _count_td_leads_status_transitions(result)
    return (created_count + transition_count) > 0


def _normalized_td_lead_payload_value(value: Any) -> str:
    normalized = str(value or "").strip()
    return normalized or "None"


def _build_td_new_lead_payload(*, store_code: str, row: Mapping[str, Any]) -> str:
    ordered_values = (
        _normalized_td_lead_payload_value(store_code),
        _normalized_td_lead_payload_value(row.get("customer_name")),
        _normalized_td_lead_payload_value(row.get("mobile")),
        _normalized_td_lead_payload_value(row.get("source")),
        _normalized_td_lead_payload_value(row.get("customer_type")),
        _normalized_td_lead_payload_value(_format_pickup_created_display(row)),
    )
    return ", ".join(ordered_values)


def _build_td_cancelled_lead_payload(*, store_code: str, row: Mapping[str, Any]) -> str:
    ordered_values = (
        _normalized_td_lead_payload_value(store_code),
        _normalized_td_lead_payload_value(row.get("customer_name")),
        _normalized_td_lead_payload_value(row.get("mobile")),
        _normalized_td_lead_payload_value(row.get("reason")),
    )
    return ", ".join(ordered_values)


def _build_td_lead_copy_control_html(*, payload: str, button_id: str) -> str:
    escaped_payload = html.escape(payload)
    payload_json = json.dumps(payload)
    onclick_js = (
        "var v="
        f"{payload_json};"
        "if(navigator.clipboard&&navigator.clipboard.writeText){"
        "navigator.clipboard.writeText(v).catch(function(){"
        "var t=document.createElement(\"textarea\");t.value=v;document.body.appendChild(t);"
        "t.select();document.execCommand(\"copy\");document.body.removeChild(t);"
        "});"
        "}else{var t=document.createElement(\"textarea\");t.value=v;document.body.appendChild(t);"
        "t.select();document.execCommand(\"copy\");document.body.removeChild(t);}"
        "return false;"
    )
    return (
        f"<a id='{html.escape(button_id, quote=True)}' href='javascript:void(0)' "
        f"onclick='{html.escape(onclick_js, quote=True)}'>📋 Copy</a>"
        f"<div style='margin-top:4px;'><code style='white-space:pre-wrap;'>{escaped_payload}</code></div>"
    )


def _build_td_cancelled_context(*, row: Mapping[str, Any]) -> str:
    cancelled_flag = str(row.get("cancelled_flag") or "").strip().lower()
    if cancelled_flag == "customer":
        classification = "Customer Cancelled"
    elif cancelled_flag == "store":
        classification = "Store Cancelled"
    else:
        classification = "Store Cancelled"
    reason = str(row.get("reason") or "").strip() or "None"
    return f"{classification} | {reason}"


def _build_td_leads_tables_html(*, summary: "LeadsRunSummary") -> str:
    ordered_results = sorted(summary.store_results.values(), key=lambda item: item.store_code)
    if not ordered_results:
        return "<div><p><em>No row-level lead details captured for this run.</em></p></div>"

    if all(not _td_leads_store_has_changes(result) for result in ordered_results):
        return "<div><p><em>No new leads/status changed across all stores.</em></p></div>"

    blocks: list[str] = ["<div>", "<h4 style='margin:16px 0 8px 0;'>Lead details by store</h4>"]
    for result in ordered_results:
        blocks.append(f"<h4 style='margin:16px 0 8px 0;'>Store {html.escape(result.store_code)}</h4>")

        if not _td_leads_store_has_changes(result):
            blocks.append("<p><em>No new leads/status changed.</em></p>")
            continue

        row_by_pickup_no = {
            str(row.get("pickup_no") or "").strip().upper(): row
            for row in result.rows
            if str(row.get("pickup_no") or "").strip()
        }
        row_by_pickup_id = {
            str(row.get("pickup_id") or "").strip().upper(): row
            for row in result.rows
            if str(row.get("pickup_id") or "").strip()
        }

        lead_change_details = result.lead_change_details if isinstance(result.lead_change_details, Mapping) else {}
        created_groups = lead_change_details.get("created_by_bucket") if isinstance(lead_change_details.get("created_by_bucket"), list) else []
        created_rows: list[list[str]] = []
        for group in created_groups:
            if not isinstance(group, Mapping):
                continue
            group_rows = group.get("rows") if isinstance(group.get("rows"), list) else []
            for created_row in group_rows:
                lead_identity = created_row.get("lead_identity") if isinstance(created_row.get("lead_identity"), Mapping) else {}
                pickup_no = str(lead_identity.get("pickup_no") or "").strip().upper()
                pickup_id = str(lead_identity.get("pickup_id") or "").strip().upper()
                matching_row = row_by_pickup_no.get(pickup_no) or row_by_pickup_id.get(pickup_id) or {}
                payload_row = {
                    "customer_name": created_row.get("customer_name") or matching_row.get("customer_name"),
                    "mobile": created_row.get("mobile") or matching_row.get("mobile"),
                    "source": created_row.get("source") or matching_row.get("source"),
                    "customer_type": created_row.get("customer_type") or matching_row.get("customer_type"),
                }
                payload = _build_td_new_lead_payload(store_code=result.store_code, row=payload_row)
                copy_id = f"td-new-copy-{html.escape(result.store_code.lower(), quote=True)}-{len(created_rows)}"
                created_rows.append(
                    [
                        payload,
                        _build_td_lead_copy_control_html(payload=payload, button_id=copy_id),
                    ]
                )

        transition_groups = (
            lead_change_details.get("transitions") if isinstance(lead_change_details.get("transitions"), list) else []
        )
        cancelled_transition_pickup_nos: list[str] = []
        seen_cancelled_transition_keys: set[tuple[str, str, str]] = set()

        def _append_cancelled_transition(*, transition: Mapping[str, Any]) -> None:
            if str(transition.get("to_status_bucket") or "").strip().lower() != "cancelled":
                return

            lead_uid = str(transition.get("lead_uid") or "").strip()
            pickup_no = str(transition.get("pickup_no") or "").strip().upper()
            if not pickup_no:
                lead_identity = transition.get("lead_identity") if isinstance(transition.get("lead_identity"), Mapping) else {}
                pickup_no = str(lead_identity.get("pickup_no") or "").strip().upper()
            dedupe_key = (lead_uid, pickup_no, "cancelled")
            if dedupe_key in seen_cancelled_transition_keys:
                return
            seen_cancelled_transition_keys.add(dedupe_key)
            cancelled_transition_pickup_nos.append(pickup_no)

        for group in transition_groups:
            if not isinstance(group, Mapping):
                continue
            if str(group.get("to_status_bucket") or "").strip().lower() != "cancelled":
                continue
            group_rows = group.get("rows") if isinstance(group.get("rows"), list) else []
            for transition_row in group_rows:
                if not isinstance(transition_row, Mapping):
                    continue
                transition_payload = dict(transition_row)
                transition_payload["to_status_bucket"] = (
                    transition_payload.get("to_status_bucket") or group.get("to_status_bucket")
                )
                _append_cancelled_transition(transition=transition_payload)

        for transition in result.status_transitions:
            if not isinstance(transition, Mapping):
                continue
            _append_cancelled_transition(transition=transition)

        def _cancelled_transition_sort_key(transition_pickup_no: str) -> tuple[Any, ...]:
            matching_row = row_by_pickup_no.get(transition_pickup_no) or {}
            created_at = _parse_td_leads_created_datetime(matching_row.get("pickup_created_at")) or _parse_td_leads_created_datetime(
                matching_row.get("pickup_created_text")
            )
            created_text = str(matching_row.get("pickup_created_text") or matching_row.get("pickup_created_at") or "").strip()
            return (
                0 if created_at is not None else 1,
                -(created_at.replace(tzinfo=None) - datetime(1970, 1, 1)).total_seconds() if created_at is not None else 0.0,
                created_text.lower(),
                transition_pickup_no.lower(),
            )

        cancelled_transition_pickup_nos = sorted(cancelled_transition_pickup_nos, key=_cancelled_transition_sort_key)
        cancelled_total = len(cancelled_transition_pickup_nos)
        cancelled_detail_rows: list[list[str]] = []
        for transition_pickup_no in cancelled_transition_pickup_nos:
            matching_row = row_by_pickup_no.get(transition_pickup_no) or {}
            resolved_reason = str(matching_row.get("reason") or "").strip()
            if _is_customer_cancelled_td_lead({"reason": resolved_reason}):
                continue
            payload = _build_td_cancelled_lead_payload(store_code=result.store_code, row=matching_row)
            copy_id = f"td-cancel-copy-{html.escape(result.store_code.lower(), quote=True)}-{len(cancelled_detail_rows)}"
            cancelled_detail_rows.append(
                [
                    payload,
                    _build_td_cancelled_context(row=matching_row),
                    _build_td_lead_copy_control_html(payload=payload, button_id=copy_id),
                ]
            )

        pending_rows = _td_leads_bucket_rows(result, "pending")
        pending_detail_rows: list[list[str]] = []
        for row in pending_rows:
            created_display = _format_pickup_created_display(row)
            pending_detail_rows.append(
                [
                    str(row.get("customer_name") or "None"),
                    str(row.get("mobile") or "None"),
                    str(created_display),
                    str(row.get("source") or "None"),
                ]
            )

        blocks.append(
            _build_td_leads_section_table_html_with_rich_cells(
                section_label=f"New Leads created ({len(created_rows)})",
                headers=("Lead Details", "Copy"),
                rows=created_rows,
                rich_html_columns={1},
            )
        )
        blocks.append(
            _build_td_leads_section_table_html_with_rich_cells(
                section_label=f"Leads Marked as Cancelled ({cancelled_total} transitions this run)",
                headers=("Lead Details", "Cancellation Context", "Copy"),
                rows=cancelled_detail_rows,
                rich_html_columns={2},
            )
        )
        blocks.append(
            _build_td_leads_section_table_html(
                section_label=f"Pending Leads ({len(pending_rows)})",
                headers=("Customer Name", "Mobile Number", "Created Date/Time", "Source"),
                rows=pending_detail_rows,
            )
        )
    blocks.append("</div>")
    return "".join(blocks)


def _resolve_td_open_lead_age_threshold_days() -> int:
    return max(0, _env_int("TD_OPEN_LEADS_AGE_THRESHOLD_DAYS", TD_OPEN_LEADS_AGE_THRESHOLD_DAYS_DEFAULT))


def _resolve_td_order_match_lookback_days() -> int:
    return max(0, _env_int("TD_LEADS_ORDER_MATCH_LOOKBACK_DAYS", TD_LEADS_ORDER_MATCH_LOOKBACK_DAYS_DEFAULT))


def _resolve_td_order_match_grace_days() -> int:
    return max(0, _env_int("TD_LEADS_ORDER_MATCH_GRACE_DAYS", TD_LEADS_ORDER_MATCH_GRACE_DAYS_DEFAULT))


def _build_td_daily_reporting(summary: "LeadsRunSummary") -> dict[str, Any]:
    open_statuses = set(_resolved_open_status_buckets())
    threshold_days = _resolve_td_open_lead_age_threshold_days()
    high_age_open_leads: list[dict[str, Any]] = []
    completed_without_order_match: list[dict[str, Any]] = []

    for result in sorted(summary.store_results.values(), key=lambda item: item.store_code):
        for row in result.rows:
            status_bucket = str(row.get("status_bucket") or "").strip().lower()
            if status_bucket in open_statuses:
                lead_created_at = row.get("pickup_created_at")
                lead_age_days = _calculate_lead_age_days(lead_created_at=lead_created_at, reference_ts=aware_now(_UTC))
                if lead_age_days is not None and lead_age_days >= threshold_days:
                    high_age_open_leads.append({
                        "store_code": result.store_code,
                        "pickup_no": row.get("pickup_no"),
                        "customer_name": row.get("customer_name"),
                        "mobile": row.get("mobile"),
                        "lead_created_at": _format_pickup_created_display(row),
                        "lead_age_days": lead_age_days,
                        "source": row.get("source"),
                        "customer_type": row.get("customer_type"),
                        "last_seen_status": status_bucket or None,
                    })
            if status_bucket == "completed":
                order_match_found = bool(row.get("order_no") or row.get("order_number") or row.get("matched_order_no"))
                if not order_match_found:
                    completed_without_order_match.append({
                        "store_code": result.store_code,
                        "pickup_no": row.get("pickup_no"),
                        "customer_name": row.get("customer_name"),
                        "mobile": row.get("mobile"),
                        "lead_created_at": _format_pickup_created_display(row),
                        "source": row.get("source"),
                        "customer_type": row.get("customer_type"),
                        "last_seen_status": status_bucket,
                    })

    return {
        "open_leads_high_age_threshold_days": threshold_days,
        "open_leads_high_age": high_age_open_leads,
        "completed_leads_without_order_match": completed_without_order_match,
    }


def _build_td_action_required_html(*, daily_reporting: Mapping[str, Any]) -> str:
    threshold_days = int(daily_reporting.get("open_leads_high_age_threshold_days") or 0)
    high_age_rows_payload = daily_reporting.get("open_leads_high_age") if isinstance(daily_reporting.get("open_leads_high_age"), list) else []
    completed_rows_payload = daily_reporting.get("completed_leads_without_order_match") if isinstance(daily_reporting.get("completed_leads_without_order_match"), list) else []

    high_age_rows = [[str(row.get(column) or "None") for column in ACTION_REQUIRED_HIGH_AGE_OUTPUT_COLUMNS] for row in high_age_rows_payload if isinstance(row, Mapping)]
    completed_rows = [[str(row.get(column) or "None") for column in ACTION_REQUIRED_COMPLETED_WITHOUT_ORDER_OUTPUT_COLUMNS] for row in completed_rows_payload if isinstance(row, Mapping)]

    blocks = ["<div>", "<h4 style='margin:16px 0 8px 0;'>Action Required</h4>"]
    blocks.append(
        _build_td_leads_section_table_html(
            section_label=f"Open leads with high age ({threshold_days}+ days) ({len(high_age_rows)})",
            headers=("Store Code", "Pickup No", "Customer Name", "Mobile", "Lead Created At", "Lead Age (Days)", "Source", "Customer Type", "Last Seen Status"),
            rows=high_age_rows,
        )
    )
    blocks.append(
        _build_td_leads_section_table_html(
            section_label=f"Completed leads without order match ({len(completed_rows)})",
            headers=("Store Code", "Pickup No", "Customer Name", "Mobile", "Lead Created At", "Source", "Customer Type", "Last Seen Status"),
            rows=completed_rows,
        )
    )
    blocks.append("</div>")
    return "".join(blocks)


def _resolve_daily_reporting_for_mode(
    *,
    summary: "LeadsRunSummary",
    reporting_mode: str | None,
    reporting_payload: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if reporting_mode in {"meeting", "day_end"} and isinstance(reporting_payload, Mapping):
        action_required = reporting_payload.get("action_required")
        if isinstance(action_required, Mapping):
            return {
                "open_leads_high_age_threshold_days": action_required.get("open_leads_high_age_threshold_days"),
                "open_leads_high_age": action_required.get("open_leads_high_age", []),
                "cancelled_leads_today": reporting_payload.get("cancelled_leads_today", []),
                "completed_leads_without_order_match": action_required.get("completed_without_order_match", []),
            }
    return _build_td_daily_reporting(summary)


def _build_td_leads_summary_html(
    *,
    summary: "LeadsRunSummary",
    duration_human: str,
    reporting_mode: str | None = None,
    reporting_payload: Mapping[str, Any] | None = None,
) -> str:
    ordered_results = sorted(summary.store_results.values(), key=lambda item: item.store_code)
    total_stores = len(ordered_results)
    total_leads = summary.total_rows()
    daily_reporting = _resolve_daily_reporting_for_mode(
        summary=summary,
        reporting_mode=reporting_mode,
        reporting_payload=reporting_payload,
    )
    blocks = [
        "<div>",
        "<h3>TD CRM Leads Sync Summary</h3>",
        "<ul>",
        f"<li><strong>Total stores processed:</strong> {total_stores}</li>",
        f"<li><strong>Total leads:</strong> {total_leads}</li>",
        f"<li><strong>Runtime duration:</strong> {html.escape(duration_human)}</li>",
        "</ul>",
        "<p style='margin:4px 0 0 0; font-size:12px; color:#555555;'>",
        f"Reference run_id: <code>{html.escape(summary.run_id)}</code>",
        "</p>",
        _build_td_leads_tables_html(summary=summary),
        _build_td_action_required_html(daily_reporting=daily_reporting),
        "</div>",
    ]
    return "".join(blocks)



@dataclass
class StoreLeadResult:
    store_code: str
    rows: list[dict[str, Any]] = field(default_factory=list)
    status_counts: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    status: str = "ok"
    message: str = ""
    artifact_path: str | None = None
    ingested_rows: int = 0
    bucket_write_counts: dict[str, dict[str, int]] = field(default_factory=dict)
    pickup_created_at_null_count: int = 0
    pickup_created_at_null_counts_by_bucket: dict[str, int] = field(default_factory=dict)
    status_transitions: list[dict[str, Any]] = field(default_factory=list)
    lead_change_details: dict[str, Any] = field(default_factory=dict)
    task_stub: dict[str, Any] | None = None


@dataclass
class LeadsRunSummary:
    run_id: str
    run_env: str
    report_date: date
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    store_results: dict[str, StoreLeadResult] = field(default_factory=dict)

    def overall_status(self) -> str:
        statuses = [result.status for result in self.store_results.values()]
        if any(status == "error" for status in statuses):
            return "failed"
        if any(status == "warning" for status in statuses):
            return "success_with_warnings"
        return "success"

    def total_rows(self) -> int:
        return sum(len(result.rows) for result in self.store_results.values())

    def has_new_leads(self) -> bool:
        return any(_count_td_leads_created_events(result) > 0 for result in self.store_results.values())

    def build_record(
        self,
        *,
        finished_at: datetime,
        reporting_mode: str | None = None,
        reporting_payload: Mapping[str, Any] | None = None,
        reporting_schema_errors: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        elapsed_seconds = max(0, int((finished_at - self.started_at).total_seconds()))
        hh, mm, ss = elapsed_seconds // 3600, (elapsed_seconds % 3600) // 60, elapsed_seconds % 60
        duration_human = f"{hh:02d}:{mm:02d}:{ss:02d}"

        store_rows_payload = [
            {
                "store_code": result.store_code,
                "status": result.status,
                "status_counts": dict(result.status_counts),
                "rows_count": len(result.rows),
                "warnings": list(result.warnings),
                "artifact_path": result.artifact_path,
                "ingested_rows": result.ingested_rows,
                "bucket_write_counts": dict(result.bucket_write_counts),
                "pickup_created_at_null_count": result.pickup_created_at_null_count,
                "pickup_created_at_null_counts_by_bucket": dict(result.pickup_created_at_null_counts_by_bucket),
                "status_transitions": list(result.status_transitions),
                "lead_change_details": dict(result.lead_change_details),
                "task_stub": dict(result.task_stub or {}),
            }
            for result in self.store_results.values()
        ]
        pickup_created_at_null_count = sum(result.pickup_created_at_null_count for result in self.store_results.values())
        pickup_created_at_null_counts_by_bucket: dict[str, int] = {}
        for result in self.store_results.values():
            for bucket, count in result.pickup_created_at_null_counts_by_bucket.items():
                pickup_created_at_null_counts_by_bucket[bucket] = pickup_created_at_null_counts_by_bucket.get(bucket, 0) + int(
                    count
                )

        ordered_store_results = sorted(self.store_results.values(), key=lambda item: item.store_code)
        summary_lines = [
            "TD CRM Leads Sync Summary",
            f"Run ID: {self.run_id}",
            f"Env: {self.run_env}",
            f"Report Date: {self.report_date.isoformat()}",
            f"Overall Status: {self.overall_status()}",
            f"Total Stores Processed: {len(ordered_store_results)}",
            f"Total Leads: {self.total_rows()}",
            f"Duration: {duration_human}",
            "Per-store totals:",
        ]
        if reporting_mode in {"meeting", "day_end"}:
            summary_lines.append(f"Reporting Mode: {reporting_mode}")
            summary_lines.append("Frozen day-report datasets attached in metrics_json.frozen_day_report_datasets")
        for result in ordered_store_results:
            summary_lines.append(
                f"- {result.store_code}: status={result.status}, rows={len(result.rows)}, "
                f"pending={result.status_counts.get('pending', 0)}, "
                f"completed={result.status_counts.get('completed', 0)}, "
                f"cancelled={result.status_counts.get('cancelled', 0)}"
            )
            if result.warnings:
                for warning in result.warnings:
                    summary_lines.append(f"  warning={warning}")
        daily_reporting = _resolve_daily_reporting_for_mode(
            summary=self,
            reporting_mode=reporting_mode,
            reporting_payload=reporting_payload,
        )
        summary_html = _build_td_leads_summary_html(
            summary=self,
            duration_human=duration_human,
            reporting_mode=reporting_mode,
            reporting_payload=reporting_payload,
        )
        lead_tables_html = _build_td_leads_tables_html(summary=self)

        frozen_day_report_datasets: dict[str, Any] | None = None
        if reporting_mode in {"meeting", "day_end"}:
            frozen_day_report_datasets = {
                "reporting_mode": reporting_mode,
                "generated_at": finished_at.isoformat(),
                "report_date": self.report_date.isoformat(),
                "daily_reporting": _normalize_json_safe(daily_reporting),
                "action_required": _build_td_action_required_html(daily_reporting=daily_reporting),
            }

        return {
            "pipeline_name": PIPELINE_NAME,
            "run_id": self.run_id,
            "run_env": self.run_env,
            "started_at": self.started_at,
            "finished_at": finished_at,
            "total_time_taken": duration_human,
            "report_date": self.report_date,
            "overall_status": self.overall_status(),
            "summary_text": "\n".join(summary_lines),
            "phases_json": _normalize_json_safe({"store": {"ok": 0, "warning": 0, "error": 0}}),
            "metrics_json": _normalize_json_safe(
                {
                    "total_rows": self.total_rows(),
                    "duration_seconds": elapsed_seconds,
                    "duration_human": duration_human,
                    "has_new_leads": self.has_new_leads(),
                    "pickup_created_at_null_count": pickup_created_at_null_count,
                    "pickup_created_at_null_counts_by_bucket": pickup_created_at_null_counts_by_bucket,
                    "summary_html": summary_html,
                    "lead_tables_html": lead_tables_html,
                    "daily_reporting": daily_reporting,
                    "frozen_day_report_datasets": frozen_day_report_datasets,
                    "reporting_schema_errors": list(reporting_schema_errors or []),
                    "stores": store_rows_payload,
                    "lead_change_details": {
                        result.store_code: dict(result.lead_change_details) for result in self.store_results.values()
                    },
                    "task_stubs": [dict(result.task_stub or {}) for result in self.store_results.values()],
                }
            ),
            "created_at": self.started_at,
        }


@dataclass
class TdLeadsStoreWorkerResult:
    store_code: str
    result: StoreLeadResult
    queued_at: datetime
    queue_wait_ms: int
    duration_ms: int


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _resolve_td_leads_concurrency_settings() -> tuple[int, int, bool]:
    configured_workers = max(TD_LEADS_MAX_WORKERS_MIN, _env_int("TD_LEADS_MAX_WORKERS", TD_LEADS_MAX_WORKERS_DEFAULT))
    parallel_enabled = _env_bool("TD_LEADS_PARALLEL_ENABLED", default=True)
    effective_workers = configured_workers if parallel_enabled else 1
    return configured_workers, effective_workers, parallel_enabled and effective_workers > 1


async def _persist_run_summary(
    *,
    logger: JsonLogger,
    summary: LeadsRunSummary,
    finished_at: datetime,
    reporting_mode: str | None = None,
    reporting_payload: Mapping[str, Any] | None = None,
    reporting_schema_errors: Sequence[str] | None = None,
) -> bool:
    if not config.database_url:
        log_event(
            logger=logger,
            phase="run_summary",
            status="warning",
            message="Skipping run summary persistence because database_url is missing",
            run_id=summary.run_id,
        )
        return False

    record = summary.build_record(
        finished_at=finished_at,
        reporting_mode=reporting_mode,
        reporting_payload=reporting_payload,
        reporting_schema_errors=reporting_schema_errors,
    )
    try:
        existing = await fetch_summary_for_run(config.database_url, summary.run_id)
        if existing:
            await update_run_summary(config.database_url, summary.run_id, record)
            action = "updated"
        else:
            try:
                await insert_run_summary(config.database_url, record)
                action = "inserted"
            except Exception:
                missing_columns = missing_required_run_summary_columns(record)
                if missing_columns:
                    log_event(
                        logger=logger,
                        phase="run_summary",
                        status="error",
                        message="Run summary insert missing required columns",
                        run_id=summary.run_id,
                        missing_columns=missing_columns,
                    )
                await update_run_summary(config.database_url, summary.run_id, record)
                action = "updated"
        log_event(
            logger=logger,
            phase="run_summary",
            message=f"Run summary {action}",
            run_id=summary.run_id,
            overall_status=summary.overall_status(),
        )
        return True
    except Exception as exc:
        log_event(
            logger=logger,
            phase="run_summary",
            status="error",
            message="Failed to persist run summary",
            run_id=summary.run_id,
            error=str(exc),
        )
        return False


async def _start_run_summary(*, logger: JsonLogger, summary: LeadsRunSummary) -> None:
    if not config.database_url:
        return
    started_record = {
        "pipeline_name": PIPELINE_NAME,
        "run_id": summary.run_id,
        "run_env": summary.run_env,
        "started_at": summary.started_at,
        "finished_at": summary.started_at,
        "total_time_taken": "00:00:00",
        "report_date": summary.report_date,
        "overall_status": "running",
        "summary_text": "Run started.",
        "phases_json": {},
        "metrics_json": {},
        "created_at": summary.started_at,
    }
    with contextlib.suppress(Exception):
        existing = await fetch_summary_for_run(config.database_url, summary.run_id)
        if existing:
            await update_run_summary(config.database_url, summary.run_id, started_record)
        else:
            await insert_run_summary(config.database_url, started_record)


def _scheduler_url_for_store(store_code: str) -> str:
    return f"https://subs.quickdrycleaning.com/{store_code.lower()}{SCHEDULER_PATH}"


async def _ensure_scheduler_page(page: Page, *, store: TdStore, logger: JsonLogger) -> bool:
    target_url = _scheduler_url_for_store(store.store_code)
    url_pattern = re.compile(r".*/frmHomePickUpScheduler(?:\.aspx)?(?:\?.*)?$", re.IGNORECASE)
    entry_selector = ",".join((SCHEDULER_HOME_ALERT_SELECTOR, *SCHEDULER_FALLBACK_SELECTORS))

    async def _wait_for_scheduler_ready() -> None:
        for selector in SCHEDULER_READY_SELECTORS:
            with contextlib.suppress(Exception):
                await page.wait_for_selector(selector, timeout=5_000)
                return
        await page.wait_for_selector("#drpStatus", timeout=NAV_TIMEOUT_MS)

    branch_used = "unknown"
    try:
        await page.wait_for_selector(entry_selector, timeout=15_000)

        if await page.locator(SCHEDULER_HOME_ALERT_SELECTOR).count():
            branch_used = "home_alert_click"
            async with page.expect_navigation(url=url_pattern, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS):
                await page.click(SCHEDULER_HOME_ALERT_SELECTOR)
        else:
            fallback_selector_used: str | None = None
            for selector in SCHEDULER_FALLBACK_SELECTORS:
                if await page.locator(selector).count():
                    fallback_selector_used = selector
                    break

            if fallback_selector_used:
                branch_used = f"fallback_click:{fallback_selector_used}"
                async with page.expect_navigation(url=url_pattern, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS):
                    await page.click(fallback_selector_used)
            else:
                branch_used = "fallback_direct_url"
                log_event(
                    logger=logger,
                    phase="navigation",
                    status="warning",
                    message="Pickup Scheduler click entrypoint missing; using controlled URL fallback",
                    store_code=store.store_code,
                    url=target_url,
                )
                await page.goto(target_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
                await page.wait_for_url(url_pattern, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)

        await _wait_for_scheduler_ready()
        log_event(
            logger=logger,
            phase="navigation",
            message="Opened Pickup Scheduler page",
            store_code=store.store_code,
            url=page.url,
            navigation_branch=branch_used,
        )
        return True
    except Exception as exc:
        final_url = page.url
        final_title: str | None = None
        with contextlib.suppress(Exception):
            final_title = await page.title()
        log_event(
            logger=logger,
            phase="navigation",
            status="error",
            message="Failed to open Pickup Scheduler page",
            store_code=store.store_code,
            url=target_url,
            navigation_branch=branch_used,
            awaited_selectors=list(SCHEDULER_READY_SELECTORS),
            final_url=final_url,
            final_title=final_title,
            error=str(exc),
        )
        return False


async def _scrape_grid_rows(page: Page, *, grid_selector: str) -> tuple[list[str], list[dict[str, Any]]]:
    payload = await page.evaluate(
        r"""
        ({ gridSelector }) => {
          const table = document.querySelector(gridSelector);
          if (!table) {
            return { headers: [], rows: [] };
          }

          const headerCells = Array.from(table.querySelectorAll('tr th'));
          const headers = headerCells.map((th) => (th.textContent || '').replace(/\s+/g, ' ').trim());

          const rows = [];
          const trList = Array.from(table.querySelectorAll('tr'));
          for (const tr of trList) {
            if (tr.querySelector('th')) {
              continue;
            }
            const tds = Array.from(tr.querySelectorAll('td'));
            if (!tds.length) {
              continue;
            }
            const values = tds.map((td) => (td.textContent || '').replace(/\s+/g, ' ').trim());
            const hasPagerLink = tds.some((td) => td.querySelector('a[href*="Page$"]'));
            if (hasPagerLink) {
              continue;
            }
            const hasMeaningfulValue = values.some((value) => value && value !== '\\u00a0');
            if (!hasMeaningfulValue) {
              continue;
            }
            const hiddenPickId = tr.querySelector('input[id="hdnPickID"], input[name$="hdnPickID"]');
            rows.push({
              values,
              pickup_id: hiddenPickId ? (hiddenPickId.getAttribute('value') || '').trim() : '',
            });
          }
          return { headers, rows };
        }
        """,
        {"gridSelector": grid_selector},
    )
    headers = [str(header or "").strip() for header in (payload.get("headers") or [])]
    rows = [dict(row) for row in (payload.get("rows") or [])]
    return headers, rows


async def _available_pager_args(page: Page, *, grid_selector: str) -> list[str]:
    values = await page.evaluate(
        r"""
        ({ gridSelector }) => {
          const table = document.querySelector(gridSelector);
          if (!table) {
            return [];
          }
          const links = Array.from(table.querySelectorAll('a[href*="Page$"]'));
          const args = [];
          for (const link of links) {
            const href = link.getAttribute('href') || '';
            const match = href.match(/Page\$\d+/i);
            if (match) {
              args.push(match[0]);
            }
          }
          return args;
        }
        """,
        {"gridSelector": grid_selector},
    )
    return [str(value) for value in values]


async def _postback_page_arg(page: Page, *, arg: str) -> None:
    await page.evaluate(
        r"""
        ({ eventArgument }) => {
          const targetInput = document.querySelector('input[name="__EVENTTARGET"]');
          if (targetInput) {
            targetInput.value = '';
          }
          const argInput = document.querySelector('input[name="__EVENTARGUMENT"]');
          if (argInput) {
            argInput.value = eventArgument;
          }
          if (typeof window.__doPostBack === 'function') {
            window.__doPostBack('', eventArgument);
            return;
          }
          const form = document.querySelector('form');
          if (form) {
            form.submit();
          }
        }
        """,
        {"eventArgument": arg},
    )


async def _switch_status(page: Page, *, status_value: str, grid_selector: str) -> None:
    await page.select_option("#drpStatus", value=status_value)
    with contextlib.suppress(Exception):
        await page.wait_for_load_state("domcontentloaded", timeout=NAV_TIMEOUT_MS)
    await page.wait_for_selector("#drpStatus", timeout=NAV_TIMEOUT_MS)
    await page.wait_for_timeout(500)
    with contextlib.suppress(Exception):
        await page.wait_for_selector(grid_selector, timeout=5_000)


def _normalize_header(header: str) -> str:
    normalized = str(header or "").strip().lower().replace(".", "")
    normalized = normalized.replace("/", " ")
    return " ".join(normalized.split())


def _field_from_headers(*, headers: Sequence[str], values: Sequence[str], field_name: str) -> str | None:
    aliases = FIELD_ALIASES.get(field_name, ())
    normalized_headers = [_normalize_header(header) for header in headers]
    for alias in aliases:
        normalized_alias = _normalize_header(alias)
        if normalized_alias in normalized_headers:
            idx = normalized_headers.index(normalized_alias)
            if idx < len(values):
                value = str(values[idx] or "").strip()
                return value or None
    return None


def _parse_created_datetime(created_datetime_text: str | None) -> tuple[str | None, datetime | None]:
    raw_value = str(created_datetime_text or "").strip()
    if not raw_value:
        return None, None

    normalized = " ".join(raw_value.split())
    for fmt in ("%d %b %Y %I:%M:%S %p", "%d %b %Y %I:%M %p"):
        with contextlib.suppress(ValueError):
            parsed = datetime.strptime(normalized, fmt).replace(tzinfo=_IST).astimezone(_UTC)
            return raw_value, parsed

    iso_candidate = normalized.replace("Z", "+00:00").replace(" ", "T", 1)
    with contextlib.suppress(ValueError):
        parsed = datetime.fromisoformat(iso_candidate)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            parsed = parsed.replace(tzinfo=_IST)
        return raw_value, parsed.astimezone(_UTC)

    return raw_value, None


async def _collect_status_rows(
    page: Page,
    *,
    store_code: str,
    status_bucket: str,
    status_value: str,
    grid_selector: str,
    logger: JsonLogger,
) -> tuple[list[dict[str, Any]], list[str]]:
    warnings: list[str] = []
    await _switch_status(page, status_value=status_value, grid_selector=grid_selector)

    collected: list[dict[str, Any]] = []
    visited_page_args: set[str] = set()
    guard = 0
    while guard < 100:
        guard += 1
        headers, rows_payload = await _scrape_grid_rows(page, grid_selector=grid_selector)

        scraped_at = aware_now(get_timezone())
        for raw_row in rows_payload:
            values = [str(value or "").strip() for value in raw_row.get("values") or []]
            pickup_date = _field_from_headers(headers=headers, values=values, field_name="pickup_date")
            pickup_time = _field_from_headers(headers=headers, values=values, field_name="pickup_time")
            raw_pickup_created_text = _field_from_headers(headers=headers, values=values, field_name="pickup_created_at")
            pickup_created_text, pickup_created_at = _parse_created_datetime(raw_pickup_created_text)

            row = {
                "store_code": store_code,
                "status_bucket": status_bucket,
                "pickup_id": str(raw_row.get("pickup_id") or "").strip() or None,
                "pickup_code": _field_from_headers(headers=headers, values=values, field_name="pickup_code"),
                "pickup_no": _field_from_headers(headers=headers, values=values, field_name="pickup_no"),
                "customer_name": _field_from_headers(headers=headers, values=values, field_name="customer_name"),
                "address": _field_from_headers(headers=headers, values=values, field_name="address"),
                "mobile": _field_from_headers(headers=headers, values=values, field_name="mobile"),
                "pickup_date": pickup_date,
                "pickup_time": pickup_time,
                "pickup_created_text": pickup_created_text,
                "pickup_created_at": pickup_created_at,
                "special_instruction": _field_from_headers(headers=headers, values=values, field_name="special_instruction"),
                "status_text": _field_from_headers(headers=headers, values=values, field_name="status_text"),
                "reason": _field_from_headers(headers=headers, values=values, field_name="reason"),
                "source": _field_from_headers(headers=headers, values=values, field_name="source"),
                "customer_type": _field_from_headers(headers=headers, values=values, field_name="customer_type"),
                "user": _field_from_headers(headers=headers, values=values, field_name="user"),
                "scraped_at": scraped_at,
            }
            collected.append(row)

        pager_args = await _available_pager_args(page, grid_selector=grid_selector)
        next_arg = next((arg for arg in pager_args if arg not in visited_page_args), None)
        if not next_arg:
            break
        visited_page_args.add(next_arg)
        try:
            await _postback_page_arg(page, arg=next_arg)
            await page.wait_for_load_state("domcontentloaded", timeout=NAV_TIMEOUT_MS)
            await page.wait_for_selector("#drpStatus", timeout=NAV_TIMEOUT_MS)
        except Exception as exc:
            warnings.append(f"pagination_failed:{status_bucket}:{next_arg}:{exc}")
            break

    if guard >= 100:
        warnings.append(f"pagination_guard_limit_reached:{status_bucket}")

    log_event(
        logger=logger,
        phase="scrape",
        message="Collected status bucket rows",
        store_code=store_code,
        status_bucket=status_bucket,
        rows=len(collected),
        warnings=warnings or None,
    )
    return collected, warnings


def _write_store_artifact(
    *,
    store_code: str,
    rows: Sequence[Mapping[str, Any]],
    output_dir: Path,
    logger: JsonLogger,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{store_code}-crm_leads.xlsx"
    temp_output_path = output_dir / f"{store_code}-crm_leads.xlsx.tmp"

    sanitized_rows = _sanitize_rows_for_xlsx_export(rows=rows)
    tz_aware_columns = _find_tz_aware_columns(rows=sanitized_rows, columns=OUTPUT_COLUMNS)
    if tz_aware_columns:
        log_event(
            logger=logger,
            phase="artifact",
            status="error",
            message="TD leads XLSX payload contains timezone-aware datetimes after sanitization",
            store_code=store_code,
            tz_aware_columns=sorted(tz_aware_columns),
        )
        raise ValueError(
            "TD leads XLSX export payload still contains timezone-aware datetime values in columns: "
            + ", ".join(sorted(tz_aware_columns))
        )

    workbook = openpyxl.Workbook()
    try:
        sheet = workbook.active
        sheet.title = "crm_leads"
        sheet.append(OUTPUT_COLUMNS)
        for row in sanitized_rows:
            sheet.append([row.get(column) for column in OUTPUT_COLUMNS])

        with contextlib.suppress(FileNotFoundError):
            temp_output_path.unlink()
        workbook.save(temp_output_path)
        workbook.close()
        os.replace(temp_output_path, output_path)
    except Exception as exc:
        with contextlib.suppress(FileNotFoundError):
            temp_output_path.unlink()
        log_event(
            logger=logger,
            phase="artifact",
            status="error",
            message="artifact_write_failed",
            store_code=store_code,
            artifact_path=str(temp_output_path),
            error=str(exc),
        )
        raise
    finally:
        with contextlib.suppress(Exception):
            workbook.close()
    return output_path


def _is_tz_aware_datetime(value: Any) -> bool:
    return isinstance(value, datetime) and value.tzinfo is not None and value.utcoffset() is not None


def _normalize_datetime_like_for_xlsx(value: Any) -> Any:
    if _is_tz_aware_datetime(value):
        return value.replace(tzinfo=None)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return value
        normalized = stripped.replace("Z", "+00:00")
        with contextlib.suppress(ValueError):
            parsed = datetime.fromisoformat(normalized)
            if _is_tz_aware_datetime(parsed):
                return parsed.replace(tzinfo=None)
    return value


def _sanitize_rows_for_xlsx_export(*, rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    sanitized_rows: list[dict[str, Any]] = []
    for row in rows:
        sanitized = dict(row)
        for column in DATETIME_LIKE_OUTPUT_COLUMNS:
            if column in sanitized:
                sanitized[column] = _normalize_datetime_like_for_xlsx(sanitized[column])
        sanitized_rows.append(sanitized)
    return sanitized_rows


def _find_tz_aware_columns(*, rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> set[str]:
    tz_aware_columns: set[str] = set()
    for row in rows:
        for column in columns:
            if _is_tz_aware_datetime(row.get(column)):
                tz_aware_columns.add(column)
    return tz_aware_columns


def _database_dialect_name(database_url: str) -> str:
    drivername = sa.engine.make_url(database_url).drivername
    return drivername.split("+", maxsplit=1)[0]


def _is_async_database_url(database_url: str) -> bool:
    return "+" in sa.engine.make_url(database_url).drivername


async def _run_store(
    *,
    browser: Browser,
    store: TdStore,
    run_id: str,
    run_env: str,
    logger: JsonLogger,
) -> StoreLeadResult:
    result = StoreLeadResult(store_code=store.store_code)
    context: BrowserContext | None = None

    try:
        storage_state_exists = store.storage_state_path.exists()
        context = await browser.new_context(
            storage_state=str(store.storage_state_path) if storage_state_exists else None,
            accept_downloads=False,
        )
        page = await context.new_page()

        session_reused = False
        login_performed = False
        verification_seen = False
        if storage_state_exists:
            probe_result = await _probe_session(page, store=store, logger=logger, timeout_ms=NAV_TIMEOUT_MS)
            verification_seen = bool(probe_result.verification_seen)
            if probe_result.valid:
                session_reused = True
            else:
                session_reused = await _perform_login(page, store=store, logger=logger, nav_timeout_ms=NAV_TIMEOUT_MS)
                login_performed = True
        else:
            session_reused = await _perform_login(page, store=store, logger=logger, nav_timeout_ms=NAV_TIMEOUT_MS)
            login_performed = True

        if not session_reused:
            result.status = "error"
            result.message = "Login failed with provided credentials"
            return result

        if login_performed or verification_seen:
            verification_ok, verification_seen = await _wait_for_otp_verification(
                page,
                store=store,
                logger=logger,
                nav_selector=store.reports_nav_selector,
            )
            if not verification_ok:
                result.status = "error"
                result.message = "OTP was not completed before dwell deadline"
                return result

        home_ready = await _wait_for_home(
            page,
            store=store,
            logger=logger,
            nav_selector=store.reports_nav_selector,
            timeout_ms=NAV_TIMEOUT_MS,
        )
        if not home_ready:
            result.status = "error"
            result.message = "Home page not ready after login"
            return result

        store.storage_state_path.parent.mkdir(parents=True, exist_ok=True)
        await context.storage_state(path=str(store.storage_state_path))

        if not await _ensure_scheduler_page(page, store=store, logger=logger):
            result.status = "error"
            result.message = "Pickup Scheduler page could not be opened"
            return result

        all_rows: list[dict[str, Any]] = []
        warnings: list[str] = []
        status_counts: dict[str, int] = defaultdict(int)

        for status_bucket, status_value, grid_selector in STATUS_CONFIG:
            try:
                status_rows, status_warnings = await _collect_status_rows(
                    page,
                    store_code=store.store_code,
                    status_bucket=status_bucket,
                    status_value=status_value,
                    grid_selector=grid_selector,
                    logger=logger,
                )
                all_rows.extend(status_rows)
                warnings.extend(status_warnings)
                status_counts[status_bucket] += len(status_rows)
            except Exception as exc:
                warning = f"status_bucket_failed:{status_bucket}:{exc}"
                warnings.append(warning)
                log_event(
                    logger=logger,
                    phase="scrape",
                    status="warning",
                    message="Status bucket extraction failed; continuing",
                    store_code=store.store_code,
                    status_bucket=status_bucket,
                    error=str(exc),
                )

        artifact_path = _write_store_artifact(
            store_code=store.store_code,
            rows=all_rows,
            output_dir=default_download_dir(),
            logger=logger,
        )

        ingest_result: TdLeadsIngestResult | None = None
        if config.database_url:
            async with _DB_MODE_LOGGED_RUN_IDS_LOCK:
                should_log_db_mode = run_id not in _DB_MODE_LOGGED_RUN_IDS
                if should_log_db_mode:
                    _DB_MODE_LOGGED_RUN_IDS.add(run_id)
            if should_log_db_mode:
                log_event(
                    logger=logger,
                    phase="store",
                    message="TD leads ingest DB mode",
                    run_id=run_id,
                    db_dialect=_database_dialect_name(config.database_url),
                    db_url_async=_is_async_database_url(config.database_url),
                    db_code_path="async_session_scope",
                )
            ingest_result = await ingest_td_crm_leads_rows(
                rows=all_rows,
                run_id=run_id,
                run_env=run_env,
                source_file=artifact_path.name,
                database_url=config.database_url,
            )

        result.rows = all_rows
        result.status_counts = dict(status_counts)
        result.warnings = warnings
        result.artifact_path = str(artifact_path)
        result.ingested_rows = ingest_result.rows_upserted if ingest_result else 0
        result.bucket_write_counts = dict(ingest_result.bucket_write_counts) if ingest_result else {}
        result.pickup_created_at_null_count = ingest_result.pickup_created_at_null_count if ingest_result else 0
        result.pickup_created_at_null_counts_by_bucket = (
            dict(ingest_result.pickup_created_at_null_counts_by_bucket) if ingest_result else {}
        )
        result.status_transitions = list(ingest_result.status_transitions) if ingest_result else []
        result.lead_change_details = dict(ingest_result.lead_change_details) if ingest_result else {}
        result.task_stub = dict(ingest_result.task_stub) if ingest_result else None
        result.status = "warning" if warnings else "ok"
        result.message = "Completed" if not warnings else "Completed with warnings"

        log_event(
            logger=logger,
            phase="store",
            message="Completed TD leads store run",
            store_code=store.store_code,
            rows=len(all_rows),
            status_counts=dict(status_counts),
            warnings=warnings or None,
            artifact_path=str(artifact_path),
            ingested_rows=result.ingested_rows,
            pickup_created_at_null_count=result.pickup_created_at_null_count,
            pickup_created_at_null_counts_by_bucket=result.pickup_created_at_null_counts_by_bucket,
        )
        return result
    except Exception as exc:
        result.status = "error"
        result.message = f"TD leads store run failed: {exc}"
        log_event(
            logger=logger,
            phase="store",
            status="error",
            message="TD leads store run failed",
            store_code=store.store_code,
            error=str(exc),
        )
        return result
    finally:
        if context is not None:
            with contextlib.suppress(Exception):
                await context.close()


async def _run_store_worker(
    *,
    browser: Browser,
    store: TdStore,
    logger: JsonLogger,
    run_env: str,
    run_id: str,
    semaphore: asyncio.Semaphore,
) -> TdLeadsStoreWorkerResult:
    store_logger = logger.bind(store_code=store.store_code)
    queued_at = datetime.now(timezone.utc)
    await semaphore.acquire()
    queue_wait_ms = int((datetime.now(timezone.utc) - queued_at).total_seconds() * 1000)
    started_at = datetime.now(timezone.utc)
    log_event(
        logger=store_logger,
        phase="store",
        message="TD leads store worker start",
        run_id=run_id,
        queued_at=queued_at.isoformat(),
        queue_wait_ms=queue_wait_ms,
        concurrency_limited=True,
    )
    try:
        result = await _run_store(
            browser=browser,
            store=store,
            run_id=run_id,
            run_env=run_env,
            logger=store_logger,
        )
        duration_ms = int((datetime.now(timezone.utc) - started_at).total_seconds() * 1000)
        log_event(
            logger=store_logger,
            phase="store",
            message="TD leads store worker complete",
            run_id=run_id,
            queue_wait_ms=queue_wait_ms,
            duration_ms=duration_ms,
            status=result.status,
            rows=len(result.rows),
            ingested_rows=result.ingested_rows,
        )
        return TdLeadsStoreWorkerResult(
            store_code=store.store_code,
            result=result,
            queued_at=queued_at,
            queue_wait_ms=queue_wait_ms,
            duration_ms=duration_ms,
        )
    except Exception as exc:
        duration_ms = int((datetime.now(timezone.utc) - started_at).total_seconds() * 1000)
        message = f"TD leads store worker failed: {exc}"
        log_event(
            logger=store_logger,
            phase="store",
            status="error",
            message="TD leads store worker failed",
            run_id=run_id,
            queued_at=queued_at.isoformat(),
            queue_wait_ms=queue_wait_ms,
            duration_ms=duration_ms,
            error=str(exc),
        )
        return TdLeadsStoreWorkerResult(
            store_code=store.store_code,
            result=StoreLeadResult(store_code=store.store_code, status="error", message=message),
            queued_at=queued_at,
            queue_wait_ms=queue_wait_ms,
            duration_ms=duration_ms,
        )
    finally:
        semaphore.release()


async def main(
    *,
    run_env: str | None = None,
    run_id: str | None = None,
    store_codes: Sequence[str] | None = None,
    reporting_mode: str | None = None,
) -> None:
    resolved_run_id = run_id or new_run_id()
    resolved_run_env = run_env or config.run_env
    report_date = aware_now(get_timezone()).date()
    run_start_ts = datetime.now(timezone.utc)
    logger = get_logger(run_id=resolved_run_id)
    summary = LeadsRunSummary(
        run_id=resolved_run_id,
        run_env=resolved_run_env,
        report_date=report_date,
        started_at=run_start_ts,
    )
    reporting_payload: Mapping[str, Any] | None = None
    reporting_schema_errors: list[str] = []

    await _start_run_summary(logger=logger, summary=summary)
    log_event(
        logger=logger,
        phase="init",
        message="Resolved TD leads reporting mode",
        run_id=resolved_run_id,
        reporting_mode=reporting_mode,
    )

    stores = await _load_td_order_stores(logger=logger, store_codes=store_codes)
    if not stores:
        log_event(
            logger=logger,
            phase="init",
            status="warning",
            message="No TD stores with sync_orders_flag found; exiting",
            run_id=resolved_run_id,
            no_scoped_stores=True,
        )
    else:
        configured_workers, max_workers, parallel_enabled = _resolve_td_leads_concurrency_settings()
        log_event(
            logger=logger,
            phase="init",
            message="Resolved TD leads concurrency settings",
            run_id=resolved_run_id,
            configured_concurrency=configured_workers,
            effective_concurrency=max_workers,
            parallel_mode_enabled=parallel_enabled,
        )

        async with async_playwright() as playwright:
            browser = await launch_browser(playwright=playwright, logger=logger)
            try:
                semaphore = asyncio.Semaphore(max_workers)
                tasks = [
                    asyncio.create_task(
                        _run_store_worker(
                            browser=browser,
                            store=store,
                            logger=logger,
                            run_env=resolved_run_env,
                            run_id=resolved_run_id,
                            semaphore=semaphore,
                        )
                    )
                    for store in stores
                ]
                worker_results = await asyncio.gather(*tasks, return_exceptions=True)
                by_store_code: dict[str, StoreLeadResult] = {}
                for worker_result in worker_results:
                    if isinstance(worker_result, Exception):
                        log_event(
                            logger=logger,
                            phase="store",
                            status="error",
                            message="Unhandled TD leads worker exception",
                            run_id=resolved_run_id,
                            error=str(worker_result),
                        )
                        continue
                    by_store_code[worker_result.store_code] = worker_result.result

                for store in stores:
                    summary.store_results[store.store_code] = by_store_code.get(
                        store.store_code,
                        StoreLeadResult(
                            store_code=store.store_code,
                            status="error",
                            message="Store worker did not return a result",
                        ),
                    )
            finally:
                with contextlib.suppress(Exception):
                    await browser.close()

    finished_at = datetime.now(timezone.utc)
    if reporting_mode in {"meeting", "day_end"}:
        reporting_payload = await build_td_leads_reporting_payload(
            database_url=config.database_url,
            report_date=summary.report_date,
            reference_ts=finished_at,
            open_leads_high_age_threshold_days=_resolve_td_open_lead_age_threshold_days(),
        )
        reporting_schema_errors = _validate_td_reporting_payload_schema(reporting_payload)
        if reporting_schema_errors:
            log_event(
                logger=logger,
                phase="reporting",
                status="error",
                message="TD reporting payload schema validation failed",
                run_id=resolved_run_id,
                reporting_mode=reporting_mode,
                schema_errors=reporting_schema_errors,
            )

    persisted = await _persist_run_summary(
        logger=logger,
        summary=summary,
        finished_at=finished_at,
        reporting_mode=reporting_mode,
        reporting_payload=reporting_payload,
        reporting_schema_errors=reporting_schema_errors,
    )
    if persisted:
        notification_result = await send_notifications_for_run(PIPELINE_NAME, resolved_run_id)
        log_event(
            logger=logger,
            phase="notifications",
            message="TD leads notification summary",
            run_id=resolved_run_id,
            emails_planned=notification_result.get("emails_planned"),
            emails_sent=notification_result.get("emails_sent"),
            notification_errors=notification_result.get("errors"),
        )



def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the TD CRM leads sync flow")
    parser.add_argument("--run-env", dest="run_env", type=str, default=None, help="Override run environment label")
    parser.add_argument("--run-id", dest="run_id", type=str, default=None, help="Override generated run id")
    parser.add_argument("--store-code", action="append", dest="store_codes")
    parser.add_argument(
        "--reporting-mode",
        dest="reporting_mode",
        choices=("meeting", "day_end"),
        default=None,
        help="Optional reporting mode for TD leads sync",
    )
    return parser


async def _async_entrypoint(argv: Sequence[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    await main(
        run_env=args.run_env,
        run_id=args.run_id,
        store_codes=args.store_codes,
        reporting_mode=args.reporting_mode,
    )


def run(argv: Sequence[str] | None = None) -> None:
    asyncio.run(_async_entrypoint(argv))


if __name__ == "__main__":  # pragma: no cover
    run()
