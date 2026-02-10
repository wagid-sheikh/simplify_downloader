from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Sequence
from urllib.parse import urlparse

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from openpyxl import Workbook
from playwright.async_api import Browser, ElementHandle, Locator, Page, TimeoutError, async_playwright

from app.common.date_utils import aware_now, get_timezone, normalize_store_codes
from app.common.db import session_scope
from app.config import config
from app.crm_downloader.browser import launch_browser
from app.crm_downloader.config import default_download_dir, default_profiles_dir
from app.crm_downloader.uc_orders_sync.archive_ingest import ingest_uc_archive_excels
from app.crm_downloader.uc_orders_sync.archive_publish import (
    publish_uc_archive_order_details_to_orders,
    publish_uc_archive_payments_to_sales,
)
from app.crm_downloader.orders_sync_window import (
    fetch_last_success_window_end,
    resolve_orders_sync_start_date,
    resolve_window_settings,
)
from app.dashboard_downloader.db_tables import orders_sync_log, pipelines
from app.dashboard_downloader.json_logger import JsonLogger, get_logger, log_event, new_run_id
from app.dashboard_downloader.notifications import send_notifications_for_run
from app.dashboard_downloader.run_summary import (
    fetch_summary_for_run,
    insert_run_summary,
    missing_required_run_summary_columns,
    update_run_summary,
)

PIPELINE_NAME = "uc_orders_sync"
NAV_TIMEOUT_MS = 90_000
ARCHIVE_PAGINATION_STABILIZATION_READS = 2
ARCHIVE_PAGINATION_STABILIZATION_INTERVAL_MS = 300
ARCHIVE_PAGINATION_STABILIZATION_TIMEOUT_MS = 8_000
SPINNER_CSS_SELECTORS = [".spinner", ".loading", ".loader", ".k-loading-mask"]
SPINNER_TEXT_SELECTOR = "text=/loading/i"
DOM_SNIPPET_MAX_CHARS = 600
UC_LOGIN_SELECTORS = {
    "username": "input[placeholder='Email'][type='email']",
    "password": "input[placeholder='Password'][type='password']",
    "submit": "button.btn-primary[type='submit']",
}
HOME_READY_SELECTORS = (
    "nav",
    "[role='navigation']",
    "aside",
    ".sidebar",
    ".menu",
    ".navbar",
    "header",
)
NAV_CONTAINER_SELECTORS = (
    "nav",
    "[role='navigation']",
    "aside",
    ".sidebar",
    ".menu",
    ".navbar",
)
GST_MENU_LABELS = (
    "GST Report",
    "GST Reports",
    "GST Summary",
    "GST",
)
GST_PAGE_LABEL_SELECTOR = "text=/GST Report|GST Reports|GST Summary|GST/i"
GST_CONTAINER_SELECTORS = (
    "form",
    "section",
    "div[role='form']",
    "div[class*=report]",
    "div[class*=gst]",
    "div[id*=gst]",
)
UC_MAX_WORKERS_DEFAULT = 2
GST_DATE_RANGE_READY_SELECTORS = (
    "input.search-user[placeholder*='Choose Start Date']",
    "input[placeholder='Choose Start Date - End Date']",
    "input.search-user[readonly]",
    "input.search-user",
)
GST_CONTROL_SELECTORS = {
    "date_range_input": [
        "input.search-user[readonly][placeholder='Choose Start Date - End Date']",
        "input.search-user[readonly]",
        "input.search-user",
        "input[placeholder*=' - '][readonly]",
        "input.search-user[placeholder*='Choose Start Date']",
        "input[placeholder='Choose Start Date - End Date']",
        "input[placeholder*='Start Date - End Date']",
        "input[placeholder*='Start Date'][placeholder*='End Date']",
        "input[placeholder*='Date Range']",
        "input[name*='date'][name*='range']",
    ],
    "apply_button": [
        ".calendar-body.show .apply .buttons button.btn.primary",
        ".calendar-body.show .apply button.btn.primary",
        ".apply .buttons button.btn.primary",
        ".apply button.btn.primary",
        "button.btn.primary:has-text('Apply')",
        "button.btn.primary:has-text('Search')",
        "button.btn.primary:has-text('Submit')",
        "button.btn.primary:has-text('Go')",
        "button:has-text('Apply')",
        "button:has-text('Search')",
        "button:has-text('Submit')",
        "button:has-text('Go')",
        "input[type='button'][value*='Apply']",
        "input[type='button'][value*='Search']",
        "input[type='button'][value*='Submit']",
        "input[type='button'][value*='Go']",
        "input[type='submit'][value*='Apply']",
        "input[type='submit'][value*='Search']",
        "input[type='submit'][value*='Submit']",
        "input[type='submit'][value*='Go']",
    ],
    "export_button": ["button.btn.primary.export", "button:has-text('Export Report')"],
}
GST_REPORT_TABLE_SELECTORS = (
    ".table-section",
    ".table-section tbody",
)
GST_REPORT_ROW_SELECTORS = (
    ".table-section tbody tr",
    ".table-section tbody tr.empty-row",
    ".table-section tbody tr:has(td:has-text('No data'))",
    ".table-section tbody tr:has(td:has-text('No Data'))",
    ".table-section tbody tr:has(td:has-text('No records'))",
    ".table-section tbody tr:has(td:has-text('No Records'))",
)
ARCHIVE_URL = "https://store.ucleanlaundry.com/archive"
ARCHIVE_FILTER_BUTTON_SELECTOR = "div.filter-btn"
ARCHIVE_DATE_TYPE_SELECTOR = "input[name='dateType'][value='Delivery Date']"
ARCHIVE_CUSTOM_OPTION_SELECTOR = "div.date-option:has-text('Custom')"
ARCHIVE_CUSTOM_INPUT_SELECTOR = ".custom-date-inputs input[type='date']"
ARCHIVE_TABLE_ROW_SELECTOR = ".table-wrapper .orders-table tbody tr"
ARCHIVE_ORDER_DETAIL_TRIGGER_SELECTORS = (
    "td.order-col span[style*='cursor']",
    "td.order-col span a",
    "td.order-col span button",
    "td.order-col span",
    "td.order-col button",
    "td.order-col a",
    "td.order-col [role='button']",
    "td.order-col",
)
ARCHIVE_ORDER_CODE_SELECTORS = (
    "td.order-col span[style*='cursor']",
    "td.order-col span a",
    "td.order-col span button",
    "td.order-col span",
    "td.order-col a",
    "td.order-col button",
    "td.order-col",
)
ARCHIVE_NEXT_BUTTON_SELECTOR = ".pagination-btn:has-text('Next')"
ARCHIVE_PREV_BUTTON_SELECTOR = ".pagination-btn:has-text('Prev')"
ARCHIVE_BASE_COLUMNS = [
    "store_code",
    "order_code",
    "pickup",
    "delivery",
    "customer_name",
    "customer_phone",
    "address",
    "payment_text",
    "instructions",
    "customer_source",
    "status",
    "status_date",
]
ARCHIVE_ORDER_DETAIL_COLUMNS = [
    "store_code",
    "order_code",
    "order_mode",
    "order_datetime",
    "pickup_datetime",
    "delivery_datetime",
    "service",
    "hsn_sac",
    "item_name",
    "rate",
    "quantity",
    "weight",
    "addons",
    "amount",
]
ARCHIVE_PAYMENT_COLUMNS = [
    "store_code",
    "order_code",
    "payment_mode",
    "amount",
    "payment_date",
    "transaction_id",
]
DATE_PICKER_POPUP_SELECTORS = (
    ".calendar-body.show",
    ".calendar-body .calendar",
    "mat-calendar",
    "[class*='mat-calendar']",
    ".mat-calendar-content",
    ".cdk-overlay-pane:has(mat-calendar)",
    ".cdk-overlay-pane:has(.calendar)",
    ".mat-datepicker-content:has(mat-calendar)",
    ".mat-datepicker-content:has(.calendar)",
    ".mat-date-range-picker-content:has(mat-calendar)",
    ".mat-date-range-picker-content:has(.calendar)",
)
DATE_PICKER_POPUP_FALLBACK_SELECTORS = (
    ".mat-datepicker-content",
    ".mat-date-range-picker-content",
    ".cdk-overlay-pane",
    ".mat-datepicker-popup",
    ".react-datepicker",
    ".react-datepicker__month-container",
    ".flatpickr-calendar",
    ".pika-single",
    "[role='dialog']",
)
CONTROL_CUES = {
    "start_date": ["Start Date", "From Date", "From"],
    "end_date": ["End Date", "To Date", "To"],
    "apply": ["Apply", "Search", "Submit", "Go"],
    "export_report": ["Export Report", "Export", "Download", "Export GST"],
}
MONTH_LOOKUP = {
    "JAN": 1,
    "JANUARY": 1,
    "FEB": 2,
    "FEBRUARY": 2,
    "MAR": 3,
    "MARCH": 3,
    "APR": 4,
    "APRIL": 4,
    "MAY": 5,
    "JUN": 6,
    "JUNE": 6,
    "JUL": 7,
    "JULY": 7,
    "AUG": 8,
    "AUGUST": 8,
    "SEP": 9,
    "SEPT": 9,
    "SEPTEMBER": 9,
    "OCT": 10,
    "OCTOBER": 10,
    "NOV": 11,
    "NOVEMBER": 11,
    "DEC": 12,
    "DECEMBER": 12,
}


@dataclass
class UcStore:
    store_code: str
    store_name: str | None
    cost_center: str | None
    sync_config: Dict[str, Any]

    @property
    def storage_state_path(self) -> Path:
        return default_profiles_dir() / f"{self.store_code}_storage_state.json"

    @property
    def login_url(self) -> str | None:
        return _get_nested_str(self.sync_config, ("urls", "login"))

    @property
    def orders_url(self) -> str | None:
        return _get_nested_str(self.sync_config, ("urls", "orders_link"))

    @property
    def home_url(self) -> str | None:
        return _get_nested_str(self.sync_config, ("urls", "home"))

    @property
    def username(self) -> str | None:
        return _get_nested_str(self.sync_config, ("username",))

    @property
    def password(self) -> str | None:
        return _get_nested_str(self.sync_config, ("password",))

    @property
    def login_selectors(self) -> Dict[str, str]:
        selectors = _coerce_dict(self.sync_config.get("login_selector"))
        return {key: value for key, value in selectors.items() if isinstance(value, str) and value.strip()}

    @property
    def home_selectors(self) -> list[str]:
        raw_selectors: list[str] = []
        raw = self.sync_config.get("home_selectors") or self.sync_config.get("home_selector")
        if isinstance(raw, str) and raw.strip():
            raw_selectors.append(raw.strip())
        elif isinstance(raw, Sequence):
            raw_selectors.extend(
                [str(value).strip() for value in raw if isinstance(value, str) and str(value).strip()]
            )
        return raw_selectors

    @property
    def gst_menu_labels(self) -> list[str]:
        raw_labels: list[str] = []
        raw = self.sync_config.get("gst_menu_label") or self.sync_config.get("orders_menu_label")
        if isinstance(raw, str) and raw.strip():
            raw_labels.append(raw.strip())
        elif isinstance(raw, Sequence):
            raw_labels.extend(
                [str(value).strip() for value in raw if isinstance(value, str) and str(value).strip()]
            )
        return raw_labels


@dataclass
class StoreOutcome:
    status: str
    message: str
    final_url: str | None = None
    storage_state: str | None = None
    login_used: bool | None = None
    session_probe_result: bool | None = None
    fallback_login_attempted: bool | None = None
    fallback_login_result: bool | None = None
    download_path: str | None = None
    skip_reason: str | None = None
    warning_count: int | None = None
    rows_downloaded: int | None = None
    rows_skipped_invalid: int | None = None
    rows_skipped_invalid_reasons: dict[str, int] | None = None
    staging_rows: int | None = None
    final_rows: int | None = None
    staging_inserted: int | None = None
    staging_updated: int | None = None
    final_inserted: int | None = None
    final_updated: int | None = None
    stage_statuses: dict[str, str] = field(default_factory=dict)
    stage_metrics: dict[str, dict[str, Any]] = field(default_factory=dict)
    archive_publish_orders: dict[str, Any] | None = None
    archive_publish_sales: dict[str, Any] | None = None
    warning_rows: list[dict[str, Any]] = field(default_factory=list)
    dropped_rows: list[dict[str, Any]] = field(default_factory=list)
    reason_codes: list[str] = field(default_factory=list)
    footer_total: int | None = None
    base_rows_extracted: int | None = None
    rows_missing_estimate: int | None = None


@dataclass
class ArchiveOrdersExtract:
    base_rows: list[dict[str, Any]] = field(default_factory=list)
    order_detail_rows: list[dict[str, Any]] = field(default_factory=list)
    payment_detail_rows: list[dict[str, Any]] = field(default_factory=list)
    skipped_order_codes: list[str] = field(default_factory=list)
    skipped_order_counters: dict[str, int] = field(default_factory=dict)
    page_count: int = 0
    footer_total: int | None = None


@dataclass
class ArchiveFilterFooterState:
    pre_filter_footer_window: tuple[int, int, int] | None = None
    post_filter_footer_window: tuple[int, int, int] | None = None


def _archive_extraction_gap(extract: ArchiveOrdersExtract) -> int | None:
    if extract.footer_total is None:
        return None
    return max(0, extract.footer_total - len(extract.base_rows))


def _record_skipped_order(extract: ArchiveOrdersExtract, *, order_code: str, reason: str) -> None:
    extract.skipped_order_codes.append(order_code)
    extract.skipped_order_counters[reason] = extract.skipped_order_counters.get(reason, 0) + 1


async def _wait_for_row_stable(row: Locator) -> None:
    with contextlib.suppress(Exception):
        await row.wait_for(state="visible", timeout=5_000)
    with contextlib.suppress(Exception):
        await row.scroll_into_view_if_needed(timeout=5_000)
    with contextlib.suppress(Exception):
        await row.evaluate(
            """async (el) => {
                const sleepFrame = () => new Promise((resolve) => requestAnimationFrame(resolve));
                let last = el.getBoundingClientRect();
                for (let i = 0; i < 3; i += 1) {
                    await sleepFrame();
                    const next = el.getBoundingClientRect();
                    const stable =
                        Math.abs(next.top - last.top) < 0.5 &&
                        Math.abs(next.left - last.left) < 0.5 &&
                        Math.abs(next.width - last.width) < 0.5 &&
                        Math.abs(next.height - last.height) < 0.5;
                    if (stable) {
                        return;
                    }
                    last = next;
                }
            }"""
        )


async def _collect_selector_diagnostics(row: Locator, selectors: Sequence[str]) -> list[dict[str, Any]]:
    diagnostics: list[dict[str, Any]] = []
    for selector in selectors:
        locator = row.locator(selector)
        count = await locator.count()
        visible = None
        enabled = None
        if count:
            first = locator.first
            with contextlib.suppress(Exception):
                visible = await first.is_visible()
            with contextlib.suppress(Exception):
                enabled = await first.is_enabled()
        diagnostics.append(
            {
                "selector": selector,
                "count": count,
                "first_visible": visible,
                "first_enabled": enabled,
            }
        )
    return diagnostics


async def _click_order_details_trigger(row: Locator) -> tuple[bool, list[dict[str, Any]], str | None]:
    diagnostics = await _collect_selector_diagnostics(row, ARCHIVE_ORDER_DETAIL_TRIGGER_SELECTORS)
    max_attempts_per_selector = 2
    last_error: str | None = None
    for selector in ARCHIVE_ORDER_DETAIL_TRIGGER_SELECTORS:
        trigger = row.locator(selector).first
        if not await trigger.count():
            continue
        for attempt in range(max_attempts_per_selector):
            force_click = attempt == 1
            await _wait_for_row_stable(row)
            with contextlib.suppress(Exception):
                await row.locator("td.order-col").first.scroll_into_view_if_needed(timeout=5_000)
            with contextlib.suppress(Exception):
                await trigger.scroll_into_view_if_needed(timeout=5_000)
            with contextlib.suppress(Exception):
                await trigger.wait_for(state="visible", timeout=5_000)
            with contextlib.suppress(Exception):
                await trigger.wait_for(state="attached", timeout=5_000)
            with contextlib.suppress(Exception):
                await trigger.click(timeout=5_000, trial=True)
            try:
                await trigger.click(timeout=10_000, force=force_click)
                return True, diagnostics, None
            except Exception as exc:
                last_error = str(exc)
    return False, diagnostics, last_error

def _normalize_output_status(status: str | None) -> str:
    normalized = str(status or "").lower()
    mapping = {
        "ok": "success",
        "success": "success",
        "warning": "success_with_warnings",
        "warn": "success_with_warnings",
        "success_with_warnings": "success_with_warnings",
        "partial": "partial",
        "skipped": "partial",
        "error": "failed",
        "failed": "failed",
    }
    return mapping.get(normalized, normalized or "unknown")


@dataclass
class DeferredOrdersSyncLog:
    store: "UcStore"
    run_id: str
    run_start_date: date
    run_end_date: date


@dataclass
class UcOrdersDiscoverySummary:
    run_id: str
    run_env: str
    report_date: date
    report_end_date: date
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    store_codes: list[str] = field(default_factory=list)
    store_outcomes: Dict[str, StoreOutcome] = field(default_factory=dict)
    phases: dict[str, Dict[str, int]] = field(
        default_factory=lambda: {"init": {"ok": 0, "warning": 0, "error": 0}, "store": {"ok": 0, "warning": 0, "error": 0}}
    )
    notes: list[str] = field(default_factory=list)
    ingest_remarks: list[dict[str, str]] = field(default_factory=list)
    deferred_orders_sync_logs: list[DeferredOrdersSyncLog] = field(default_factory=list)
    window_audit: list[dict[str, Any]] = field(default_factory=list)

    def mark_phase(self, phase: str, status: str) -> None:
        counters = self.phases.setdefault(phase, {"ok": 0, "warning": 0, "error": 0})
        normalized = "warning" if status in {"warn", "warning"} else status
        if normalized not in counters:
            normalized = "ok"
        counters[normalized] += 1

    def record_store(self, store_code: str, outcome: StoreOutcome) -> None:
        self.store_outcomes[store_code] = outcome
        self.mark_phase("store", outcome.status)

    def overall_status(self) -> str:
        statuses = [outcome.status for outcome in self.store_outcomes.values()]
        if any(status == "error" for status in statuses):
            return "failed"
        if any(status == "warning" for status in statuses):
            return "success_with_warnings"
        if any("partial_extraction" in (outcome.reason_codes or []) for outcome in self.store_outcomes.values()):
            return "success_with_warnings"
        if not statuses and not self.store_codes:
            return "success_with_warnings"
        return "success" if statuses else "failed"

    def _window_summary(self) -> Dict[str, Any]:
        expected_windows = len(self.store_codes)
        completed_store_codes = [code for code in self.store_codes if code in self.store_outcomes]
        missing_store_codes = [code for code in self.store_codes if code not in self.store_outcomes]
        completed_windows = len(completed_store_codes)
        missing_windows = max(0, expected_windows - completed_windows)
        return {
            "expected_windows": expected_windows,
            "completed_windows": completed_windows,
            "missing_windows": missing_windows,
            "completed_store_codes": completed_store_codes,
            "missing_store_codes": missing_store_codes,
        }

    def summary_text(self) -> str:
        total = len(self.store_codes)
        ok = sum(1 for outcome in self.store_outcomes.values() if outcome.status == "ok")
        warn = sum(1 for outcome in self.store_outcomes.values() if outcome.status == "warning")
        error = sum(1 for outcome in self.store_outcomes.values() if outcome.status == "error")
        overall_status = self.overall_status()
        status_explanation = {
            "success": "run completed with no issues recorded",
            "success_with_warnings": "run completed with row-level warnings",
            "partial": "run completed but row-level issues were recorded",
            "failed": "run failed or data could not be downloaded",
        }.get(overall_status, "run completed with mixed results")
        window_summary = self._window_summary()
        missing_windows_line = f"Missing Windows: {window_summary['missing_windows']}"
        if window_summary["missing_store_codes"]:
            missing_windows_line += f" ({', '.join(window_summary['missing_store_codes'])})"
        return (
            "UC Archive Orders Run Summary\n"
            f"Overall Status: {overall_status} ({status_explanation})\n"
            f"Stores: {ok} success, {warn} success_with_warnings, {error} failed across {total} stores\n"
            f"Windows Completed: {window_summary['completed_windows']} / {window_summary['expected_windows']}\n"
            f"{missing_windows_line}"
        )

    def add_ingest_remarks(self, remarks: Iterable[dict[str, str]]) -> None:
        for entry in remarks:
            store_code = entry.get("store_code")
            order_number = entry.get("order_number")
            remark_text = entry.get("ingest_remarks")
            if not remark_text:
                continue
            self.ingest_remarks.append(
                {
                    "store_code": (store_code or "").upper(),
                    "order_number": str(order_number) if order_number is not None else "",
                    "ingest_remarks": str(remark_text),
                }
            )

    def _store_status_counts(self) -> Dict[str, int]:
        counts = {"ok": 0, "warning": 0, "error": 0}
        for outcome in self.store_outcomes.values():
            if outcome.status in counts:
                counts[outcome.status] += 1
        return counts

    def _build_stage_summary(self) -> Dict[str, Dict[str, int]]:
        stage_summary: Dict[str, Dict[str, int]] = {}
        for outcome in self.store_outcomes.values():
            for stage_name, stage_status in (outcome.stage_statuses or {}).items():
                counters = stage_summary.setdefault(stage_name, {"success": 0, "failed": 0, "skipped": 0})
                normalized = stage_status if stage_status in counters else "skipped"
                counters[normalized] += 1
        return stage_summary

    def _build_store_summary(self) -> Dict[str, Dict[str, Any]]:
        summary: Dict[str, Dict[str, Any]] = {}
        to_date = self.report_end_date or self.report_date
        for store_code in self.store_codes:
            code = store_code.upper()
            outcome = self.store_outcomes.get(code)
            filename = _format_gst_filename(code, self.report_date, to_date)
            error_message = None
            info_message = None
            if outcome:
                if outcome.status == "error":
                    error_message = outcome.message
                else:
                    info_message = outcome.message
            summary[code] = {
                "status": _normalize_output_status(outcome.status if outcome else "error"),
                "message": outcome.message if outcome else "No outcome recorded",
                "error_message": error_message,
                "info_message": info_message,
                "skip_reason": outcome.skip_reason if outcome else None,
                "filename": filename,
                "download_path": outcome.download_path if outcome else None,
                "warning_count": outcome.warning_count if outcome else None,
                "session_probe_result": outcome.session_probe_result if outcome else None,
                "fallback_login_attempted": outcome.fallback_login_attempted if outcome else None,
                "fallback_login_result": outcome.fallback_login_result if outcome else None,
                "row_counts": {
                    "staging_rows": outcome.staging_rows if outcome else None,
                    "final_rows": outcome.final_rows if outcome else None,
                    "staging_inserted": outcome.staging_inserted if outcome else None,
                    "staging_updated": outcome.staging_updated if outcome else None,
                    "final_inserted": outcome.final_inserted if outcome else None,
                    "final_updated": outcome.final_updated if outcome else None,
                    "archive_publish_orders": outcome.archive_publish_orders if outcome else None,
                    "archive_publish_sales": outcome.archive_publish_sales if outcome else None,
                },
                "stage_statuses": dict(outcome.stage_statuses) if outcome else {},
                "stage_metrics": dict(outcome.stage_metrics) if outcome else {},
                "rows_downloaded": outcome.rows_downloaded if outcome else None,
                "rows_skipped_invalid": outcome.rows_skipped_invalid if outcome else None,
                "rows_skipped_invalid_reasons": outcome.rows_skipped_invalid_reasons if outcome else None,
            }
        return summary

    def _build_warning_entries(self) -> list[str]:
        warnings: list[str] = []
        for store_code, outcome in self.store_outcomes.items():
            warning_count = outcome.warning_count if outcome else None
            if warning_count is None or warning_count <= 0:
                continue
            warnings.append(
                f"UC_STORE_WARNINGS: {store_code} reported {warning_count} row-level warning(s)"
            )
        return warnings

    def _build_notification_payload(self, *, finished_at: datetime, total_time_taken: str) -> Dict[str, Any]:
        stores: list[dict[str, Any]] = []
        to_date = self.report_end_date or self.report_date
        for store_code in self.store_codes:
            code = store_code.upper()
            outcome = self.store_outcomes.get(code)
            filename = _format_gst_filename(code, self.report_date, to_date)
            error_message = None
            info_message = None
            if outcome:
                if outcome.status == "error":
                    error_message = outcome.message
                else:
                    info_message = outcome.message
            stores.append(
                {
                    "store_code": code,
                    "status": _normalize_output_status(outcome.status if outcome else "error"),
                    "message": outcome.message if outcome else "No outcome recorded",
                    "error_message": error_message,
                    "info_message": info_message,
                    "skip_reason": outcome.skip_reason if outcome else None,
                    "warning_count": outcome.warning_count if outcome else None,
                    "session_probe_result": outcome.session_probe_result if outcome else None,
                    "fallback_login_attempted": outcome.fallback_login_attempted if outcome else None,
                    "fallback_login_result": outcome.fallback_login_result if outcome else None,
                    "warning_rows": list(outcome.warning_rows) if outcome else [],
                    "dropped_rows": list(outcome.dropped_rows) if outcome else [],
                    "filename": filename,
                    "staging_rows": outcome.staging_rows if outcome else None,
                    "final_rows": outcome.final_rows if outcome else None,
                    "staging_inserted": outcome.staging_inserted if outcome else None,
                    "staging_updated": outcome.staging_updated if outcome else None,
                    "final_inserted": outcome.final_inserted if outcome else None,
                    "final_updated": outcome.final_updated if outcome else None,
                    "rows_downloaded": outcome.rows_downloaded if outcome else None,
                    "rows_skipped_invalid": outcome.rows_skipped_invalid if outcome else None,
                    "rows_skipped_invalid_reasons": outcome.rows_skipped_invalid_reasons if outcome else None,
                    "stage_statuses": dict(outcome.stage_statuses) if outcome else {},
                    "stage_metrics": dict(outcome.stage_metrics) if outcome else {},
                }
            )
        return {
            "overall_status": self.overall_status(),
            "stores": stores,
            "warnings": self._build_warning_entries(),
            "started_at": self.started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "total_time_taken": total_time_taken,
        }

    def build_record(self, *, finished_at: datetime) -> Dict[str, Any]:
        total_seconds = max(0, int((finished_at - self.started_at).total_seconds()))
        hh = total_seconds // 3600
        mm = (total_seconds % 3600) // 60
        ss = total_seconds % 60
        total_time_taken = f"{hh:02d}:{mm:02d}:{ss:02d}"
        store_summary = self._build_store_summary()
        metrics = {
            "stores": {
                "configured": list(self.store_codes),
                "outcomes": {code: outcome.__dict__ for code, outcome in self.store_outcomes.items()},
            },
            "window_summary": self._window_summary(),
            "window_audit": list(self.window_audit),
            "stores_summary": {
                "counts": self._store_status_counts(),
                "stages": self._build_stage_summary(),
                "stores": store_summary,
                "store_order": list(self.store_codes),
                "report_range": {"from": self.report_date.isoformat(), "to": self.report_end_date.isoformat()},
            },
            "notes": list(self.notes),
            "warnings": self._build_warning_entries(),
            "ingest_remarks": {"rows": list(self.ingest_remarks), "total": len(self.ingest_remarks)},
        }
        metrics["notification_payload"] = self._build_notification_payload(
            finished_at=finished_at, total_time_taken=total_time_taken
        )
        return {
            "pipeline_name": PIPELINE_NAME,
            "run_id": self.run_id,
            "run_env": self.run_env,
            "started_at": self.started_at,
            "finished_at": finished_at,
            "total_time_taken": total_time_taken,
            "report_date": self.report_date,
            "overall_status": self.overall_status(),
            "summary_text": self.summary_text(),
            "phases_json": {phase: dict(counts) for phase, counts in self.phases.items()},
            "metrics_json": metrics,
        }


async def main(
    *,
    run_env: str | None = None,
    run_id: str | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
    store_codes: Sequence[str] | None = None,
    run_orders: bool = True,
    run_sales: bool = True,
) -> None:
    """Run the UC Archive Orders download flow."""

    _ = run_orders, run_sales
    resolved_run_id = run_id or new_run_id()
    resolved_run_date = datetime.now(get_timezone())
    resolved_env = run_env or config.run_env
    current_date = aware_now(get_timezone()).date()
    # TODO: Remove this fixed sampling window before Stage 2; restore dynamic date range handling.
    from_date = current_date - timedelta(days=30)
    to_date = current_date
    run_end_date = to_date or current_date
    if from_date and from_date > run_end_date:
        raise ValueError(f"from_date ({from_date}) must be on or before to_date ({run_end_date})")
    logger = get_logger(run_id=resolved_run_id)
    stores = await _load_uc_order_stores(logger=logger, store_codes=store_codes)
    store_start_dates: dict[str, date] = {}
    pipeline_id: int | None = None
    if stores and from_date is None and config.database_url:
        pipeline_id = await _fetch_pipeline_id(
            database_url=config.database_url, pipeline_name=PIPELINE_NAME, logger=logger
        )
    for store in stores:
        backfill, window_size, overlap = resolve_window_settings(sync_config=store.sync_config)
        last_success = None
        if from_date is None and pipeline_id and config.database_url:
            last_success = await fetch_last_success_window_end(
                database_url=config.database_url,
                pipeline_id=pipeline_id,
                store_code=store.store_code,
            )
        store_start_dates[store.store_code] = resolve_orders_sync_start_date(
            end_date=run_end_date,
            last_success=last_success,
            overlap_days=overlap,
            backfill_days=backfill,
            window_days=window_size,
            from_date=from_date,
            store_start_date=None,
        )
    report_start_date = from_date or (min(store_start_dates.values()) if store_start_dates else run_end_date)
    summary = UcOrdersDiscoverySummary(
        run_id=resolved_run_id,
        run_env=resolved_env,
        report_date=report_start_date,
        report_end_date=run_end_date,
    )

    persist_attempted = False
    browser: Browser | None = None

    try:
        await _start_run_summary(summary=summary, logger=logger)
        log_event(
            logger=logger,
            phase="init",
            message="Starting UC orders sync discovery flow",
            run_env=resolved_env,
            from_date=summary.report_date,
            to_date=summary.report_end_date,
        )
        log_event(
            logger=logger,
            phase="init",
            message="Pipeline DOM logging configuration",
            pipeline_skip_dom_logging=config.pipeline_skip_dom_logging,
        )

        summary.store_codes = [store.store_code for store in stores]
        if not stores:
            log_event(
                logger=logger,
                phase="init",
                status="warn",
                message="No UC stores with sync_orders_flag found; exiting",
            )
            summary.mark_phase("init", "warning")
            summary.notes.append("No UC stores with sync_orders_flag found; exiting")
            persist_attempted = True
            if await _persist_summary(summary=summary, logger=logger):
                notification_result = await send_notifications_for_run(PIPELINE_NAME, resolved_run_id)
                log_event(
                    logger=logger,
                    phase="notifications",
                    message="UC orders sync notification summary",
                    run_id=resolved_run_id,
                    run_env=resolved_env,
                    emails_planned=notification_result["emails_planned"],
                    emails_sent=notification_result["emails_sent"],
                    notification_errors=notification_result["errors"],
                )
            return

        download_timeout_ms = await _fetch_dashboard_nav_timeout_ms(config.database_url)
        async with async_playwright() as playwright:
            browser = await launch_browser(playwright=playwright, logger=logger)
            semaphore = asyncio.Semaphore(_resolve_uc_max_workers())

            async def _guarded(store: UcStore) -> None:
                async with semaphore:
                    store_start_date = store_start_dates.get(store.store_code, summary.report_date)
                    await _run_store_discovery(
                        browser=browser,
                        store=store,
                        logger=logger,
                        run_env=resolved_env,
                        run_id=resolved_run_id,
                        run_date=resolved_run_date,
                        summary=summary,
                        from_date=store_start_date,
                        to_date=run_end_date,
                        download_timeout_ms=download_timeout_ms,
                    )

            await asyncio.gather(*[_guarded(store) for store in stores])
            await browser.close()

        log_event(
            logger=logger,
            phase="notifications",
            message="UC orders sync discovery flow complete; notifying",
            run_env=resolved_env,
        )
        persist_attempted = True
        if await _persist_summary(summary=summary, logger=logger):
            notification_result = await send_notifications_for_run(PIPELINE_NAME, resolved_run_id)
            log_event(
                logger=logger,
                phase="notifications",
                message="UC orders sync notification summary",
                run_id=resolved_run_id,
                run_env=resolved_env,
                emails_planned=notification_result["emails_planned"],
                emails_sent=notification_result["emails_sent"],
                notification_errors=notification_result["errors"],
            )
        else:
            log_event(
                logger=logger,
                phase="notifications",
                status="warn",
                message="Skipping notifications because run summary was not recorded",
                run_id=resolved_run_id,
            )
    except Exception as exc:
        log_event(
            logger=logger,
            phase="store",
            status="error",
            message="UC orders sync discovery failed",
            run_id=resolved_run_id,
            error=str(exc),
        )
        summary.notes.append(f"Run failed unexpectedly: {exc}")
        summary.mark_phase("store", "error")
        if not persist_attempted:
            persist_attempted = True
            await _persist_summary(summary=summary, logger=logger)
        raise
    finally:
        if not persist_attempted:
            await _persist_summary(summary=summary, logger=logger)
        with contextlib.suppress(Exception):
            if browser is not None:
                await browser.close()
        with contextlib.suppress(Exception):
            logger.close()


def _coerce_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if value is None:
        return {}
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {}
    return {}


def _parse_attempt_no(run_id: str | None) -> int | None:
    if not run_id:
        return None
    match = re.search(r"_attempt(\d+)$", run_id)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:  # pragma: no cover - exercised via CLI parsing
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}; expected YYYY-MM-DD") from exc


def _get_nested_str(mapping: Mapping[str, Any], path: Sequence[str]) -> str | None:
    current: Any = mapping
    for key in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    if current is None:
        return None
    return str(current).strip() or None


def _normalize_id_selector(selector: str) -> str:
    selector = selector.strip()
    prefixes = ("#", ".", "[", "text=", "css=", "xpath=", "role=", "id=")
    if selector.startswith(prefixes):
        return selector
    if re.fullmatch(r"[A-Za-z0-9_-]+", selector):
        return f"#{selector}"
    return selector


def _format_gst_filename(store_code: str, from_date: date, to_date: date) -> str:
    return f"{store_code}_uc_gst_{from_date:%Y%m%d}_{to_date:%Y%m%d}.xlsx"


def _format_archive_base_filename(store_code: str, from_date: date, to_date: date) -> str:
    return f"{store_code}-base_order_info_{from_date:%Y%m%d}_{to_date:%Y%m%d}.xlsx"


def _format_archive_order_details_filename(store_code: str, from_date: date, to_date: date) -> str:
    return f"{store_code}-order_details_{from_date:%Y%m%d}_{to_date:%Y%m%d}.xlsx"


def _format_archive_payment_details_filename(store_code: str, from_date: date, to_date: date) -> str:
    return f"{store_code}-payment_details_{from_date:%Y%m%d}_{to_date:%Y%m%d}.xlsx"


def _write_excel_rows(path: Path, rows: Sequence[dict[str, Any]], columns: Sequence[str]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(list(columns))
    for row in rows:
        sheet.append([row.get(column) for column in columns])
    workbook.save(path)
    return len(rows)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _resolve_uc_max_workers() -> int:
    return max(1, _env_int("UC_MAX_WORKERS", UC_MAX_WORKERS_DEFAULT))


def _resolve_uc_download_dir(run_id: str, store_code: str, from_date: date, to_date: date) -> Path:
    return default_download_dir()


async def _load_uc_order_stores(
    *, logger: JsonLogger, store_codes: Sequence[str] | None = None
) -> list[UcStore]:
    if not config.database_url:
        log_event(
            logger=logger,
            phase="init",
            status="error",
            message="database_url missing; cannot load UC store rows",
        )
        return []

    normalized_codes = normalize_store_codes(store_codes or [])
    query_text = """
        SELECT store_code, store_name, cost_center, sync_config
        FROM store_master
        WHERE sync_group = :sync_group
          AND sync_orders_flag = TRUE
          AND (is_active IS NULL OR is_active = TRUE)
    """
    if normalized_codes:
        query_text += " AND UPPER(store_code) IN :store_codes"
    query = sa.text(query_text)
    if normalized_codes:
        query = query.bindparams(sa.bindparam("store_codes", expanding=True))

    async with session_scope(config.database_url) as session:
        params = {"sync_group": "UC"}
        if normalized_codes:
            params["store_codes"] = normalized_codes
        result = await session.execute(query, params)
        stores: list[UcStore] = []
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
                UcStore(
                    store_code=raw_code.upper(),
                    store_name=row.get("store_name"),
                    cost_center=row.get("cost_center"),
                    sync_config=_coerce_dict(row.get("sync_config")),
                )
            )

    log_event(
        logger=logger,
        phase="init",
        message="Loaded UC store rows",
        store_count=len(stores),
        stores=[store.store_code for store in stores],
    )
    return stores


async def _fetch_dashboard_nav_timeout_ms(database_url: str | None) -> int:
    if not database_url:
        return NAV_TIMEOUT_MS

    try:
        async with session_scope(database_url) as session:
            result = await session.execute(
                sa.text(
                    "SELECT value FROM system_config WHERE key = :key AND is_active = TRUE LIMIT 1"
                ),
                {"key": "DASHBOARD_DOWNLOAD_NAV_TIMEOUT"},
            )
            row = result.first()
    except Exception:
        return NAV_TIMEOUT_MS

    raw_value = row[0] if row else None
    try:
        return int(str(raw_value).strip())
    except (TypeError, ValueError):
        return NAV_TIMEOUT_MS


async def _fetch_pipeline_id(*, database_url: str, pipeline_name: str, logger: JsonLogger) -> int | None:
    try:
        async with session_scope(database_url) as session:
            pipeline_id = (
                await session.execute(sa.select(pipelines.c.id).where(pipelines.c.code == pipeline_name))
            ).scalar_one_or_none()
    except Exception as exc:  # pragma: no cover - defensive
        log_event(
            logger=logger,
            phase="orders_sync_log",
            status="warn",
            message="Failed to fetch pipeline id for orders sync log",
            pipeline_name=pipeline_name,
            error=str(exc),
        )
        return None
    if not pipeline_id:
        log_event(
            logger=logger,
            phase="orders_sync_log",
            status="warn",
            message="Pipeline id not found for orders sync log",
            pipeline_name=pipeline_name,
        )
    return pipeline_id


async def _insert_orders_sync_log(
    *,
    logger: JsonLogger,
    summary: UcOrdersDiscoverySummary,
    store: UcStore,
    run_id: str,
    run_env: str,
    run_start_date: date,
    run_end_date: date,
    allow_defer: bool = True,
) -> int | None:
    if not config.database_url:
        log_event(
            logger=logger,
            phase="orders_sync_log",
            status="warn",
            message="Skipping orders sync log insert because database_url is missing",
            store_code=store.store_code,
        )
        return None

    pipeline_id = await _fetch_pipeline_id(
        database_url=config.database_url, pipeline_name=PIPELINE_NAME, logger=logger
    )
    if not pipeline_id:
        return None
    existing_summary = await fetch_summary_for_run(config.database_url, run_id)
    if not existing_summary:
        log_event(
            logger=logger,
            phase="orders_sync_log",
            status="warn",
            message="Skipping orders sync log insert because run summary row is missing",
            run_id=run_id,
            store_code=store.store_code,
        )
        summary_started = await _start_run_summary(summary=summary, logger=logger)
        if not summary_started:
            if allow_defer:
                _defer_orders_sync_log(summary, store, run_id, run_start_date, run_end_date)
            logger.error(
                phase="orders_sync_log",
                message=(
                    "Hard error: run summary start failed; deferred orders sync log insert queued "
                    "for final retry"
                ),
                run_id=run_id,
                store_code=store.store_code,
            )
            return None
        existing_summary = await fetch_summary_for_run(config.database_url, run_id)
        if not existing_summary:
            log_event(
                logger=logger,
                phase="orders_sync_log",
                status="error",
                message="Run summary row still missing; deferring orders sync log insert until final summary persistence",
                run_id=run_id,
                store_code=store.store_code,
            )
            if allow_defer:
                _defer_orders_sync_log(summary, store, run_id, run_start_date, run_end_date)
            return None

    attempt_no = _parse_attempt_no(run_id) or 1
    insert_values = {
        "pipeline_id": pipeline_id,
        "run_id": run_id,
        "run_env": run_env,
        "cost_center": store.cost_center,
        "store_code": store.store_code,
        "from_date": run_start_date,
        "to_date": run_end_date,
        "status": "running",
        "attempt_no": attempt_no,
        "created_at": sa.func.now(),
        "updated_at": sa.func.now(),
    }
    try:
        async with session_scope(config.database_url) as session:
            log_id = (
                await session.execute(
                    pg_insert(orders_sync_log)
                    .values(**insert_values)
                    .on_conflict_do_update(
                        index_elements=(
                            "pipeline_id",
                            "store_code",
                            "from_date",
                            "to_date",
                            "run_id",
                        ),
                        set_={
                            "attempt_no": orders_sync_log.c.attempt_no + 1,
                            "status": "running",
                            "updated_at": sa.func.now(),
                        },
                    )
                    .returning(orders_sync_log.c.id)
                )
            ).scalar_one()
            await session.commit()
            exists = (
                await session.execute(
                    sa.select(orders_sync_log.c.id).where(orders_sync_log.c.id == log_id)
                )
            ).scalar_one_or_none()
            if not exists:
                log_event(
                    logger=logger,
                    phase="orders_sync_log",
                    status="error",
                    message="Hard error: orders sync log row missing after insert",
                    log_id=log_id,
                    store_code=store.store_code,
                )
                return None
            log_event(
                logger=logger,
                phase="orders_sync_log",
                status="info",
                message="Inserted orders sync log row",
                log_id=log_id,
                store_code=store.store_code,
            )
            log_event(
                logger=logger,
                phase="orders_sync_log",
                status="info",
                message="Verified orders sync log row exists",
                log_id=log_id,
                store_code=store.store_code,
            )
    except Exception as exc:  # pragma: no cover - defensive
        log_event(
            logger=logger,
            phase="orders_sync_log",
            status="error",
            message="Failed to insert orders sync log row (DB error)",
            store_code=store.store_code,
            error=str(exc),
            insert_values=insert_values,
        )
        return None
    return log_id


def _defer_orders_sync_log(
    summary: UcOrdersDiscoverySummary,
    store: UcStore,
    run_id: str,
    run_start_date: date,
    run_end_date: date,
) -> None:
    if any(
        entry.store.store_code == store.store_code
        and entry.run_start_date == run_start_date
        and entry.run_end_date == run_end_date
        and entry.run_id == run_id
        for entry in summary.deferred_orders_sync_logs
    ):
        return
    summary.deferred_orders_sync_logs.append(
        DeferredOrdersSyncLog(
            store=store, run_id=run_id, run_start_date=run_start_date, run_end_date=run_end_date
        )
    )


async def _flush_deferred_orders_sync_logs(
    *, summary: UcOrdersDiscoverySummary, logger: JsonLogger, run_id: str, run_env: str
) -> None:
    if not summary.deferred_orders_sync_logs or not config.database_url:
        return
    existing_summary = await fetch_summary_for_run(config.database_url, run_id)
    if not existing_summary:
        summary_started = await _start_run_summary(summary=summary, logger=logger)
        if not summary_started:
            log_event(
                logger=logger,
                phase="orders_sync_log",
                status="error",
                message="Unable to start run summary; skipping deferred orders sync log inserts",
                run_id=run_id,
            )
            return
        existing_summary = await fetch_summary_for_run(config.database_url, run_id)
        if not existing_summary:
            log_event(
                logger=logger,
                phase="orders_sync_log",
                status="error",
                message="Run summary row still missing; skipping deferred orders sync log inserts",
                run_id=run_id,
            )
            return

    pending = list(summary.deferred_orders_sync_logs)
    summary.deferred_orders_sync_logs.clear()
    for entry in pending:
        log_id = await _insert_orders_sync_log(
            logger=logger,
            summary=summary,
            store=entry.store,
            run_id=run_id,
            run_env=run_env,
            run_start_date=entry.run_start_date,
            run_end_date=entry.run_end_date,
            allow_defer=False,
        )
        if log_id is None:
            continue
        outcome = summary.store_outcomes.get(entry.store.store_code)
        if outcome:
            status = _resolve_sync_log_status(
                outcome=outcome,
                download_succeeded=bool(outcome.download_path),
                row_count=outcome.rows_downloaded,
            )
            error_message = outcome.message if outcome.status == "error" else None
            status_note = _resolve_sync_log_status_note(
                status=status, outcome=outcome, row_count=outcome.rows_downloaded
            )
            resolved_message = _resolve_sync_log_message(
                status=status, error_message=error_message, status_note=status_note
            )
            primary_metrics, secondary_metrics = _build_unified_metrics(outcome)
            await _update_orders_sync_log(
                logger=logger,
                log_id=log_id,
                status=status,
                error_message=resolved_message,
                primary_metrics=primary_metrics,
                secondary_metrics=secondary_metrics,
            )


async def _update_orders_sync_log(
    *,
    logger: JsonLogger,
    log_id: int | None,
    status: str | None = None,
    orders_pulled_at: datetime | None = None,
    error_message: str | None = None,
    primary_metrics: Mapping[str, Any] | None = None,
    secondary_metrics: Mapping[str, Any] | None = None,
) -> None:
    if not log_id or not config.database_url:
        return
    values: dict[str, Any] = {}
    if status is not None:
        values["status"] = status
    if orders_pulled_at is not None:
        values["orders_pulled_at"] = orders_pulled_at
    if error_message is not None:
        values["error_message"] = error_message
    if primary_metrics is not None:
        values.update(primary_metrics)
    if secondary_metrics is not None:
        values.update(secondary_metrics)
    if not values:
        return
    values["updated_at"] = sa.func.now()
    try:
        async with session_scope(config.database_url) as session:
            result = await session.execute(
                sa.update(orders_sync_log).where(orders_sync_log.c.id == log_id).values(**values)
            )
            await session.commit()
            if not result.rowcount:
                error_message = "Orders sync log update matched no rows"
                log_event(
                    logger=logger,
                    phase="orders_sync_log",
                    status="error",
                    message=error_message,
                    log_id=log_id,
                    update_values=values,
                )
                raise RuntimeError(error_message)
    except Exception as exc:  # pragma: no cover - defensive
        log_event(
            logger=logger,
            phase="orders_sync_log",
            status="error",
            message="Failed to update orders sync log row",
            log_id=log_id,
            error=str(exc),
            update_values=values,
        )
        raise


def _build_unified_metrics(outcome: StoreOutcome | None) -> tuple[dict[str, int | None], dict[str, int | None]]:
    primary_metrics = {
        "primary_rows_downloaded": outcome.rows_downloaded if outcome else None,
        "primary_rows_ingested": outcome.final_rows if outcome else None,
        "primary_staging_rows": outcome.staging_rows if outcome else None,
        "primary_staging_inserted": outcome.staging_inserted if outcome else None,
        "primary_staging_updated": outcome.staging_updated if outcome else None,
        "primary_final_inserted": outcome.final_inserted if outcome else None,
        "primary_final_updated": outcome.final_updated if outcome else None,
    }
    secondary_metrics = {
        "secondary_rows_downloaded": None,
        "secondary_rows_ingested": None,
        "secondary_staging_rows": None,
        "secondary_staging_inserted": None,
        "secondary_staging_updated": None,
        "secondary_final_inserted": None,
        "secondary_final_updated": None,
    }
    return primary_metrics, secondary_metrics


def _has_rows(row_count: int | None, staging_rows: int | None) -> bool:
    return any(count is not None and count > 0 for count in (row_count, staging_rows))


def _resolve_sync_log_status(
    *, outcome: StoreOutcome, download_succeeded: bool, row_count: int | None
) -> str:
    if outcome.status == "error":
        return "failed"
    has_rows = _has_rows(row_count, outcome.staging_rows)
    has_output = download_succeeded or has_rows
    if not has_output:
        return "skipped"
    if outcome.status == "warning":
        return "success_with_warnings"
    return "success"


def _resolve_sync_log_status_note(
    *, status: str, outcome: StoreOutcome, row_count: int | None
) -> str | None:
    if status == "skipped":
        no_data = not outcome.download_path and outcome.staging_rows == 0
        has_rows = _has_rows(row_count, outcome.staging_rows)
        if outcome.skip_reason:
            return outcome.skip_reason
        if no_data:
            return "no data"
        if has_rows:
            return outcome.message or "low rows"
        return outcome.message
    if status == "partial":
        return outcome.message or "Completed with warnings"
    if status == "success_with_warnings":
        return "Completed with warnings"
    return None


def _resolve_sync_log_message(
    *, status: str, error_message: str | None, status_note: str | None
) -> str | None:
    if status == "failed":
        return error_message
    if status in {"skipped", "partial"}:
        if error_message and status_note and status_note not in error_message:
            return f"{error_message}; {status_note}"
        return error_message or status_note
    return None


def _log_ui_issues(
    *,
    logger: JsonLogger,
    store: UcStore,
    ui_issues: Sequence[dict[str, Any]],
    status: str,
) -> None:
    for issue in ui_issues:
        payload = dict(issue)
        message = payload.pop("message", "GST UI issue observed")
        log_event(
            logger=logger,
            phase="filters",
            status=status,
            message=message,
            store_code=store.store_code,
            **payload,
        )


async def _navigate_to_archive_orders(*, page: Page, store: UcStore, logger: JsonLogger) -> bool:
    log_event(
        logger=logger,
        phase="navigation",
        message="Navigating to Archive Orders",
        store_code=store.store_code,
        current_url=page.url,
    )
    log_event(
        logger=logger,
        phase="navigation",
        message="Navigating to Archive Orders via direct URL",
        store_code=store.store_code,
        target_url=ARCHIVE_URL,
    )
    await page.goto(ARCHIVE_URL, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)

    try:
        await page.wait_for_url("**/archive", timeout=NAV_TIMEOUT_MS)
    except TimeoutError:
        log_event(
            logger=logger,
            phase="navigation",
            status="warn",
            message="Archive Orders URL confirmation timed out",
            store_code=store.store_code,
            current_url=page.url,
        )
        return False
    log_event(
        logger=logger,
        phase="navigation",
        message="Archive Orders URL confirmed",
        store_code=store.store_code,
        current_url=page.url,
    )
    return True


async def _apply_archive_date_filter(
    *,
    page: Page,
    store: UcStore,
    logger: JsonLogger,
    from_date: date,
    to_date: date,
) -> ArchiveFilterFooterState | None:
    filter_button = page.locator(ARCHIVE_FILTER_BUTTON_SELECTOR).first
    if not await filter_button.count():
        log_event(
            logger=logger,
            phase="filters",
            status="warn",
            message="Archive Orders filter control missing",
            store_code=store.store_code,
            selector=ARCHIVE_FILTER_BUTTON_SELECTOR,
        )
        return None
    await filter_button.click()
    log_event(
        logger=logger,
        phase="filters",
        message="Archive Orders filter dropdown opened",
        store_code=store.store_code,
    )

    dropdown_selectors = [".date-type-section", ".date-options"]
    dropdown_ready = False
    try:
        await page.locator(dropdown_selectors[0]).first.wait_for(state="visible", timeout=5_000)
        dropdown_ready = True
    except TimeoutError:
        with contextlib.suppress(TimeoutError):
            await page.locator(dropdown_selectors[1]).first.wait_for(state="visible", timeout=5_000)
            dropdown_ready = True
    if not dropdown_ready:
        log_event(
            logger=logger,
            phase="filters",
            status="warn",
            message="Archive Orders filter dropdown content failed to render after clicking filter button",
            store_code=store.store_code,
            selectors=dropdown_selectors,
        )
        return None

    date_type = page.locator(ARCHIVE_DATE_TYPE_SELECTOR).first
    if await date_type.count():
        with contextlib.suppress(Exception):
            await date_type.check()
        with contextlib.suppress(Exception):
            await date_type.click()
        log_event(
            logger=logger,
            phase="filters",
            message="Delivery Date filter selected",
            store_code=store.store_code,
        )
    else:
        log_event(
            logger=logger,
            phase="filters",
            status="warn",
            message="Delivery Date filter radio missing",
            store_code=store.store_code,
            selector=ARCHIVE_DATE_TYPE_SELECTOR,
        )

    custom_option = page.locator(ARCHIVE_CUSTOM_OPTION_SELECTOR).first
    if not await custom_option.count():
        log_event(
            logger=logger,
            phase="filters",
            status="warn",
            message="Custom date option missing",
            store_code=store.store_code,
            selector=ARCHIVE_CUSTOM_OPTION_SELECTOR,
        )
        return None
    await custom_option.click()
    inputs = page.locator(ARCHIVE_CUSTOM_INPUT_SELECTOR)
    if await inputs.count() < 2:
        log_event(
            logger=logger,
            phase="filters",
            status="warn",
            message="Custom date inputs missing",
            store_code=store.store_code,
            selector=ARCHIVE_CUSTOM_INPUT_SELECTOR,
        )
        return None
    await inputs.nth(0).fill(from_date.isoformat())
    await inputs.nth(1).fill(to_date.isoformat())
    log_event(
        logger=logger,
        phase="filters",
        message="Custom date range filled",
        store_code=store.store_code,
        from_date=from_date,
        to_date=to_date,
    )
    pre_filter_footer_window = await _get_archive_footer_window(page)
    with contextlib.suppress(Exception):
        await page.locator("body").click(position={"x": 5, "y": 5})
    with contextlib.suppress(TimeoutError):
        await page.wait_for_selector(".table-wrapper", timeout=NAV_TIMEOUT_MS)

    post_filter_footer_window = await _wait_for_archive_filter_refresh_completion(
        page=page,
        store=store,
        logger=logger,
        pre_filter_footer_window=pre_filter_footer_window,
    )
    log_event(
        logger=logger,
        phase="filters",
        message="Archive Orders filter refresh completed",
        store_code=store.store_code,
        pre_filter_footer_window=pre_filter_footer_window,
        post_filter_footer_window=post_filter_footer_window,
    )
    return ArchiveFilterFooterState(
        pre_filter_footer_window=pre_filter_footer_window,
        post_filter_footer_window=post_filter_footer_window,
    )


async def _locator_text(locator: Locator) -> str | None:
    if not await locator.count():
        return None
    text = (await locator.first.inner_text()).strip()
    return text or None


def _normalize_cell_text(text: str | None) -> str | None:
    if not text:
        return None
    cleaned = re.sub(r"\s*\n\s*", "; ", text.strip())
    return cleaned or None


def _split_multiline_cell(text: str | None) -> list[str]:
    if not text:
        return []
    parts = re.split(r"\s*\n\s*", text.strip())
    cleaned_items: list[str] = []
    for part in parts:
        cleaned = re.sub(r"\s+", " ", part.strip())
        if cleaned:
            cleaned_items.append(cleaned)
    return cleaned_items


def _normalize_order_mode(text: str | None) -> str | None:
    if not text:
        return None
    cleaned = text.strip()
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = cleaned[1:-1].strip()
    return cleaned or None


def _normalize_order_info_key(text: str | None) -> str:
    if not text:
        return ""
    lowered = text.lower()
    lowered = lowered.replace(".", " ").replace(":", " ")
    return re.sub(r"\s+", " ", lowered).strip()


ARCHIVE_FOOTER_TOTAL_SELECTORS = (
    "div[style*='justify-content: flex-end'] p",
    ".table-footer p",
    ".pagination-footer p",
    ".pagination-summary p",
)


def _parse_archive_footer_total(text: str) -> int | None:
    match = re.search(r"showing\s+results\s+\d+\s+to\s+\d+\s+of\s+([\d,]+)\s+total", text, re.I)
    if not match:
        match = re.search(r"\bof\s+([\d,]+)\b", text, re.I)
    if not match:
        return None
    return int(match.group(1).replace(",", ""))


def _parse_archive_footer_window(text: str) -> tuple[int, int, int] | None:
    match = re.search(r"showing\s+results\s+([\d,]+)\s+to\s+([\d,]+)\s+of\s+([\d,]+)\s+total", text, re.I)
    if not match:
        return None
    return tuple(int(match.group(i).replace(",", "")) for i in (1, 2, 3))


async def _get_archive_footer_total(page: Page) -> int | None:
    for selector in ARCHIVE_FOOTER_TOTAL_SELECTORS:
        locator = page.locator(selector).first
        if not await locator.count():
            continue
        text = await _locator_text(locator)
        if not text:
            continue
        total = _parse_archive_footer_total(text)
        if total is not None:
            return total
    return None


async def _get_archive_footer_window(page: Page) -> tuple[int, int, int] | None:
    for selector in ARCHIVE_FOOTER_TOTAL_SELECTORS:
        locator = page.locator(selector).first
        if not await locator.count():
            continue
        text = await _locator_text(locator)
        if not text:
            continue
        parsed = _parse_archive_footer_window(text)
        if parsed is not None:
            return parsed
    return None


async def _get_archive_footer_text(page: Page) -> str | None:
    for selector in ARCHIVE_FOOTER_TOTAL_SELECTORS:
        locator = page.locator(selector).first
        if not await locator.count():
            continue
        text = await _locator_text(locator)
        if text:
            return text
    return None


async def _wait_for_archive_filter_refresh_completion(
    *,
    page: Page,
    store: UcStore,
    logger: JsonLogger,
    pre_filter_footer_window: tuple[int, int, int] | None,
) -> tuple[int, int, int] | None:
    pre_filter_footer_text = await _get_archive_footer_text(page)
    pre_filter_row_signatures = await _get_archive_row_signatures(page)
    deadline = asyncio.get_running_loop().time() + (NAV_TIMEOUT_MS / 1000)
    redraw_detected = False
    footer_changed = False

    while asyncio.get_running_loop().time() < deadline:
        current_row_signatures = await _get_archive_row_signatures(page)
        if current_row_signatures != pre_filter_row_signatures:
            redraw_detected = True

        current_footer_text = await _get_archive_footer_text(page)
        if pre_filter_footer_text:
            if current_footer_text and current_footer_text != pre_filter_footer_text:
                footer_changed = True
        elif current_footer_text:
            footer_changed = True

        if redraw_detected and footer_changed and current_footer_text:
            await page.wait_for_timeout(600)
            stable_footer_text_mid = await _get_archive_footer_text(page)
            await page.wait_for_timeout(600)
            stable_footer_text_end = await _get_archive_footer_text(page)
            if (
                stable_footer_text_mid
                and stable_footer_text_end
                and current_footer_text == stable_footer_text_mid == stable_footer_text_end
            ):
                post_filter_footer_window = _parse_archive_footer_window(current_footer_text)
                log_event(
                    logger=logger,
                    phase="filters",
                    message="Archive Orders filter refresh settled",
                    store_code=store.store_code,
                    pre_filter_footer_window=pre_filter_footer_window,
                    post_filter_footer_window=post_filter_footer_window,
                )
                return post_filter_footer_window

        await page.wait_for_timeout(250)

    post_filter_footer_window = await _get_archive_footer_window(page)
    log_event(
        logger=logger,
        phase="filters",
        status="warn",
        message="Archive Orders filter refresh completion signal timed out",
        store_code=store.store_code,
        pre_filter_footer_window=pre_filter_footer_window,
        post_filter_footer_window=post_filter_footer_window,
    )
    return post_filter_footer_window


def _is_payment_pending(payment_text: str | None) -> bool:
    if not payment_text:
        return False
    return "payment pending" in payment_text.lower()


async def _extract_archive_base_row(
    *,
    row: Locator,
    store: UcStore,
    logger: JsonLogger,
    row_index: int,
    order_code: str,
) -> dict[str, Any] | None:
    pickup = await _locator_text(row.locator("td").nth(1))
    delivery = await _locator_text(row.locator("td").nth(2))
    customer_name = await _locator_text(row.locator(".customer-name"))
    customer_phone = await _locator_text(row.locator(".customer-phone"))
    address = await _locator_text(row.locator(".address-col"))
    payment_text = await _locator_text(row.locator(".payment-col"))
    instructions = await _locator_text(row.locator("td").nth(6))
    status = await _locator_text(row.locator(".status-col span"))
    status_date = await _locator_text(row.locator(".status-col .status-date"))
    return {
        "store_code": store.store_code,
        "order_code": order_code,
        "pickup": pickup,
        "delivery": delivery,
        "customer_name": customer_name,
        "customer_phone": customer_phone,
        "address": address,
        "payment_text": payment_text,
        "instructions": instructions,
        "customer_source": None,
        "status": status,
        "status_date": status_date,
    }


async def _extract_order_details(
    *,
    page: Page,
    row: Locator,
    store: UcStore,
    logger: JsonLogger,
    order_code: str,
) -> tuple[list[dict[str, Any]], str | None, str | None, bool]:
    details: list[dict[str, Any]] = []
    clicked, selector_diagnostics, click_error = await _click_order_details_trigger(row)
    if not clicked:
        row_html = ""
        with contextlib.suppress(Exception):
            row_html = (await row.inner_html())[:DOM_SNIPPET_MAX_CHARS]
        log_event(
            logger=logger,
            phase="warnings",
            status="warn",
            message="Order details trigger click failed; skipping order",
            store_code=store.store_code,
            order_code=order_code,
            row_html_snippet=row_html,
            selector_diagnostics=selector_diagnostics,
            click_error=click_error,
        )
        return details, None, None, False

    modal = page.locator("mat-dialog-container").last
    try:
        await modal.wait_for(timeout=NAV_TIMEOUT_MS)
    except TimeoutError:
        row_html = ""
        with contextlib.suppress(Exception):
            row_html = (await row.inner_html())[:DOM_SNIPPET_MAX_CHARS]
        log_event(
            logger=logger,
            phase="warnings",
            status="warn",
            message="Order details modal did not appear; skipping order",
            store_code=store.store_code,
            order_code=order_code,
        )
        return details, None, None, False

    order_number = order_code
    order_mode = _normalize_order_mode(
        await _locator_text(modal.locator(".order-info-label .order-mode").first)
    )
    order_datetime = None
    pickup_datetime = None
    delivery_datetime = None
    info_items = modal.locator(".order-info-item")
    for idx in range(await info_items.count()):
        item = info_items.nth(idx)
        label = await _locator_text(item.locator(".order-info-label"))
        value = await _locator_text(item.locator(".order-info-value"))
        if not value:
            value = await _locator_text(item.locator("div[style*='font-size: 13px']").first)
        if not value:
            value = await _locator_text(item.locator("div").nth(1))

        item_text = await _locator_text(item)
        key = _normalize_order_info_key(label or item_text)

        if "order no" in key:
            match = re.search(r"Order No\.\s*-\s*([A-Za-z0-9-]+)", label or "")
            if match:
                order_number = match.group(1)
            mode_match = re.search(r"\(([^)]+)\)", label or "")
            if mode_match and not order_mode:
                order_mode = _normalize_order_mode(mode_match.group(0))
            order_datetime = value or order_datetime
        elif "order date" in key:
            order_datetime = value or order_datetime
        elif "pickup" in key:
            pickup_datetime = value or pickup_datetime
        elif "delivery" in key:
            delivery_datetime = value or delivery_datetime

    item_rows = modal.locator(".order-breakup tbody tr")
    for idx in range(await item_rows.count()):
        item_row = item_rows.nth(idx)
        cells = item_row.locator("td")
        service = _normalize_cell_text(await _locator_text(cells.nth(1)))
        hsn_sac = _normalize_cell_text(await _locator_text(cells.nth(2)))
        item_names = _split_multiline_cell(await _locator_text(cells.nth(3)))
        rates = _split_multiline_cell(await _locator_text(cells.nth(4)))
        quantities = _split_multiline_cell(await _locator_text(cells.nth(5)))
        weights = _split_multiline_cell(await _locator_text(cells.nth(6)))
        addons_list = _split_multiline_cell(await _locator_text(cells.nth(7)))
        amounts = _split_multiline_cell(await _locator_text(cells.nth(8)))

        max_items = max(
            1,
            len(item_names),
            len(rates),
            len(quantities),
            len(weights),
            len(addons_list),
            len(amounts),
        )
        for item_index in range(max_items):
            details.append(
                {
                    "store_code": store.store_code,
                    "order_code": order_number,
                    "order_mode": order_mode,
                    "order_datetime": order_datetime,
                    "pickup_datetime": pickup_datetime,
                    "delivery_datetime": delivery_datetime,
                    "service": service,
                    "hsn_sac": hsn_sac,
                    "item_name": item_names[item_index] if item_index < len(item_names) else None,
                    "rate": rates[item_index] if item_index < len(rates) else None,
                    "quantity": quantities[item_index] if item_index < len(quantities) else None,
                    "weight": weights[item_index] if item_index < len(weights) else None,
                    "addons": addons_list[item_index] if item_index < len(addons_list) else None,
                    "amount": amounts[item_index] if item_index < len(amounts) else None,
                }
            )

    close_button = modal.locator("button:has-text('Close')").first
    if await close_button.count():
        with contextlib.suppress(Exception):
            await close_button.click()
    else:
        with contextlib.suppress(Exception):
            await page.keyboard.press("Escape")
    with contextlib.suppress(TimeoutError):
        await modal.wait_for(state="detached", timeout=10_000)
    return details, order_mode, pickup_datetime, True


async def _extract_payment_details(
    *,
    page: Page,
    row: Locator,
    store: UcStore,
    logger: JsonLogger,
    order_code: str,
) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    payment_trigger = row.locator(".payment-details-available").first
    if not await payment_trigger.count():
        return details
    await payment_trigger.click()
    modal = page.locator(".modal-content").last
    try:
        await modal.wait_for(timeout=NAV_TIMEOUT_MS)
    except TimeoutError:
        log_event(
            logger=logger,
            phase="warnings",
            status="warn",
            message="Payment details modal did not appear",
            store_code=store.store_code,
            order_code=order_code,
        )
        return details

    header_text = await _locator_text(modal.locator("h2"))
    resolved_order_code = order_code
    if header_text:
        match = re.search(r"Order\s+([A-Za-z0-9-]+)", header_text)
        if match:
            resolved_order_code = match.group(1)
    payment_items = modal.locator(".payment-item")
    for idx in range(await payment_items.count()):
        item = payment_items.nth(idx)
        mode = None
        amount = None
        payment_date = None
        transaction_id = None
        for line in await item.locator("div").all_inner_texts():
            if ":" not in line:
                continue
            label, value = line.split(":", 1)
            normalized = label.strip().lower()
            value = value.strip()
            if "mode" in normalized:
                mode = value
            elif "amount" in normalized:
                amount = value
            elif "date" in normalized:
                payment_date = value
            elif "transaction" in normalized or "txn" in normalized:
                transaction_id = value
        details.append(
            {
                "store_code": store.store_code,
                "order_code": resolved_order_code,
                "payment_mode": mode,
                "amount": amount,
                "payment_date": payment_date,
                "transaction_id": transaction_id,
            }
        )

    close_button = modal.locator(".close-button").first
    if await close_button.count():
        with contextlib.suppress(Exception):
            await close_button.click()
    else:
        with contextlib.suppress(Exception):
            await page.keyboard.press("Escape")
    with contextlib.suppress(TimeoutError):
        await modal.wait_for(state="detached", timeout=10_000)
    return details


async def _get_archive_order_code(row: Locator) -> str | None:
    for selector in ARCHIVE_ORDER_CODE_SELECTORS:
        locator = row.locator(selector)
        text = await _locator_text(locator)
        if text:
            return text.strip()

        nested_candidates = (
            locator.locator("span"),
            locator.locator("a"),
            locator.locator("button"),
            locator.locator("[role='button']"),
        )
        for nested in nested_candidates:
            nested_text = await _locator_text(nested)
            if nested_text:
                return nested_text.strip()
    return None


async def _get_first_order_code(page: Page) -> str | None:
    locator = page.locator(f"{ARCHIVE_TABLE_ROW_SELECTOR} td.order-col span").first
    return await _locator_text(locator)


async def _get_archive_row_signatures(page: Page) -> tuple[str, ...]:
    row_locator = page.locator(ARCHIVE_TABLE_ROW_SELECTOR)
    signatures: list[str] = []
    for idx in range(await row_locator.count()):
        order_code = await _get_archive_order_code(row_locator.nth(idx))
        if order_code:
            signatures.append(order_code)
    return tuple(signatures)


async def _wait_for_archive_pagination_stability(
    *,
    page: Page,
    initial_footer_window: tuple[int, int, int] | None,
    initial_row_signatures: tuple[str, ...],
    required_consecutive_reads: int = ARCHIVE_PAGINATION_STABILIZATION_READS,
    interval_ms: int = ARCHIVE_PAGINATION_STABILIZATION_INTERVAL_MS,
    timeout_ms: int = ARCHIVE_PAGINATION_STABILIZATION_TIMEOUT_MS,
) -> tuple[tuple[int, int, int] | None, tuple[str, ...], bool]:
    reads_needed = max(1, required_consecutive_reads)
    stable_reads = 0
    last_footer_window = initial_footer_window
    last_row_signatures = initial_row_signatures
    deadline = asyncio.get_running_loop().time() + (timeout_ms / 1000)

    while asyncio.get_running_loop().time() < deadline:
        current_footer_window = await _get_archive_footer_window(page)
        current_row_signatures = await _get_archive_row_signatures(page)
        if current_footer_window == last_footer_window and current_row_signatures == last_row_signatures:
            stable_reads += 1
            if stable_reads >= reads_needed:
                return current_footer_window, current_row_signatures, True
        else:
            stable_reads = 0
            last_footer_window = current_footer_window
            last_row_signatures = current_row_signatures
        await page.wait_for_timeout(interval_ms)

    return last_footer_window, last_row_signatures, False


async def _is_button_disabled(locator: Locator) -> bool:
    if not await locator.count():
        return True
    if await locator.is_disabled():
        return True
    aria_disabled = (await locator.get_attribute("aria-disabled")) or ""
    if aria_disabled.strip().lower() in {"true", "1"}:
        return True
    if await locator.get_attribute("disabled") is not None:
        return True
    classes = (await locator.get_attribute("class")) or ""
    return "disabled" in classes.lower()


async def _get_archive_page_number(page: Page) -> int | None:
    selectors = [
        ".pagination-btn.active",
        ".pagination-btn.current",
        ".pagination-btn[aria-current='page']",
        ".pagination .active",
        ".pagination [aria-current='page']",
    ]
    for selector in selectors:
        locator = page.locator(selector).first
        if not await locator.count():
            continue
        text = await _locator_text(locator)
        if not text:
            continue
        match = re.search(r"\d+", text)
        if match:
            return int(match.group(0))
    return None


async def _collect_archive_orders(
    *,
    page: Page,
    store: UcStore,
    logger: JsonLogger,
    filtered_footer_window: tuple[int, int, int] | None,
) -> ArchiveOrdersExtract:
    extract = ArchiveOrdersExtract()
    seen_orders: set[str] = set()
    page_index = 1
    footer_total: int | None = filtered_footer_window[2] if filtered_footer_window is not None else None
    partial_reason: str | None = None

    if footer_total is not None:
        extract.footer_total = footer_total

    while True:
        with contextlib.suppress(TimeoutError):
            await page.wait_for_selector(ARCHIVE_TABLE_ROW_SELECTOR, timeout=NAV_TIMEOUT_MS)
        row_locator = page.locator(ARCHIVE_TABLE_ROW_SELECTOR)
        raw_row_count = await row_locator.count()
        valid_rows: list[tuple[Locator, str]] = []
        for idx in range(raw_row_count):
            row = row_locator.nth(idx)
            order_code = await _get_archive_order_code(row)
            if not order_code:
                log_event(
                    logger=logger,
                    phase="warnings",
                    status="warn",
                    message="Archive Orders row missing order code; skipping details",
                    store_code=store.store_code,
                    page_number=page_index,
                    row_index=idx,
                )
                continue
            valid_rows.append((row, order_code))

        initial_footer_window = filtered_footer_window if page_index == 1 else await _get_archive_footer_window(page)
        row_signatures = tuple(order_code for _, order_code in valid_rows)
        footer_window, row_signatures, pagination_stable = await _wait_for_archive_pagination_stability(
            page=page,
            initial_footer_window=initial_footer_window,
            initial_row_signatures=row_signatures,
        )
        if footer_window is not None:
            footer_total = footer_window[2]
            extract.footer_total = footer_total
        if not pagination_stable:
            log_event(
                logger=logger,
                phase="pagination",
                status="warn",
                message="Archive Orders pagination stability wait timed out; using latest observed snapshot",
                store_code=store.store_code,
                page_number=page_index,
                footer_window=footer_window,
                row_signatures=row_signatures,
            )

        row_count = raw_row_count
        extract.page_count += 1
        log_event(
            logger=logger,
            phase="pagination",
            message="Archive Orders page loaded",
            store_code=store.store_code,
            page_number=page_index,
            row_count=row_count,
            footer_total=footer_total,
            footer_window=footer_window,
        )
        if row_count == 0:
            log_event(
                logger=logger,
                phase="warnings",
                status="warn",
                message="No archive rows found on page",
                store_code=store.store_code,
                page_number=page_index,
            )
        new_unique_orders = 0
        for idx, (row, order_code) in enumerate(valid_rows):
            base_row = await _extract_archive_base_row(
                row=row, store=store, logger=logger, row_index=idx, order_code=order_code
            )
            if not base_row:
                continue
            if order_code in seen_orders:
                log_event(
                    logger=logger,
                    phase="warnings",
                    status="warn",
                    message="Duplicate order code encountered; skipping",
                    store_code=store.store_code,
                    order_code=order_code,
                    page_number=page_index,
                )
                continue
            seen_orders.add(order_code)
            new_unique_orders += 1
            extract.base_rows.append(base_row)

            try:
                detail_rows, customer_source, pickup_datetime, details_extracted = await _extract_order_details(
                    page=page,
                    row=row,
                    store=store,
                    logger=logger,
                    order_code=order_code,
                )
                extract.order_detail_rows.extend(detail_rows)
                if not details_extracted:
                    _record_skipped_order(
                        extract,
                        order_code=order_code,
                        reason="order_details_unavailable",
                    )
                if customer_source:
                    base_row["customer_source"] = customer_source
                if not base_row.get("pickup") and pickup_datetime:
                    base_row["pickup"] = pickup_datetime
            except Exception as exc:
                log_event(
                    logger=logger,
                    phase="warnings",
                    status="warn",
                    message="Failed to extract order details",
                    store_code=store.store_code,
                    order_code=order_code,
                    error=str(exc),
                )
            try:
                if _is_payment_pending(base_row.get("payment_text")):
                    log_event(
                        logger=logger,
                        phase="extraction",
                        message="Payment pending; skipping payment details extraction",
                        store_code=store.store_code,
                        order_code=order_code,
                        page_number=page_index,
                    )
                else:
                    payment_rows = await _extract_payment_details(
                        page=page,
                        row=row,
                        store=store,
                        logger=logger,
                        order_code=order_code,
                    )
                    extract.payment_detail_rows.extend(payment_rows)
            except Exception as exc:
                log_event(
                    logger=logger,
                    phase="warnings",
                    status="warn",
                    message="Failed to extract payment details",
                    store_code=store.store_code,
                    order_code=order_code,
                    error=str(exc),
                )

        if footer_window is not None and footer_window[1] >= footer_window[2]:
            log_event(
                logger=logger,
                phase="pagination",
                message="Archive Orders footer window reached total; stopping pagination",
                store_code=store.store_code,
                page_number=page_index,
                footer_total=footer_window[2],
                footer_window=footer_window,
                total_rows=len(seen_orders),
            )
            break
        if footer_window is None and footer_total is not None and len(seen_orders) >= footer_total:
            log_event(
                logger=logger,
                phase="pagination",
                message="Archive Orders footer total reached; stopping pagination",
                store_code=store.store_code,
                page_number=page_index,
                footer_total=footer_total,
                total_rows=len(seen_orders),
            )
            break

        force_retry_navigation = False
        if footer_window is not None and new_unique_orders == 0 and footer_window[1] < footer_window[2]:
            force_retry_navigation = True
            pre_retry_signatures = await _get_archive_row_signatures(page)
            log_event(
                logger=logger,
                phase="pagination",
                status="warn",
                message="No new unique orders found while footer indicates more rows; forcing pagination retry",
                store_code=store.store_code,
                page_number=page_index,
                footer_window=footer_window,
                row_signatures=pre_retry_signatures,
            )

        next_button = page.locator(ARCHIVE_NEXT_BUTTON_SELECTOR).first
        if await _is_button_disabled(next_button):
            if footer_window is not None and footer_window[1] < footer_window[2]:
                partial_reason = "partial_extraction_next_disabled_before_footer_total"
                log_event(
                    logger=logger,
                    phase="pagination",
                    status="warn",
                    message="Archive Orders pagination aborted early: next button disabled before footer total",
                    store_code=store.store_code,
                    partial_reason=partial_reason,
                    extracted_rows=len(extract.base_rows),
                    footer_total=footer_window[2],
                    page_count=extract.page_count,
                    footer_window=footer_window,
                )
                break
            log_event(
                logger=logger,
                phase="pagination",
                message="Archive Orders pagination complete",
                store_code=store.store_code,
                page_number=page_index,
                total_rows=len(extract.base_rows),
                footer_total=footer_total,
            )
            break

        if force_retry_navigation:
            with contextlib.suppress(Exception):
                await next_button.scroll_into_view_if_needed(timeout=5_000)
            with contextlib.suppress(Exception):
                await page.wait_for_timeout(500)

        previous_first = await _get_first_order_code(page)
        previous_page_number = await _get_archive_page_number(page)
        previous_footer_window = footer_window
        await next_button.click()
        log_event(
            logger=logger,
            phase="pagination",
            message="Navigated to next page",
            store_code=store.store_code,
            page_number=page_index + 1,
        )
        if previous_first or previous_page_number is not None or previous_footer_window is not None:
            with contextlib.suppress(TimeoutError):
                await page.wait_for_function(
                    r"""([selectors, previousPage, firstSelector, previousFirst, footerSelectors, previousFooter]) => {
                        const findPage = () => {
                            for (const selector of selectors) {
                                const el = document.querySelector(selector);
                                if (!el) continue;
                                const text = el.textContent || "";
                                const match = text.match(/\\d+/);
                                if (match) return parseInt(match[0], 10);
                            }
                            return null;
                        };
                        const findFooterWindow = () => {
                            for (const selector of footerSelectors) {
                                const el = document.querySelector(selector);
                                if (!el) continue;
                                const text = (el.textContent || "").trim();
                                const match = text.match(/showing\s+results\s+([\d,]+)\s+to\s+([\d,]+)\s+of\s+([\d,]+)\s+total/i);
                                if (!match) continue;
                                return [1, 2, 3].map((idx) => parseInt(match[idx].replace(/,/g, ""), 10));
                            }
                            return null;
                        };
                        const currentPage = findPage();
                        const firstEl = document.querySelector(firstSelector);
                        const firstText = firstEl ? (firstEl.textContent || "").trim() : null;
                        const footer = findFooterWindow();
                        const pageChanged =
                            previousPage !== null && currentPage !== null && currentPage !== previousPage;
                        const firstChanged =
                            previousFirst && firstText && firstText !== previousFirst;
                        const footerChanged = previousFooter && footer && footer.join("|") !== previousFooter.join("|");
                        return pageChanged || firstChanged || footerChanged;
                    }""",
                    arg=[
                        [
                            ".pagination-btn.active",
                            ".pagination-btn.current",
                            ".pagination-btn[aria-current='page']",
                            ".pagination .active",
                            ".pagination [aria-current='page']",
                        ],
                        previous_page_number,
                        f"{ARCHIVE_TABLE_ROW_SELECTOR} td.order-col span",
                        previous_first,
                        list(ARCHIVE_FOOTER_TOTAL_SELECTORS),
                        list(previous_footer_window) if previous_footer_window is not None else None,
                    ],
                    timeout=NAV_TIMEOUT_MS,
                )
        current_page_number = await _get_archive_page_number(page)
        current_first = await _get_first_order_code(page)
        current_footer_window = await _get_archive_footer_window(page)
        if force_retry_navigation:
            log_event(
                logger=logger,
                phase="pagination",
                message="Forced pagination retry completed",
                store_code=store.store_code,
                previous_footer_window=previous_footer_window,
                current_footer_window=current_footer_window,
            )
        progress_signals: list[bool] = []
        if previous_page_number is not None and current_page_number is not None:
            progress_signals.append(current_page_number != previous_page_number)
        if previous_first and current_first:
            progress_signals.append(current_first != previous_first)
        if previous_footer_window is not None and current_footer_window is not None:
            progress_signals.append(current_footer_window != previous_footer_window)
        if progress_signals and not any(progress_signals):
            retry_reference_first = current_first
            retry_reference_footer_window = current_footer_window
            retry_reference_page_number = current_page_number
            log_event(
                logger=logger,
                phase="pagination",
                status="warn",
                message="Pagination did not advance; retrying next click once",
                store_code=store.store_code,
                previous_page_number=previous_page_number,
                current_page_number=current_page_number,
                previous_first_order=previous_first,
                current_first_order=current_first,
                previous_footer_window=previous_footer_window,
                current_footer_window=current_footer_window,
            )
            with contextlib.suppress(Exception):
                await next_button.scroll_into_view_if_needed(timeout=5_000)
            with contextlib.suppress(Exception):
                await page.wait_for_timeout(500)
            await next_button.click()
            if (
                retry_reference_first
                or retry_reference_page_number is not None
                or retry_reference_footer_window is not None
            ):
                with contextlib.suppress(TimeoutError):
                    await page.wait_for_function(
                        r"""([selectors, previousPage, firstSelector, previousFirst, footerSelectors, previousFooter]) => {
                            const findPage = () => {
                                for (const selector of selectors) {
                                    const el = document.querySelector(selector);
                                    if (!el) continue;
                                    const text = el.textContent || "";
                                    const match = text.match(/\d+/);
                                    if (match) return parseInt(match[0], 10);
                                }
                                return null;
                            };
                            const findFooterWindow = () => {
                                for (const selector of footerSelectors) {
                                    const el = document.querySelector(selector);
                                    if (!el) continue;
                                    const text = (el.textContent || "").trim();
                                    const match = text.match(/showing\s+results\s+([\d,]+)\s+to\s+([\d,]+)\s+of\s+([\d,]+)\s+total/i);
                                    if (!match) continue;
                                    return [1, 2, 3].map((idx) => parseInt(match[idx].replace(/,/g, ""), 10));
                                }
                                return null;
                            };
                            const currentPage = findPage();
                            const firstEl = document.querySelector(firstSelector);
                            const firstText = firstEl ? (firstEl.textContent || "").trim() : null;
                            const footer = findFooterWindow();
                            const pageChanged =
                                previousPage !== null && currentPage !== null && currentPage !== previousPage;
                            const firstChanged =
                                previousFirst && firstText && firstText !== previousFirst;
                            const footerChanged = previousFooter && footer && footer.join("|") !== previousFooter.join("|");
                            return pageChanged || firstChanged || footerChanged;
                        }""",
                        arg=[
                            [
                                ".pagination-btn.active",
                                ".pagination-btn.current",
                                ".pagination-btn[aria-current='page']",
                                ".pagination .active",
                                ".pagination [aria-current='page']",
                            ],
                            retry_reference_page_number,
                            f"{ARCHIVE_TABLE_ROW_SELECTOR} td.order-col span",
                            retry_reference_first,
                            list(ARCHIVE_FOOTER_TOTAL_SELECTORS),
                            list(retry_reference_footer_window) if retry_reference_footer_window is not None else None,
                        ],
                        timeout=NAV_TIMEOUT_MS,
                    )
            current_page_number = await _get_archive_page_number(page)
            current_first = await _get_first_order_code(page)
            current_footer_window = await _get_archive_footer_window(page)
            progress_signals = []
            if previous_page_number is not None and current_page_number is not None:
                progress_signals.append(current_page_number != previous_page_number)
            if previous_first and current_first:
                progress_signals.append(current_first != previous_first)
            if previous_footer_window is not None and current_footer_window is not None:
                progress_signals.append(current_footer_window != previous_footer_window)
            if progress_signals and not any(progress_signals):
                partial_reason = "partial_extraction_non_advancing_next_click_after_retry"
                log_event(
                    logger=logger,
                    phase="pagination",
                    status="warn",
                    message="Pagination did not advance after one retry; aborting early",
                    store_code=store.store_code,
                    partial_reason=partial_reason,
                    extracted_rows=len(extract.base_rows),
                    footer_total=footer_total,
                    page_count=extract.page_count,
                    previous_page_number=previous_page_number,
                    current_page_number=current_page_number,
                    previous_first_order=previous_first,
                    current_first_order=current_first,
                    previous_footer_window=previous_footer_window,
                    current_footer_window=current_footer_window,
                )
                break
        page_index += 1

    if partial_reason is not None:
        _record_skipped_order(
            extract,
            order_code=f"partial_extraction:{store.store_code}",
            reason=partial_reason,
        )

    if extract.footer_total is not None and len(extract.base_rows) < extract.footer_total:
        _record_skipped_order(
            extract,
            order_code=f"partial_extraction:{store.store_code}",
            reason="partial_extraction_footer_total_mismatch",
        )
        log_event(
            logger=logger,
            phase="warnings",
            status="warn",
            message="Archive Orders extraction ended before footer total was reached",
            store_code=store.store_code,
            extracted_base_rows=len(extract.base_rows),
            footer_total=extract.footer_total,
        )

    log_event(
        logger=logger,
        phase="extraction",
        message="Archive Orders extraction complete",
        store_code=store.store_code,
        base_rows=len(extract.base_rows),
        order_detail_rows=len(extract.order_detail_rows),
        payment_detail_rows=len(extract.payment_detail_rows),
        skipped_order_codes=extract.skipped_order_codes,
        skipped_order_counters=extract.skipped_order_counters,
        page_count=extract.page_count,
        footer_total=extract.footer_total,
    )
    return extract


async def _run_store_discovery(
    *,
    browser: Browser,
    store: UcStore,
    logger: JsonLogger,
    run_env: str,
    run_id: str,
    run_date: datetime,
    summary: UcOrdersDiscoverySummary,
    from_date: date,
    to_date: date,
    download_timeout_ms: int,
) -> None:
    log_event(
        logger=logger,
        phase="store",
        message="Starting UC store discovery",
        store_code=store.store_code,
        store_name=store.store_name,
        run_env=run_env,
        run_id=run_id,
    )

    sync_log_id = await _insert_orders_sync_log(
        logger=logger,
        summary=summary,
        store=store,
        run_id=run_id,
        run_env=run_env,
        run_start_date=from_date,
        run_end_date=to_date,
    )
    if sync_log_id is None:
        log_event(
            logger=logger,
            phase="orders_sync_log",
            status="error",
            message="Hard error: orders sync log row missing; aborting store discovery",
            store_code=store.store_code,
            run_id=run_id,
        )
        return
    download_succeeded = False
    sync_error_message: str | None = None
    row_count: int | None = None

    storage_state_path = store.storage_state_path
    storage_state_exists = storage_state_path.exists()
    storage_state_value = str(storage_state_path) if storage_state_exists else None
    context = await browser.new_context(storage_state=storage_state_value)
    page = await context.new_page()
    outcome = StoreOutcome(status="error", message="uninitialized")

    try:
        if from_date > to_date:
            outcome = StoreOutcome(
                status="warning",
                message="Invalid date range; skipping Archive Orders download",
                login_used=False,
                skip_reason="date range invalid",
            )
            log_event(
                logger=logger,
                phase="filters",
                status="warn",
                message=outcome.message,
                store_code=store.store_code,
                from_date=from_date,
                to_date=to_date,
                skip_reason=outcome.skip_reason,
            )
            summary.record_store(store.store_code, outcome)
            return

        login_used = not storage_state_exists
        session_probe_result: bool | None = None
        fallback_login_attempted = False
        fallback_login_result: bool | None = None

        if storage_state_exists:
            log_event(
                logger=logger,
                phase="login",
                message="Reusing existing storage state",
                store_code=store.store_code,
                storage_state=storage_state_value,
            )

        if not store.home_url:
            outcome = StoreOutcome(
                status="error",
                message="Missing home URL in sync_config",
                login_used=login_used,
                session_probe_result=session_probe_result,
                fallback_login_attempted=fallback_login_attempted,
                fallback_login_result=fallback_login_result,
            )
            log_event(
                logger=logger,
                phase="navigation",
                status="error",
                message=outcome.message,
                store_code=store.store_code,
            )
            summary.record_store(store.store_code, outcome)
            return

        if not store.orders_url:
            outcome = StoreOutcome(
                status="error",
                message="Missing orders_link URL in sync_config",
                login_used=login_used,
                session_probe_result=session_probe_result,
                fallback_login_attempted=fallback_login_attempted,
                fallback_login_result=fallback_login_result,
            )
            log_event(
                logger=logger,
                phase="navigation",
                status="error",
                message=outcome.message,
                store_code=store.store_code,
            )
            summary.record_store(store.store_code, outcome)
            return

        await page.goto(store.home_url, wait_until="domcontentloaded")
        session_probe_result = await _assert_home_ready(page=page, store=store, logger=logger, source="session")
        if not session_probe_result:
            fallback_login_attempted = True
            log_event(
                logger=logger,
                phase="login",
                status="warn",
                message="Session probe failed; attempting full login",
                store_code=store.store_code,
                current_url=page.url,
            )
            login_used = True
            fallback_login_result = await _perform_login(page=page, store=store, logger=logger)
            if fallback_login_result:
                storage_state_path.parent.mkdir(parents=True, exist_ok=True)
                await context.storage_state(path=str(storage_state_path))
                log_event(
                    logger=logger,
                    phase="login",
                    message="Saved storage state",
                    store_code=store.store_code,
                    storage_state=str(storage_state_path),
                )
                if not await _assert_home_ready(page=page, store=store, logger=logger, source="post-login"):
                    fallback_login_result = False
            if not fallback_login_result:
                outcome = StoreOutcome(
                    status="error",
                    message="Login failed after session probe",
                    final_url=page.url,
                    storage_state=str(storage_state_path) if storage_state_path.exists() else None,
                    login_used=login_used,
                    session_probe_result=session_probe_result,
                    fallback_login_attempted=fallback_login_attempted,
                    fallback_login_result=fallback_login_result,
                )
                summary.record_store(store.store_code, outcome)
                return

        try:
            gst_download_ok = False
            gst_download_path: str | None = None
            gst_row_count = 0
            gst_container: Locator | None = None
            gst_navigation_ok = await _navigate_to_gst_reports(page=page, store=store, logger=logger)
            if gst_navigation_ok:
                gst_ready, gst_container = await _wait_for_gst_report_ready(
                    page=page,
                    logger=logger,
                    store=store,
                )
            else:
                gst_ready = False

            if not gst_ready:
                gst_ready, gst_container = await _try_direct_gst_reports(
                    page=page,
                    store=store,
                    logger=logger,
                )

            if not gst_ready or gst_container is None:
                log_event(
                    logger=logger,
                    phase="navigation",
                    status="warn",
                    message="GST report navigation failed; skipping GST download",
                    store_code=store.store_code,
                    current_url=page.url,
                )
            else:
                download_dir = _resolve_uc_download_dir(run_id, store.store_code, from_date, to_date)
                download_dir.mkdir(parents=True, exist_ok=True)
                apply_ok, gst_row_count, row_visibility_issue, ui_issues, failure_reason = await _apply_date_range(
                    page=page,
                    container=gst_container,
                    logger=logger,
                    store=store,
                    from_date=from_date,
                    to_date=to_date,
                )
                if ui_issues:
                    _log_ui_issues(
                        logger=logger,
                        store=store,
                        ui_issues=ui_issues,
                        status="warn" if not apply_ok else "info",
                    )
                if not apply_ok:
                    log_event(
                        logger=logger,
                        phase="filters",
                        status="warn",
                        message="GST report date range apply failed; skipping download",
                        store_code=store.store_code,
                        row_count=gst_row_count,
                        failure_reason=failure_reason,
                    )
                else:
                    if row_visibility_issue:
                        log_event(
                            logger=logger,
                            phase="filters",
                            status="warn",
                            message="GST report rows not visible but export may still be ready",
                            store_code=store.store_code,
                            row_count=gst_row_count,
                        )
                    gst_download_ok, gst_download_path, gst_message = await _download_gst_report(
                        page=page,
                        logger=logger,
                        store=store,
                        from_date=from_date,
                        to_date=to_date,
                        download_dir=download_dir,
                        download_timeout_ms=download_timeout_ms,
                        row_count=gst_row_count,
                    )
                    log_event(
                        logger=logger,
                        phase="download",
                        status=None if gst_download_ok else "warn",
                        message=gst_message if gst_message else "GST report download complete",
                        store_code=store.store_code,
                        download_path=gst_download_path,
                        row_count=gst_row_count,
                    )
        except Exception as exc:
            log_event(
                logger=logger,
                phase="download",
                status="warn",
                message="GST report download failed; continuing to Archive Orders",
                store_code=store.store_code,
                error=str(exc),
            )

        archive_ready = await _navigate_to_archive_orders(page=page, store=store, logger=logger)
        if not archive_ready:
            outcome = StoreOutcome(
                status="warning",
                message="Archive Orders navigation failed",
                final_url=page.url,
                storage_state=str(storage_state_path) if storage_state_path.exists() else None,
                login_used=login_used,
                session_probe_result=session_probe_result,
                fallback_login_attempted=fallback_login_attempted,
                fallback_login_result=fallback_login_result,
                skip_reason="navigation failed",
            )
            summary.record_store(store.store_code, outcome)
            return

        filter_footer_state = await _apply_archive_date_filter(
            page=page,
            store=store,
            logger=logger,
            from_date=from_date,
            to_date=to_date,
        )
        if filter_footer_state is None:
            outcome = StoreOutcome(
                status="warning",
                message="Archive Orders filter failed",
                final_url=page.url,
                storage_state=str(storage_state_path) if storage_state_path.exists() else None,
                login_used=login_used,
                session_probe_result=session_probe_result,
                fallback_login_attempted=fallback_login_attempted,
                fallback_login_result=fallback_login_result,
                skip_reason="date range invalid",
            )
            summary.record_store(store.store_code, outcome)
            return

        download_dir = _resolve_uc_download_dir(run_id, store.store_code, from_date, to_date)
        extract = await _collect_archive_orders(
            page=page,
            store=store,
            logger=logger,
            filtered_footer_window=filter_footer_state.post_filter_footer_window,
        )
        row_count = len(extract.base_rows)
        rows_missing_estimate = _archive_extraction_gap(extract)
        is_partial_extraction = rows_missing_estimate is not None and rows_missing_estimate > 0
        reason_codes: list[str] = ["partial_extraction"] if is_partial_extraction else []
        base_path = download_dir / _format_archive_base_filename(store.store_code, from_date, to_date)
        order_details_path = download_dir / _format_archive_order_details_filename(store.store_code, from_date, to_date)
        payment_details_path = download_dir / _format_archive_payment_details_filename(store.store_code, from_date, to_date)
        base_count = _write_excel_rows(base_path, extract.base_rows, ARCHIVE_BASE_COLUMNS)
        order_details_count = _write_excel_rows(
            order_details_path, extract.order_detail_rows, ARCHIVE_ORDER_DETAIL_COLUMNS
        )
        payment_details_count = _write_excel_rows(
            payment_details_path, extract.payment_detail_rows, ARCHIVE_PAYMENT_COLUMNS
        )
        download_succeeded = True
        await _update_orders_sync_log(
            logger=logger,
            log_id=sync_log_id,
            orders_pulled_at=aware_now(),
        )
        log_event(
            logger=logger,
            phase="output",
            message="Archive Orders files saved",
            store_code=store.store_code,
            base_file=str(base_path),
            base_rows=base_count,
            order_details_file=str(order_details_path),
            order_details_rows=order_details_count,
            payment_details_file=str(payment_details_path),
            payment_details_rows=payment_details_count,
        )

        archive_publish_orders: dict[str, Any] | None = None
        archive_publish_sales: dict[str, Any] | None = None
        stage_statuses: dict[str, str] = {"download": "success", "archive_ingest": "skipped", "archive_publish": "skipped"}
        stage_metrics: dict[str, dict[str, Any]] = {
            "download": {
                "base_rows": base_count,
                "order_details_rows": order_details_count,
                "payment_details_rows": payment_details_count,
                "skipped_order_counters": extract.skipped_order_counters,
                "skipped_order_codes": extract.skipped_order_codes,
                "footer_total": extract.footer_total,
                "base_rows_extracted": len(extract.base_rows),
                "rows_missing_estimate": rows_missing_estimate,
                "reason_codes": reason_codes,
            }
        }

        if not config.database_url:
            stage_statuses["archive_ingest"] = "failed"
            stage_statuses["archive_publish"] = "skipped"
            stage_metrics["archive_ingest"] = {"error": "database_url is required for archive ingest/publish"}
            log_event(
                logger=logger,
                phase="archive_ingest",
                status="warn",
                message="UC archive ingest failed; continuing with partial success",
                store_code=store.store_code,
                error=stage_metrics["archive_ingest"]["error"],
            )
        else:
            ingest_completed = False
            try:
                ingest_result = await ingest_uc_archive_excels(
                    database_url=config.database_url,
                    run_id=run_id,
                    run_date=aware_now(),
                    store_code=store.store_code,
                    cost_center=store.cost_center,
                    base_order_info_path=base_path,
                    order_details_path=order_details_path,
                    payment_details_path=payment_details_path,
                    logger=logger,
                )
                ingest_metrics = {
                    "files": {k: vars(v) for k, v in ingest_result.files.items()},
                    "rejects": len(ingest_result.rejects),
                }
                stage_metrics["archive_ingest"] = ingest_metrics
                stage_statuses["archive_ingest"] = "success"
                ingest_completed = True
                log_event(
                    logger=logger,
                    phase="archive_ingest",
                    status="info",
                    message="UC archive ingest completed",
                    store_code=store.store_code,
                    **ingest_metrics,
                )
            except Exception as exc:
                stage_statuses["archive_ingest"] = "failed"
                stage_statuses["archive_publish"] = "skipped"
                stage_metrics["archive_ingest"] = {"error": str(exc)}
                log_event(
                    logger=logger,
                    phase="archive_ingest",
                    status="warn",
                    message="UC archive ingest failed; continuing with partial success",
                    store_code=store.store_code,
                    error=str(exc),
                )

            if ingest_completed:
                publish_failed = False
                orders_error: str | None = None
                sales_error: str | None = None
                try:
                    orders_publish = await publish_uc_archive_order_details_to_orders(database_url=config.database_url)
                    archive_publish_orders = {
                        "inserted": orders_publish.inserted,
                        "updated": orders_publish.updated,
                        "skipped": orders_publish.skipped,
                        "warnings": orders_publish.warnings,
                        "reason_codes": orders_publish.reason_codes,
                    }
                    log_event(
                        logger=logger,
                        phase="archive_publish_orders",
                        status="info",
                        message="UC archive order-details publish completed",
                        store_code=store.store_code,
                        metrics=archive_publish_orders,
                    )
                except Exception as exc:
                    publish_failed = True
                    orders_error = str(exc)
                    archive_publish_orders = {"error": orders_error}
                    log_event(
                        logger=logger,
                        phase="archive_publish_orders",
                        status="warn",
                        message="UC archive order-details publish failed; continuing to payments publish",
                        store_code=store.store_code,
                        error=orders_error,
                    )

                try:
                    sales_publish = await publish_uc_archive_payments_to_sales(database_url=config.database_url)
                    archive_publish_sales = {
                        "inserted": sales_publish.inserted,
                        "updated": sales_publish.updated,
                        "skipped": sales_publish.skipped,
                        "warnings": sales_publish.warnings,
                        "reason_codes": sales_publish.reason_codes,
                        "publish_parent_match_rate": sales_publish.publish_parent_match_rate,
                        "missing_parent_count": sales_publish.missing_parent_count,
                        "preflight_warning": sales_publish.preflight_warning,
                        "preflight_diagnostics": sales_publish.preflight_diagnostics,
                    }
                    log_event(
                        logger=logger,
                        phase="archive_publish_sales",
                        status="warn" if sales_publish.preflight_warning else "info",
                        message="UC archive payment publish completed",
                        store_code=store.store_code,
                        metrics=archive_publish_sales,
                    )
                except Exception as exc:
                    publish_failed = True
                    sales_error = str(exc)
                    archive_publish_sales = {"error": sales_error}
                    log_event(
                        logger=logger,
                        phase="archive_publish_sales",
                        status="warn",
                        message="UC archive payment publish failed; continuing with partial success",
                        store_code=store.store_code,
                        error=sales_error,
                    )

                stage_statuses["archive_publish"] = "failed" if publish_failed else "success"
                stage_metrics["archive_publish"] = {
                    "order_details_to_orders": archive_publish_orders,
                    "payment_details_to_sales": archive_publish_sales,
                }
                if orders_error or sales_error:
                    stage_metrics["archive_publish"]["errors"] = {
                        "order_details_to_orders": orders_error,
                        "payment_details_to_sales": sales_error,
                    }

        status_label = "warning" if any(v == "failed" for k, v in stage_statuses.items() if k != "download") else "ok"
        download_message = "Archive Orders download complete"
        if is_partial_extraction:
            status_label = "warning"
            download_message = "Archive Orders extracted partially (footer total mismatch)"
        elif status_label == "warning":
            failed_stages = [name for name, stage_status in stage_statuses.items() if stage_status == "failed"]
            download_message = (
                "Archive Orders download complete; archive stages failed: " + ", ".join(sorted(failed_stages))
            )
        outcome = StoreOutcome(
            status=status_label,
            message=download_message,
            final_url=page.url,
            storage_state=str(storage_state_path) if storage_state_path.exists() else None,
            login_used=login_used,
            session_probe_result=session_probe_result,
            fallback_login_attempted=fallback_login_attempted,
            fallback_login_result=fallback_login_result,
            download_path=str(base_path),
            rows_downloaded=row_count,
            stage_statuses=stage_statuses,
            stage_metrics=stage_metrics,
            archive_publish_orders=archive_publish_orders,
            archive_publish_sales=archive_publish_sales,
            reason_codes=reason_codes,
            footer_total=extract.footer_total,
            base_rows_extracted=len(extract.base_rows),
            rows_missing_estimate=rows_missing_estimate,
        )
        summary.record_store(store.store_code, outcome)
        log_event(
            logger=logger,
            phase="store",
            message="UC store discovery complete",
            store_code=store.store_code,
            final_url=page.url,
        )
    except TimeoutError as exc:
        outcome = StoreOutcome(
            status="warning",
            message="Timeout while loading Archive Orders page",
            final_url=page.url,
            storage_state=str(storage_state_path) if storage_state_path.exists() else None,
            login_used=login_used,
            session_probe_result=session_probe_result,
            fallback_login_attempted=fallback_login_attempted,
            fallback_login_result=fallback_login_result,
            skip_reason="timeout",
        )
        summary.record_store(store.store_code, outcome)
        log_event(
            logger=logger,
            phase="store",
            status="warn",
            message=str(exc),
            store_code=store.store_code,
        )
    except Exception as exc:
        outcome = StoreOutcome(
            status="error",
            message="Store discovery failed",
            final_url=page.url,
            storage_state=str(storage_state_path) if storage_state_path.exists() else None,
            login_used=login_used,
            session_probe_result=session_probe_result,
            fallback_login_attempted=fallback_login_attempted,
            fallback_login_result=fallback_login_result,
        )
        sync_error_message = str(exc)
        summary.record_store(store.store_code, outcome)
        log_event(
            logger=logger,
            phase="store",
            status="error",
            message="Store discovery failed",
            store_code=store.store_code,
            error=str(exc),
        )
    finally:
        sync_status = _resolve_sync_log_status(
            outcome=outcome, download_succeeded=download_succeeded, row_count=row_count
        )
        has_rows = _has_rows(row_count, outcome.staging_rows)
        no_data = not outcome.download_path and outcome.staging_rows == 0
        skip_reason: str | None = None
        if sync_status == "skipped":
            skip_reason = outcome.skip_reason
            if skip_reason is None and no_data:
                skip_reason = "no data"
            if skip_reason is None and outcome.message:
                skip_reason = outcome.message
            outcome.skip_reason = skip_reason
        sync_error_value = None
        if sync_status == "failed":
            sync_error_value = sync_error_message or outcome.message
        status_note = _resolve_sync_log_status_note(
            status=sync_status, outcome=outcome, row_count=row_count
        )
        resolved_message = _resolve_sync_log_message(
            status=sync_status, error_message=sync_error_value, status_note=status_note
        )
        if resolved_message:
            outcome.message = resolved_message
        primary_metrics, secondary_metrics = _build_unified_metrics(outcome)
        attempt_no = _parse_attempt_no(run_id) or 1
        await _update_orders_sync_log(
            logger=logger,
            log_id=sync_log_id,
            status=sync_status,
            error_message=resolved_message,
            primary_metrics=primary_metrics,
            secondary_metrics=secondary_metrics,
        )
        log_event(
            logger=logger,
            phase="window_result",
            message="UC Archive Orders window completed",
            store_code=store.store_code,
            run_id=run_id,
            from_date=from_date,
            to_date=to_date,
            window_status=sync_status,
            status_note=status_note,
            error_message=resolved_message,
            attempt_no=attempt_no,
            download_path=outcome.download_path,
            final_rows=outcome.final_rows,
            rows_downloaded=outcome.rows_downloaded,
            unique_inserted=outcome.final_inserted,
            overlap_duplicates_updated=outcome.final_updated,
            rows_skipped_invalid=outcome.rows_skipped_invalid,
            rows_skipped_invalid_reasons=outcome.rows_skipped_invalid_reasons,
            reason_codes=outcome.reason_codes,
            footer_total=outcome.footer_total,
            base_rows_extracted=outcome.base_rows_extracted,
            rows_missing_estimate=outcome.rows_missing_estimate,
            primary_metrics=primary_metrics,
            secondary_metrics=secondary_metrics,
        )
        log_event(
            logger=logger,
            phase="uc_window",
            message="UC Archive Orders window complete",
            store_code=store.store_code,
            from_date=from_date,
            to_date=to_date,
            archive_download_path=outcome.download_path,
            archive_rows=outcome.rows_downloaded,
            final_status=sync_status,
            skip_reason=status_note,
            attempt_no=attempt_no,
        )
        summary.window_audit.append(
            {
                "store_code": store.store_code,
                "from_date": from_date.isoformat(),
                "to_date": to_date.isoformat(),
                "status": sync_status,
                "status_note": status_note,
                "error_message": resolved_message,
                "download_path": outcome.download_path,
                "final_rows": outcome.final_rows,
                "rows_downloaded": outcome.rows_downloaded,
                "unique_inserted": outcome.final_inserted,
                "overlap_duplicates_updated": outcome.final_updated,
                "rows_skipped_invalid": outcome.rows_skipped_invalid,
                "rows_skipped_invalid_reasons": outcome.rows_skipped_invalid_reasons,
                "reason_codes": list(outcome.reason_codes),
                "footer_total": outcome.footer_total,
                "base_rows_extracted": outcome.base_rows_extracted,
                "rows_missing_estimate": outcome.rows_missing_estimate,
                "attempt_no": attempt_no,
                "orders_sync_log_id": sync_log_id,
            }
        )
        with contextlib.suppress(Exception):
            await context.close()


async def _perform_login(*, page: Page, store: UcStore, logger: JsonLogger) -> bool:
    missing_fields = [
        label
        for label, value in (
            ("login_url", store.login_url),
            ("username", store.username),
            ("password", store.password),
        )
        if not value
    ]
    if missing_fields:
        log_event(
            logger=logger,
            phase="login",
            status="error",
            message="Login configuration incomplete",
            store_code=store.store_code,
            missing_fields=missing_fields,
        )
        return False

    selectors = UC_LOGIN_SELECTORS

    await page.goto(store.login_url or "", wait_until="domcontentloaded")
    element_locator = page.locator("input, button, select")
    visible_elements: list[dict[str, str]] = []
    try:
        element_count = await element_locator.count()
    except Exception:
        element_count = 0
    for idx in range(element_count):
        element = element_locator.nth(idx)
        try:
            if not await element.is_visible():
                continue
            tag_name = await element.evaluate("el => el.tagName.toLowerCase()")
            class_name = await element.get_attribute("class") or ""
            visible_elements.append(
                {
                    "tag": tag_name,
                    "type": await element.get_attribute("type") or "",
                    "name": await element.get_attribute("name") or "",
                    "id": await element.get_attribute("id") or "",
                    "placeholder": await element.get_attribute("placeholder") or "",
                    "aria_label": await element.get_attribute("aria-label") or "",
                    "class": " ".join(class_name.split()),
                }
            )
        except Exception:
            continue
    if visible_elements:
        log_event(
            logger=logger,
            phase="login",
            status="debug",
            message="Login page form elements snapshot (remove once selectors confirmed)",
            store_code=store.store_code,
            current_url=page.url,
            element_count=len(visible_elements),
            elements=visible_elements[:20],
        )
    for field in ("username", "password"):
        selector = selectors[field]
        locator = page.locator(selector).first
        try:
            await locator.wait_for(state="visible", timeout=NAV_TIMEOUT_MS)
        except TimeoutError:
            log_event(
                logger=logger,
                phase="login",
                status="error",
                message="Login selector not visible within timeout",
                store_code=store.store_code,
                selector=selector,
                field=field,
                timeout_ms=NAV_TIMEOUT_MS,
            )
            return False

    username_locator = page.locator(selectors["username"]).first
    password_locator = page.locator(selectors["password"]).first
    await username_locator.click()
    await username_locator.type(store.username or "", delay=50)
    await username_locator.press("Tab")
    await password_locator.click()
    await password_locator.type(store.password or "", delay=50)
    await password_locator.press("Tab")

    username_value = (await username_locator.input_value()).strip()
    password_value = (await password_locator.input_value()).strip()
    log_event(
        logger=logger,
        phase="login",
        status="debug",
        message="Login inputs populated",
        store_code=store.store_code,
        username_length=len(username_value),
        password_length=len(password_value),
    )
    empty_fields = [name for name, value in (("username", username_value), ("password", password_value)) if not value]
    if empty_fields:
        dom_snippet = await _maybe_get_dom_snippet(page)
        log_event(
            logger=logger,
            phase="login",
            status="warn",
            message="Login input empty after fill",
            store_code=store.store_code,
            empty_fields=empty_fields,
            **_dom_snippet_fields(dom_snippet),
        )
        log_event(
            logger=logger,
            phase="login",
            status="error",
            message="Login aborted due to empty credential fields",
            store_code=store.store_code,
        )
        return False

    def _login_response_matches(url: str) -> bool:
        url_lower = url.lower()
        login_url = (store.login_url or "").lower()
        if login_url and login_url in url_lower:
            return True
        return any(token in url_lower for token in ("/login", "auth", "signin", "session"))

    response = None
    try:
        async with page.expect_response(
            lambda response: response.status in {200, 302} and _login_response_matches(response.url),
            timeout=15_000,
        ) as response_info:
            await page.click(selectors["submit"])
        response = await response_info.value
    except TimeoutError:
        log_event(
            logger=logger,
            phase="login",
            status="warning",
            message="Login response not observed after submit; falling back to post-login checks",
            store_code=store.store_code,
            current_url=page.url,
        )
    except Exception as exc:
        log_event(
            logger=logger,
            phase="login",
            status="warning",
            message="Login submit click failed",
            store_code=store.store_code,
            selector=selectors["submit"],
            error=str(exc),
        )
        return False

    await page.wait_for_load_state("domcontentloaded")
    log_event(
        logger=logger,
        phase="login",
        message="Login submitted",
        store_code=store.store_code,
        selector=selectors["submit"],
    )
    if response is not None:
        log_event(
            logger=logger,
            phase="login",
            status="debug",
            message="Login response observed",
            store_code=store.store_code,
            response_url=response.url,
            response_status=response.status,
        )
    else:
        log_event(
            logger=logger,
            phase="login",
            status="warning",
            message="Login response missing; relying on post-login element checks",
            store_code=store.store_code,
        )
    with contextlib.suppress(TimeoutError):
        await page.wait_for_url(lambda url: not _url_is_login(url, store), timeout=10_000)
        log_event(
            logger=logger,
            phase="login",
            status="debug",
            message="Login URL changed away from /login",
            store_code=store.store_code,
            current_url=page.url,
        )

    nav_selector = ", ".join(store.home_selectors or HOME_READY_SELECTORS)
    error_selector = ", ".join(
        (
            "[role='alert']",
            ".alert",
            ".toast",
            ".toast-error",
            ".notification",
            ".error",
            ".invalid-feedback",
            ".message-error",
        )
    )
    nav_locator = page.locator(nav_selector).first
    error_locator = page.locator(error_selector).first
    post_login_tasks = {
        "nav": asyncio.create_task(nav_locator.wait_for(state="visible", timeout=10_000)),
        "error": asyncio.create_task(error_locator.wait_for(state="visible", timeout=10_000)),
    }
    done, pending = await asyncio.wait(post_login_tasks.values(), timeout=10, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    if done:
        completed_label = next(label for label, task in post_login_tasks.items() if task in done)
        if completed_label == "error":
            banner_text = ""
            with contextlib.suppress(Exception):
                banner_text = (await error_locator.inner_text()).strip()
            log_event(
                logger=logger,
                phase="login",
                status="error",
                message="Login error banner detected after submit; aborting login",
                store_code=store.store_code,
                banner_text=banner_text,
                current_url=page.url,
            )
            return False
        log_event(
            logger=logger,
            phase="login",
            status="debug",
            message="Post-login navigation element detected",
            store_code=store.store_code,
            selector=nav_selector,
        )
    else:
        log_event(
            logger=logger,
            phase="login",
            status="error",
            message="Post-login navigation element not detected after submit; aborting login",
            store_code=store.store_code,
            selector=nav_selector,
            current_url=page.url,
        )
        return False
    if not await _wait_for_home_ready(page=page, store=store, logger=logger, source="login"):
        return False
    return True


async def _session_invalid(*, page: Page, store: UcStore) -> bool:
    current_url = page.url or ""
    login_url = store.login_url or ""
    if login_url and (current_url.startswith(login_url) or login_url in current_url):
        return True

    selectors = {
        "username": UC_LOGIN_SELECTORS["username"],
        "password": UC_LOGIN_SELECTORS["password"],
    }
    markers = [selector for selector in selectors.values() if selector]
    for marker in markers:
        try:
            if await page.locator(marker).count():
                return True
        except Exception:
            continue
    return False


async def _wait_for_gst_report_ready(
    *, page: Page, logger: JsonLogger, store: UcStore
) -> tuple[bool, Locator | None]:
    await page.wait_for_load_state("domcontentloaded")
    try:
        await page.locator(GST_PAGE_LABEL_SELECTOR).first.wait_for(timeout=NAV_TIMEOUT_MS)
    except TimeoutError:
        dom_snippet = await _maybe_get_dom_snippet(page)
        log_event(
            logger=logger,
            phase="navigation",
            status="warn",
            message="GST page label not detected",
            store_code=store.store_code,
            selector=GST_PAGE_LABEL_SELECTOR,
            current_url=page.url,
            **_dom_snippet_fields(dom_snippet),
        )
        return False, None

    readiness_selector = ", ".join(GST_DATE_RANGE_READY_SELECTORS)
    fallback_selector = "text=/Start Date|End Date|Export Report|Apply/i"
    primary_count = 0
    try:
        primary_count = await page.locator(readiness_selector).count()
    except Exception:
        primary_count = 0
    if primary_count:
        container = await _find_gst_report_container(page=page, readiness_selector=readiness_selector)
    else:
        container = await _find_gst_report_container(page=page, readiness_selector=fallback_selector)
        if container is not None:
            readiness_selector = fallback_selector

    if container is None:
        dom_snippet = await _maybe_get_dom_snippet(page)
        log_event(
            logger=logger,
            phase="navigation",
            status="warn",
            message="GST report container not detected",
            store_code=store.store_code,
            selector=readiness_selector,
            fallback_selector=fallback_selector,
            current_url=page.url,
            **_dom_snippet_fields(dom_snippet),
        )
        return False, None

    try:
        await container.locator(readiness_selector).first.wait_for(timeout=NAV_TIMEOUT_MS)
    except TimeoutError:
        dom_snippet = await _maybe_get_dom_snippet(page)
        log_event(
            logger=logger,
            phase="navigation",
            status="warn",
            message="GST report readiness signal missing",
            store_code=store.store_code,
            selector=readiness_selector,
            fallback_selector=fallback_selector,
            current_url=page.url,
            **_dom_snippet_fields(dom_snippet),
        )
        return False, container

    spinner_selector = ", ".join(SPINNER_CSS_SELECTORS)
    if spinner_selector:
        with contextlib.suppress(TimeoutError):
            await page.wait_for_selector(spinner_selector, state="hidden", timeout=NAV_TIMEOUT_MS)

    log_event(
        logger=logger,
        phase="navigation",
        message="GST report readiness signal detected",
        store_code=store.store_code,
    )
    return True, container


async def _try_direct_gst_reports(*, page: Page, store: UcStore, logger: JsonLogger) -> tuple[bool, Locator | None]:
    orders_url = store.orders_url or ""
    try:
        await page.goto(orders_url, wait_until="domcontentloaded")
    except TimeoutError:
        log_event(
            logger=logger,
            phase="navigation",
            status="warn",
            message="Direct GST report navigation timed out",
            store_code=store.store_code,
            orders_url=orders_url,
            current_url=page.url,
        )
        return False, None

    ready, container = await _wait_for_gst_report_ready(page=page, logger=logger, store=store)
    if ready and container is not None:
        return True, container

    on_dashboard = await _is_on_home_dashboard(page=page, store=store)
    log_event(
        logger=logger,
        phase="navigation",
        status="warn",
        message="Direct GST report navigation did not reach report page",
        store_code=store.store_code,
        orders_url=orders_url,
        current_url=page.url,
        on_dashboard=on_dashboard,
    )
    return False, container


async def _assert_home_ready(*, page: Page, store: UcStore, logger: JsonLogger, source: str) -> bool:
    session_invalid = await _session_invalid(page=page, store=store)
    if session_invalid:
        log_event(
            logger=logger,
            phase="navigation",
            status="warn",
            message="Home readiness probe failed; session appears invalid",
            store_code=store.store_code,
            current_url=page.url,
            source=source,
        )
        return False
    return await _wait_for_home_ready(page=page, store=store, logger=logger, source=source)


async def _wait_for_home_ready(
    *, page: Page, store: UcStore, logger: JsonLogger, source: str
) -> bool:
    home_url = store.home_url
    if not home_url:
        log_event(
            logger=logger,
            phase="navigation",
            status="error",
            message="Home URL missing; cannot confirm home readiness",
            store_code=store.store_code,
            source=source,
        )
        return False

    try:
        await page.wait_for_url(lambda url: _url_matches_target(url, home_url), timeout=NAV_TIMEOUT_MS)
    except TimeoutError:
        log_event(
            logger=logger,
            phase="navigation",
            status="warn",
            message="Timed out waiting for home URL",
            store_code=store.store_code,
            home_url=home_url,
            current_url=page.url,
            source=source,
        )

    current_url = page.url or ""
    if _url_is_login(current_url, store):
        dom_snippet = await _maybe_get_dom_snippet(page)
        log_event(
            logger=logger,
            phase="navigation",
            status="error",
            message="Home page not reached; still on login",
            store_code=store.store_code,
            home_url=home_url,
            current_url=current_url,
            source=source,
            **_dom_snippet_fields(dom_snippet),
        )
        return False

    home_selectors = store.home_selectors or list(HOME_READY_SELECTORS)
    per_selector_timeout = max(2_000, NAV_TIMEOUT_MS // max(len(home_selectors), 1))
    for selector in home_selectors:
        try:
            await page.locator(selector).first.wait_for(state="visible", timeout=per_selector_timeout)
        except TimeoutError:
            continue
        except Exception:
            continue
        log_event(
            logger=logger,
            phase="navigation",
            message="Home page ready",
            store_code=store.store_code,
            home_url=home_url,
            selector=selector,
            current_url=current_url,
            source=source,
        )
        return True

    dom_snippet = await _maybe_get_dom_snippet(page)
    log_event(
        logger=logger,
        phase="navigation",
        status="warn",
        message="Home page marker not detected",
        store_code=store.store_code,
        home_url=home_url,
        current_url=current_url,
        selectors=home_selectors,
        **_dom_snippet_fields(dom_snippet),
        source=source,
    )
    return False


async def _apply_date_range(
    *,
    page: Page,
    container: Locator,
    logger: JsonLogger,
    store: UcStore,
    from_date: date,
    to_date: date,
) -> tuple[bool, int, bool, list[dict[str, Any]], str | None]:
    input_selectors = [
        "input.search-user[placeholder='Choose Start Date - End Date']",
        *GST_CONTROL_SELECTORS["date_range_input"],
    ]
    date_input = None
    row_visibility_issue = False
    ui_issues: list[dict[str, Any]] = []
    failure_reason: str | None = None
    for selector in input_selectors:
        try:
            if await container.locator(selector).count():
                date_input = container.locator(selector).first
                break
        except Exception:
            continue
    if date_input is None:
        log_event(
            logger=logger,
            phase="filters",
            status="warn",
            message="Date range input not found; skipping range selection",
            store_code=store.store_code,
            selectors=input_selectors,
        )
        failure_reason = "filter_validation_failed"
        return False, 0, row_visibility_issue, ui_issues, failure_reason

    async def _fallback_set_date_range_input(
        reason: str,
    ) -> tuple[bool, int, bool, list[dict[str, Any]]]:
        date_range_value = f"{from_date:%Y-%m-%d} - {to_date:%Y-%m-%d}"
        try:
            await date_input.evaluate(
                """
                (input, value) => {
                    if (input.hasAttribute("readonly")) {
                        input.removeAttribute("readonly");
                    }
                    input.value = value;
                    input.dispatchEvent(new Event("input", { bubbles: true }));
                    input.dispatchEvent(new Event("change", { bubbles: true }));
                }
                """,
                date_range_value,
            )
        except Exception as exc:
            log_event(
                logger=logger,
                phase="filters",
                status="warn",
                message="Date range fallback failed to update input",
                store_code=store.store_code,
                reason=reason,
                date_range_value=date_range_value,
                error=str(exc),
            )
            return False, 0, row_visibility_issue, ui_issues
        log_event(
            logger=logger,
            phase="filters",
            status="warn",
            message="Date range selection fallback used",
            store_code=store.store_code,
            reason=reason,
            date_range_value=date_range_value,
        )
        refreshed, row_count, refreshed_row_issue = await _wait_for_report_refresh(
            page=page,
            container=container,
            logger=logger,
            store=store,
        )
        return refreshed, row_count, refreshed_row_issue, ui_issues

    with contextlib.suppress(Exception):
        await date_input.scroll_into_view_if_needed()
    popup = None
    for attempt in range(2):
        await date_input.click()
        popup = await _wait_for_date_picker_popup(
            page=page,
            logger=logger,
            store=store,
            input_locator=date_input,
        )
        if popup is not None:
            break
        if attempt == 0:
            log_event(
                logger=logger,
                phase="filters",
                status="warn",
                message="Date picker popup missing; retrying click",
                store_code=store.store_code,
                attempt=attempt + 1,
            )
            await asyncio.sleep(0.3)
    if popup is None:
        log_event(
            logger=logger,
            phase="filters",
            status="warn",
            message="Date picker popup missing; reopening input",
            store_code=store.store_code,
        )
        popup = await _reopen_date_picker_popup(page=page, logger=logger, store=store)
    if popup is None:
        refreshed, row_count, refreshed_row_issue = await _fallback_set_date_range_input("popup-missing")
        if not refreshed:
            failure_reason = "filter_validation_failed"
        return refreshed, row_count, refreshed_row_issue, ui_issues, failure_reason

    calendars = await _get_calendar_locators(popup=popup)
    start_calendar = calendars[0]
    end_calendar = calendars[1] if len(calendars) > 1 else calendars[0]

    try:
        start_ok = await _select_calendar_date(
            calendar=start_calendar,
            target_date=from_date,
            logger=logger,
            store=store,
            label="start",
        )
        end_ok = await _select_calendar_date(
            calendar=end_calendar,
            target_date=to_date,
            logger=logger,
            store=store,
            label="end",
        )
    except Exception as exc:
        log_event(
            logger=logger,
            phase="filters",
            status="warn",
            message="Date range selection failed while navigating calendars",
            store_code=store.store_code,
            start_date=from_date,
            end_date=to_date,
            error=str(exc),
        )
        refreshed, row_count, refreshed_row_issue = await _fallback_set_date_range_input(
            "calendar-navigation-error"
        )
        if not refreshed:
            failure_reason = "filter_validation_failed"
        return refreshed, row_count, refreshed_row_issue, ui_issues, failure_reason
    if not start_ok and end_ok:
        log_event(
            logger=logger,
            phase="filters",
            status="warn",
            message="Start date selection failed; reopening date picker to retry start date",
            store_code=store.store_code,
            start_date=from_date,
        )
        retry_popup = await _reopen_date_picker_popup(page=page, logger=logger, store=store)
        if retry_popup is not None:
            retry_calendars = await _get_calendar_locators(popup=retry_popup)
            retry_start_calendar = retry_calendars[0]
            retry_clicked = False
            try:
                retry_label = await _navigate_calendar_to_month(
                    calendar=retry_start_calendar,
                    target_date=from_date,
                    logger=logger,
                    store=store,
                    label="start",
                )
                if retry_label is not None:
                    retry_clicked = await _click_day_in_calendar(
                        calendar=retry_start_calendar,
                        target_date=from_date,
                        logger=logger,
                        store=store,
                        label="start",
                    )
            except Exception as exc:
                log_event(
                    logger=logger,
                    phase="filters",
                    status="warn",
                    message="Start date retry failed after reopening date picker",
                    store_code=store.store_code,
                    start_date=from_date,
                    error=str(exc),
                )
            if retry_clicked:
                start_ok = True
    if not start_ok or not end_ok:
        log_event(
            logger=logger,
            phase="filters",
            status="warn",
            message="Date range selection incomplete",
            store_code=store.store_code,
            start_date=from_date,
            end_date=to_date,
            start_ok=start_ok,
            end_ok=end_ok,
        )
        refreshed, row_count, refreshed_row_issue = await _fallback_set_date_range_input(
            "calendar-selection-incomplete"
        )
        if not refreshed:
            failure_reason = "filter_validation_failed"
        return refreshed, row_count, refreshed_row_issue, ui_issues, failure_reason

    overlay_selector = ".calendar-body.show"
    try:
        await page.wait_for_selector(overlay_selector, state="visible", timeout=5_000)
    except Exception:
        log_event(
            logger=logger,
            phase="filters",
            status="warn",
            message="Calendar overlay not visible after date selection",
            store_code=store.store_code,
            selector=overlay_selector,
        )

    async def _is_export_button_missing() -> bool:
        for selector in GST_CONTROL_SELECTORS["export_button"]:
            try:
                if await container.locator(selector).count():
                    return False
            except Exception:
                continue
        return True

    apply_section = container.locator(".apply").first
    start_value = ""
    end_value = ""
    apply_section_found = False
    apply_section_visible = False
    for _ in range(3):
        if await apply_section.count():
            apply_section_found = True
            with contextlib.suppress(Exception):
                apply_section_visible = await apply_section.is_visible()
            if apply_section_visible:
                break
        await asyncio.sleep(0.2)
    if apply_section_found and apply_section_visible:
        start_input = apply_section.locator("input[readonly][placeholder='Start Date']").first
        end_input = apply_section.locator("input[readonly][placeholder='End Date']").first
        if await start_input.count() and await end_input.count():
            for _ in range(10):
                with contextlib.suppress(Exception):
                    start_value = await start_input.input_value()
                with contextlib.suppress(Exception):
                    end_value = await end_input.input_value()
                if start_value and end_value:
                    break
                await asyncio.sleep(0.3)
            status = "info" if start_value and end_value else "warn"
            log_event(
                logger=logger,
                phase="filters",
                status=None if status == "info" else "warn",
                message="Apply inputs populated after date selection"
                if status == "info"
                else "Apply inputs missing values after date selection",
                store_code=store.store_code,
                start_value=start_value,
                end_value=end_value,
            )
        else:
            log_event(
                logger=logger,
                phase="filters",
                status="warn" if await _is_export_button_missing() else "info",
                message="Apply inputs not found after date selection",
                store_code=store.store_code,
            )
    else:
        apply_warn = await _is_export_button_missing()
        ui_issues.append(
            {
                "message": "Apply section not found after date selection",
                "apply_warn": apply_warn,
            }
        )

    overlay_apply_container_selector = ".calendar-body.show .apply"
    try:
        await page.wait_for_selector(overlay_apply_container_selector, state="visible", timeout=2_500)
    except TimeoutError:
        overlay_apply_warn = await _is_export_button_missing()
        ui_issues.append(
            {
                "message": "Apply section missing after date selection; reopening date picker",
                "selector": overlay_apply_container_selector,
                "apply_warn": overlay_apply_warn,
            }
        )
        await _reopen_date_picker_popup(page=page, logger=logger, store=store)
        try:
            await page.wait_for_selector(overlay_apply_container_selector, state="visible", timeout=2_500)
            log_event(
                logger=logger,
                phase="filters",
                status="info",
                message="Apply section found after reopening date picker",
                store_code=store.store_code,
                selector=overlay_apply_container_selector,
            )
        except TimeoutError:
            overlay_apply_warn = await _is_export_button_missing()
            ui_issues.append(
                {
                    "message": "Apply section still missing after reopening date picker",
                    "selector": overlay_apply_container_selector,
                    "apply_warn": overlay_apply_warn,
                }
            )
            failure_reason = "apply_button_not_found"
            return False, 0, row_visibility_issue, ui_issues, failure_reason

    applied = False
    for attempt in range(2):
        overlay_count = 0
        apply_count = 0
        with contextlib.suppress(Exception):
            overlay_count = await page.locator(overlay_selector).count()
        with contextlib.suppress(Exception):
            apply_count = await page.locator(f"{overlay_selector} .apply").count()
        log_event(
            logger=logger,
            phase="filters",
            status="debug",
            message="Calendar overlay visibility counts",
            store_code=store.store_code,
            overlay_count=overlay_count,
            apply_count=apply_count,
        )
        overlay_apply_button, overlay_apply_selector_used = await _find_apply_button(
            page=page,
            container=container,
            apply_section=apply_section if apply_section_found else None,
            logger=logger,
            store=store,
        )
        if overlay_apply_button is None:
            if attempt == 0:
                with contextlib.suppress(Exception):
                    await date_input.click()
                popup = await _wait_for_date_picker_popup(
                    page=page,
                    logger=logger,
                    store=store,
                    input_locator=date_input,
                )
                if popup is not None:
                    with contextlib.suppress(Exception):
                        await page.wait_for_selector(overlay_selector, state="visible", timeout=5_000)
                continue
            log_event(
                logger=logger,
                phase="filters",
                status="warn",
                message="Apply control not found after date selection; skipping export",
                store_code=store.store_code,
                selectors=GST_CONTROL_SELECTORS["apply_button"],
                reason="apply_control_missing",
            )
            ui_issues.append(
                {
                    "message": "Apply control missing after date selection",
                    "reason": "apply_button_not_found",
                    "selectors": GST_CONTROL_SELECTORS["apply_button"],
                }
            )
            failure_reason = "apply_button_not_found"
            return False, 0, row_visibility_issue, ui_issues, failure_reason

        with contextlib.suppress(Exception):
            await overlay_apply_button.scroll_into_view_if_needed()
        try:
            await overlay_apply_button.wait_for(state="visible", timeout=5_000)
        except Exception:
            log_event(
                logger=logger,
                phase="filters",
                status="warn",
                message="Apply button not ready after date selection",
                store_code=store.store_code,
                selector=overlay_apply_selector_used,
            )
            failure_reason = "apply_button_not_found"
            return False, 0, row_visibility_issue, ui_issues, failure_reason

        apply_enabled = False
        for _ in range(10):
            with contextlib.suppress(Exception):
                apply_enabled = await overlay_apply_button.is_enabled()
            if apply_enabled:
                break
            await asyncio.sleep(0.2)
        if not apply_enabled:
            log_event(
                logger=logger,
                phase="filters",
                status="warn",
                message="Apply button not enabled after date selection",
                store_code=store.store_code,
                selector=overlay_apply_selector_used,
            )
            failure_reason = "apply_button_not_found"
            return False, 0, row_visibility_issue, ui_issues, failure_reason

        await overlay_apply_button.click()
        date_filter_updated = await _confirm_date_filter_update(
            page=page,
            container=container,
            apply_section=apply_section if apply_section_found else None,
            from_date=from_date,
            to_date=to_date,
            logger=logger,
            store=store,
        )
        if not date_filter_updated:
            if attempt == 0:
                log_event(
                    logger=logger,
                    phase="filters",
                    status="warn",
                    message="GST banner values remained zero after apply click; retrying apply",
                    store_code=store.store_code,
                )
                continue
            failure_reason = "apply_click_unconfirmed"
            return False, 0, row_visibility_issue, ui_issues, failure_reason

        refreshed, row_count, refreshed_row_issue = await _wait_for_report_refresh(
            page=page,
            container=container,
            logger=logger,
            store=store,
        )
        row_visibility_issue = row_visibility_issue or refreshed_row_issue
        overlay_open = False
        with contextlib.suppress(Exception):
            overlay_open = await page.locator(overlay_selector).first.is_visible()
        log_event(
            logger=logger,
            phase="filters",
            message="GST report row count after apply",
            store_code=store.store_code,
            row_count=row_count,
            refreshed=refreshed,
            overlay_open=overlay_open,
        )
        if row_count > 0:
            applied = True
            break
        if overlay_open and attempt == 0:
            log_event(
                logger=logger,
                phase="filters",
                status="warn",
                message="GST report rows not detected and overlay still open; retrying apply",
                store_code=store.store_code,
                row_count=row_count,
            )
            continue
        if not overlay_open:
            applied = True
            break
        log_event(
            logger=logger,
            phase="filters",
            status="warn",
            message="GST report rows missing and overlay still open after apply",
            store_code=store.store_code,
            row_count=row_count,
        )
        failure_reason = "row_count_zero_after_apply"
        return False, row_count, row_visibility_issue, ui_issues, failure_reason

    if not applied:
        failure_reason = failure_reason or "filter_validation_failed"
        return False, 0, row_visibility_issue, ui_issues, failure_reason
    if apply_section_found:
        await _confirm_apply_dates(
            apply_section=apply_section,
            from_date=from_date,
            to_date=to_date,
            logger=logger,
            store=store,
        )
    return True, row_count, row_visibility_issue, ui_issues, failure_reason


async def _download_gst_report(
    *,
    page: Page,
    logger: JsonLogger,
    store: UcStore,
    from_date: date,
    to_date: date,
    download_dir: Path,
    download_timeout_ms: int,
    row_count: int,
    max_attempts: int = 3,
) -> tuple[bool, str | None, str]:
    filename = _format_gst_filename(store.store_code, from_date, to_date)
    target_path = (download_dir / filename).resolve()

    export_locator = page.locator("button.btn.primary.export")
    export_selector_used = "button.btn.primary.export"
    await page.wait_for_timeout(500)
    export_count = await export_locator.count()
    if export_count == 0:
        export_locator = page.get_by_role("button", name="Export Report")
        export_selector_used = "button[role=button][name='Export Report']"
        export_count = await export_locator.count()
    if export_count == 0:
        message = "Export Report button not found; skipping download"
        log_event(
            logger=logger,
            phase="download",
            status="warn",
            message=message,
            store_code=store.store_code,
            selectors=GST_CONTROL_SELECTORS["export_button"],
            export_count=export_count,
        )
        return False, None, message

    disabled_retry_count = 0
    max_disabled_retries = 3
    disabled_retry_delay_s = 1
    for attempt in range(1, max_attempts + 1):
        export_button = export_locator.first
        export_count = await export_locator.count()
        is_visible = False
        if export_count > 0:
            with contextlib.suppress(Exception):
                is_visible = await export_button.is_visible()
        log_event(
            logger=logger,
            phase="download",
            message="Checked Export Report button visibility",
            store_code=store.store_code,
            export_selector=export_selector_used,
            export_count=export_count,
            export_is_visible=is_visible,
            attempt=attempt,
        )
        if export_count > 0 and not is_visible:
            with contextlib.suppress(Exception):
                await export_button.scroll_into_view_if_needed()
            await asyncio.sleep(0.5)
            continue

        is_enabled = True
        with contextlib.suppress(Exception):
            is_enabled = await export_button.is_enabled()
        if not is_enabled:
            if disabled_retry_count < max_disabled_retries:
                disabled_retry_count += 1
                log_event(
                    logger=logger,
                    phase="download",
                    status="warn",
                    message="Export Report button disabled; waiting before retry",
                    store_code=store.store_code,
                    attempt=attempt,
                    max_attempts=max_attempts,
                    disabled_retry=disabled_retry_count,
                    max_disabled_retries=max_disabled_retries,
                )
                with contextlib.suppress(Exception):
                    await export_button.wait_for(state="enabled", timeout=10_000)
                await asyncio.sleep(disabled_retry_delay_s)
                continue
            log_event(
                logger=logger,
                phase="download",
                status="warn",
                message="Export Report button disabled after retry; giving up",
                store_code=store.store_code,
                attempt=attempt,
                max_attempts=max_attempts,
                disabled_retry=disabled_retry_count,
                max_disabled_retries=max_disabled_retries,
            )
            await asyncio.sleep(disabled_retry_delay_s)
            return False, None, "Export Report button disabled after retry"

        try:
            async with page.expect_download(timeout=download_timeout_ms) as download_info:
                log_event(
                    logger=logger,
                    phase="download",
                    message="Clicking Export Report button",
                    store_code=store.store_code,
                    export_selector=export_selector_used,
                    row_count=row_count,
                    attempt=attempt,
                )
                await export_button.click()
                log_event(
                    logger=logger,
                    phase="download",
                    message="Export Report button click completed",
                    store_code=store.store_code,
                    export_selector=export_selector_used,
                    row_count=row_count,
                    attempt=attempt,
                    click_outcome="success",
                )
            download = await download_info.value
            with contextlib.suppress(Exception):
                await download.path()
            await download.save_as(str(target_path))
            file_size = await _wait_for_non_empty_file(
                target_path=target_path,
                timeout_ms=download_timeout_ms,
            )
            if file_size <= 0:
                message = "GST report download saved but file empty"
                log_event(
                    logger=logger,
                    phase="download",
                    status="warn",
                    message=message,
                    store_code=store.store_code,
                    download_path=str(target_path),
                    export_selector=export_selector_used,
                    row_count=row_count,
                    attempt=attempt,
                    file_size=file_size,
                )
                return False, None, message
            log_event(
                logger=logger,
                phase="download",
                message="GST report download saved",
                store_code=store.store_code,
                download_path=str(target_path),
                suggested_filename=download.suggested_filename,
                export_selector=export_selector_used,
                row_count=row_count,
                attempt=attempt,
                file_size=file_size,
            )
            return True, str(target_path), "GST report download saved"
        except TimeoutError:
            log_event(
                logger=logger,
                phase="download",
                status="warn",
                message="GST report download did not start before timeout; retrying",
                store_code=store.store_code,
                attempt=attempt,
                max_attempts=max_attempts,
                timeout_ms=download_timeout_ms,
                click_outcome="timeout",
            )
        except Exception as exc:
            log_event(
                logger=logger,
                phase="download",
                status="warn",
                message="GST report download attempt failed",
                store_code=store.store_code,
                attempt=attempt,
                max_attempts=max_attempts,
                error=str(exc),
                click_outcome="error",
            )
        await asyncio.sleep(1)

    message = "GST report download failed after retries"
    log_event(
        logger=logger,
        phase="download",
        status="error",
        message=message,
        store_code=store.store_code,
        download_path=str(target_path),
    )
    return False, None, message


async def _wait_for_non_empty_file(*, target_path: Path, timeout_ms: int, poll_s: float = 0.5) -> int:
    deadline = asyncio.get_running_loop().time() + (timeout_ms / 1000)
    while asyncio.get_running_loop().time() < deadline:
        if target_path.exists():
            with contextlib.suppress(OSError):
                size = target_path.stat().st_size
                if size > 0:
                    return size
        await asyncio.sleep(poll_s)
    if target_path.exists():
        with contextlib.suppress(OSError):
            return target_path.stat().st_size
    return 0


async def _reopen_date_picker_popup(
    *, page: Page, logger: JsonLogger, store: UcStore
) -> Locator | None:
    reopened = False
    input_locator: Locator | None = None
    for selector in GST_CONTROL_SELECTORS["date_range_input"]:
        input_locator = page.locator(selector).first
        try:
            if await input_locator.count():
                await input_locator.click()
                reopened = True
                log_event(
                    logger=logger,
                    phase="filters",
                    status="warn",
                    message="Reopened date picker input to restore popup",
                    store_code=store.store_code,
                    selector=selector,
                )
                break
        except Exception:
            continue
    if not reopened:
        log_event(
            logger=logger,
            phase="filters",
            status="warn",
            message="Unable to reopen date picker input",
            store_code=store.store_code,
        )
        return None
    popup = await _wait_for_date_picker_popup(
        page=page,
        logger=logger,
        store=store,
        input_locator=input_locator,
    )
    if popup is None:
        log_event(
            logger=logger,
            phase="filters",
            status="warn",
            message="Date picker popup still missing after reopen",
            store_code=store.store_code,
        )
    return popup


async def _wait_for_date_picker_popup(
    *, page: Page, logger: JsonLogger, store: UcStore, input_locator: Locator | None = None
) -> Locator | None:
    fallback_selectors = ("mat-calendar", "[class*='mat-calendar']", "[role='dialog']")
    for attempt in range(3):
        popup_selectors = list(
            dict.fromkeys(
                (
                    *DATE_PICKER_POPUP_SELECTORS,
                    *fallback_selectors,
                    *(DATE_PICKER_POPUP_FALLBACK_SELECTORS if attempt > 0 else ()),
                )
            )
        )
        popup_selector = ", ".join(popup_selectors)
        if attempt > 0 and input_locator is not None:
            with contextlib.suppress(Exception):
                await input_locator.click()
            await asyncio.sleep(0.4)
        detected_selectors: dict[str, int] = {}
        for selector in popup_selectors:
            try:
                count = await page.locator(selector).count()
            except Exception:
                continue
            if count:
                detected_selectors[selector] = count
        log_event(
            logger=logger,
            phase="filters",
            status="debug",
            message="Date picker popup selector detection results",
            store_code=store.store_code,
            attempt=attempt + 1,
            detected_selectors=detected_selectors or None,
        )
        popup = page.locator(popup_selector).first
        try:
            await popup.wait_for(state="visible", timeout=2_000)
            return popup
        except TimeoutError:
            if attempt < 2:
                continue
        log_event(
            logger=logger,
            phase="filters",
            status="warn",
            message="Date picker popup not visible after clicking input",
            store_code=store.store_code,
            selector=popup_selector,
            detected_selectors=detected_selectors or None,
            attempts=attempt + 1,
        )
    return None


async def _find_apply_button(
    *,
    page: Page,
    container: Locator,
    apply_section: Locator | None,
    logger: JsonLogger,
    store: UcStore,
) -> tuple[Locator | None, str | None]:
    cue_regex = re.compile("|".join(re.escape(cue) for cue in CONTROL_CUES["apply"]), re.I)
    search_scopes: list[tuple[str, Locator]] = []

    overlay_apply = page.locator(".calendar-body.show .apply").first
    if await overlay_apply.count():
        search_scopes.append(("overlay_apply", overlay_apply))

    if apply_section is not None and await apply_section.count():
        search_scopes.append(("apply_section", apply_section))

    apply_container = container.locator(".apply").first
    if await apply_container.count():
        search_scopes.append(("container_apply", apply_container))

    for scope_name, scope in search_scopes:
        for child_selector in (".buttons", ""):
            scope_locator = scope.locator(child_selector) if child_selector else scope
            try:
                button_candidate = scope_locator.get_by_role("button", name=cue_regex).first
                if await button_candidate.count():
                    return button_candidate, f"{scope_name} >> role=button"
            except Exception:
                pass
            try:
                text_button = scope_locator.locator("button").filter(has_text=cue_regex).first
                if await text_button.count():
                    return text_button, f"{scope_name} >> button text"
            except Exception:
                pass
            try:
                input_buttons = scope_locator.locator("input[type='button'], input[type='submit']")
                input_count = await input_buttons.count()
                for idx in range(input_count):
                    candidate = input_buttons.nth(idx)
                    value = await candidate.get_attribute("value")
                    if value and cue_regex.search(value):
                        return candidate, f"{scope_name} >> input[value='{value}']"
            except Exception:
                pass

    for selector in GST_CONTROL_SELECTORS["apply_button"]:
        candidate = page.locator(selector)
        try:
            if await candidate.count():
                return candidate.first, selector
        except Exception:
            continue

    try:
        role_candidate = page.get_by_role("button", name=cue_regex).first
        if await role_candidate.count():
            return role_candidate, "role=button[name~apply]"
    except Exception:
        pass

    log_event(
        logger=logger,
        phase="filters",
        status="warn",
        message="Apply button not located using expanded cues",
        store_code=store.store_code,
        cues=CONTROL_CUES["apply"],
    )
    return None, None


async def _get_calendar_locators(*, popup: Locator) -> list[Locator]:
    calendars: list[Locator] = []
    for selector in (".start-date", ".end-date"):
        locator = popup.locator(selector)
        count = await locator.count()
        for idx in range(count):
            calendars.append(locator.nth(idx))
    if calendars:
        return calendars

    for selector in (".calendar-body.show", ".calendar-body .calendar"):
        locator = popup.locator(selector)
        count = await locator.count()
        for idx in range(count):
            calendars.append(locator.nth(idx))

    return calendars or [popup]


async def _select_calendar_date(
    *,
    calendar: Locator,
    target_date: date,
    logger: JsonLogger,
    store: UcStore,
    label: str,
) -> bool:
    target_label = target_date.strftime("%b %Y").upper()
    current_label = await _navigate_calendar_to_month(
        calendar=calendar,
        target_date=target_date,
        logger=logger,
        store=store,
        label=label,
    )
    if current_label is None:
        return False

    clicked = await _click_day_in_calendar(
        calendar=calendar,
        target_date=target_date,
        logger=logger,
        store=store,
        label=label,
    )
    if not clicked:
        log_event(
            logger=logger,
            phase="filters",
            status="warn",
            message="Target day not found in calendar; retrying selection",
            store_code=store.store_code,
            calendar_label=label,
            target_day=target_date.day,
            target_month=target_label,
        )
        retry_calendar = await _reopen_calendar_for_retry(
            page=calendar.page, logger=logger, store=store, label=label
        )
        if retry_calendar is not None:
            log_event(
                logger=logger,
                phase="filters",
                status="warn",
                message="Reopened date picker; retrying calendar selection",
                store_code=store.store_code,
                calendar_label=label,
            )
            current_label = await _navigate_calendar_to_month(
                calendar=retry_calendar,
                target_date=target_date,
                logger=logger,
                store=store,
                label=label,
            )
            if current_label is not None:
                clicked = await _click_day_in_calendar(
                    calendar=retry_calendar,
                    target_date=target_date,
                    logger=logger,
                    store=store,
                    label=label,
                )
        if not clicked:
            log_event(
                logger=logger,
                phase="filters",
                status="warn",
                message="Attempting typed date fallback after missing target day",
                store_code=store.store_code,
                calendar_label=label,
                target_date=target_date.isoformat(),
            )
            typed = await _try_fill_date_input(
                page=calendar.page,
                target_date=target_date,
                logger=logger,
                store=store,
                label=label,
            )
            if typed:
                return True
            log_event(
                logger=logger,
                phase="filters",
                status="warn",
                message="Attempting closest available day fallback",
                store_code=store.store_code,
                calendar_label=label,
                target_date=target_date.isoformat(),
            )
            fallback_calendar = retry_calendar or calendar
            closest_date = await _select_closest_available_day(
                calendar=fallback_calendar,
                target_date=target_date,
                logger=logger,
                store=store,
                label=label,
            )
            if closest_date is not None:
                return True
            log_event(
                logger=logger,
                phase="filters",
                status="warn",
                message="Target day not found after retry and fallbacks",
                store_code=store.store_code,
                calendar_label=label,
                target_day=target_date.day,
                target_month=target_label,
            )
            return False
    log_event(
        logger=logger,
        phase="filters",
        message="Selected calendar date",
        store_code=store.store_code,
        calendar_label=label,
        selected_date=target_date.isoformat(),
        displayed_month=current_label,
    )
    return True


async def _navigate_calendar_to_month(
    *,
    calendar: Locator,
    target_date: date,
    logger: JsonLogger,
    store: UcStore,
    label: str,
) -> str | None:
    target_key = (target_date.year, target_date.month)
    clicks_prev = 0
    clicks_next = 0
    max_steps = 48
    current_label = None
    reached = False
    for _ in range(max_steps):
        header_text = await _get_calendar_header_text(calendar=calendar, label=label)
        if not header_text:
            log_event(
                logger=logger,
                phase="filters",
                status="warn",
                message="Calendar header missing; retrying with fallback selectors",
                store_code=store.store_code,
                calendar_label=label,
            )
            header_text = await _get_calendar_header_text(
                calendar=calendar, label=label, allow_fallback=True
            )
        parsed = _parse_month_year(header_text or "")
        if not parsed:
            log_event(
                logger=logger,
                phase="filters",
                status="warn",
                message="Calendar header parsing failed; retrying lookup",
                store_code=store.store_code,
                calendar_label=label,
                header_text=header_text,
            )
            header_text = await _get_calendar_header_text(
                calendar=calendar, label=label, allow_fallback=True
            )
            parsed = _parse_month_year(header_text or "")
        if not parsed:
            log_event(
                logger=logger,
                phase="filters",
                status="warn",
                message="Unable to parse calendar header",
                store_code=store.store_code,
                calendar_label=label,
                header_text=header_text,
            )
            return None
        current_key = (parsed[0], parsed[1])
        current_label = header_text
        if current_key == target_key:
            reached = True
            break
        if current_key < target_key:
            await _click_calendar_nav(calendar=calendar, direction="next")
            clicks_next += 1
        else:
            await _click_calendar_nav(calendar=calendar, direction="prev")
            clicks_prev += 1
        await asyncio.sleep(0.2)
    if not reached:
        log_event(
            logger=logger,
            phase="filters",
            status="warn",
            message="Calendar navigation exceeded maximum steps",
            store_code=store.store_code,
            calendar_label=label,
            target_month=target_date.strftime("%b %Y").upper(),
            displayed_month=current_label,
            prev_clicks=clicks_prev,
            next_clicks=clicks_next,
        )
        return None
    log_event(
        logger=logger,
        phase="filters",
        message="Navigated calendar month",
        store_code=store.store_code,
        calendar_label=label,
        target_month=target_date.strftime("%b %Y").upper(),
        displayed_month=current_label,
        prev_clicks=clicks_prev,
        next_clicks=clicks_next,
    )
    return current_label


async def _reopen_calendar_for_retry(
    *, page: Page, logger: JsonLogger, store: UcStore, label: str
) -> Locator | None:
    reopened = False
    input_locator: Locator | None = None
    for selector in GST_CONTROL_SELECTORS["date_range_input"]:
        input_locator = page.locator(selector).first
        try:
            if await input_locator.count():
                await input_locator.click()
                reopened = True
                break
        except Exception:
            continue
    if not reopened:
        log_event(
            logger=logger,
            phase="filters",
            status="warn",
            message="Unable to reopen date picker for retry",
            store_code=store.store_code,
            calendar_label=label,
        )
        return None
    popup = await _wait_for_date_picker_popup(
        page=page,
        logger=logger,
        store=store,
        input_locator=input_locator,
    )
    if popup is None:
        return None
    calendars = await _get_calendar_locators(popup=popup)
    if not calendars:
        return None
    if label == "start":
        return calendars[0]
    return calendars[1] if len(calendars) > 1 else calendars[0]


async def _try_fill_date_input(
    *,
    page: Page,
    target_date: date,
    logger: JsonLogger,
    store: UcStore,
    label: str,
) -> bool:
    selectors: list[str] = []
    if label == "start":
        selectors = [
            "input[placeholder*='Start'][type='text']:not([readonly])",
            "input[placeholder*='From'][type='text']:not([readonly])",
            "input[name*='start']:not([readonly])",
            "input[name*='from']:not([readonly])",
        ]
    else:
        selectors = [
            "input[placeholder*='End'][type='text']:not([readonly])",
            "input[placeholder*='To'][type='text']:not([readonly])",
            "input[name*='end']:not([readonly])",
            "input[name*='to']:not([readonly])",
        ]
    date_value = target_date.strftime("%Y-%m-%d")
    for selector in selectors:
        input_locator = page.locator(selector).first
        try:
            if await input_locator.count():
                readonly = await input_locator.get_attribute("readonly")
                disabled = await input_locator.get_attribute("disabled")
                if readonly is None and disabled is None:
                    await input_locator.fill(date_value)
                    log_event(
                        logger=logger,
                        phase="filters",
                        status="warn",
                        message="Fallback to typed date input",
                        store_code=store.store_code,
                        calendar_label=label,
                        selector=selector,
                        typed_value=date_value,
                    )
                    return True
        except Exception:
            continue
    return False


def _select_fallback_date(target_date: date, candidates: list[date], label: str) -> date | None:
    if not candidates:
        return None
    before = sorted([candidate for candidate in candidates if candidate <= target_date])
    after = sorted([candidate for candidate in candidates if candidate >= target_date])
    if label == "start":
        if after:
            return after[0]
        return before[-1] if before else None
    if before:
        return before[-1]
    return after[0] if after else None


async def _select_closest_available_day(
    *,
    calendar: Locator,
    target_date: date,
    logger: JsonLogger,
    store: UcStore,
    label: str,
) -> date | None:
    day_cells = calendar.locator("td[aria-label]")
    try:
        total = await day_cells.count()
    except Exception:
        total = 0
    candidates: list[tuple[date, Locator]] = []
    for idx in range(total):
        cell = day_cells.nth(idx)
        try:
            aria_label = await cell.get_attribute("aria-label")
            if not aria_label:
                continue
            parsed = _parse_aria_date_label(aria_label)
            if parsed is None:
                continue
            if parsed.year != target_date.year or parsed.month != target_date.month:
                continue
            aria_disabled = (await cell.get_attribute("aria-disabled")) or ""
            class_name = (await cell.get_attribute("class")) or ""
            if aria_disabled.lower() == "true":
                continue
            if any(token in class_name.lower() for token in ("disabled", "off")):
                continue
            candidates.append((parsed, cell))
        except Exception:
            continue
    fallback_date = _select_fallback_date(target_date, [candidate[0] for candidate in candidates], label)
    if fallback_date is None:
        log_event(
            logger=logger,
            phase="filters",
            status="warn",
            message="No fallback calendar days available",
            store_code=store.store_code,
            calendar_label=label,
            target_date=target_date.isoformat(),
        )
        return None
    for candidate_date, candidate_cell in candidates:
        if candidate_date == fallback_date:
            try:
                await candidate_cell.scroll_into_view_if_needed()
                await candidate_cell.click()
            except Exception:
                pass
            clicked = await _click_day_in_calendar(
                calendar=calendar,
                target_date=fallback_date,
                logger=logger,
                store=store,
                label=label,
            )
            if clicked:
                log_event(
                    logger=logger,
                    phase="filters",
                    status="warn",
                    message="Fallback to closest available calendar day",
                    store_code=store.store_code,
                    calendar_label=label,
                    target_date=target_date.isoformat(),
                    fallback_date=fallback_date.isoformat(),
                )
                return fallback_date
            break
    return None


def _parse_aria_date_label(aria_label: str) -> date | None:
    match = re.search(r"([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})", aria_label)
    if not match:
        return None
    month_text = match.group(1).upper()
    month = MONTH_LOOKUP.get(month_text) or MONTH_LOOKUP.get(month_text[:3])
    if not month:
        return None
    try:
        return date(int(match.group(3)), month, int(match.group(2)))
    except ValueError:
        return None


async def _get_calendar_header_text(
    *, calendar: Locator, label: str, allow_fallback: bool = False
) -> str | None:
    id_selector = None
    if label == "start":
        id_selector = "#mat-calendar-button-0"
    elif label == "end":
        id_selector = "#mat-calendar-button-1"

    selectors = [
        selector
        for selector in (
            id_selector,
            ".month",
            ".month-name",
            ".datepicker-switch",
            ".flatpickr-current-month",
            ".react-datepicker__current-month",
            ".mat-calendar-period-button",
        )
        if selector
    ]
    if allow_fallback:
        selectors.extend(
            [
                ".calendar-header",
                ".calendar-title",
                ".mat-calendar-header",
                ".react-datepicker__header",
                ".flatpickr-month",
                ".flatpickr-current-month .cur-month",
                ".pika-label",
            ]
        )
    for selector in selectors:
        locator = calendar.locator(selector).first
        try:
            if await locator.count():
                text = (await locator.inner_text()).strip()
                if text:
                    return text
        except Exception:
            continue
    if allow_fallback:
        try:
            text = await calendar.evaluate(
                """(node) => {
                    const candidates = node.querySelectorAll('*');
                    for (const el of candidates) {
                        const value = (el.textContent || '').trim();
                        if (/^[A-Za-z]{3,9}\\s+\\d{4}$/.test(value)) {
                            return value;
                        }
                    }
                    return null;
                }"""
            )
            if text:
                return str(text).strip()
        except Exception:
            return None
    return None


def _parse_month_year(label: str) -> tuple[int, int] | None:
    if not label:
        return None
    normalized = re.sub(r"\s+", " ", label).strip().upper()
    match = re.search(r"([A-Z]{3,9})\s+(\d{4})", normalized)
    if not match:
        return None
    month_text = match.group(1).upper()
    month = MONTH_LOOKUP.get(month_text) or MONTH_LOOKUP.get(month_text[:3])
    if not month:
        return None
    return int(match.group(2)), month


async def _click_calendar_nav(*, calendar: Locator, direction: str) -> None:
    if direction == "prev":
        selectors = [".mat-calendar-previous-button"]
    else:
        selectors = [".mat-calendar-next-button"]
    for selector in selectors:
        locator = calendar.locator(selector).first
        try:
            if await locator.count():
                await locator.click()
                return
        except Exception:
            continue
    raise TimeoutError(f"Calendar navigation button not found for direction={direction}")


async def _click_day_in_calendar(
    *,
    calendar: Locator,
    target_date: date,
    logger: JsonLogger,
    store: UcStore,
    label: str,
) -> bool:
    async def _is_disabled(cell: Locator) -> bool:
        try:
            aria_disabled = (await cell.get_attribute("aria-disabled")) or ""
            if aria_disabled.lower() == "true":
                return True
            class_name = (await cell.get_attribute("class")) or ""
            return "disabled" in class_name.lower()
        except Exception:
            return False

    async def _count_candidates(candidates: list[Locator]) -> int:
        total = 0
        for candidate in candidates:
            try:
                total += await candidate.count()
            except Exception:
                continue
        return total

    async def _first_enabled_day_cell(candidates: list[Locator]) -> Locator | None:
        for candidate in candidates:
            try:
                count = await candidate.count()
            except Exception:
                continue
            for index in range(count):
                cell = candidate.nth(index)
                if not await _is_disabled(cell):
                    return cell
        return None

    month_name = target_date.strftime("%B")
    month_name_abbrev = target_date.strftime("%b")
    day_label = str(target_date.day)
    day_label_padded = target_date.strftime("%d")
    aria_labels = [
        f"{month_name} {day_label_padded}, {target_date.year}",
        f"{month_name} {day_label}, {target_date.year}",
        f"{month_name_abbrev} {day_label_padded}, {target_date.year}",
        f"{month_name_abbrev} {day_label}, {target_date.year}",
        f"{month_name} {day_label_padded} {target_date.year}",
        f"{month_name} {day_label} {target_date.year}",
        f"{month_name_abbrev} {day_label_padded} {target_date.year}",
        f"{month_name_abbrev} {day_label} {target_date.year}",
        f"{day_label_padded} {month_name} {target_date.year}",
        f"{day_label} {month_name} {target_date.year}",
        f"{day_label_padded} {month_name_abbrev} {target_date.year}",
        f"{day_label} {month_name_abbrev} {target_date.year}",
    ]
    partial_aria_label = f"{month_name} {day_label}"
    partial_aria_label_with_year = f"{month_name} {day_label}, {target_date.year}"
    aria_label_unpadded = f"{month_name} {day_label}, {target_date.year}"
    day_cell_text_regex = re.compile(rf"^0?{day_label}$")
    scope = calendar
    day_cell_candidates: list[Locator] = []
    if label == "end":
        match_count = 0
        end_container_selector = ".calendar-body.show .end-date"
        for attempt in range(2):
            end_container = calendar.page.locator(end_container_selector).first
            scope = end_container
            mat_body = scope.locator("mat-calendar-body").first
            try:
                if await mat_body.count():
                    scope = mat_body
            except Exception:
                pass
            day_cell_candidates = [
                scope.locator(f"[aria-label='{label_value}']")
                for label_value in aria_labels
            ]
            day_cell_candidates.extend(
                [
                    scope.locator(f"td[aria-label*='{partial_aria_label_with_year}']"),
                    scope.locator(f"[aria-label*='{partial_aria_label}']"),
                    scope.locator(
                        "[role='gridcell']",
                        has_text=day_cell_text_regex,
                    ),
                    scope.get_by_role("gridcell", name=day_label),
                    scope.get_by_role("gridcell", name=day_label_padded),
                    scope.locator(
                        "button",
                        has_text=day_cell_text_regex,
                    ),
                    scope.locator(
                        "td",
                        has_text=day_cell_text_regex,
                    ),
                ]
            )
            match_count = await _count_candidates(day_cell_candidates)
            if match_count:
                break
            january_count = 0
            try:
                january_count = await end_container.locator(
                    f"td[aria-label*='{month_name}']"
                ).count()
            except Exception:
                january_count = 0
            log_event(
                logger=logger,
                phase="filters",
                status="debug",
                message="End-date calendar month cell count before retry",
                store_code=store.store_code,
                month_name=month_name,
                month_cell_count=january_count,
            )
            if january_count == 0 and attempt == 0:
                reopened = False
                for selector in GST_CONTROL_SELECTORS["date_range_input"]:
                    input_locator = calendar.page.locator(selector).first
                    try:
                        if await input_locator.count():
                            await input_locator.click()
                            reopened = True
                            break
                    except Exception:
                        continue
                if reopened:
                    await _wait_for_date_picker_popup(
                        page=calendar.page,
                        logger=logger,
                        store=store,
                        input_locator=input_locator,
                    )
                    continue
            break
    else:
        body_locator = calendar.locator(".calendar")
        try:
            scope = body_locator.first if await body_locator.count() else calendar
        except Exception:
            scope = calendar
        day_cell_candidates = [
            scope.locator(f"[aria-label='{label_value}']") for label_value in aria_labels
        ]
        day_cell_candidates.extend(
            [
                scope.locator(f"td[aria-label*='{partial_aria_label_with_year}']"),
                scope.locator(f"[aria-label*='{partial_aria_label}']"),
                scope.locator(
                    "[role='gridcell']",
                    has_text=day_cell_text_regex,
                ),
                scope.get_by_role("gridcell", name=day_label),
                scope.get_by_role("gridcell", name=day_label_padded),
                scope.locator(
                    "button",
                    has_text=day_cell_text_regex,
                ),
                scope.locator(
                    "td",
                    has_text=day_cell_text_regex,
                ),
            ]
        )

    day_cell = await _first_enabled_day_cell(day_cell_candidates)
    try:
        if not day_cell:
            return False
        await day_cell.scroll_into_view_if_needed()
        await day_cell.click()
        button_locator = day_cell.locator("[aria-selected]").first
        for _ in range(5):
            aria_selected = (await day_cell.get_attribute("aria-selected")) or ""
            if aria_selected == "true":
                if label == "end":
                    log_event(
                        logger=logger,
                        phase="filters",
                        status="debug",
                        message="End-date calendar day cell selected",
                        store_code=store.store_code,
                        aria_selected=aria_selected,
                    )
                return True
            if await button_locator.count():
                button_selected = (await button_locator.get_attribute("aria-selected")) or ""
                if button_selected == "true":
                    if label == "end":
                        log_event(
                            logger=logger,
                            phase="filters",
                            status="debug",
                            message="End-date calendar day cell selected",
                            store_code=store.store_code,
                            aria_selected=button_selected,
                        )
                    return True
            if label == "end":
                end_container = calendar.page.locator(
                    ".calendar-body.show .end-date"
                ).first
                selected_cell = end_container.locator(
                    f"td[aria-label='{aria_label_unpadded}'][aria-selected='true']"
                )
                try:
                    if await selected_cell.count():
                        log_event(
                            logger=logger,
                            phase="filters",
                            status="debug",
                            message="End-date calendar day cell selected",
                            store_code=store.store_code,
                            aria_selected="true",
                        )
                        return True
                except Exception:
                    pass
            await asyncio.sleep(0.1)
    except Exception:
        return False
    return False


def _matches_date_value(value: str, target_date: date) -> bool:
    if not value:
        return False
    normalized = value.strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y", "%b %d, %Y", "%B %d, %Y"):
        if target_date.strftime(fmt) in normalized:
            return True
    return False


def _matches_date_range_value(value: str, from_date: date, to_date: date) -> bool:
    if not value:
        return False
    return _matches_date_value(value, from_date) and _matches_date_value(value, to_date)


async def _confirm_date_filter_update(
    *,
    page: Page,
    container: Locator,
    apply_section: Locator | None,
    from_date: date,
    to_date: date,
    logger: JsonLogger,
    store: UcStore,
) -> bool:
    banner_values: list[str] = []
    banner_count = 0
    row_count = 0
    row_locator = container.locator(", ".join(GST_REPORT_ROW_SELECTORS))
    timeout_s = 10.0
    start = asyncio.get_event_loop().time()
    while (asyncio.get_event_loop().time() - start) < timeout_s:
        banner_values = []
        banner_locator = container.locator(".gst-banner .gst-banner-card .value")
        with contextlib.suppress(Exception):
            banner_count = await banner_locator.count()
        for index in range(banner_count):
            with contextlib.suppress(Exception):
                value_text = await banner_locator.nth(index).inner_text()
                banner_values.append(value_text.strip())

        with contextlib.suppress(Exception):
            row_count = await row_locator.count()
        if row_count > 0:
            log_event(
                logger=logger,
                phase="filters",
                message="GST report rows detected after apply click",
                store_code=store.store_code,
                row_count=row_count,
            )
            return True
        if banner_count >= 4 and any(value != "₹0" for value in banner_values):
            log_event(
                logger=logger,
                phase="filters",
                message="GST banner values updated after apply click",
                store_code=store.store_code,
                banner_values=banner_values,
                banner_count=banner_count,
            )
            return True
        await asyncio.sleep(0.3)

    log_event(
        logger=logger,
        phase="filters",
        status="warn",
        message="GST banner values remained zero and no rows after apply click",
        store_code=store.store_code,
        banner_values=banner_values,
        banner_count=banner_count,
        row_count=row_count,
    )
    return False


async def _confirm_apply_dates(
    *,
    apply_section: Locator,
    from_date: date,
    to_date: date,
    logger: JsonLogger,
    store: UcStore,
) -> bool:
    start_input = apply_section.locator("input[readonly][placeholder='Start Date']").first
    end_input = apply_section.locator("input[readonly][placeholder='End Date']").first
    if not await start_input.count() or not await end_input.count():
        log_event(
            logger=logger,
            phase="filters",
            status="warn",
            message="Apply inputs missing before export",
            store_code=store.store_code,
        )
        return False

    start_value = ""
    end_value = ""
    for _ in range(10):
        with contextlib.suppress(Exception):
            start_value = await start_input.input_value()
        with contextlib.suppress(Exception):
            end_value = await end_input.input_value()
        if _matches_date_value(start_value, from_date) and _matches_date_value(end_value, to_date):
            return True
        await asyncio.sleep(0.3)

    log_event(
        logger=logger,
        phase="filters",
        status="warn",
        message="Apply inputs did not reflect selected dates",
        store_code=store.store_code,
        start_value=start_value,
        end_value=end_value,
        expected_start=from_date.isoformat(),
        expected_end=to_date.isoformat(),
    )
    return False


async def _wait_for_report_refresh(
    *, page: Page, container: Locator, logger: JsonLogger, store: UcStore
) -> tuple[bool, int, bool]:
    row_locator = container.locator(", ".join(GST_REPORT_ROW_SELECTORS))
    row_visibility_issue = False
    try:
        initial_count = await row_locator.count()
    except Exception:
        initial_count = 0

    if initial_count > 0:
        log_event(
            logger=logger,
            phase="filters",
            message="GST report rows already present after date range apply",
            store_code=store.store_code,
            row_count=initial_count,
        )
        return True, initial_count, row_visibility_issue

    spinner_selector = ", ".join(SPINNER_CSS_SELECTORS)
    timeout_s = NAV_TIMEOUT_MS / 1000
    start = asyncio.get_event_loop().time()
    spinner_locator = page.locator(spinner_selector).first if spinner_selector else None
    network_idle_task = asyncio.create_task(
        page.wait_for_load_state("networkidle", timeout=round(timeout_s * 1000))
    )
    network_idle_reached = False
    while (asyncio.get_event_loop().time() - start) < timeout_s:
        if not network_idle_reached and network_idle_task.done():
            network_idle_reached = True
        try:
            if spinner_locator is not None and await spinner_locator.is_visible():
                with contextlib.suppress(TimeoutError):
                    await page.wait_for_selector(spinner_selector, state="hidden", timeout=10_000)
        except Exception:
            pass
        try:
            current_count = await row_locator.count()
        except Exception:
            current_count = initial_count
        if current_count > 0:
            if not network_idle_task.done():
                network_idle_task.cancel()
            log_event(
                logger=logger,
                phase="filters",
                message="GST report rows detected after date range apply",
                store_code=store.store_code,
                row_count=current_count,
            )
            return True, current_count, row_visibility_issue
        if network_idle_reached:
            break
        await asyncio.sleep(0.5)

    if not network_idle_reached:
        with contextlib.suppress(asyncio.TimeoutError, asyncio.CancelledError):
            await network_idle_task
        network_idle_reached = True

    export_ready = False
    export_selector_used: str | None = None
    export_button: Locator | None = None
    for selector in GST_CONTROL_SELECTORS["export_button"]:
        export_locator = container.locator(selector)
        try:
            export_count = await export_locator.count()
        except Exception:
            export_count = 0
        if export_count > 0:
            export_button = export_locator.first
            export_selector_used = selector
            break

    if export_button is not None:
        is_visible = False
        is_enabled = False
        with contextlib.suppress(Exception):
            is_visible = await export_button.is_visible()
        if is_visible:
            with contextlib.suppress(Exception):
                is_enabled = await export_button.is_enabled()
        export_ready = is_visible and is_enabled
        if export_ready:
            row_visibility_issue = True
            return True, initial_count, row_visibility_issue

    log_event(
        logger=logger,
        phase="filters",
        status="warn",
        message="GST report rows not detected after applying date range",
        store_code=store.store_code,
        row_count=initial_count,
    )
    return False, initial_count, row_visibility_issue


async def _validate_gst_report_visible(
    *, container: Locator, logger: JsonLogger, store: UcStore, row_count: int
) -> bool:
    start = asyncio.get_event_loop().time()
    timeout_s = 8.0
    table_section = container.locator(".table-section").first
    row_locator = container.locator(", ".join(GST_REPORT_ROW_SELECTORS))
    while (asyncio.get_event_loop().time() - start) < timeout_s:
        table_present = False
        try:
            if await table_section.count():
                table_present = True
        except Exception:
            table_present = False
        if table_present:
            current_row_count = 0
            with contextlib.suppress(Exception):
                current_row_count = await row_locator.count()
            if current_row_count > 0:
                log_event(
                    logger=logger,
                    phase="filters",
                    message="GST report table visibility confirmed after apply",
                    store_code=store.store_code,
                    selector=".table-section",
                    row_count=current_row_count,
                    waited_seconds=round(asyncio.get_event_loop().time() - start, 2),
                )
                return True
        await asyncio.sleep(0.5)
    table_count = 0
    with contextlib.suppress(Exception):
        table_count = await table_section.count()
    current_row_count = 0
    if table_count:
        with contextlib.suppress(Exception):
            current_row_count = await row_locator.count()
    if table_count and current_row_count > 0:
        log_event(
            logger=logger,
            phase="filters",
            message="GST report table visibility confirmed after apply",
            store_code=store.store_code,
            selector=".table-section",
            row_count=current_row_count,
            waited_seconds=round(asyncio.get_event_loop().time() - start, 2),
        )
        return True
    log_event(
        logger=logger,
        phase="filters",
        status="warn",
        message="GST report table not visible after apply",
        store_code=store.store_code,
        selectors=list(GST_REPORT_TABLE_SELECTORS),
        row_count=current_row_count or row_count,
        waited_seconds=round(asyncio.get_event_loop().time() - start, 2),
        table_section_found=table_count > 0,
    )
    return False


async def _is_gst_export_ready(
    *, container: Locator, logger: JsonLogger, store: UcStore
) -> tuple[bool, str | None]:
    export_button: Locator | None = None
    selector_used: str | None = None
    for selector in GST_CONTROL_SELECTORS["export_button"]:
        locator = container.locator(selector)
        try:
            if await locator.count():
                export_button = locator.first
                selector_used = selector
                break
        except Exception:
            continue
    if export_button is None:
        return False, None
    is_visible = False
    is_enabled = False
    with contextlib.suppress(Exception):
        is_visible = await export_button.is_visible()
    if is_visible:
        with contextlib.suppress(Exception):
            is_enabled = await export_button.is_enabled()
    export_ready = is_visible and is_enabled
    log_event(
        logger=logger,
        phase="filters",
        status=None if export_ready else "warn",
        message="Checked GST export button readiness after table visibility check",
        store_code=store.store_code,
        export_selector=selector_used,
        export_ready=export_ready,
        export_visible=is_visible,
        export_enabled=is_enabled,
    )
    return export_ready, selector_used


async def _is_on_home_dashboard(*, page: Page, store: UcStore) -> bool:
    current_url = page.url or ""
    if store.home_url and _url_matches_target(current_url, store.home_url):
        return True
    selectors = store.home_selectors or list(HOME_READY_SELECTORS)
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            if await locator.is_visible():
                return True
        except Exception:
            continue
    return False


async def _navigate_to_gst_reports(*, page: Page, store: UcStore, logger: JsonLogger) -> bool:
    orders_url = store.orders_url or ""
    gst_report_url = "https://store.ucleanlaundry.com/gst-report"
    try:
        await page.goto(gst_report_url, wait_until="domcontentloaded")
    except Exception as exc:
        log_event(
            logger=logger,
            phase="navigation",
            status="warn",
            message="GST reports direct navigation failed",
            store_code=store.store_code,
            orders_url=orders_url,
            current_url=page.url,
            target_url=gst_report_url,
            error=str(exc),
        )
        return False

    log_event(
        logger=logger,
        phase="navigation",
        message="GST reports direct navigation complete",
        store_code=store.store_code,
        orders_url=orders_url,
        current_url=page.url,
        target_url=gst_report_url,
    )
    return True


async def _locate_nav_link_by_href(nav_root: Locator, href_fragment: str) -> Locator | None:
    locator = nav_root.locator(f"a[href*='{href_fragment}']")
    try:
        if await locator.count():
            return locator.first
    except Exception:
        return None
    return None


async def _expand_nav_groups(*, page: Page, nav_root: Locator, logger: JsonLogger, store: UcStore) -> None:
    toggle_selectors = [
        "button[aria-expanded='false']",
        "[role='button'][aria-expanded='false']",
        ".collapsed",
        ".menu-toggle",
        ".sidebar-toggle",
        "[data-toggle='collapse']",
        "[aria-controls]",
    ]
    for selector in toggle_selectors:
        toggles = nav_root.locator(selector)
        try:
            count = await toggles.count()
        except Exception:
            continue
        for idx in range(count):
            toggle = toggles.nth(idx)
            try:
                if not await toggle.is_visible():
                    continue
                expanded = await toggle.get_attribute("aria-expanded")
                if expanded and expanded.lower() == "true":
                    continue
                await toggle.click()
                await page.wait_for_timeout(250)
                log_event(
                    logger=logger,
                    phase="navigation",
                    message="Expanded navigation group",
                    store_code=store.store_code,
                    selector=selector,
                )
            except Exception:
                continue


async def _locate_nav_ancestor_by_text(nav_root: Locator, labels: Sequence[str]) -> ElementHandle | None:
    for label in labels:
        locator = nav_root.locator(":scope *").filter(has_text=re.compile(label, re.I))
        try:
            handles = await locator.element_handles()
        except Exception:
            continue
        for handle in handles:
            try:
                ancestor = await handle.evaluate_handle(
                    """
                    (node) => node.closest('a, button, [role="menuitem"]')
                    """
                )
                element = ancestor.as_element()
                if element is not None:
                    return element
            except Exception:
                continue
    return None


async def _summarize_nav_links(nav_root: Locator) -> list[dict[str, str]]:
    links = nav_root.locator("a[href]")
    summary: list[dict[str, str]] = []
    try:
        count = await links.count()
    except Exception:
        return summary
    for idx in range(count):
        link = links.nth(idx)
        try:
            if not await link.is_visible():
                continue
            text = (await link.inner_text()).strip()
            href = (await link.get_attribute("href")) or ""
            if text or href:
                summary.append({"text": text, "href": href})
        except Exception:
            continue
    return summary


async def _find_nav_root(page: Page) -> Locator:
    for selector in NAV_CONTAINER_SELECTORS:
        locator = page.locator(selector)
        try:
            if await locator.count():
                return locator.first
        except Exception:
            continue
    return page.locator("body")


def _build_url_candidates(url: str) -> list[str]:
    if not url:
        return []
    candidates: list[str] = [url]
    parsed = urlparse(url)
    if parsed.path:
        candidates.append(parsed.path)
        candidates.append(parsed.path.strip("/"))
    unique: list[str] = []
    for candidate in candidates:
        candidate = candidate.strip()
        if candidate and candidate not in unique:
            unique.append(candidate)
    return unique


def _url_matches_target(current_url: str, target_url: str) -> bool:
    if not current_url or not target_url:
        return False
    if current_url.startswith(target_url) or target_url in current_url:
        return True
    current = urlparse(current_url)
    target = urlparse(target_url)
    if target.netloc and current.netloc and target.netloc != current.netloc:
        return False
    if target.path:
        return target.path.rstrip("/") in current.path.rstrip("/")
    return False


def _url_is_login(current_url: str, store: UcStore) -> bool:
    if not current_url:
        return False
    parsed_current = urlparse(current_url)
    if parsed_current.path.rstrip("/").endswith("/login"):
        return True
    login_url = store.login_url or ""
    if not login_url:
        return False
    parsed_login = urlparse(login_url)
    if parsed_login.netloc and parsed_current.netloc and parsed_login.netloc != parsed_current.netloc:
        return False
    if parsed_login.path and parsed_current.path.rstrip("/") == parsed_login.path.rstrip("/"):
        return True
    return False


def _dom_snippet_fields(dom_snippet: str | None) -> Dict[str, str]:
    if dom_snippet is None:
        return {}
    return {"dom_snippet": dom_snippet}


async def _maybe_get_dom_snippet(page: Page) -> str | None:
    if config.pipeline_skip_dom_logging:
        return None
    return await _get_dom_snippet(page)


async def _find_gst_report_container(*, page: Page, readiness_selector: str) -> Locator | None:
    readiness_locator = page.locator(readiness_selector)
    for selector in GST_CONTAINER_SELECTORS:
        locator = page.locator(selector).filter(has=readiness_locator)
        try:
            if await locator.count():
                return locator.first
        except Exception:
            continue
    return None


async def _get_dom_snippet(page: Page) -> str:
    try:
        content = await page.content()
    except Exception:
        return ""
    cleaned = re.sub(r"\s+", " ", content).strip()
    if len(cleaned) <= DOM_SNIPPET_MAX_CHARS:
        return cleaned
    return cleaned[: DOM_SNIPPET_MAX_CHARS - 1] + "…"


async def _log_selector_cues(
    *,
    logger: JsonLogger,
    store_code: str,
    container: Locator,
    page: Page,
) -> None:
    if config.pipeline_skip_dom_logging:
        log_event(
            logger=logger,
            phase="selectors",
            message="Skipped GST report selector cue capture because DOM logging is disabled",
            store_code=store_code,
        )
        return
    selectors_payload = await _discover_selector_cues(container=container, page=page)
    spinner_payload = await _discover_spinner_cues(page)
    log_event(
        logger=logger,
        phase="selectors",
        message="Captured GST report selector cues",
        store_code=store_code,
        controls=selectors_payload,
        spinners=spinner_payload,
    )


async def _discover_selector_cues(*, container: Locator, page: Page) -> Dict[str, Any]:
    results: Dict[str, Any] = {
        "selector_matches": await _probe_gst_control_selectors(container=container),
        "date_picker_popup": await _probe_date_picker_popup(page=page),
    }
    for label, cues in CONTROL_CUES.items():
        matches: list[Dict[str, Any]] = []
        for cue in cues:
            selector = f"text=/{re.escape(cue)}/i"
            locator = container.locator(selector)
            try:
                count = await locator.count()
            except Exception:
                continue
            if not count:
                continue
            sample_count = min(count, 3)
            for idx in range(sample_count):
                entry = locator.nth(idx)
                text = (await entry.text_content()) or ""
                tag = await entry.evaluate("el => el.tagName")
                entry_class = await entry.get_attribute("class")
                entry_id = await entry.get_attribute("id")
                matches.append(
                    {
                        "cue": cue,
                        "selector": selector,
                        "text": text.strip(),
                        "tag": tag,
                        "class": entry_class,
                        "id": entry_id,
                    }
                )
            if count > sample_count:
                matches.append({"cue": cue, "selector": selector, "truncated": True, "count": count})
        results[label] = {"cues": cues, "matches": matches}
    return results


async def _probe_gst_control_selectors(*, container: Locator) -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    for label, selectors in GST_CONTROL_SELECTORS.items():
        matches: list[Dict[str, Any]] = []
        for selector in selectors:
            locator = container.locator(selector)
            try:
                count = await locator.count()
            except Exception:
                continue
            if not count:
                continue
            sample_count = min(count, 3)
            samples: list[Dict[str, Any]] = []
            for idx in range(sample_count):
                entry = locator.nth(idx)
                text = (await entry.text_content()) or ""
                samples.append({"text": text.strip()})
            matches.append(
                {
                    "selector": selector,
                    "count": count,
                    "samples": samples,
                }
            )
        results[label] = {"selectors": selectors, "matches": matches}
    return results


async def _probe_date_picker_popup(*, page: Page) -> Dict[str, Any]:
    matches: list[Dict[str, Any]] = []
    popup_present = False
    for selector in DATE_PICKER_POPUP_SELECTORS:
        locator = page.locator(selector)
        try:
            count = await locator.count()
        except Exception:
            continue
        if not count:
            continue
        visible = False
        with contextlib.suppress(Exception):
            visible = await locator.first.is_visible()
        matches.append({"selector": selector, "count": count, "visible": visible})
        if visible or count:
            popup_present = True
    return {"present": popup_present, "matches": matches}


async def _discover_spinner_cues(page: Page) -> list[Dict[str, Any]]:
    results: list[Dict[str, Any]] = []
    for selector in SPINNER_CSS_SELECTORS + [SPINNER_TEXT_SELECTOR]:
        locator = page.locator(selector)
        try:
            count = await locator.count()
        except Exception:
            continue
        if count:
            results.append({"selector": selector, "count": count})

    class_locator = page.locator("[class*=spinner], [class*=loading], [class*=loader]")
    try:
        count = await class_locator.count()
    except Exception:
        count = 0
    if count:
        samples = []
        for idx in range(min(count, 3)):
            entry = class_locator.nth(idx)
            samples.append(
                {
                    "class": await entry.get_attribute("class"),
                    "id": await entry.get_attribute("id"),
                }
            )
        results.append({"selector": "[class*=spinner|loading|loader]", "count": count, "samples": samples})
    return results


async def _persist_summary(*, summary: UcOrdersDiscoverySummary, logger: JsonLogger) -> bool:
    finished_at = datetime.now(timezone.utc)
    record = summary.build_record(finished_at=finished_at)
    if not config.database_url:
        log_event(
            logger=logger,
            phase="run_summary",
            status="warn",
            message="Skipping run summary persistence because database_url is missing",
            run_id=summary.run_id,
        )
        return False

    try:
        await _flush_deferred_orders_sync_logs(
            summary=summary, logger=logger, run_id=summary.run_id, run_env=summary.run_env
        )
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
    except Exception as exc:  # pragma: no cover - defensive persistence
        log_event(
            logger=logger,
            phase="run_summary",
            status="error",
            message="Failed to persist run summary",
            run_id=summary.run_id,
            error=str(exc),
        )
        return False


async def _start_run_summary(*, summary: UcOrdersDiscoverySummary, logger: JsonLogger) -> bool:
    if not config.database_url:
        log_event(
            logger=logger,
            phase="run_summary",
            status="warn",
            message="Skipping run summary start because database_url is missing",
            run_id=summary.run_id,
        )
        return False

    record = {
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
            message=f"Run summary {action} at start",
            run_id=summary.run_id,
            overall_status="running",
        )
        return True
    except Exception as exc:  # pragma: no cover - defensive persistence
        log_event(
            logger=logger,
            phase="run_summary",
            status="error",
            message="Failed to start run summary",
            run_id=summary.run_id,
            error=str(exc),
        )
        return False


# ── CLI entrypoint ───────────────────────────────────────────────────────────


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the UC Orders discovery flow")
    parser.add_argument("--run-env", dest="run_env", type=str, default=None, help="Override run environment label")
    parser.add_argument("--run-id", dest="run_id", type=str, default=None, help="Override generated run id")
    parser.add_argument("--from-date", dest="from_date", type=_parse_date, default=None, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", dest="to_date", type=_parse_date, default=None, help="End date (YYYY-MM-DD)")
    return parser


async def _async_entrypoint(argv: Sequence[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    await main(
        run_env=args.run_env,
        run_id=args.run_id,
        from_date=args.from_date,
        to_date=args.to_date,
    )


def run(argv: Sequence[str] | None = None) -> None:
    asyncio.run(_async_entrypoint(argv))


if __name__ == "__main__":  # pragma: no cover
    run()
