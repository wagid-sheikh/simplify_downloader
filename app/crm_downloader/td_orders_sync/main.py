from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import contextlib
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Sequence
from urllib.parse import urlparse, urljoin

import sqlalchemy as sa
from playwright.async_api import Browser, BrowserContext, FrameLocator, Locator, Page, TimeoutError, async_playwright

from app.common.db import session_scope
from app.common.date_utils import get_timezone
from app.config import config
from app.crm_downloader.config import default_download_dir, default_profiles_dir
from app.crm_downloader.td_orders_sync.ingest import TdOrdersIngestResult, ingest_td_orders_workbook
from app.crm_downloader.td_orders_sync.sales_ingest import TdSalesIngestResult, ingest_td_sales_workbook
from app.dashboard_downloader.json_logger import JsonLogger, get_logger, log_event, new_run_id
from app.dashboard_downloader.notifications import send_notifications_for_run
from app.dashboard_downloader.run_summary import fetch_summary_for_run, insert_run_summary, update_run_summary

PIPELINE_NAME = "td_orders_sync"
DASHBOARD_DOWNLOAD_NAV_TIMEOUT_DEFAULT_MS = 90_000
LOADING_LOCATOR_SELECTORS = ("text=/loading/i", ".k-loading-mask")
OTP_VERIFICATION_DWELL_SECONDS = 600
TEMP_ENABLED_STORES = {"A668", "A817"}
REPORT_REQUEST_MAX_TIMEOUT_MS = 60_000
REPORT_REQUEST_POLL_LOG_INTERVAL_SECONDS = 20.0
REPORT_REQUEST_REFRESH_INTERVAL_SECONDS = 15.0
PENDING_MIN_POLL_SECONDS = 135
STABLE_LOCATOR_STRATEGIES = {
    "container_locator_strategy": "heading_preferred_wrapper",
    "range_match_strategy": "date_range_text_pattern",
    "download_locator_strategy": "href_contains_order_reports",
}
ENABLE_LEGACY_REPORT_REQUEST_ROW_LOCATORS = False
INGEST_REMARKS_MAX_ROWS = 50
INGEST_REMARKS_MAX_CHARS = 200
DOM_SNIPPET_MAX_CHARS = 600
WARNING_SAMPLE_LIMIT = 3
VERBOSE_DOM_LOGGING = os.environ.get("TD_ORDERS_VERBOSE_LOGGING", "").strip().lower() in {"1", "true", "yes"}
NAV_SNAPSHOT_SAMPLE_LIMIT = 8
SALES_NAV_SAMPLE_LIMIT = 8
ROW_SAMPLE_LIMIT = 6
SNAPSHOT_TEXT_MAX_CHARS = 120


def _truncate_text(value: str | None, *, max_chars: int = SNAPSHOT_TEXT_MAX_CHARS) -> str | None:
    if not value:
        return value
    value = value.strip()
    if len(value) <= max_chars:
        return value
    return value[: max_chars - 1] + "…"


def _summarize_text_samples(values: Sequence[str], *, limit: int = ROW_SAMPLE_LIMIT) -> dict[str, Any]:
    samples: list[str] = []
    for value in values:
        trimmed = _truncate_text(value)
        if trimmed not in samples:
            samples.append(trimmed or "")
        if len(samples) >= limit:
            break
    return {"count": len(values), "samples": samples, "truncated": len(values) > len(samples)}


def _summarize_warnings(
    warnings: Sequence[str], *, sample_size: int = WARNING_SAMPLE_LIMIT
) -> dict[str, int | bool | list[str]]:
    samples: list[str] = []
    for warning in warnings:
        if warning not in samples:
            samples.append(warning)
        if len(samples) >= sample_size:
            break
    return {"count": len(warnings), "samples": samples, "truncated": len(warnings) > len(samples)}


def _format_warning_preview(summary: Mapping[str, Any]) -> str:
    count = int(summary.get("count") or 0)
    samples = [str(entry) for entry in (summary.get("samples") or [])]
    if not count:
        return ""
    suffix = "…" if summary.get("truncated") else ""
    if samples:
        return f"{count} warning(s) (samples: {', '.join(samples)}{suffix})"
    return f"{count} warning(s)"


async def main(
    *,
    run_env: str | None = None,
    run_id: str | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
    run_orders: bool = True,
    run_sales: bool = True,
) -> None:
    """Run the TD Orders sync flow (login + iframe historical orders download)."""

    resolved_run_id = run_id or new_run_id()
    resolved_run_date = datetime.now(get_timezone())
    resolved_env = run_env or config.run_env
    today = datetime.now(timezone.utc).date()
    run_start_date = from_date or today - timedelta(days=30)
    run_end_date = to_date or today
    logger = get_logger(run_id=resolved_run_id)
    summary = TdOrdersDiscoverySummary(
        run_id=resolved_run_id,
        run_env=resolved_env,
        report_date=run_start_date,
    )

    interrupted = False
    persist_attempted = False
    browser: Browser | None = None

    try:
        log_event(
            logger=logger,
            phase="init",
            message="Starting TD orders sync discovery flow",
            run_env=resolved_env,
            from_date=run_start_date,
            to_date=run_end_date,
        )

        nav_timeout_ms = await _fetch_dashboard_nav_timeout_ms(config.database_url)

        stores = await _load_td_order_stores(logger=logger)
        summary.store_codes = [store.store_code for store in stores]
        if not stores:
            log_event(
                logger=logger,
                phase="init",
                status="warn",
                message="No TD stores with sync_orders_flag found; exiting",
            )
            summary.mark_phase("init", "warning")
            summary.add_note("No TD stores with sync_orders_flag found; exiting")
            persist_attempted = True
            if await _persist_summary(summary=summary, logger=logger):
                await send_notifications_for_run(PIPELINE_NAME, resolved_run_id)
            else:
                log_event(
                    logger=logger,
                    phase="notifications",
                    status="warn",
                    message="Skipping notifications because run summary was not recorded",
                    run_id=resolved_run_id,
                )
            return

        async with async_playwright() as p:
            browser = await _launch_browser(playwright=p, logger=logger)
            for store in stores:
                await _run_store_discovery(
                    browser=browser,
                    store=store,
                    logger=logger,
                    run_env=resolved_env,
                    run_id=resolved_run_id,
                    run_date=resolved_run_date,
                    run_start_date=run_start_date,
                    run_end_date=run_end_date,
                    nav_timeout_ms=nav_timeout_ms,
                    summary=summary,
                    run_orders=run_orders,
                    run_sales=run_sales,
                )

            await browser.close()

        log_event(
            logger=logger,
            phase="notifications",
            message="TD orders sync discovery flow complete; notifying",
            run_env=resolved_env,
        )
        persist_attempted = True
        if await _persist_summary(summary=summary, logger=logger):
            await send_notifications_for_run(PIPELINE_NAME, resolved_run_id)
        else:
            log_event(
                logger=logger,
                phase="notifications",
                status="warn",
                message="Skipping notifications because run summary was not recorded",
                run_env=resolved_env,
                run_id=resolved_run_id,
            )
    except asyncio.CancelledError:
        interrupted = True
        summary.add_note("Run interrupted by cancellation")
        summary.mark_phase("store", "warning")
        log_event(
            logger=logger,
            phase="store",
            status="warn",
            message="TD orders sync discovery interrupted; attempting graceful shutdown",
            run_id=resolved_run_id,
        )
        with contextlib.suppress(Exception):
            if browser:
                await browser.close()
        if not persist_attempted:
            persist_attempted = True
            await _persist_summary(summary=summary, logger=logger)
        return
    except KeyboardInterrupt:
        interrupted = True
        summary.add_note("Run interrupted by keyboard interrupt")
        summary.mark_phase("store", "warning")
        log_event(
            logger=logger,
            phase="store",
            status="warn",
            message="TD orders sync discovery interrupted by KeyboardInterrupt; attempting graceful shutdown",
            run_id=resolved_run_id,
        )
        with contextlib.suppress(Exception):
            if browser:
                await browser.close()
        if not persist_attempted:
            persist_attempted = True
            await _persist_summary(summary=summary, logger=logger)
        return
    except Exception as exc:  # pragma: no cover - defensive guard
        log_event(
            logger=logger,
            phase="store",
            status="error",
            message="TD orders sync discovery failed unexpectedly",
            run_id=resolved_run_id,
            error=str(exc),
        )
        summary.add_note(f"Run failed unexpectedly: {exc}")
        summary.mark_phase("store", "error")
        if not persist_attempted:
            persist_attempted = True
            await _persist_summary(summary=summary, logger=logger)
        raise
    finally:
        if not persist_attempted and not interrupted:
            await _persist_summary(summary=summary, logger=logger)
        with contextlib.suppress(Exception):
            logger.close()


# ── Data helpers ─────────────────────────────────────────────────────────────


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:  # pragma: no cover - exercised via CLI parsing
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}; expected YYYY-MM-DD") from exc


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


def _format_report_range_text(from_date: date, to_date: date) -> str:
    return f"{from_date.strftime('%b')} {from_date.day}, {from_date.year} - {to_date.strftime('%b')} {to_date.day}, {to_date.year}"


def _format_report_range_text_candidates(from_date: date, to_date: date) -> list[str]:
    def _append_unique(candidate: str, seen: set[str], items: list[str]) -> None:
        if candidate not in seen:
            seen.add(candidate)
            items.append(candidate)

    month_from = from_date.strftime("%b")
    month_to = to_date.strftime("%b")
    from_day_variants = [f"{from_date.day:02d}", str(from_date.day)]
    to_day_variants = [f"{to_date.day:02d}", str(to_date.day)]

    seen: set[str] = set()
    variants: list[str] = []
    for from_day in from_day_variants:
        for to_day in to_day_variants:
            table_style = f"{from_day} {month_from} {from_date.year} - {to_day} {month_to} {to_date.year}"
            month_first = f"{month_from} {from_day}, {from_date.year} - {month_to} {to_day}, {to_date.year}"
            month_first_no_comma = f"{month_from} {from_day} {from_date.year} - {month_to} {to_day} {to_date.year}"
            _append_unique(table_style, seen, variants)
            _append_unique(month_first, seen, variants)
            _append_unique(month_first_no_comma, seen, variants)
    return variants


def _build_date_range_patterns(from_date: date, to_date: date) -> list[re.Pattern[str]]:
    month_from_short = from_date.strftime("%b")
    month_from_long = from_date.strftime("%B")
    month_to_short = to_date.strftime("%b")
    month_to_long = to_date.strftime("%B")
    from_day = f"{from_date.day:02d}".lstrip("0")
    to_day = f"{to_date.day:02d}".lstrip("0")
    return [
        re.compile(
            rf"\b({month_from_short}|{month_from_long})\s+0?{from_day},?\s+{from_date.year}\s*-\s*({month_to_short}|{month_to_long})\s+0?{to_day},?\s+{to_date.year}",
            re.IGNORECASE,
        ),
        re.compile(
            rf"\b0?{from_day}\s+({month_from_short}|{month_from_long})\s+{from_date.year}\s*-\s*0?{to_day}\s+({month_to_short}|{month_to_long})\s+{to_date.year}",
            re.IGNORECASE,
        ),
    ]


def _format_orders_filename(store_code: str, from_date: date, to_date: date) -> str:
    return f"{store_code}_td_orders_{from_date.strftime('%Y%m%d')}_{to_date.strftime('%Y%m%d')}.xlsx"


def _format_sales_filename(store_code: str, from_date: date, to_date: date) -> str:
    return f"{store_code}_td_sales_{from_date.strftime('%Y%m%d')}_{to_date.strftime('%Y%m%d')}.xlsx"


@dataclass
class TdStore:
    store_code: str
    store_name: str | None
    cost_center: str | None
    sync_config: Dict[str, Any]

    @property
    def storage_state_path(self) -> Path:
        return default_profiles_dir() / f"{self.store_code}_storage_state.json"

    @property
    def default_home_url(self) -> str | None:
        code = (self.store_code or "").strip().lower()
        if not code:
            return None
        return f"https://subs.quickdrycleaning.com/{code}/App/home"

    @property
    def session_probe_url(self) -> str | None:
        code = (self.store_code or "").strip().lower()
        if not code:
            return None
        return f"https://subs.quickdrycleaning.com/{code}/App/home?EventClick=True"

    @property
    def login_url(self) -> str | None:
        return _get_nested_str(self.sync_config, ("urls", "login"))

    @property
    def home_url(self) -> str | None:
        return _get_nested_str(self.sync_config, ("urls", "home"))

    @property
    def orders_url(self) -> str | None:
        return _get_nested_str(self.sync_config, ("urls", "orders_link"))

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
    def reports_nav_selector(self) -> str:
        raw = _get_nested_str(self.sync_config, ("selectors", "reports_nav")) or self.sync_config.get(
            "reports_nav_selector"
        )
        return _normalize_id_selector(raw or "#achrOrderReport")


@dataclass
class StoreOutcome:
    status: str
    message: str
    final_url: str | None = None
    iframe_attached: bool | None = None
    verification_seen: bool | None = None
    storage_state: str | None = None


@dataclass
class StoreReport:
    status: str
    filenames: list[str] = field(default_factory=list)
    staging_rows: int | None = None
    final_rows: int | None = None
    message: str | None = None
    error_message: str | None = None
    warnings: list[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "filenames": list(self.filenames),
            "staging_rows": self.staging_rows,
            "final_rows": self.final_rows,
            "message": self.message,
            "error_message": self.error_message,
            "warnings": list(self.warnings),
        }


@dataclass
class TdOrdersDiscoverySummary:
    run_id: str
    run_env: str
    report_date: date
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    store_codes: list[str] = field(default_factory=list)
    store_outcomes: Dict[str, StoreOutcome] = field(default_factory=dict)
    phases: defaultdict[str, Dict[str, int]] = field(
        default_factory=lambda: defaultdict(lambda: {"ok": 0, "warning": 0, "error": 0})
    )
    notes: list[str] = field(default_factory=list)
    ingest_remarks: list[dict[str, str]] = field(default_factory=list)
    orders_results: Dict[str, StoreReport] = field(default_factory=dict)
    sales_results: Dict[str, StoreReport] = field(default_factory=dict)

    def mark_phase(self, phase: str, status: str) -> None:
        counters = self.phases.setdefault(phase, {"ok": 0, "warning": 0, "error": 0})
        normalized = status if status in counters else "ok"
        counters[normalized] += 1

    def record_store(
        self,
        store_code: str,
        outcome: StoreOutcome,
        *,
        orders_result: StoreReport | None = None,
        sales_result: StoreReport | None = None,
    ) -> None:
        self.store_outcomes[store_code] = outcome
        resolved_orders = orders_result or StoreReport(
            status=outcome.status,
            message=outcome.message,
            error_message=outcome.message if outcome.status in {"error", "warning"} else None,
        )
        fallback_sales_status = outcome.status if outcome.status in {"error", "warning"} else "warning"
        resolved_sales = sales_result or StoreReport(
            status=fallback_sales_status,
            message=outcome.message or "Sales not attempted",
            error_message=outcome.message if fallback_sales_status in {"error", "warning"} else None,
        )
        self.orders_results[store_code] = resolved_orders
        self.sales_results[store_code] = resolved_sales
        self.mark_phase("store", outcome.status)

    def add_note(self, note: str) -> None:
        if note not in self.notes:
            self.notes.append(note)

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

    def overall_status(self) -> str:
        if not self.store_outcomes:
            return "warning" if self.phases.get("init", {}).get("warning") else "ok"

        statuses = {outcome.status for outcome in self.store_outcomes.values()}
        if statuses == {"error"}:
            return "error"
        if "error" in statuses:
            return "warning"
        return "ok"

    def orders_overall_status(self) -> str:
        return self._overall_status_for_reports(self.orders_results)

    def sales_overall_status(self) -> str:
        return self._overall_status_for_reports(self.sales_results)

    def _overall_status_for_reports(self, reports: Mapping[str, StoreReport]) -> str:
        if not reports:
            return "warning"
        statuses = {report.status for report in reports.values()}
        normalized = {status for status in statuses if status != "skipped"}
        if not normalized:
            return "ok"
        if "error" in normalized:
            return "error"
        if "warning" in normalized:
            return "warning"
        if normalized == {"ok"}:
            return "ok"
        return "warning"

    def _format_duration(self, finished_at: datetime) -> str:
        seconds = max(0, int((finished_at - self.started_at).total_seconds()))
        hh = seconds // 3600
        mm = (seconds % 3600) // 60
        ss = seconds % 60
        return f"{hh:02d}:{mm:02d}:{ss:02d}"

    def summary_text(self, *, finished_at: datetime | None = None) -> str:
        resolved_finished_at = finished_at or datetime.now(timezone.utc)
        remarks_lines, truncated_rows, truncated_length = self._ingest_remarks_section(
            max_rows=INGEST_REMARKS_MAX_ROWS, max_chars=INGEST_REMARKS_MAX_CHARS
        )
        lines = [
            f"Pipeline: {PIPELINE_NAME}",
            f"Run ID: {self.run_id}",
            f"Env: {self.run_env}",
            f"Started: {self.started_at.isoformat()}",
            f"Finished: {resolved_finished_at.isoformat()}",
            f"Report Date: {self.report_date.isoformat()}",
            f"Overall Status: {self.overall_status()}",
            f"Orders Status: {self.orders_overall_status()}",
            f"Sales Status: {self.sales_overall_status()}",
            "",
            "Orders results:",
        ]
        lines.extend(self._format_report_section(self.orders_results))
        lines.append("")
        lines.append("Sales results:")
        lines.extend(self._format_report_section(self.sales_results))
        if self.notes:
            lines.append("")
            lines.append("Notes:")
            lines.extend(f"- {note}" for note in self.notes)
        lines.append("")
        lines.append("Ingest remarks:")
        lines.extend(remarks_lines)
        if truncated_rows:
            lines.append(
                f"... additional {len(self.ingest_remarks) - INGEST_REMARKS_MAX_ROWS} remarks truncated"
            )
        elif truncated_length:
            lines.append("... some remarks truncated for length")
        return "\n".join(lines)

    def _ingest_warnings_payload(self, *, max_rows: int, max_chars: int) -> dict[str, Any]:
        if not self.ingest_remarks:
            return {"rows": [], "total": 0, "truncated": False}
        truncated = len(self.ingest_remarks) > max_rows
        rows: list[dict[str, str]] = []
        for entry in self.ingest_remarks[:max_rows]:
            remark = entry.get("ingest_remarks") or ""
            if len(remark) > max_chars:
                remark = remark[: max_chars - 1] + "…"
            rows.append(
                {
                    "store_code": (entry.get("store_code") or "").upper(),
                    "order_number": entry.get("order_number") or "",
                    "ingest_remarks": remark,
                }
            )
        return {"rows": rows, "total": len(self.ingest_remarks), "truncated": truncated}

    def _ingest_remarks_section(self, *, max_rows: int, max_chars: int) -> tuple[list[str], bool, bool]:
        if not self.ingest_remarks:
            return ["- None."], False, False
        truncated_length = False
        truncated_rows = len(self.ingest_remarks) > max_rows
        limited_rows = self.ingest_remarks[:max_rows]
        lines = []
        for entry in limited_rows:
            remark = entry.get("ingest_remarks") or ""
            if len(remark) > max_chars:
                remark = remark[: max_chars - 1] + "…"
                truncated_length = True
            store_code = (entry.get("store_code") or "").upper()
            order_number = entry.get("order_number") or ""
            lines.append(f"- {store_code} {order_number}: {remark}")
        return lines, truncated_rows, truncated_length

    def _format_report_section(self, reports: Mapping[str, StoreReport]) -> list[str]:
        if not reports:
            return ["- none recorded"]
        lines: list[str] = []
        for code in sorted(reports):
            report = reports[code]
            counts = self._format_report_counts(report)
            files = ", ".join(report.filenames) if report.filenames else "none"
            details: list[str] = [counts, f"files: {files}"]
            if report.error_message:
                details.append(f"error: {report.error_message}")
            elif report.message:
                details.append(report.message)
            if report.warnings:
                details.append(f"warnings: {', '.join(report.warnings)}")
            lines.append(f"- {code}: {report.status.upper()} | " + " | ".join(details))
        return lines

    def _format_report_counts(self, report: StoreReport) -> str:
        if report.final_rows is not None and report.staging_rows is not None:
            return f"rows: staging={report.staging_rows}, final={report.final_rows}"
        if report.final_rows is not None:
            return f"rows: final={report.final_rows}"
        if report.staging_rows is not None:
            return f"rows: staging={report.staging_rows}"
        return "rows: n/a"

    def _store_codes_for_payload(self) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for code in self.store_codes:
            normalized = code.upper()
            if normalized and normalized not in seen:
                seen.add(normalized)
                ordered.append(normalized)
        for mapping in (self.orders_results, self.sales_results, self.store_outcomes):
            for code in mapping:
                normalized = code.upper()
                if normalized not in seen:
                    seen.add(normalized)
                    ordered.append(normalized)
        return ordered

    def _build_notification_payload(self, *, finished_at: datetime) -> Dict[str, Any]:
        stores: list[dict[str, Any]] = []
        for code in self._store_codes_for_payload():
            outcome = self.store_outcomes.get(code)
            stores.append(
                {
                    "store_code": code,
                    "status": outcome.status if outcome else None,
                    "message": outcome.message if outcome else None,
                    "orders": (self.orders_results.get(code) or StoreReport(status="skipped")).as_dict(),
                    "sales": (self.sales_results.get(code) or StoreReport(status="skipped")).as_dict(),
                }
            )

        return {
            "overall_status": self.overall_status(),
            "orders_status": self.orders_overall_status(),
            "sales_status": self.sales_overall_status(),
            "stores": stores,
            "ingest_warnings": self._ingest_warnings_payload(
                max_rows=INGEST_REMARKS_MAX_ROWS, max_chars=INGEST_REMARKS_MAX_CHARS
            ),
            "started_at": self.started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
        }

    def build_record(self, *, finished_at: datetime) -> Dict[str, Any]:
        metrics = {
            "stores": {code: asdict(outcome) for code, outcome in self.store_outcomes.items()},
            "store_order": self.store_codes,
            "ingest_remarks": {
                "rows": list(self.ingest_remarks),
                "total": len(self.ingest_remarks),
            },
            "orders": {
                "overall_status": self.orders_overall_status(),
                "stores": {code: report.as_dict() for code, report in self.orders_results.items()},
            },
            "sales": {
                "overall_status": self.sales_overall_status(),
                "stores": {code: report.as_dict() for code, report in self.sales_results.items()},
            },
        }
        metrics["notification_payload"] = self._build_notification_payload(finished_at=finished_at)
        return {
            "pipeline_name": PIPELINE_NAME,
            "run_id": self.run_id,
            "run_env": self.run_env,
            "started_at": self.started_at,
            "finished_at": finished_at,
            "total_time_taken": self._format_duration(finished_at),
            "report_date": self.report_date,
            "overall_status": self.overall_status(),
            "summary_text": self.summary_text(finished_at=finished_at),
            "phases_json": {phase: dict(counts) for phase, counts in self.phases.items()},
            "metrics_json": metrics,
        }


@dataclass
class SessionProbeResult:
    valid: bool
    final_url: str | None
    reason: str | None = None
    contains_store_code: bool | None = None
    contains_store_path: bool | None = None
    verification_seen: bool | None = None
    login_detected: bool | None = None
    nav_visible: bool | None = None
    home_card_visible: bool | None = None


async def _load_td_order_stores(*, logger: JsonLogger) -> List[TdStore]:
    query = sa.text(
        """
        SELECT store_code, store_name, sync_config, cost_center
        FROM store_master
        WHERE sync_group = :sync_group
          AND sync_orders_flag = TRUE
          AND (is_active IS NULL OR is_active = TRUE)
        """
    )

    async with session_scope(config.database_url) as session:
        result = await session.execute(query, {"sync_group": "TD"})
        stores: List[TdStore] = []
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
            sync_config = _coerce_dict(row.get("sync_config"))
            stores.append(
                TdStore(
                    store_code=raw_code.upper(),
                    store_name=row.get("store_name"),
                    cost_center=row.get("cost_center"),
                    sync_config=sync_config,
                )
            )

    if TEMP_ENABLED_STORES:
        scoped = [store for store in stores if store.store_code in TEMP_ENABLED_STORES]
        skipped = sorted({store.store_code for store in stores} - TEMP_ENABLED_STORES)
        if scoped:
            log_event(
                logger=logger,
                phase="init",
                status="warn",
                message="Temporarily restricting TD orders discovery to a subset of stores",
                stores=[store.store_code for store in scoped],
                skipped_stores=skipped or None,
            )
        stores = scoped

    log_event(
        logger=logger,
        phase="init",
        message="Loaded TD store rows",
        store_count=len(stores),
        stores=[store.store_code for store in stores],
    )
    return stores


async def _fetch_dashboard_nav_timeout_ms(database_url: str | None) -> int:
    if not database_url:
        return DASHBOARD_DOWNLOAD_NAV_TIMEOUT_DEFAULT_MS

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
        return DASHBOARD_DOWNLOAD_NAV_TIMEOUT_DEFAULT_MS

    raw_value = row[0] if row else None
    try:
        return int(str(raw_value).strip())
    except (TypeError, ValueError):
        return DASHBOARD_DOWNLOAD_NAV_TIMEOUT_DEFAULT_MS


async def _persist_summary(*, summary: TdOrdersDiscoverySummary, logger: JsonLogger) -> bool:
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
        existing = await fetch_summary_for_run(config.database_url, summary.run_id)
        if existing:
            await update_run_summary(config.database_url, summary.run_id, record)
            action = "updated"
        else:
            await insert_run_summary(config.database_url, record)
            action = "inserted"
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


# ── Browser helpers ───────────────────────────────────────────────────────────


async def _launch_browser(*, playwright: Any, logger: JsonLogger) -> Browser:
    backend = (config.pdf_render_backend or "").lower()
    chrome_exec = (config.pdf_render_chrome_executable or "").strip() or None
    headless = config.etl_headless
    launch_kwargs: Dict[str, Any] = {"headless": headless}

    if backend == "local_chrome":
        if chrome_exec and Path(chrome_exec).is_file():
            launch_kwargs["executable_path"] = chrome_exec
            log_event(
                logger=logger,
                phase="init",
                message="Launching Playwright with local Chrome executable",
                backend=backend,
                executable_path=chrome_exec,
                headless=headless,
            )
        else:
            log_event(
                logger=logger,
                phase="init",
                status="warn",
                message="Configured local Chrome executable missing; falling back to bundled Chromium",
                backend=backend,
                executable_path=chrome_exec,
                headless=headless,
            )
    else:
        log_event(
            logger=logger,
            phase="init",
            message="Launching Playwright with bundled Chromium",
            backend=backend or "bundled_chromium",
            headless=headless,
        )

    try:
        return await playwright.chromium.launch(**launch_kwargs)
    except Exception as exc:
        if launch_kwargs.pop("executable_path", None) is not None:
            log_event(
                logger=logger,
                phase="init",
                status="warn",
                message="Local Chrome launch failed; retrying with bundled Chromium",
                backend=backend,
                executable_path=chrome_exec,
                headless=headless,
                error=str(exc),
            )
            return await playwright.chromium.launch(**launch_kwargs)
        raise


# ── Playwright helpers ───────────────────────────────────────────────────────


async def _probe_session(page: Page, *, store: TdStore, logger: JsonLogger, timeout_ms: int) -> SessionProbeResult:
    target_url = store.session_probe_url or store.home_url or store.default_home_url or store.orders_url
    if not target_url:
        log_event(
            logger=logger,
            phase="session",
            status="warn",
            message="No home URL available to probe session",
            store_code=store.store_code,
        )
        return SessionProbeResult(valid=False, final_url=None, reason="no_probe_target")

    response = None
    probe_error: str | None = None
    try:
        response = await page.goto(target_url, wait_until="domcontentloaded", timeout=timeout_ms)
    except Exception as exc:
        probe_error = str(exc)

    final_url = page.url
    url_lower = (final_url or "").lower()
    store_code_path = f"/{store.store_code.lower()}/"
    contains_store = store.store_code.lower() in url_lower
    contains_store_path = store_code_path in url_lower
    verification_seen = "frmverification" in url_lower
    login_detected = False
    nav_visible = False
    home_card_visible = False
    try:
        login_detected = await page.locator("#txtUserId, input[name='username']").first.is_visible()
    except Exception:
        login_detected = False

    async def _probe_visibility() -> tuple[bool, bool]:
        nav_ok = False
        home_ok = False
        try:
            nav_ok = await page.locator(store.reports_nav_selector).first.is_visible()
        except Exception:
            nav_ok = False
        try:
            home_ok = await page.locator("h5.card-title:has-text(\"Daily Operations Tracker\")").is_visible()
        except Exception:
            home_ok = False
        return nav_ok, home_ok

    nav_visible, home_card_visible = await _probe_visibility()
    if not (nav_visible or home_card_visible):
        visibility_deadline = asyncio.get_event_loop().time() + 5.0
        while asyncio.get_event_loop().time() < visibility_deadline:
            await asyncio.sleep(0.3)
            nav_visible, home_card_visible = await _probe_visibility()
            if nav_visible or home_card_visible:
                break

    nav_ready = (contains_store_path or contains_store) and nav_visible
    home_ready = (contains_store_path or contains_store) and home_card_visible
    state_valid = (nav_ready or home_ready) and not verification_seen and not login_detected and probe_error is None
    if probe_error:
        reason = "probe_navigation_error"
    elif verification_seen:
        reason = "verification_redirect"
    elif login_detected:
        reason = "login_form_visible"
    elif not (nav_visible or home_card_visible):
        reason = "navigation_controls_missing"
    elif not contains_store:
        reason = "store_code_missing_from_url"
    else:
        reason = None

    log_event(
        logger=logger,
        phase="session",
        status="ok" if state_valid else "warn",
        message="Probed session with existing storage state",
        store_code=store.store_code,
        response_status=getattr(response, "status", None),
        final_url=final_url,
        contains_store_code=contains_store,
        verification_seen=verification_seen,
        login_detected=login_detected,
        nav_visible=nav_visible,
        home_card_visible=home_card_visible,
        state_valid=state_valid,
        invalid_reason=reason,
        probe_error=probe_error,
        probe_target=target_url,
        contains_store_path=contains_store_path,
    )
    return SessionProbeResult(
        valid=state_valid,
        final_url=final_url,
        reason=reason,
        contains_store_code=contains_store,
        contains_store_path=contains_store_path,
        verification_seen=verification_seen,
        login_detected=login_detected,
        nav_visible=nav_visible,
        home_card_visible=home_card_visible,
    )


async def _perform_login(page: Page, *, store: TdStore, logger: JsonLogger, nav_timeout_ms: int) -> bool:
    missing_fields = [
        label for label, value in (("login_url", store.login_url), ("username", store.username), ("password", store.password)) if not value
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

    selectors = {
        "username": _normalize_id_selector(store.login_selectors.get("username", "#txtUserId")),
        "password": _normalize_id_selector(store.login_selectors.get("password", "#txtPassword")),
        "store_code": _normalize_id_selector(store.login_selectors.get("store_code", "#txtBranchPin")),
        "submit": _normalize_id_selector(store.login_selectors.get("submit", "#btnLogin")),
    }

    await page.goto(store.login_url, wait_until="domcontentloaded")

    try:
        await page.wait_for_selector("#txtUserId", timeout=nav_timeout_ms)
    except TimeoutError:
        log_event(
            logger=logger,
            phase="login",
            status="error",
            message="Username selector not found within timeout",
            store_code=store.store_code,
            selector="#txtUserId",
            timeout_ms=nav_timeout_ms,
        )
        return False

    await page.fill(selectors["username"], store.username or "")
    await page.fill(selectors["password"], store.password or "")

    if selectors.get("store_code"):
        await page.fill(selectors["store_code"], store.store_code)

    post_login_tasks: list[tuple[str, asyncio.Task[Any]]] = []
    if store.store_code:
        post_login_tasks.append(
            (
                "url_contains_store_code",
                asyncio.create_task(
                    page.wait_for_url(
                        re.compile(re.escape(store.store_code), re.IGNORECASE),
                        wait_until="domcontentloaded",
                        timeout=nav_timeout_ms,
                    )
                ),
            )
        )

    post_login_tasks.append(
        (
            "home_card_visible",
            asyncio.create_task(
                page.wait_for_selector(
                    "h5.card-title:has-text(\"Daily Operations Tracker\")",
                    timeout=nav_timeout_ms,
                )
            ),
        )
    )

    submit_error: str | None = None
    try:
        await page.click(selectors["submit"])
    except Exception as exc:
        submit_error = str(exc)
        try:
            await page.get_by_role("button", name="Login").click()
        except Exception as fallback_exc:
            submit_error = f"{submit_error}; fallback_click_error={fallback_exc}"

    completed_label: str | None = None
    post_login_error: str | None = None
    if post_login_tasks:
        done, pending = await asyncio.wait(
            [task for _, task in post_login_tasks],
            return_when=asyncio.FIRST_COMPLETED,
            timeout=nav_timeout_ms / 1000,
        )

        for task in pending:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        if done:
            for label, task in post_login_tasks:
                if task in done:
                    completed_label = label
                    try:
                        task.result()
                    except Exception as exc:  # pragma: no cover - defensive logging only
                        post_login_error = str(exc)
                    break
        else:
            post_login_error = f"post-login wait timed out after {nav_timeout_ms}ms"

    final_url = page.url
    contains_store = store.store_code.lower() in (final_url or "").lower()
    log_event(
        logger=logger,
        phase="login",
        status="ok" if contains_store else "warn",
        message="Completed login attempt",
        store_code=store.store_code,
        final_url=final_url,
        contains_store_code=contains_store,
        post_login_wait_completed=completed_label,
        post_login_wait_error=post_login_error,
        submit_error=submit_error,
        selectors=selectors,
    )
    return contains_store


def _url_matches_home(current_url: str, store: TdStore) -> bool:
    url_lower = (current_url or "").lower()
    if store.home_url:
        try:
            if url_lower.startswith((store.home_url or "").lower()):
                return True
        except Exception:
            pass
    if store.default_home_url:
        try:
            if url_lower.startswith(store.default_home_url.lower()):
                return True
        except Exception:
            pass
    if store.session_probe_url:
        try:
            if url_lower.startswith(store.session_probe_url.lower().split("?")[0]):
                return True
        except Exception:
            pass
    return store.store_code.lower() in url_lower


async def _wait_for_home(
    page: Page, *, store: TdStore, logger: JsonLogger, nav_selector: str, timeout_ms: int
) -> bool:
    deadline = asyncio.get_event_loop().time() + (timeout_ms / 1000)
    seen_visible = False
    home_card_seen = False
    home_card_selector = "h5.card-title:has-text(\"Daily Operations Tracker\")"

    async def _home_ready() -> tuple[bool, str, bool, bool]:
        current_url = page.url or ""
        url_ok = _url_matches_home(current_url, store)
        nav_visible = False
        home_visible = False
        try:
            nav_visible = await page.locator(nav_selector).first.is_visible()
            seen_home = page.locator(home_card_selector)
            home_visible = await seen_home.is_visible()
        except Exception:
            nav_visible = False
        return url_ok and (nav_visible or home_visible), current_url, nav_visible, home_visible

    while asyncio.get_event_loop().time() < deadline:
        ready, current_url, nav_visible, home_visible = await _home_ready()
        seen_visible = seen_visible or nav_visible
        home_card_seen = home_card_seen or home_visible
        if ready:
            log_event(
                logger=logger,
                phase="login",
                message="Home page ready after login/verification",
                store_code=store.store_code,
                final_url=current_url,
                nav_selector=nav_selector,
                nav_visible=nav_visible,
                home_card_visible=home_visible,
            )
            return True
        await asyncio.sleep(1)

    log_event(
        logger=logger,
        phase="login",
        status="error",
        message="Home page not ready after timeout",
        store_code=store.store_code,
        final_url=page.url,
        nav_selector=nav_selector,
        nav_visible=seen_visible,
        home_card_visible=home_card_seen,
    )
    return False


async def _log_home_nav_diagnostics(page: Page, *, logger: JsonLogger, store: TdStore) -> None:
    try:
        links = page.locator("a.padding6, a#achrOrderReport, a[href*='Reports']")
        link_count = await links.count()
        snapshot: list[dict[str, Any]] = []
        for idx in range(min(link_count, NAV_SNAPSHOT_SAMPLE_LIMIT)):
            handle = links.nth(idx)
            try:
                text = _truncate_text(await handle.inner_text())
                href = await handle.get_attribute("href")
                visible = await handle.is_visible()
            except Exception:
                continue
            snapshot.append({"index": idx, "text": text, "href": href, "visible": visible})
        log_event(
            logger=logger,
            phase="home",
            message="Navigation controls snapshot",
            store_code=store.store_code,
            links=snapshot,
            link_count=link_count,
        )
    except Exception as exc:  # pragma: no cover - diagnostics best effort
        log_event(
            logger=logger,
            phase="home",
            status="warn",
            message="Failed to capture navigation diagnostics",
            store_code=store.store_code,
            error=str(exc),
        )


async def _capture_orders_left_nav_snapshot(
    page: Page,
    *,
    logger: JsonLogger,
    store: TdStore,
    context: str,
    nav_timeout_ms: int,
    sales_only_mode: bool = False,
) -> None:
    try:
        nav_root = page.locator("ul.nav.nav-sidebar")
        await nav_root.wait_for(state="visible", timeout=nav_timeout_ms)
    except Exception as exc:  # pragma: no cover - diagnostics best effort
        log_event(
            logger=logger,
            phase="orders",
            status="warn",
            message="Orders left-nav snapshot not available",
            store_code=store.store_code,
            context=context,
            sales_only_mode=sales_only_mode,
            error=str(exc),
            final_url=page.url,
        )
        return

    links = nav_root.locator("a")
    count = await links.count()
    snapshot: list[dict[str, Any]] = []
    for idx in range(min(count, NAV_SNAPSHOT_SAMPLE_LIMIT)):
        anchor = links.nth(idx)
        try:
            href = await anchor.get_attribute("href")
            text = _truncate_text(await anchor.inner_text()) or ""
            visible = await anchor.is_visible()
            normalized = urljoin(page.url or "", href) if href else None
        except Exception:
            continue

        snapshot.append(
            {
                "index": idx,
                "text": text.strip(),
                "href": href,
                "normalized_href": normalized,
                "visible": visible,
            }
        )

    reports_links: list[dict[str, Any]] = []
    try:
        reports_sections = nav_root.locator("li:has(> a:has-text(\"Reports\"))")
        if await reports_sections.count():
            reports_root = reports_sections.first
            report_links = reports_root.locator("a")
            reports_count = await report_links.count()
            for idx in range(min(reports_count, NAV_SNAPSHOT_SAMPLE_LIMIT)):
                anchor = report_links.nth(idx)
                try:
                    href = await anchor.get_attribute("href")
                    text = _truncate_text(await anchor.inner_text()) or ""
                    normalized = urljoin(page.url or "", href) if href else None
                    visible = await anchor.is_visible()
                except Exception:
                    continue

                reports_links.append(
                    {
                        "index": idx,
                        "text": text.strip(),
                        "href": href,
                        "normalized_href": normalized,
                        "visible": visible,
                    }
                )
    except Exception:
        reports_links = reports_links

    log_event(
        logger=logger,
        phase="orders",
        message="Orders left-nav snapshot",
        store_code=store.store_code,
        context=context,
        sales_only_mode=sales_only_mode,
        nav_url=page.url,
        link_count=count,
        reports_count=len(reports_links),
        links=snapshot,
        reports_links=reports_links,
    )


async def _navigate_to_orders_container(
    page: Page,
    *,
    store: TdStore,
    logger: JsonLogger,
    nav_selector: str,
    nav_timeout_ms: int,
    capture_left_nav: bool = False,
    nav_snapshot_context: str | None = None,
    sales_only_mode: bool = False,
) -> bool:
    target_pattern = re.compile(r"/Reports/OrderReport", re.IGNORECASE)
    snapshot_context = nav_snapshot_context or "orders_entry"

    async def _log_left_nav(context_label: str) -> None:
        if not capture_left_nav:
            return
        await _capture_orders_left_nav_snapshot(
            page,
            logger=logger,
            store=store,
            context=context_label,
            nav_timeout_ms=nav_timeout_ms,
            sales_only_mode=sales_only_mode,
        )

    if target_pattern.search(page.url or ""):
        log_event(
            logger=logger,
            phase="orders",
            message="Already on Orders container; waiting for iframe",
            store_code=store.store_code,
            final_url=page.url,
        )
        await _log_left_nav(f"{snapshot_context}:already_loaded")
        return True

    try:
        await page.wait_for_selector(nav_selector, timeout=nav_timeout_ms)
    except TimeoutError:
        log_event(
            logger=logger,
            phase="orders",
            status="error",
            message="Reports navigation selector not found on home page",
            store_code=store.store_code,
            nav_selector=nav_selector,
            final_url=page.url,
        )
        return False

    try:
        await page.click(nav_selector)
        await page.wait_for_url(target_pattern, wait_until="domcontentloaded", timeout=nav_timeout_ms)
    except TimeoutError:
        log_event(
            logger=logger,
            phase="orders",
            status="error",
            message="Orders container did not load via Reports navigation",
            store_code=store.store_code,
            final_url=page.url,
            nav_selector=nav_selector,
        )
        return False

    final_url = page.url or ""
    await _log_left_nav(f"{snapshot_context}:after_navigation")
    if "simplifytumbledry.in/tms/orders" in final_url.lower():
        log_event(
            logger=logger,
            phase="orders",
            status="error",
            message="Navigated to unsupported TMS Orders URL instead of Orders Report container",
            store_code=store.store_code,
            final_url=final_url,
        )
        return False

    log_event(
        logger=logger,
        phase="orders",
        message="Navigated to Orders container via Reports entry",
        store_code=store.store_code,
        final_url=final_url,
        nav_selector=nav_selector,
    )
    return True


def _build_sales_report_url(store: TdStore, current_url: str | None = None) -> str | None:
    base_url = store.home_url or store.default_home_url or current_url
    if not base_url:
        return None
    parsed = urlparse(base_url)
    if not parsed.scheme or not parsed.netloc:
        return None
    store_code_lower = (store.store_code or "").strip().lower()
    if not store_code_lower:
        return None
    return f"{parsed.scheme}://{parsed.netloc}/{store_code_lower}/App/Reports/NEWSalesAndDeliveryReport"


async def _navigate_to_sales_report(
    page: Page,
    *,
    store: TdStore,
    logger: JsonLogger,
    nav_timeout_ms: int,
    sales_only_mode: bool = False,
) -> bool:
    target_url = _build_sales_report_url(store, page.url)
    if not target_url:
        log_event(
            logger=logger,
            phase="sales",
            status="warn",
            message="Unable to build Sales & Delivery report URL",
            store_code=store.store_code,
            current_url=page.url,
        )
        return False

    target_fragment = "/app/reports/newsalesanddeliveryreport"
    target_pattern = re.compile(
        rf"/{re.escape(store.store_code)}/App/Reports/NEWSalesAndDeliveryReport",
        re.IGNORECASE,
    )
    orders_target_pattern = re.compile(r"/Reports/OrderReport", re.IGNORECASE)
    nav_selector = store.reports_nav_selector
    navigation_path_left_nav = "orders_left_nav"
    attempts: list[dict[str, Any]] = []
    last_attempt: dict[str, Any] | None = None

    def _is_login_url(url: str | None) -> bool:
        if not url:
            return False
        url_lower = url.lower()
        login_url = store.login_url or ""
        try:
            if login_url and login_url.lower() in url_lower:
                return True
        except Exception:
            pass
        return "/login" in url_lower

    def _validate_destination(url: str) -> tuple[bool, bool, bool]:
        url_lower = url.lower()
        return (
            store.store_code.lower() in url_lower and target_fragment in url_lower and not _is_login_url(url),
            store.store_code.lower() in url_lower,
            target_fragment in url_lower,
        )

    def _record_attempt(
        *,
        method_label: str,
        navigation_path: str,
        attempt_label: str,
        retry_status: str,
        success: bool,
        final_url: str,
        reason: str,
        url_transitions: list[dict[str, str]] | None = None,
        sales_nav_selector: str | None = None,
        nav_samples: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        attempt = {
            "navigation_method": method_label,
            "navigation_path": navigation_path,
            "attempt": attempt_label,
            "retry_status": retry_status,
            "success": success,
            "final_url": final_url,
            "target_url": target_url,
            "nav_selector": nav_selector,
            "sales_nav_selector": sales_nav_selector,
            "reason": reason,
            "url_transitions": url_transitions,
            "nav_samples": nav_samples,
        }
        attempts.append(attempt)
        log_event(
            logger=logger,
            phase="sales",
            message="Sales navigation attempt",
            store_code=store.store_code,
            **attempt,
        )
        return attempt

    def _log_navigation_outcome(
        *,
        status: str,
        navigation_method: str,
        navigation_path: str,
        retry_status: str,
        final_url: str,
        reason: str | None = None,
    ) -> None:
        log_event(
            logger=logger,
            phase="sales",
            message="Sales navigation outcome",
            status=status if status != "ok" else None,
            store_code=store.store_code,
            navigation_method=navigation_method,
            navigation_path=navigation_path,
            retry_status=retry_status,
            final_url=final_url,
            reason=reason,
            attempts=attempts,
        )

    async def _refresh_sales_session_with_guard(*, retry_label: str) -> tuple[bool, str | None]:
        relogin_attempts = 0
        last_reason: str | None = None
        while relogin_attempts < 2:
            relogin_attempts += 1
            relogin_ok = await _refresh_sales_session()
            current_url = page.url or ""
            if not relogin_ok:
                last_reason = "relogin_failed"
                if _is_login_url(current_url) and relogin_attempts < 2:
                    log_event(
                        logger=logger,
                        phase="sales",
                        status="warn",
                        message="Sales re-login attempt failed and page is still Login; retrying once more",
                        store_code=store.store_code,
                        retry_status=retry_label,
                        relogin_attempt=relogin_attempts,
                        final_url=current_url,
                    )
                    continue
                break
            if _is_login_url(current_url):
                last_reason = "stuck_on_login"
                log_event(
                    logger=logger,
                    phase="sales",
                    status="warn",
                    message="Detected Login page immediately after Sales re-login; retrying once more",
                    store_code=store.store_code,
                    retry_status=retry_label,
                    relogin_attempt=relogin_attempts,
                    final_url=current_url,
                )
                continue
            return True, None

        return False, last_reason or "relogin_failed"

    async def _refresh_sales_session() -> bool:
        relogin_ok = await _perform_login(page, store=store, logger=logger, nav_timeout_ms=nav_timeout_ms)
        if not relogin_ok:
            log_event(
                logger=logger,
                phase="sales",
                status="warn",
                message="Sales navigation retry login failed",
                store_code=store.store_code,
                final_url=page.url,
                target_url=target_url,
            )
            return False

        home_ready = await _wait_for_home(
            page,
            store=store,
            logger=logger,
            nav_selector=nav_selector,
            timeout_ms=nav_timeout_ms,
        )
        if not home_ready:
            log_event(
                logger=logger,
                phase="sales",
                status="warn",
                message="Home not ready after Sales re-login attempt",
                store_code=store.store_code,
                final_url=page.url,
                target_url=target_url,
            )
            return False

        store.storage_state_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            await page.context.storage_state(path=str(store.storage_state_path))
            log_event(
                logger=logger,
                phase="sales",
                message="Refreshed storage state after Sales re-login",
                store_code=store.store_code,
                storage_state=str(store.storage_state_path),
                final_url=page.url,
            )
        except Exception as exc:  # pragma: no cover - best effort persistence
            log_event(
                logger=logger,
                phase="sales",
                status="warn",
                message="Failed to persist refreshed Sales storage state",
                store_code=store.store_code,
                storage_state=str(store.storage_state_path),
                error=str(exc),
            )
        return True

    async def _ensure_orders_container(
        *, retry_label: str
    ) -> tuple[bool, str, list[dict[str, str]], str]:
        transitions: list[dict[str, str]] = [{"label": "start", "url": page.url or ""}]
        if orders_target_pattern.search(page.url or ""):
            await _capture_orders_left_nav_snapshot(
                page,
                logger=logger,
                store=store,
                context=f"sales_left_nav:{retry_label}:already_on_orders",
                nav_timeout_ms=nav_timeout_ms,
                sales_only_mode=sales_only_mode,
            )
            transitions.append({"label": "already_on_orders", "url": page.url or ""})
            return True, page.url or "", transitions, "ok"

        log_event(
            logger=logger,
            phase="sales",
            message="Navigating to Orders container before Sales",
            store_code=store.store_code,
            navigation_method="orders_reports_tile",
            navigation_path=navigation_path_left_nav,
            retry_status=retry_label,
            nav_selector=nav_selector,
        )
        ready = await _navigate_to_orders_container(
            page,
            store=store,
            logger=logger,
            nav_selector=nav_selector,
            nav_timeout_ms=nav_timeout_ms,
            capture_left_nav=True,
            nav_snapshot_context=f"sales_left_nav:{retry_label}",
            sales_only_mode=sales_only_mode,
        )
        transitions.append({"label": "after_orders_nav", "url": page.url or ""})
        if ready:
            return True, page.url or "", transitions, "ok"

        return False, page.url or "", transitions, "orders_not_ready"

    async def _locate_sales_left_nav() -> tuple[Locator | None, str | None, list[dict[str, Any]], bool]:
        def _normalize_label(value: str | None) -> str:
            normalized = (value or "").replace("&", " and ")
            normalized = re.sub(r"\s+", " ", normalized)
            return normalized.strip().lower()

        try:
            nav_root = page.locator("ul.nav.nav-sidebar")
            await nav_root.wait_for(state="visible", timeout=nav_timeout_ms)
            reports_section = nav_root.locator("li:has(> a:has-text(\"Reports\"))").first
            await reports_section.wait_for(state="visible", timeout=nav_timeout_ms)
        except Exception as exc:
            log_event(
                logger=logger,
                phase="sales",
                status="warn",
                message="Sales left-nav submenu not available",
                store_code=store.store_code,
                error=str(exc),
                final_url=page.url,
            )
            return None, None, [], False

        hover_error: str | None = None
        hover_attempted = False
        submenu_visible = False
        submenu_root = reports_section.locator("ul")
        try:
            await reports_section.hover()
            hover_attempted = True
        except Exception as exc:
            hover_error = str(exc)
            try:
                await reports_section.locator("> a").first.hover()
                hover_attempted = True
                hover_error = None
            except Exception as inner_exc:
                hover_error = hover_error or str(inner_exc)

        try:
            await submenu_root.wait_for(state="visible", timeout=nav_timeout_ms)
            submenu_visible = True
        except TimeoutError as exc:
            hover_error = hover_error or str(exc)

        links = submenu_root.locator("a")
        count = await links.count()
        samples: list[dict[str, Any]] = []
        selected_locator: Locator | None = None
        normalized_href: str | None = None
        target_label = "sales and delivery"

        for idx in range(min(count, SALES_NAV_SAMPLE_LIMIT)):
            locator = links.nth(idx)
            href = None
            text = None
            visible = False
            try:
                href = await locator.get_attribute("href")
                text = (await locator.inner_text()) or ""
                visible = await locator.is_visible()
            except Exception:
                href = href or None
                text = text or None
                visible = False

            normalized_text = _normalize_label(text)
            normalized = urljoin(page.url or target_url, href) if href else None
            matches_label = target_label in normalized_text

            samples.append(
                {
                    "index": idx,
                    "href": href,
                    "normalized_href": normalized,
                "text": _truncate_text(text),
                "normalized_text": _truncate_text(normalized_text),
                "visible": visible,
                "matches_label": matches_label,
            }
        )

            if selected_locator is None and matches_label:
                selected_locator = locator
                normalized_href = normalized or href

        log_event(
            logger=logger,
            phase="sales",
            message="Sales left-nav submenu snapshot",
            store_code=store.store_code,
            samples=samples,
            link_count=count,
            submenu_visible=submenu_visible,
            hover_attempted=hover_attempted,
            hover_error=hover_error,
        )

        return selected_locator, normalized_href, samples, submenu_visible

    async def _click_sales_left_nav(
        *, retry_label: str
    ) -> tuple[bool, str, str, list[dict[str, str]], bool, str | None, list[dict[str, Any]]]:
        orders_ready, _, transitions, orders_reason = await _ensure_orders_container(retry_label=retry_label)
        if not orders_ready:
            return False, page.url or "", f"orders_not_ready:{orders_reason}", transitions, False, None, []

        sales_locator, normalized_href, samples, submenu_visible = await _locate_sales_left_nav()
        transitions.append({"label": "orders_ready", "url": page.url or ""})
        if sales_locator is None:
            reason = "sales_nav_not_found" if submenu_visible else "sales_submenu_not_visible"
            return False, page.url or "", reason, transitions, False, normalized_href, samples

        try:
            await sales_locator.wait_for(state="visible", timeout=nav_timeout_ms)
        except TimeoutError:
            return False, page.url or "", "sales_nav_not_visible", transitions, True, normalized_href, samples

        log_event(
            logger=logger,
            phase="sales",
            message="Clicking Sales navigation from Orders left-nav",
            store_code=store.store_code,
            navigation_method="orders_left_nav",
            navigation_path=navigation_path_left_nav,
            retry_status=retry_label,
            sales_nav_href=normalized_href,
            samples=samples,
        )

        try:
            await sales_locator.click()
            transitions.append({"label": "after_sales_nav_click", "url": page.url or ""})
        except Exception as exc:
            return (
                False,
                page.url or "",
                f"sales_nav_click_failed:{exc}",
                transitions,
                True,
                normalized_href,
                samples,
            )

        try:
            await page.wait_for_url(target_pattern, wait_until="domcontentloaded", timeout=nav_timeout_ms)
            transitions.append({"label": "after_sales_nav_wait", "url": page.url or ""})
        except Exception:
            pass

        final_url = page.url or ""
        valid, contains_store, contains_path = _validate_destination(final_url)
        if valid:
            return True, final_url, "ok", transitions, True, normalized_href, samples

        is_login_url = _is_login_url(final_url)
        return (
            False,
            final_url,
            "login_redirect" if is_login_url else f"validation_failed:{contains_store}:{contains_path}",
            transitions,
            True,
            normalized_href,
            samples,
        )

    current_url = page.url or ""
    if target_fragment in current_url.lower() and store.store_code.lower() in current_url.lower():
        log_event(
            logger=logger,
            phase="sales",
            message="Already on Sales & Delivery report page",
            store_code=store.store_code,
            final_url=current_url,
        )
        _log_navigation_outcome(
            status="ok",
            navigation_method="already_loaded",
            navigation_path="already_loaded",
            retry_status="initial",
            final_url=current_url,
        )
        return True

    (
        success,
        final_url,
        reason,
        transitions,
        nav_found,
        sales_nav_href,
        nav_samples,
    ) = await _click_sales_left_nav(retry_label="initial")
    nav_missing = reason == "sales_nav_not_found"
    last_attempt = _record_attempt(
        method_label="orders_left_nav",
        navigation_path=navigation_path_left_nav,
        attempt_label="initial",
        retry_status="initial",
        success=success,
        final_url=final_url,
        reason=reason,
        url_transitions=transitions,
        sales_nav_selector=sales_nav_href,
        nav_samples=nav_samples,
    )
    if success:
        log_event(
            logger=logger,
            phase="sales",
            message="Navigated to Sales & Delivery report page",
            store_code=store.store_code,
            final_url=final_url,
            target_url=target_url,
            navigation_method="orders_left_nav",
            navigation_path=navigation_path_left_nav,
            retry_status="initial",
            attempts=attempts,
        )
        _log_navigation_outcome(
            status="ok",
            navigation_method="orders_left_nav",
            navigation_path=navigation_path_left_nav,
            retry_status="initial",
            final_url=final_url,
        )
        return True

    if reason.startswith("login_redirect"):
        relogin_ok, relogin_reason = await _refresh_sales_session_with_guard(retry_label="after_login")
        if not relogin_ok:
            log_event(
                logger=logger,
                phase="sales",
                status="warn",
                message="Sales navigation retry aborted because session refresh failed",
                store_code=store.store_code,
                final_url=page.url,
                target_url=target_url,
                reason=reason,
                attempts=attempts,
                retry_status="after_login",
            )
            _log_navigation_outcome(
                status="warn",
                navigation_method=(last_attempt or {}).get("navigation_method", "orders_left_nav"),
                navigation_path=(last_attempt or {}).get("navigation_path", navigation_path_left_nav),
                retry_status="after_login",
                final_url=page.url or "",
                reason=relogin_reason or reason,
            )
            return False

        target_url = _build_sales_report_url(store, page.url) or target_url
        log_event(
            logger=logger,
            phase="sales",
            message="Retrying Sales navigation via Orders left-nav after login redirect",
            store_code=store.store_code,
            retry_status="after_login",
            target_url=target_url,
            last_reason=reason,
            navigation_path=navigation_path_left_nav,
        )
        (
            success,
            final_url,
            reports_reason,
            transitions,
            nav_found_retry,
            sales_nav_href,
            nav_samples,
        ) = await _click_sales_left_nav(retry_label="after_login")
        nav_found = nav_found or nav_found_retry
        nav_missing = nav_missing or reports_reason == "sales_nav_not_found"
        last_attempt = _record_attempt(
            method_label="orders_left_nav",
            navigation_path=navigation_path_left_nav,
            attempt_label="after_login",
            retry_status="after_login",
            success=success,
            final_url=final_url,
            reason=reports_reason,
            url_transitions=transitions,
            sales_nav_selector=sales_nav_href,
            nav_samples=nav_samples,
        )
        if success:
            log_event(
                logger=logger,
                phase="sales",
                message="Navigated to Sales & Delivery report page via Orders left-nav after login",
                store_code=store.store_code,
                final_url=final_url,
                target_url=target_url,
                navigation_method="orders_left_nav_after_login",
                navigation_path=navigation_path_left_nav,
                retry_status="after_login",
                attempts=attempts,
            )
            _log_navigation_outcome(
                status="ok",
                navigation_method="orders_left_nav_after_login",
                navigation_path=navigation_path_left_nav,
                retry_status="after_login",
                final_url=final_url,
            )
            return True
        reason = reports_reason

    elif reason.startswith("validation_failed"):
        relogin_ok, relogin_reason = await _refresh_sales_session_with_guard(retry_label="after_validation")
        if not relogin_ok:
            _log_navigation_outcome(
                status="warn",
                navigation_method=(last_attempt or {}).get("navigation_method", "orders_left_nav"),
                navigation_path=(last_attempt or {}).get("navigation_path", navigation_path_left_nav),
                retry_status="after_validation",
                final_url=page.url or "",
                reason=relogin_reason or reason,
            )
            return False

        target_url = _build_sales_report_url(store, page.url) or target_url
        log_event(
            logger=logger,
            phase="sales",
            message="Retrying Sales navigation via Orders left-nav after validation failure",
            store_code=store.store_code,
            retry_status="after_validation",
            target_url=target_url,
            last_reason=reason,
            navigation_path=navigation_path_left_nav,
        )
        (
            success,
            final_url,
            reports_reason,
            transitions,
            nav_found_retry,
            sales_nav_href,
            nav_samples,
        ) = await _click_sales_left_nav(retry_label="after_validation")
        nav_found = nav_found or nav_found_retry
        nav_missing = nav_missing or reports_reason == "sales_nav_not_found"
        last_attempt = _record_attempt(
            method_label="orders_left_nav",
            navigation_path=navigation_path_left_nav,
            attempt_label="after_validation",
            retry_status="after_validation",
            success=success,
            final_url=final_url,
            reason=reports_reason,
            url_transitions=transitions,
            sales_nav_selector=sales_nav_href,
            nav_samples=nav_samples,
        )
        if success:
            _log_navigation_outcome(
                status="ok",
                navigation_method="orders_left_nav_after_validation",
                navigation_path=navigation_path_left_nav,
                retry_status="after_validation",
                final_url=final_url,
            )
            return True
        reason = reports_reason

    log_event(
        logger=logger,
        phase="sales",
        status="warn",
        message="Sales & Delivery report navigation failed after retry",
        store_code=store.store_code,
        final_url=final_url,
        target_url=target_url,
        reason=reason,
        attempts=attempts,
        navigation_method=(last_attempt or {}).get("navigation_method"),
        navigation_path=(last_attempt or {}).get("navigation_path"),
        retry_status=(last_attempt or {}).get("retry_status"),
    )
    if nav_missing:
        log_event(
            logger=logger,
            phase="sales",
            status="warn",
            message="Sales left-nav link not found; exiting without direct URL navigation",
            store_code=store.store_code,
            final_url=final_url,
            attempts=attempts,
        )
    _log_navigation_outcome(
        status="warn",
        navigation_method=(last_attempt or {}).get("navigation_method", "unknown"),
        navigation_path=(last_attempt or {}).get("navigation_path", "unknown"),
        retry_status=(last_attempt or {}).get("retry_status", "unknown"),
        final_url=final_url,
        reason=reason,
    )
    return False


async def _wait_for_iframe(
    page: Page,
    *,
    store: TdStore,
    logger: JsonLogger,
    require_src: bool = False,
    phase: str = "orders",
) -> FrameLocator | None:
    try:
        await page.wait_for_selector("#ifrmReport", state="attached", timeout=20_000)
    except TimeoutError:
        log_event(
            logger=logger,
            phase=phase,
            status="error",
            message="iframe#ifrmReport not attached within timeout",
            store_code=store.store_code,
            final_url=page.url,
            iframe_attached=False,
        )
        return None

    iframe_src = None
    try:
        handle = await page.query_selector("#ifrmReport")
        if handle:
            iframe_src = await handle.get_attribute("src")
    except Exception:
        iframe_src = None

    if require_src and not (iframe_src or "").strip():
        log_event(
            logger=logger,
            phase=phase,
            status="error",
            message="iframe#ifrmReport attached without src",
            store_code=store.store_code,
            final_url=page.url,
            iframe_attached=True,
        )
        return None

    iframe_src_length = len(iframe_src) if iframe_src else None
    iframe_src_prefix = iframe_src[:64] if iframe_src else None
    iframe_src_hostname = urlparse(iframe_src).hostname if iframe_src else None

    log_event(
        logger=logger,
        phase=phase,
        message="Report container ready; iframe attached",
        store_code=store.store_code,
        final_url=page.url,
        iframe_src_hostname=iframe_src_hostname,
        iframe_src_prefix=iframe_src_prefix,
        iframe_src_length=iframe_src_length,
        iframe_attached=True,
    )
    return page.frame_locator("#ifrmReport")


async def _observe_iframe_hydration(
    frame: FrameLocator, *, store: TdStore, logger: JsonLogger, timeout_ms: int = 20_000
) -> None:
    spinner_candidates = [
        {"label": "kendo_loading_mask", "selector": ".k-loading-mask"},
        {"label": "progress_loader", "selector": "img[alt*='loading' i]"},
        {"label": "loading_text", "selector": "text=/loading/i"},
    ]
    control_candidates = [
        {"label": "expand_button", "locator": frame.get_by_role("button", name=re.compile("expand", re.I))},
        {
            "label": "download_historical",
            "locator": frame.get_by_role("button", name=re.compile("download historical report", re.I)),
        },
        {"label": "generate_report", "locator": frame.get_by_role("button", name=re.compile("generate report", re.I))},
        {"label": "download_link", "locator": frame.get_by_role("link", name=re.compile("download", re.I))},
    ]

    observed_spinners: Dict[str, Dict[str, Any]] = {}
    observed_controls: Dict[str, Dict[str, Any]] = {}
    deadline = asyncio.get_event_loop().time() + (timeout_ms / 1000)

    async def _record_spinner(candidate: Dict[str, str]) -> None:
        locator = frame.locator(candidate["selector"])
        count = await locator.count()
        visible = False
        if count:
            try:
                visible = await locator.first.is_visible()
            except Exception:
                visible = False
        previous = observed_spinners.get(candidate["label"], {})
        waited_for_hidden = bool(previous.get("waited_for_hidden"))
        observed_spinners[candidate["label"]] = {
            "label": candidate["label"],
            "selector": candidate["selector"],
            "count": count,
            "visible": visible,
            "ever_visible": bool(previous.get("ever_visible")) or visible,
            "waited_for_hidden": waited_for_hidden or visible,
        }
        if visible and not waited_for_hidden:
            try:
                await locator.first.wait_for(state="hidden", timeout=timeout_ms)
            except TimeoutError:
                pass

    async def _record_control(candidate: Dict[str, Any]) -> bool:
        locator = candidate["locator"]
        count = await locator.count()
        if count == 0:
            return False
        visible_texts: List[str] = []
        for idx in range(min(count, 5)):
            handle = locator.nth(idx)
            try:
                if await handle.is_visible():
                    text = (await handle.inner_text()).strip()
                    if text:
                        visible_texts.append(text)
            except Exception:
                continue
        observed_controls[candidate["label"]] = {"label": candidate["label"], "count": count, "texts": visible_texts}
        return bool(visible_texts)

    while asyncio.get_event_loop().time() < deadline:
        for candidate in spinner_candidates:
            await _record_spinner(candidate)

        control_visible = False
        for candidate in control_candidates:
            control_visible = await _record_control(candidate) or control_visible

        if control_visible:
            break
        await asyncio.sleep(0.5)

    log_event(
        logger=logger,
        phase="iframe",
        message="Iframe hydration observations",
        store_code=store.store_code,
        observed_controls=list(observed_controls.values()),
        observed_spinners=list(observed_spinners.values()),
    )


async def _wait_for_loading_indicators(
    frame: FrameLocator,
    *,
    store: TdStore,
    logger: JsonLogger,
    timeout_ms: int,
    phase: str = "iframe",
) -> None:
    deadline = asyncio.get_event_loop().time() + (timeout_ms / 1000)
    seen_visible = False
    last_error: str | None = None

    while asyncio.get_event_loop().time() < deadline:
        visible = False
        for selector in LOADING_LOCATOR_SELECTORS:
            try:
                locator = frame.locator(selector).first
                if await locator.is_visible():
                    visible = True
                    seen_visible = True
                    break
            except Exception as exc:  # pragma: no cover - diagnostics only
                last_error = str(exc)
                continue

        if not visible:
            if seen_visible:
                log_event(
                    logger=logger,
                    phase=phase,
                    message="Loading indicators cleared",
                    store_code=store.store_code,
                    selectors=list(LOADING_LOCATOR_SELECTORS),
                )
            return

        await asyncio.sleep(0.5)

    if seen_visible:
        log_event(
            logger=logger,
            phase=phase,
            status="warn",
            message="Loading indicators still visible after timeout",
            store_code=store.store_code,
            selectors=list(LOADING_LOCATOR_SELECTORS),
            error=last_error,
        )


async def _first_visible_locator(candidates: list[Locator], *, timeout_ms: int) -> Locator | None:
    deadline = asyncio.get_event_loop().time() + (timeout_ms / 1000)
    while asyncio.get_event_loop().time() < deadline:
        for candidate in candidates:
            try:
                count = await candidate.count()
                if count and await candidate.first.is_visible():
                    return candidate.first
            except Exception:
                continue
        await asyncio.sleep(0.3)
    return None


async def _first_visible_locator_with_label(
    candidates: list[tuple[str, Locator]], *, timeout_ms: int
) -> tuple[Locator | None, str | None]:
    deadline = asyncio.get_event_loop().time() + (timeout_ms / 1000)
    while asyncio.get_event_loop().time() < deadline:
        for label, candidate in candidates:
            try:
                count = await candidate.count()
                if count and await candidate.first.is_visible():
                    return candidate.first, label
            except Exception:
                continue
        await asyncio.sleep(0.3)
    return None, None


async def _wait_for_date_ui(
    frame: FrameLocator, *, timeout_ms: int
) -> list[dict[str, Any]]:
    cues = [
        {"label": "step_select_date", "locator": frame.locator("text=/Step\\s*1.*Select Date/i")},
        {"label": "select_date_text", "locator": frame.locator("text=/Select Date/i")},
        {"label": "from_text", "locator": frame.locator("text=/\\bfrom\\b/i")},
        {"label": "to_text", "locator": frame.locator("text=/\\bto\\b/i")},
    ]
    observations: list[dict[str, Any]] = []
    deadline = asyncio.get_event_loop().time() + (timeout_ms / 1000)

    while asyncio.get_event_loop().time() < deadline:
        visible_any = False
        observations.clear()
        for cue in cues:
            locator = cue["locator"]
            try:
                count = await locator.count()
                visible = count > 0 and await locator.first.is_visible()
            except Exception:
                count = 0
                visible = False
            observations.append({"label": cue["label"], "count": count, "visible": visible})
            visible_any = visible_any or visible
        if visible_any:
            break
        await asyncio.sleep(0.5)
    return observations


async def _locate_date_range_control(
    frame: FrameLocator, *, timeout_ms: int
) -> tuple[Locator | None, list[dict[str, Any]]]:
    attempts: list[dict[str, Any]] = []
    candidates: list[tuple[str, Locator]] = [
        (
            "button_with_range_text",
            frame.get_by_role("button", name=re.compile(r"[a-zA-Z]{3}\s+\d{1,2}.*,\s*\d{4}\s*-", re.I)),
        ),
        ("calendar_icon_button", frame.locator("button:has(.k-icon.k-i-calendar)")),
        ("k-daterangepicker", frame.locator(".k-daterangepicker button, .k-daterangepicker .k-input-inner")),
        ("text_select_date_button", frame.get_by_role("button", name=re.compile("date range|select date|calendar", re.I))),
        ("div_range_with_icon", frame.locator("div:has(.k-icon.k-i-calendar):has-text('-')")),
    ]

    for label, locator in candidates:
        try:
            count = await locator.count()
        except Exception:
            count = 0
        attempt = {"label": label, "count": count, "used": False}
        attempts.append(attempt)
        if count:
            found = await _first_visible_locator([locator], timeout_ms=timeout_ms)
            if found is not None:
                attempt["used"] = True
                return found, attempts
    return None, attempts


async def _locate_date_picker_popup(frame: FrameLocator, *, timeout_ms: int) -> Locator | None:
    popup_candidates = [
        frame.locator("[role='dialog'][data-state='open']:has(.k-calendar)"),
        frame.locator(".k-animation-container:has(.k-calendar)"),
        frame.locator(".k-daterangepicker-popup"),
        frame.locator(".k-popup:has(.k-calendar)"),
        frame.get_by_role("dialog"),
    ]
    return await _first_visible_locator(popup_candidates, timeout_ms=timeout_ms)


async def _select_date_in_open_picker(container: Locator, *, target_date: date, label: str) -> tuple[bool, dict[str, Any]]:
    details: dict[str, Any] = {"label": label, "target_date": target_date.isoformat()}
    day_label = str(target_date.day)
    day_candidates = [
        container.get_by_role("gridcell", name=re.compile(rf"^{day_label}\b")),
        container.get_by_role("button", name=re.compile(rf"^{day_label}\b")),
        container.locator(f"text={day_label}"),
    ]
    day_locator = await _first_visible_locator(day_candidates, timeout_ms=3_000)
    if day_locator is None:
        details["error"] = "day_not_found"
        return False, details

    try:
        await day_locator.click()
        details["clicked"] = True
    except Exception as exc:
        details["error"] = f"day_click_failed:{exc}"
        return False, details

    return True, details


async def _wait_for_range_text_update(
    control: Locator,
    *,
    expected_texts: Sequence[str],
    timeout_ms: int,
    logger: JsonLogger,
    store: TdStore,
) -> tuple[bool, str | None]:
    deadline = asyncio.get_event_loop().time() + (timeout_ms / 1000)
    last_text: str | None = None
    normalized_expected = {" ".join(text.split()).lower() for text in expected_texts}

    while asyncio.get_event_loop().time() < deadline:
        try:
            current_text = await control.inner_text()
            last_text = current_text
        except Exception as exc:
            last_text = f"<unreadable:{exc}>"
            await asyncio.sleep(0.3)
            continue

        normalized_current = " ".join((current_text or "").split()).lower()
        if normalized_current in normalized_expected:
            return True, current_text

        await asyncio.sleep(0.3)

    log_event(
        logger=logger,
        phase="iframe",
        status="warn",
        message="Date range text did not update to expected value",
        store_code=store.store_code,
        expected_texts=list(expected_texts),
        final_text=last_text,
    )
    return False, last_text


async def _locate_date_inputs(
    frame: FrameLocator,
    *,
    timeout_ms: int,
    logger: JsonLogger,
    store: TdStore,
    popup: Locator | None = None,
) -> tuple[Locator | None, Locator | None, list[dict[str, Any]]]:
    from_input: Locator | None = None
    to_input: Locator | None = None
    search_timeout = min(timeout_ms, 10_000)
    attempts: list[dict[str, Any]] = []

    cue_observations = await _wait_for_date_ui(frame, timeout_ms=search_timeout)
    log_event(
        logger=logger,
        phase="iframe",
        message="Observed date UI cues",
        store_code=store.store_code,
        date_ui_cues=cue_observations,
    )

    container = popup or frame

    try:
        date_inputs = container.locator("input[type='number'], input[inputmode='numeric'], input[type='tel']")
        numeric_count = await date_inputs.count()
    except Exception:
        date_inputs = container.locator("input[type='number'], input[inputmode='numeric'], input[type='tel']")
        numeric_count = 0

    attempts.append(
        {
            "label": "numeric_date_inputs",
            "selector": "input[type=number|inputmode=numeric|type=tel]",
            "count": numeric_count,
            "used": False,
        }
    )
    if numeric_count >= 2:
        attempts[-1]["used"] = True
        return date_inputs.nth(0), date_inputs.nth(1), attempts
    if numeric_count == 1:
        from_input = date_inputs.first

    try:
        date_inputs = container.locator("input[type='date']")
        date_input_count = await date_inputs.count()
    except Exception:
        date_input_count = 0
        date_inputs = container.locator("input[type='date']")
    attempts.append(
        {"label": "input[type=date]", "selector": "input[type='date']", "count": date_input_count, "used": False}
    )

    if date_input_count >= 2:
        attempts[-1]["used"] = True
        return date_inputs.nth(0), date_inputs.nth(1), attempts
    if date_input_count == 1:
        from_input = date_inputs.first

    section_inputs = container.locator("section:has-text(\"Select Date\") input")
    try:
        section_input_count = await section_inputs.count()
    except Exception:
        section_input_count = 0

    attempts.append(
        {
            "label": "section_select_date_inputs",
            "selector": "section:has-text(\"Select Date\") input",
            "count": section_input_count,
            "used": False,
        }
    )

    if section_input_count and from_input is None:
        from_input = section_inputs.nth(0)
    if section_input_count >= 2 and to_input is None:
        to_input = section_inputs.nth(1)
    if section_input_count:
        attempts[-1]["used"] = True

    # Role/text based fallbacks
    try:
        from_textbox = container.get_by_role("textbox", name=re.compile("from", re.I))
        from_textbox_count = await from_textbox.count()
    except Exception:
        from_textbox = container.get_by_role("textbox", name=re.compile("from", re.I))
        from_textbox_count = 0

    attempts.append(
        {"label": "textbox_from_accessible_name", "selector": "role=textbox name=from", "count": from_textbox_count, "used": False}
    )
    if from_input is None and from_textbox_count:
        from_input = await _first_visible_locator([from_textbox], timeout_ms=search_timeout)
        attempts[-1]["used"] = from_input is not None

    try:
        to_textbox = container.get_by_role("textbox", name=re.compile("to", re.I))
        to_textbox_count = await to_textbox.count()
    except Exception:
        to_textbox = container.get_by_role("textbox", name=re.compile("to", re.I))
        to_textbox_count = 0

    attempts.append(
        {"label": "textbox_to_accessible_name", "selector": "role=textbox name=to", "count": to_textbox_count, "used": False}
    )
    if to_input is None and to_textbox_count:
        to_input = await _first_visible_locator([to_textbox], timeout_ms=search_timeout)
        attempts[-1]["used"] = to_input is not None

    adjacent_select_date = container.locator(
        "label:has-text(\"Select Date\") ~ input, label:has-text(\"Select Date\") + input"
    )
    try:
        adjacent_count = await adjacent_select_date.count()
    except Exception:
        adjacent_count = 0
    attempts.append(
        {
            "label": "inputs_adjacent_select_date_label",
            "selector": "label:has-text(\"Select Date\") ~ input",
            "count": adjacent_count,
            "used": False,
        }
    )
    if adjacent_count:
        if from_input is None:
            from_input = adjacent_select_date.nth(0)
        if adjacent_count >= 2 and to_input is None:
            to_input = adjacent_select_date.nth(1)
        attempts[-1]["used"] = True

    # Legacy placeholder/aria/name fallbacks
    if from_input is None:
        from_candidates = [
            container.get_by_label(re.compile("from", re.I)),
            container.get_by_placeholder(re.compile("from", re.I)),
            container.locator("input[aria-label*='from' i]"),
            container.locator("input[name*='from' i]"),
        ]
        from_input = await _first_visible_locator(from_candidates, timeout_ms=search_timeout)
        attempts.append({"label": "from_fallbacks", "selector": "label/placeholder/aria/name from", "used": from_input is not None})

    if to_input is None:
        to_candidates = [
            container.get_by_label(re.compile(r"to", re.I)),
            container.get_by_placeholder(re.compile(r"to", re.I)),
            container.locator("input[aria-label*='to' i]"),
            container.locator("input[name*='to' i]"),
        ]
        to_input = await _first_visible_locator(to_candidates, timeout_ms=search_timeout)
        attempts.append({"label": "to_fallbacks", "selector": "label/placeholder/aria/name to", "used": to_input is not None})

    if to_input is None and date_input_count == 1:
        to_input = date_inputs.first

    return from_input, to_input, attempts


async def _fill_date_via_picker(
    container: FrameLocator | Locator, locator: Locator, *, target_date: date, label: str
) -> tuple[bool, dict[str, Any]]:
    details: dict[str, Any] = {"strategy": "picker", "opened": False, "selected_day": False}
    try:
        await locator.click()
        details["opened"] = True
    except Exception as exc:
        details["error"] = f"click_failed:{exc}"
        return False, details

    calendar_candidates = [
        container.locator(".k-animation-container .k-calendar"),
        container.locator(".k-calendar"),
    ]
    if isinstance(container, FrameLocator):
        calendar_candidates.append(container.get_by_role("grid", name=re.compile("calendar", re.I)))
    else:
        calendar_candidates.append(container.locator("[role='grid']:has-text('calendar')"))

    calendar = await _first_visible_locator(calendar_candidates, timeout_ms=4_000)
    if calendar is None:
        details["error"] = "calendar_not_visible"
        return False, details

    day_label = str(target_date.day)
    day_candidates = [
        calendar.get_by_role("gridcell", name=re.compile(rf"^{day_label}$")),
        calendar.get_by_role("button", name=re.compile(rf"^{day_label}$")),
        calendar.locator(f"text={day_label}"),
    ]
    day_locator = await _first_visible_locator(day_candidates, timeout_ms=3_000)
    if day_locator is None:
        details["error"] = "day_not_found"
        return False, details

    try:
        await day_locator.click()
        details["selected_day"] = True
    except Exception as exc:
        details["error"] = f"day_click_failed:{exc}"
        return False, details

    return True, details


async def _fill_numeric_triplet(
    fields: list[Locator],
    *,
    target_date: date,
    label: str,
    logger: JsonLogger,
    store: TdStore,
) -> tuple[bool, dict[str, Any]]:
    detail: dict[str, Any] = {
        "label": label,
        "strategy": "numeric_triplet",
        "field_count": len(fields),
        "values": {"day": f"{target_date.day:02d}", "month": f"{target_date.month:02d}", "year": f"{target_date.year:04d}"},
        "fills": [],
    }
    if len(fields) < 3:
        detail["error"] = "insufficient_fields"
        return False, detail

    success = True
    for idx, (part_label, value) in enumerate(
        (("day", f"{target_date.day:02d}"), ("month", f"{target_date.month:02d}"), ("year", f"{target_date.year:04d}"))
    ):
        field = fields[idx]
        entry: dict[str, Any] = {"part": part_label, "value": value}
        try:
            await field.click()
            await field.fill(value)
            entry["filled"] = True
        except Exception as exc:
            entry["error"] = str(exc)
            success = False
        detail["fills"].append(entry)

    if not success:
        log_event(
            logger=logger,
            phase="iframe",
            status="warn",
            message="Failed to fill numeric date inputs",
            store_code=store.store_code,
            label=label,
            detail=detail,
        )
    return success, detail


async def _fill_date_input(
    container: FrameLocator | Locator,
    locator: Locator,
    *,
    target_date: date,
    label: str,
    logger: JsonLogger,
    store: TdStore,
) -> tuple[bool, dict[str, Any]]:
    attr_type = ((await locator.get_attribute("type")) or "").lower()
    attr_inputmode = ((await locator.get_attribute("inputmode")) or "").lower()
    value_formats: list[str] = []
    if attr_type == "date":
        value_formats.append("%Y-%m-%d")
    elif attr_type == "number" or attr_inputmode == "numeric":
        value_formats.extend(["%d/%m/%Y", "%d%m%Y", "%Y%m%d"])
    else:
        value_formats.extend(["%d %b %Y", "%d/%m/%Y"])
    value = target_date.strftime(value_formats[0])
    read_only_attr = await locator.get_attribute("readonly")
    details: dict[str, Any] = {
        "label": label,
        "input_type": attr_type,
        "inputmode": attr_inputmode,
        "value": value,
        "readonly_attr": read_only_attr,
        "strategies": [],
    }

    try:
        await locator.click()
    except Exception as exc:
        details["strategies"].append({"name": "click", "error": str(exc)})

    direct_success = False
    for fmt in value_formats:
        value_attempt = target_date.strftime(fmt)
        try:
            await locator.fill(value_attempt)
            direct_success = True
            details["strategies"].append({"name": "direct_fill", "success": True, "value": value_attempt})
            return True, details
        except Exception as exc:
            details["strategies"].append({"name": "direct_fill", "error": str(exc), "value": value_attempt})

    picker_result = None
    if read_only_attr is not None or not direct_success:
        picker_ok, picker_details = await _fill_date_via_picker(
            container, locator, target_date=target_date, label=label
        )
        picker_result = picker_details
        picker_details["success"] = picker_ok
        details["strategies"].append(picker_details)
        if picker_ok:
            return True, details

    try:
        await locator.evaluate(
            "(el, value) => { if (el.readOnly) el.readOnly = false; el.value = value; el.dispatchEvent(new Event('input', { bubbles: true })); el.dispatchEvent(new Event('change', { bubbles: true })); }",
            value,
        )
        details["strategies"].append({"name": "js_fill", "success": True})
        return True, details
    except Exception as exc:
        details["strategies"].append({"name": "js_fill", "error": str(exc), "picker_result": picker_result})

    log_event(
        logger=logger,
        phase="iframe",
        status="warn",
        message="Failed to fill date input",
        store_code=store.store_code,
        label=label,
        details=details,
    )
    return False, details


async def _set_date_range(
    frame: FrameLocator,
    *,
    from_date: date,
    to_date: date,
    logger: JsonLogger,
    store: TdStore,
    timeout_ms: int,
    attempt: int = 1,
) -> tuple[bool, str | None]:
    final_range_text: str | None = None
    range_control, control_attempts = await _locate_date_range_control(frame, timeout_ms=timeout_ms)
    if range_control is None:
        log_event(
            logger=logger,
            phase="iframe",
            status="warn",
            message="Date range control not located inside iframe",
            store_code=store.store_code,
            control_attempts=control_attempts,
            attempt=attempt,
        )
        return False, final_range_text

    try:
        await range_control.wait_for(state="visible", timeout=timeout_ms)
        await range_control.click()
    except Exception as exc:
        log_event(
            logger=logger,
            phase="iframe",
            status="warn",
            message="Failed to open date range picker",
            store_code=store.store_code,
            error=str(exc),
            control_attempts=control_attempts,
            attempt=attempt,
        )
        return False, final_range_text

    picker_popup = await _locate_date_picker_popup(frame, timeout_ms=min(timeout_ms, 8_000))
    if picker_popup is not None:
        with contextlib.suppress(Exception):
            await picker_popup.wait_for(state="visible", timeout=timeout_ms)
    else:
        log_event(
            logger=logger,
            phase="iframe",
            status="warn",
            message="Date picker popup not visible after clicking control",
            store_code=store.store_code,
            control_attempts=control_attempts,
            attempt=attempt,
        )
        return False, final_range_text

    try:
        numeric_inputs = picker_popup.locator("input[type='number'], input[inputmode='numeric'], input[type='tel']")
        numeric_count = await numeric_inputs.count()
    except Exception:
        numeric_inputs = picker_popup.locator("input[type='number'], input[inputmode='numeric'], input[type='tel']")
        numeric_count = 0

    use_numeric_triplets = numeric_count >= 6
    from_input = to_input = None
    from_ok = to_ok = False
    update_click_result: dict[str, Any] | None = None

    if use_numeric_triplets:
        from_fields = [numeric_inputs.nth(idx) for idx in range(3)]
        to_fields = [numeric_inputs.nth(idx) for idx in range(3, 6)]
        from_ok, _ = await _fill_numeric_triplet(
            from_fields, target_date=from_date, label="from", logger=logger, store=store
        )
        to_ok, _ = await _fill_numeric_triplet(
            to_fields, target_date=to_date, label="to", logger=logger, store=store
        )
    else:
        from_input, to_input, _ = await _locate_date_inputs(
            frame, timeout_ms=timeout_ms, logger=logger, store=store, popup=picker_popup
        )
        if from_input and to_input:
            from_ok, _ = await _fill_date_input(
                picker_popup, from_input, target_date=from_date, label="from", logger=logger, store=store
            )
            to_ok, _ = await _fill_date_input(
                picker_popup, to_input, target_date=to_date, label="to", logger=logger, store=store
            )
        else:
            from_ok, _ = await _select_date_in_open_picker(picker_popup, target_date=from_date, label="from")
            to_ok, _ = await _select_date_in_open_picker(picker_popup, target_date=to_date, label="to")

    update_button = await _first_visible_locator(
        [
            picker_popup.get_by_role("button", name=re.compile("^update$", re.I)),
            picker_popup.locator("button:has-text(\"UPDATE\"), [role='button']:has-text(\"UPDATE\")"),
        ],
        timeout_ms=min(timeout_ms, 4_000),
    )
    if update_button is not None:
        update_click_result = {"found": True}
        try:
            await update_button.click()
            update_click_result["clicked"] = True
            await _wait_for_loading_indicators(
                frame, store=store, logger=logger, timeout_ms=min(timeout_ms, 10_000), phase="iframe_range_update"
            )
        except Exception as exc:
            update_click_result["error"] = str(exc)
    else:
        update_click_result = {"found": False}

    expected_range_texts = _format_report_range_text_candidates(from_date, to_date)
    range_text_ok, final_range_text = await _wait_for_range_text_update(
        range_control,
        expected_texts=expected_range_texts,
        timeout_ms=min(timeout_ms, 10_000),
        logger=logger,
        store=store,
    )

    update_ok = update_click_result is None or update_click_result.get("clicked") or not update_click_result.get("found")

    if from_ok and to_ok and range_text_ok and update_ok:
        log_event(
            logger=logger,
            phase="iframe",
            message="Date range set via date-range control",
            store_code=store.store_code,
            from_value=from_date.isoformat(),
            to_value=to_date.isoformat(),
            numeric_input_count=numeric_count,
            used_numeric_triplets=use_numeric_triplets,
            range_text=final_range_text,
            update_click=update_click_result,
            attempt=attempt,
        )
        return True, final_range_text

    log_event(
        logger=logger,
        phase="iframe",
        status="warn",
        message="Failed to set date range via date-range control",
        store_code=store.store_code,
        from_ok=from_ok,
        to_ok=to_ok,
        numeric_input_count=numeric_count,
        used_numeric_triplets=use_numeric_triplets,
        range_text=final_range_text,
        update_click=update_click_result,
        attempt=attempt,
    )
    return False, final_range_text


async def _set_date_range_with_retry(
    frame: FrameLocator,
    *,
    from_date: date,
    to_date: date,
    logger: JsonLogger,
    store: TdStore,
    timeout_ms: int,
    attempts: int = 2,
) -> tuple[bool, str | None]:
    final_text: str | None = None
    for attempt in range(1, max(1, attempts) + 1):
        success, range_text = await _set_date_range(
            frame,
            from_date=from_date,
            to_date=to_date,
            logger=logger,
            store=store,
            timeout_ms=timeout_ms,
            attempt=attempt,
        )
        final_text = range_text
        if success:
            return True, range_text
        await asyncio.sleep(0.5)
    return False, final_text


async def _extract_row_status_text(row: Locator) -> str | None:
    try:
        cell_texts = await row.locator("td").all_inner_texts()
        normalized_cells = [" ".join(text.split()) for text in cell_texts if text]
        for text in normalized_cells:
            lowered = text.lower()
            if any(keyword in lowered for keyword in ("pending", "download", "ready", "processing")):
                return text
        if normalized_cells:
            return normalized_cells[-1]
    except Exception:
        pass
    with contextlib.suppress(Exception):
        text = await row.inner_text()
        if text:
            return " ".join(text.split())
    return None


def _parse_eta_seconds(status_text: str | None) -> int | None:
    if not status_text:
        return None
    lowered = status_text.lower()
    match = re.search(r"(\d+)\s*h(?=\D|$)", lowered)
    hours = int(match.group(1)) * 3600 if match else 0
    match = re.search(r"(\d+)\s*m(?=\D|$)", lowered)
    minutes = int(match.group(1)) * 60 if match else 0
    match = re.search(r"(\d+)\s*s(?=\D|$)", lowered)
    seconds = int(match.group(1)) if match else 0
    total = hours + minutes + seconds
    if total > 0:
        return total
    match = re.search(r"eta\s*[:\-]?\s*(\d+)", lowered)
    if match:
        return int(match.group(1))
    return None


async def _capture_outer_html(locator: Locator) -> str | None:
    try:
        return await locator.evaluate("(el) => el.outerHTML")
    except Exception:
        return None


async def _capture_body_inner_html(frame: FrameLocator) -> str | None:
    try:
        return await frame.locator("body").first.evaluate("(el) => el.innerHTML")
    except Exception:
        return None


async def _log_iframe_body_dom(
    frame: FrameLocator,
    *,
    logger: JsonLogger,
    store: TdStore,
    label: str = "iframe_body_after_modal",
    capture_html: bool = False,
) -> bool:
    body_html: str | None = None
    if capture_html:
        body_html = await _capture_body_inner_html(frame)

    log_event(
        logger=logger,
        phase="iframe",
        message="Iframe body log executed",
        store_code=store.store_code,
        label=label,
        body_log_attempted=True,
        body_html_captured=bool(body_html) if capture_html else False,
        body_html_length=len(body_html) if capture_html and body_html else None,
    )
    return True


async def _wait_for_modal_close(
    frame: FrameLocator,
    *,
    logger: JsonLogger,
    store: TdStore,
    timeout_ms: int,
) -> None:
    wait_timeout_ms = min(timeout_ms, 5_000)
    start_time = asyncio.get_event_loop().time()
    modal_candidates = [
        frame.locator(".k-window"),
        frame.locator("[role='dialog']"),
    ]
    overlay_candidates = [
        frame.locator(".k-overlay"),
        frame.locator(".k-animation-container"),
    ]
    deadline = asyncio.get_event_loop().time() + (wait_timeout_ms / 1000)
    saw_modal = False

    while asyncio.get_event_loop().time() < deadline:
        modal_visible = False
        overlay_visible = False
        for candidate in modal_candidates:
            try:
                if await candidate.count() and await candidate.first.is_visible():
                    modal_visible = True
                    break
            except Exception:
                continue
        for candidate in overlay_candidates:
            try:
                if await candidate.count() and await candidate.first.is_visible():
                    overlay_visible = True
                    break
            except Exception:
                continue

        if modal_visible or overlay_visible:
            saw_modal = saw_modal or modal_visible or overlay_visible
            await asyncio.sleep(0.3)
            continue
        break

    if saw_modal:
        elapsed_ms = int((asyncio.get_event_loop().time() - start_time) * 1000)
        log_event(
            logger=logger,
            phase="iframe",
            message="Report modal closed after request",
            store_code=store.store_code,
            waited_ms=elapsed_ms,
        )


async def _collect_report_request_rows(
    container: Locator,
    *,
    max_rows: int = 20,
) -> list[Locator]:
    rows: list[Locator] = []
    seen_handles: set[int] = set()
    candidates = [
        container.locator(":scope > section > div.flex.items-start.justify-between"),
        container.locator(":scope > section > div.border-b.border-gray-100"),
        container.locator(":scope > section > div"),
        container.locator(":scope > div.flex.items-start.justify-between"),
        container.locator(":scope > div.border-b.border-gray-100"),
        container.locator(":scope > div"),
    ]
    if ENABLE_LEGACY_REPORT_REQUEST_ROW_LOCATORS:
        candidates.extend(
            [
                container.locator(":scope > div.flex.items-start.justify-between"),
                container.locator(":scope > div.border-b.border-gray-100"),
                container.locator(":scope > div"),
                container.locator(":scope > section"),
                container.locator(":scope > article"),
                container.locator(":scope > ul > li"),
                container.locator(":scope li"),
            ]
        )

    for candidate in candidates:
        try:
            count = await candidate.count()
        except Exception:
            continue
        for idx in range(min(count, max_rows)):
            row_locator = candidate.nth(idx)
            try:
                handle = await row_locator.element_handle()
            except Exception:
                handle = None
            if handle is None:
                continue
            key = id(handle)
            if key in seen_handles:
                continue
            seen_handles.add(key)
            with contextlib.suppress(Exception):
                if not await row_locator.is_visible():
                    continue
            rows.append(row_locator)
    return rows


def _summarize_row_texts(row_texts: Sequence[str], *, max_range_examples: int = 3) -> dict[str, Any]:
    range_pattern = re.compile(
        r"\b(?:\d{1,2}\s+\w{3,9}\s+\d{4}|\w{3,9}\s+\d{1,2},?\s+\d{4})\s*-\s*(?:\d{1,2}\s+\w{3,9}\s+\d{4}|\w{3,9}\s+\d{1,2},?\s+\d{4})",
        re.IGNORECASE,
    )

    matched_range_examples: list[str] = []
    seen_ranges: set[str] = set()
    for text in row_texts:
        candidate = text
        match = range_pattern.search(text)
        if match:
            candidate = " ".join(match.group(0).split())
        if candidate in seen_ranges:
            continue
        seen_ranges.add(candidate)
        matched_range_examples.append(candidate)
        if len(matched_range_examples) >= max_range_examples:
            break

    return {
        "row_count": len(row_texts),
        "first_row_text": row_texts[0] if row_texts else None,
        "matched_range_examples": matched_range_examples or None,
    }


async def _wait_for_report_requests_container(
    frame: FrameLocator,
    *,
    logger: JsonLogger,
    store: TdStore,
    timeout_ms: int,
) -> tuple[Locator | None, dict[str, Any]]:
    deadline = asyncio.get_event_loop().time() + (timeout_ms / 1000)
    last_seen_summary: dict[str, Any] | None = None
    header_seen = False
    heading_texts: list[str] = []
    diagnostics: dict[str, Any] = {}
    container_strategy_seen: str | None = None
    strategies_logged = False
    container_label = STABLE_LOCATOR_STRATEGIES["container_locator_strategy"]

    while asyncio.get_event_loop().time() < deadline:
        try:
            heading = await _first_visible_locator(
                [
                    frame.get_by_role("heading", name=re.compile("report requests", re.I)),
                    frame.locator("text=/Report Requests/i"),
                ],
                timeout_ms=1_200,
            )
            if heading:
                header_seen = True
                with contextlib.suppress(Exception):
                    await heading.scroll_into_view_if_needed()
                with contextlib.suppress(Exception):
                    text = await heading.inner_text()
                if text:
                    heading_texts.append(" ".join(text.split()))
            if heading:
                preferred_wrapper = heading.locator("xpath=ancestor::div[contains(@class,'py-10')][1]")
                container_candidates = [(container_label, preferred_wrapper)]
            else:
                container_candidates = []

            container, container_strategy = await _first_visible_locator_with_label(container_candidates, timeout_ms=1_000)
            if container:
                if container_strategy and not container_strategy_seen:
                    container_strategy_seen = container_strategy
                    log_event(
                        logger=logger,
                        phase="iframe",
                        message="Report Requests container located",
                        store_code=store.store_code,
                        container_locator_strategy=container_strategy_seen,
                    )
                    if not strategies_logged:
                        log_event(
                            logger=logger,
                            phase="iframe",
                            message="Using TD orders locator strategies",
                            store_code=store.store_code,
                            **STABLE_LOCATOR_STRATEGIES,
                        )
                    strategies_logged = True
                with contextlib.suppress(Exception):
                    await container.scroll_into_view_if_needed()
                rows = await _collect_report_request_rows(
                    container,
                    max_rows=12,
                )
                sample: list[str] = []
                for row in rows:
                    with contextlib.suppress(Exception):
                        sample_text = await row.inner_text()
                        if sample_text:
                            sample.append(" ".join(sample_text.split()))
                if sample:
                    summary = _summarize_row_texts(sample[:10])
                    last_seen_summary = summary
                    log_event(
                        logger=logger,
                        phase="iframe",
                        message="Observed Report Requests rows",
                        store_code=store.store_code,
                        row_count=summary["row_count"],
                        first_row_text=summary["first_row_text"],
                        matched_range_examples=summary["matched_range_examples"],
                        container_locator_strategy=container_strategy,
                    )
                diagnostics = {
                    "header_seen": header_seen,
                    "heading_texts": heading_texts or None,
                    "last_seen_rows_summary": last_seen_summary,
                    "container_locator_strategy": container_strategy,
                }
                return container, diagnostics
        except Exception:
            pass

        await asyncio.sleep(0.4)

    diagnostics = {
        "header_seen": header_seen,
        "heading_texts": heading_texts or None,
        "last_seen_rows_summary": last_seen_summary,
    }
    log_event(
        logger=logger,
        phase="iframe",
        status="warn",
        message="Report Requests container not visible after request",
        store_code=store.store_code,
        timeout_ms=timeout_ms,
        last_seen_rows_summary=last_seen_summary,
        header_seen=header_seen,
        heading_texts=heading_texts or None,
        container_locator_strategy=container_strategy_seen,
    )
    return None, diagnostics


async def _log_report_requests_dom(
    container: Locator,
    *,
    logger: JsonLogger,
    store: TdStore,
    label: str = "report_requests_container_dom",
) -> None:
    row_count: int | None = None
    try:
        rows = await _collect_report_request_rows(container, max_rows=20)
        row_count = len(rows)
    except Exception:
        row_count = None
    log_event(
        logger=logger,
        phase="iframe",
        message="Captured Report Requests container structure",
        store_code=store.store_code,
        label=label,
        row_count=row_count,
        container_locator_strategy=STABLE_LOCATOR_STRATEGIES["container_locator_strategy"],
    )
    return None


async def _locate_report_request_download(row: Locator) -> tuple[Locator | None, str | None]:
    download_candidates: list[tuple[str, Locator]] = [
        ("href_contains_order_reports", row.locator(":scope a[href*='order-reports']")),
        ("href_contains_sales_reports", row.locator(":scope a[href*='sales']")),
        ("link_role_download", row.get_by_role("link", name=re.compile("download", re.I))),
        ("has_text_download", row.locator(':scope a:has-text("Download")')),
        ("href_contains_report", row.locator(":scope a[href*='report']")),
    ]
    return await _first_visible_locator_with_label(download_candidates, timeout_ms=1_200)


async def _wait_for_report_request_download_link(
    frame: FrameLocator,
    page: Page,
    container: Locator,
    *,
    expected_range_texts: Sequence[str],
    range_patterns: Sequence[re.Pattern[str]],
    logger: JsonLogger,
    store: TdStore,
    timeout_ms: int,
    download_path: Path,
    download_wait_timeout_ms: int,
) -> tuple[bool, str | None, str | None, str | None]:
    start_time = asyncio.get_event_loop().time()
    poll_timeout_ms = min(REPORT_REQUEST_MAX_TIMEOUT_MS, timeout_ms)
    deadline = start_time + (poll_timeout_ms / 1000)
    last_seen_texts: list[str] = []
    last_seen_summary: dict[str, Any] | None = None
    last_status: str | None = None
    backoff = 0.5
    matched_row_seen = False
    last_range_match_strategy: str | None = STABLE_LOCATOR_STRATEGIES["range_match_strategy"]
    last_pending_log_at = 0.0
    pending_attempts = 0
    download_strategy: str | None = None
    last_refresh_attempt_at = 0.0
    selected_row_state: str | None = None
    selection_source: str | None = None
    last_matched_range_text: str | None = None
    last_download_locator_strategy: str | None = None

    while asyncio.get_event_loop().time() < deadline:
        try:
            with contextlib.suppress(Exception):
                await container.scroll_into_view_if_needed()
            rows = await _collect_report_request_rows(container, max_rows=25)
            visible_rows: list[str] = []
            matched_candidates: list[dict[str, Any]] = []
            for row_index, row in enumerate(rows):
                date_range_text_raw = None
                with contextlib.suppress(Exception):
                    date_range_text_raw = await row.locator("div.w-1\\/5.text-sm.italic").first.inner_text()
                if not date_range_text_raw:
                    continue
                date_range_text = " ".join(date_range_text_raw.split())
                visible_rows.append(date_range_text)
                matches_expected = any(pattern.search(date_range_text) for pattern in range_patterns)
                if matches_expected:
                    status_text = await _extract_row_status_text(row)
                    download_locator, strategy = await _locate_report_request_download(row)
                    matched_candidates.append(
                        {
                            "index": row_index,
                            "row": row,
                            "range_text": date_range_text,
                            "status_text": status_text,
                            "download_locator": download_locator,
                            "download_strategy": strategy,
                        }
                    )

            if matched_candidates:
                matched_row_seen = True
                last_seen_texts = visible_rows or [matched_candidates[0]["range_text"]]
                sorted_candidates = sorted(matched_candidates, key=lambda candidate: candidate["index"])
                downloadable_candidates = [candidate for candidate in sorted_candidates if candidate["download_locator"] is not None]
                pending_candidates = [
                    candidate
                    for candidate in sorted_candidates
                    if candidate.get("status_text") and "pending" in candidate["status_text"].lower()
                ]
                selected_candidate: dict[str, Any] | None = None
                if downloadable_candidates:
                    selected_candidate = downloadable_candidates[0]
                    matched_row_state = "downloadable"
                    selection_source = "downloadable_row"
                elif pending_candidates:
                    selected_candidate = pending_candidates[0]
                    matched_row_state = "pending"
                    selection_source = "pending_row"
                else:
                    selected_candidate = sorted_candidates[0]
                    matched_row_state = "unknown"
                    selection_source = "first_match_row"

                matched_text = selected_candidate["range_text"]
                matched_status = selected_candidate.get("status_text")
                last_status = matched_status or last_status
                download_locator = selected_candidate.get("download_locator")
                download_strategy = selected_candidate.get("download_strategy") or download_strategy
                selected_row_state = matched_row_state
                last_matched_range_text = matched_text or last_matched_range_text
                last_download_locator_strategy = download_strategy or last_download_locator_strategy
                last_seen_summary = _summarize_text_samples(visible_rows) if visible_rows else None

                log_event(
                    logger=logger,
                    phase="iframe",
                    message="Selected Report Requests row for follow-up",
                    store_code=store.store_code,
                    matched_range_text=matched_text,
                    status_text=matched_status,
                    expected_range_texts=list(expected_range_texts),
                    row_count=last_seen_summary["count"] if last_seen_summary else None,
                    row_samples=last_seen_summary["samples"] if last_seen_summary else None,
                    rows_truncated=last_seen_summary["truncated"] if last_seen_summary else None,
                    range_match_strategy=last_range_match_strategy,
                    download_locator_strategy=download_strategy,
                    container_locator_strategy=STABLE_LOCATOR_STRATEGIES["container_locator_strategy"],
                    selected_row_state=matched_row_state,
                    selection_source=selection_source,
                )

                if matched_status:
                    eta_seconds = _parse_eta_seconds(matched_status)
                    pending_eta_seconds = None
                    if "pending" in matched_status.lower():
                        pending_eta_seconds = max(eta_seconds or 0, PENDING_MIN_POLL_SECONDS)
                        desired_deadline = start_time + pending_eta_seconds
                    else:
                        desired_deadline = start_time + eta_seconds if eta_seconds is not None else None
                    if desired_deadline and desired_deadline > deadline:
                        deadline = desired_deadline
                        poll_timeout_ms = int((deadline - start_time) * 1000)
                        log_event(
                            logger=logger,
                            phase="iframe",
                            message="Extended report request poll window to ETA",
                            store_code=store.store_code,
                            matched_range_text=matched_text,
                            eta_seconds=eta_seconds,
                            pending_eta_seconds=pending_eta_seconds,
                            new_timeout_ms=poll_timeout_ms,
                            range_match_strategy=last_range_match_strategy,
                        )

                if download_locator:
                    log_event(
                        logger=logger,
                        phase="iframe",
                        message="Report Requests row ready for download",
                        store_code=store.store_code,
                        matched_range_text=matched_text,
                        status_text=matched_status,
                        expected_range_texts=list(expected_range_texts),
                        row_count=last_seen_summary["count"] if last_seen_summary else None,
                        row_samples=last_seen_summary["samples"] if last_seen_summary else None,
                        rows_truncated=last_seen_summary["truncated"] if last_seen_summary else None,
                        download_control_visible=True,
                        matched_row_text_full=matched_text,
                        range_match_strategy=last_range_match_strategy,
                        download_locator_strategy=download_strategy,
                        container_locator_strategy=STABLE_LOCATOR_STRATEGIES["container_locator_strategy"],
                        selected_row_state=matched_row_state,
                        selection_source=selection_source,
                    )
                    try:
                        async with page.expect_download(timeout=download_wait_timeout_ms) as download_info:
                            await download_locator.click()
                        download = await download_info.value
                        await download.save_as(str(download_path))
                        log_event(
                            logger=logger,
                            phase="iframe",
                            message="Orders report download saved",
                            store_code=store.store_code,
                            download_path=str(download_path),
                            suggested_filename=download.suggested_filename,
                            matched_range_text=matched_text,
                            expected_range_texts=list(expected_range_texts),
                            download_locator_strategy=download_strategy,
                            range_match_strategy=last_range_match_strategy,
                            container_locator_strategy=STABLE_LOCATOR_STRATEGIES["container_locator_strategy"],
                            selected_row_state=matched_row_state,
                            selection_source=selection_source,
                        )
                        return True, str(download_path), matched_text, matched_status
                    except Exception as exc:
                        last_status = str(exc)
                        log_event(
                            logger=logger,
                            phase="iframe",
                            status="warn",
                            message="Failed to download report after locating link",
                            store_code=store.store_code,
                            matched_range_text=matched_text,
                            error=str(exc),
                            range_match_strategy=last_range_match_strategy,
                            download_locator_strategy=download_strategy,
                            selected_row_state=matched_row_state,
                            selection_source=selection_source,
                        )
                        return False, None, matched_text, last_status

                if matched_status and "pending" in matched_status.lower():
                    pending_attempts += 1
                    now = asyncio.get_event_loop().time()
                    if now - last_pending_log_at >= REPORT_REQUEST_POLL_LOG_INTERVAL_SECONDS or pending_attempts == 1:
                        pending_summary = last_seen_summary or (
                            _summarize_text_samples(visible_rows) if visible_rows else None
                        )
                        log_event(
                            logger=logger,
                            phase="iframe",
                            message="Report Requests row pending; retrying download poll",
                            store_code=store.store_code,
                            matched_range_text=matched_text,
                            status_text=matched_status,
                            expected_range_texts=list(expected_range_texts),
                            backoff_seconds=backoff,
                            timeout_ms=poll_timeout_ms,
                            last_seen_rows_count=pending_summary["count"] if pending_summary else None,
                            last_seen_rows_samples=pending_summary["samples"] if pending_summary else None,
                            last_seen_rows_truncated=pending_summary["truncated"] if pending_summary else None,
                            range_match_strategy=last_range_match_strategy,
                            container_locator_strategy=STABLE_LOCATOR_STRATEGIES["container_locator_strategy"],
                            download_locator_strategy=download_strategy,
                            selected_row_state=matched_row_state,
                            selection_source=selection_source,
                        )
                        last_pending_log_at = now

                    if now - last_refresh_attempt_at >= REPORT_REQUEST_REFRESH_INTERVAL_SECONDS:
                        refresh_locator = await _first_visible_locator(
                            [
                                container.get_by_role("button", name=re.compile("refresh", re.I)),
                                frame.get_by_role("button", name=re.compile("refresh", re.I)),
                                container.locator("text=/Refresh/i"),
                            ],
                            timeout_ms=1_200,
                        )
                        refresh_timestamp = datetime.now(timezone.utc).isoformat()
                        if refresh_locator:
                            try:
                                await refresh_locator.click()
                                log_event(
                                    logger=logger,
                                    phase="iframe",
                                    message="Triggered Report Requests refresh during pending state",
                                    store_code=store.store_code,
                                    matched_range_text=matched_text,
                                    status_text=matched_status,
                                    refresh_timestamp=refresh_timestamp,
                                    range_match_strategy=last_range_match_strategy,
                                    container_locator_strategy=STABLE_LOCATOR_STRATEGIES["container_locator_strategy"],
                                    selected_row_state=matched_row_state,
                                    selection_source=selection_source,
                                )
                            except Exception as exc:
                                log_event(
                                    logger=logger,
                                    phase="iframe",
                                    status="warn",
                                    message="Failed to click Refresh during pending state",
                                    store_code=store.store_code,
                                    matched_range_text=matched_text,
                                    status_text=matched_status,
                                    refresh_timestamp=refresh_timestamp,
                                    error=str(exc),
                                    range_match_strategy=last_range_match_strategy,
                                    container_locator_strategy=STABLE_LOCATOR_STRATEGIES["container_locator_strategy"],
                                    selected_row_state=matched_row_state,
                                    selection_source=selection_source,
                                )
                        else:
                            log_event(
                                logger=logger,
                                phase="iframe",
                                status="warn",
                                message="Refresh control not visible during pending state",
                                store_code=store.store_code,
                                matched_range_text=matched_text,
                                status_text=matched_status,
                                refresh_timestamp=refresh_timestamp,
                                range_match_strategy=last_range_match_strategy,
                                container_locator_strategy=STABLE_LOCATOR_STRATEGIES["container_locator_strategy"],
                                selected_row_state=matched_row_state,
                                selection_source=selection_source,
                            )
                        last_refresh_attempt_at = now
                else:
                    break
            if visible_rows:
                last_seen_texts = visible_rows
                last_seen_summary = _summarize_text_samples(visible_rows)
        except Exception as exc:
            last_status = last_status or str(exc)

        await asyncio.sleep(backoff)

    log_event(
        logger=logger,
        phase="iframe",
        status="warn",
        message="Report Requests download link not available before timeout",
        store_code=store.store_code,
        expected_range_texts=list(expected_range_texts),
        last_seen_rows_count=last_seen_summary["count"] if last_seen_summary else None,
        last_seen_rows_samples=last_seen_summary["samples"] if last_seen_summary else None,
        last_seen_rows_truncated=last_seen_summary["truncated"] if last_seen_summary else None,
        last_status=last_status,
        row_seen=matched_row_seen,
        timeout_ms=poll_timeout_ms,
        range_match_strategy=last_range_match_strategy,
        download_locator_strategy=last_download_locator_strategy or download_strategy,
        container_locator_strategy=STABLE_LOCATOR_STRATEGIES["container_locator_strategy"],
        selected_row_state=selected_row_state or ("pending" if matched_row_seen else None),
        matched_range_text=last_matched_range_text,
    )
    return False, None, last_matched_range_text, last_status


async def _run_report_iframe_flow(
    page: Page,
    frame: FrameLocator,
    *,
    store: TdStore,
    logger: JsonLogger,
    from_date: date,
    to_date: date,
    nav_timeout_ms: int,
    download_dir: Path,
    report_label: str,
    filename_builder: Callable[[str, date, date], str],
) -> tuple[bool, str | None]:
    await _wait_for_loading_indicators(
        frame, store=store, logger=logger, timeout_ms=nav_timeout_ms, phase="iframe_preflight"
    )

    expand_locator = await _first_visible_locator(
        [
            frame.get_by_role("button", name=re.compile("expand", re.I)),
            frame.locator("text=Expand"),
        ],
        timeout_ms=5_000,
    )
    if expand_locator:
        try:
            await expand_locator.click()
            await _wait_for_loading_indicators(
                frame, store=store, logger=logger, timeout_ms=nav_timeout_ms, phase="iframe_expand"
            )
            log_event(
                logger=logger,
                phase="iframe",
                message="Clicked Expand button inside iframe",
                store_code=store.store_code,
            )
        except Exception as exc:
            log_event(
                logger=logger,
                phase="iframe",
                status="warn",
                message="Failed to click Expand button",
                store_code=store.store_code,
                error=str(exc),
            )

    historical_locator = await _first_visible_locator(
        [
            frame.get_by_role("button", name=re.compile("download historical report", re.I)),
            frame.get_by_role("link", name=re.compile("download historical report", re.I)),
            frame.locator("text=Download Historical Report"),
        ],
        timeout_ms=nav_timeout_ms,
    )
    if not historical_locator:
        return False, f"{report_label.title()} Download Historical Report control not visible inside iframe"

    await historical_locator.click()
    await _wait_for_loading_indicators(
        frame, store=store, logger=logger, timeout_ms=nav_timeout_ms, phase="iframe_download_historical"
    )

    generate_locator = await _first_visible_locator(
        [
            frame.get_by_role("button", name=re.compile("generate report", re.I)),
            frame.locator("text=Generate Report"),
        ],
        timeout_ms=nav_timeout_ms,
    )
    if not generate_locator:
        return False, f"{report_label.title()} Generate Report control not visible inside iframe"

    await generate_locator.click()
    await _wait_for_loading_indicators(
        frame, store=store, logger=logger, timeout_ms=nav_timeout_ms, phase="iframe_generate_report"
    )

    date_range_set, observed_range_text = await _set_date_range_with_retry(
        frame,
        from_date=from_date,
        to_date=to_date,
        logger=logger,
        store=store,
        timeout_ms=nav_timeout_ms,
        attempts=2,
    )
    if not date_range_set:
        return False, "Date range selection failed"

    range_text_candidates = _format_report_range_text_candidates(from_date, to_date)
    if observed_range_text:
        range_text_candidates = [observed_range_text] + [text for text in range_text_candidates if text != observed_range_text]
    range_patterns = _build_date_range_patterns(from_date, to_date)

    request_locator = await _first_visible_locator(
        [
            frame.get_by_role("button", name=re.compile("request report", re.I)),
            frame.locator("text=Request Report"),
        ],
        timeout_ms=nav_timeout_ms,
    )
    if not request_locator:
        return False, f"{report_label.title()} Request Report control not visible inside iframe"

    await request_locator.click()
    await _wait_for_loading_indicators(
        frame, store=store, logger=logger, timeout_ms=nav_timeout_ms, phase="iframe_request_report"
    )

    await _wait_for_modal_close(frame, logger=logger, store=store, timeout_ms=nav_timeout_ms)
    with contextlib.suppress(Exception):
        await _log_iframe_body_dom(
            frame,
            logger=logger,
            store=store,
            label="iframe_body_after_modal_close",
        )
    container_locator, container_diagnostics = await _wait_for_report_requests_container(
        frame, logger=logger, store=store, timeout_ms=nav_timeout_ms
    )
    if container_locator is None:
        return False, "Report Requests list not visible after requesting report"

    with contextlib.suppress(Exception):
        await _log_report_requests_dom(
            container_locator,
            logger=logger,
            store=store,
            label="report_requests_after_request",
        )
    filename = filename_builder(store.store_code, from_date, to_date)
    target_path = download_dir / filename
    download_wait_timeout_ms = min(nav_timeout_ms, 8_000)

    downloaded, downloaded_path, matched_range_text, last_status = await _wait_for_report_request_download_link(
        frame,
        page,
        container_locator,
        expected_range_texts=range_text_candidates,
        range_patterns=range_patterns,
        logger=logger,
        store=store,
        timeout_ms=nav_timeout_ms,
        download_path=target_path,
        download_wait_timeout_ms=download_wait_timeout_ms,
    )
    if not downloaded:
        return False, last_status or f"Matching {report_label.title()} Report Requests row not ready for download"

    return True, downloaded_path or str(target_path)


async def _run_orders_iframe_flow(
    page: Page,
    frame: FrameLocator,
    *,
    store: TdStore,
    logger: JsonLogger,
    from_date: date,
    to_date: date,
    nav_timeout_ms: int,
    download_dir: Path,
) -> tuple[bool, str | None]:
    return await _run_report_iframe_flow(
        page,
        frame,
        store=store,
        logger=logger,
        from_date=from_date,
        to_date=to_date,
        nav_timeout_ms=nav_timeout_ms,
        download_dir=download_dir,
        report_label="orders",
        filename_builder=_format_orders_filename,
    )


async def _run_sales_iframe_flow(
    page: Page,
    frame: FrameLocator,
    *,
    store: TdStore,
    logger: JsonLogger,
    from_date: date,
    to_date: date,
    nav_timeout_ms: int,
    download_dir: Path,
) -> tuple[bool, str | None]:
    return await _run_report_iframe_flow(
        page,
        frame,
        store=store,
        logger=logger,
        from_date=from_date,
        to_date=to_date,
        nav_timeout_ms=nav_timeout_ms,
        download_dir=download_dir,
        report_label="sales",
        filename_builder=_format_sales_filename,
    )


async def _execute_sales_flow(
    page: Page,
    *,
    store: TdStore,
    logger: JsonLogger,
    from_date: date,
    to_date: date,
    nav_timeout_ms: int,
    download_dir: Path,
    run_id: str,
    run_date: datetime,
    summary: TdOrdersDiscoverySummary,
    sales_only_mode: bool = False,
) -> StoreReport:
    sales_status: str | None = None
    sales_message: str | None = None
    sales_download_path: str | None = None
    sales_ingest_result: TdSalesIngestResult | None = None
    sales_warning_summary: dict[str, Any] | None = None
    sales_counts_message: str | None = None

    sales_nav_ready = await _navigate_to_sales_report(
        page, store=store, logger=logger, nav_timeout_ms=nav_timeout_ms, sales_only_mode=sales_only_mode
    )
    if sales_nav_ready:
        sales_iframe = await _wait_for_iframe(page, store=store, logger=logger, require_src=True, phase="sales")
        if sales_iframe is not None:
            await _observe_iframe_hydration(sales_iframe, store=store, logger=logger, timeout_ms=nav_timeout_ms)
            sales_success, sales_detail = await _run_sales_iframe_flow(
                page,
                sales_iframe,
                store=store,
                logger=logger,
                from_date=from_date,
                to_date=to_date,
                nav_timeout_ms=nav_timeout_ms,
                download_dir=download_dir,
            )
            if sales_success:
                sales_status = "ok"
                sales_download_path = sales_detail
                log_event(
                    logger=logger,
                    phase="sales",
                    message="Sales & Delivery report downloaded",
                    store_code=store.store_code,
                    download_path=sales_detail,
                )
                if config.database_url:
                    if not store.cost_center:
                        log_event(
                            logger=logger,
                            phase="sales_ingest",
                            status="error",
                            message="Missing cost_center for TD store; cannot ingest sales",
                            store_code=store.store_code,
                        )
                        sales_status = "error"
                        sales_message = "Missing cost_center for TD store; cannot ingest sales"
                    else:
                        try:
                            sales_ingest_result = await ingest_td_sales_workbook(
                                workbook_path=Path(sales_detail),
                                store_code=store.store_code,
                                cost_center=store.cost_center or "",
                                run_id=run_id,
                                run_date=run_date,
                                database_url=config.database_url,
                                logger=logger,
                            )
                            sales_status = "ok"
                            sales_counts_message = (
                                f"staging={sales_ingest_result.staging_rows}, final={sales_ingest_result.final_rows}"
                            )
                            if sales_ingest_result.warnings:
                                sales_warning_summary = _summarize_warnings(sales_ingest_result.warnings)
                                log_event(
                                    logger=logger,
                                    phase="sales_ingest",
                                    status="warn",
                                    message="TD Sales workbook ingested with warnings",
                                    store_code=store.store_code,
                                    warning_count=sales_warning_summary["count"],
                                    warning_samples=sales_warning_summary["samples"],
                                    warnings_truncated=sales_warning_summary["truncated"],
                                    staging_rows=sales_ingest_result.staging_rows,
                                    final_rows=sales_ingest_result.final_rows,
                                )
                                warning_preview = _format_warning_preview(sales_warning_summary)
                                warning_message = (
                                    f"Sales ingested with warnings ({warning_preview})"
                                    if warning_preview
                                    else "Sales ingested with warnings"
                                )
                                if sales_counts_message:
                                    warning_message = f"{warning_message}; {sales_counts_message}"
                                sales_message = warning_message
                            else:
                                log_event(
                                    logger=logger,
                                    phase="sales_ingest",
                                    message="TD Sales workbook ingested",
                                    store_code=store.store_code,
                                    staging_rows=sales_ingest_result.staging_rows,
                                    final_rows=sales_ingest_result.final_rows,
                                )
                            summary.add_ingest_remarks(sales_ingest_result.ingest_remarks)
                        except Exception as exc:
                            log_event(
                                logger=logger,
                                phase="sales_ingest",
                                status="error",
                                message="TD Sales ingestion failed",
                                store_code=store.store_code,
                                error=str(exc),
                            )
                            sales_status = "error"
                            sales_message = f"Sales ingestion failed: {exc}"
                        else:
                            if sales_ingest_result and sales_message is None:
                                sales_message = (
                                    f"Sales ingested: staging={sales_ingest_result.staging_rows}, "
                                    f"final={sales_ingest_result.final_rows}"
                                )
                else:
                    log_event(
                        logger=logger,
                        phase="sales_ingest",
                        status="warn",
                        message="Skipping TD Sales ingestion because database_url is missing",
                        store_code=store.store_code,
                    )
                    sales_status = "warning"
                    sales_message = "Skipping TD Sales ingestion because database_url is missing"
            else:
                sales_status = "warning"
                sales_message = sales_detail or "Sales iframe flow failed"
                log_event(
                    logger=logger,
                    phase="sales",
                    status="warn",
                    message="Sales & Delivery iframe flow failed",
                    store_code=store.store_code,
                    error=sales_detail,
                )
        else:
            sales_status = "warning"
            sales_message = "Sales iframe did not attach"
            log_event(
                logger=logger,
                phase="sales",
                status="warn",
                message="Sales iframe not ready after navigation",
                store_code=store.store_code,
                final_url=page.url,
            )
    else:
        sales_status = "warning"
        sales_message = "Sales & Delivery report navigation failed"
        log_event(
            logger=logger,
            phase="sales",
            status="warn",
            message="Navigation to Sales & Delivery report did not complete",
            store_code=store.store_code,
            final_url=page.url,
        )

    if sales_status == "ok" and sales_message is None:
        sales_message = "Sales report downloaded"
        if sales_counts_message:
            sales_message = f"Sales ingested: {sales_counts_message}"
        elif sales_ingest_result:
            sales_message = (
                f"Sales ingested: staging={sales_ingest_result.staging_rows}, "
                f"final={sales_ingest_result.final_rows}"
            )
    elif sales_status == "warning" and sales_message is None:
        sales_message = "Sales completed with warnings"
    elif sales_status == "error" and sales_message is None:
        sales_message = "Sales sync failed"

    return StoreReport(
        status=sales_status or "error",
        filenames=[Path(sales_download_path).name] if sales_download_path else [],
        staging_rows=sales_ingest_result.staging_rows if sales_ingest_result else None,
        final_rows=sales_ingest_result.final_rows if sales_ingest_result else None,
        message=sales_message,
        error_message=sales_message if sales_status == "error" else None,
        warnings=sales_ingest_result.warnings if sales_ingest_result else [],
    )


async def _wait_for_otp_verification(
    page: Page,
    *,
    store: TdStore,
    logger: JsonLogger,
    nav_selector: str,
    dwell_seconds: int = OTP_VERIFICATION_DWELL_SECONDS,
) -> tuple[bool, bool]:
    verification_fragment = "/App/frmVerification"
    current_url = page.url or ""
    verification_seen = verification_fragment.lower() in current_url.lower()
    if not verification_seen:
        return True, False

    deadline = datetime.now(timezone.utc) + timedelta(seconds=dwell_seconds)
    log_event(
        logger=logger,
        phase="login",
        status="warn",
        message="Verification page detected; pausing for manual OTP entry",
        store_code=store.store_code,
        current_url=current_url,
        otp_deadline=deadline.isoformat(),
        dwell_seconds=dwell_seconds,
    )

    async def _home_ready() -> tuple[bool, str, bool, bool]:
        url_now = page.url or ""
        url_ok = _url_matches_home(url_now, store)
        nav_visible = False
        home_card_visible = False
        try:
            nav_visible = await page.locator(nav_selector).first.is_visible()
            home_card_visible = await page.locator("h5.card-title:has-text(\"Daily Operations Tracker\")").is_visible()
        except Exception:
            nav_visible = False
        return url_ok and (nav_visible or home_card_visible), url_now, nav_visible, home_card_visible

    end_time = asyncio.get_event_loop().time() + dwell_seconds
    while asyncio.get_event_loop().time() < end_time:
        ready, url_now, nav_visible, home_card_visible = await _home_ready()
        if ready:
            log_event(
                logger=logger,
                phase="login",
                message="OTP verification completed; home detected",
                store_code=store.store_code,
                final_url=url_now,
                nav_visible=nav_visible,
                home_card_visible=home_card_visible,
                dwell_seconds=dwell_seconds,
            )
            return True, True

        await asyncio.sleep(2)

    ready, final_url, nav_visible, home_card_visible = await _home_ready()
    if ready:
        log_event(
            logger=logger,
            phase="login",
            message="OTP verification completed near dwell deadline; home detected",
            store_code=store.store_code,
            final_url=final_url,
            nav_visible=nav_visible,
            home_card_visible=home_card_visible,
            dwell_seconds=dwell_seconds,
        )
        return True, True

    log_event(
        logger=logger,
        phase="login",
        status="warn",
        message="OTP not completed; home page not reached before dwell deadline",
        store_code=store.store_code,
        final_url=final_url,
        nav_visible=nav_visible,
        home_card_visible=home_card_visible,
        otp_deadline=deadline.isoformat(),
        dwell_seconds=dwell_seconds,
    )
    return False, True


async def _run_store_discovery(
    *,
    browser: Browser,
    store: TdStore,
    logger: JsonLogger,
    run_env: str,
    run_id: str,
    run_date: datetime,
    run_start_date: date,
    run_end_date: date,
    nav_timeout_ms: int,
    summary: TdOrdersDiscoverySummary,
    run_orders: bool,
    run_sales: bool,
) -> None:
    store_logger = logger.bind(store_code=store.store_code)
    log_event(
        logger=store_logger,
        phase="store",
        message="Starting TD orders store discovery",
        run_env=run_env,
        from_date=run_start_date,
        to_date=run_end_date,
    )

    storage_state_exists = store.storage_state_path.exists()
    download_dir = default_download_dir()
    context = await browser.new_context(
        storage_state=str(store.storage_state_path) if storage_state_exists else None,
        accept_downloads=True,
    )
    page = await context.new_page()
    nav_selector = store.reports_nav_selector
    outcome: StoreOutcome | None = None
    orders_report: StoreReport | None = None
    sales_report: StoreReport | None = None
    stored_state_path: str | None = None
    probe_reason: str | None = None
    probe_result: SessionProbeResult | None = None
    try:
        session_reused = False
        login_performed = False
        verification_seen = False
        verification_ok = True
        if storage_state_exists:
            log_event(
                logger=store_logger,
                phase="session",
                message="Storage state found; probing session validity",
                store_code=store.store_code,
                storage_state=str(store.storage_state_path),
            )
            probe_result = await _probe_session(
                page, store=store, logger=store_logger, timeout_ms=nav_timeout_ms
            )
            probe_reason = probe_result.reason or "state_valid"
            verification_seen = bool(probe_result.verification_seen)
            if probe_result.valid:
                session_reused = True
                log_event(
                    logger=store_logger,
                    phase="session",
                    message="Storage state probe valid; reusing session",
                    store_code=store.store_code,
                    storage_state=str(store.storage_state_path),
                    probe_reason=probe_reason,
                    probe_valid=probe_result.valid,
                    re_login_performed=login_performed,
                )
            else:
                log_event(
                    logger=store_logger,
                    phase="session",
                    message="Storage state probe invalid; performing login",
                    store_code=store.store_code,
                    storage_state=str(store.storage_state_path),
                    probe_reason=probe_reason,
                    probe_valid=probe_result.valid,
                    verification_seen=verification_seen,
                    nav_visible=probe_result.nav_visible,
                    home_card_visible=probe_result.home_card_visible,
                    re_login_performed=True,
                )
                session_reused = await _perform_login(
                    page, store=store, logger=store_logger, nav_timeout_ms=nav_timeout_ms
                )
                login_performed = True
        else:  # no storage state → must login
            probe_reason = "no_storage_state"
            log_event(
                logger=store_logger,
                phase="session",
                message="No storage state found; performing login",
                store_code=store.store_code,
                re_login_performed=True,
            )
            session_reused = await _perform_login(
                page, store=store, logger=store_logger, nav_timeout_ms=nav_timeout_ms
            )
            login_performed = True

        if session_reused and not login_performed:
            log_event(
                logger=store_logger,
                phase="session",
                message="state reused (no OTP)",
                store_code=store.store_code,
                final_url=page.url,
                storage_state=str(store.storage_state_path),
                probe_reason=probe_reason,
                probe_valid=probe_result.valid if probe_result else None,
                re_login_performed=login_performed,
            )
            if store.store_code.upper() == "A817":
                log_event(
                    logger=store_logger,
                    phase="session",
                    message="A817 storage state reused without OTP",
                    store_code=store.store_code,
                    storage_state=str(store.storage_state_path),
                )
            verification_ok = True
            verification_seen = False
        elif session_reused and login_performed:
            log_event(
                logger=store_logger,
                phase="session",
                message="Login completed; session ready",
                store_code=store.store_code,
                final_url=page.url,
                storage_state=str(store.storage_state_path),
                probe_reason=probe_reason,
                probe_valid=probe_result.valid if probe_result else None,
                login_performed=login_performed,
                re_login_performed=login_performed,
            )
            verification_ok = True
            verification_seen = False
        else:
            log_event(
                logger=store_logger,
                phase="session",
                message="Session invalid after probe/login attempt",
                store_code=store.store_code,
                storage_state=str(store.storage_state_path),
                probe_reason=probe_reason,
                verification_seen=verification_seen,
                probe_valid=probe_result.valid if probe_result else None,
                re_login_performed=login_performed,
            )
            if not login_performed:
                session_reused = await _perform_login(
                    page, store=store, logger=store_logger, nav_timeout_ms=nav_timeout_ms
                )
                login_performed = True

        if not session_reused:
            outcome = StoreOutcome(
                status="error",
                message="Login failed with provided credentials",
                final_url=page.url,
            )
            return

        should_wait_for_otp = login_performed or verification_seen
        if should_wait_for_otp:
            verification_ok, verification_seen = await _wait_for_otp_verification(
                page, store=store, logger=store_logger, nav_selector=nav_selector
            )
            if verification_ok:
                log_event(
                    logger=store_logger,
                    phase="session",
                    message="Session ready after OTP/verification wait",
                    store_code=store.store_code,
                    storage_state=str(store.storage_state_path),
                    final_url=page.url,
                    verification_seen=verification_seen,
                    login_performed=login_performed,
                )

        if not verification_ok:
            log_event(
                logger=store_logger,
                phase="store",
                status="warn",
                message="Aborting TD orders discovery because OTP was not completed",
                store_code=store.store_code,
            )
            outcome = StoreOutcome(
                status="error",
                message="OTP was not completed before dwell deadline",
                final_url=page.url,
                verification_seen=verification_seen,
            )
            return

        home_ready = await _wait_for_home(
            page, store=store, logger=store_logger, nav_selector=nav_selector, timeout_ms=nav_timeout_ms
        )
        if not home_ready:
            outcome = StoreOutcome(
                status="error",
                message="Home page not ready after login/verification",
                final_url=page.url,
                verification_seen=verification_seen,
            )
            return

        await _log_home_nav_diagnostics(page, logger=store_logger, store=store)

        store.storage_state_path.parent.mkdir(parents=True, exist_ok=True)
        await context.storage_state(path=str(store.storage_state_path))
        stored_state_path = str(store.storage_state_path)
        log_event(
            logger=store_logger,
            phase="session",
            message="Stored refreshed session state after home detection",
            store_code=store.store_code,
            storage_state=stored_state_path,
            verification_seen=verification_seen,
        )
        if store.store_code.upper() == "A817":
            log_event(
                logger=store_logger,
                phase="session",
                message="Persisted storage state for A817 after home detection",
                store_code=store.store_code,
                storage_state=stored_state_path,
                session_reused=session_reused,
                refreshed=login_performed,
            )

        sales_only_mode = not run_orders
        if not run_orders:
            log_event(
                logger=store_logger,
                phase="orders",
                message="Skipping Orders iframe flow because Orders segment was disabled by flag",
                store_code=store.store_code,
            )
            orders_report = StoreReport(status="skipped", message="Orders sync skipped by flag")
        else:
            container_ready = await _navigate_to_orders_container(
                page,
                store=store,
                logger=store_logger,
                nav_selector=nav_selector,
                nav_timeout_ms=nav_timeout_ms,
                capture_left_nav=True,
                nav_snapshot_context="orders_iframe",
                sales_only_mode=sales_only_mode,
            )
            if not container_ready:
                outcome = StoreOutcome(
                    status="error",
                    message="Orders container did not load from Reports navigation",
                    final_url=page.url,
                    verification_seen=verification_seen,
                    storage_state=stored_state_path,
                )
                return

            iframe_locator = await _wait_for_iframe(page, store=store, logger=store_logger)
            if iframe_locator is not None:
                await _observe_iframe_hydration(iframe_locator, store=store, logger=store_logger)
                success, detail = await _run_orders_iframe_flow(
                    page,
                    iframe_locator,
                    store=store,
                    logger=store_logger,
                    from_date=run_start_date,
                    to_date=run_end_date,
                    nav_timeout_ms=nav_timeout_ms,
                    download_dir=download_dir,
                )
                if success:
                    ingest_result: TdOrdersIngestResult | None = None
                    status_label = "ok"
                    orders_warning_summary: dict[str, Any] | None = None
                    if config.database_url:
                        if not store.cost_center:
                            log_event(
                                logger=store_logger,
                                phase="ingest",
                                status="error",
                                message="Missing cost_center for TD store; cannot ingest orders",
                                store_code=store.store_code,
                            )
                            orders_report = StoreReport(
                                status="error",
                                filenames=[Path(detail).name],
                                message="Orders downloaded but could not ingest",
                                error_message="Missing cost_center for TD store; cannot ingest orders",
                            )
                            outcome = StoreOutcome(
                                status="error",
                                message="Missing cost_center for TD store; cannot ingest orders",
                                final_url=page.url,
                                iframe_attached=True,
                                verification_seen=verification_seen,
                                storage_state=stored_state_path,
                            )
                            return
                        try:
                            ingest_result = await ingest_td_orders_workbook(
                                workbook_path=Path(detail),
                                store_code=store.store_code,
                                cost_center=store.cost_center or "",
                                run_id=run_id,
                                run_date=run_date,
                                database_url=config.database_url,
                                logger=store_logger,
                            )
                            if ingest_result.warnings:
                                orders_warning_summary = _summarize_warnings(ingest_result.warnings)
                                log_event(
                                    logger=store_logger,
                                    phase="ingest",
                                    status="warn",
                                    message="TD Orders workbook ingested with warnings",
                                    store_code=store.store_code,
                                    warning_count=orders_warning_summary["count"],
                                    warning_samples=orders_warning_summary["samples"],
                                    warnings_truncated=orders_warning_summary["truncated"],
                                    staging_rows=ingest_result.staging_rows,
                                    final_rows=ingest_result.final_rows,
                                )
                            else:
                                log_event(
                                    logger=store_logger,
                                    phase="ingest",
                                    message="TD Orders workbook ingested",
                                    store_code=store.store_code,
                                    staging_rows=ingest_result.staging_rows,
                                    final_rows=ingest_result.final_rows,
                                )
                            summary.add_ingest_remarks(ingest_result.ingest_remarks)
                        except Exception as exc:
                            log_event(
                                logger=store_logger,
                                phase="ingest",
                                status="error",
                                message="TD Orders ingestion failed",
                                store_code=store.store_code,
                                error=str(exc),
                            )
                            orders_report = StoreReport(
                                status="error",
                                filenames=[Path(detail).name],
                                message="Orders ingestion failed",
                                error_message=str(exc),
                            )
                            outcome = StoreOutcome(
                                status="error",
                                message=f"Orders ingestion failed: {exc}",
                                final_url=page.url,
                                iframe_attached=True,
                                verification_seen=verification_seen,
                                storage_state=stored_state_path,
                            )
                            return
                    else:
                        log_event(
                            logger=store_logger,
                            phase="ingest",
                            status="warn",
                            message="Skipping TD Orders ingestion because database_url is missing",
                            store_code=store.store_code,
                        )
                        status_label = "warning"

                    outcome_status = "warning" if status_label == "warning" else "ok"
                    outcome_message = "Orders report downloaded"
                    if ingest_result:
                        outcome_message = (
                            f"Orders ingested: staging={ingest_result.staging_rows}, final={ingest_result.final_rows}"
                        )
                        if ingest_result.warnings:
                            warning_preview = _format_warning_preview(
                                orders_warning_summary or _summarize_warnings(ingest_result.warnings)
                            )
                            if warning_preview:
                                outcome_message = f"{outcome_message} (warnings: {warning_preview})"
                    orders_report = StoreReport(
                        status=status_label,
                        filenames=[Path(detail).name],
                        staging_rows=ingest_result.staging_rows if ingest_result else None,
                        final_rows=ingest_result.final_rows if ingest_result else None,
                        message=outcome_message,
                        warnings=ingest_result.warnings if ingest_result else [],
                    )

                    if run_sales:
                        sales_report = await _execute_sales_flow(
                            page,
                            store=store,
                            logger=store_logger,
                            from_date=run_start_date,
                            to_date=run_end_date,
                            nav_timeout_ms=nav_timeout_ms,
                            download_dir=download_dir,
                            run_id=run_id,
                            run_date=run_date,
                            summary=summary,
                            sales_only_mode=sales_only_mode,
                        )
                        sales_status = sales_report.status
                        sales_message = sales_report.message
                    else:
                        sales_message = "Sales sync skipped by flag"
                        sales_status = "skipped"
                        sales_report = StoreReport(status="skipped", message=sales_message)

                    if sales_status == "ok":
                        detail_message = sales_message or "Sales report downloaded"
                        outcome_message = f"{outcome_message}; {detail_message}"
                    elif sales_status == "warning":
                        if outcome_status == "ok":
                            outcome_status = "warning"
                        outcome_message = f"{outcome_message}; Sales issue: {sales_message}"
                    elif sales_status == "error":
                        outcome_status = "error"
                        outcome_message = f"{outcome_message}; Sales failed: {sales_message}"
                    elif sales_status == "skipped":
                        outcome_message = f"{outcome_message}; Sales skipped"

                    log_event(
                        logger=store_logger,
                        phase="iframe",
                        message="Orders iframe flow completed",
                        store_code=store.store_code,
                        download_path=detail,
                    )
                    outcome = StoreOutcome(
                        status=outcome_status,
                        message=outcome_message,
                        final_url=page.url,
                        iframe_attached=True,
                        verification_seen=verification_seen,
                        storage_state=stored_state_path,
                    )
                else:
                    log_event(
                        logger=store_logger,
                        phase="iframe",
                        status="error",
                        message="Orders iframe flow failed",
                        store_code=store.store_code,
                        error=detail,
                    )
                    orders_report = StoreReport(
                        status="error",
                        message=detail or "Orders iframe flow failed",
                        error_message=detail or "Orders iframe flow failed",
                    )
                    outcome = StoreOutcome(
                        status="error",
                        message=detail or "Orders iframe flow failed",
                        final_url=page.url,
                        iframe_attached=True,
                        verification_seen=verification_seen,
                        storage_state=stored_state_path,
                    )
            else:
                outcome = StoreOutcome(
                    status="error",
                    message="Orders iframe did not attach",
                    final_url=page.url,
                    iframe_attached=False,
                    verification_seen=verification_seen,
                    storage_state=stored_state_path,
                )
                orders_report = StoreReport(
                    status="error",
                    message="Orders iframe did not attach",
                    error_message="Orders iframe did not attach",
                )

        if sales_report is None and run_sales:
            sales_report = await _execute_sales_flow(
                page,
                store=store,
                logger=store_logger,
                from_date=run_start_date,
                to_date=run_end_date,
                nav_timeout_ms=nav_timeout_ms,
                download_dir=download_dir,
                run_id=run_id,
                run_date=run_date,
                summary=summary,
                sales_only_mode=sales_only_mode,
            )
        elif sales_report is None:
            log_event(
                logger=store_logger,
                phase="sales",
                message="Skipping Sales & Delivery report because flag disabled sales run",
                store_code=store.store_code,
            )
            sales_report = StoreReport(status="skipped", message="Sales sync skipped by flag")

        if outcome is None:

            def _merge_status(current: str, incoming: str | None) -> str:
                if incoming == "error":
                    return "error"
                if incoming == "warning" and current != "error":
                    return "warning"
                return current

            outcome_status = "ok"
            outcome_messages: list[str] = []
            if orders_report:
                outcome_status = _merge_status(outcome_status, orders_report.status)
                if orders_report.message:
                    outcome_messages.append(f"Orders: {orders_report.message}")
            if sales_report:
                outcome_status = _merge_status(outcome_status, sales_report.status)
                if sales_report.message:
                    outcome_messages.append(f"Sales: {sales_report.message}")
            outcome = StoreOutcome(
                status=outcome_status,
                message="; ".join(outcome_messages) if outcome_messages else "Store run completed",
                final_url=page.url,
                verification_seen=verification_seen,
                storage_state=stored_state_path,
            )

        log_event(
            logger=store_logger,
            phase="store",
            message="Completed TD orders store discovery",
            store_code=store.store_code,
            session_reused=session_reused,
        )
    except asyncio.CancelledError as exc:
        outcome = StoreOutcome(
            status="warning",
            message="TD orders discovery cancelled",
            final_url=page.url,
        )
        log_event(
            logger=store_logger,
            phase="store",
            status="warn",
            message="TD orders store discovery cancelled; closing context",
            store_code=store.store_code,
        )
        raise exc
    except KeyboardInterrupt as exc:
        outcome = StoreOutcome(
            status="warning",
            message="TD orders discovery interrupted",
            final_url=page.url,
        )
        log_event(
            logger=store_logger,
            phase="store",
            status="warn",
            message="TD orders store discovery interrupted; closing context",
            store_code=store.store_code,
            error=str(exc),
        )
        raise exc
    except Exception as exc:  # pragma: no cover - runtime safeguard
        outcome = StoreOutcome(
            status="error",
            message=f"TD orders discovery failed: {exc}",
            final_url=page.url,
        )
        log_event(
            logger=store_logger,
            phase="store",
            status="error",
            message="TD orders discovery failed",
            store_code=store.store_code,
            error=str(exc),
        )
    finally:
        summary.record_store(
            store.store_code,
            outcome,
            orders_result=orders_report,
            sales_result=sales_report,
        )
        await _close_context(context)


async def _close_context(context: BrowserContext | None) -> None:
    if context is None:
        return
    with contextlib.suppress(Exception):
        await context.close()


# ── CLI entrypoint ───────────────────────────────────────────────────────────


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the TD Orders discovery flow")
    parser.add_argument("--run-env", dest="run_env", type=str, default=None, help="Override run environment label")
    parser.add_argument("--run-id", dest="run_id", type=str, default=None, help="Override generated run id")
    parser.add_argument("--from-date", dest="from_date", type=_parse_date, default=None, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", dest="to_date", type=_parse_date, default=None, help="End date (YYYY-MM-DD)")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--orders-only",
        dest="orders_only",
        action="store_true",
        help="Run only the Orders sync (skip Sales)",
    )
    group.add_argument(
        "--sales-only",
        dest="sales_only",
        action="store_true",
        help="Run only the Sales sync (skip Orders)",
    )
    return parser


async def _async_entrypoint(argv: Sequence[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    await main(
        run_env=args.run_env,
        run_id=args.run_id,
        from_date=args.from_date,
        to_date=args.to_date,
        run_orders=not args.sales_only,
        run_sales=not args.orders_only,
    )


def run(argv: Sequence[str] | None = None) -> None:
    asyncio.run(_async_entrypoint(argv))


if __name__ == "__main__":  # pragma: no cover
    run()
