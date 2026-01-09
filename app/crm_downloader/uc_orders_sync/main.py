from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Sequence
from urllib.parse import urlparse

import sqlalchemy as sa
from playwright.async_api import Browser, ElementHandle, Locator, Page, TimeoutError, async_playwright

from app.common.date_utils import aware_now
from app.common.db import session_scope
from app.config import config
from app.crm_downloader.browser import launch_browser
from app.crm_downloader.config import default_profiles_dir
from app.dashboard_downloader.json_logger import JsonLogger, get_logger, log_event, new_run_id
from app.dashboard_downloader.notifications import send_notifications_for_run
from app.dashboard_downloader.run_summary import fetch_summary_for_run, insert_run_summary, update_run_summary

PIPELINE_NAME = "uc_orders_sync"
NAV_TIMEOUT_MS = 90_000
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
GST_DATE_RANGE_READY_SELECTORS = (
    "input.search-user[placeholder*='Choose Start Date']",
    "input[placeholder='Choose Start Date - End Date']",
)
GST_CONTROL_SELECTORS = {
    "date_range_input": [
        "input.search-user[readonly][placeholder='Choose Start Date - End Date']",
        "input.search-user[placeholder*='Choose Start Date']",
        "input[placeholder='Choose Start Date - End Date']",
    ],
    "apply_button": ["button:has-text('Apply')"],
    "export_button": ["button:has-text('Export Report')"],
}
DATE_PICKER_POPUP_SELECTORS = (
    ".daterangepicker",
    ".datepicker",
    ".flatpickr-calendar",
    ".react-datepicker",
    ".mat-datepicker-content",
    "[class*=date-picker]",
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


@dataclass
class UcOrdersDiscoverySummary:
    run_id: str
    run_env: str
    report_date: date
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    store_codes: list[str] = field(default_factory=list)
    store_outcomes: Dict[str, StoreOutcome] = field(default_factory=dict)
    phases: dict[str, Dict[str, int]] = field(
        default_factory=lambda: {"init": {"ok": 0, "warning": 0, "error": 0}, "store": {"ok": 0, "warning": 0, "error": 0}}
    )
    notes: list[str] = field(default_factory=list)

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
        for counters in self.phases.values():
            if counters.get("error"):
                return "error"
        for counters in self.phases.values():
            if counters.get("warning"):
                return "warning"
        return "ok"

    def summary_text(self) -> str:
        total = len(self.store_codes)
        ok = sum(1 for outcome in self.store_outcomes.values() if outcome.status == "ok")
        warn = sum(1 for outcome in self.store_outcomes.values() if outcome.status == "warning")
        error = sum(1 for outcome in self.store_outcomes.values() if outcome.status == "error")
        return f"UC orders discovery: {ok} ok, {warn} warnings, {error} errors across {total} stores"

    def build_record(self, *, finished_at: datetime) -> Dict[str, Any]:
        total_seconds = max(0, int((finished_at - self.started_at).total_seconds()))
        hh = total_seconds // 3600
        mm = (total_seconds % 3600) // 60
        ss = total_seconds % 60
        total_time_taken = f"{hh:02d}:{mm:02d}:{ss:02d}"
        metrics = {
            "stores": {
                "configured": list(self.store_codes),
                "outcomes": {code: outcome.__dict__ for code, outcome in self.store_outcomes.items()},
            },
            "notes": list(self.notes),
        }
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
) -> None:
    """Run the UC GST report discovery flow (login + selector identification)."""

    resolved_run_id = run_id or new_run_id()
    resolved_env = run_env or config.run_env
    run_start_date, run_end_date = _resolve_date_range(from_date=from_date, to_date=to_date)
    logger = get_logger(run_id=resolved_run_id)
    summary = UcOrdersDiscoverySummary(
        run_id=resolved_run_id,
        run_env=resolved_env,
        report_date=run_start_date,
    )

    persist_attempted = False
    browser: Browser | None = None

    try:
        log_event(
            logger=logger,
            phase="init",
            message="Starting UC orders sync discovery flow",
            run_env=resolved_env,
            from_date=run_start_date,
            to_date=run_end_date,
        )

        stores = await _load_uc_order_stores(logger=logger)
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
                await send_notifications_for_run(PIPELINE_NAME, resolved_run_id)
            return

        async with async_playwright() as playwright:
            browser = await launch_browser(playwright=playwright, logger=logger)
            for store in stores:
                await _run_store_discovery(
                    browser=browser,
                    store=store,
                    logger=logger,
                    run_env=resolved_env,
                    run_id=resolved_run_id,
                    summary=summary,
                    from_date=run_start_date,
                    to_date=run_end_date,
                )
            await browser.close()

        log_event(
            logger=logger,
            phase="notifications",
            message="UC orders sync discovery flow complete; notifying",
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


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:  # pragma: no cover - exercised via CLI parsing
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}; expected YYYY-MM-DD") from exc


def _resolve_date_range(*, from_date: date | None, to_date: date | None) -> tuple[date, date]:
    if from_date and to_date:
        resolved_from = from_date
        resolved_to = to_date
    elif from_date:
        resolved_from = from_date
        resolved_to = from_date
    elif to_date:
        resolved_from = to_date
        resolved_to = to_date
    else:
        today = aware_now().date()
        resolved_from = today - timedelta(days=90)
        resolved_to = today

    if resolved_from > resolved_to:
        resolved_from, resolved_to = resolved_to, resolved_from
    return resolved_from, resolved_to


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


async def _load_uc_order_stores(*, logger: JsonLogger) -> list[UcStore]:
    if not config.database_url:
        log_event(
            logger=logger,
            phase="init",
            status="error",
            message="database_url missing; cannot load UC store rows",
        )
        return []

    query = sa.text(
        """
        SELECT store_code, store_name, sync_config
        FROM store_master
        WHERE sync_group = :sync_group
          AND sync_orders_flag = TRUE
          AND (is_active IS NULL OR is_active = TRUE)
        """
    )

    async with session_scope(config.database_url) as session:
        result = await session.execute(query, {"sync_group": "UC"})
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


async def _run_store_discovery(
    *,
    browser: Browser,
    store: UcStore,
    logger: JsonLogger,
    run_env: str,
    run_id: str,
    summary: UcOrdersDiscoverySummary,
    from_date: date,
    to_date: date,
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

    storage_state_path = store.storage_state_path
    storage_state_exists = storage_state_path.exists()
    storage_state_value = str(storage_state_path) if storage_state_exists else None
    context = await browser.new_context(storage_state=storage_state_value)
    page = await context.new_page()
    outcome = StoreOutcome(status="error", message="uninitialized")

    try:
        if storage_state_exists:
            log_event(
                logger=logger,
                phase="login",
                message="Reusing existing storage state",
                store_code=store.store_code,
                storage_state=storage_state_value,
            )
            login_used = False
        else:
            login_used = True
            login_ok = await _perform_login(page=page, store=store, logger=logger)
            if not login_ok:
                outcome = StoreOutcome(
                    status="error",
                    message="Login failed",
                    storage_state=None,
                    login_used=True,
                )
                summary.record_store(store.store_code, outcome)
                return
            storage_state_path.parent.mkdir(parents=True, exist_ok=True)
            await context.storage_state(path=str(storage_state_path))
            log_event(
                logger=logger,
                phase="login",
                message="Saved storage state",
                store_code=store.store_code,
                storage_state=str(storage_state_path),
            )

        if not store.home_url:
            outcome = StoreOutcome(
                status="error",
                message="Missing home URL in sync_config",
                login_used=login_used,
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
        if await _session_invalid(page=page, store=store):
            log_event(
                logger=logger,
                phase="login",
                status="warn",
                message="Session invalid; re-authenticated",
                store_code=store.store_code,
                current_url=page.url,
            )
            login_used = True
            login_ok = await _perform_login(page=page, store=store, logger=logger)
            if not login_ok:
                outcome = StoreOutcome(
                    status="error",
                    message="Login failed after session invalidation",
                    storage_state=str(storage_state_path) if storage_state_path.exists() else None,
                    login_used=True,
                )
                summary.record_store(store.store_code, outcome)
                return
            storage_state_path.parent.mkdir(parents=True, exist_ok=True)
            await context.storage_state(path=str(storage_state_path))
            log_event(
                logger=logger,
                phase="login",
                message="Saved storage state",
                store_code=store.store_code,
                storage_state=str(storage_state_path),
            )
            if not await _wait_for_home_ready(page=page, store=store, logger=logger, source="post-login"):
                outcome = StoreOutcome(
                    status="error",
                    message="Home page not reached after login",
                    final_url=page.url,
                    storage_state=str(storage_state_path) if storage_state_path.exists() else None,
                    login_used=login_used,
                )
                summary.record_store(store.store_code, outcome)
                return
        else:
            if not await _wait_for_home_ready(page=page, store=store, logger=logger, source="session"):
                on_login = _url_is_login(page.url or "", store)
                outcome = StoreOutcome(
                    status="error" if on_login else "warning",
                    message="Home page not reached; skipping GST discovery"
                    if on_login
                    else "Home page not ready; skipping GST discovery",
                    final_url=page.url,
                    storage_state=str(storage_state_path) if storage_state_path.exists() else None,
                    login_used=login_used,
                )
                summary.record_store(store.store_code, outcome)
                return

        direct_ready, container = await _try_direct_gst_reports(page=page, store=store, logger=logger)
        if direct_ready and container is not None:
            log_event(
                logger=logger,
                phase="navigation",
                message="GST reports navigation path selected",
                store_code=store.store_code,
                path="direct URL",
                current_url=page.url,
            )
            ready = True
        else:
            log_event(
                logger=logger,
                phase="navigation",
                message="GST reports navigation path selected",
                store_code=store.store_code,
                path="menu fallback",
                current_url=page.url,
            )
            gst_clicked = await _navigate_to_gst_reports(page=page, store=store, logger=logger)
            if not gst_clicked:
                outcome = StoreOutcome(
                    status="warning",
                    message="GST reports navigation failed",
                    final_url=page.url,
                    storage_state=str(storage_state_path) if storage_state_path.exists() else None,
                    login_used=login_used,
                )
                summary.record_store(store.store_code, outcome)
                return

            ready, container = await _wait_for_gst_report_ready(page=page, logger=logger, store=store)
        if not ready or container is None:
            outcome = StoreOutcome(
                status="warning",
                message="GST report readiness signal missing",
                final_url=page.url,
                storage_state=str(storage_state_path) if storage_state_path.exists() else None,
                login_used=login_used,
            )
            summary.record_store(store.store_code, outcome)
            return
        await _apply_date_range(
            page=page,
            container=container,
            logger=logger,
            store=store,
            from_date=from_date,
            to_date=to_date,
        )
        selectors_payload = await _discover_selector_cues(container=container, page=page)
        spinner_payload = await _discover_spinner_cues(page)
        log_event(
            logger=logger,
            phase="selectors",
            message="Captured GST report selector cues",
            store_code=store.store_code,
            controls=selectors_payload,
            spinners=spinner_payload,
        )

        outcome = StoreOutcome(
            status="ok",
            message="GST report page loaded",
            final_url=page.url,
            storage_state=str(storage_state_path) if storage_state_path.exists() else None,
            login_used=login_used,
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
            message="Timeout while loading GST report page",
            final_url=page.url,
            storage_state=str(storage_state_path) if storage_state_path.exists() else None,
            login_used=login_used,
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
        )
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
        dom_snippet = await _get_dom_snippet(page)
        log_event(
            logger=logger,
            phase="login",
            status="warn",
            message="Login input empty after fill",
            store_code=store.store_code,
            empty_fields=empty_fields,
            dom_snippet=dom_snippet,
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
        dom_snippet = await _get_dom_snippet(page)
        log_event(
            logger=logger,
            phase="navigation",
            status="warn",
            message="GST page label not detected",
            store_code=store.store_code,
            selector=GST_PAGE_LABEL_SELECTOR,
            current_url=page.url,
            dom_snippet=dom_snippet,
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
        dom_snippet = await _get_dom_snippet(page)
        log_event(
            logger=logger,
            phase="navigation",
            status="warn",
            message="GST report container not detected",
            store_code=store.store_code,
            selector=readiness_selector,
            fallback_selector=fallback_selector,
            current_url=page.url,
            dom_snippet=dom_snippet,
        )
        return False, None

    try:
        await container.locator(readiness_selector).first.wait_for(timeout=NAV_TIMEOUT_MS)
    except TimeoutError:
        dom_snippet = await _get_dom_snippet(page)
        log_event(
            logger=logger,
            phase="navigation",
            status="warn",
            message="GST report readiness signal missing",
            store_code=store.store_code,
            selector=readiness_selector,
            fallback_selector=fallback_selector,
            current_url=page.url,
            dom_snippet=dom_snippet,
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
        dom_snippet = await _get_dom_snippet(page)
        log_event(
            logger=logger,
            phase="navigation",
            status="error",
            message="Home page not reached; still on login",
            store_code=store.store_code,
            home_url=home_url,
            current_url=current_url,
            source=source,
            dom_snippet=dom_snippet,
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

    dom_snippet = await _get_dom_snippet(page)
    log_event(
        logger=logger,
        phase="navigation",
        status="warn",
        message="Home page marker not detected",
        store_code=store.store_code,
        home_url=home_url,
        current_url=current_url,
        selectors=home_selectors,
        dom_snippet=dom_snippet,
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
) -> bool:
    input_selectors = [
        "input.search-user[placeholder='Choose Start Date - End Date']",
        *GST_CONTROL_SELECTORS["date_range_input"],
    ]
    date_input = None
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
        return False

    with contextlib.suppress(Exception):
        await date_input.scroll_into_view_if_needed()
    await date_input.click()
    popup = await _wait_for_date_picker_popup(page=page, logger=logger, store=store)
    if popup is None:
        return False

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
        return False
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
        return False

    apply_selectors = GST_CONTROL_SELECTORS["apply_button"]
    apply_button = None
    for selector in apply_selectors:
        try:
            if await popup.locator(selector).count():
                apply_button = popup.locator(selector).first
                break
            if await container.locator(selector).count():
                apply_button = container.locator(selector).first
                break
        except Exception:
            continue
    if apply_button is None:
        log_event(
            logger=logger,
            phase="filters",
            status="warn",
            message="Apply button not found after date selection",
            store_code=store.store_code,
            selectors=apply_selectors,
        )
        return False

    await apply_button.click()
    await _wait_for_report_refresh(page=page, container=container, logger=logger, store=store)
    return True


async def _wait_for_date_picker_popup(
    *, page: Page, logger: JsonLogger, store: UcStore
) -> Locator | None:
    popup_selector = ", ".join(DATE_PICKER_POPUP_SELECTORS)
    popup = page.locator(popup_selector).first
    try:
        await popup.wait_for(state="visible", timeout=NAV_TIMEOUT_MS)
        return popup
    except TimeoutError:
        log_event(
            logger=logger,
            phase="filters",
            status="warn",
            message="Date picker popup not visible after clicking input",
            store_code=store.store_code,
            selector=popup_selector,
        )
    return None


async def _get_calendar_locators(*, popup: Locator) -> list[Locator]:
    calendars: list[Locator] = []
    for selector in (".drp-calendar.left", ".drp-calendar.right"):
        locator = popup.locator(selector)
        count = await locator.count()
        for idx in range(count):
            calendars.append(locator.nth(idx))
    if calendars:
        return calendars

    for selector in (
        ".drp-calendar",
        ".react-datepicker__month-container",
        ".flatpickr-calendar",
        ".datepicker",
        ".mat-calendar",
    ):
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

    clicked = await _click_day_in_calendar(calendar=calendar, target_date=target_date)
    if not clicked:
        log_event(
            logger=logger,
            phase="filters",
            status="warn",
            message="Target day not found in calendar",
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
        header_text = await _get_calendar_header_text(calendar=calendar)
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


async def _get_calendar_header_text(*, calendar: Locator) -> str | None:
    selectors = (
        ".month",
        ".month-name",
        ".datepicker-switch",
        ".flatpickr-current-month",
        ".react-datepicker__current-month",
        ".mat-calendar-period-button",
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
        selectors = [
            "button:has-text('<<')",
            "th:has-text('<<')",
            "span:has-text('<<')",
            "button.prev",
            "th.prev",
            ".prev",
        ]
    else:
        selectors = [
            "button:has-text('>>')",
            "th:has-text('>>')",
            "span:has-text('>>')",
            "button.next",
            "th.next",
            ".next",
        ]
    for selector in selectors:
        locator = calendar.locator(selector).first
        try:
            if await locator.count():
                await locator.click()
                return
        except Exception:
            continue
    raise TimeoutError(f"Calendar navigation button not found for direction={direction}")


async def _click_day_in_calendar(*, calendar: Locator, target_date: date) -> bool:
    day_text = str(target_date.day)
    candidates = calendar.locator("td, button, span").filter(has_text=re.compile(rf"^{day_text}$"))
    count = await candidates.count()
    for idx in range(count):
        candidate = candidates.nth(idx)
        try:
            if not await candidate.is_visible():
                continue
            class_name = (await candidate.get_attribute("class")) or ""
            aria_disabled = (await candidate.get_attribute("aria-disabled")) or ""
            if "off" in class_name.split() or "disabled" in class_name.split() or aria_disabled == "true":
                continue
            await candidate.click()
            return True
        except Exception:
            continue
    return False


async def _wait_for_report_refresh(
    *, page: Page, container: Locator, logger: JsonLogger, store: UcStore
) -> bool:
    row_locator = container.locator("table tbody tr")
    try:
        initial_count = await row_locator.count()
    except Exception:
        initial_count = 0

    spinner_selector = ", ".join(SPINNER_CSS_SELECTORS)
    timeout_s = NAV_TIMEOUT_MS / 1000
    start = asyncio.get_event_loop().time()
    spinner_locator = page.locator(spinner_selector).first if spinner_selector else None
    while (asyncio.get_event_loop().time() - start) < timeout_s:
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
        if current_count != initial_count:
            log_event(
                logger=logger,
                phase="filters",
                message="Report refreshed after date range apply",
                store_code=store.store_code,
                previous_row_count=initial_count,
                current_row_count=current_count,
            )
            return True
        await asyncio.sleep(0.5)

    log_event(
        logger=logger,
        phase="filters",
        status="warn",
        message="Report did not refresh within timeout after applying date range",
        store_code=store.store_code,
        previous_row_count=initial_count,
    )
    return False


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
    try:
        await page.wait_for_load_state("networkidle", timeout=15_000)
    except Exception:
        pass
    nav_root = await _find_nav_root(page)
    link_locator: Locator | None = None
    matched_selector: str | None = None
    gst_href_candidate = "/gst-report"
    link_locator = await _locate_nav_link_by_href(nav_root, gst_href_candidate)
    if link_locator is not None:
        matched_selector = f"a[href*='{gst_href_candidate}']"
    else:
        await _expand_nav_groups(page=page, nav_root=nav_root, logger=logger, store=store)
        link_locator = await _locate_nav_link_by_href(nav_root, gst_href_candidate)
        if link_locator is not None:
            matched_selector = f"a[href*='{gst_href_candidate}']"

    url_candidates = _build_url_candidates(orders_url)
    if link_locator is None:
        for candidate in url_candidates:
            locator = nav_root.locator(f"a[href*=\"{candidate}\"]")
            try:
                if await locator.count():
                    link_locator = locator.first
                    matched_selector = f"a[href*='{candidate}']"
                    break
            except Exception:
                continue

    fallback_handle = None
    if link_locator is None:
        label_candidates = store.gst_menu_labels or list(GST_MENU_LABELS)
        label_candidates = list(dict.fromkeys([*label_candidates, "Reports"]))
        for label in label_candidates:
            locator = nav_root.locator("a, button, [role='menuitem']").filter(has_text=re.compile(label, re.I))
            try:
                if await locator.count():
                    link_locator = locator.first
                    matched_selector = f"nav text '{label}'"
                    break
            except Exception:
                continue
        if link_locator is None:
            fallback_handle = await _locate_nav_ancestor_by_text(nav_root, label_candidates)
            if fallback_handle is not None:
                matched_selector = "nav ancestor by text"

    if link_locator is None and fallback_handle is None:
        nav_links = await _summarize_nav_links(nav_root)
        dom_snippet = await _get_dom_snippet(page)
        log_event(
            logger=logger,
            phase="navigation",
            status="warn",
            message="GST reports navigation item not found",
            store_code=store.store_code,
            orders_url=orders_url,
            current_url=page.url,
            nav_links=nav_links,
            dom_snippet=dom_snippet,
        )
        return False

    try:
        if link_locator is not None:
            await link_locator.click()
        else:
            await fallback_handle.click()
    except Exception as exc:
        log_event(
            logger=logger,
            phase="navigation",
            status="warn",
            message="GST reports navigation click failed",
            store_code=store.store_code,
            orders_url=orders_url,
            current_url=page.url,
            error=str(exc),
        )
        return False

    log_event(
        logger=logger,
        phase="navigation",
        message="GST reports navigation clicked",
        store_code=store.store_code,
        orders_url=orders_url,
        current_url=page.url,
        selector=matched_selector,
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
