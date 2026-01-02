from __future__ import annotations

import argparse
import asyncio
import json
import re
import contextlib
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence

import sqlalchemy as sa
from playwright.async_api import Browser, BrowserContext, FrameLocator, Locator, Page, TimeoutError, async_playwright

from app.common.db import session_scope
from app.config import config
from app.crm_downloader.config import default_download_dir, default_profiles_dir
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


async def main(
    *,
    run_env: str | None = None,
    run_id: str | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> None:
    """Run the TD Orders sync flow (login + iframe historical orders download)."""

    resolved_run_id = run_id or new_run_id()
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
                    run_start_date=run_start_date,
                    run_end_date=run_end_date,
                    nav_timeout_ms=nav_timeout_ms,
                    summary=summary,
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


@dataclass
class TdStore:
    store_code: str
    store_name: str | None
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

    def mark_phase(self, phase: str, status: str) -> None:
        counters = self.phases.setdefault(phase, {"ok": 0, "warning": 0, "error": 0})
        normalized = status if status in counters else "ok"
        counters[normalized] += 1

    def record_store(self, store_code: str, outcome: StoreOutcome) -> None:
        self.store_outcomes[store_code] = outcome
        self.mark_phase("store", outcome.status)

    def add_note(self, note: str) -> None:
        if note not in self.notes:
            self.notes.append(note)

    def overall_status(self) -> str:
        if any(outcome.status == "error" for outcome in self.store_outcomes.values()):
            return "error"
        if any(outcome.status == "warning" for outcome in self.store_outcomes.values()):
            return "warning"
        if self.phases.get("init", {}).get("warning"):
            return "warning"
        return "ok"

    def _format_duration(self, finished_at: datetime) -> str:
        seconds = max(0, int((finished_at - self.started_at).total_seconds()))
        hh = seconds // 3600
        mm = (seconds % 3600) // 60
        ss = seconds % 60
        return f"{hh:02d}:{mm:02d}:{ss:02d}"

    def summary_text(self) -> str:
        lines = [
            f"Pipeline: {PIPELINE_NAME}",
            f"Run ID: {self.run_id}",
            f"Env: {self.run_env}",
            f"Report Date: {self.report_date.isoformat()}",
            f"Overall Status: {self.overall_status()}",
            "",
            "Stores:",
        ]
        if not self.store_outcomes:
            lines.append("- none processed")
        else:
            for code in sorted(self.store_outcomes):
                outcome = self.store_outcomes[code]
                message = outcome.message or "completed"
                lines.append(f"- {code}: {outcome.status.upper()} – {message}")
        if self.notes:
            lines.append("")
            lines.append("Notes:")
            lines.extend(f"- {note}" for note in self.notes)
        return "\n".join(lines)

    def build_record(self, *, finished_at: datetime) -> Dict[str, Any]:
        metrics = {
            "stores": {code: asdict(outcome) for code, outcome in self.store_outcomes.items()},
            "store_order": self.store_codes,
        }
        return {
            "pipeline_name": PIPELINE_NAME,
            "run_id": self.run_id,
            "run_env": self.run_env,
            "started_at": self.started_at,
            "finished_at": finished_at,
            "total_time_taken": self._format_duration(finished_at),
            "report_date": self.report_date,
            "overall_status": self.overall_status(),
            "summary_text": self.summary_text(),
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
        SELECT store_code, store_name, sync_config
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
        for idx in range(min(link_count, 5)):
            handle = links.nth(idx)
            try:
                text = (await handle.inner_text()).strip()
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


async def _navigate_to_orders_container(
    page: Page, *, store: TdStore, logger: JsonLogger, nav_selector: str, nav_timeout_ms: int
) -> bool:
    target_pattern = re.compile(r"/app/Reports/OrderReport", re.IGNORECASE)
    if target_pattern.search(page.url or ""):
        log_event(
            logger=logger,
            phase="orders",
            message="Already on Orders container; waiting for iframe",
            store_code=store.store_code,
            final_url=page.url,
        )
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


async def _wait_for_iframe(page: Page, *, store: TdStore, logger: JsonLogger) -> FrameLocator | None:
    try:
        await page.wait_for_selector("#ifrmReport", state="attached", timeout=20_000)
    except TimeoutError:
        log_event(
            logger=logger,
            phase="orders",
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

    log_event(
        logger=logger,
        phase="orders",
        message="Orders container ready; iframe attached",
        store_code=store.store_code,
        final_url=page.url,
        iframe_src=iframe_src,
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
    picker_dom: str | None = None
    if picker_popup is not None:
        with contextlib.suppress(Exception):
            await picker_popup.wait_for(state="visible", timeout=timeout_ms)
        try:
            picker_dom = await picker_popup.evaluate("el => el.outerHTML.slice(0, 2000)")
        except Exception:
            picker_dom = None
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
    numeric_fill_details: list[dict[str, Any]] = []

    from_input = to_input = None
    locate_attempts: list[dict[str, Any]] = []
    fill_details: list[dict[str, Any]] = []
    select_details: list[dict[str, Any]] = []
    from_ok = to_ok = False
    update_click_result: dict[str, Any] | None = None

    if use_numeric_triplets:
        from_fields = [numeric_inputs.nth(idx) for idx in range(3)]
        to_fields = [numeric_inputs.nth(idx) for idx in range(3, 6)]
        from_ok, from_detail = await _fill_numeric_triplet(
            from_fields, target_date=from_date, label="from", logger=logger, store=store
        )
        to_ok, to_detail = await _fill_numeric_triplet(
            to_fields, target_date=to_date, label="to", logger=logger, store=store
        )
        numeric_fill_details.extend([from_detail, to_detail])
    else:
        from_input, to_input, locate_attempts = await _locate_date_inputs(
            frame, timeout_ms=timeout_ms, logger=logger, store=store, popup=picker_popup
        )
        if from_input and to_input:
            from_ok, from_detail = await _fill_date_input(
                picker_popup, from_input, target_date=from_date, label="from", logger=logger, store=store
            )
            to_ok, to_detail = await _fill_date_input(
                picker_popup, to_input, target_date=to_date, label="to", logger=logger, store=store
            )
            fill_details.extend([from_detail, to_detail])
        else:
            from_ok, from_detail = await _select_date_in_open_picker(
                picker_popup, target_date=from_date, label="from"
            )
            to_ok, to_detail = await _select_date_in_open_picker(
                picker_popup, target_date=to_date, label="to"
            )
            select_details.extend([from_detail, to_detail])

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
            control_attempts=control_attempts,
            locate_attempts=locate_attempts,
            fill_details=fill_details or None,
            selection_details=select_details or None,
            numeric_fill_details=numeric_fill_details or None,
            range_text=final_range_text,
            picker_dom_preview=picker_dom,
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
        control_attempts=control_attempts,
        locate_attempts=locate_attempts,
        fill_details=fill_details or None,
        selection_details=select_details or None,
        numeric_fill_details=numeric_fill_details or None,
        range_text=final_range_text,
        picker_dom_preview=picker_dom,
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


def _truncate_html(html: str | None, limit: int = 4000) -> str | None:
    if html is None:
        return None
    html = html.strip()
    if len(html) <= limit:
        return html
    return f"{html[:limit]}…[truncated {len(html) - limit} chars]"


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
) -> str | None:
    html = await _capture_body_inner_html(frame)
    snippet = _truncate_html(html)
    log_event(
        logger=logger,
        phase="iframe",
        message="Captured iframe body HTML",
        store_code=store.store_code,
        label=label,
        body_html_snippet=snippet,
        body_html_length=len(html) if html else None,
    )
    return snippet


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
    extra_containers: Sequence[Locator] | None = None,
) -> list[Locator]:
    rows: list[Locator] = []
    seen_handles: set[int] = set()
    containers = [container]
    if extra_containers:
        containers.extend(extra_containers)

    for source in containers:
        candidates = [
            source.locator(":scope > section > div.flex.items-start.justify-between"),
            source.locator(":scope > section > div.border-b.border-gray-100"),
            source.locator(":scope > section > div"),
            source.locator(":scope > div.flex.items-start.justify-between"),
            source.locator(":scope > div.border-b.border-gray-100"),
            source.locator(":scope > div"),
            source.locator(":scope > section"),
            source.locator(":scope > article"),
            source.locator(":scope > ul > li"),
            source.locator(":scope li"),
        ]

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


async def _wait_for_report_requests_container(
    frame: FrameLocator,
    *,
    logger: JsonLogger,
    store: TdStore,
    timeout_ms: int,
) -> tuple[Locator | None, dict[str, Any]]:
    deadline = asyncio.get_event_loop().time() + (timeout_ms / 1000)
    last_seen: list[str] = []
    header_seen = False
    heading_texts: list[str] = []
    diagnostics: dict[str, Any] = {}
    container_strategy_seen: str | None = None
    strategies_logged = False

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
                container_candidates = [("heading_preferred_wrapper", preferred_wrapper)]
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
                            container_locator_strategy="heading_preferred_wrapper",
                            range_match_strategy="date_range_text_pattern",
                            download_locator_strategy="underline_link",
                        )
                        strategies_logged = True
                with contextlib.suppress(Exception):
                    await container.scroll_into_view_if_needed()
                rows = await _collect_report_request_rows(
                    container,
                    max_rows=12,
                    extra_containers=[
                        container.locator(":scope > section"),
                        container.locator(":scope > div"),
                    ],
                )
                sample: list[str] = []
                for row in rows:
                    with contextlib.suppress(Exception):
                        sample_text = await row.inner_text()
                        if sample_text:
                            sample.append(" ".join(sample_text.split()))
                if sample:
                    last_seen = sample[:10]
                    log_event(
                        logger=logger,
                        phase="iframe",
                        message="Observed Report Requests rows",
                        store_code=store.store_code,
                        rows=last_seen,
                        container_locator_strategy=container_strategy,
                    )
                diagnostics = {
                    "header_seen": header_seen,
                    "heading_texts": heading_texts or None,
                    "last_seen_rows": last_seen or None,
                    "container_locator_strategy": container_strategy,
                }
                return container, diagnostics
        except Exception:
            pass

        await asyncio.sleep(0.4)

    diagnostics = {"header_seen": header_seen, "heading_texts": heading_texts or None, "last_seen_rows": last_seen or None}
    log_event(
        logger=logger,
        phase="iframe",
        status="warn",
        message="Report Requests container not visible after request",
        store_code=store.store_code,
        timeout_ms=timeout_ms,
        last_seen_rows=last_seen or None,
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
) -> str | None:
    html = await _capture_outer_html(container)
    snippet = _truncate_html(html)
    log_event(
        logger=logger,
        phase="iframe",
        message="Captured Report Requests container outerHTML",
        store_code=store.store_code,
        label=label,
        outer_html_snippet=snippet,
        outer_html_length=len(html) if html else None,
    )
    return snippet


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
    container_html_snippet: str | None = None,
) -> tuple[bool, str | None, str | None, str | None]:
    poll_timeout_ms = min(REPORT_REQUEST_MAX_TIMEOUT_MS, timeout_ms)
    deadline = asyncio.get_event_loop().time() + (poll_timeout_ms / 1000)
    last_seen_texts: list[str] = []
    last_status: str | None = None
    backoff = 0.5
    matched_row_seen = False
    last_row_html: str | None = None
    last_range_match_strategy: str | None = "date_range_text_pattern"
    last_pending_log_at = 0.0
    pending_attempts = 0
    download_strategy: str | None = None

    while asyncio.get_event_loop().time() < deadline:
        try:
            with contextlib.suppress(Exception):
                await container.scroll_into_view_if_needed()
            rows = await _collect_report_request_rows(container, max_rows=25)
            visible_rows: list[str] = []
            matched_row: Locator | None = None
            matched_text: str | None = None
            for row in rows:
                date_range_text_raw = None
                with contextlib.suppress(Exception):
                    date_range_text_raw = await row.locator("div.w-1\\/5.text-sm.italic").first.inner_text()
                if not date_range_text_raw:
                    continue
                date_range_text = " ".join(date_range_text_raw.split())
                visible_rows.append(date_range_text)
                matches_expected = any(pattern.search(date_range_text) for pattern in range_patterns)
                if matches_expected:
                    matched_row = row
                    matched_text = date_range_text
                    with contextlib.suppress(Exception):
                        last_row_html = _truncate_html(await _capture_outer_html(row))
                    break
            if matched_row and matched_text:
                matched_row_seen = True
                last_seen_texts = visible_rows or [matched_text]
                status_text = await _extract_row_status_text(matched_row)
                if status_text:
                    last_status = status_text
                download_locator, download_strategy = await _first_visible_locator_with_label(
                    [("underline_link", matched_row.locator("a.text-sm.underline"))],
                    timeout_ms=800,
                )
                if download_locator:
                    log_event(
                        logger=logger,
                        phase="iframe",
                        message="Report Requests row ready for download",
                        store_code=store.store_code,
                        matched_range_text=matched_text,
                        status_text=last_status,
                        expected_range_texts=list(expected_range_texts),
                        all_row_texts=visible_rows or [matched_text],
                        download_control_visible=True,
                        matched_row_text_full=matched_text,
                        row_html_snippet=last_row_html,
                        range_match_strategy=last_range_match_strategy,
                        download_locator_strategy=download_strategy,
                        container_locator_strategy="heading_preferred_wrapper",
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
                            container_locator_strategy="heading_preferred_wrapper",
                        )
                        return True, str(download_path), matched_text, last_status
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
                        )
                        return False, None, matched_text, last_status

                if status_text and "pending" in status_text.lower():
                    pending_attempts += 1
                    now = asyncio.get_event_loop().time()
                    if now - last_pending_log_at >= REPORT_REQUEST_POLL_LOG_INTERVAL_SECONDS or pending_attempts == 1:
                        log_event(
                            logger=logger,
                            phase="iframe",
                            message="Report Requests row pending; retrying download poll",
                            store_code=store.store_code,
                            matched_range_text=matched_text,
                            status_text=status_text,
                            expected_range_texts=list(expected_range_texts),
                            backoff_seconds=backoff,
                            timeout_ms=poll_timeout_ms,
                            last_seen_rows=last_seen_texts or [matched_text],
                            row_html_snippet=last_row_html,
                            range_match_strategy=last_range_match_strategy,
                            container_locator_strategy="heading_preferred_wrapper",
                        )
                        last_pending_log_at = now
                else:
                    break
            if visible_rows:
                last_seen_texts = visible_rows
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
        last_seen_rows=last_seen_texts or None,
        last_status=last_status,
        row_seen=matched_row_seen,
        timeout_ms=poll_timeout_ms,
        row_html_snippet=last_row_html,
        container_html_snippet=container_html_snippet,
        range_match_strategy=last_range_match_strategy,
    )
    return False, None, None, last_status


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
        return False, "Download Historical Report control not visible inside iframe"

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
        return False, "Generate Report control not visible inside iframe"

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
        return False, "Request Report control not visible inside iframe"

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

    container_html_snippet = None
    with contextlib.suppress(Exception):
        container_html_snippet = await _log_report_requests_dom(
            container_locator,
            logger=logger,
            store=store,
            label="report_requests_after_request",
        )
    filename = _format_orders_filename(store.store_code, from_date, to_date)
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
        container_html_snippet=container_html_snippet or container_diagnostics.get("container_html_snippet"),
    )
    if not downloaded:
        return False, last_status or "Matching Report Requests row not ready for download"

    return True, downloaded_path or str(target_path)


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
    run_start_date: date,
    run_end_date: date,
    nav_timeout_ms: int,
    summary: TdOrdersDiscoverySummary,
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
    outcome = StoreOutcome(status="error", message="Store run did not complete")
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

        container_ready = await _navigate_to_orders_container(
            page, store=store, logger=store_logger, nav_selector=nav_selector, nav_timeout_ms=nav_timeout_ms
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
                log_event(
                    logger=store_logger,
                    phase="iframe",
                    message="Orders iframe flow completed",
                    store_code=store.store_code,
                    download_path=detail,
                )
                outcome = StoreOutcome(
                    status="ok",
                    message="Orders report downloaded",
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
        summary.record_store(store.store_code, outcome)
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
    return parser


async def _async_entrypoint(argv: Sequence[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    await main(run_env=args.run_env, run_id=args.run_id, from_date=args.from_date, to_date=args.to_date)


def run(argv: Sequence[str] | None = None) -> None:
    asyncio.run(_async_entrypoint(argv))


if __name__ == "__main__":  # pragma: no cover
    run()
