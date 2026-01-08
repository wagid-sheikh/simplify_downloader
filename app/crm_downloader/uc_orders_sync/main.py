from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Sequence

import sqlalchemy as sa
from playwright.async_api import Browser, Page, TimeoutError, async_playwright

from app.common.date_utils import get_daily_report_date
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
CONTROL_CUES = {
    "start_date": ["Start Date", "From Date", "From"],
    "end_date": ["End Date", "To Date", "To"],
    "apply": ["Apply", "Search", "Submit", "Go"],
    "export_report": ["Export Report", "Export", "Download", "Export GST"],
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
    def username(self) -> str | None:
        return _get_nested_str(self.sync_config, ("username",))

    @property
    def password(self) -> str | None:
        return _get_nested_str(self.sync_config, ("password",))

    @property
    def login_selectors(self) -> Dict[str, str]:
        selectors = _coerce_dict(self.sync_config.get("login_selector"))
        return {key: value for key, value in selectors.items() if isinstance(value, str) and value.strip()}


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
    run_start_date = from_date or get_daily_report_date()
    run_end_date = to_date or run_start_date
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

        await page.goto(store.orders_url, wait_until="domcontentloaded")
        await _wait_for_gst_report_ready(page=page, logger=logger, store=store)
        selectors_payload = await _discover_selector_cues(page)
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

    selectors = {
        "username": _normalize_id_selector(store.login_selectors.get("username", "")),
        "password": _normalize_id_selector(store.login_selectors.get("password", "")),
        "submit": _normalize_id_selector(store.login_selectors.get("submit", "")),
    }
    missing_selectors = [key for key, value in selectors.items() if not value]
    if missing_selectors:
        log_event(
            logger=logger,
            phase="login",
            status="error",
            message="Login selectors missing",
            store_code=store.store_code,
            missing_selectors=missing_selectors,
        )
        return False

    await page.goto(store.login_url or "", wait_until="domcontentloaded")
    try:
        await page.wait_for_selector(selectors["username"], timeout=NAV_TIMEOUT_MS)
    except TimeoutError:
        log_event(
            logger=logger,
            phase="login",
            status="error",
            message="Username selector not found within timeout",
            store_code=store.store_code,
            selector=selectors["username"],
            timeout_ms=NAV_TIMEOUT_MS,
        )
        return False

    await page.fill(selectors["username"], store.username or "")
    await page.fill(selectors["password"], store.password or "")

    try:
        await page.click(selectors["submit"])
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
    return True


async def _wait_for_gst_report_ready(*, page: Page, logger: JsonLogger, store: UcStore) -> None:
    await page.wait_for_load_state("domcontentloaded")
    spinner_selector = ", ".join(SPINNER_CSS_SELECTORS)
    readiness_selector = "text=/Start Date|End Date|Export Report|Apply/i"

    tasks = []
    if spinner_selector:
        tasks.append(
            asyncio.create_task(
                page.wait_for_selector(spinner_selector, state="hidden", timeout=NAV_TIMEOUT_MS)
            )
        )
    tasks.append(
        asyncio.create_task(page.wait_for_selector(readiness_selector, timeout=NAV_TIMEOUT_MS))
    )

    done, pending = await asyncio.wait(
        tasks,
        return_when=asyncio.FIRST_COMPLETED,
        timeout=NAV_TIMEOUT_MS / 1000,
    )
    for task in pending:
        task.cancel()

    if not done:
        log_event(
            logger=logger,
            phase="navigation",
            status="warn",
            message="GST report readiness check timed out",
            store_code=store.store_code,
            selector=readiness_selector,
        )
        return

    log_event(
        logger=logger,
        phase="navigation",
        message="GST report readiness signal detected",
        store_code=store.store_code,
    )


async def _discover_selector_cues(page: Page) -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    for label, cues in CONTROL_CUES.items():
        matches: list[Dict[str, Any]] = []
        for cue in cues:
            selector = f"text=/{re.escape(cue)}/i"
            locator = page.locator(selector)
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
