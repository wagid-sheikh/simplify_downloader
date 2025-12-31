from __future__ import annotations

import argparse
import asyncio
import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence

import sqlalchemy as sa
from playwright.async_api import Browser, FrameLocator, Page, TimeoutError, async_playwright

from app.common.date_utils import get_daily_report_date
from app.common.db import session_scope
from app.config import config
from app.crm_downloader.config import default_profiles_dir
from app.dashboard_downloader.json_logger import JsonLogger, get_logger, log_event, new_run_id
from app.dashboard_downloader.notifications import send_notifications_for_run

PIPELINE_NAME = "td_orders_sync"


async def main(
    *,
    run_env: str | None = None,
    run_id: str | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> None:
    """Run the TD Orders discovery flow (login + iframe readiness only)."""

    resolved_run_id = run_id or new_run_id()
    resolved_env = run_env or config.run_env
    run_start_date = from_date or get_daily_report_date()
    run_end_date = to_date or run_start_date
    logger = get_logger(run_id=resolved_run_id)

    log_event(
        logger=logger,
        phase="init",
        message="Starting TD orders sync discovery flow",
        run_env=resolved_env,
        from_date=run_start_date,
        to_date=run_end_date,
    )

    stores = await _load_td_order_stores(logger=logger)
    if not stores:
        log_event(
            logger=logger,
            phase="init",
            status="warn",
            message="No TD stores with sync_orders_flag found; exiting",
        )
        await send_notifications_for_run(PIPELINE_NAME, resolved_run_id)
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=config.etl_headless)
        for store in stores:
            await _run_store_discovery(
                browser=browser,
                store=store,
                logger=logger,
                run_env=resolved_env,
                run_start_date=run_start_date,
                run_end_date=run_end_date,
            )

        await browser.close()

    log_event(
        logger=logger,
        phase="notifications",
        message="TD orders sync discovery flow complete; notifying",
        run_env=resolved_env,
    )
    await send_notifications_for_run(PIPELINE_NAME, resolved_run_id)


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


@dataclass
class TdStore:
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

    log_event(
        logger=logger,
        phase="init",
        message="Loaded TD store rows",
        store_count=len(stores),
        stores=[store.store_code for store in stores],
    )
    return stores


# ── Playwright helpers ───────────────────────────────────────────────────────


async def _probe_session(page: Page, *, store: TdStore, logger: JsonLogger) -> bool:
    target_url = store.home_url or store.orders_url or store.login_url
    if not target_url:
        log_event(
            logger=logger,
            phase="session",
            status="warn",
            message="No target URL available to probe session",
            store_code=store.store_code,
        )
        return False

    response = await page.goto(target_url, wait_until="domcontentloaded")
    final_url = page.url
    contains_store = store.store_code.lower() in (final_url or "").lower()
    log_event(
        logger=logger,
        phase="session",
        status="ok" if contains_store else "warn",
        message="Probed session with existing storage state",
        store_code=store.store_code,
        response_status=getattr(response, "status", None),
        final_url=final_url,
        contains_store_code=contains_store,
    )
    return contains_store


async def _perform_login(page: Page, *, store: TdStore, logger: JsonLogger) -> bool:
    missing_fields = [
        label
        for label, value in (
            ("login_url", store.login_url),
            ("username", store.username),
            ("password", store.password),
            ("username_selector", store.login_selectors.get("username")),
            ("password_selector", store.login_selectors.get("password")),
            ("submit_selector", store.login_selectors.get("submit")),
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

    await page.goto(store.login_url, wait_until="domcontentloaded")
    await page.fill(store.login_selectors["username"], store.username or "")
    await page.fill(store.login_selectors["password"], store.password or "")

    store_selector = store.login_selectors.get("store_code")
    if store_selector:
        await page.fill(store_selector, store.store_code)

    await page.click(store.login_selectors["submit"])
    try:
        await page.wait_for_load_state("networkidle", timeout=15_000)
    except TimeoutError:
        log_event(
            logger=logger,
            phase="login",
            status="warn",
            message="Navigation after login did not reach network idle",
            store_code=store.store_code,
            final_url=page.url,
        )

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
    )
    return contains_store


async def _wait_for_iframe(page: Page, *, store: TdStore, logger: JsonLogger) -> FrameLocator | None:
    if not store.orders_url:
        log_event(
            logger=logger,
            phase="orders",
            status="warn",
            message="Orders URL missing in sync_config; skipping iframe check",
            store_code=store.store_code,
        )
        return None

    response = await page.goto(store.orders_url, wait_until="domcontentloaded")
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
            response_status=getattr(response, "status", None),
        )
        return None

    log_event(
        logger=logger,
        phase="orders",
        message="Orders container ready; iframe attached",
        store_code=store.store_code,
        final_url=page.url,
        response_status=getattr(response, "status", None),
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


async def _run_store_discovery(
    *,
    browser: Browser,
    store: TdStore,
    logger: JsonLogger,
    run_env: str,
    run_start_date: date,
    run_end_date: date,
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
    context = await browser.new_context(storage_state=str(store.storage_state_path) if storage_state_exists else None)
    page = await context.new_page()
    try:
        session_reused = False
        if storage_state_exists:
            session_reused = await _probe_session(page, store=store, logger=store_logger)

        if not session_reused:
            if storage_state_exists:
                await context.close()
                context = await browser.new_context()
                page = await context.new_page()
            log_event(
                logger=store_logger,
                phase="login",
                message="Attempting full login with provided credentials",
                store_code=store.store_code,
                storage_state=str(store.storage_state_path),
            )
            session_reused = await _perform_login(page, store=store, logger=store_logger)
            if session_reused:
                store.storage_state_path.parent.mkdir(parents=True, exist_ok=True)
                await context.storage_state(path=str(store.storage_state_path))
                log_event(
                    logger=store_logger,
                    phase="session",
                    message="Stored refreshed session state",
                    store_code=store.store_code,
                    storage_state=str(store.storage_state_path),
                )

        iframe_locator = await _wait_for_iframe(page, store=store, logger=store_logger)
        if iframe_locator is not None:
            await _observe_iframe_hydration(iframe_locator, store=store, logger=store_logger)

        log_event(
            logger=store_logger,
            phase="store",
            message="Completed TD orders store discovery",
            store_code=store.store_code,
            session_reused=session_reused,
        )
    except Exception as exc:  # pragma: no cover - runtime safeguard
        log_event(
            logger=store_logger,
            phase="store",
            status="error",
            message="TD orders discovery failed",
            store_code=store.store_code,
            error=str(exc),
        )
    finally:
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
