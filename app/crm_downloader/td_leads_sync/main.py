from __future__ import annotations

import argparse
import asyncio
import contextlib
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import openpyxl
from playwright.async_api import Browser, BrowserContext, Page, async_playwright

from app.common.date_utils import aware_now, get_timezone
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
    "pickup_no": ("pickup no.", "pickup no", "pickupno"),
    "customer_name": ("customer name", "name"),
    "address": ("address",),
    "mobile": ("mobile", "mobile no", "mobile no."),
    "pickup_date": ("pickup date", "pickup created date", "created date"),
    "pickup_time": ("pickup time", "time"),
    "special_instruction": ("special instruction", "instruction"),
    "status_text": ("status",),
    "reason": ("reason",),
    "source": ("source",),
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
    "pickup_time",
    "special_instruction",
    "status_text",
    "reason",
    "source",
    "user",
    "scraped_at",
]


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

    def build_record(self, *, finished_at: datetime) -> dict[str, Any]:
        elapsed = max(0, int((finished_at - self.started_at).total_seconds()))
        hh, mm, ss = elapsed // 3600, (elapsed % 3600) // 60, elapsed % 60
        total_time_taken = f"{hh:02d}:{mm:02d}:{ss:02d}"

        store_rows_payload = [
            {
                "store_code": result.store_code,
                "status": result.status,
                "status_counts": dict(result.status_counts),
                "rows_count": len(result.rows),
                "warnings": list(result.warnings),
                "artifact_path": result.artifact_path,
                "ingested_rows": result.ingested_rows,
                "rows": [
                    {
                        "status": row.get("status_bucket"),
                        "customer_name": row.get("customer_name"),
                        "mobile": row.get("mobile"),
                        "pickup_created_date": row.get("pickup_date"),
                    }
                    for row in result.rows
                ],
            }
            for result in self.store_results.values()
        ]

        summary_lines = [
            "TD CRM Leads Sync Summary",
            f"Run ID: {self.run_id}",
            f"Env: {self.run_env}",
            f"Report Date: {self.report_date.isoformat()}",
            f"Overall Status: {self.overall_status()}",
            f"Total Rows: {self.total_rows()}",
            "",
            "Store Details:",
        ]
        for result in sorted(self.store_results.values(), key=lambda item: item.store_code):
            summary_lines.append(
                f"- {result.store_code}: status={result.status}, rows={len(result.rows)}, "
                f"pending={result.status_counts.get('pending', 0)}, "
                f"completed={result.status_counts.get('completed', 0)}, "
                f"cancelled={result.status_counts.get('cancelled', 0)}"
            )
            for row in result.rows:
                summary_lines.append(
                    "  "
                    f"status={row.get('status_bucket')}, "
                    f"customer_name={row.get('customer_name') or ''}, "
                    f"mobile={row.get('mobile') or ''}, "
                    f"pickup_created_date={row.get('pickup_date') or ''}"
                )
            if result.warnings:
                for warning in result.warnings:
                    summary_lines.append(f"  warning={warning}")

        return {
            "pipeline_name": PIPELINE_NAME,
            "run_id": self.run_id,
            "run_env": self.run_env,
            "started_at": self.started_at,
            "finished_at": finished_at,
            "total_time_taken": total_time_taken,
            "report_date": self.report_date,
            "overall_status": self.overall_status(),
            "summary_text": "\n".join(summary_lines),
            "phases_json": _normalize_json_safe({"store": {"ok": 0, "warning": 0, "error": 0}}),
            "metrics_json": _normalize_json_safe(
                {
                    "total_rows": self.total_rows(),
                    "stores": store_rows_payload,
                }
            ),
            "created_at": self.started_at,
        }


async def _persist_run_summary(*, logger: JsonLogger, summary: LeadsRunSummary, finished_at: datetime) -> bool:
    if not config.database_url:
        log_event(
            logger=logger,
            phase="run_summary",
            status="warning",
            message="Skipping run summary persistence because database_url is missing",
            run_id=summary.run_id,
        )
        return False

    record = summary.build_record(finished_at=finished_at)
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
    url_pattern = re.compile(r".*/frmHomePickUpScheduler(?:\\.aspx)?(?:\\?.*)?$", re.IGNORECASE)
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
        """
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
        """
        ({ gridSelector }) => {
          const table = document.querySelector(gridSelector);
          if (!table) {
            return [];
          }
          const links = Array.from(table.querySelectorAll('a[href*="Page$"]'));
          const args = [];
          for (const link of links) {
            const href = link.getAttribute('href') || '';
            const match = href.match(/Page\$[0-9]+/i);
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
        """
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
    return " ".join(str(header or "").strip().lower().replace(".", "").split())


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
            row = {
                "store_code": store_code,
                "status_bucket": status_bucket,
                "pickup_id": str(raw_row.get("pickup_id") or "").strip() or None,
                "pickup_no": _field_from_headers(headers=headers, values=values, field_name="pickup_no"),
                "customer_name": _field_from_headers(headers=headers, values=values, field_name="customer_name"),
                "address": _field_from_headers(headers=headers, values=values, field_name="address"),
                "mobile": _field_from_headers(headers=headers, values=values, field_name="mobile"),
                "pickup_date": _field_from_headers(headers=headers, values=values, field_name="pickup_date"),
                "pickup_time": _field_from_headers(headers=headers, values=values, field_name="pickup_time"),
                "special_instruction": _field_from_headers(headers=headers, values=values, field_name="special_instruction"),
                "status_text": _field_from_headers(headers=headers, values=values, field_name="status_text"),
                "reason": _field_from_headers(headers=headers, values=values, field_name="reason"),
                "source": _field_from_headers(headers=headers, values=values, field_name="source"),
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


def _write_store_artifact(*, store_code: str, rows: Sequence[Mapping[str, Any]], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{store_code}-crm_leads.xlsx"

    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "crm_leads"
    sheet.append(OUTPUT_COLUMNS)
    for row in rows:
        sheet.append([row.get(column) for column in OUTPUT_COLUMNS])
    workbook.save(output_path)
    return output_path


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
        )

        ingest_result: TdLeadsIngestResult | None = None
        if config.database_url:
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


async def main(
    *,
    run_env: str | None = None,
    run_id: str | None = None,
    store_codes: Sequence[str] | None = None,
) -> None:
    resolved_run_id = run_id or new_run_id()
    resolved_run_env = run_env or config.run_env
    report_date = aware_now(get_timezone()).date()
    logger = get_logger(run_id=resolved_run_id)
    summary = LeadsRunSummary(run_id=resolved_run_id, run_env=resolved_run_env, report_date=report_date)

    await _start_run_summary(logger=logger, summary=summary)

    stores = await _load_td_order_stores(logger=logger, store_codes=store_codes)
    if not stores:
        log_event(
            logger=logger,
            phase="init",
            status="warning",
            message="No TD stores with sync_orders_flag found; exiting",
        )

    async with async_playwright() as playwright:
        browser = await launch_browser(playwright=playwright, logger=logger)
        try:
            for store in stores:
                store_logger = logger.bind(store_code=store.store_code)
                store_result = await _run_store(
                    browser=browser,
                    store=store,
                    run_id=resolved_run_id,
                    run_env=resolved_run_env,
                    logger=store_logger,
                )
                summary.store_results[store.store_code] = store_result
        finally:
            with contextlib.suppress(Exception):
                await browser.close()

    finished_at = datetime.now(timezone.utc)
    persisted = await _persist_run_summary(logger=logger, summary=summary, finished_at=finished_at)
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
    return parser


async def _async_entrypoint(argv: Sequence[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    await main(
        run_env=args.run_env,
        run_id=args.run_id,
        store_codes=args.store_codes,
    )


def run(argv: Sequence[str] | None = None) -> None:
    asyncio.run(_async_entrypoint(argv))


if __name__ == "__main__":  # pragma: no cover
    run()
