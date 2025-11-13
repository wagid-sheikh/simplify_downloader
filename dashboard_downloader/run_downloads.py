from __future__ import annotations

import csv
from pathlib import Path
import re
from typing import Iterable
from typing import Dict, List
from datetime import datetime

from playwright.async_api import async_playwright, Page

from common.ingest.service import _looks_like_html

from . import page_selectors
from .config import (
    STORES,
    PKG_ROOT,
    DATA_DIR,
    FILE_SPECS,
    MERGED_NAMES,
)
from .json_logger import JsonLogger, log_event


def _normalize_html_tokens(html: str) -> str:
    return html.lower().replace("'", '"')


_ATTR_SELECTOR_PATTERN = re.compile(r"\[([^=\]\s]+)\s*=\s*['\"]?([^'\"\]]+)['\"]?\]")
_ID_SELECTOR_PATTERN = re.compile(r"#([A-Za-z0-9_-]+)")


def _tokens_from_selector(selector: str) -> Iterable[str]:
    tokens: set[str] = set()
    for segment in selector.split(","):
        part = segment.strip()
        if not part:
            continue

        for attr, value in _ATTR_SELECTOR_PATTERN.findall(part):
            tokens.add(f'{attr.lower()}="{value}"')

        for match in _ID_SELECTOR_PATTERN.findall(part):
            tokens.add(f'id="{match}"')

    return tokens


def _login_tokens() -> Iterable[str]:
    selector_tokens = set()
    selector_tokens.update(_tokens_from_selector(page_selectors.LOGIN_USERNAME))
    selector_tokens.update(_tokens_from_selector(page_selectors.LOGIN_PASSWORD))
    selector_tokens.update(_tokens_from_selector(page_selectors.LOGIN_SUBMIT))

    # Historical fallbacks for older login forms and to handle non-selector
    # heuristics in HTML snippets.
    selector_tokens.update(
        {
            'name="username"',
            'id="username"',
            'name="login"',
            'id="login"',
        }
    )

    return tuple(sorted(selector_tokens))


def _looks_like_login_html_text(html: str) -> bool:
    if not html:
        return False

    normalized = _normalize_html_tokens(html)

    has_password_field = "type=\"password\"" in normalized or "name=\"password\"" in normalized
    if not has_password_field:
        return False

    if any(token in normalized for token in _login_tokens()):
        return True

    return "login" in normalized or "log in" in normalized or "sign in" in normalized


def _looks_like_login_html_bytes(payload: bytes) -> bool:
    if not payload:
        return False

    snippet = payload[:4096]
    try:
        decoded = snippet.decode("utf-8", errors="ignore")
    except Exception:  # pragma: no cover - extremely defensive
        return False

    return _looks_like_login_html_text(decoded)

def _ensure_profile_dir(store_name: str) -> Path:
    p = PKG_ROOT / "profiles" / store_name
    p.mkdir(parents=True, exist_ok=True)
    return p

def _render(template: str, sc: str) -> str:
    return template.format(sc=sc, ymd=datetime.now().strftime("%Y%m%d"))

async def _download_one_spec(page: Page, store_cfg: Dict, spec: Dict, *, logger: JsonLogger) -> Path | None:
    sc = store_cfg["store_code"]
    url = _render(spec["url_template"], sc)
    out_name = _render(spec["out_name_template"], sc)
    final_path = DATA_DIR / out_name

    def _log(status: str, message: str, *, extras: Dict | None = None) -> None:
        log_event(
            logger=logger,
            phase="download",
            status=status,
            store_code=sc,
            bucket=None,
            message=message,
            extras={"url": url, **(extras or {})},
        )

    attempted_refresh = False

    while True:
        try:
            response = await page.context.request.get(url, timeout=60_000)
        except Exception as exc:
            _log("error", f"request failed for {spec['key']}", extras={"error": str(exc)})
            return None

        status = response.status
        body = await response.body()

        if status == 200 and body and not _looks_like_login_html_bytes(body):
            final_path.parent.mkdir(parents=True, exist_ok=True)
            final_path.write_bytes(body)
            return final_path

        needs_refresh = False

        if status in {401, 403}:
            _log("warn", f"received {status} for {spec['key']} — refreshing session")
            needs_refresh = True
        elif not body:
            _log("warn", f"empty response for {spec['key']}")
        elif _looks_like_login_html_bytes(body):
            _log(
                "warn",
                f"html response for {spec['key']} — authentication likely expired",
            )
            needs_refresh = True
        else:
            _log("error", f"unexpected status {status} for {spec['key']}")

        if not needs_refresh:
            return None

        if attempted_refresh:
            _log("error", f"retry after session refresh failed for {spec['key']}")
            return None

        attempted_refresh = True

        try:
            await _ensure_dashboard(page, store_cfg, logger)
        except Exception as exc:
            _log("error", f"session refresh failed for {spec['key']}", extras={"error": str(exc)})
            return None


async def _is_login_page(page: Page) -> bool:
    """Heuristic check to determine whether the current page is the login form."""

    url = (page.url or "").lower()
    if "login" in url:
        return True

    # Primary signal: does our explicit username locator resolve?
    try:
        locator = page.locator(page_selectors.LOGIN_USERNAME)
        if await locator.count() > 0:
            return True
    except Exception:  # pragma: no cover - defensive; locator failures shouldn't break flow
        pass

    # Fallback: inspect the rendered HTML for common login markers. Some environments
    # return the login form HTML while keeping the original dashboard URL, so we need
    # to look at the content directly.
    try:
        content = (await page.content()).lower()
    except Exception:  # pragma: no cover - if Playwright can't give us content just bail
        return False

    if not content:
        return False

    return _looks_like_login_html_text(content)


async def _ensure_dashboard(page: Page, store_cfg: Dict, logger: JsonLogger) -> None:
    """Navigate to the dashboard, refreshing the login session when required."""

    dashboard_url = store_cfg["dashboard_url"]
    await page.goto(dashboard_url, wait_until="domcontentloaded")

    if not await _is_login_page(page):
        return

    username = store_cfg.get("username")
    password = store_cfg.get("password")
    if not username or not password:
        raise RuntimeError(f"Missing credentials for store_code={store_cfg.get('store_code')}")

    log_event(
        logger=logger,
        phase="download",
        status="warn",
        store_code=store_cfg.get("store_code"),
        bucket=None,
        message="session expired; attempting re-login",
    )

    await page.fill(page_selectors.LOGIN_USERNAME, username)
    await page.fill(page_selectors.LOGIN_PASSWORD, password)
    await page.click(page_selectors.LOGIN_SUBMIT)
    await page.wait_for_load_state("networkidle")
    await page.goto(dashboard_url, wait_until="networkidle")

    if await _is_login_page(page):
        log_event(
            logger=logger,
            phase="download",
            status="error",
            store_code=store_cfg.get("store_code"),
            bucket=None,
            message="login failed; still on login page",
        )
        raise RuntimeError("Automated login failed; manual login required")

    log_event(
        logger=logger,
        phase="download",
        store_code=store_cfg.get("store_code"),
        bucket=None,
        message="session refreshed via login",
    )

def _merge_bucket(files: List[Path], output: Path) -> None:
    """
    Simple CSV merge: writes header from the first file, then appends rows from all files.
    Skips empty/missing files gracefully.
    """
    if not files:
        print(f"[merge] No files to merge for {output.name}")
        return

    written_header = False
    rows_written = 0

    with output.open("w", newline="", encoding="utf-8") as out_f:
        writer = None

        for f in files:
            if not f or not f.exists() or f.stat().st_size == 0:
                continue

            if _looks_like_html(f):
                continue

            with f.open("r", newline="", encoding="utf-8", errors="ignore") as in_f:
                reader = csv.reader(in_f)
                try:
                    header = next(reader)
                except StopIteration:
                    continue

                if not written_header:
                    writer = csv.writer(out_f)
                    writer.writerow(header)
                    written_header = True

                for row in reader:
                    writer.writerow(row)
                    rows_written += 1

    _ = rows_written


def _count_rows(csv_path: Path) -> int:
    if not csv_path.exists():
        return 0

    if _looks_like_html(csv_path):
        return 0

    with csv_path.open("r", newline="", encoding="utf-8", errors="ignore") as handle:
        reader = csv.reader(handle)
        try:
            next(reader)
        except StopIteration:
            return 0
        return sum(1 for _ in reader)

def filter_merged_missed_leads(input_path: Path, output_path: Path) -> None:
    """
    Reads merged_missed_leads_YYYYMMDD.csv, removes:
      - rows where is_order_placed == 1 (accepts 1, "1", "1.0", etc.)
      - rows where customer_type == 'Existing' (case-insensitive, trimmed)
    Writes filtered CSV to filtered_merged_missed_leads_YYYYMMDD.csv.
    Gracefully handles missing columns by copying input to output unchanged.
    """
    if not input_path.exists() or input_path.stat().st_size == 0:
        print(f"[filter] Input missing or empty, skipping: {input_path.name}")
        return

    with input_path.open("r", newline="", encoding="utf-8", errors="ignore") as fin:
        reader = csv.reader(fin)
        try:
            header = next(reader)
        except StopIteration:
            print(f"[filter] No rows in {input_path.name}")
            return

        # Column indices (fallback to None if not present)
        def idx(col: str) -> int | None:
            try:
                return header.index(col)
            except ValueError:
                return None

        i_is_order_placed = idx("is_order_placed")
        i_customer_type   = idx("customer_type")

        # If required columns aren’t present, just copy input -> output
        if i_is_order_placed is None or i_customer_type is None:
            print(f"[filter] Required columns missing; copying {input_path.name} unchanged.")
            with input_path.open("r", encoding="utf-8", errors="ignore") as src, \
                 output_path.open("w", encoding="utf-8", newline="") as dst:
                dst.write(src.read())
            return

        rows_out = []
        for row in reader:
            # Defensive: pad short rows
            if len(row) <= max(i_is_order_placed, i_customer_type):
                continue

            raw_is_placed = (row[i_is_order_placed] or "").strip().lower()
            raw_cust_type = (row[i_customer_type] or "").strip().lower()

            # Normalize is_order_placed to numeric-ish
            is_placed = raw_is_placed in {"1", "1.0", "true", "yes"}  # treat these as placed
            is_existing = (raw_cust_type == "existing")

            if is_placed:
                continue
            if is_existing:
                continue

            rows_out.append(row)

    with output_path.open("w", newline="", encoding="utf-8") as fout:
        writer = csv.writer(fout)
        writer.writerow(header)
        writer.writerows(rows_out)

    print(f"[filter] {output_path.name} — kept {len(rows_out)} rows after filtering.")

async def run_all_stores(
    stores: Dict[str, dict] | None = None,
    logger: JsonLogger | None = None,
) -> Dict[str, Dict[str, Dict[str, object]]]:
    """
    Opens a persistent profile for each store, goes to the store's TMS dashboard,
    downloads all FILE_SPECS where download=True, saves into dashboard_downloader/data/,
    and finally performs merges per merge_bucket.
    """
    logger = logger or JsonLogger()
    merged_buckets: Dict[str, List[Path]] = {}
    download_counts: Dict[str, Dict[str, Dict[str, object]]] = {}

    async with async_playwright() as p:
        # NOTE: On macOS we can keep channel="chrome" if you prefer;
        # leaving it off keeps it portable for Linux server (Chromium).
        for store_name, cfg in (stores or STORES).items():
            user_dir = _ensure_profile_dir(store_name)
            sc = cfg["store_code"]
            dashboard_url = cfg["dashboard_url"]

            ctx = await p.chromium.launch_persistent_context(
                user_data_dir=str(user_dir),
                headless=True,
                accept_downloads=True,
                # macOS dev: uncomment next line to force system Chrome
                channel="chrome",
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
            )

            try:
                page = await ctx.new_page()
                # Hit the dashboard and automatically refresh the session when needed.
                await _ensure_dashboard(page, cfg, logger)

                log_event(
                    logger=logger,
                    phase="download",
                    message="store dashboard reached",
                    store_code=sc,
                    bucket=None,
                    extras={"dashboard_url": dashboard_url},
                )

                for spec in FILE_SPECS:
                    if not spec.get("download", True):
                        continue

                    saved = await _download_one_spec(page, cfg, spec, logger=logger)
                    if saved and spec.get("merge_bucket"):
                        bucket = spec["merge_bucket"]
                        merged_buckets.setdefault(bucket, []).append(saved)
                        download_counts.setdefault(bucket, {})[sc] = {
                            "rows": _count_rows(saved),
                            "path": str(saved),
                        }

                log_event(
                    logger=logger,
                    phase="download",
                    message="store download completed",
                    store_code=sc,
                    bucket=None,
                )

            finally:
                await ctx.close()

    # ---- Merges (by bucket) ----
    for bucket, files in merged_buckets.items():
        out_name = MERGED_NAMES.get(bucket, f"Merged_{bucket}_{datetime.now().strftime('%Y%m%d')}.csv")
        out_path = DATA_DIR / out_name
        _merge_bucket(files, out_path)
        log_event(
            logger=logger,
            phase="merge",
            bucket=bucket,
            merged_file=str(out_path),
            counts={
                "download_total": sum(
                    entry["rows"] for entry in download_counts.get(bucket, {}).values()
                ),
                "merged_rows": _count_rows(out_path),
            },
            message="merge complete",
        )
        download_counts.setdefault(bucket, {})["__merged__"] = {
            "rows": _count_rows(out_path),
            "path": str(out_path),
        }

        # Auto-filter for missed_leads bucket
        if bucket == "missed_leads":
            filtered_name = f"filtered_merged_missed_leads_{datetime.now().strftime('%Y%m%d')}.csv"
            filtered_path = DATA_DIR / filtered_name
            filter_merged_missed_leads(out_path, filtered_path)

    return download_counts

if __name__ == "__main__":
    import asyncio

    asyncio.run(run_all_stores())

