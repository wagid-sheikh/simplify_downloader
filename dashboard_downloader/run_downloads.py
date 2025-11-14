from __future__ import annotations

import csv
import json
import os
from pathlib import Path
import re
from typing import Any, Dict, Iterable, List
from datetime import datetime
from urllib.parse import urlparse

import magic

from playwright.async_api import (
    async_playwright,
    BrowserContext,
    Page,
    TimeoutError as PlaywrightTimeoutError,
)

from common.ingest.service import _looks_like_html

from . import page_selectors
from .config import (
    DEFAULT_STORE_CODES,
    PKG_ROOT,
    DATA_DIR,
    FILE_SPECS,
    HOME_URL,
    MERGED_NAMES,
    LOGIN_URL,
    stores_from_list,
    storage_state_path,
    tms_dashboard_url,
)
from .settings import PipelineSettings
from .json_logger import JsonLogger, log_event


DASHBOARD_DOWNLOAD_CONTROL_TIMEOUT_MS = 90_000


def _normalize_url_path(path: str | None) -> str:
    if not path:
        return "/"
    normalized = path.strip()
    if not normalized.startswith("/"):
        normalized = f"/{normalized}"
    normalized = normalized.rstrip("/")
    return normalized or "/"


_LOGIN_URL_PARTS = urlparse(LOGIN_URL)
_LOGIN_HOST = (_LOGIN_URL_PARTS.hostname or "").lower()
_LOGIN_PATH = _normalize_url_path(_LOGIN_URL_PARTS.path)


def _validate_downloaded_csv(csv_path: Path, *, body: bytes) -> tuple[bool, str | None]:
    try:
        mime_type = magic.from_buffer(body, mime=True)
    except Exception as exc:
        return False, f"unable to detect MIME type: {exc}"

    if not mime_type:
        return False, "missing MIME type"

    normalized_mime = mime_type.lower()
    if not any(hint in normalized_mime for hint in ("csv", "excel", "plain")):
        return False, f"unexpected MIME type: {mime_type}"

    try:
        with csv_path.open("r", newline="", encoding="utf-8", errors="ignore") as handle:
            sample = handle.read(2048)
            if not sample or not sample.strip():
                return False, "empty or whitespace-only content"
            handle.seek(0)
            try:
                sniff_sample = sample if sample.endswith("\n") else f"{sample}\n"
                csv.Sniffer().sniff(sniff_sample)
            except csv.Error as exc:
                return False, f"csv sniff failed: {exc}"
    except OSError as exc:
        return False, f"unable to read file: {exc}"

    return True, None


class SkipStoreDashboardError(Exception):
    """Raised when a store's dashboard cannot be used (no controls, no creds, etc.)."""
    pass


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

    return tuple(sorted(selector_tokens))


_LOGIN_ERROR_PATTERNS = (
    "invalid username",
    "invalid password",
    "incorrect username",
    "incorrect password",
    "need to login",
    "need to log in",
    "need to be logged in",
    "you need to be logged in",
    "please login",
    "please log in",
)


def _looks_like_login_html_text(html: str) -> bool:
    if not html:
        return False

    error_hint = _extract_login_error(html)
    if error_hint:
        return True

    normalized = _normalize_html_tokens(html)

    has_password_field = "type=\"password\"" in normalized or "name=\"password\"" in normalized
    if has_password_field and any(token in normalized for token in _login_tokens()):
        return True

    return False


def _extract_login_error(html: str | None) -> str | None:
    if not html:
        return None

    normalized = _normalize_html_tokens(html)
    for pattern in _LOGIN_ERROR_PATTERNS:
        if pattern in normalized:
            return pattern
    return None


def _looks_like_login_html_bytes(payload: bytes) -> bool:
    if not payload:
        return False

    snippet = payload[:4096]
    try:
        decoded = snippet.decode("utf-8", errors="ignore")
    except Exception:  # pragma: no cover - extremely defensive
        return False

    return _looks_like_login_html_text(decoded)

def _ensure_profile_dir(store_name: str | Path) -> Path:
    if isinstance(store_name, Path):
        p = store_name
    else:
        p = PKG_ROOT / "profiles" / store_name
    p.mkdir(parents=True, exist_ok=True)
    return p


async def _prime_context_with_storage_state(
    ctx: BrowserContext,
    storage_state_file: Path,
    *,
    store_code: str,
    logger: JsonLogger,
) -> None:
    try:
        raw_state = storage_state_file.read_text()
    except FileNotFoundError:
        log_event(
            logger=logger,
            phase="download",
            status="warn",
            store_code=store_code,
            bucket=None,
            message="storage state file missing",
            extras={"storage_state": str(storage_state_file)},
        )
        return
    except Exception as exc:  # pragma: no cover - runtime guard
        log_event(
            logger=logger,
            phase="download",
            status="warn",
            store_code=store_code,
            bucket=None,
            message="unable to read storage state",
            extras={"storage_state": str(storage_state_file), "error": str(exc)},
        )
        return

    try:
        storage_state = json.loads(raw_state)
    except json.JSONDecodeError as exc:
        log_event(
            logger=logger,
            phase="download",
            status="warn",
            store_code=store_code,
            bucket=None,
            message="invalid storage state JSON",
            extras={"storage_state": str(storage_state_file), "error": str(exc)},
        )
        return

    cookies_applied = 0
    cookies = storage_state.get("cookies") or []
    if cookies:
        sanitized: List[dict] = []
        for cookie in cookies:
            cleaned = {k: v for k, v in cookie.items() if v is not None}
            expires = cleaned.get("expires")
            if expires is None or not isinstance(expires, (int, float)):
                cleaned.pop("expires", None)
            sanitized.append(cleaned)

        try:
            await ctx.add_cookies(sanitized)
            cookies_applied = len(sanitized)
        except Exception as exc:  # pragma: no cover - Playwright runtime guard
            log_event(
                logger=logger,
                phase="download",
                status="warn",
                store_code=store_code,
                bucket=None,
                message="unable to apply cookies from storage state",
                extras={"error": str(exc)},
            )

    origins = storage_state.get("origins") or []
    hydrated_origins = 0
    priming_page: Page | None = None

    if origins:
        log_event(
            logger=logger,
            phase="download",
            status="info",
            store_code=store_code,
            bucket=None,
            message="creating priming page for storage state",
            extras={"origin_count": len(origins)},
        )
        try:
            priming_page = await ctx.new_page()
        except Exception as exc:  # pragma: no cover - runtime guard
            log_event(
                logger=logger,
                phase="download",
                status="warn",
                store_code=store_code,
                bucket=None,
                message="unable to create priming page",
                extras={"error": str(exc)},
            )
            priming_page = None

        if priming_page is not None:
            try:
                for origin in origins:
                    origin_url = origin.get("origin")
                    if not origin_url:
                        continue

                    try:
                        await priming_page.goto(origin_url, wait_until="domcontentloaded")
                    except Exception as exc:  # pragma: no cover - remote navigation guard
                        log_event(
                            logger=logger,
                            phase="download",
                            status="warn",
                            store_code=store_code,
                            bucket=None,
                            message="unable to initialize localStorage for origin",
                            extras={"origin": origin_url, "error": str(exc)},
                        )
                        continue

                    entries = origin.get("localStorage") or []
                    if not entries:
                        continue

                    origin_hydrated = False
                    for entry in entries:
                        name = entry.get("name")
                        value = entry.get("value")
                        if name is None or value is None:
                            continue

                        try:
                            await priming_page.evaluate(
                                "window.localStorage.setItem(arguments[0], arguments[1]);",
                                name,
                                value,
                            )
                            origin_hydrated = True
                        except Exception as exc:  # pragma: no cover - runtime guard
                            log_event(
                                logger=logger,
                                phase="download",
                                status="warn",
                                store_code=store_code,
                                bucket=None,
                                message="unable to persist localStorage entry",
                                extras={"origin": origin_url, "key": name, "error": str(exc)},
                            )

                    if origin_hydrated:
                        hydrated_origins += 1
            finally:
                try:
                    await priming_page.close()
                finally:
                    log_event(
                        logger=logger,
                        phase="download",
                        status="info",
                        store_code=store_code,
                        bucket=None,
                        message="priming page closed",
                        extras={"origin_count": len(origins)},
                    )

    status_message = "storage state primed" if origins else "storage state cookies primed"
    log_event(
        logger=logger,
        phase="download",
        status="info",
        store_code=store_code,
        bucket=None,
        message=status_message,
        extras={"cookies": cookies_applied, "origins": hydrated_origins},
    )

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

    request = page.context.request
    attempted_refresh = False

    while True:
        try:
            response = await request.get(url)
        except Exception as exc:
            _log("error", f"request failed for {spec['key']}", extras={"error": str(exc)})
            return None

        if response is None:
            _log("error", f"no response returned for {spec['key']}")
            return None

        try:
            status = response.status
        except Exception as exc:  # pragma: no cover - defensive guard
            status = None
            _log("warn", f"unable to read status for {spec['key']}", extras={"error": str(exc)})

        try:
            body = await response.body()
        except Exception as exc:
            _log("error", f"unable to read body for {spec['key']}", extras={"error": str(exc)})
            body = b""

        if status == 200 and body and not _looks_like_login_html_bytes(body):
            final_path.parent.mkdir(parents=True, exist_ok=True)
            final_path.write_bytes(body)
            is_valid, reason = _validate_downloaded_csv(final_path, body=body)
            if not is_valid:
                _log(
                    "warn",
                    f"discarding invalid CSV download for {spec['key']}",
                    extras={"reason": reason} if reason else None,
                )
                try:
                    final_path.unlink(missing_ok=True)
                except OSError as exc:
                    _log(
                        "warn",
                        f"unable to delete invalid download for {spec['key']}",
                        extras={"error": str(exc)},
                    )
                return None
            return final_path

        needs_refresh = False

        if status in {401, 403}:
            _log("warn", f"received {status} for {spec['key']} — refreshing session")
            needs_refresh = True
        elif not body:
            _log("warn", f"empty response for {spec['key']}")
        elif _looks_like_login_html_bytes(body):
            _log("warn", f"html/login-like response for {spec['key']} — refreshing session")
            needs_refresh = True
        elif status != 200:
            _log("error", f"unexpected status {status} for {spec['key']}")
        else:
            _log("error", f"unexpected response content for {spec['key']}")

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


async def _download_specs_for_store(
    page: Page,
    store_cfg: Dict,
    *,
    logger: JsonLogger,
    merged_buckets: Dict[str, List[Path]],
    download_counts: Dict[str, Dict[str, Dict[str, object]]],
) -> None:
    sc = store_cfg["store_code"]

    for spec in FILE_SPECS:
        if not spec.get("download", True):
            continue

        saved = await _download_one_spec(page, store_cfg, spec, logger=logger)
        if saved and spec.get("merge_bucket"):
            bucket = spec["merge_bucket"]
            merged_buckets.setdefault(bucket, []).append(saved)
            download_counts.setdefault(bucket, {})[sc] = {
                "rows": _count_rows(saved),
                "path": str(saved),
            }


def _finalize_merges(
    merged_buckets: Dict[str, List[Path]],
    download_counts: Dict[str, Dict[str, Dict[str, object]]],
    *,
    logger: JsonLogger,
) -> None:
    for bucket, files in merged_buckets.items():
        if not files:
            continue

        if bucket not in MERGED_NAMES:
            log_event(
                logger=logger,
                phase="merge",
                bucket=bucket,
                message="no merged filename configured; skipping bucket",
                status="warn",
            )
            continue

        merged_path = _manual_merge_bucket(bucket, files)
        if not merged_path:
            continue

        bucket_downloads = download_counts.get(bucket, {})
        download_total = sum(
            entry.get("rows", 0)
            for key, entry in bucket_downloads.items()
            if key != "__merged__" and isinstance(entry, dict)
        )
        merged_rows = _count_rows(merged_path)
        log_event(
            logger=logger,
            phase="merge",
            bucket=bucket,
            merged_file=str(merged_path),
            counts={
                "download_total": download_total,
                "merged_rows": merged_rows,
            },
            message="merge complete",
        )
        download_counts.setdefault(bucket, {})["__merged__"] = {
            "rows": merged_rows,
            "path": str(merged_path),
        }


def _manual_merge_bucket(bucket: str, files: List[Path]) -> Path | None:
    if not files:
        return None

    merged_name = MERGED_NAMES.get(bucket)
    if not merged_name:
        return None

    merged_path = DATA_DIR / merged_name

    with merged_path.open("wb") as out_f:
        first = True
        for file_path in files:
            if not file_path.exists():
                continue

            data = file_path.read_bytes()
            if first:
                out_f.write(data)
                first = False
                continue

            try:
                header_end = data.index(b"\n")
            except ValueError:
                continue

            out_f.write(data[header_end + 1 :])

    return merged_path


async def _bootstrap_session_via_home_and_tracker(
    page: Page,
    store_cfg: Dict[str, Any],
    logger: JsonLogger,
) -> Page:
    store_code = store_cfg.get("store_code")

    def _log(status: str, message: str, *, extras: Dict | None = None) -> None:
        log_event(
            logger=logger,
            phase="download",
            status=status,
            store_code=store_code,
            bucket=None,
            message=message,
            extras=extras,
        )

    login_url = store_cfg.get("login_url") or LOGIN_URL
    home_url = store_cfg.get("home_url") or HOME_URL

    username = store_cfg.get("username")
    password = store_cfg.get("password")

    _log(
        "info",
        "starting single-session bootstrap",
        extras={"login_url": login_url, "home_url": home_url},
    )

    await page.goto(login_url, wait_until="domcontentloaded")

    if username and password:
        _log("info", "filling login form for bootstrap")
        await page.fill(page_selectors.LOGIN_USERNAME, username)
        await page.fill(page_selectors.LOGIN_PASSWORD, password)
        await page.click(page_selectors.LOGIN_SUBMIT)
    else:
        _log(
            "warn",
            "missing credentials for bootstrap; relying on existing session",
            extras={"username_present": bool(username), "password_present": bool(password)},
        )

    try:
        await page.wait_for_url(f"{home_url}*", timeout=60_000)
    except PlaywrightTimeoutError as exc:
        _log(
            "error",
            "home navigation after login timed out",
            extras={"error": str(exc), "current_url": page.url},
        )
        raise

    await page.wait_for_timeout(2000)

    tracker_heading = page.locator(
        "h5.card-title",
        has_text="Daily Operations Tracker",
    )

    try:
        await tracker_heading.first.wait_for(state="visible", timeout=30_000)
    except Exception as exc:
        _log(
            "error",
            "daily operations tracker heading not found",
            extras={"error": str(exc)},
        )
        raise

    tracker_target = tracker_heading.first
    context = page.context

    try:
        async with context.expect_page() as page_info:
            await tracker_target.click()
        tms_page = await page_info.value
        await tms_page.wait_for_load_state("domcontentloaded")
        await tms_page.wait_for_timeout(2000)
        _log(
            "info",
            "tms page opened in new tab",
            extras={"tms_url": tms_page.url},
        )
        return tms_page
    except PlaywrightTimeoutError:
        _log("warn", "no new tab detected; falling back to same page")
        try:
            async with page.expect_navigation(wait_until="domcontentloaded", timeout=60_000):
                await tracker_target.click()
        except PlaywrightTimeoutError as exc:
            _log(
                "error",
                "navigation to TMS page timed out",
                extras={"error": str(exc)},
            )
            raise

        await page.wait_for_load_state("domcontentloaded")
        await page.wait_for_timeout(2000)
        _log(
            "info",
            "tms page opened in same tab",
            extras={"tms_url": page.url},
        )
        return page


async def _switch_to_store_dashboard_and_download(
    page: Page,
    store_cfg: Dict[str, Any],
    *,
    logger: JsonLogger,
    settings: PipelineSettings,
    merged_buckets: Dict[str, List[Path]],
    download_counts: Dict[str, Dict[str, Dict[str, object]]],
) -> None:
    _ = settings
    store_code = store_cfg.get("store_code")

    def _log(status: str, message: str, *, extras: Dict | None = None) -> None:
        log_event(
            logger=logger,
            phase="download",
            status=status,
            store_code=store_code,
            bucket=None,
            message=message,
            extras=extras,
        )

    target_url = store_cfg.get("dashboard_url")
    if not target_url:
        sc = store_cfg.get("store_code")
        if not sc:
            raise RuntimeError("Store configuration missing dashboard_url and store_code")
        target_url = tms_dashboard_url(sc)
        store_cfg["dashboard_url"] = target_url
    target_url = str(target_url)

    _log(
        "info",
        "navigating to store dashboard in single session",
        extras={"target_url": target_url},
    )

    await page.goto(target_url, wait_until="domcontentloaded")
    _log("info", "store dashboard reached", extras={"dashboard_url": target_url})

    request = page.context.request

    for spec in FILE_SPECS:
        if not spec.get("download", False):
            continue

        url = _render(spec["url_template"], store_code)
        out_name = _render(spec["out_name_template"], store_code)
        final_path = DATA_DIR / out_name

        try:
            response = await request.get(url)
        except Exception as exc:
            _log(
                "error",
                f"request failed for {spec['key']}",
                extras={"url": url, "error": str(exc)},
            )
            continue

        status = None
        try:
            status = response.status
        except Exception:
            status = None

        if status != 200:
            _log(
                "warn",
                f"unexpected status for {spec['key']}",
                extras={"url": url, "status": status},
            )
            continue

        body = await response.body()
        final_path.parent.mkdir(parents=True, exist_ok=True)
        final_path.write_bytes(body)

        _log(
            "info",
            f"download complete for {spec['key']}",
            extras={"url": url, "path": str(final_path), "status": status},
        )

        bucket = spec.get("merge_bucket")
        if bucket:
            merged_buckets.setdefault(bucket, []).append(final_path)
            download_counts.setdefault(bucket, {})[store_code] = {
                "rows": _count_rows(final_path),
                "path": str(final_path),
            }


async def _is_login_page(page: Page, logger: JsonLogger | None = None) -> bool:
    """Determine whether the current page is the Simplify login form."""

    current_url = page.url or ""
    if not current_url:
        return False

    parsed = urlparse(current_url)
    host = (parsed.hostname or "").lower()
    path = _normalize_url_path(parsed.path)

    if not host:
        return False

    if host.endswith("tms.simplifytumbledry.in"):
        return False

    host_matches = host == _LOGIN_HOST or (
        _LOGIN_HOST and host.endswith(f".{_LOGIN_HOST}")
    )

    if not host_matches or path != _LOGIN_PATH:
        return False

    try:
        username_count = await page.locator(page_selectors.LOGIN_USERNAME).count()
    except Exception:  # pragma: no cover - defensive; locator failures shouldn't break flow
        return False

    try:
        password_count = await page.locator(page_selectors.LOGIN_PASSWORD).count()
    except Exception:  # pragma: no cover - defensive
        return False

    if username_count > 0 and password_count > 0:
        return True

    return False


async def _navigate_via_home_to_dashboard(page: Page, store_cfg: Dict, logger: JsonLogger) -> None:
    """Navigate from the home page to the store dashboard via the tracker card."""

    store_code = store_cfg.get("store_code")

    def _log(status: str, message: str, *, extras: Dict | None = None) -> None:
        log_event(
            logger=logger,
            phase="download",
            status=status,
            store_code=store_code,
            bucket=None,
            message=message,
            extras=extras,
        )

    home_url = store_cfg.get("home_url")
    if not home_url:
        home_url = LOGIN_URL
        if home_url.endswith("/login"):
            home_url = home_url[: -len("/login")]
        else:
            parts = home_url.rstrip("/").rsplit("/", 1)
            home_url = parts[0] if len(parts) == 2 else home_url

    _log("info", "navigating via home to dashboard", extras={"home_url": home_url})

    response = await page.goto(home_url, wait_until="domcontentloaded")
    response_status = None
    if response is not None:
        try:
            response_status = response.status
        except Exception:  # pragma: no cover - defensive
            response_status = None

    _log(
        "info",
        "home page loaded",
        extras={"home_url": home_url, "current_url": page.url, "response_status": response_status},
    )

    if await _is_login_page(page, logger):
        _log("info", "home page requires authentication; invoking login flow")
        await _perform_login_flow(page, store_cfg, logger)
        if await _is_login_page(page, logger):
            _log(
                "error",
                "home navigation remained on login page",
                extras={"home_url": home_url, "current_url": page.url},
            )
            raise RuntimeError("Home navigation returned to login page")

    tracker_heading = page.locator("h5.card-title:has-text(\"Daily Operations Tracker\")")

    try:
        await tracker_heading.first.wait_for(state="visible", timeout=30_000)
    except Exception as exc:
        _log(
            "error",
            "daily operations tracker heading not found",
            extras={"home_url": home_url, "error": str(exc)},
        )
        raise

    tracker_card = tracker_heading.locator(
        "xpath=ancestor-or-self::*[self::a or contains(concat(' ', normalize-space(@class), ' '), ' card ')][1]"
    )

    click_target = tracker_card.first if await tracker_card.count() > 0 else tracker_heading.first

    navigation_response = None
    try:
        async with page.expect_navigation(wait_until="domcontentloaded", timeout=60_000) as nav_info:
            await click_target.click()
        navigation_response = await nav_info
    except PlaywrightTimeoutError as exc:
        _log(
            "error",
            "navigation via home timed out after clicking tracker",
            extras={"error": str(exc)},
        )
        raise

    response_status = None
    if navigation_response is not None:
        try:
            response_status = navigation_response.status
        except Exception:  # pragma: no cover - defensive
            response_status = None

    _log(
        "info",
        "daily operations tracker clicked",
        extras={
            "post_click_url": page.url,
            "response_status": response_status,
            "dashboard_url": store_cfg.get("dashboard_url"),
        },
    )


async def _perform_login_flow(page: Page, store_cfg: Dict, logger: JsonLogger) -> None:
    """Explicitly visit the login form and authenticate like a human user."""

    username = store_cfg.get("username")
    password = store_cfg.get("password")
    store_code = store_cfg.get("store_code")

    def _log(status: str, message: str, *, extras: Dict | None = None) -> None:
        log_event(
            logger=logger,
            phase="download",
            status=status,
            store_code=store_code,
            bucket=None,
            message=message,
            extras=extras,
        )

    current_url = page.url or ""
    if not current_url or "login" not in current_url.lower():
        await page.goto(LOGIN_URL, wait_until="domcontentloaded")
        current_url = page.url or ""

    if not await _is_login_page(page, logger):
        # Storage state/session cookies already landed us past login.
        _log("info", "login page bypassed; session already active", extras={"current_url": page.url})
        return

    if not username or not password:
        raise RuntimeError(f"Missing credentials for store_code={store_code}")

    username_locator = page.locator(page_selectors.LOGIN_USERNAME)
    password_locator = page.locator(page_selectors.LOGIN_PASSWORD)
    submit_locator = page.locator(page_selectors.LOGIN_SUBMIT)

    _log(
        "info",
        "preparing automated login",
        extras={
            "store_code": store_code,
            "current_url": page.url,
            "login_url": LOGIN_URL,
            "username": username,
            "password_len": len(password),
            "login_username_selector": page_selectors.LOGIN_USERNAME,
            "login_password_selector": page_selectors.LOGIN_PASSWORD,
            "login_submit_selector": page_selectors.LOGIN_SUBMIT,
        },
    )

    await page.wait_for_selector(page_selectors.LOGIN_USERNAME, timeout=15_000)

    username_locator_present: bool | None = None
    password_locator_present: bool | None = None
    submit_locator_present: bool | None = None
    username_locator_error: str | None = None
    password_locator_error: str | None = None
    submit_locator_error: str | None = None
    username_fill_error: str | None = None
    password_fill_error: str | None = None
    submit_click_error: str | None = None

    try:
        count = await username_locator.count()
        username_locator_present = count > 0
    except Exception as exc:  # pragma: no cover - defensive logging only
        username_locator_error = str(exc)

    try:
        await page.fill(page_selectors.LOGIN_USERNAME, username)
    except Exception as exc:
        username_fill_error = str(exc)

    try:
        count = await password_locator.count()
        password_locator_present = count > 0
    except Exception as exc:  # pragma: no cover - defensive logging only
        password_locator_error = str(exc)

    try:
        await page.fill(page_selectors.LOGIN_PASSWORD, password)
    except Exception as exc:
        password_fill_error = str(exc)

    navigation_error: Exception | None = None
    try:
        async with page.expect_navigation(wait_until="domcontentloaded", timeout=60_000):
            try:
                count = await submit_locator.count()
                submit_locator_present = count > 0
            except Exception as exc:  # pragma: no cover - defensive logging only
                submit_locator_error = str(exc)

            await page.click(page_selectors.LOGIN_SUBMIT)
    except PlaywrightTimeoutError as exc:  # pragma: no cover - depends on remote latency
        navigation_error = exc
    except Exception as exc:  # pragma: no cover - defensive against Playwright quirks
        navigation_error = exc
        submit_click_error = str(exc)

    if navigation_error is not None:
        _log(
            "warn",
            "login navigation signalled a timeout; validating page state",
            extras={"error": str(navigation_error)},
        )

    # Prefer explicit confirmation that the login form disappeared over generic
    # load-state events.
    login_form_cleared = False
    login_form_error: Exception | None = None
    for state, timeout in (("detached", 30_000), ("hidden", 15_000)):
        try:
            await page.wait_for_selector(
                page_selectors.LOGIN_USERNAME,
                state=state,
                timeout=timeout,
            )
            login_form_cleared = True
            login_form_error = None
            break
        except PlaywrightTimeoutError as exc:  # pragma: no cover - depends on remote latency
            login_form_error = exc
            continue
        except Exception as exc:  # pragma: no cover - defensive against Playwright quirks
            login_form_error = exc
            break

    if not login_form_cleared and login_form_error is not None:
        _log(
            "warn",
            "login form still present after submission; checking page content",
            extras={"error": str(login_form_error)},
        )

    try:
        content_after_login = await page.content()
    except Exception:  # pragma: no cover - defensive, shouldn't normally fail
        content_after_login = None

    if content_after_login:
        error_msg = _extract_login_error(content_after_login)
        if error_msg:
            _log(
                "error",
                "login failed; site returned explicit error",
                extras={"error": error_msg},
            )
            raise RuntimeError("Automated login failed; site reports login error")

    still_login_page = await _is_login_page(page, logger)

    extras = {
        "post_login_url": page.url,
        "navigation_error": str(navigation_error) if navigation_error else None,
        "login_form_cleared": login_form_cleared,
        "login_form_error": str(login_form_error) if login_form_error else None,
        "still_login_page": still_login_page,
        "username_locator_present": username_locator_present,
        "password_locator_present": password_locator_present,
        "submit_locator_present": submit_locator_present,
        "username_locator_error": username_locator_error,
        "password_locator_error": password_locator_error,
        "submit_locator_error": submit_locator_error,
        "username_fill_error": username_fill_error,
        "password_fill_error": password_fill_error,
        "submit_click_error": submit_click_error,
    }

    if content_after_login:
        extras["login_error_hint"] = _extract_login_error(content_after_login)

    # Drop None values for cleaner logs.
    extras = {k: v for k, v in extras.items() if v is not None}

    _log("info", "login submission completed", extras=extras)


async def _ensure_dashboard(page: Page, store_cfg: Dict, logger: JsonLogger) -> None:
    """Navigate to the dashboard, refreshing the login session when required."""

    dashboard_url = store_cfg["dashboard_url"]
    store_code = store_cfg.get("store_code")

    def _log(status: str, message: str, *, extras: Dict | None = None) -> None:
        log_event(
            logger=logger,
            phase="download",
            status=status,
            store_code=store_code,
            bucket=None,
            message=message,
            extras=extras,
        )

    _log("info", "opening dashboard", extras={"target_url": dashboard_url})
    response = await page.goto(dashboard_url, wait_until="domcontentloaded")
    response_status = None
    if response is not None:
        try:
            response_status = response.status
        except Exception:  # pragma: no cover - defensive
            response_status = None
    _log(
        "info",
        "dashboard navigation completed",
        extras={"current_url": page.url, "response_status": response_status},
    )

    performed_login_flow = False
    has_creds = bool(store_cfg.get("username") and store_cfg.get("password"))

    if await _is_login_page(page, logger):
        if not has_creds:
            _log(
                "warn",
                "login page detected but credentials missing; skipping store",
                extras={"dashboard_url": dashboard_url, "current_url": page.url},
            )
            raise SkipStoreDashboardError(
                f"Skipping store_code={store_code}: login required but credentials are not configured."
            )

        _log(
            "info",
            "dashboard requires login; attempting automated login",
            extras={"dashboard_url": dashboard_url, "current_url": page.url},
        )
        await _perform_login_flow(page, store_cfg, logger)
        performed_login_flow = True
        try:
            await _navigate_via_home_to_dashboard(page, store_cfg, logger)
        except Exception as exc:
            _log("error", "home navigation failed after login", extras={"error": str(exc)})
            raise

    while True:
        try:
            await page.wait_for_selector(
                page_selectors.DOWNLOAD_LINKS,
                state="visible",
                timeout=DASHBOARD_DOWNLOAD_CONTROL_TIMEOUT_MS,
            )
            break
        except PlaywrightTimeoutError as exc:
            _log(
                "warn",
                "dashboard download controls not detected; validating page content",
                extras={"error": str(exc)},
            )

            if await _is_login_page(page, logger):
                if not has_creds:
                    _log(
                        "warn",
                        "dashboard redirected to login but credentials are missing; skipping store",
                        extras={"current_url": page.url},
                    )
                    raise SkipStoreDashboardError(
                        f"Skipping store_code={store_code}: redirected to login without credentials."
                    )

                if performed_login_flow:
                    _log(
                        "error",
                        "login loop detected when opening dashboard",
                        extras={"current_url": page.url},
                    )
                    raise RuntimeError("Automated login failed; manual login required")

                _log(
                    "info",
                    "dashboard redirected to login; attempting automated login",
                    extras={"dashboard_url": dashboard_url, "login_url": page.url},
                )
                await _perform_login_flow(page, store_cfg, logger)
                performed_login_flow = True
                try:
                    await _navigate_via_home_to_dashboard(page, store_cfg, logger)
                except Exception as nav_exc:
                    _log(
                        "error",
                        "home navigation failed after login",
                        extras={"error": str(nav_exc)},
                    )
                    raise
                continue

            if not has_creds:
                _log(
                    "warn",
                    "dashboard controls not found and no credentials for store; skipping store",
                    extras={"current_url": page.url},
                )
                raise SkipStoreDashboardError(
                    f"Skipping store_code={store_code}: no download controls visible and no credentials configured."
                )

            raise RuntimeError(
                f"Dashboard controls not found for store_code={store_code}; verify layout or selectors."
            )
        except Exception as exc:
            _log(
                "error",
                "unexpected error while waiting for dashboard controls",
                extras={"error": str(exc)},
            )
            raise

    _log(
        "info",
        "store dashboard reached",
        extras={"dashboard_url": store_cfg.get("dashboard_url")},
    )


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
    raw_store_env: str | None = None,
) -> Dict[str, Dict[str, Dict[str, object]]]:
    """
    Opens a persistent profile for each store, goes to the store's TMS dashboard,
    downloads all FILE_SPECS where download=True, saves into dashboard_downloader/data/,
    and finally performs merges per merge_bucket.
    """
    logger = logger or JsonLogger()
    merged_buckets: Dict[str, List[Path]] = {}
    download_counts: Dict[str, Dict[str, Dict[str, object]]] = {}

    resolved_stores = stores or stores_from_list(DEFAULT_STORE_CODES)
    env_value = raw_store_env if raw_store_env is not None else os.getenv("stores_list") or os.getenv("STORES_LIST") or ""
    log_event(
        logger=logger,
        phase="download",
        store_code=None,
        bucket=None,
        message="resolved stores for run",
        extras={
            "raw_STORES_LIST": env_value,
            "store_codes": [cfg.get("store_code") for cfg in resolved_stores.values()],
        },
    )

    async with async_playwright() as p:
        # NOTE: On macOS we can keep channel="chrome" if you prefer;
        # leaving it off keeps it portable for Linux server (Chromium).
        for store_name, cfg in resolved_stores.items():
            profile_dir_cfg = cfg.get("profile_dir")
            profile_key = cfg.get("profile_key") or store_name
            if profile_dir_cfg:
                user_dir = _ensure_profile_dir(Path(profile_dir_cfg))
            else:
                user_dir = _ensure_profile_dir(profile_key)
            sc = cfg["store_code"]

            storage_state_cfg = cfg.get("storage_state")
            storage_state_file = None
            storage_state_source: str | None = None
            if storage_state_cfg:
                storage_state_file = Path(storage_state_cfg)
                if not storage_state_file.exists():
                    log_event(
                        logger=logger,
                        phase="download",
                        status="warn",
                        store_code=sc,
                        bucket=None,
                        message="storage state not found; falling back to credential login",
                        extras={"storage_state": str(storage_state_file)},
                    )
                    storage_state_file = None
                else:
                    storage_state_source = "store_cfg"

            context_kwargs = dict(
                user_data_dir=str(user_dir),
                headless=False,
                accept_downloads=True,
                # macOS dev: uncomment next line to force system Chrome
                channel="chrome",
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
            )

            if storage_state_file is None and storage_state_cfg is None:
                # Provide a sane default for callers that rely on first_login.py.
                default_state = storage_state_path()
                if default_state.exists():
                    storage_state_file = default_state
                    storage_state_source = "default"

            ctx = await p.chromium.launch_persistent_context(**context_kwargs)

            if storage_state_file is not None:
                log_event(
                    logger=logger,
                    phase="download",
                    status="info",
                    store_code=sc,
                    bucket=None,
                    message="loading storage state",
                    extras={
                        "storage_state": str(storage_state_file),
                        "source": storage_state_source or "unspecified",
                    },
                )
                await _prime_context_with_storage_state(
                    ctx,
                    storage_state_file,
                    store_code=sc,
                    logger=logger,
                )

            try:
                log_event(
                    logger=logger,
                    phase="download",
                    status="info",
                    store_code=sc,
                    bucket=None,
                    message="creating primary page for store",
                )
                page = await ctx.new_page()
                # Hit the dashboard and automatically refresh the session when needed.
                try:
                    await _ensure_dashboard(page, cfg, logger)
                except SkipStoreDashboardError as exc:
                    log_event(
                        logger=logger,
                        phase="download",
                        status="warn",
                        store_code=sc,
                        bucket=None,
                        message="skipping store due to dashboard unavailability",
                        extras={"reason": str(exc)},
                    )
                    await page.close()
                    continue

                await _download_specs_for_store(
                    page,
                    cfg,
                    logger=logger,
                    merged_buckets=merged_buckets,
                    download_counts=download_counts,
                )

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
    _finalize_merges(merged_buckets, download_counts, logger=logger)

    return download_counts


async def run_all_stores_single_session(
    *,
    settings: PipelineSettings,
    logger: JsonLogger,
) -> Dict[str, Dict[str, Dict[str, object]]]:
    """Run dashboard downloads for all stores using a single persistent session."""

    merged_buckets: Dict[str, List[Path]] = {}
    download_counts: Dict[str, Dict[str, Dict[str, object]]] = {}

    resolved_stores = settings.stores or {}
    if not resolved_stores:
        resolved_stores = stores_from_list(DEFAULT_STORE_CODES)

    env_value = getattr(settings, "raw_store_env", "")
    log_event(
        logger=logger,
        phase="download",
        store_code=None,
        bucket=None,
        message="resolved stores for single-session run",
        extras={
            "raw_STORES_LIST": env_value,
            "store_codes": [cfg.get("store_code") for cfg in resolved_stores.values()],
        },
    )

    store_items = list(resolved_stores.items())
    if not store_items:
        _finalize_merges(merged_buckets, download_counts, logger=logger)
        return download_counts

    first_store_name, first_store_cfg = store_items[0]
    profile_dir_cfg = first_store_cfg.get("profile_dir")
    profile_key = first_store_cfg.get("profile_key") or first_store_name
    if profile_dir_cfg:
        user_dir = _ensure_profile_dir(Path(profile_dir_cfg))
    else:
        user_dir = _ensure_profile_dir(profile_key)

    storage_state_cfg = first_store_cfg.get("storage_state")
    storage_state_file = None
    storage_state_source: str | None = None
    if storage_state_cfg:
        storage_state_candidate = Path(storage_state_cfg)
        if storage_state_candidate.exists():
            storage_state_file = storage_state_candidate
            storage_state_source = "store_cfg"
        else:
            log_event(
                logger=logger,
                phase="download",
                status="warn",
                store_code=first_store_cfg.get("store_code"),
                bucket=None,
                message="storage state not found; falling back to credential login",
                extras={"storage_state": str(storage_state_candidate)},
            )
            storage_state_file = None

    if storage_state_file is None and storage_state_cfg is None:
        default_state = storage_state_path()
        if default_state.exists():
            storage_state_file = default_state
            storage_state_source = "default"

    context_kwargs = dict(
        user_data_dir=str(user_dir),
        headless=False,
        accept_downloads=True,
        channel="chrome",
        args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
    )

    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context(**context_kwargs)

        if storage_state_file is not None:
            log_event(
                logger=logger,
                phase="download",
                status="info",
                store_code=first_store_cfg.get("store_code"),
                bucket=None,
                message="loading storage state for single-session run",
                extras={
                    "storage_state": str(storage_state_file),
                    "source": storage_state_source or "unspecified",
                },
            )
            await _prime_context_with_storage_state(
                ctx,
                storage_state_file,
                store_code=first_store_cfg.get("store_code", ""),
                logger=logger,
            )

        pages = ctx.pages
        home_page = pages[0] if pages else await ctx.new_page()
        tms_page = await _bootstrap_session_via_home_and_tracker(home_page, first_store_cfg, logger)

        try:
            for _, cfg in store_items:
                sc = cfg.get("store_code")
                try:
                    await _switch_to_store_dashboard_and_download(
                        tms_page,
                        cfg,
                        logger=logger,
                        settings=settings,
                        merged_buckets=merged_buckets,
                        download_counts=download_counts,
                    )
                except SkipStoreDashboardError as exc:
                    log_event(
                        logger=logger,
                        phase="download",
                        status="warn",
                        store_code=sc,
                        bucket=None,
                        message="skipping store due to dashboard unavailability",
                        extras={"reason": str(exc)},
                    )
                    continue

                log_event(
                    logger=logger,
                    phase="download",
                    message="store download completed",
                    store_code=sc,
                    bucket=None,
                )
        finally:
            ctx_is_closed = getattr(ctx, "is_closed", None)
            if callable(ctx_is_closed):
                if not ctx.is_closed():
                    await ctx.close()
            else:
                await ctx.close()

    _finalize_merges(merged_buckets, download_counts, logger=logger)

    return download_counts

if __name__ == "__main__":
    import asyncio

    asyncio.run(run_all_stores())

