from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, TYPE_CHECKING

from playwright.async_api import Browser, BrowserContext, Page

from app.crm_downloader.td_orders_sync.td_api_client import (
    TdApiClient,
    TdApiClientConfig,
    TdApiFetchResult,
    td_api_fetch_auth_failure_endpoints,
)
from app.dashboard_downloader.json_logger import JsonLogger, log_event

if TYPE_CHECKING:  # pragma: no cover - imported only for static typing.
    from app.crm_downloader.td_orders_sync.main import TdApiContext, TdStore


@dataclass(frozen=True)
class TdSourceSnapshotConfig:
    """Runtime knobs for a TD source snapshot fetch.

    ``source_mode`` is carried into logs and failure policy decisions.  The
    optional ``api_client_config`` lets rebuilds and order sync use identical
    API client timeout/auth semantics without duplicating client construction.
    """

    source_mode: str = "api_shadow"
    context_source: str = "iframe"
    api_client_config: TdApiClientConfig | None = None
    navigate_to_report_iframe: bool = True
    accept_downloads: bool = True


@dataclass
class TdSourceSnapshotFetchResult:
    """Typed TD API source snapshot returned to all TD API consumers."""

    api_fetch_result: TdApiFetchResult
    garments_rows: list[dict[str, Any]]
    garment_order_snapshots: list[dict[str, Any]]
    endpoint_health: dict[str, dict[str, Any]]
    source_fetch_status: str
    failure_class: str | None
    source_fetch_error_class: str | None
    request_metadata: list[dict[str, Any]]
    endpoint_errors: dict[str, str]
    endpoint_error_diagnostics: dict[str, dict[str, Any]]
    report_iframe_src: str | None = None
    stored_state_path: str | None = None
    session_reused: bool | None = None
    login_performed: bool | None = None
    verification_seen: bool | None = None
    fetch_exception: BaseException | None = None

    @property
    def garments_endpoint_health(self) -> dict[str, Any]:
        health = self.endpoint_health.get("/garments/details") or {}
        return dict(health) if isinstance(health, dict) else {}

    @property
    def garments_fetch_completeness(self) -> str | None:
        value = self.garments_endpoint_health.get("garments_fetch_completeness")
        return str(value).strip().lower() if value is not None else None

    @property
    def auth_failed_endpoints(self) -> list[str]:
        return td_api_fetch_auth_failure_endpoints(self.api_fetch_result)


def _classify_failure(
    exc: BaseException | None, result: TdApiFetchResult
) -> str | None:
    if (
        result.source_fetch_status == "auth_failed"
        or td_api_fetch_auth_failure_endpoints(result)
    ):
        return "store_auth_failure"
    if result.source_fetch_error_class:
        error_class = result.source_fetch_error_class.lower()
        if any(
            token in error_class
            for token in ("timeout", "rate", "temporary", "transient")
        ):
            return "retryable_transient_failure"
        return "source_endpoint_failure"
    if exc is None:
        return None
    text = f"{type(exc).__name__} {exc}".lower()
    if any(
        token in text for token in ("timeout", "temporar", "connection", "network")
    ):
        return "retryable_transient_failure"
    if any(
        token in text
        for token in ("browser executable", "playwright", "missing dependencies")
    ):
        return "systemic_setup_failure"
    return "source_fetch_exception"


async def fetch_td_source_snapshot(
    *,
    store: "TdStore",
    from_date: date,
    to_date: date,
    run_id: str,
    logger: JsonLogger,
    source_config: TdSourceSnapshotConfig | None = None,
    browser: Browser | None = None,
    context: BrowserContext | None = None,
    page: Page | None = None,
    report_iframe_src: str | None = None,
) -> TdSourceSnapshotFetchResult:
    """Prepare TD auth/session state, resolve report iframe auth, and fetch API reports.

    Callers may pass an already-prepared Playwright context.  When no context is
    supplied the helper owns the browser lifecycle and uses the mature TD orders
    sync preparation helper so rebuilds and the normal TD sync share auth and
    completeness semantics.
    """

    source_config = source_config or TdSourceSnapshotConfig()
    owns_browser = browser is None and context is None
    owns_context = context is None
    api_context: TdApiContext | None = None

    try:
        if context is None:
            from app.crm_downloader.browser import launch_browser
            from app.crm_downloader.td_orders_sync.main import (
                prepare_td_api_context_for_store,
            )

            if context is None and browser is None:
                from playwright.async_api import async_playwright

                playwright_cm = async_playwright()
                playwright = await playwright_cm.__aenter__()
            else:
                playwright_cm = None
                playwright = None

            try:
                if context is None and browser is None:
                    browser = await launch_browser(playwright=playwright, logger=logger)
                api_context = await prepare_td_api_context_for_store(
                    browser=browser,
                    context=context,
                    page=page,
                    store=store,
                    logger=logger,
                    run_id=run_id,
                    run_start_date=from_date,
                    run_end_date=to_date,
                    navigate_to_report_iframe=source_config.navigate_to_report_iframe,
                    accept_downloads=source_config.accept_downloads,
                )
                context = api_context.context
                report_iframe_src = api_context.report_iframe_src
            except Exception:
                if playwright_cm is not None:
                    await playwright_cm.__aexit__(None, None, None)
                raise
            else:
                # Keep the Playwright manager alive until cleanup below.
                _playwright_cm = playwright_cm
        else:
            _playwright_cm = None

        if context is None:
            raise RuntimeError("TD API source snapshot context was not prepared")

        client_class = TdApiClient
        try:
            from app.crm_downloader.td_orders_sync import main as td_orders_main

            # Keep existing tests and monkeypatch diagnostics compatible while
            # production code still centralizes construction in this helper.
            client_class = getattr(td_orders_main, "TdApiClient", TdApiClient)
        except Exception:
            client_class = TdApiClient

        client = client_class(
            store_code=store.store_code,
            context=context,
            storage_state_path=store.storage_state_path,
            run_id=run_id,
            structured_logger=logger,
            report_iframe_src=report_iframe_src,
            config=source_config.api_client_config,
        )
        session_artifact = client.read_session_artifact()
        log_event(
            logger=logger,
            phase="td_source_snapshot",
            message="Prepared shared TD source snapshot client",
            run_id=run_id,
            store_code=store.store_code,
            source_mode=source_config.source_mode,
            context_source=source_config.context_source,
            report_iframe_src_present=bool(report_iframe_src),
            artifact_has_cookies=bool(session_artifact.get("cookies")),
            artifact_has_origins=bool(session_artifact.get("origins")),
        )

        fetch_exception: BaseException | None = None
        try:
            api_fetch_result = await client.fetch_reports(
                from_date=from_date, to_date=to_date
            )
        except Exception as exc:  # Return a typed failed snapshot so callers share policy.
            fetch_exception = exc
            api_fetch_result = TdApiFetchResult(
                source_fetch_status="failed",
                source_fetch_error_class=type(exc).__name__,
            )

        failure_class = _classify_failure(fetch_exception, api_fetch_result)
        result = TdSourceSnapshotFetchResult(
            api_fetch_result=api_fetch_result,
            garments_rows=list(api_fetch_result.garments_rows),
            garment_order_snapshots=list(api_fetch_result.garment_order_snapshots),
            endpoint_health=dict(api_fetch_result.endpoint_health or {}),
            source_fetch_status=api_fetch_result.source_fetch_status,
            failure_class=failure_class,
            source_fetch_error_class=api_fetch_result.source_fetch_error_class,
            request_metadata=list(api_fetch_result.request_metadata),
            endpoint_errors=dict(api_fetch_result.endpoint_errors or {}),
            endpoint_error_diagnostics=dict(
                api_fetch_result.endpoint_error_diagnostics or {}
            ),
            report_iframe_src=report_iframe_src,
            stored_state_path=(
                api_context.stored_state_path
                if api_context
                else str(store.storage_state_path)
            ),
            session_reused=(api_context.session_reused if api_context else None),
            login_performed=(api_context.login_performed if api_context else None),
            verification_seen=(api_context.verification_seen if api_context else None),
            fetch_exception=fetch_exception,
        )
        log_event(
            logger=logger,
            phase="td_source_snapshot",
            status="warning" if failure_class else "ok",
            message="Fetched shared TD source snapshot",
            run_id=run_id,
            store_code=store.store_code,
            source_mode=source_config.source_mode,
            context_source=source_config.context_source,
            source_fetch_status=result.source_fetch_status,
            failure_class=failure_class,
            source_fetch_error_class=result.source_fetch_error_class,
            orders_rows=len(api_fetch_result.orders_rows),
            sales_rows=len(api_fetch_result.sales_rows),
            garments_rows=len(result.garments_rows),
            endpoint_errors=result.endpoint_errors,
        )
        return result
    finally:
        if owns_context and api_context is not None:
            try:
                await api_context.context.close()
            except Exception:
                pass
        if owns_browser and browser is not None:
            try:
                await browser.close()
            except Exception:
                pass
        playwright_cm = locals().get("_playwright_cm")
        if playwright_cm is not None:
            await playwright_cm.__aexit__(None, None, None)
