from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import parse_qs, unquote, urlparse
from playwright.async_api import BrowserContext, Error as PlaywrightError, Frame
from app.crm_downloader.td_orders_sync.td_api_compare import build_api_request_metadata, parse_token_expiry

REPORTING_API_BASE_URL = "https://reporting-api.quickdrycleaning.com"
REPORTS_ORIGIN_HOST = "reports.quickdrycleaning.com"
ORDERS_ENDPOINT = "/reports/order-report"
SALES_ENDPOINT = "/sales-and-deliveries/sales"
GARMENTS_ENDPOINT = "/garments/details"
HAR_COMPATIBLE_ENDPOINTS = frozenset({SALES_ENDPOINT, GARMENTS_ENDPOINT})

logger = logging.getLogger(__name__)

_TIMEOUT_ERROR_CLASSES = {"read_timeout", "connect_timeout", "total_timeout", "network_timeout", "timeout", "TimeoutError", "PlaywrightError"}

_SUMMARY_MARKERS = ("total", "summary", "grand total")
_LABEL_LIKE_FIELD_SIGNALS = ("label", "name", "title", "description", "remark", "note", "particular")
_SUMMARY_TEXT_FIELDS = ("orderdate", "paymentdate", "customername", "description", "type")
_STABLE_TRANSACTION_ID_FIELDS = (
    "ordernumber",
    "orderno",
    "orderid",
    "order_id",
    "transactionid",
    "transaction_id",
    "invoiceno",
    "invoice_no",
    "receiptno",
    "receipt_no",
    "paymentid",
    "payment_id",
)


@dataclass(frozen=True)
class TdApiClientConfig:
    timeout_ms: int = int(os.environ.get("TD_API_TIMEOUT_MS", "20000"))
    connect_timeout_ms: int = int(os.environ.get("TD_API_CONNECT_TIMEOUT_MS", "5000"))
    max_retries: int = int(os.environ.get("TD_API_MAX_RETRIES", "3"))
    backoff_base_seconds: float = float(os.environ.get("TD_API_BACKOFF_BASE_SECONDS", "0.5"))
    max_backoff_seconds: float = float(os.environ.get("TD_API_MAX_BACKOFF_SECONDS", "4.0"))
    backoff_jitter_seconds: float = float(os.environ.get("TD_API_BACKOFF_JITTER_SECONDS", "0.25"))
    min_interval_seconds: float = float(os.environ.get("TD_API_MIN_INTERVAL_SECONDS", "0.35"))
    page: int = int(os.environ.get("TD_API_PAGE", "1"))
    page_size: int = int(os.environ.get("TD_API_PAGE_SIZE", "500"))
    max_pages: int = max(1, int(os.environ.get("TD_API_MAX_PAGES", "100")))
    default_read_timeout_ms: int = int(os.environ.get("TD_API_READ_TIMEOUT_MS", "20000"))
    default_total_timeout_ms: int = int(os.environ.get("TD_API_TOTAL_TIMEOUT_MS", os.environ.get("TD_API_TIMEOUT_MS", "20000")))
    orders_read_timeout_ms: int = int(os.environ.get("TD_API_ORDERS_READ_TIMEOUT_MS", os.environ.get("TD_API_READ_TIMEOUT_MS", "20000")))
    sales_read_timeout_ms: int = int(os.environ.get("TD_API_SALES_READ_TIMEOUT_MS", "45000"))
    garments_read_timeout_ms: int = int(os.environ.get("TD_API_GARMENTS_READ_TIMEOUT_MS", "45000"))
    orders_total_timeout_ms: int = int(
        os.environ.get("TD_API_ORDERS_TOTAL_TIMEOUT_MS", os.environ.get("TD_API_TOTAL_TIMEOUT_MS", os.environ.get("TD_API_TIMEOUT_MS", "20000")))
    )
    sales_total_timeout_ms: int = int(os.environ.get("TD_API_SALES_TOTAL_TIMEOUT_MS", "60000"))
    garments_total_timeout_ms: int = int(os.environ.get("TD_API_GARMENTS_TOTAL_TIMEOUT_MS", "60000"))
    orders_max_retries: int = int(os.environ.get("TD_API_ORDERS_MAX_RETRIES", os.environ.get("TD_API_MAX_RETRIES", "3")))
    sales_max_retries: int = int(os.environ.get("TD_API_SALES_MAX_RETRIES", "5"))
    garments_max_retries: int = int(os.environ.get("TD_API_GARMENTS_MAX_RETRIES", "5"))
    orders_backoff_base_seconds: float = float(
        os.environ.get("TD_API_ORDERS_BACKOFF_BASE_SECONDS", os.environ.get("TD_API_BACKOFF_BASE_SECONDS", "0.5"))
    )
    sales_backoff_base_seconds: float = float(os.environ.get("TD_API_SALES_BACKOFF_BASE_SECONDS", "0.75"))
    garments_backoff_base_seconds: float = float(os.environ.get("TD_API_GARMENTS_BACKOFF_BASE_SECONDS", "0.75"))
    orders_max_backoff_seconds: float = float(
        os.environ.get("TD_API_ORDERS_MAX_BACKOFF_SECONDS", os.environ.get("TD_API_MAX_BACKOFF_SECONDS", "4.0"))
    )
    sales_max_backoff_seconds: float = float(os.environ.get("TD_API_SALES_MAX_BACKOFF_SECONDS", "6.0"))
    garments_max_backoff_seconds: float = float(os.environ.get("TD_API_GARMENTS_MAX_BACKOFF_SECONDS", "6.0"))
    timeout_retry_limit: int = int(os.environ.get("TD_API_TIMEOUT_RETRY_LIMIT", "2"))
    page_size_fallbacks: tuple[int, ...] = tuple(
        int(size.strip())
        for size in os.environ.get("TD_API_PAGE_SIZE_FALLBACKS", "250,100").split(",")
        if size.strip().isdigit() and int(size.strip()) > 0
    )
    source_mode: str = os.environ.get("TD_SOURCE_MODE", "ui").strip().lower()
    garments_latency_threshold_ms: int = int(os.environ.get("TD_API_GARMENTS_LATENCY_THRESHOLD_MS", "8000"))
    garments_max_wall_time_ms: int = int(os.environ.get("TD_API_GARMENTS_MAX_WALL_TIME_MS", "120000"))
    garments_max_timeout_pages: int = int(os.environ.get("TD_API_GARMENTS_MAX_TIMEOUT_PAGES", "2"))
    garments_api_only_read_timeout_ms: int = int(os.environ.get("TD_API_GARMENTS_API_ONLY_READ_TIMEOUT_MS", "25000"))
    garments_api_only_total_timeout_ms: int = int(os.environ.get("TD_API_GARMENTS_API_ONLY_TOTAL_TIMEOUT_MS", "35000"))
    garments_api_only_max_retries: int = int(os.environ.get("TD_API_GARMENTS_API_ONLY_MAX_RETRIES", "2"))
    garments_api_only_timeout_retry_limit: int = int(os.environ.get("TD_API_GARMENTS_API_ONLY_TIMEOUT_RETRY_LIMIT", "1"))
    garments_api_only_backoff_base_seconds: float = float(os.environ.get("TD_API_GARMENTS_API_ONLY_BACKOFF_BASE_SECONDS", "0.4"))
    garments_api_only_max_backoff_seconds: float = float(os.environ.get("TD_API_GARMENTS_API_ONLY_MAX_BACKOFF_SECONDS", "2.5"))


@dataclass
class TdApiFetchResult:
    raw_orders_payload: Any = field(default_factory=dict)
    raw_sales_payload: Any = field(default_factory=dict)
    raw_garments_payload: Any = field(default_factory=dict)
    orders_rows: list[dict[str, Any]] = field(default_factory=list)
    sales_rows: list[dict[str, Any]] = field(default_factory=list)
    garments_rows: list[dict[str, Any]] = field(default_factory=list)
    request_metadata: list[dict[str, Any]] = field(default_factory=list)
    endpoint_errors: dict[str, str] = field(default_factory=dict)
    endpoint_error_diagnostics: dict[str, dict[str, Any]] = field(default_factory=dict)
    endpoint_health: dict[str, dict[str, Any]] = field(default_factory=dict)
    metrics_counters: dict[str, int] = field(default_factory=dict)
    orders_summary_rows_filtered: int = 0
    sales_summary_rows_filtered: int = 0


@dataclass(frozen=True)
class TdApiAuthPreparationResult:
    ready: bool
    token_present: bool
    token_source: str | None
    token_expiry: str | None
    cookies_present: bool
    endpoint_readiness: dict[str, bool]
    endpoint_failure_reasons: dict[str, str]
    failure_reason: str | None


@dataclass(frozen=True)
class _TokenDiscoveryResult:
    token: str | None
    source: str | None
    expiry: str | None


@dataclass(frozen=True)
class _JsonFetchResult:
    ok: bool
    payload: Any
    error: str | None = None
    status: int | None = None
    attempts: int = 0
    auth_shape: str = "legacy"
    latency_ms: int | None = None


@dataclass(frozen=True)
class _AuthRequestShape:
    name: str
    include_authorization_header: bool
    include_token_query: bool


@dataclass(frozen=True)
class _AuthContextValidation:
    ready: bool
    requires_tokenized_auth: bool
    requires_cookie_source: bool
    token_present: bool
    token_expired: bool
    cookies_present: bool
    failure_reason: str | None = None


@dataclass
class _SharedAuthState:
    token_discovery: _TokenDiscoveryResult | None = None
    refresh_attempted: bool = False
    auth_ready_logged_endpoints: set[str] = field(default_factory=set)


class _StoreRateLimiter:
    _locks: dict[str, asyncio.Lock] = {}
    _next_allowed_at: dict[str, float] = {}

    @classmethod
    async def wait_turn(cls, store_code: str, min_interval_seconds: float) -> None:
        normalized_store = store_code.upper().strip()
        lock = cls._locks.setdefault(normalized_store, asyncio.Lock())
        async with lock:
            now = time.monotonic()
            next_allowed = cls._next_allowed_at.get(normalized_store, now)
            delay = max(0.0, next_allowed - now)
            if delay > 0:
                await asyncio.sleep(delay)
            cls._next_allowed_at[normalized_store] = time.monotonic() + max(min_interval_seconds, 0.0)


class TdApiClient:
    def __init__(
        self,
        *,
        store_code: str,
        context: BrowserContext,
        storage_state_path: Path,
        config: TdApiClientConfig | None = None,
        report_iframe_src: str | None = None,
    ) -> None:
        self.store_code = store_code.upper().strip()
        self.context = context
        self.storage_state_path = storage_state_path
        self.config = config or TdApiClientConfig()
        self._auth_state = _SharedAuthState()
        self._report_iframe_src = (report_iframe_src or "").strip() or None
        self._metrics_counters: dict[str, int] = {}

    def read_session_artifact(self) -> dict[str, Any]:
        if not self.storage_state_path.exists():
            return {}
        try:
            parsed = json.loads(self.storage_state_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    async def fetch_reports(self, *, from_date: date, to_date: date) -> TdApiFetchResult:
        common_params = self._build_base_query_params(
            from_date=from_date,
            to_date=to_date,
            page=self.config.page,
            page_size=self.config.page_size,
        )
        self._auth_state = _SharedAuthState()
        self._metrics_counters = {}
        metadata: list[dict[str, Any]] = []
        errors: dict[str, str] = {}
        error_diagnostics: dict[str, dict[str, Any]] = {}
        endpoint_health: dict[str, dict[str, Any]] = {}

        if not await self._auth_preflight(metadata=metadata):
            errors = {ORDERS_ENDPOINT: "auth_unavailable", SALES_ENDPOINT: "auth_unavailable", GARMENTS_ENDPOINT: "auth_unavailable"}
            empty_payload = {
                "data": [],
                "pages": [],
                "error": "auth_unavailable",
                "pagination": {
                    "pages_fetched": 0,
                    "total_rows": 0,
                    "reported_total_rows": None,
                    "reported_total_pages": None,
                    "rows_per_page": self.config.page_size,
                },
            }
            return TdApiFetchResult(
                raw_orders_payload=dict(empty_payload),
                raw_sales_payload=dict(empty_payload),
                raw_garments_payload=dict(empty_payload),
                request_metadata=metadata,
                endpoint_errors=errors,
                endpoint_error_diagnostics={endpoint: self._build_auth_diagnostics_payload() for endpoint in errors},
                endpoint_health={
                    endpoint: {"success": False, "final_error_class": "auth_unavailable", "attempts": 0}
                    for endpoint in errors
                },
            )

        order_payload = await self._fetch_endpoint_rows(
            endpoint=ORDERS_ENDPOINT,
            params={**common_params, "expandData": "true"},
            metadata=metadata,
            errors=errors,
            error_diagnostics=error_diagnostics,
            endpoint_health=endpoint_health,
        )
        sales_payload = await self._fetch_endpoint_rows(
            endpoint=SALES_ENDPOINT,
            params={**common_params, "expandData": "true"},
            metadata=metadata,
            errors=errors,
            error_diagnostics=error_diagnostics,
            endpoint_health=endpoint_health,
        )
        garments_payload = await self._fetch_endpoint_rows(
            endpoint=GARMENTS_ENDPOINT,
            params=common_params,
            metadata=metadata,
            errors=errors,
            error_diagnostics=error_diagnostics,
            endpoint_health=endpoint_health,
        )

        orders_rows_raw = _extract_rows(order_payload)
        sales_rows_raw = _extract_rows(sales_payload)
        garments_rows = _extract_rows(garments_payload)

        orders_rows, orders_summary_filtered = _filter_summary_rows(orders_rows_raw)
        sales_rows, sales_summary_filtered = _filter_summary_rows(sales_rows_raw)

        self._log_summary_rows_filtered(
            endpoint=ORDERS_ENDPOINT,
            summary_rows_filtered=orders_summary_filtered,
        )
        self._log_summary_rows_filtered(
            endpoint=SALES_ENDPOINT,
            summary_rows_filtered=sales_summary_filtered,
        )

        self._log_endpoint_total(endpoint=ORDERS_ENDPOINT, total_rows=len(orders_rows))
        self._log_endpoint_total(endpoint=SALES_ENDPOINT, total_rows=len(sales_rows))
        self._log_endpoint_total(endpoint=GARMENTS_ENDPOINT, total_rows=len(garments_rows))

        return TdApiFetchResult(
            raw_orders_payload=order_payload,
            raw_sales_payload=sales_payload,
            raw_garments_payload=garments_payload,
            orders_rows=orders_rows,
            sales_rows=sales_rows,
            garments_rows=garments_rows,
            request_metadata=metadata,
            endpoint_errors=errors,
            endpoint_error_diagnostics=error_diagnostics,
            endpoint_health=endpoint_health,
            metrics_counters=dict(self._metrics_counters),
            orders_summary_rows_filtered=orders_summary_filtered,
            sales_summary_rows_filtered=sales_summary_filtered,
        )

    async def prepare_auth_context(self) -> TdApiAuthPreparationResult:
        token_discovery = await self._discover_reporting_token()
        self._auth_state.token_discovery = token_discovery
        endpoint_readiness: dict[str, bool] = {}
        endpoint_failure_reasons: dict[str, str] = {}
        for endpoint in (ORDERS_ENDPOINT, SALES_ENDPOINT, GARMENTS_ENDPOINT):
            validation = self._validate_auth_context_for_endpoint(endpoint=endpoint, token_discovery=token_discovery)
            endpoint_readiness[endpoint] = validation.ready
            if validation.failure_reason:
                endpoint_failure_reasons[endpoint] = validation.failure_reason
        ready = all(endpoint_readiness.values())
        failure_reason = ";".join(
            f"{endpoint}:{reason}" for endpoint, reason in endpoint_failure_reasons.items() if reason
        ) or None
        return TdApiAuthPreparationResult(
            ready=ready,
            token_present=bool((token_discovery.token or "").strip()),
            token_source=token_discovery.source,
            token_expiry=token_discovery.expiry,
            cookies_present=self._has_cookie_auth_source(),
            endpoint_readiness=endpoint_readiness,
            endpoint_failure_reasons=endpoint_failure_reasons,
            failure_reason=failure_reason,
        )


    @staticmethod
    def _merge_query_params(*, base_params: Mapping[str, Any], overrides: Mapping[str, Any] | None = None) -> dict[str, Any]:
        merged = dict(base_params)
        if overrides:
            merged.update(dict(overrides))
        return merged

    def _build_base_query_params(self, *, from_date: date, to_date: date, page: int, page_size: int) -> dict[str, Any]:
        return {
            "startDate": from_date.isoformat(),
            "endDate": to_date.isoformat(),
            "page": int(page),
            "pageSize": int(page_size),
        }

    @staticmethod
    def _metadata_query_map(params: Mapping[str, Any]) -> dict[str, list[str]]:
        return {str(key): [str(value)] for key, value in params.items()}

    def _validate_required_query_params(self, *, endpoint: str, params: Mapping[str, Any]) -> str | None:
        if endpoint not in HAR_COMPATIBLE_ENDPOINTS:
            return None
        required_keys = ("startDate", "endDate", "page", "pageSize")
        missing = [key for key in required_keys if params.get(key) in (None, "")]
        if missing:
            logger.error(
                "TD API request assembly validation failed",
                extra={
                    "store_code": self.store_code,
                    "endpoint": endpoint,
                    "required_keys": list(required_keys),
                    "missing_keys": missing,
                    "query_params": self._metadata_query_map(params),
                },
            )
            return f"missing_required_query_params:{','.join(missing)}"
        return None

    async def _auth_preflight(self, *, metadata: list[dict[str, Any]]) -> bool:
        token_discovery = await self._discover_reporting_token()
        self._auth_state.token_discovery = token_discovery
        has_cookie_auth = self._has_cookie_auth_source()
        auth_available = bool((token_discovery.token or "").strip()) or has_cookie_auth
        if auth_available:
            return True

        metadata.append(
            {
                "endpoint": "auth_preflight",
                "method": "PRECHECK",
                "query_params": {},
                "status": None,
                "latency_ms": 0,
                "retry_count": 0,
                "token_refresh_attempted": False,
                "outcome": "auth_unavailable",
                "store_code": self.store_code,
                "token_source": token_discovery.source,
                "token_expiry": token_discovery.expiry,
                "cookies_found": has_cookie_auth,
            }
        )
        logger.error(
            "TD API auth unavailable; skipping endpoint fetches",
            extra={
                "store_code": self.store_code,
                "outcome": "auth_unavailable",
                "token_source": token_discovery.source,
                "token_expiry": token_discovery.expiry,
                "cookies_found": has_cookie_auth,
            },
        )
        return False

    def _has_cookie_auth_source(self) -> bool:
        session_artifact = self.read_session_artifact()
        cookies = session_artifact.get("cookies") if isinstance(session_artifact, Mapping) else None
        return isinstance(cookies, list) and bool(cookies)

    def _log_endpoint_total(self, *, endpoint: str, total_rows: int) -> None:
        logger.info(
            "TD API pagination totals",
            extra={
                "store_code": self.store_code,
                "endpoint": endpoint,
                "api_total_rows": total_rows,
            },
        )

    def _log_summary_rows_filtered(self, *, endpoint: str, summary_rows_filtered: int) -> None:
        logger.info(
            "TD API summary rows filtered",
            extra={
                "store_code": self.store_code,
                "endpoint": endpoint,
                "summary_rows_filtered": summary_rows_filtered,
            },
        )

    async def _fetch_endpoint_rows(
        self,
        *,
        endpoint: str,
        params: Mapping[str, Any],
        metadata: list[dict[str, Any]],
        errors: dict[str, str],
        error_diagnostics: dict[str, dict[str, Any]],
        endpoint_health: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        is_garments_endpoint = endpoint == GARMENTS_ENDPOINT
        endpoint_started_at = time.perf_counter()
        page = 1
        cumulative_rows = 0
        total_rows_hint: int | None = None
        total_pages_hint: int | None = None
        aggregated_rows: list[dict[str, Any]] = []
        page_payloads: list[Any] = []
        active_page_size = int(params.get("pageSize") or self.config.page_size)
        available_page_sizes = self._page_size_candidates(starting_page_size=active_page_size)
        page_size_index = 0
        fallback_used = False
        fallback_attempts = 0
        fallback_successes = 0
        endpoint_attempts = 0
        eventual_success_recorded = False
        retry_profile = self._retry_profile_for_endpoint(endpoint)
        garments_latency_distribution_ms: list[int] = []
        garments_timeout_count = 0
        garments_retry_count = 0
        garments_fallback_count = 0
        garments_adaptive_downgrades = 0
        garments_degraded_reason: str | None = None

        logger.info(
            "TD API endpoint pagination configuration",
            extra={
                "store_code": self.store_code,
                "endpoint": endpoint,
                "initial_page_size": active_page_size,
                "available_page_sizes": available_page_sizes,
                "max_retries": retry_profile["max_retries"],
                "timeout_retry_limit": retry_profile["timeout_retry_limit"],
                "backoff_base_seconds": retry_profile["backoff_base_seconds"],
                "max_backoff_seconds": retry_profile["max_backoff_seconds"],
                "read_timeout_ms": self._read_timeout_ms_for_endpoint(endpoint),
            },
        )

        self._log_auth_context_ready_once(endpoint=endpoint)

        while True:
            if is_garments_endpoint and self.config.garments_max_wall_time_ms > 0:
                elapsed_wall_time_ms = int((time.perf_counter() - endpoint_started_at) * 1000)
                if elapsed_wall_time_ms >= self.config.garments_max_wall_time_ms:
                    garments_degraded_reason = "garments_wall_time_budget_exhausted"
                    errors[endpoint] = garments_degraded_reason
                    logger.warning(
                        "TD API garments fetch degraded due to wall-time budget",
                        extra={
                            "store_code": self.store_code,
                            "endpoint": endpoint,
                            "degraded_reason": garments_degraded_reason,
                            "garments_elapsed_wall_time_ms": elapsed_wall_time_ms,
                            "garments_max_wall_time_ms": self.config.garments_max_wall_time_ms,
                            "page": page,
                        },
                    )
                    break

            active_page_size = available_page_sizes[page_size_index]
            page_params = self._merge_query_params(
                base_params=params,
                overrides={"page": page, "pageSize": active_page_size},
            )
            page_result = await self._get_json(
                endpoint=endpoint,
                params=page_params,
                metadata=metadata,
                connect_timeout_ms=self.config.connect_timeout_ms,
                read_timeout_ms=self._read_timeout_ms_for_endpoint(endpoint, page_size=active_page_size),
                max_retries=retry_profile["max_retries"],
                backoff_base_seconds=retry_profile["backoff_base_seconds"],
                max_backoff_seconds=retry_profile["max_backoff_seconds"],
                timeout_retry_limit=retry_profile["timeout_retry_limit"],
            )

            endpoint_attempts += max(int(page_result.attempts or 0), 0)
            if is_garments_endpoint:
                garments_retry_count += max(int(page_result.attempts or 0) - 1, 0)
                if page_result.error in _TIMEOUT_ERROR_CLASSES:
                    garments_timeout_count += 1

            if not page_result.ok:
                timeout_degradation_reached = (
                    is_garments_endpoint
                    and page_result.error in _TIMEOUT_ERROR_CLASSES
                    and self.config.garments_max_timeout_pages > 0
                    and garments_timeout_count >= self.config.garments_max_timeout_pages
                )
                if page_result.error in _TIMEOUT_ERROR_CLASSES and page_size_index < len(available_page_sizes) - 1:
                    previous_page_size = active_page_size
                    fallback_attempts += 1
                    page_size_index += 1
                    next_page_size = available_page_sizes[page_size_index]
                    fallback_used = True
                    if is_garments_endpoint:
                        garments_fallback_count += 1
                    metadata.append(
                        {
                            "endpoint": endpoint,
                            "method": "GET",
                            "query_params": self._metadata_query_map(
                                self._merge_query_params(
                                    base_params=params,
                                    overrides={"page": page, "pageSize": previous_page_size},
                                )
                            ),
                            "status": page_result.status,
                            "latency_ms": None,
                            "retry_count": retry_profile["max_retries"],
                            "token_refresh_attempted": self._auth_state.refresh_attempted,
                            "retry_reason": "page_size_fallback",
                            "fallback_page_size_from": previous_page_size,
                            "fallback_page_size_to": next_page_size,
                        }
                    )
                    logger.warning(
                        "TD API page-size fallback triggered",
                        extra={
                            "store_code": self.store_code,
                            "endpoint": endpoint,
                            "page": page,
                            "fallback_page_size_from": previous_page_size,
                            "fallback_page_size_to": next_page_size,
                            "retry_profile": retry_profile,
                            "error_class": page_result.error,
                        },
                    )
                    continue

                final_error = page_result.error or "unknown_error"
                if timeout_degradation_reached:
                    garments_degraded_reason = "garments_timeout_budget_exhausted"
                    final_error = garments_degraded_reason
                    logger.warning(
                        "TD API garments fetch degraded due to timeout budget",
                        extra={
                            "store_code": self.store_code,
                            "endpoint": endpoint,
                            "degraded_reason": garments_degraded_reason,
                            "garments_timeout_count": garments_timeout_count,
                            "garments_timeout_budget": self.config.garments_max_timeout_pages,
                            "page": page,
                        },
                    )

                errors[endpoint] = final_error
                diagnostics = self._build_auth_diagnostics_payload()
                error_diagnostics[endpoint] = diagnostics
                endpoint_health[endpoint] = {
                    "success": False,
                    "final_error_class": final_error,
                    "attempts": endpoint_attempts,
                }
                if page == 1 and page_result.status == 401:
                    logger.error(
                        "TD API endpoint unauthorized on first page",
                        extra={
                            "store_code": self.store_code,
                            "endpoint": endpoint,
                            "status": page_result.status,
                            **diagnostics,
                        },
                    )
                break

            page_payload = page_result.payload
            page_payloads.append(page_payload)
            if endpoint_attempts > 1 and not eventual_success_recorded:
                self._increment_metric(name="eventual_success_after_retry", endpoint=endpoint)
                eventual_success_recorded = True

            payload_error = self._extract_payload_error_class(page_payload)
            if payload_error:
                errors[endpoint] = payload_error
                diagnostics = self._build_auth_diagnostics_payload()
                error_diagnostics[endpoint] = diagnostics
                endpoint_health[endpoint] = {
                    "success": False,
                    "final_error_class": payload_error,
                    "attempts": endpoint_attempts,
                }
                logger.error(
                    "TD API endpoint payload reported error",
                    extra={
                        "store_code": self.store_code,
                        "endpoint": endpoint,
                        "page": page,
                        "error_class": payload_error,
                        **diagnostics,
                    },
                )
                break

            rows = _extract_rows(page_payload)
            if fallback_used:
                fallback_successes += 1
            aggregated_rows.extend(rows)
            cumulative_rows += len(rows)

            if is_garments_endpoint and page_result.latency_ms is not None:
                garments_latency_distribution_ms.append(max(int(page_result.latency_ms), 0))
                latency_threshold_ms = max(int(self.config.garments_latency_threshold_ms), 0)
                if (
                    latency_threshold_ms > 0
                    and int(page_result.latency_ms) >= latency_threshold_ms
                    and page_size_index < len(available_page_sizes) - 1
                ):
                    previous_page_size = active_page_size
                    page_size_index += 1
                    fallback_used = True
                    fallback_attempts += 1
                    garments_fallback_count += 1
                    garments_adaptive_downgrades += 1
                    logger.info(
                        "TD API garments adaptive page-size downgrade applied",
                        extra={
                            "store_code": self.store_code,
                            "endpoint": endpoint,
                            "page": page,
                            "latency_ms": page_result.latency_ms,
                            "latency_threshold_ms": latency_threshold_ms,
                            "adaptive_page_size_from": previous_page_size,
                            "adaptive_page_size_to": available_page_sizes[page_size_index],
                        },
                    )

            total_rows_hint = _extract_total_rows_hint(page_payload) or total_rows_hint
            total_pages_hint = _extract_total_pages_hint(page_payload) or total_pages_hint

            for item in metadata:
                query_params = item.get("query_params")
                if item.get("endpoint") != endpoint or not isinstance(query_params, dict):
                    continue
                page_values = query_params.get("page")
                if page_values != [str(page)]:
                    continue
                item["page_number"] = page
                item["rows_in_page"] = len(rows)
                item["rows_per_page"] = active_page_size
                item["cumulative_rows"] = cumulative_rows

            if not rows:
                break
            if total_pages_hint and page >= total_pages_hint:
                break
            if total_rows_hint is not None and cumulative_rows >= total_rows_hint:
                break
            if page >= self.config.max_pages:
                logger.warning(
                    "TD API max page cap reached before dataset completion",
                    extra={
                        "store_code": self.store_code,
                        "endpoint": endpoint,
                        "max_pages": self.config.max_pages,
                        "cumulative_rows": cumulative_rows,
                        "reported_total_rows": total_rows_hint,
                        "reported_total_pages": total_pages_hint,
                    },
                )
                break

            page += 1

        endpoint_health.setdefault(
            endpoint,
            {
                "success": True,
                "final_error_class": None,
                "attempts": endpoint_attempts,
            },
        )

        total_wall_time_ms = int((time.perf_counter() - endpoint_started_at) * 1000)
        logger.info(
            "TD API page-size fallback decision",
            extra={
                "store_code": self.store_code,
                "endpoint": endpoint,
                "fallback_used": fallback_used,
                "final_page_size": available_page_sizes[page_size_index],
                "available_page_sizes": available_page_sizes,
                "fallback_attempts": fallback_attempts,
                "fallback_successes": fallback_successes,
            },
        )

        if is_garments_endpoint:
            latency_p50 = _percentile(garments_latency_distribution_ms, 50)
            latency_p95 = _percentile(garments_latency_distribution_ms, 95)
            latency_max = max(garments_latency_distribution_ms) if garments_latency_distribution_ms else 0
            logger.info(
                "TD API garments compact metrics summary",
                extra={
                    "store_code": self.store_code,
                    "endpoint": endpoint,
                    "garments_metrics_before_page_size": int(params.get("pageSize") or self.config.page_size),
                    "garments_metrics_before_max_retries": retry_profile["max_retries"],
                    "garments_metrics_before_timeout_retry_limit": retry_profile["timeout_retry_limit"],
                    "garments_metrics_after_page_size": available_page_sizes[page_size_index],
                    "garments_metrics_after_rows": cumulative_rows,
                    "garments_fetch_total_wall_time_ms": total_wall_time_ms,
                    "garments_page_latency_p50_ms": latency_p50,
                    "garments_page_latency_p95_ms": latency_p95,
                    "garments_page_latency_max_ms": latency_max,
                    "garments_timeout_count": garments_timeout_count,
                    "garments_retry_count": garments_retry_count,
                    "garments_page_size_fallback_count": garments_fallback_count,
                    "garments_adaptive_downgrade_count": garments_adaptive_downgrades,
                    "garments_degraded_reason": garments_degraded_reason or errors.get(endpoint),
                    "source_mode": self.config.source_mode,
                },
            )

        if fallback_attempts > 0:
            self._increment_metric(
                name="fallback_page_size_attempts",
                endpoint=endpoint,
                count=fallback_attempts,
            )
            self._increment_metric(
                name="fallback_page_size_successes",
                endpoint=endpoint,
                count=fallback_successes,
            )
        return {
            "data": aggregated_rows,
            "pages": page_payloads,
            "error": errors.get(endpoint),
            "pagination": {
                "pages_fetched": len(page_payloads),
                "total_rows": cumulative_rows,
                "reported_total_rows": total_rows_hint,
                "reported_total_pages": total_pages_hint,
                "rows_per_page": available_page_sizes[page_size_index],
            },
        }

    @staticmethod
    def _extract_payload_error_class(payload: Any) -> str | None:
        if not isinstance(payload, Mapping):
            return None
        raw_error = payload.get("error")
        if raw_error is None:
            return None
        error_text = str(raw_error).strip()
        return error_text or "unknown_error"

    def _read_timeout_ms_for_endpoint(self, endpoint: str, *, page_size: int | None = None) -> int:
        if endpoint == ORDERS_ENDPOINT:
            return self.config.orders_read_timeout_ms
        if endpoint == SALES_ENDPOINT:
            return self.config.sales_read_timeout_ms
        if endpoint == GARMENTS_ENDPOINT:
            if self.config.source_mode == "api_only":
                return max(1, self.config.garments_api_only_read_timeout_ms)
            return self.config.garments_read_timeout_ms
        return self.config.default_read_timeout_ms

    def _total_timeout_ms_for_endpoint(self, endpoint: str) -> int:
        if endpoint == ORDERS_ENDPOINT:
            return self.config.orders_total_timeout_ms
        if endpoint == SALES_ENDPOINT:
            return self.config.sales_total_timeout_ms
        if endpoint == GARMENTS_ENDPOINT:
            if self.config.source_mode == "api_only":
                return max(1, self.config.garments_api_only_total_timeout_ms)
            return self.config.garments_total_timeout_ms
        return self.config.default_total_timeout_ms


    def _retry_profile_for_endpoint(self, endpoint: str) -> dict[str, float | int]:
        if endpoint == SALES_ENDPOINT:
            return {
                "max_retries": self.config.sales_max_retries,
                "backoff_base_seconds": self.config.sales_backoff_base_seconds,
                "max_backoff_seconds": self.config.sales_max_backoff_seconds,
                "timeout_retry_limit": max(0, self.config.timeout_retry_limit),
            }
        if endpoint == GARMENTS_ENDPOINT:
            if self.config.source_mode == "api_only":
                return {
                    "max_retries": max(0, self.config.garments_api_only_max_retries),
                    "backoff_base_seconds": max(0.0, self.config.garments_api_only_backoff_base_seconds),
                    "max_backoff_seconds": max(0.0, self.config.garments_api_only_max_backoff_seconds),
                    "timeout_retry_limit": max(0, self.config.garments_api_only_timeout_retry_limit),
                }
            return {
                "max_retries": self.config.garments_max_retries,
                "backoff_base_seconds": self.config.garments_backoff_base_seconds,
                "max_backoff_seconds": self.config.garments_max_backoff_seconds,
                "timeout_retry_limit": max(0, self.config.timeout_retry_limit),
            }
        return {
            "max_retries": self.config.orders_max_retries,
            "backoff_base_seconds": self.config.orders_backoff_base_seconds,
            "max_backoff_seconds": self.config.orders_max_backoff_seconds,
            "timeout_retry_limit": max(0, self.config.timeout_retry_limit),
        }

    def _page_size_candidates(self, *, starting_page_size: int) -> list[int]:
        candidates = [starting_page_size]
        for fallback_size in self.config.page_size_fallbacks:
            if fallback_size < starting_page_size and fallback_size not in candidates:
                candidates.append(fallback_size)
        return candidates

    def _increment_metric(self, *, name: str, endpoint: str, count: int = 1, **labels: str) -> None:
        label_components = [f"endpoint={endpoint}"]
        for key in sorted(labels):
            label_components.append(f"{key}={labels[key]}")
        metric_key = f"{name}|" + "|".join(label_components)
        self._metrics_counters[metric_key] = self._metrics_counters.get(metric_key, 0) + max(0, int(count))

    def _build_auth_diagnostics_payload(self) -> dict[str, Any]:
        token_discovery = self._auth_state.token_discovery or _TokenDiscoveryResult(token=None, source=None, expiry=None)
        return {
            "token_found": bool((token_discovery.token or "").strip()),
            "token_source": token_discovery.source,
            "token_expiry": token_discovery.expiry,
            "cookies_found": self._has_cookie_auth_source(),
        }

    def _auth_usage_flags(
        self,
        *,
        headers: Mapping[str, str],
        request_params: Mapping[str, Any],
        token_source: str | None,
    ) -> dict[str, Any]:
        return {
            "auth_context_used_authorization_header": bool((headers.get("Authorization") or "").strip()),
            "auth_context_used_token_query": bool((request_params.get("token") or "").strip()),
            "auth_context_used_token_source_known": bool(token_source),
            "auth_context_used_cookies_present": self._has_cookie_auth_source(),
        }

    def _auth_shapes_for_endpoint(self, endpoint: str) -> tuple[_AuthRequestShape, ...]:
        if endpoint in HAR_COMPATIBLE_ENDPOINTS:
            return (
                _AuthRequestShape(name="legacy", include_authorization_header=True, include_token_query=True),
                _AuthRequestShape(name="har_like", include_authorization_header=False, include_token_query=False),
            )
        return (_AuthRequestShape(name="legacy", include_authorization_header=True, include_token_query=True),)

    def _should_attempt_auth_shape_fallback(self, *, endpoint: str, shape_result: _JsonFetchResult) -> bool:
        if endpoint not in HAR_COMPATIBLE_ENDPOINTS:
            return False
        if shape_result.ok:
            return False
        if shape_result.status in {401, 403}:
            return True
        return shape_result.error in {"http_401", "http_403"}

    def _request_context_headers(self) -> dict[str, str]:
        headers = {"accept": "*/*", "origin": f"https://{REPORTS_ORIGIN_HOST}", "referer": f"https://{REPORTS_ORIGIN_HOST}/"}
        iframe_src = (self._report_iframe_src or "").strip()
        if not iframe_src:
            return headers
        parsed_iframe = urlparse(iframe_src)
        if parsed_iframe.scheme and parsed_iframe.netloc:
            headers["origin"] = f"{parsed_iframe.scheme}://{parsed_iframe.netloc}"
            headers["referer"] = iframe_src
        return headers

    def _log_auth_context_ready_once(self, *, endpoint: str) -> None:
        if endpoint in self._auth_state.auth_ready_logged_endpoints:
            return
        token_discovery = self._auth_state.token_discovery or _TokenDiscoveryResult(token=None, source=None, expiry=None)
        logger.info(
            "TD API auth context ready",
            extra={
                "store_code": self.store_code,
                "endpoint": endpoint,
                "token_present": bool((token_discovery.token or "").strip()),
                "token_source": token_discovery.source,
                "token_expiry": token_discovery.expiry,
                "cookies_present": self._has_cookie_auth_source(),
            },
        )
        self._auth_state.auth_ready_logged_endpoints.add(endpoint)

    @staticmethod
    def _is_token_expired(expiry: str | None) -> bool:
        if not expiry:
            return False
        try:
            normalized = expiry.replace("Z", "+00:00")
            parsed_expiry = datetime.fromisoformat(normalized)
        except ValueError:
            return False
        if parsed_expiry.tzinfo is None:
            parsed_expiry = parsed_expiry.replace(tzinfo=timezone.utc)
        return parsed_expiry <= datetime.now(timezone.utc)

    def _validate_auth_context_for_endpoint(
        self,
        *,
        endpoint: str,
        token_discovery: _TokenDiscoveryResult,
    ) -> _AuthContextValidation:
        token_present = bool((token_discovery.token or "").strip())
        token_expired = self._is_token_expired(token_discovery.expiry)
        cookies_present = self._has_cookie_auth_source()
        requires_tokenized_auth = endpoint not in HAR_COMPATIBLE_ENDPOINTS
        requires_cookie_source = endpoint in HAR_COMPATIBLE_ENDPOINTS

        if requires_tokenized_auth and (not token_present or token_expired):
            failure_reason = "token_missing_or_expired"
        elif requires_cookie_source and not cookies_present:
            failure_reason = "cookie_session_missing"
        elif token_present and token_expired and not cookies_present:
            failure_reason = "token_expired_and_cookie_session_missing"
        else:
            failure_reason = None

        return _AuthContextValidation(
            ready=failure_reason is None,
            requires_tokenized_auth=requires_tokenized_auth,
            requires_cookie_source=requires_cookie_source,
            token_present=token_present,
            token_expired=token_expired,
            cookies_present=cookies_present,
            failure_reason=failure_reason,
        )

    async def _get_paginated_json(
        self,
        *,
        endpoint: str,
        params: Mapping[str, Any],
        metadata: list[dict[str, Any]],
    ) -> dict[str, Any]:
        errors: dict[str, str] = {}
        error_diagnostics: dict[str, dict[str, Any]] = {}
        endpoint_health: dict[str, dict[str, Any]] = {}
        return await self._fetch_endpoint_rows(
            endpoint=endpoint,
            params=params,
            metadata=metadata,
            errors=errors,
            error_diagnostics=error_diagnostics,
            endpoint_health=endpoint_health,
        )

    async def _get_json(
        self,
        *,
        endpoint: str,
        params: Mapping[str, Any],
        metadata: list[dict[str, Any]],
        connect_timeout_ms: int | None = None,
        read_timeout_ms: int | None = None,
        max_retries: int | None = None,
        backoff_base_seconds: float | None = None,
        max_backoff_seconds: float | None = None,
        timeout_retry_limit: int | None = None,
    ) -> _JsonFetchResult:
        url = f"{REPORTING_API_BASE_URL}{endpoint}"
        token_discovery = await self._discover_reporting_token()
        iframe_token_discovery = self._discover_token_from_iframe_url_query()
        request_context_headers = self._request_context_headers()
        request_params = dict(params)
        token_value = (token_discovery.token or "").strip()

        validation_error = self._validate_required_query_params(endpoint=endpoint, params=request_params)
        if validation_error:
            metadata.append(
                {
                    "endpoint": endpoint,
                    "method": "GET",
                    "query_params": self._metadata_query_map(request_params),
                    "status": None,
                    "latency_ms": 0,
                    "retry_count": 0,
                    "token_refresh_attempted": self._auth_state.refresh_attempted,
                    "retry_reason": validation_error,
                }
            )
            return _JsonFetchResult(ok=False, payload=None, error=validation_error, status=None, attempts=0)

        if iframe_token_discovery.token and not token_value:
            logger.warning(
                "TD API auth regression warning: iframe token discovered but request token missing",
                extra={
                    "store_code": self.store_code,
                    "endpoint": endpoint,
                    "token_found": False,
                    "token_source": token_discovery.source,
                    "iframe_token_source": iframe_token_discovery.source,
                },
            )

        auth_validation = self._validate_auth_context_for_endpoint(endpoint=endpoint, token_discovery=token_discovery)
        if not auth_validation.ready:
            await self._attempt_auth_refresh_once()
            token_discovery = await self._discover_reporting_token(force_refresh=True)
            token_value = (token_discovery.token or "").strip()
            auth_validation = self._validate_auth_context_for_endpoint(endpoint=endpoint, token_discovery=token_discovery)

        if not auth_validation.ready:
            metadata.append(
                {
                    "endpoint": endpoint,
                    "method": "GET",
                    "query_params": self._metadata_query_map(request_params),
                    "status": None,
                    "latency_ms": 0,
                    "retry_count": 0,
                    "token_refresh_attempted": self._auth_state.refresh_attempted,
                    "retry_reason": "auth_context_not_ready",
                    "auth_context_failure_reason": auth_validation.failure_reason,
                    "auth_context_requires_tokenized_auth": auth_validation.requires_tokenized_auth,
                    "auth_context_requires_cookie_source": auth_validation.requires_cookie_source,
                    "auth_context_token_present": auth_validation.token_present,
                    "auth_context_token_expired": auth_validation.token_expired,
                    "auth_context_cookies_present": auth_validation.cookies_present,
                }
            )
            return _JsonFetchResult(ok=False, payload=None, error="auth_context_not_ready", status=None, attempts=0)

        auth_shapes = self._auth_shapes_for_endpoint(endpoint)
        primary_auth_shape = auth_shapes[0].name if auth_shapes else "legacy"
        baseline_status: int | None = None
        baseline_latency_ms: int | None = None
        cumulative_attempts = 0
        shape_last_result: _JsonFetchResult | None = None

        for shape_index, auth_shape in enumerate(auth_shapes):
            shape_headers = dict(request_context_headers)
            shape_params = dict(request_params)
            if token_value and auth_shape.include_authorization_header:
                shape_headers["Authorization"] = f"Bearer {token_value}"
            if token_value and auth_shape.include_token_query:
                shape_params["token"] = token_value

            if auth_shape.name == "legacy" and endpoint in HAR_COMPATIBLE_ENDPOINTS and not token_value:
                fail_fast_error = "missing_auth_token_at_dispatch"
                metadata.append(
                    {
                        "endpoint": endpoint,
                        "method": "GET",
                        "query_params": self._metadata_query_map(shape_params),
                        "status": None,
                        "latency_ms": 0,
                        "retry_count": 0,
                        "token_refresh_attempted": self._auth_state.refresh_attempted,
                        "retry_reason": fail_fast_error,
                        "primary_auth_shape": primary_auth_shape,
                        "auth_shape": auth_shape.name,
                        "auth_shape_fallback_from_har_like": auth_shape.name != primary_auth_shape,
                    }
                )
                shape_last_result = _JsonFetchResult(ok=False, payload=None, error=fail_fast_error, status=None, attempts=0, auth_shape=auth_shape.name)
                continue

            shape_result = await self._execute_json_request_shape(
                endpoint=endpoint,
                url=url,
                metadata=metadata,
                request_params=shape_params,
                headers=shape_headers,
                token_discovery=token_discovery,
                connect_timeout_ms=connect_timeout_ms,
                read_timeout_ms=read_timeout_ms,
                max_retries=max_retries,
                backoff_base_seconds=backoff_base_seconds,
                max_backoff_seconds=max_backoff_seconds,
                timeout_retry_limit=timeout_retry_limit,
                auth_shape=auth_shape,
                baseline_status=baseline_status,
                baseline_latency_ms=baseline_latency_ms,
                primary_auth_shape=primary_auth_shape,
                auth_shape_fallback_used=auth_shape.name != primary_auth_shape,
            )
            cumulative_attempts += shape_result.attempts
            shape_last_result = shape_result
            if shape_index == 0:
                baseline_status = shape_result.status
                baseline_latency_ms = shape_result.latency_ms
            should_fallback = self._should_attempt_auth_shape_fallback(endpoint=endpoint, shape_result=shape_result)
            if shape_result.ok or shape_index >= len(auth_shapes) - 1 or not should_fallback:
                return _JsonFetchResult(
                    ok=shape_result.ok,
                    payload=shape_result.payload,
                    error=shape_result.error,
                    status=shape_result.status,
                    attempts=cumulative_attempts,
                    auth_shape=shape_result.auth_shape,
                    latency_ms=shape_result.latency_ms,
                )

            logger.info(
                "TD API auth shape fallback engaged from primary auth shape",
                extra={
                    "store_code": self.store_code,
                    "endpoint": endpoint,
                    "primary_auth_shape": primary_auth_shape,
                    "previous_auth_shape": auth_shape.name,
                    "next_auth_shape": auth_shapes[shape_index + 1].name,
                    "baseline_status": baseline_status,
                    "baseline_latency_ms": baseline_latency_ms,
                },
            )

        return _JsonFetchResult(
            ok=False,
            payload=None,
            error=(shape_last_result.error if shape_last_result else "unknown_error"),
            status=(shape_last_result.status if shape_last_result else None),
            attempts=cumulative_attempts,
            auth_shape=(shape_last_result.auth_shape if shape_last_result else "legacy"),
            latency_ms=(shape_last_result.latency_ms if shape_last_result else None),
        )

    async def _execute_json_request_shape(
        self,
        *,
        endpoint: str,
        url: str,
        metadata: list[dict[str, Any]],
        request_params: Mapping[str, Any],
        headers: Mapping[str, str],
        token_discovery: _TokenDiscoveryResult,
        connect_timeout_ms: int | None,
        read_timeout_ms: int | None,
        max_retries: int | None,
        backoff_base_seconds: float | None,
        max_backoff_seconds: float | None,
        timeout_retry_limit: int | None,
        auth_shape: _AuthRequestShape,
        baseline_status: int | None,
        baseline_latency_ms: int | None,
        primary_auth_shape: str,
        auth_shape_fallback_used: bool,
    ) -> _JsonFetchResult:
        last_error: Exception | None = None
        status_code: int | None = None
        resolved_connect_timeout_ms = connect_timeout_ms or self.config.connect_timeout_ms or self.config.timeout_ms
        resolved_read_timeout_ms = read_timeout_ms or self.config.default_read_timeout_ms or self.config.timeout_ms
        resolved_total_timeout_ms = self._total_timeout_ms_for_endpoint(endpoint)
        resolved_max_retries = self.config.max_retries if max_retries is None else max(0, int(max_retries))
        resolved_backoff_base_seconds = self.config.backoff_base_seconds if backoff_base_seconds is None else float(backoff_base_seconds)
        resolved_max_backoff_seconds = self.config.max_backoff_seconds if max_backoff_seconds is None else float(max_backoff_seconds)
        resolved_timeout_retry_limit = (
            max(0, int(self.config.timeout_retry_limit))
            if timeout_retry_limit is None
            else max(0, int(timeout_retry_limit))
        )
        attempts = 0
        timeout_failures = 0
        for attempt in range(resolved_max_retries + 1):
            attempts = attempt + 1
            configured_attempt_timeout_budget_ms = resolved_connect_timeout_ms + resolved_read_timeout_ms
            effective_total_timeout_ms = max(1, max(configured_attempt_timeout_budget_ms, resolved_total_timeout_ms))
            timeout_diagnostics = {
                "configured": {
                    "connect_timeout_ms": resolved_connect_timeout_ms,
                    "read_timeout_ms": resolved_read_timeout_ms,
                    "total_timeout_ms": resolved_total_timeout_ms,
                    "attempt_timeout_budget_ms": configured_attempt_timeout_budget_ms,
                },
                "effective": {
                    "connect_timeout_ms": resolved_connect_timeout_ms,
                    "read_timeout_ms": resolved_read_timeout_ms,
                    "total_timeout_ms": effective_total_timeout_ms,
                    "attempt_timeout_budget_ms": effective_total_timeout_ms,
                },
            }
            logger.info(
                "TD API request attempt started",
                extra={
                    "store_code": self.store_code,
                    "endpoint": endpoint,
                    "attempt": attempts,
                    "max_attempts": resolved_max_retries + 1,
                    "timeout_diagnostics": timeout_diagnostics,
                    "primary_auth_shape": primary_auth_shape,
                    "auth_shape": auth_shape.name,
                    "auth_shape_fallback_used": auth_shape_fallback_used,
                    **self._auth_usage_flags(headers=headers, request_params=request_params, token_source=token_discovery.source),
                },
            )
            await _StoreRateLimiter.wait_turn(self.store_code, self.config.min_interval_seconds)
            started = time.perf_counter()
            retry_reason: str | None = None
            timed_out_during_response_read = False
            try:
                response = await asyncio.wait_for(
                    self.context.request.get(
                    url,
                    params=request_params,
                    headers=headers,
                    timeout=effective_total_timeout_ms,
                    ),
                    timeout=max(effective_total_timeout_ms / 1000.0, 0.001),
                )
                latency_ms = int((time.perf_counter() - started) * 1000)
                status_code = response.status

                if status_code == 401:
                    refreshed = await self._attempt_auth_refresh_once()
                    metadata.append(
                        {
                            **build_api_request_metadata(
                                url=str(response.url),
                                method="GET",
                                query_params=request_params,
                                status=status_code,
                                latency_ms=latency_ms,
                                retry_count=attempt,
                                token_refresh_attempted=refreshed,
                                retry_reason="http_401",
                            ).as_dict(),
                            "timeout_diagnostics": timeout_diagnostics,
                            "primary_auth_shape": primary_auth_shape,
                            "auth_shape": auth_shape.name,
                            "auth_shape_fallback_used": auth_shape_fallback_used,
                            "auth_shape_fallback_from_har_like": auth_shape_fallback_used,
                            "auth_shape_baseline_status": baseline_status,
                            "auth_shape_baseline_latency_ms": baseline_latency_ms,
                            "auth_shape_status_delta": _safe_delta(status_code, baseline_status),
                            "auth_shape_latency_delta_ms": _safe_delta(latency_ms, baseline_latency_ms),
                            **self._auth_usage_flags(headers=headers, request_params=request_params, token_source=token_discovery.source),
                        }
                    )
                    if refreshed:
                        refreshed_token = (self._auth_state.token_discovery.token or "").strip() if self._auth_state.token_discovery else ""
                        if refreshed_token:
                            mutable_headers = dict(headers)
                            mutable_params = dict(request_params)
                            if auth_shape.include_authorization_header:
                                mutable_headers["Authorization"] = f"Bearer {refreshed_token}"
                            if auth_shape.include_token_query:
                                mutable_params["token"] = refreshed_token
                            headers = mutable_headers
                            request_params = mutable_params
                            token_discovery = self._auth_state.token_discovery or token_discovery
                        continue
                    return _JsonFetchResult(ok=False, payload=None, error="http_401", status=status_code, attempts=attempts, auth_shape=auth_shape.name, latency_ms=latency_ms)

                metadata.append(
                    {
                        **build_api_request_metadata(
                        url=str(response.url),
                        method="GET",
                        query_params=request_params,
                        status=status_code,
                        latency_ms=latency_ms,
                        retry_count=attempt,
                        token_refresh_attempted=self._auth_state.refresh_attempted,
                    ).as_dict(),
                        "timeout_diagnostics": timeout_diagnostics,
                        "primary_auth_shape": primary_auth_shape,
                        "auth_shape": auth_shape.name,
                        "auth_shape_fallback_used": auth_shape_fallback_used,
                        "auth_shape_fallback_from_har_like": auth_shape_fallback_used,
                        "auth_shape_baseline_status": baseline_status,
                        "auth_shape_baseline_latency_ms": baseline_latency_ms,
                        "auth_shape_status_delta": _safe_delta(status_code, baseline_status),
                        "auth_shape_latency_delta_ms": _safe_delta(latency_ms, baseline_latency_ms),
                        **self._auth_usage_flags(headers=headers, request_params=request_params, token_source=token_discovery.source),
                    }
                )

                if status_code < 400:
                    timed_out_during_response_read = True
                    payload = await asyncio.wait_for(
                        response.json(),
                        timeout=max(resolved_read_timeout_ms / 1000.0, 0.001),
                    )
                    logger.info(
                        "TD API request attempt succeeded",
                        extra={
                            "store_code": self.store_code,
                            "endpoint": endpoint,
                            "attempt": attempts,
                            "max_attempts": resolved_max_retries + 1,
                            "timeout_diagnostics": timeout_diagnostics,
                        },
                    )
                    return _JsonFetchResult(ok=True, payload=payload, status=status_code, attempts=attempts, auth_shape=auth_shape.name, latency_ms=latency_ms)
                if status_code not in {408, 429, 500, 502, 503, 504}:
                    return _JsonFetchResult(ok=False, payload=None, error=f"http_{status_code}", status=status_code, attempts=attempts, auth_shape=auth_shape.name, latency_ms=latency_ms)
                last_error = RuntimeError(f"HTTP {status_code} from {endpoint}")
                retry_reason = f"http_{status_code}"
            except asyncio.TimeoutError:
                status_code = None
                latency_ms = int((time.perf_counter() - started) * 1000)
                timeout_class = "read_timeout" if timed_out_during_response_read else "total_timeout"
                retry_reason = timeout_class
                metadata.append(
                    {
                        **build_api_request_metadata(
                        url=url,
                        method="GET",
                        query_params=request_params,
                        status=status_code,
                        latency_ms=latency_ms,
                        retry_count=attempt,
                        token_refresh_attempted=self._auth_state.refresh_attempted,
                        retry_reason=timeout_class,
                    ).as_dict(),
                        "timeout_type": timeout_class,
                        "timeout_diagnostics": timeout_diagnostics,
                        "primary_auth_shape": primary_auth_shape,
                        "auth_shape": auth_shape.name,
                        "auth_shape_fallback_used": auth_shape_fallback_used,
                        "auth_shape_fallback_from_har_like": auth_shape_fallback_used,
                        "auth_shape_baseline_status": baseline_status,
                        "auth_shape_baseline_latency_ms": baseline_latency_ms,
                        "auth_shape_status_delta": _safe_delta(status_code, baseline_status),
                        "auth_shape_latency_delta_ms": _safe_delta(latency_ms, baseline_latency_ms),
                        **self._auth_usage_flags(headers=headers, request_params=request_params, token_source=token_discovery.source),
                    }
                )
                self._increment_metric(name="timeout_class", endpoint=endpoint, timeout_class=timeout_class)
                last_error = RuntimeError(timeout_class)
            except PlaywrightError as exc:
                latency_ms = int((time.perf_counter() - started) * 1000)
                retry_reason = "network_timeout"
                metadata.append(
                    {
                        **build_api_request_metadata(
                        url=url,
                        method="GET",
                        query_params=request_params,
                        status=status_code,
                        latency_ms=latency_ms,
                        retry_count=attempt,
                        token_refresh_attempted=self._auth_state.refresh_attempted,
                        retry_reason="network_timeout",
                    ).as_dict(),
                        "timeout_diagnostics": timeout_diagnostics,
                        "primary_auth_shape": primary_auth_shape,
                        "auth_shape": auth_shape.name,
                        "auth_shape_fallback_used": auth_shape_fallback_used,
                        "auth_shape_fallback_from_har_like": auth_shape_fallback_used,
                        "auth_shape_baseline_status": baseline_status,
                        "auth_shape_baseline_latency_ms": baseline_latency_ms,
                        "auth_shape_status_delta": _safe_delta(status_code, baseline_status),
                        "auth_shape_latency_delta_ms": _safe_delta(latency_ms, baseline_latency_ms),
                        **self._auth_usage_flags(headers=headers, request_params=request_params, token_source=token_discovery.source),
                    }
                )
                last_error = exc

            final_error_class = retry_reason or (str(last_error) if last_error else "unknown_error")
            logger.warning(
                "TD API request attempt failed",
                extra={
                    "store_code": self.store_code,
                    "endpoint": endpoint,
                    "attempt": attempts,
                    "max_attempts": resolved_max_retries + 1,
                    "timeout_type": retry_reason,
                    "timeout_diagnostics": timeout_diagnostics,
                    "retry_reason": retry_reason,
                    "final_error_class": final_error_class,
                },
            )

            should_retry = attempt < resolved_max_retries
            if retry_reason in _TIMEOUT_ERROR_CLASSES:
                timeout_failures += 1
                should_retry = should_retry and timeout_failures <= resolved_timeout_retry_limit

            if should_retry:
                backoff_ceiling = min(resolved_max_backoff_seconds, resolved_backoff_base_seconds * (2**attempt))
                jitter_window = max(self.config.backoff_jitter_seconds, 0.0)
                jitter = random.uniform(0.0, jitter_window if jitter_window > 0 else backoff_ceiling)
                backoff = min(resolved_max_backoff_seconds, backoff_ceiling + jitter)
                await asyncio.sleep(max(backoff, 0.0))
            else:
                break

        if status_code is not None:
            return _JsonFetchResult(ok=False, payload=None, error=f"http_{status_code}", status=status_code, attempts=attempts, auth_shape=auth_shape.name)
        if last_error:
            message = str(last_error)
            if message in {"connect_timeout", "read_timeout", "total_timeout"}:
                return _JsonFetchResult(ok=False, payload=None, error=message, status=None, attempts=attempts, auth_shape=auth_shape.name)
            return _JsonFetchResult(ok=False, payload=None, error=type(last_error).__name__, status=None, attempts=attempts, auth_shape=auth_shape.name)
        return _JsonFetchResult(ok=False, payload=None, error="unknown_error", status=None, attempts=attempts, auth_shape=auth_shape.name)

    async def _attempt_auth_refresh_once(self) -> bool:
        if self._auth_state.refresh_attempted:
            return False
        self._auth_state.refresh_attempted = True
        refreshed_discovery = await self._discover_reporting_token(force_refresh=True)
        self._auth_state.token_discovery = refreshed_discovery
        return bool((refreshed_discovery.token or "").strip())

    async def _discover_reporting_token(self, *, force_refresh: bool = False) -> _TokenDiscoveryResult:
        cached_discovery = self._auth_state.token_discovery
        if cached_discovery is not None and not force_refresh:
            if cached_discovery.source == "iframe_url_query" and cached_discovery.token:
                return cached_discovery

            from_iframe_latest = self._discover_token_from_iframe_url_query()
            if from_iframe_latest.token:
                if cached_discovery.token != from_iframe_latest.token or cached_discovery.source != "iframe_url_query":
                    self._auth_state.token_discovery = from_iframe_latest
                    self._log_token_diagnostics(from_iframe_latest)
                return self._auth_state.token_discovery

            from_iframe_snapshot = self._discover_token_from_report_iframe_src()
            if from_iframe_snapshot.token:
                if cached_discovery.token != from_iframe_snapshot.token or cached_discovery.source != "iframe_src_snapshot":
                    self._auth_state.token_discovery = from_iframe_snapshot
                    self._log_token_diagnostics(from_iframe_snapshot)
                return self._auth_state.token_discovery

            return cached_discovery

        from_iframe = self._discover_token_from_iframe_url_query()
        if from_iframe.token:
            self._auth_state.token_discovery = from_iframe
            self._log_token_diagnostics(from_iframe)
            return from_iframe

        from_iframe_snapshot = self._discover_token_from_report_iframe_src()
        if from_iframe_snapshot.token:
            self._auth_state.token_discovery = from_iframe_snapshot
            self._log_token_diagnostics(from_iframe_snapshot)
            return from_iframe_snapshot

        from_runtime_storage = await self._discover_token_from_runtime_storage()
        if from_runtime_storage.token:
            self._auth_state.token_discovery = from_runtime_storage
            self._log_token_diagnostics(from_runtime_storage)
            return from_runtime_storage

        from_storage_state = self._discover_token_from_storage_state()
        self._auth_state.token_discovery = from_storage_state
        self._log_token_diagnostics(from_storage_state)
        return from_storage_state


    def _discover_token_from_report_iframe_src(self) -> _TokenDiscoveryResult:
        iframe_src = (self._report_iframe_src or "").strip()
        if not iframe_src:
            return _TokenDiscoveryResult(token=None, source=None, expiry=None)
        parsed = urlparse(iframe_src)
        if REPORTS_ORIGIN_HOST not in (parsed.netloc or ""):
            return _TokenDiscoveryResult(token=None, source=None, expiry=None)
        token_values = parse_qs(parsed.query, keep_blank_values=False).get("token")
        if token_values:
            token = (token_values[0] or "").strip()
            if token:
                decoded_token = unquote(token).strip()
                if decoded_token:
                    return _TokenDiscoveryResult(
                        token=decoded_token,
                        source="iframe_src_snapshot",
                        expiry=parse_token_expiry(decoded_token),
                    )
        return _TokenDiscoveryResult(token=None, source=None, expiry=None)

    def _discover_token_from_iframe_url_query(self) -> _TokenDiscoveryResult:
        for page in self.context.pages:
            for frame in page.frames:
                frame_url = frame.url or ""
                parsed = urlparse(frame_url)
                if REPORTS_ORIGIN_HOST not in parsed.netloc:
                    continue
                token_values = parse_qs(parsed.query, keep_blank_values=False).get("token")
                if token_values:
                    token = (token_values[0] or "").strip()
                    if token:
                        return _TokenDiscoveryResult(
                            token=token,
                            source="iframe_url_query",
                            expiry=parse_token_expiry(token),
                        )
        return _TokenDiscoveryResult(token=None, source=None, expiry=None)

    async def _discover_token_from_runtime_storage(self) -> _TokenDiscoveryResult:
        for page in self.context.pages:
            for frame in page.frames:
                token = await _extract_token_from_frame_storage(frame)
                if token:
                    return _TokenDiscoveryResult(
                        token=token,
                        source="runtime_storage",
                        expiry=parse_token_expiry(token),
                    )
        return _TokenDiscoveryResult(token=None, source=None, expiry=None)

    def _discover_token_from_storage_state(self) -> _TokenDiscoveryResult:
        session_artifact = self.read_session_artifact()
        origins = session_artifact.get("origins") if isinstance(session_artifact, Mapping) else None
        if not isinstance(origins, list):
            return _TokenDiscoveryResult(token=None, source=None, expiry=None)

        for origin in origins:
            if not isinstance(origin, Mapping):
                continue
            origin_url = str(origin.get("origin") or "")
            if REPORTS_ORIGIN_HOST not in origin_url:
                continue
            local_storage = origin.get("localStorage")
            if not isinstance(local_storage, list):
                continue
            for entry in local_storage:
                if not isinstance(entry, Mapping):
                    continue
                name = str(entry.get("name") or "").lower()
                value = str(entry.get("value") or "").strip()
                if value and any(key in name for key in ("token", "auth", "jwt", "bearer")):
                    return _TokenDiscoveryResult(
                        token=value,
                        source="storage_state",
                        expiry=parse_token_expiry(value),
                    )

        return _TokenDiscoveryResult(token=None, source="storage_state", expiry=None)

    def _log_token_diagnostics(self, result: _TokenDiscoveryResult) -> None:
        logger.info(
            "TD reporting token diagnostics",
            extra={
                "token_found": bool(result.token),
                "token_source": result.source,
                "token_expiry": result.expiry,
            },
        )


async def _extract_token_from_frame_storage(frame: Frame) -> str | None:
    try:
        payload = await frame.evaluate(
            """() => {
                const matches = [];
                const hasSignal = (key) => /token|auth|jwt|bearer/i.test(String(key || ""));
                const collect = (storage) => {
                    if (!storage) return;
                    for (let i = 0; i < storage.length; i += 1) {
                        const key = storage.key(i);
                        if (!hasSignal(key)) continue;
                        const value = storage.getItem(key);
                        if (value) {
                            matches.push(String(value));
                        }
                    }
                };
                collect(window.localStorage);
                collect(window.sessionStorage);
                return matches;
            }"""
        )
    except PlaywrightError:
        return None
    except Exception:
        return None

    if isinstance(payload, list):
        for candidate in payload:
            value = str(candidate or "").strip()
            if value:
                return value
    return None


def _extract_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("data", "items", "rows", "results"):
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
    nested_data = payload.get("data")
    if isinstance(nested_data, dict):
        for key in ("items", "rows", "results"):
            value = nested_data.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
    return []


def _filter_summary_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    filtered_rows: list[dict[str, Any]] = []
    summary_rows_filtered = 0
    for row in rows:
        if _is_summary_or_footer_row(row):
            summary_rows_filtered += 1
            continue
        filtered_rows.append(row)
    return filtered_rows, summary_rows_filtered


def _is_summary_or_footer_row(row: Mapping[str, Any]) -> bool:
    normalized_values = {str(key).strip().lower(): value for key, value in row.items()}
    order_number = _first_non_empty_value(normalized_values, ("ordernumber", "orderno", "order_number"))

    # Some TD API pages emit footer rows like "Total Order" while still carrying a
    # numeric orderNumber equal to page-row-count. Treat explicit summary labels as
    # authoritative so these rows do not leak into compare/export datasets.
    if _summary_text_field_contains_marker(normalized_values) or _label_field_contains_summary_marker(normalized_values):
        return True

    if not order_number and _row_contains_summary_marker(normalized_values):
        return True

    if _has_stable_transaction_identifier(normalized_values):
        return False

    if not _row_contains_summary_marker(normalized_values):
        return False

    numeric_fields = 0
    non_numeric_non_empty_fields = 0
    for value in normalized_values.values():
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        if _looks_numeric(text):
            numeric_fields += 1
        elif not _contains_summary_marker(text):
            non_numeric_non_empty_fields += 1

    return numeric_fields >= 1 and non_numeric_non_empty_fields == 0


def _first_non_empty_value(values: Mapping[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        candidate = values.get(key)
        text = str(candidate or "").strip()
        if text:
            return text
    return ""


def _safe_delta(current: int | None, baseline: int | None) -> int | None:
    if current is None or baseline is None:
        return None
    return int(current) - int(baseline)


def _percentile(values: list[int], percentile: int) -> int:
    if not values:
        return 0
    sorted_values = sorted(int(value) for value in values)
    pct = min(100, max(0, int(percentile)))
    index = int(round((pct / 100) * (len(sorted_values) - 1)))
    return sorted_values[index]


def _has_stable_transaction_identifier(values: Mapping[str, Any]) -> bool:
    for key, value in values.items():
        if key not in _STABLE_TRANSACTION_ID_FIELDS:
            continue
        text = str(value or "").strip()
        if text:
            return True
    return False




def _summary_text_field_contains_marker(values: Mapping[str, Any]) -> bool:
    for key in _SUMMARY_TEXT_FIELDS:
        if _contains_summary_marker(str(values.get(key) or "")):
            return True
    return False

def _label_field_contains_summary_marker(values: Mapping[str, Any]) -> bool:
    for key, value in values.items():
        if key in {"ordernumber", "orderno", "order_number"}:
            continue
        if not _is_label_like_key(key):
            continue
        if _contains_summary_marker(str(value or "")):
            return True
    return False


def _row_contains_summary_marker(values: Mapping[str, Any]) -> bool:
    for value in values.values():
        if _contains_summary_marker(str(value or "")):
            return True
    return False


def _contains_summary_marker(text: str) -> bool:
    lowered = text.strip().lower()
    return bool(lowered) and any(marker in lowered for marker in _SUMMARY_MARKERS)


def _is_label_like_key(key: str) -> bool:
    return any(signal in key for signal in _LABEL_LIKE_FIELD_SIGNALS)


def _looks_numeric(text: str) -> bool:
    normalized = text.replace(",", "").replace("₹", "").strip()
    if not normalized:
        return False
    if normalized.startswith("(") and normalized.endswith(")"):
        normalized = f"-{normalized[1:-1]}"
    try:
        float(normalized)
    except ValueError:
        return False
    return True


def _extract_total_rows_hint(payload: Any) -> int | None:
    candidates = _extract_pagination_candidates(payload)
    for key in ("total", "totalRows", "total_rows", "totalCount", "count"):
        value = candidates.get(key)
        if isinstance(value, int) and value >= 0:
            return value
    return None


def _extract_total_pages_hint(payload: Any) -> int | None:
    candidates = _extract_pagination_candidates(payload)
    for key in ("pages", "totalPages", "total_pages", "pageCount"):
        value = candidates.get(key)
        if isinstance(value, int) and value > 0:
            return value
    return None


def _extract_pagination_candidates(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}

    candidates: dict[str, Any] = {}
    for key in ("total", "totalRows", "total_rows", "totalCount", "count", "pages", "totalPages", "total_pages", "pageCount"):
        if key in payload:
            candidates[key] = payload.get(key)

    nested_data = payload.get("data")
    if isinstance(nested_data, dict):
        for key in ("total", "totalRows", "total_rows", "totalCount", "count", "pages", "totalPages", "total_pages", "pageCount"):
            if key in nested_data:
                candidates[key] = nested_data.get(key)

    pagination = payload.get("pagination")
    if isinstance(pagination, dict):
        for key in ("total", "totalRows", "total_rows", "totalCount", "count", "pages", "totalPages", "total_pages", "pageCount"):
            if key in pagination:
                candidates[key] = pagination.get(key)

    return candidates
