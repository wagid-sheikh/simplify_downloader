from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import parse_qs, urlparse
from playwright.async_api import BrowserContext, Error as PlaywrightError, Frame
from app.crm_downloader.td_orders_sync.td_api_compare import build_api_request_metadata, parse_token_expiry

REPORTING_API_BASE_URL = "https://reporting-api.quickdrycleaning.com"
REPORTS_ORIGIN_HOST = "reports.quickdrycleaning.com"

logger = logging.getLogger(__name__)

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
    orders_read_timeout_ms: int = int(os.environ.get("TD_API_ORDERS_READ_TIMEOUT_MS", os.environ.get("TD_API_READ_TIMEOUT_MS", "20000")))
    sales_read_timeout_ms: int = int(os.environ.get("TD_API_SALES_READ_TIMEOUT_MS", "35000"))
    garments_read_timeout_ms: int = int(os.environ.get("TD_API_GARMENTS_READ_TIMEOUT_MS", "35000"))
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
    page_size_fallbacks: tuple[int, ...] = tuple(
        int(size.strip())
        for size in os.environ.get("TD_API_PAGE_SIZE_FALLBACKS", "250,100").split(",")
        if size.strip().isdigit() and int(size.strip()) > 0
    )


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
    orders_summary_rows_filtered: int = 0
    sales_summary_rows_filtered: int = 0


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


@dataclass
class _SharedAuthState:
    token_discovery: _TokenDiscoveryResult | None = None
    refresh_attempted: bool = False


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
    ) -> None:
        self.store_code = store_code.upper().strip()
        self.context = context
        self.storage_state_path = storage_state_path
        self.config = config or TdApiClientConfig()
        self._auth_state = _SharedAuthState()

    def read_session_artifact(self) -> dict[str, Any]:
        if not self.storage_state_path.exists():
            return {}
        try:
            parsed = json.loads(self.storage_state_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    async def fetch_reports(self, *, from_date: date, to_date: date) -> TdApiFetchResult:
        common_params = {
            "pageSize": self.config.page_size,
            "startDate": from_date.isoformat(),
            "endDate": to_date.isoformat(),
        }
        self._auth_state = _SharedAuthState()
        metadata: list[dict[str, Any]] = []
        errors: dict[str, str] = {}
        error_diagnostics: dict[str, dict[str, Any]] = {}
        endpoint_health: dict[str, dict[str, Any]] = {}

        if not await self._auth_preflight(metadata=metadata):
            errors = {
                "/reports/order-report": "auth_unavailable",
                "/sales-and-deliveries/sales": "auth_unavailable",
                "/garments/details": "auth_unavailable",
            }
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
            endpoint="/reports/order-report",
            params={**common_params, "expandData": "true"},
            metadata=metadata,
            errors=errors,
            error_diagnostics=error_diagnostics,
            endpoint_health=endpoint_health,
        )
        sales_payload = await self._fetch_endpoint_rows(
            endpoint="/sales-and-deliveries/sales",
            params={**common_params, "expandData": "true"},
            metadata=metadata,
            errors=errors,
            error_diagnostics=error_diagnostics,
            endpoint_health=endpoint_health,
        )
        garments_payload = await self._fetch_endpoint_rows(
            endpoint="/garments/details",
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
            endpoint="/reports/order-report",
            summary_rows_filtered=orders_summary_filtered,
        )
        self._log_summary_rows_filtered(
            endpoint="/sales-and-deliveries/sales",
            summary_rows_filtered=sales_summary_filtered,
        )

        self._log_endpoint_total(endpoint="/reports/order-report", total_rows=len(orders_rows))
        self._log_endpoint_total(endpoint="/sales-and-deliveries/sales", total_rows=len(sales_rows))
        self._log_endpoint_total(endpoint="/garments/details", total_rows=len(garments_rows))

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
            orders_summary_rows_filtered=orders_summary_filtered,
            sales_summary_rows_filtered=sales_summary_filtered,
        )

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
        endpoint_attempts = 0
        retry_profile = self._retry_profile_for_endpoint(endpoint)
        logger.info(
            "TD API endpoint pagination configuration",
            extra={
                "store_code": self.store_code,
                "endpoint": endpoint,
                "initial_page_size": active_page_size,
                "available_page_sizes": available_page_sizes,
                "max_retries": retry_profile["max_retries"],
                "backoff_base_seconds": retry_profile["backoff_base_seconds"],
                "max_backoff_seconds": retry_profile["max_backoff_seconds"],
                "read_timeout_ms": self._read_timeout_ms_for_endpoint(endpoint),
            },
        )

        while True:
            active_page_size = available_page_sizes[page_size_index]
            page_params = {**dict(params), "page": page, "pageSize": active_page_size}
            page_result = await self._get_json(
                endpoint=endpoint,
                params=page_params,
                metadata=metadata,
                connect_timeout_ms=self.config.connect_timeout_ms,
                read_timeout_ms=self._read_timeout_ms_for_endpoint(endpoint),
                max_retries=retry_profile["max_retries"],
                backoff_base_seconds=retry_profile["backoff_base_seconds"],
                max_backoff_seconds=retry_profile["max_backoff_seconds"],
            )

            endpoint_attempts += max(int(page_result.attempts or 0), 0)

            if not page_result.ok:
                if (
                    page_result.error in {"read_timeout", "connect_timeout", "PlaywrightError", "TimeoutError", "timeout"}
                    and page_size_index < len(available_page_sizes) - 1
                ):
                    previous_page_size = active_page_size
                    page_size_index += 1
                    next_page_size = available_page_sizes[page_size_index]
                    fallback_used = True
                    metadata.append(
                        {
                            "endpoint": endpoint,
                            "method": "GET",
                            "query_params": {
                                "page": [str(page)],
                                "pageSize": [str(previous_page_size)],
                            },
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
            aggregated_rows.extend(rows)
            cumulative_rows += len(rows)

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
        logger.info(
            "TD API page-size fallback decision",
            extra={
                "store_code": self.store_code,
                "endpoint": endpoint,
                "fallback_used": fallback_used,
                "final_page_size": active_page_size,
                "available_page_sizes": available_page_sizes,
            },
        )
        return {
            "data": aggregated_rows,
            "pages": page_payloads,
            "error": errors.get(endpoint),
            "pagination": {
                "pages_fetched": page,
                "total_rows": cumulative_rows,
                "reported_total_rows": total_rows_hint,
                "reported_total_pages": total_pages_hint,
                "rows_per_page": active_page_size,
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

    def _read_timeout_ms_for_endpoint(self, endpoint: str) -> int:
        if endpoint == "/reports/order-report":
            return self.config.orders_read_timeout_ms
        if endpoint == "/sales-and-deliveries/sales":
            return self.config.sales_read_timeout_ms
        if endpoint == "/garments/details":
            return self.config.garments_read_timeout_ms
        return self.config.default_read_timeout_ms


    def _retry_profile_for_endpoint(self, endpoint: str) -> dict[str, float | int]:
        if endpoint == "/sales-and-deliveries/sales":
            return {
                "max_retries": self.config.sales_max_retries,
                "backoff_base_seconds": self.config.sales_backoff_base_seconds,
                "max_backoff_seconds": self.config.sales_max_backoff_seconds,
            }
        if endpoint == "/garments/details":
            return {
                "max_retries": self.config.garments_max_retries,
                "backoff_base_seconds": self.config.garments_backoff_base_seconds,
                "max_backoff_seconds": self.config.garments_max_backoff_seconds,
            }
        return {
            "max_retries": self.config.orders_max_retries,
            "backoff_base_seconds": self.config.orders_backoff_base_seconds,
            "max_backoff_seconds": self.config.orders_max_backoff_seconds,
        }

    def _page_size_candidates(self, *, starting_page_size: int) -> list[int]:
        candidates = [starting_page_size]
        for fallback_size in self.config.page_size_fallbacks:
            if fallback_size < starting_page_size and fallback_size not in candidates:
                candidates.append(fallback_size)
        return candidates

    def _build_auth_diagnostics_payload(self) -> dict[str, Any]:
        token_discovery = self._auth_state.token_discovery or _TokenDiscoveryResult(token=None, source=None, expiry=None)
        return {
            "token_found": bool((token_discovery.token or "").strip()),
            "token_source": token_discovery.source,
            "token_expiry": token_discovery.expiry,
            "cookies_found": self._has_cookie_auth_source(),
        }

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
    ) -> _JsonFetchResult:
        url = f"{REPORTING_API_BASE_URL}{endpoint}"
        token_discovery = await self._discover_reporting_token()
        iframe_token_discovery = self._discover_token_from_iframe_url_query()
        headers = {
            "accept": "*/*",
            "origin": "https://reports.quickdrycleaning.com",
            "referer": "https://reports.quickdrycleaning.com/",
        }
        request_params = dict(params)
        token_value = (token_discovery.token or "").strip()
        if token_value:
            headers["Authorization"] = f"Bearer {token_value}"
            request_params.setdefault("token", token_value)

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

        last_error: Exception | None = None
        status_code: int | None = None
        resolved_connect_timeout_ms = connect_timeout_ms or self.config.connect_timeout_ms or self.config.timeout_ms
        resolved_read_timeout_ms = read_timeout_ms or self.config.default_read_timeout_ms or self.config.timeout_ms
        resolved_max_retries = self.config.max_retries if max_retries is None else max(0, int(max_retries))
        resolved_backoff_base_seconds = self.config.backoff_base_seconds if backoff_base_seconds is None else float(backoff_base_seconds)
        resolved_max_backoff_seconds = self.config.max_backoff_seconds if max_backoff_seconds is None else float(max_backoff_seconds)
        attempts = 0
        for attempt in range(resolved_max_retries + 1):
            attempts = attempt + 1
            await _StoreRateLimiter.wait_turn(self.store_code, self.config.min_interval_seconds)
            started = time.perf_counter()
            try:
                response = await asyncio.wait_for(
                    self.context.request.get(
                    url,
                    params=request_params,
                    headers=headers,
                    timeout=resolved_connect_timeout_ms,
                    ),
                    timeout=max(resolved_connect_timeout_ms / 1000.0, 0.001),
                )
                latency_ms = int((time.perf_counter() - started) * 1000)
                status_code = response.status

                if status_code == 401:
                    refreshed = await self._attempt_auth_refresh_once()
                    metadata.append(
                        build_api_request_metadata(
                            url=str(response.url),
                            method="GET",
                            status=status_code,
                            latency_ms=latency_ms,
                            retry_count=attempt,
                            token_refresh_attempted=refreshed,
                            retry_reason="auth_refresh" if refreshed else None,
                        ).as_dict()
                    )
                    if refreshed:
                        refreshed_token = (self._auth_state.token_discovery.token or "").strip() if self._auth_state.token_discovery else ""
                        if refreshed_token:
                            headers["Authorization"] = f"Bearer {refreshed_token}"
                            request_params["token"] = refreshed_token
                        continue
                    return _JsonFetchResult(ok=False, payload=None, error="http_401", status=status_code, attempts=attempts)

                metadata.append(
                    build_api_request_metadata(
                        url=str(response.url),
                        method="GET",
                        status=status_code,
                        latency_ms=latency_ms,
                        retry_count=attempt,
                        token_refresh_attempted=self._auth_state.refresh_attempted,
                    ).as_dict()
                )

                if status_code < 400:
                    payload = await asyncio.wait_for(
                        response.json(),
                        timeout=max(resolved_read_timeout_ms / 1000.0, 0.001),
                    )
                    return _JsonFetchResult(ok=True, payload=payload, status=status_code, attempts=attempts)
                if status_code not in {408, 429, 500, 502, 503, 504}:
                    return _JsonFetchResult(ok=False, payload=None, error=f"http_{status_code}", status=status_code, attempts=attempts)
                last_error = RuntimeError(f"HTTP {status_code} from {endpoint}")
            except asyncio.TimeoutError:
                status_code = None
                latency_ms = int((time.perf_counter() - started) * 1000)
                timed_out_on_connect = latency_ms <= resolved_connect_timeout_ms
                timeout_class = "connect_timeout" if timed_out_on_connect else "read_timeout"
                metadata.append(
                    build_api_request_metadata(
                        url=url,
                        method="GET",
                        status=status_code,
                        latency_ms=latency_ms,
                        retry_count=attempt,
                        token_refresh_attempted=self._auth_state.refresh_attempted,
                        retry_reason=timeout_class,
                    ).as_dict()
                )
                last_error = RuntimeError(timeout_class)
            except PlaywrightError as exc:
                latency_ms = int((time.perf_counter() - started) * 1000)
                metadata.append(
                    build_api_request_metadata(
                        url=url,
                        method="GET",
                        status=status_code,
                        latency_ms=latency_ms,
                        retry_count=attempt,
                        token_refresh_attempted=self._auth_state.refresh_attempted,
                        retry_reason="network_timeout",
                    ).as_dict()
                )
                last_error = exc

            if attempt < resolved_max_retries:
                backoff = min(resolved_max_backoff_seconds, resolved_backoff_base_seconds * (2**attempt))
                jitter = random.uniform(0.0, max(self.config.backoff_jitter_seconds, 0.0))
                await asyncio.sleep(max(backoff + jitter, 0.0))

        if status_code is not None:
            return _JsonFetchResult(ok=False, payload=None, error=f"http_{status_code}", status=status_code, attempts=attempts)
        if last_error:
            message = str(last_error)
            if message in {"connect_timeout", "read_timeout"}:
                return _JsonFetchResult(ok=False, payload=None, error=message, status=None, attempts=attempts)
            return _JsonFetchResult(ok=False, payload=None, error=type(last_error).__name__, status=None, attempts=attempts)
        return _JsonFetchResult(ok=False, payload=None, error="unknown_error", status=None, attempts=attempts)

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

            return cached_discovery

        from_iframe = self._discover_token_from_iframe_url_query()
        if from_iframe.token:
            self._auth_state.token_discovery = from_iframe
            self._log_token_diagnostics(from_iframe)
            return from_iframe

        from_runtime_storage = await self._discover_token_from_runtime_storage()
        if from_runtime_storage.token:
            self._auth_state.token_discovery = from_runtime_storage
            self._log_token_diagnostics(from_runtime_storage)
            return from_runtime_storage

        from_storage_state = self._discover_token_from_storage_state()
        self._auth_state.token_discovery = from_storage_state
        self._log_token_diagnostics(from_storage_state)
        return from_storage_state

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
