from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Mapping

from playwright.async_api import BrowserContext, Error as PlaywrightError

from app.crm_downloader.td_orders_sync.td_api_compare import build_api_request_metadata, parse_token_expiry

REPORTING_API_BASE_URL = "https://reporting-api.quickdrycleaning.com"


@dataclass(frozen=True)
class TdApiClientConfig:
    timeout_ms: int = int(os.environ.get("TD_API_TIMEOUT_MS", "20000"))
    max_retries: int = int(os.environ.get("TD_API_MAX_RETRIES", "3"))
    backoff_base_seconds: float = float(os.environ.get("TD_API_BACKOFF_BASE_SECONDS", "0.5"))
    max_backoff_seconds: float = float(os.environ.get("TD_API_MAX_BACKOFF_SECONDS", "4.0"))
    min_interval_seconds: float = float(os.environ.get("TD_API_MIN_INTERVAL_SECONDS", "0.35"))
    page: int = int(os.environ.get("TD_API_PAGE", "1"))
    page_size: int = int(os.environ.get("TD_API_PAGE_SIZE", "500"))
    max_pages: int = int(os.environ.get("TD_API_MAX_PAGES", "100"))


@dataclass
class TdApiFetchResult:
    normalized_orders: list[dict[str, Any]] = field(default_factory=list)
    normalized_sales: list[dict[str, Any]] = field(default_factory=list)
    normalized_garments: list[dict[str, Any]] = field(default_factory=list)
    raw_orders: list[dict[str, Any]] = field(default_factory=list)
    raw_sales: list[dict[str, Any]] = field(default_factory=list)
    raw_garments: list[dict[str, Any]] = field(default_factory=list)
    request_metadata: list[dict[str, Any]] = field(default_factory=list)
    endpoint_errors: dict[str, str] = field(default_factory=dict)
    auth_diagnostics: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class _EndpointFetchResponse:
    ok: bool
    payload: Any | None
    error: str | None = None


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
        report_token: str | None = None,
        token_source: str | None = None,
    ) -> None:
        self.store_code = store_code.upper().strip()
        self.context = context
        self.storage_state_path = storage_state_path
        self.config = config or TdApiClientConfig()
        self._report_token = report_token
        self._token_source = token_source

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
            "page": self.config.page,
            "pageSize": self.config.page_size,
            "startDate": from_date.isoformat(),
            "endDate": to_date.isoformat(),
        }
        metadata: list[dict[str, Any]] = []
        errors: dict[str, str] = {}

        orders_rows = await self._fetch_endpoint_rows(
            endpoint="/reports/order-report",
            params={**common_params, "expandData": "false"},
            metadata=metadata,
            errors=errors,
        )
        sales_rows = await self._fetch_endpoint_rows(
            endpoint="/sales-and-deliveries/sales",
            params=common_params,
            metadata=metadata,
            errors=errors,
        )
        garments_rows = await self._fetch_endpoint_rows(
            endpoint="/garments/details",
            params=common_params,
            metadata=metadata,
            errors=errors,
        )

        return TdApiFetchResult(
            normalized_orders=_normalize_order_rows(orders_rows),
            normalized_sales=_normalize_sales_rows(sales_rows),
            normalized_garments=_normalize_garment_rows(garments_rows),
            raw_orders=orders_rows,
            raw_sales=sales_rows,
            raw_garments=garments_rows,
            request_metadata=metadata,
            endpoint_errors=errors,
            auth_diagnostics={
                "token_found": bool(self._report_token),
                "token_source": self._token_source,
                "token_expiry": parse_token_expiry(self._report_token),
            },
        )

    async def _fetch_endpoint_rows(
        self,
        *,
        endpoint: str,
        params: Mapping[str, Any],
        metadata: list[dict[str, Any]],
        errors: dict[str, str],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        base_page = int(params.get("page") or self.config.page)
        for page in range(base_page, base_page + max(self.config.max_pages, 1)):
            page_params = dict(params)
            page_params["page"] = page
            response = await self._get_json(endpoint=endpoint, params=page_params, metadata=metadata)
            if not response.ok:
                errors.setdefault(endpoint, response.error or "request_failed_or_timed_out")
                break
            page_rows = _extract_rows(response.payload)
            if not page_rows:
                break
            rows.extend(page_rows)
            if len(page_rows) < self.config.page_size:
                break
        return rows

    async def _get_json(
        self,
        *,
        endpoint: str,
        params: Mapping[str, Any],
        metadata: list[dict[str, Any]],
    ) -> _EndpointFetchResponse:
        url = f"{REPORTING_API_BASE_URL}{endpoint}"
        status_code: int | None = None
        for attempt in range(self.config.max_retries + 1):
            await _StoreRateLimiter.wait_turn(self.store_code, self.config.min_interval_seconds)
            started = time.perf_counter()
            try:
                headers = {
                    "accept": "*/*",
                    "origin": "https://reports.quickdrycleaning.com",
                    "referer": "https://reports.quickdrycleaning.com/",
                }
                if self._report_token:
                    headers["Authorization"] = f"Bearer {self._report_token}"
                response = await self.context.request.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=self.config.timeout_ms,
                )
                latency_ms = int((time.perf_counter() - started) * 1000)
                status_code = response.status
                metadata.append(
                    build_api_request_metadata(
                        url=str(response.url),
                        method="GET",
                        status=status_code,
                        latency_ms=latency_ms,
                        retry_count=attempt,
                        token_refresh_attempted=False,
                    ).as_dict()
                )
                if status_code < 400:
                    return _EndpointFetchResponse(ok=True, payload=await response.json())
                if status_code == 401 and self._report_token and attempt == 0:
                    # allow one immediate retry with same token; callers may provide refreshed token upstream
                    continue
                if status_code not in {408, 429, 500, 502, 503, 504}:
                    return _EndpointFetchResponse(ok=False, payload=None, error=f"http_{status_code}")
            except PlaywrightError:
                latency_ms = int((time.perf_counter() - started) * 1000)
                metadata.append(
                    build_api_request_metadata(
                        url=url,
                        method="GET",
                        status=status_code,
                        latency_ms=latency_ms,
                        retry_count=attempt,
                        token_refresh_attempted=False,
                    ).as_dict()
                )
            if attempt < self.config.max_retries:
                backoff = min(self.config.max_backoff_seconds, self.config.backoff_base_seconds * (2**attempt))
                await asyncio.sleep(max(backoff, 0.0))
        return _EndpointFetchResponse(ok=False, payload=None, error=(f"http_{status_code}" if status_code else "request_failed_or_timed_out"))


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


def _normalize_order_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "store_code": row.get("storeCode") or row.get("store_code"),
            "order_no": row.get("orderNo") or row.get("orderNumber") or row.get("order_no"),
            "order_id": row.get("orderId") or row.get("order_id"),
            "invoice_no": row.get("invoiceNo") or row.get("invoice_no"),
            "order_date": row.get("orderDate") or row.get("order_date"),
            "amount": row.get("amount") or row.get("netAmount") or row.get("net_amount"),
            "status": row.get("status") or row.get("orderStatus") or row.get("order_status"),
        }
        for row in rows
    ]


def _normalize_sales_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "store_code": row.get("storeCode") or row.get("store_code"),
            "order_no": row.get("orderNo") or row.get("orderNumber") or row.get("order_no"),
            "invoice_no": row.get("invoiceNo") or row.get("invoice_no"),
            "order_date": row.get("orderDate") or row.get("order_date"),
            "amount": row.get("total") or row.get("amount") or row.get("netAmount"),
            "status": row.get("status") or row.get("deliveryStatus"),
        }
        for row in rows
    ]


def _normalize_garment_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "order_no": row.get("orderNo") or row.get("orderNumber") or row.get("order_no"),
            "order_number": row.get("orderNo") or row.get("orderNumber") or row.get("order_no"),
            "api_order_id": row.get("orderId") or row.get("order_id"),
            "api_line_item_id": row.get("lineItemId") or row.get("line_item_id") or row.get("itemId"),
            "api_garment_id": row.get("garmentId") or row.get("garment_id"),
            "line_item_key": row.get("lineItemKey") or row.get("itemKey") or row.get("line_item_key"),
            "garment_name": row.get("garmentName") or row.get("garment") or row.get("itemName"),
            "service_name": row.get("serviceName") or row.get("service") or row.get("processName"),
            "quantity": row.get("quantity") or row.get("qty"),
            "amount": row.get("amount") or row.get("total") or row.get("lineAmount"),
            "status": row.get("status") or row.get("stage"),
            "updated_at": row.get("updatedAt") or row.get("updated_at"),
            "order_date": row.get("orderDate") or row.get("order_date"),
        }
        for row in rows
    ]
