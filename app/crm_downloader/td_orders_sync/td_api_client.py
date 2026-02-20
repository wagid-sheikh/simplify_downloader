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

from app.crm_downloader.td_orders_sync.td_api_compare import build_api_request_metadata

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


@dataclass
class TdApiFetchResult:
    normalized_orders: list[dict[str, Any]] = field(default_factory=list)
    normalized_sales: list[dict[str, Any]] = field(default_factory=list)
    normalized_garments: list[dict[str, Any]] = field(default_factory=list)
    request_metadata: list[dict[str, Any]] = field(default_factory=list)


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

        order_payload = await self._get_json(
            endpoint="/reports/order-report",
            params={**common_params, "expandData": "false"},
            metadata=metadata,
        )
        sales_payload = await self._get_json(
            endpoint="/sales-and-deliveries/sales",
            params=common_params,
            metadata=metadata,
        )
        garments_payload = await self._get_json(
            endpoint="/garments/details",
            params=common_params,
            metadata=metadata,
        )

        return TdApiFetchResult(
            normalized_orders=_normalize_order_rows(_extract_rows(order_payload)),
            normalized_sales=_normalize_sales_rows(_extract_rows(sales_payload)),
            normalized_garments=_normalize_garment_rows(_extract_rows(garments_payload)),
            request_metadata=metadata,
        )

    async def _get_json(
        self,
        *,
        endpoint: str,
        params: Mapping[str, Any],
        metadata: list[dict[str, Any]],
    ) -> Any:
        url = f"{REPORTING_API_BASE_URL}{endpoint}"
        last_error: Exception | None = None
        status_code: int | None = None
        for attempt in range(self.config.max_retries + 1):
            await _StoreRateLimiter.wait_turn(self.store_code, self.config.min_interval_seconds)
            started = time.perf_counter()
            try:
                response = await self.context.request.get(
                    url,
                    params=params,
                    headers={
                        "accept": "*/*",
                        "origin": "https://reports.quickdrycleaning.com",
                        "referer": "https://reports.quickdrycleaning.com/",
                    },
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
                    ).as_dict()
                )
                if status_code < 400:
                    return await response.json()
                if status_code not in {408, 429, 500, 502, 503, 504}:
                    return {}
                last_error = RuntimeError(f"HTTP {status_code} from {endpoint}")
            except PlaywrightError as exc:
                latency_ms = int((time.perf_counter() - started) * 1000)
                metadata.append(
                    build_api_request_metadata(
                        url=url,
                        method="GET",
                        status=status_code,
                        latency_ms=latency_ms,
                        retry_count=attempt,
                    ).as_dict()
                )
                last_error = exc
            if attempt < self.config.max_retries:
                backoff = min(self.config.max_backoff_seconds, self.config.backoff_base_seconds * (2**attempt))
                await asyncio.sleep(max(backoff, 0.0))
        if last_error:
            raise last_error
        return {}


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
            "order_no": row.get("orderNo") or row.get("orderNumber") or row.get("order_no"),
            "order_id": row.get("orderId") or row.get("order_id"),
            "invoice_no": row.get("invoiceNo") or row.get("invoice_no"),
            "amount": row.get("amount") or row.get("netAmount") or row.get("net_amount"),
            "status": row.get("status") or row.get("orderStatus") or row.get("order_status"),
        }
        for row in rows
    ]


def _normalize_sales_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "order_no": row.get("orderNo") or row.get("orderNumber") or row.get("order_no"),
            "invoice_no": row.get("invoiceNo") or row.get("invoice_no"),
            "amount": row.get("total") or row.get("amount") or row.get("netAmount"),
            "status": row.get("status") or row.get("deliveryStatus"),
        }
        for row in rows
    ]


def _normalize_garment_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "order_no": row.get("orderNo") or row.get("orderNumber"),
            "invoice_no": row.get("invoiceNo"),
            "amount": row.get("amount") or row.get("total"),
            "status": row.get("status") or row.get("stage"),
        }
        for row in rows
    ]

