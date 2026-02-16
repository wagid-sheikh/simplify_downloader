from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Mapping

from playwright.async_api import Page

from app.crm_downloader.uc_orders_sync.archive_api_extract import (
    _fetch_invoice_html_with_retries,
    _parse_invoice_order_details,
    _resolve_archive_bearer_token,
)
from app.dashboard_downloader.json_logger import JsonLogger, log_event

GST_API_URL = "https://store.ucleanlaundry.com/api/v1/stores/generateGST?franchise=UCLEAN"
BOOKING_SEARCH_URL_TEMPLATE = (
    "https://store.ucleanlaundry.com/api/v1/bookings/search"
    "?query={query}&sortQuery=&page=1&filterQuery=&type="
)
GST_REFERER = "https://store.ucleanlaundry.com/gst-report"
REQUEST_ACCEPT = "application/json, text/plain, */*"
REQUESTED_WITH = "XMLHttpRequest"
MAX_RETRIES = 3

GST_API_BASE_COLUMNS = [
    "store_code",
    "order_code",
    "pickup",
    "delivery",
    "customer_name",
    "customer_phone",
    "address",
    "payment_text",
    "instructions",
    "customer_source",
    "status",
    "status_date",
]


@dataclass
class GstApiExtract:
    base_rows: list[dict[str, Any]] = field(default_factory=list)
    order_detail_rows: list[dict[str, Any]] = field(default_factory=list)
    skipped_order_codes: list[str] = field(default_factory=list)
    skipped_order_counters: dict[str, int] = field(default_factory=dict)
    booking_lookup_hits: int = 0
    booking_lookup_misses: int = 0


def _record_skip(extract: GstApiExtract, *, order_code: str, reason: str) -> None:
    extract.skipped_order_codes.append(order_code)
    extract.skipped_order_counters[reason] = extract.skipped_order_counters.get(reason, 0) + 1


def _build_headers(*, bearer_token: str | None) -> dict[str, str]:
    headers = {
        "Accept": REQUEST_ACCEPT,
        "Referer": GST_REFERER,
        "X-Requested-With": REQUESTED_WITH,
        "Content-Type": "application/json",
    }
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    return headers


async def _request_json_with_retries(
    *,
    page: Page,
    method: str,
    url: str,
    headers: Mapping[str, str],
    data: Mapping[str, Any] | None,
) -> Mapping[str, Any] | list[Any] | None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if method.upper() == "POST":
                response = await page.request.post(url, headers=dict(headers), data=data, timeout=90_000)
            else:
                response = await page.request.get(url, headers=dict(headers), timeout=90_000)
            if response.status >= 400:
                if response.status >= 500 and attempt < MAX_RETRIES:
                    await asyncio.sleep(0.5 * attempt)
                    continue
                return None
            payload = await response.json()
            if isinstance(payload, (Mapping, list)):
                return payload
            return None
        except Exception:
            if attempt < MAX_RETRIES:
                await asyncio.sleep(0.5 * attempt)
    return None


async def _resolve_booking_id_for_order(
    *,
    page: Page,
    order_code: str,
    headers: Mapping[str, str],
) -> tuple[int | None, Mapping[str, Any] | None]:
    url = BOOKING_SEARCH_URL_TEMPLATE.format(query=order_code)
    payload = await _request_json_with_retries(
        page=page,
        method="GET",
        url=url,
        headers=headers,
        data=None,
    )
    candidates: list[Mapping[str, Any]] = []
    if isinstance(payload, list):
        candidates = [row for row in payload if isinstance(row, Mapping)]
    elif isinstance(payload, Mapping):
        data = payload.get("data")
        if isinstance(data, list):
            candidates = [row for row in data if isinstance(row, Mapping)]

    for row in candidates:
        booking_code = str(row.get("booking_code") or "").strip()
        if booking_code != order_code:
            continue
        booking_id = row.get("id")
        if isinstance(booking_id, int):
            return booking_id, row
        try:
            return int(str(booking_id)), row
        except Exception:
            return None, row
    return None, None


async def collect_gst_orders_via_api(
    *,
    page: Page,
    store_code: str,
    logger: JsonLogger,
    from_date: date,
    to_date: date,
) -> GstApiExtract:
    extract = GstApiExtract()
    token = await _resolve_archive_bearer_token(page=page, logger=logger, store_code=store_code)
    headers = _build_headers(bearer_token=token)

    payload = await _request_json_with_retries(
        page=page,
        method="POST",
        url=GST_API_URL,
        headers=headers,
        data={"from_date": from_date.isoformat(), "to_date": to_date.isoformat()},
    )
    if not isinstance(payload, Mapping):
        _record_skip(extract, order_code=f"store:{store_code}", reason="gst_api_failed")
        return extract

    rows = payload.get("data")
    if not isinstance(rows, list):
        _record_skip(extract, order_code=f"store:{store_code}", reason="gst_api_invalid_data")
        return extract

    seen_orders: set[str] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        order_code = str(row.get("order_number") or "").strip()
        if not order_code:
            _record_skip(extract, order_code="missing_order_code", reason="missing_order_code")
            continue
        if order_code in seen_orders:
            _record_skip(extract, order_code=order_code, reason="duplicate_order_code")
            continue
        seen_orders.add(order_code)

        base_row = {
            "store_code": store_code,
            "order_code": order_code,
            "pickup": None,
            "delivery": row.get("invoice_date"),
            "customer_name": row.get("name"),
            "customer_phone": row.get("customer_phone"),
            "address": row.get("address"),
            "payment_text": row.get("final_amount"),
            "instructions": None,
            "customer_source": None,
            "status": row.get("payment_status") or "Unknown",
            "status_date": row.get("invoice_date"),
        }

        booking_id, booking_row = await _resolve_booking_id_for_order(
            page=page,
            order_code=order_code,
            headers=headers,
        )
        if booking_id is None:
            extract.booking_lookup_misses += 1
            _record_skip(extract, order_code=order_code, reason="booking_lookup_miss")
            extract.base_rows.append(base_row)
            continue

        extract.booking_lookup_hits += 1
        if booking_row is not None:
            if booking_row.get("address"):
                base_row["address"] = booking_row.get("address")
            if booking_row.get("suggestions"):
                base_row["instructions"] = booking_row.get("suggestions")

        invoice_html = await _fetch_invoice_html_with_retries(
            page=page,
            booking_id=booking_id,
            store_code=store_code,
            order_code=order_code,
            logger=logger,
        )
        if not invoice_html:
            _record_skip(extract, order_code=order_code, reason="invoice_fetch_failed")
            extract.base_rows.append(base_row)
            continue

        extract.order_detail_rows.extend(
            _parse_invoice_order_details(
                invoice_html=invoice_html,
                store_code=store_code,
                order_code=order_code,
            )
        )
        extract.base_rows.append(base_row)

    log_event(
        logger=logger,
        phase="gst_api_extract",
        message="GST API experimental extraction complete",
        store_code=store_code,
        base_rows=len(extract.base_rows),
        order_detail_rows=len(extract.order_detail_rows),
        booking_lookup_hits=extract.booking_lookup_hits,
        booking_lookup_misses=extract.booking_lookup_misses,
        skipped_order_counters=extract.skipped_order_counters,
    )
    return extract
