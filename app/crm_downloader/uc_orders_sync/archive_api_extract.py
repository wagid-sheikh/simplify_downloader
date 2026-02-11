from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass, field
from datetime import date
from html import unescape
from typing import Any, Mapping
from urllib.parse import urlencode

from playwright.async_api import Page

from app.dashboard_downloader.json_logger import JsonLogger, log_event

ARCHIVE_API_BASE_URL = "https://store.ucleanlaundry.com/api/v1/bookings/getDeliveredOrders"
ARCHIVE_INVOICE_URL_TEMPLATE = "https://store.ucleanlaundry.com/api/v1/bookings/generateInvoice/{booking_id}?franchise=UCLEAN"
ARCHIVE_API_LIMIT = 30
ARCHIVE_API_MAX_RETRIES = 3
ARCHIVE_API_RETRY_BASE_SECONDS = 1.0

PAYMENT_MODE_MAP = {
    1: "UPI",
    2: "Debit/Credit Card",
    3: "Bank Transfer",
    4: "Cash",
}

STATUS_MAP = {
    0: "Cancelled",
    7: "Delivered",
}


@dataclass
class ArchiveApiExtract:
    base_rows: list[dict[str, Any]] = field(default_factory=list)
    order_detail_rows: list[dict[str, Any]] = field(default_factory=list)
    payment_detail_rows: list[dict[str, Any]] = field(default_factory=list)
    skipped_order_codes: list[str] = field(default_factory=list)
    skipped_order_counters: dict[str, int] = field(default_factory=dict)
    page_count: int = 0
    api_total: int | None = None
    total_pages: int | None = None


def _record_skip(extract: ArchiveApiExtract, *, order_code: str, reason: str) -> None:
    extract.skipped_order_codes.append(order_code)
    extract.skipped_order_counters[reason] = extract.skipped_order_counters.get(reason, 0) + 1


def _strip_tags(text: str) -> str:
    cleaned = re.sub(r"<[^>]+>", " ", text)
    cleaned = unescape(cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _parse_cell_list(cell_html: str) -> list[str]:
    li_values = [_strip_tags(match) for match in re.findall(r"<li[^>]*>(.*?)</li>", cell_html, flags=re.I | re.S)]
    li_values = [value for value in li_values if value and value != "-"]
    if li_values:
        return li_values

    div_values = [_strip_tags(match) for match in re.findall(r"<div[^>]*>(.*?)</div>", cell_html, flags=re.I | re.S)]
    div_values = [value for value in div_values if value and value != "-"]
    if div_values:
        return div_values

    text = _strip_tags(cell_html)
    if not text or text == "-":
        return []
    return [text]


def _extract_order_info(invoice_html: str, default_order_code: str) -> tuple[str, str | None, str | None, str | None, str | None]:
    order_code = default_order_code
    order_mode: str | None = None
    order_datetime: str | None = None
    pickup_datetime: str | None = None
    delivery_datetime: str | None = None

    order_no_match = re.search(
        r"Order\s*No\.\s*-\s*([A-Za-z0-9-]+)\s*(?:<span[^>]*class=\"order-mode\"[^>]*>\(([^)]+)\)</span>)?",
        invoice_html,
        flags=re.I | re.S,
    )
    if order_no_match:
        order_code = (order_no_match.group(1) or default_order_code).strip()
        if order_no_match.group(2):
            order_mode = order_no_match.group(2).strip()

    dt_match = re.search(r"<div[^>]*font-size:\s*13px[^>]*>(.*?)</div>", invoice_html, flags=re.I | re.S)
    if dt_match:
        order_datetime = _strip_tags(dt_match.group(1)) or None

    pickup_match = re.search(
        r"Pickup\s*Done\s*Date\s*&amp;\s*Time\s*</div>\s*<div[^>]*class=\"order-info-value\"[^>]*>(.*?)</div>",
        invoice_html,
        flags=re.I | re.S,
    )
    if pickup_match:
        pickup_datetime = _strip_tags(pickup_match.group(1)) or None

    delivery_match = re.search(
        r"Delivery\s*Date\s*&amp;\s*Time\s*</div>\s*<div[^>]*class=\"order-info-value\"[^>]*>(.*?)</div>",
        invoice_html,
        flags=re.I | re.S,
    )
    if delivery_match:
        delivery_datetime = _strip_tags(delivery_match.group(1)) or None

    return order_code, order_mode, order_datetime, pickup_datetime, delivery_datetime


def _parse_invoice_order_details(*, invoice_html: str, store_code: str, order_code: str) -> list[dict[str, Any]]:
    resolved_order_code, order_mode, order_datetime, pickup_datetime, delivery_datetime = _extract_order_info(invoice_html, order_code)
    tbody_match = re.search(r"<tbody[^>]*>(.*?)</tbody>", invoice_html, flags=re.I | re.S)
    if not tbody_match:
        return []

    tbody_html = tbody_match.group(1)
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", tbody_html, flags=re.I | re.S)
    details: list[dict[str, Any]] = []
    for row_html in rows:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row_html, flags=re.I | re.S)
        if len(cells) < 9:
            continue

        service = _strip_tags(cells[1]) or None
        hsn_sac = _strip_tags(cells[2]) or None
        item_names = _parse_cell_list(cells[3])
        rates = _parse_cell_list(cells[4])
        quantities = _parse_cell_list(cells[5])
        weights = _parse_cell_list(cells[6])
        addons = _parse_cell_list(cells[7])
        amounts = _parse_cell_list(cells[8])

        max_items = max(1, len(item_names), len(rates), len(quantities), len(weights), len(addons), len(amounts))
        for idx in range(max_items):
            details.append(
                {
                    "store_code": store_code,
                    "order_code": resolved_order_code,
                    "order_mode": order_mode,
                    "order_datetime": order_datetime,
                    "pickup_datetime": pickup_datetime,
                    "delivery_datetime": delivery_datetime,
                    "service": service,
                    "hsn_sac": hsn_sac,
                    "item_name": item_names[idx] if idx < len(item_names) else None,
                    "rate": rates[idx] if idx < len(rates) else None,
                    "quantity": quantities[idx] if idx < len(quantities) else None,
                    "weight": weights[idx] if idx < len(weights) else None,
                    "addons": addons[idx] if idx < len(addons) else None,
                    "amount": amounts[idx] if idx < len(amounts) else None,
                }
            )
    return details


def _normalize_date_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text == "0000-00-00":
        return None
    return text


def _map_status(raw_status: Any) -> str:
    try:
        as_int = int(raw_status)
    except (ValueError, TypeError):
        return "Unknown"
    return STATUS_MAP.get(as_int, "Unknown")


def _parse_payment_rows(*, store_code: str, order_code: str, payment_details: Any, logger: JsonLogger) -> list[dict[str, Any]]:
    if payment_details in (None, "", "null"):
        return []

    parsed: list[Mapping[str, Any]]
    try:
        parsed_payload = json.loads(str(payment_details))
        parsed = parsed_payload if isinstance(parsed_payload, list) else []
    except Exception:
        log_event(
            logger=logger,
            phase="archive_api",
            status="warn",
            message="Failed to parse payment_details payload",
            store_code=store_code,
            order_code=order_code,
            payment_details=payment_details,
        )
        return []

    rows: list[dict[str, Any]] = []
    for payment in parsed:
        raw_mode = payment.get("payment_mode")
        mode_name = "UNKNOWN"
        try:
            mode_name = PAYMENT_MODE_MAP.get(int(raw_mode), "UNKNOWN")
        except (ValueError, TypeError):
            mode_name = "UNKNOWN"

        rows.append(
            {
                "store_code": store_code,
                "order_code": order_code,
                "payment_mode": mode_name,
                "amount": payment.get("payment_amount"),
                "payment_date": payment.get("created_at"),
                "transaction_id": None,
            }
        )
    return rows


async def _api_get_json_with_retries(
    *,
    page: Page,
    url: str,
    logger: JsonLogger,
    store_code: str,
    context: str,
) -> Mapping[str, Any] | None:
    last_error: str | None = None
    for attempt in range(1, ARCHIVE_API_MAX_RETRIES + 1):
        try:
            response = await page.request.get(url, timeout=90_000)
            status = response.status
            if status >= 500 or status == 429:
                last_error = f"http_status_{status}"
            elif status >= 400:
                log_event(
                    logger=logger,
                    phase="archive_api",
                    status="warn",
                    message="Archive API request failed",
                    store_code=store_code,
                    context=context,
                    attempt=attempt,
                    status_code=status,
                    url=url,
                )
                return None
            else:
                payload = await response.json()
                return payload if isinstance(payload, Mapping) else None
        except Exception as exc:
            last_error = str(exc)

        if attempt < ARCHIVE_API_MAX_RETRIES:
            await asyncio.sleep(ARCHIVE_API_RETRY_BASE_SECONDS * attempt)

    log_event(
        logger=logger,
        phase="archive_api",
        status="warn",
        message="Archive API request retries exhausted",
        store_code=store_code,
        context=context,
        error=last_error,
        url=url,
    )
    return None


async def _fetch_invoice_html_with_retries(*, page: Page, booking_id: int | str, store_code: str, order_code: str, logger: JsonLogger) -> str | None:
    url = ARCHIVE_INVOICE_URL_TEMPLATE.format(booking_id=booking_id)
    last_error: str | None = None
    for attempt in range(1, ARCHIVE_API_MAX_RETRIES + 1):
        try:
            response = await page.request.get(url, timeout=90_000)
            status = response.status
            if status >= 500 or status == 429:
                last_error = f"http_status_{status}"
            elif status >= 400:
                log_event(
                    logger=logger,
                    phase="archive_api",
                    status="warn",
                    message="Invoice API request failed",
                    store_code=store_code,
                    order_code=order_code,
                    booking_id=booking_id,
                    attempt=attempt,
                    status_code=status,
                    url=url,
                )
                return None
            else:
                return await response.text()
        except Exception as exc:
            last_error = str(exc)

        if attempt < ARCHIVE_API_MAX_RETRIES:
            await asyncio.sleep(ARCHIVE_API_RETRY_BASE_SECONDS * attempt)

    log_event(
        logger=logger,
        phase="archive_api",
        status="warn",
        message="Invoice API retries exhausted",
        store_code=store_code,
        order_code=order_code,
        booking_id=booking_id,
        error=last_error,
        url=url,
    )
    return None


async def collect_archive_orders_via_api(
    *,
    page: Page,
    store_code: str,
    logger: JsonLogger,
    from_date: date,
    to_date: date,
) -> ArchiveApiExtract:
    extract = ArchiveApiExtract()
    seen_orders: set[str] = set()

    page_number = 1
    while True:
        query = urlencode(
            {
                "franchise": "UCLEAN",
                "page": page_number,
                "limit": ARCHIVE_API_LIMIT,
                "dateRange": "custom",
                "startDate": from_date.isoformat(),
                "endDate": to_date.isoformat(),
                "dateType": "delivery",
            }
        )
        url = f"{ARCHIVE_API_BASE_URL}?{query}"
        payload = await _api_get_json_with_retries(
            page=page,
            url=url,
            logger=logger,
            store_code=store_code,
            context="getDeliveredOrders",
        )
        if payload is None:
            _record_skip(extract, order_code=f"page:{page_number}", reason="archive_api_page_failed")
            break

        data = payload.get("data")
        pagination = payload.get("pagination") if isinstance(payload.get("pagination"), Mapping) else {}
        if not isinstance(data, list):
            _record_skip(extract, order_code=f"page:{page_number}", reason="archive_api_invalid_data")
            break

        extract.page_count += 1
        total = pagination.get("total")
        total_pages = pagination.get("totalPages")
        if isinstance(total, int):
            extract.api_total = total
        if isinstance(total_pages, int):
            extract.total_pages = total_pages

        log_event(
            logger=logger,
            phase="archive_api",
            message="Archive API page fetched",
            store_code=store_code,
            page=page_number,
            rows_fetched=len(data),
            api_total=extract.api_total,
            total_pages=extract.total_pages,
            url=url,
        )

        if not data:
            break

        for booking in data:
            if not isinstance(booking, Mapping):
                continue
            order_code = str(booking.get("booking_code") or "").strip()
            booking_id = booking.get("id")
            if not order_code:
                _record_skip(extract, order_code=f"page:{page_number}", reason="missing_booking_code")
                continue
            if order_code in seen_orders:
                _record_skip(extract, order_code=order_code, reason="duplicate_order_code")
                continue
            seen_orders.add(order_code)

            status_text = _map_status(booking.get("status"))
            status_date = booking.get("delivered_at") if status_text == "Delivered" else booking.get("cancel_at")
            if not status_date:
                status_date = booking.get("updated_at")

            extract.base_rows.append(
                {
                    "store_code": store_code,
                    "order_code": order_code,
                    "pickup": _normalize_date_text(booking.get("pickupDate")),
                    "delivery": _normalize_date_text(booking.get("dropDate")),
                    "customer_name": booking.get("name"),
                    "customer_phone": booking.get("mobile"),
                    "address": booking.get("address"),
                    "payment_text": booking.get("final_amount"),
                    "instructions": booking.get("suggestion"),
                    "customer_source": None,
                    "status": status_text,
                    "status_date": _normalize_date_text(status_date),
                }
            )

            extract.payment_detail_rows.extend(
                _parse_payment_rows(
                    store_code=store_code,
                    order_code=order_code,
                    payment_details=booking.get("payment_details"),
                    logger=logger,
                )
            )

            if booking_id in (None, ""):
                _record_skip(extract, order_code=order_code, reason="missing_booking_id_for_invoice")
                continue

            invoice_html = await _fetch_invoice_html_with_retries(
                page=page,
                booking_id=booking_id,
                store_code=store_code,
                order_code=order_code,
                logger=logger,
            )
            if not invoice_html:
                _record_skip(extract, order_code=order_code, reason="invoice_fetch_failed")
                continue
            detail_rows = _parse_invoice_order_details(
                invoice_html=invoice_html,
                store_code=store_code,
                order_code=order_code,
            )
            if not detail_rows:
                _record_skip(extract, order_code=order_code, reason="invoice_parse_no_rows")
                continue
            extract.order_detail_rows.extend(detail_rows)

        stop_by_total_pages = isinstance(extract.total_pages, int) and page_number >= extract.total_pages
        if stop_by_total_pages:
            break

        page_number += 1

    if extract.api_total is not None and len(extract.base_rows) != extract.api_total:
        log_event(
            logger=logger,
            phase="archive_api",
            status="warn",
            message="Archive API total mismatch detected",
            store_code=store_code,
            api_total=extract.api_total,
            extracted_base_rows=len(extract.base_rows),
            difference=extract.api_total - len(extract.base_rows),
        )

    return extract
