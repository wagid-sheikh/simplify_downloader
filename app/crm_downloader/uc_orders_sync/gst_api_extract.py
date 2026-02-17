from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Mapping
from urllib.parse import urlencode

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
DELIVERED_ORDERS_URL = "https://store.ucleanlaundry.com/api/v1/bookings/getDeliveredOrders"
REQUEST_ACCEPT = "application/json, text/plain, */*"
REQUESTED_WITH = "XMLHttpRequest"
MAX_RETRIES = 3
DELIVERED_ORDERS_LIMIT = 30

PAYMENT_MODE_MAP = {
    1: "UPI",
    2: "Debit/Credit Card",
    3: "Bank Transfer",
    4: "Cash",
}

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

GST_API_GST_COLUMNS = [
    "store_code",
    "order_number",
    "invoice_number",
    "invoice_date",
    "name",
    "customer_phone",
    "customer_gst",
    "address",
    "store_address",
    "city_name",
    "taxable_value",
    "cgst",
    "sgst",
    "total_tax",
    "final_amount",
    "payment_status",
]

GST_API_PAYMENT_COLUMNS = [
    "store_code",
    "order_code",
    "payment_mode",
    "amount",
    "payment_date",
    "transaction_id",
]


@dataclass
class GstApiExtract:
    gst_rows: list[dict[str, Any]] = field(default_factory=list)
    base_rows: list[dict[str, Any]] = field(default_factory=list)
    order_detail_rows: list[dict[str, Any]] = field(default_factory=list)
    payment_detail_rows: list[dict[str, Any]] = field(default_factory=list)
    skipped_order_codes: list[str] = field(default_factory=list)
    skipped_order_counters: dict[str, int] = field(default_factory=dict)
    booking_lookup_hits: int = 0
    booking_lookup_misses: int = 0
    delivered_rows_scanned: int = 0
    delivered_rows_matched_gst: int = 0
    delivered_payment_rows_produced: int = 0
    gst_orders_without_payments: int = 0


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



def _build_payment_rows_from_booking(*, store_code: str, order_code: str, booking_row: Mapping[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    payment_details = booking_row.get("payment_details")
    if payment_details in (None, "", "null"):
        return [], ["payment_details_missing"]

    try:
        parsed_payload = json.loads(str(payment_details))
    except Exception:
        return [], ["payment_details_unparseable"]

    if not isinstance(parsed_payload, list):
        return [], ["payment_details_not_list"]

    rows: list[dict[str, Any]] = []
    reason_codes: list[str] = []
    for payment in parsed_payload:
        if not isinstance(payment, Mapping):
            reason_codes.append("payment_row_not_mapping")
            continue

        raw_mode = payment.get("payment_mode")
        mode_name = "UNKNOWN"
        try:
            mode_name = PAYMENT_MODE_MAP.get(int(raw_mode), "UNKNOWN")
            if mode_name == "UNKNOWN":
                reason_codes.append("payment_mode_unmapped")
        except (ValueError, TypeError):
            mode_name = "UNKNOWN"
            reason_codes.append("payment_mode_unmapped")

        amount = payment.get("payment_amount")
        payment_date = payment.get("created_at")
        if amount in (None, ""):
            reason_codes.append("payment_amount_missing")
        if payment_date in (None, ""):
            reason_codes.append("payment_date_missing")

        if amount in (None, "") or payment_date in (None, ""):
            reason_codes.append("payment_row_dropped_missing_core_fields")
            continue

        rows.append(
            {
                "store_code": store_code,
                "order_code": order_code,
                "payment_mode": mode_name,
                "amount": amount,
                "payment_date": payment_date,
                "transaction_id": payment.get("transaction_id") or payment.get("txn_id"),
            }
        )

    if not rows and parsed_payload:
        reason_codes.append("payment_rows_all_dropped")
    return rows, sorted(set(reason_codes))


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


async def _collect_gst_payment_rows_from_delivered_orders(
    *,
    page: Page,
    store_code: str,
    headers: Mapping[str, str],
    from_date: date,
    to_date: date,
    gst_order_codes: set[str],
    extract: GstApiExtract,
) -> None:
    page_number = 1
    seen_payment_keys: set[tuple[Any, ...]] = set()
    matched_orders_with_payments: set[str] = set()

    while True:
        query = urlencode(
            {
                "franchise": "UCLEAN",
                "page": page_number,
                "limit": DELIVERED_ORDERS_LIMIT,
                "dateRange": "custom",
                "startDate": from_date.isoformat(),
                "endDate": to_date.isoformat(),
                "dateType": "delivery",
            }
        )
        payload = await _request_json_with_retries(
            page=page,
            method="GET",
            url=f"{DELIVERED_ORDERS_URL}?{query}",
            headers=headers,
            data=None,
        )
        if not isinstance(payload, Mapping):
            _record_skip(extract, order_code=f"page:{page_number}", reason="delivered_orders_page_failed")
            break

        data = payload.get("data")
        if not isinstance(data, list):
            _record_skip(extract, order_code=f"page:{page_number}", reason="delivered_orders_invalid_data")
            break
        if not data:
            break

        for booking in data:
            if not isinstance(booking, Mapping):
                continue
            extract.delivered_rows_scanned += 1
            order_code = str(booking.get("booking_code") or "").strip()
            if not order_code or order_code not in gst_order_codes:
                continue

            extract.delivered_rows_matched_gst += 1
            payment_rows, payment_reason_codes = _build_payment_rows_from_booking(
                store_code=store_code,
                order_code=order_code,
                booking_row=booking,
            )
            for reason in payment_reason_codes:
                _record_skip(extract, order_code=order_code, reason=reason)

            appended_for_order = False
            for row in payment_rows:
                row_key = (
                    row.get("store_code"),
                    row.get("order_code"),
                    row.get("payment_mode"),
                    row.get("amount"),
                    row.get("payment_date"),
                    row.get("transaction_id"),
                )
                if row_key in seen_payment_keys:
                    continue
                seen_payment_keys.add(row_key)
                extract.payment_detail_rows.append(row)
                extract.delivered_payment_rows_produced += 1
                appended_for_order = True

            if appended_for_order:
                matched_orders_with_payments.add(order_code)

        page_number += 1

    extract.gst_orders_without_payments = len(gst_order_codes - matched_orders_with_payments)


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

    gst_order_codes: set[str] = set()
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
        gst_order_codes.add(order_code)

        gst_row = {
            "store_code": store_code,
            "order_number": order_code,
            "invoice_number": row.get("invoice_number"),
            "invoice_date": row.get("invoice_date"),
            "name": row.get("name"),
            "customer_phone": row.get("customer_phone"),
            "customer_gst": row.get("customer_gst"),
            "address": row.get("address"),
            "store_address": row.get("store_address"),
            "city_name": row.get("city_name"),
            "taxable_value": row.get("taxable_value"),
            "cgst": row.get("cgst"),
            "sgst": row.get("sgst"),
            "total_tax": row.get("total_tax"),
            "final_amount": row.get("final_amount"),
            "payment_status": row.get("payment_status"),
        }
        extract.gst_rows.append(gst_row)

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
            # Keep the base row so aggregate order reporting still includes the order,
            # but skip order-detail extraction when invoice content is unavailable.
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

    await _collect_gst_payment_rows_from_delivered_orders(
        page=page,
        store_code=store_code,
        headers=headers,
        from_date=from_date,
        to_date=to_date,
        gst_order_codes=gst_order_codes,
        extract=extract,
    )

    log_event(
        logger=logger,
        phase="gst_api_extract",
        message="GST API experimental extraction complete",
        store_code=store_code,
        gst_rows=len(extract.gst_rows),
        base_rows=len(extract.base_rows),
        order_detail_rows=len(extract.order_detail_rows),
        payment_detail_rows=len(extract.payment_detail_rows),
        booking_lookup_hits=extract.booking_lookup_hits,
        booking_lookup_misses=extract.booking_lookup_misses,
        delivered_rows_scanned=extract.delivered_rows_scanned,
        delivered_rows_matched_gst=extract.delivered_rows_matched_gst,
        delivered_payment_rows_produced=extract.delivered_payment_rows_produced,
        gst_orders_without_payments=extract.gst_orders_without_payments,
        skipped_order_counters=extract.skipped_order_counters,
    )
    return extract
