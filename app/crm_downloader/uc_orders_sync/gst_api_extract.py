from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Mapping
from urllib.parse import urlencode

from playwright.async_api import Page

from app.crm_downloader.uc_orders_sync.archive_api_extract import (
    _extract_invoice_customer_address,
    InvoiceApiCallStats,
    TOKEN_KEY_PATTERN,
    _extract_order_info,
    _fetch_invoice_html_with_retries,
    _parse_invoice_order_details,
    _resolve_archive_bearer_token,
)
from app.dashboard_downloader.json_logger import JsonLogger, log_event

GST_API_URL = "https://store.ucleanlaundry.com/api/v1/stores/report/tax-report"
GST_API_PAGE_LIMIT = 20
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
    "service_names",
    "cloth_item_names",
    "cloth_quantity",
    "address",
    "customer_source",
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

GST_API_ORDER_DETAIL_SNAPSHOT_COLUMNS = [
    "store_code",
    "order_code",
    "snapshot_outcome",
    "detail_row_count",
]


@dataclass
class GstApiExtract:
    gst_rows: list[dict[str, Any]] = field(default_factory=list)
    base_rows: list[dict[str, Any]] = field(default_factory=list)
    order_detail_rows: list[dict[str, Any]] = field(default_factory=list)
    payment_detail_rows: list[dict[str, Any]] = field(default_factory=list)
    order_detail_snapshot_rows: list[dict[str, Any]] = field(default_factory=list)
    skipped_order_codes: list[str] = field(default_factory=list)
    skipped_order_counters: dict[str, int] = field(default_factory=dict)
    booking_lookup_hits: int = 0
    booking_lookup_misses: int = 0
    delivered_rows_scanned: int = 0
    delivered_rows_matched_gst: int = 0
    delivered_payment_rows_produced: int = 0
    gst_orders_without_payments: int = 0
    invoice_retry_count: int = 0
    source_fetch_status: str | None = None
    source_fetch_error_class: str | None = None
    source_fetch_failure_reason: str | None = None
    confirmed_empty: bool = False
    extractor_status: str | None = None


def _log_gst_api_extract_complete(
    *,
    logger: JsonLogger,
    store_code: str,
    extract: GstApiExtract,
) -> None:
    log_event(
        logger=logger,
        phase="gst_api_extract",
        message="GST API experimental extraction complete",
        store_code=store_code,
        gst_rows=len(extract.gst_rows),
        base_rows=len(extract.base_rows),
        order_detail_rows=len(extract.order_detail_rows),
        payment_detail_rows=len(extract.payment_detail_rows),
        order_detail_snapshot_rows=len(extract.order_detail_snapshot_rows),
        booking_lookup_hits=extract.booking_lookup_hits,
        booking_lookup_misses=extract.booking_lookup_misses,
        delivered_rows_scanned=extract.delivered_rows_scanned,
        delivered_rows_matched_gst=extract.delivered_rows_matched_gst,
        delivered_payment_rows_produced=extract.delivered_payment_rows_produced,
        gst_orders_without_payments=extract.gst_orders_without_payments,
        invoice_retry_count=extract.invoice_retry_count,
        skipped_order_counters=extract.skipped_order_counters,
        source_fetch_status=extract.source_fetch_status,
        source_fetch_error_class=extract.source_fetch_error_class,
        source_fetch_failure_reason=extract.source_fetch_failure_reason,
        confirmed_empty=extract.confirmed_empty,
        extractor_status=extract.extractor_status,
    )


def _log_gst_api_source_fetch_failure(
    *,
    logger: JsonLogger,
    store_code: str,
    extract: GstApiExtract,
    message: str,
) -> None:
    log_event(
        logger=logger,
        phase="gst_api_extract",
        status="warning",
        message=message,
        store_code=store_code,
        gst_rows=len(extract.gst_rows),
        source_fetch_status=extract.source_fetch_status,
        source_fetch_error_class=extract.source_fetch_error_class,
        source_fetch_failure_reason=extract.source_fetch_failure_reason,
        confirmed_empty=extract.confirmed_empty,
        extractor_status=extract.extractor_status,
        skipped_order_counters=extract.skipped_order_counters,
    )


def _record_skip(extract: GstApiExtract, *, order_code: str, reason: str) -> None:
    extract.skipped_order_codes.append(order_code)
    extract.skipped_order_counters[reason] = extract.skipped_order_counters.get(reason, 0) + 1


def _record_invoice_snapshot(
    extract: GstApiExtract,
    *,
    store_code: str,
    order_code: str,
    snapshot_outcome: str,
    detail_row_count: int,
) -> None:
    extract.order_detail_snapshot_rows.append(
        {
            "store_code": store_code,
            "order_code": order_code,
            "snapshot_outcome": snapshot_outcome,
            "detail_row_count": detail_row_count,
        }
    )


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


def detect_archive_bearer_token_in_storage_state(
    storage_state: Mapping[str, Any],
) -> str | None:
    """Return a UC archive bearer token from a Playwright storage-state artifact.

    The archive/GST APIs depend on a browser-local bearer token.  This helper is
    intentionally side-effect free so rebuild preflight can fail fast before it
    opens historical rebuild windows.
    """
    token_regexes = (
        re.compile(r"(?:^|\s)Bearer\s+([A-Za-z0-9\-_.~+/]+=*)", flags=re.I),
        re.compile(r"^([A-Za-z0-9\-_.]+\.[A-Za-z0-9\-_.]+\.[A-Za-z0-9\-_.]+)$"),
    )

    def parse_token(value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        trimmed = value.strip()
        if not trimmed:
            return None
        for pattern in token_regexes:
            match = pattern.match(trimmed)
            if match:
                return (match.group(1) or match.group(0)).strip()
        return None

    def deep_search(value: Any, depth: int = 0) -> str | None:
        if value is None or depth > 4:
            return None
        direct = parse_token(value)
        if direct:
            return direct
        if isinstance(value, str):
            try:
                parsed_value = json.loads(value)
            except Exception:
                parsed_value = None
            if parsed_value is not None:
                token = deep_search(parsed_value, depth + 1)
                if token:
                    return token
        if isinstance(value, Mapping):
            for key, nested_value in value.items():
                if isinstance(key, str) and TOKEN_KEY_PATTERN.search(key):
                    token = deep_search(nested_value, depth + 1)
                    if token:
                        return token
            for nested_value in value.values():
                token = deep_search(nested_value, depth + 1)
                if token:
                    return token
        if isinstance(value, list):
            for item in value:
                token = deep_search(item, depth + 1)
                if token:
                    return token
        return None

    origins = storage_state.get("origins")
    if not isinstance(origins, list):
        return None
    for origin in origins:
        if not isinstance(origin, Mapping):
            continue
        local_storage = origin.get("localStorage")
        if not isinstance(local_storage, list):
            continue
        for entry in local_storage:
            if not isinstance(entry, Mapping):
                continue
            key = entry.get("name")
            value = entry.get("value")
            if isinstance(key, str) and TOKEN_KEY_PATTERN.search(key):
                token = deep_search(value)
                if token:
                    return token
            token = deep_search(value)
            if token:
                return token
    return None


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
            if not order_code:
                continue
            if order_code in gst_order_codes:
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

            if appended_for_order and order_code in gst_order_codes:
                matched_orders_with_payments.add(order_code)

        page_number += 1

    extract.gst_orders_without_payments = len(gst_order_codes - matched_orders_with_payments)


async def _collect_tax_report_rows(
    *,
    page: Page,
    headers: Mapping[str, str],
    from_date: date,
    to_date: date,
    page_limit: int = GST_API_PAGE_LIMIT,
) -> tuple[str, list[Mapping[str, Any]] | None]:
    rows: list[Mapping[str, Any]] = []
    page_number = 1

    while True:
        query = urlencode(
            {
                "from": from_date.isoformat(),
                "to": to_date.isoformat(),
                "page": page_number,
                "limit": page_limit,
                "sortBy": "invoice_date",
                "sortOrder": "DESC",
                "export": "false",
                "franchise": "UCLEAN",
            }
        )
        payload = await _request_json_with_retries(
            page=page,
            method="GET",
            url=f"{GST_API_URL}?{query}",
            headers=headers,
            data=None,
        )
        if not isinstance(payload, Mapping):
            return "failed", None

        page_rows = payload.get("data")
        if not isinstance(page_rows, list):
            return "invalid_data", None
        if not page_rows:
            return "empty" if page_number == 1 and not rows else "success", rows

        rows.extend(row for row in page_rows if isinstance(row, Mapping))
        if len(page_rows) < page_limit:
            return "success", rows
        page_number += 1


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

    fetch_status, rows = await _collect_tax_report_rows(
        page=page,
        headers=headers,
        from_date=from_date,
        to_date=to_date,
    )
    if fetch_status == "invalid_data":
        extract.source_fetch_status = "failed"
        extract.source_fetch_error_class = "gst_api_invalid_data"
        extract.source_fetch_failure_reason = "GST API payload data field was not a list"
        extract.extractor_status = "failed"
        _record_skip(extract, order_code=f"store:{store_code}", reason="gst_api_invalid_data")
        _log_gst_api_source_fetch_failure(
            logger=logger,
            store_code=store_code,
            extract=extract,
            message="GST API source payload data was invalid",
        )
        _log_gst_api_extract_complete(logger=logger, store_code=store_code, extract=extract)
        return extract

    if fetch_status == "failed" or rows is None:
        extract.source_fetch_status = "failed"
        extract.source_fetch_error_class = "gst_api_failed"
        extract.source_fetch_failure_reason = "GST API request did not return a JSON mapping"
        extract.extractor_status = "failed"
        _record_skip(extract, order_code=f"store:{store_code}", reason="gst_api_failed")
        _log_gst_api_source_fetch_failure(
            logger=logger,
            store_code=store_code,
            extract=extract,
            message="GST API source fetch failed",
        )
        _log_gst_api_extract_complete(logger=logger, store_code=store_code, extract=extract)
        return extract

    if fetch_status == "empty":
        extract.source_fetch_status = "complete"
        extract.confirmed_empty = True
        extract.extractor_status = "success"
        log_event(
            logger=logger,
            phase="gst_api_extract",
            status="info",
            message="GST API source returned no rows",
            store_code=store_code,
            gst_rows=0,
            source_fetch_status=extract.source_fetch_status,
            confirmed_empty=extract.confirmed_empty,
            extractor_status=extract.extractor_status,
        )
        _log_gst_api_extract_complete(logger=logger, store_code=store_code, extract=extract)
        return extract

    extract.source_fetch_status = "success"

    gst_order_codes: set[str] = set()
    seen_orders: set[str] = set()
    invoice_call_stats = InvoiceApiCallStats()
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
            "service_names": row.get("service_names"),
            "cloth_item_names": row.get("cloth_item_names"),
            "cloth_quantity": row.get("cloth_quantity"),
            "address": None,
            "customer_source": None,
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
            "address": None,
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
            _record_invoice_snapshot(
                extract,
                store_code=store_code,
                order_code=order_code,
                snapshot_outcome="incomplete_or_failed",
                detail_row_count=0,
            )
            extract.base_rows.append(base_row)
            continue

        extract.booking_lookup_hits += 1
        if booking_row is not None:
            if booking_row.get("suggestions"):
                base_row["instructions"] = booking_row.get("suggestions")

        invoice_html, invoice_retries = await _fetch_invoice_html_with_retries(
            page=page,
            booking_id=booking_id,
            store_code=store_code,
            order_code=order_code,
            logger=logger,
            trace_invoice_success=False,
            invoice_call_stats=invoice_call_stats,
        )
        extract.invoice_retry_count += invoice_retries
        if not invoice_html:
            _record_skip(extract, order_code=order_code, reason="invoice_fetch_failed")
            _record_invoice_snapshot(
                extract,
                store_code=store_code,
                order_code=order_code,
                snapshot_outcome="incomplete_or_failed",
                detail_row_count=0,
            )
            # Keep the base row so aggregate order reporting still includes the order,
            # but skip order-detail extraction when invoice content is unavailable.
            extract.base_rows.append(base_row)
            continue

        try:
            _, order_mode, _, _, _ = _extract_order_info(invoice_html, order_code)
            if order_mode:
                base_row["customer_source"] = order_mode
                gst_row["customer_source"] = order_mode

            invoice_address = _extract_invoice_customer_address(invoice_html)
            if invoice_address:
                base_row["address"] = invoice_address
                gst_row["address"] = invoice_address

            detail_rows = _parse_invoice_order_details(
                invoice_html=invoice_html,
                store_code=store_code,
                order_code=order_code,
            )
        except Exception:
            _record_skip(extract, order_code=order_code, reason="invoice_parse_failed")
            _record_invoice_snapshot(
                extract,
                store_code=store_code,
                order_code=order_code,
                snapshot_outcome="incomplete_or_failed",
                detail_row_count=0,
            )
            extract.base_rows.append(base_row)
            continue

        extract.order_detail_rows.extend(detail_rows)
        _record_invoice_snapshot(
            extract,
            store_code=store_code,
            order_code=order_code,
            snapshot_outcome="complete_with_rows" if detail_rows else "complete_empty",
            detail_row_count=len(detail_rows),
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

    extract.extractor_status = "success"

    _log_gst_api_extract_complete(logger=logger, store_code=store_code, extract=extract)
    return extract
