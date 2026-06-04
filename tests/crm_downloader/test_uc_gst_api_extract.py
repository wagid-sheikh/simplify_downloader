from __future__ import annotations

import asyncio
import json
from datetime import date
from io import StringIO

from app.crm_downloader.uc_orders_sync import gst_api_extract
from app.crm_downloader.uc_orders_sync.gst_api_extract import collect_gst_orders_via_api
from app.dashboard_downloader.json_logger import JsonLogger, get_logger


class _FakePage:
    pass


def _memory_logger() -> tuple[JsonLogger, StringIO]:
    stream = StringIO()
    return JsonLogger(run_id="test_uc_gst_api_extract", stream=stream, log_file_path=None), stream


def _logged_events(stream: StringIO) -> list[dict[str, object]]:
    return [json.loads(line) for line in stream.getvalue().splitlines() if line.strip()]


def test_collect_gst_orders_via_api_keeps_base_row_when_invoice_fetch_fails(monkeypatch) -> None:
    async def _fake_resolve_archive_bearer_token(*, page, logger, store_code):
        return "token"

    async def _fake_request_json_with_retries(*, page, method, url, headers, data):
        if method == "POST":
            return {
                "data": [
                    {
                        "order_number": "UC610-0001",
                        "invoice_number": "INV-001",
                        "invoice_date": "2026-01-01",
                        "name": "Test User",
                        "customer_phone": "9999999999",
                        "address": "GST Address",
                        "store_address": "Store Address",
                        "city_name": "Pune",
                        "taxable_value": 100,
                        "cgst": 9,
                        "sgst": 9,
                        "total_tax": 18,
                        "final_amount": 118,
                        "payment_status": "Paid",
                    }
                ]
            }
        return {"data": []}

    async def _fake_resolve_booking_id_for_order(*, page, order_code, headers):
        return 101, {
            "booking_code": order_code,
            "id": 101,
            "payment_status": "pending",
            "address": "Booking Address",
            "suggestions": "Handle carefully",
        }

    async def _fake_fetch_invoice_html_with_retries(*, page, booking_id, store_code, order_code, logger, **_kwargs):
        return None, 0

    monkeypatch.setattr(
        gst_api_extract,
        "_resolve_archive_bearer_token",
        _fake_resolve_archive_bearer_token,
    )
    monkeypatch.setattr(
        gst_api_extract,
        "_request_json_with_retries",
        _fake_request_json_with_retries,
    )
    monkeypatch.setattr(
        gst_api_extract,
        "_resolve_booking_id_for_order",
        _fake_resolve_booking_id_for_order,
    )
    monkeypatch.setattr(
        gst_api_extract,
        "_fetch_invoice_html_with_retries",
        _fake_fetch_invoice_html_with_retries,
    )

    extract = asyncio.run(
        collect_gst_orders_via_api(
            page=_FakePage(),
            store_code="UC610",
            logger=get_logger("test_uc_gst_api_extract"),
            from_date=date(2026, 1, 1),
            to_date=date(2026, 1, 31),
        )
    )

    assert len(extract.gst_rows) == 1
    assert len(extract.base_rows) == 1
    assert len(extract.order_detail_rows) == 0
    assert extract.base_rows[0]["order_code"] == "UC610-0001"
    assert extract.base_rows[0]["address"] is None
    assert extract.base_rows[0]["instructions"] == "Handle carefully"
    assert extract.skipped_order_counters["invoice_fetch_failed"] == 1
    assert extract.order_detail_snapshot_rows == [
        {
            "store_code": "UC610",
            "order_code": "UC610-0001",
            "snapshot_outcome": "incomplete_or_failed",
            "detail_row_count": 0,
        }
    ]
    assert extract.invoice_retry_count == 0


def test_collect_gst_orders_via_api_builds_payment_rows_from_payment_details(monkeypatch) -> None:
    async def _fake_resolve_archive_bearer_token(*, page, logger, store_code):
        return "token"

    async def _fake_request_json_with_retries(*, page, method, url, headers, data):
        if method == "POST":
            return {
                "data": [
                    {
                        "order_number": "UC610-0002",
                        "invoice_number": "INV-002",
                        "invoice_date": "2026-01-02",
                        "name": "Test User 2",
                        "customer_phone": "9999999998",
                        "address": "GST Address 2",
                        "store_address": "Store Address",
                        "city_name": "Pune",
                        "taxable_value": 200,
                        "cgst": 18,
                        "sgst": 18,
                        "total_tax": 36,
                        "final_amount": 236,
                        "payment_status": "Paid",
                    }
                ]
            }
        if "page=1" in url:
            return {
                "data": [
                    {
                        "booking_code": "UC610-0002",
                        "payment_details": "[{\"payment_mode\": 1, \"payment_amount\": 100, \"created_at\": \"2026-01-02 10:00:00\", \"transaction_id\": \"TXN-1\"}, {\"payment_mode\": \"99\", \"payment_amount\": 136, \"created_at\": \"2026-01-02 10:05:00\"}]",
                    }
                ]
            }
        return {"data": []}

    async def _fake_resolve_booking_id_for_order(*, page, order_code, headers):
        return 102, {
            "booking_code": order_code,
            "id": 102,
            "payment_status": "paid",
            "payment_details": '[{"payment_mode": 1, "payment_amount": 100, "created_at": "2026-01-02 10:00:00", "transaction_id": "TXN-1"}, {"payment_mode": "99", "payment_amount": 136, "created_at": "2026-01-02 10:05:00"}]',
        }

    async def _fake_fetch_invoice_html_with_retries(*, page, booking_id, store_code, order_code, logger, **_kwargs):
        return "<html></html>", 0

    monkeypatch.setattr(gst_api_extract, "_resolve_archive_bearer_token", _fake_resolve_archive_bearer_token)
    monkeypatch.setattr(gst_api_extract, "_request_json_with_retries", _fake_request_json_with_retries)
    monkeypatch.setattr(gst_api_extract, "_resolve_booking_id_for_order", _fake_resolve_booking_id_for_order)
    monkeypatch.setattr(gst_api_extract, "_fetch_invoice_html_with_retries", _fake_fetch_invoice_html_with_retries)
    monkeypatch.setattr(gst_api_extract, "_parse_invoice_order_details", lambda **kwargs: [])

    extract = asyncio.run(
        collect_gst_orders_via_api(
            page=_FakePage(),
            store_code="UC610",
            logger=get_logger("test_uc_gst_api_extract"),
            from_date=date(2026, 1, 1),
            to_date=date(2026, 1, 31),
        )
    )

    assert len(extract.payment_detail_rows) == 2
    assert extract.payment_detail_rows[0]["payment_mode"] == "UPI"
    assert extract.payment_detail_rows[0]["amount"] == 100
    assert extract.payment_detail_rows[1]["payment_mode"] == "UNKNOWN"
    assert extract.payment_detail_rows[1]["amount"] == 136
    assert extract.skipped_order_counters["payment_mode_unmapped"] == 1
    assert extract.order_detail_snapshot_rows[0]["snapshot_outcome"] == "complete_empty"
    assert extract.invoice_retry_count == 0


def test_collect_gst_orders_via_api_keeps_gst_and_base_rows_when_booking_lookup_misses(monkeypatch) -> None:
    async def _fake_resolve_archive_bearer_token(*, page, logger, store_code):
        return "token"

    async def _fake_request_json_with_retries(*, page, method, url, headers, data):
        if method == "POST":
            return {
                "data": [
                    {
                        "order_number": "UC610-0003",
                        "invoice_number": "INV-003",
                        "invoice_date": "2026-01-03",
                        "name": "Test User 3",
                        "customer_phone": "9999999997",
                        "address": "GST Address 3",
                        "store_address": "Store Address",
                        "city_name": "Pune",
                        "taxable_value": 300,
                        "cgst": 27,
                        "sgst": 27,
                        "total_tax": 54,
                        "final_amount": 354,
                        "payment_status": "Paid",
                    }
                ]
            }
        return {"data": []}

    async def _fake_resolve_booking_id_for_order(*, page, order_code, headers):
        return None, None

    monkeypatch.setattr(gst_api_extract, "_resolve_archive_bearer_token", _fake_resolve_archive_bearer_token)
    monkeypatch.setattr(gst_api_extract, "_request_json_with_retries", _fake_request_json_with_retries)
    monkeypatch.setattr(gst_api_extract, "_resolve_booking_id_for_order", _fake_resolve_booking_id_for_order)

    extract = asyncio.run(
        collect_gst_orders_via_api(
            page=_FakePage(),
            store_code="UC610",
            logger=get_logger("test_uc_gst_api_extract"),
            from_date=date(2026, 1, 1),
            to_date=date(2026, 1, 31),
        )
    )

    assert len(extract.gst_rows) == 1
    assert len(extract.base_rows) == 1
    assert extract.booking_lookup_misses == 1
    assert extract.skipped_order_counters["booking_lookup_miss"] == 1
    assert extract.order_detail_snapshot_rows[0]["snapshot_outcome"] == "incomplete_or_failed"


def test_collect_gst_orders_via_api_collects_disjoint_delivered_payment_rows(monkeypatch) -> None:
    async def _fake_resolve_archive_bearer_token(*, page, logger, store_code):
        return "token"

    async def _fake_request_json_with_retries(*, page, method, url, headers, data):
        if method == "POST":
            return {
                "data": [
                    {
                        "order_number": "UC610-GST-1",
                        "invoice_number": "INV-GST-1",
                        "invoice_date": "2026-01-04",
                        "name": "GST User",
                        "customer_phone": "9999999991",
                        "address": "GST Address 4",
                        "store_address": "Store Address",
                        "city_name": "Pune",
                        "taxable_value": 100,
                        "cgst": 9,
                        "sgst": 9,
                        "total_tax": 18,
                        "final_amount": 118,
                        "payment_status": "Paid",
                    }
                ]
            }
        if "page=1" in url:
            return {
                "data": [
                    {
                        "booking_code": "UC610-DEL-1",
                        "payment_details": '[{"payment_mode": 1, "payment_amount": 118, "created_at": "2026-01-04 11:00:00", "transaction_id": "TXN-DISJOINT-1"}]',
                    }
                ]
            }
        return {"data": []}

    async def _fake_resolve_booking_id_for_order(*, page, order_code, headers):
        return 104, {"booking_code": order_code, "id": 104}

    async def _fake_fetch_invoice_html_with_retries(*, page, booking_id, store_code, order_code, logger, **_kwargs):
        return "<html></html>", 0

    monkeypatch.setattr(gst_api_extract, "_resolve_archive_bearer_token", _fake_resolve_archive_bearer_token)
    monkeypatch.setattr(gst_api_extract, "_request_json_with_retries", _fake_request_json_with_retries)
    monkeypatch.setattr(gst_api_extract, "_resolve_booking_id_for_order", _fake_resolve_booking_id_for_order)
    monkeypatch.setattr(gst_api_extract, "_fetch_invoice_html_with_retries", _fake_fetch_invoice_html_with_retries)
    monkeypatch.setattr(gst_api_extract, "_parse_invoice_order_details", lambda **kwargs: [])

    extract = asyncio.run(
        collect_gst_orders_via_api(
            page=_FakePage(),
            store_code="UC610",
            logger=get_logger("test_uc_gst_api_extract"),
            from_date=date(2026, 1, 1),
            to_date=date(2026, 1, 31),
        )
    )

    assert len(extract.gst_rows) == 1
    assert len(extract.payment_detail_rows) == 1
    assert extract.payment_detail_rows[0]["order_code"] == "UC610-DEL-1"
    assert extract.delivered_rows_matched_gst == 0
    assert extract.gst_orders_without_payments == 1


def test_collect_gst_orders_via_api_dedupes_payment_rows_with_overlap(monkeypatch) -> None:
    async def _fake_resolve_archive_bearer_token(*, page, logger, store_code):
        return "token"

    async def _fake_request_json_with_retries(*, page, method, url, headers, data):
        if method == "POST":
            return {
                "data": [
                    {
                        "order_number": "UC610-OVERLAP-1",
                        "invoice_number": "INV-OVERLAP-1",
                        "invoice_date": "2026-01-05",
                        "name": "Overlap User",
                        "customer_phone": "9999999990",
                        "address": "GST Address 5",
                        "store_address": "Store Address",
                        "city_name": "Pune",
                        "taxable_value": 200,
                        "cgst": 18,
                        "sgst": 18,
                        "total_tax": 36,
                        "final_amount": 236,
                        "payment_status": "Paid",
                    }
                ]
            }
        if "page=1" in url:
            duplicated_payment = '{"payment_mode": 1, "payment_amount": 236, "created_at": "2026-01-05 12:00:00", "transaction_id": "TXN-OVERLAP-1"}'
            return {
                "data": [
                    {
                        "booking_code": "UC610-OVERLAP-1",
                        "payment_details": f'[{duplicated_payment}]',
                    },
                    {
                        "booking_code": "UC610-OVERLAP-1",
                        "payment_details": f'[{duplicated_payment}]',
                    },
                ]
            }
        return {"data": []}

    async def _fake_resolve_booking_id_for_order(*, page, order_code, headers):
        return 105, {"booking_code": order_code, "id": 105}

    async def _fake_fetch_invoice_html_with_retries(*, page, booking_id, store_code, order_code, logger, **_kwargs):
        return "<html></html>", 0

    monkeypatch.setattr(gst_api_extract, "_resolve_archive_bearer_token", _fake_resolve_archive_bearer_token)
    monkeypatch.setattr(gst_api_extract, "_request_json_with_retries", _fake_request_json_with_retries)
    monkeypatch.setattr(gst_api_extract, "_resolve_booking_id_for_order", _fake_resolve_booking_id_for_order)
    monkeypatch.setattr(gst_api_extract, "_fetch_invoice_html_with_retries", _fake_fetch_invoice_html_with_retries)
    monkeypatch.setattr(gst_api_extract, "_parse_invoice_order_details", lambda **kwargs: [])

    extract = asyncio.run(
        collect_gst_orders_via_api(
            page=_FakePage(),
            store_code="UC610",
            logger=get_logger("test_uc_gst_api_extract"),
            from_date=date(2026, 1, 1),
            to_date=date(2026, 1, 31),
        )
    )

    assert len(extract.payment_detail_rows) == 1
    assert extract.payment_detail_rows[0]["order_code"] == "UC610-OVERLAP-1"
    assert extract.delivered_rows_matched_gst == 2
    assert extract.gst_orders_without_payments == 0


def test_collect_gst_orders_via_api_failed_payload_logs_and_returns_failed_status(monkeypatch) -> None:
    async def _fake_resolve_archive_bearer_token(*, page, logger, store_code):
        return "token"

    async def _fake_request_json_with_retries(*, page, method, url, headers, data):
        return None

    monkeypatch.setattr(gst_api_extract, "_resolve_archive_bearer_token", _fake_resolve_archive_bearer_token)
    monkeypatch.setattr(gst_api_extract, "_request_json_with_retries", _fake_request_json_with_retries)
    logger, stream = _memory_logger()

    extract = asyncio.run(
        collect_gst_orders_via_api(
            page=_FakePage(),
            store_code="UC610",
            logger=logger,
            from_date=date(2026, 1, 1),
            to_date=date(2026, 1, 31),
        )
    )

    assert extract.source_fetch_status == "failed"
    assert extract.source_fetch_error_class == "gst_api_failed"
    assert extract.source_fetch_failure_reason == "GST API request did not return a JSON mapping"
    assert extract.confirmed_empty is False
    assert extract.extractor_status == "failed"
    assert extract.skipped_order_counters == {"gst_api_failed": 1}

    events = _logged_events(stream)
    assert any(
        event["phase"] == "gst_api_extract"
        and event["status"] == "warning"
        and event["source_fetch_status"] == "failed"
        and event["source_fetch_error_class"] == "gst_api_failed"
        for event in events
    )
    assert events[-1]["message"] == "GST API experimental extraction complete"
    assert events[-1]["source_fetch_status"] == "failed"
    assert events[-1]["source_fetch_error_class"] == "gst_api_failed"


def test_collect_gst_orders_via_api_invalid_payload_logs_and_returns_failed_status(monkeypatch) -> None:
    async def _fake_resolve_archive_bearer_token(*, page, logger, store_code):
        return "token"

    async def _fake_request_json_with_retries(*, page, method, url, headers, data):
        return {"data": {"unexpected": "mapping"}}

    monkeypatch.setattr(gst_api_extract, "_resolve_archive_bearer_token", _fake_resolve_archive_bearer_token)
    monkeypatch.setattr(gst_api_extract, "_request_json_with_retries", _fake_request_json_with_retries)
    logger, stream = _memory_logger()

    extract = asyncio.run(
        collect_gst_orders_via_api(
            page=_FakePage(),
            store_code="UC610",
            logger=logger,
            from_date=date(2026, 1, 1),
            to_date=date(2026, 1, 31),
        )
    )

    assert extract.source_fetch_status == "failed"
    assert extract.source_fetch_error_class == "gst_api_invalid_data"
    assert extract.source_fetch_failure_reason == "GST API payload data field was not a list"
    assert extract.confirmed_empty is False
    assert extract.extractor_status == "failed"
    assert extract.skipped_order_counters == {"gst_api_invalid_data": 1}

    events = _logged_events(stream)
    assert any(
        event["phase"] == "gst_api_extract"
        and event["status"] == "warning"
        and event["source_fetch_status"] == "failed"
        and event["source_fetch_error_class"] == "gst_api_invalid_data"
        for event in events
    )
    assert events[-1]["message"] == "GST API experimental extraction complete"
    assert events[-1]["source_fetch_error_class"] == "gst_api_invalid_data"


def test_collect_gst_orders_via_api_empty_data_logs_confirmed_empty(monkeypatch) -> None:
    async def _fake_resolve_archive_bearer_token(*, page, logger, store_code):
        return "token"

    async def _fake_request_json_with_retries(*, page, method, url, headers, data):
        return {"data": []}

    monkeypatch.setattr(gst_api_extract, "_resolve_archive_bearer_token", _fake_resolve_archive_bearer_token)
    monkeypatch.setattr(gst_api_extract, "_request_json_with_retries", _fake_request_json_with_retries)
    logger, stream = _memory_logger()

    extract = asyncio.run(
        collect_gst_orders_via_api(
            page=_FakePage(),
            store_code="UC610",
            logger=logger,
            from_date=date(2026, 1, 1),
            to_date=date(2026, 1, 31),
        )
    )

    assert extract.source_fetch_status == "complete"
    assert extract.source_fetch_error_class is None
    assert extract.source_fetch_failure_reason is None
    assert extract.confirmed_empty is True
    assert extract.extractor_status == "success"
    assert extract.skipped_order_counters == {}
    assert extract.gst_rows == []

    events = _logged_events(stream)
    assert any(
        event["phase"] == "gst_api_extract"
        and event["message"] == "GST API source returned no rows"
        and event["gst_rows"] == 0
        and event["source_fetch_status"] == "complete"
        and event["confirmed_empty"] is True
        for event in events
    )
    assert events[-1]["message"] == "GST API experimental extraction complete"
    assert events[-1]["gst_rows"] == 0
    assert events[-1]["confirmed_empty"] is True


def test_collect_gst_orders_via_api_non_empty_data_preserves_current_status(monkeypatch) -> None:
    async def _fake_resolve_archive_bearer_token(*, page, logger, store_code):
        return "token"

    async def _fake_request_json_with_retries(*, page, method, url, headers, data):
        if method == "POST":
            return {
                "data": [
                    {
                        "order_number": "UC610-0006",
                        "invoice_number": "INV-006",
                        "invoice_date": "2026-01-06",
                        "name": "Test User 6",
                        "customer_phone": "9999999986",
                        "store_address": "Store Address",
                        "city_name": "Pune",
                        "taxable_value": 100,
                        "cgst": 9,
                        "sgst": 9,
                        "total_tax": 18,
                        "final_amount": 118,
                        "payment_status": "Paid",
                    }
                ]
            }
        return {"data": []}

    async def _fake_resolve_booking_id_for_order(*, page, order_code, headers):
        return None, None

    monkeypatch.setattr(gst_api_extract, "_resolve_archive_bearer_token", _fake_resolve_archive_bearer_token)
    monkeypatch.setattr(gst_api_extract, "_request_json_with_retries", _fake_request_json_with_retries)
    monkeypatch.setattr(gst_api_extract, "_resolve_booking_id_for_order", _fake_resolve_booking_id_for_order)
    logger, stream = _memory_logger()

    extract = asyncio.run(
        collect_gst_orders_via_api(
            page=_FakePage(),
            store_code="UC610",
            logger=logger,
            from_date=date(2026, 1, 1),
            to_date=date(2026, 1, 31),
        )
    )

    assert extract.source_fetch_status == "success"
    assert extract.source_fetch_error_class is None
    assert extract.source_fetch_failure_reason is None
    assert extract.confirmed_empty is False
    assert len(extract.gst_rows) == 1
    assert len(extract.base_rows) == 1

    events = _logged_events(stream)
    assert events[-1]["message"] == "GST API experimental extraction complete"
    assert events[-1]["source_fetch_status"] == "success"
    assert events[-1]["confirmed_empty"] is False
