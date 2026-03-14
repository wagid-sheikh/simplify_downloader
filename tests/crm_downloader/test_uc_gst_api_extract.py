from __future__ import annotations

import asyncio
from datetime import date

from app.crm_downloader.uc_orders_sync import gst_api_extract
from app.crm_downloader.uc_orders_sync.gst_api_extract import collect_gst_orders_via_api
from app.dashboard_downloader.json_logger import get_logger


class _FakePage:
    pass


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

    async def _fake_fetch_invoice_html_with_retries(*, page, booking_id, store_code, order_code, logger):
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

    async def _fake_fetch_invoice_html_with_retries(*, page, booking_id, store_code, order_code, logger):
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
