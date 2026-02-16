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
        assert method == "POST"
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

    async def _fake_resolve_booking_id_for_order(*, page, order_code, headers):
        return 101, {
            "booking_code": order_code,
            "id": 101,
            "payment_status": "pending",
            "address": "Booking Address",
            "suggestions": "Handle carefully",
        }

    async def _fake_fetch_invoice_html_with_retries(*, page, booking_id, store_code, order_code, logger):
        return None

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
    assert extract.base_rows[0]["address"] == "Booking Address"
    assert extract.base_rows[0]["instructions"] == "Handle carefully"
    assert extract.skipped_order_counters["invoice_fetch_failed"] == 1
