from __future__ import annotations

import asyncio
import io
import json
from datetime import date

from app.crm_downloader.uc_orders_sync import archive_api_extract
from app.crm_downloader.uc_orders_sync.archive_api_extract import (
    ArchiveApiExtract,
    _map_status,
    _parse_invoice_order_details,
    _parse_payment_rows,
    _fetch_invoice_html_with_retries,
    _record_extractor_error,
    _resolve_archive_bearer_token,
    collect_archive_orders_via_api,
)
from app.dashboard_downloader.json_logger import JsonLogger, get_logger


class _FakePage:
    def __init__(self, evaluate_result):
        self._evaluate_result = evaluate_result

    async def evaluate(self, _script: str):
        return self._evaluate_result


def _read_events(raw: str) -> list[dict[str, object]]:
    lines = [line for line in raw.splitlines() if line.strip()]
    return [json.loads(line) for line in lines]


def test_resolve_archive_bearer_token_logs_source_length_and_candidate_keys_once_per_store() -> None:
    archive_api_extract._TOKEN_KEY_DEBUG_LOGGED_STORES.clear()
    stream = io.StringIO()
    logger = JsonLogger(run_id="test", stream=stream, log_file_path=None)
    page = _FakePage(
        {
            "token": "Bearer abc.def.ghi",
            "tokenSourceType": "localStorage_direct_key",
            "candidateLocalStorageKeys": ["authToken", "jwt", "profile"],
        }
    )

    token = asyncio.run(
        _resolve_archive_bearer_token(page=page, logger=logger, store_code="UC610")
    )
    second_token = asyncio.run(
        _resolve_archive_bearer_token(page=page, logger=logger, store_code="UC610")
    )

    assert token == "Bearer abc.def.ghi"
    assert second_token == "Bearer abc.def.ghi"

    events = _read_events(stream.getvalue())
    diagnostics = [e for e in events if e.get("message") == "Resolved archive bearer token diagnostics"]
    assert len(diagnostics) == 1
    assert diagnostics[0]["token_source_type"] == "localStorage_direct_key"
    assert diagnostics[0]["token_length"] == len("Bearer abc.def.ghi")

    key_events = [e for e in events if e.get("message") == "Archive token key candidates detected in localStorage"]
    assert len(key_events) == 1
    assert key_events[0]["candidate_local_storage_keys"] == ["authToken", "jwt"]


def test_resolve_archive_bearer_token_logs_none_source_without_token_value() -> None:
    archive_api_extract._TOKEN_KEY_DEBUG_LOGGED_STORES.clear()
    stream = io.StringIO()
    logger = JsonLogger(run_id="test", stream=stream, log_file_path=None)
    page = _FakePage(
        {
            "token": None,
            "tokenSourceType": "none",
            "candidateLocalStorageKeys": ["profileAuthState"],
        }
    )

    token = asyncio.run(
        _resolve_archive_bearer_token(page=page, logger=logger, store_code="UC567")
    )

    assert token is None

    events = _read_events(stream.getvalue())
    diagnostics = [e for e in events if e.get("message") == "Resolved archive bearer token diagnostics"]
    assert len(diagnostics) == 1
    assert diagnostics[0]["token_source_type"] == "none"
    assert diagnostics[0]["token_length"] == 0


def test_parse_invoice_order_details_multiline_items() -> None:
    html = """
    <div class=\"order-info-label\">Order No. - UC610-0759 <span class=\"order-mode\">(App)</span></div>
    <div style=\"font-size: 13px; color: #666;\">2026-01-04 10:45:53</div>
    <div class=\"order-info-label\">Pickup Done Date &amp; Time</div>
    <div class=\"order-info-value\">2026-01-04 11 AM - 1 PM</div>
    <div class=\"order-info-label\">Delivery Date &amp; Time</div>
    <div class=\"order-info-value\">2026-01-06 11 AM - 1 PM</div>
    <table>
      <tbody>
        <tr>
          <td>1.</td>
          <td>Dry cleaning</td>
          <td>999712</td>
          <td><ul><li>Formal and Casual Trousers / Pants</li><li>Blazer / Coat - Short</li></ul></td>
          <td><div>109.00</div><div>299.00</div></td>
          <td><div>1</div><div>1</div></td>
          <td>-</td>
          <td>0.00</td>
          <td><strong>408.00</strong></td>
        </tr>
      </tbody>
    </table>
    """

    rows = _parse_invoice_order_details(
        invoice_html=html,
        store_code="UC610",
        order_code="UC610-0759",
    )

    assert len(rows) == 2
    assert rows[0]["service"] == "Dry cleaning"
    assert rows[0]["item_name"] == "Formal and Casual Trousers / Pants"
    assert rows[1]["item_name"] == "Blazer / Coat - Short"
    assert rows[0]["order_mode"] == "App"


def test_parse_payment_rows_multi_mode_and_unknown() -> None:
    logger = get_logger("test_uc_archive_api_extract")
    payment_details = (
        '[{"created_at":"2026-01-20 18:49:36.000000","payment_mode":1,"payment_amount":20.00},'
        '{"created_at":"2026-01-20 18:49:36.000000","payment_mode":4,"payment_amount":392.00},'
        '{"created_at":"2026-01-20 18:49:36.000000","payment_mode":99,"payment_amount":1.00}]'
    )
    rows = _parse_payment_rows(
        store_code="UC610",
        order_code="UC610-0769",
        payment_details="".join(payment_details),
        logger=logger,
    )

    assert len(rows) == 3
    assert rows[0]["payment_mode"] == "UPI"
    assert rows[1]["payment_mode"] == "Cash"
    assert rows[2]["payment_mode"] == "UNKNOWN"


def test_map_status_values() -> None:
    assert _map_status(7) == "Delivered"
    assert _map_status(0) == "Cancelled"
    assert _map_status(99) == "Unknown"
    assert _map_status(None) == "Unknown"


def test_record_extractor_error_tracks_counters_and_unique_reason_codes() -> None:
    extract = ArchiveApiExtract()

    _record_extractor_error(extract, reason="archive_api_page_failed")
    _record_extractor_error(extract, reason="archive_api_page_failed")
    _record_extractor_error(extract, reason="auth_401")

    assert extract.extractor_error_counters == {
        "archive_api_page_failed": 2,
        "auth_401": 1,
    }
    assert extract.extractor_reason_codes == ["archive_api_page_failed", "auth_401"]


class _FakeResponse:
    def __init__(self, *, status: int, payload: str = "") -> None:
        self.status = status
        self._payload = payload

    async def text(self) -> str:
        return self._payload


class _FakeRequestClient:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self._responses = responses

    async def get(self, _url: str, timeout: int, headers: dict[str, str]) -> _FakeResponse:
        assert timeout == 90_000
        assert headers.get("Referer") == archive_api_extract.ARCHIVE_REFERER
        return self._responses.pop(0)


class _FakeArchivePage:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self.request = _FakeRequestClient(responses)

    async def evaluate(self, _script: str):
        return {
            "token": "abc.def",
            "tokenSourceType": "localStorage_direct_key",
            "candidateLocalStorageKeys": ["authToken"],
        }


def test_fetch_invoice_success_logs_debug_not_info() -> None:
    archive_api_extract._TOKEN_DIAGNOSTICS_LOGGED_STORES.clear()
    archive_api_extract._TOKEN_KEY_DEBUG_LOGGED_STORES.clear()
    stream = io.StringIO()
    logger = JsonLogger(run_id="test", stream=stream, log_file_path=None)
    page = _FakeArchivePage([_FakeResponse(status=200, payload="<html>ok</html>")])

    html, retries = asyncio.run(
        _fetch_invoice_html_with_retries(
            page=page,
            booking_id=123,
            store_code="UC610",
            order_code="UC610-0001",
            logger=logger,
        )
    )

    assert html == "<html>ok</html>"
    assert retries == 0
    events = _read_events(stream.getvalue())
    success_events = [e for e in events if e.get("message") == "Invoice API request succeeded"]
    assert len(success_events) == 1
    assert success_events[0]["status"] == "debug"


def test_collect_archive_orders_logs_store_summary_payload() -> None:
    archive_api_extract._TOKEN_DIAGNOSTICS_LOGGED_STORES.clear()
    archive_api_extract._TOKEN_KEY_DEBUG_LOGGED_STORES.clear()
    stream = io.StringIO()
    logger = JsonLogger(run_id="test", stream=stream, log_file_path=None)

    async def _fake_api_get_json_with_retries(**_kwargs):
        return (
            {
                "data": [
                    {
                        "booking_code": "UC610-0001",
                        "id": 101,
                        "status": 7,
                        "pickupDate": "2026-01-01",
                        "dropDate": "2026-01-02",
                        "name": "Customer 1",
                        "mobile": "123",
                        "address": "Addr 1",
                        "final_amount": "100",
                        "suggestion": None,
                        "payment_details": "[]",
                        "delivered_at": "2026-01-02",
                        "updated_at": "2026-01-02",
                    },
                    {
                        "booking_code": "UC610-0002",
                        "id": 102,
                        "status": 7,
                        "pickupDate": "2026-01-03",
                        "dropDate": "2026-01-04",
                        "name": "Customer 2",
                        "mobile": "456",
                        "address": "Addr 2",
                        "final_amount": "200",
                        "suggestion": None,
                        "payment_details": "[]",
                        "delivered_at": "2026-01-04",
                        "updated_at": "2026-01-04",
                    },
                ],
                "pagination": {"total": 2, "totalPages": 1},
            },
            None,
        )

    async def _fake_fetch_invoice_html_with_retries(*, order_code: str, **_kwargs):
        if order_code == "UC610-0001":
            return (
                """
                <div class="order-info-label">Order No. - UC610-0001 <span class="order-mode">(App)</span></div>
                <table><tbody><tr>
                <td>1.</td><td>Dry cleaning</td><td>999712</td><td><div>Shirt</div></td><td><div>50</div></td><td><div>1</div></td><td>-</td><td>0</td><td><div>50</div></td>
                </tr></tbody></table>
                """,
                1,
            )
        return None, 0

    original_api = archive_api_extract._api_get_json_with_retries
    original_invoice = archive_api_extract._fetch_invoice_html_with_retries
    archive_api_extract._api_get_json_with_retries = _fake_api_get_json_with_retries
    archive_api_extract._fetch_invoice_html_with_retries = _fake_fetch_invoice_html_with_retries
    try:
        extract = asyncio.run(
            collect_archive_orders_via_api(
                page=_FakePage({}),
                store_code="UC610",
                logger=logger,
                from_date=date(2026, 1, 1),
                to_date=date(2026, 1, 31),
            )
        )
    finally:
        archive_api_extract._api_get_json_with_retries = original_api
        archive_api_extract._fetch_invoice_html_with_retries = original_invoice

    assert len(extract.base_rows) == 2
    assert len(extract.order_detail_rows) == 1
    events = _read_events(stream.getvalue())
    summary_events = [e for e in events if e.get("message") == "Archive API store summary"]
    assert len(summary_events) == 1
    summary = summary_events[0]
    assert summary["status"] == "ok"
    assert summary["attempted_invoices"] == 2
    assert summary["successful_invoices"] == 1
    assert summary["failed_invoices"] == 1
    assert summary["retry_count"] == 1
    assert summary["sample_failed_order_codes"] == ["UC610-0002"]
    assert isinstance(summary["elapsed_seconds"], float)
