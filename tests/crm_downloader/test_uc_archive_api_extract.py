from __future__ import annotations

from app.crm_downloader.uc_orders_sync.archive_api_extract import (
    ArchiveApiExtract,
    _record_extractor_error,
    _map_status,
    _parse_invoice_order_details,
    _parse_payment_rows,
)
from app.dashboard_downloader.json_logger import get_logger


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
