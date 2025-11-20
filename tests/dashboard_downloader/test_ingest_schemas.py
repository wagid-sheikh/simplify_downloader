import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.common.ingest.schemas import coerce_csv_row, normalize_headers


def _headers_with_required(extra: list[str] | None = None) -> list[str]:
    base = ["Pickup Row Id", "Store Code", "Mobile Number"]
    if extra:
        base.extend(extra)
    return base


def _row_with_required(**extra):
    row = {
        "Pickup Row Id": "12345",
        "Store Code": "A001",
        "Mobile Number": "9876543210",
    }
    row.update(extra)
    return row


def test_bool_coercion_true():
    headers = _headers_with_required(["is_order_placed"])
    header_map = normalize_headers(headers)
    row = _row_with_required(**{"is_order_placed": "1"})
    result = coerce_csv_row("missed_leads", row, header_map)
    assert result["is_order_placed"] is True


def test_bool_coercion_false_for_zero():
    headers = _headers_with_required(["is_order_placed"])
    header_map = normalize_headers(headers)
    row = _row_with_required(**{"is_order_placed": "0"})
    result = coerce_csv_row("missed_leads", row, header_map)
    assert result["is_order_placed"] is False


def test_bool_coercion_false_for_other():
    headers = _headers_with_required(["is_order_placed"])
    header_map = normalize_headers(headers)
    row = _row_with_required(**{"is_order_placed": "yes"})
    result = coerce_csv_row("missed_leads", row, header_map)
    assert result["is_order_placed"] is False


def test_coercion_with_spaced_headers():
    headers = _headers_with_required(["Customer Name"])
    header_map = normalize_headers(headers)
    row = _row_with_required(**{"Customer Name": "Alice"})
    result = coerce_csv_row("missed_leads", row, header_map)
    assert result["pickup_row_id"] == 12345
    assert result["mobile_number"] == "9876543210"
    assert result["customer_name"] == "Alice"


def test_missing_required_raises_value_error():
    headers = _headers_with_required()
    header_map = normalize_headers(headers)
    row = {
        "Store Code": "A001",
        "Mobile Number": "9876543210",
    }
    try:
        coerce_csv_row("missed_leads", row, header_map)
    except ValueError as exc:
        assert "pickup_row_id" in str(exc)
    else:
        raise AssertionError("Expected ValueError for missing required column")


def test_undelivered_uses_order_no_for_order_id():
    headers = [
        "order_no",
        "order_date",
        "store_code",
    ]
    header_map = normalize_headers(headers)
    row = {
        "order_no": "ORD-123",
        "order_date": "2024-03-20",
        "store_code": "SC001",
    }

    result = coerce_csv_row("undelivered_all", row, header_map)

    assert result["order_id"] == "ORD-123"


def test_undelivered_missing_order_id_and_order_no_raises():
    headers = [
        "order_date",
        "store_code",
    ]
    header_map = normalize_headers(headers)
    row = {
        "order_date": "2024-03-20",
        "store_code": "SC001",
    }

    with pytest.raises(ValueError) as excinfo:
        coerce_csv_row("undelivered_all", row, header_map)

    assert "order_id" in str(excinfo.value)
