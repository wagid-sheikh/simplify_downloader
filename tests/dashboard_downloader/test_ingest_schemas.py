from simplify_downloader.common.ingest.schemas import coerce_csv_row, normalize_headers


def test_bool_coercion_true():
    headers = ["is_order_placed"]
    header_map = normalize_headers(headers)
    row = {"is_order_placed": "1"}
    result = coerce_csv_row("missed_leads", row, header_map)
    assert result["is_order_placed"] is True


def test_bool_coercion_false_for_zero():
    headers = ["is_order_placed"]
    header_map = normalize_headers(headers)
    row = {"is_order_placed": "0"}
    result = coerce_csv_row("missed_leads", row, header_map)
    assert result["is_order_placed"] is False


def test_bool_coercion_false_for_other():
    headers = ["is_order_placed"]
    header_map = normalize_headers(headers)
    row = {"is_order_placed": "yes"}
    result = coerce_csv_row("missed_leads", row, header_map)
    assert result["is_order_placed"] is False


def test_coercion_with_spaced_headers():
    headers = ["Pickup Row Id", "Mobile Number", "Customer Name"]
    header_map = normalize_headers(headers)
    row = {
        "Pickup Row Id": "12345",
        "Mobile Number": "9876543210",
        "Customer Name": "Alice",
    }
    result = coerce_csv_row("missed_leads", row, header_map)
    assert result["pickup_row_id"] == 12345
    assert result["mobile_number"] == "9876543210"
    assert result["customer_name"] == "Alice"
