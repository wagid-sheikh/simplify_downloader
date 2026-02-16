from app.crm_downloader.uc_orders_sync.extract_comparator import compare_extracts


def test_compare_extracts_reports_coverage_and_mismatches() -> None:
    summary, details = compare_extracts(
        legacy_gst_rows=[
            {
                "order_number": "A1",
                "invoice_number": "INV-1",
                "invoice_date": "2026-01-01",
                "name": "Alice",
                "customer_phone": "9000",
                "address": "X",
                "final_amount": 100,
                "payment_status": "Pending",
            },
            {"order_number": "A2", "name": "Bob"},
        ],
        candidate_gst_rows=[
            {
                "order_number": "A1",
                "invoice_number": "INV-1",
                "invoice_date": "2026-01-01",
                "name": "Alice",
                "customer_phone": "9000",
                "address": "Y",
                "final_amount": 100,
                "payment_status": "Pending",
            },
            {"order_number": "A3", "name": "Cara"},
        ],
        legacy_base_rows=[
            {"order_code": "A1", "customer_name": "Alice", "address": "X", "status": "Delivered"},
            {"order_code": "A2", "customer_name": "Bob", "address": "Y", "status": "Pending"},
        ],
        legacy_order_detail_rows=[{"order_code": "A1"}],
        legacy_payment_rows=[{"order_code": "A1", "payment_mode": "UPI", "amount": 100, "payment_date": "2026-01-01"}],
        candidate_base_rows=[
            {"order_code": "A1", "customer_name": "Alice", "address": "Z", "status": "Delivered"},
            {"order_code": "A3", "customer_name": "Cara", "address": "Q", "status": "Pending"},
        ],
        candidate_order_detail_rows=[{"order_code": "A1"}, {"order_code": "A3"}],
        candidate_payment_rows=[{"order_code": "A1", "payment_mode": "UNKNOWN", "amount": 100, "payment_date": "2026-01-01"}],
    )

    assert summary.legacy_gst_rows == 2
    assert summary.candidate_gst_rows == 2
    assert summary.common_gst == 1
    assert summary.missing_in_candidate_gst == 1
    assert summary.missing_in_legacy_gst == 1
    assert summary.legacy_base_rows == 2
    assert summary.candidate_base_rows == 2
    assert summary.common_base == 1
    assert summary.missing_in_candidate_base == 1
    assert summary.missing_in_legacy_base == 1
    assert details["gst_field_mismatch_counts"]["address"] == 1
    assert details["base_field_mismatch_counts"]["address"] == 1
    assert details["payment_field_mismatch_counts"]["payment_mode"] == 1
    assert details["sample_missing_in_candidate_gst"] == ["A2"]
    assert details["sample_missing_in_legacy_gst"] == ["A3"]
    assert details["sample_missing_in_candidate_base"] == ["A2"]
    assert details["sample_missing_in_legacy_base"] == ["A3"]
