from app.crm_downloader.uc_orders_sync.extract_comparator import (
    MigrationThresholds,
    compare_extracts,
)


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


def test_compare_extracts_threshold_evaluation_and_migration_signal() -> None:
    _, details = compare_extracts(
        legacy_gst_rows=[
            {"order_number": "A1"},
            {"order_number": "A2"},
            {"order_number": "A3"},
        ],
        candidate_gst_rows=[
            {"order_number": "A1"},
        ],
        legacy_base_rows=[{"order_code": "A1"}],
        legacy_order_detail_rows=[],
        legacy_payment_rows=[
            {"order_code": "A1", "payment_mode": "UPI", "amount": 100, "payment_date": "2026-01-01"},
            {"order_code": "A2", "payment_mode": "COD", "amount": 200, "payment_date": "2026-01-01"},
        ],
        candidate_base_rows=[{"order_code": "A1"}],
        candidate_order_detail_rows=[],
        candidate_payment_rows=[
            {"order_code": "A1", "payment_mode": "CARD", "amount": 100, "payment_date": "2026-01-01"},
        ],
        thresholds=MigrationThresholds(
            gst_key_parity_min_pct=80.0,
            payment_coverage_min_pct=80.0,
            payment_field_mismatch_max_pct=0.0,
        ),
    )

    assert details["threshold_evaluation"]["metrics"]["gst_key_parity_pct"] == 33.33
    assert details["threshold_evaluation"]["metrics"]["payment_coverage_pct"] == 50.0
    assert details["threshold_evaluation"]["metrics"]["payment_field_mismatch_pct"] == 33.33
    assert details["migration_ready"] is False
    assert details["migration_reason_codes"] == [
        "below_gst_key_parity_threshold",
        "below_payment_coverage_threshold",
        "above_payment_field_mismatch_threshold",
    ]


def test_compare_extracts_handles_multiple_payment_rows_per_order() -> None:
    _, details = compare_extracts(
        legacy_gst_rows=[],
        candidate_gst_rows=[],
        legacy_base_rows=[],
        legacy_order_detail_rows=[],
        legacy_payment_rows=[
            {"order_code": "A1", "payment_mode": "UPI", "amount": 50, "payment_date": "2026-01-01 10:00:00"},
            {"order_code": "A1", "payment_mode": "Cash", "amount": 75, "payment_date": "2026-01-01 11:00:00"},
        ],
        candidate_base_rows=[],
        candidate_order_detail_rows=[],
        candidate_payment_rows=[
            {"order_code": "A1", "payment_mode": "UPI", "amount": 50, "payment_date": "2026-01-01 10:00:00"},
            {"order_code": "A1", "payment_mode": "Cash", "amount": 70, "payment_date": "2026-01-01 11:00:00"},
        ],
    )

    assert details["payment_coverage"]["legacy_payment_rows"] == 2
    assert details["payment_coverage"]["candidate_payment_rows"] == 2
    assert details["payment_coverage"]["common_payment_rows_compared"] == 2
    assert details["payment_field_mismatch_counts"]["payment_mode"] == 0
    assert details["payment_field_mismatch_counts"]["amount"] == 1
