from app.crm_downloader.uc_orders_sync.extract_comparator import compare_extracts


def test_compare_extracts_reports_coverage_and_mismatches() -> None:
    summary, details = compare_extracts(
        legacy_base_rows=[
            {"order_code": "A1", "customer_name": "Alice", "address": "X", "status": "Delivered"},
            {"order_code": "A2", "customer_name": "Bob", "address": "Y", "status": "Pending"},
        ],
        legacy_order_detail_rows=[{"order_code": "A1"}],
        legacy_payment_rows=[{"order_code": "A1"}],
        candidate_base_rows=[
            {"order_code": "A1", "customer_name": "Alice", "address": "Z", "status": "Delivered"},
            {"order_code": "A3", "customer_name": "Cara", "address": "Q", "status": "Pending"},
        ],
        candidate_order_detail_rows=[{"order_code": "A1"}, {"order_code": "A3"}],
        candidate_payment_rows=[],
    )

    assert summary.legacy_base_rows == 2
    assert summary.candidate_base_rows == 2
    assert summary.common_base == 1
    assert summary.missing_in_candidate_base == 1
    assert summary.missing_in_legacy_base == 1
    assert details["field_mismatch_counts"]["address"] == 1
    assert details["sample_missing_in_candidate"] == ["A2"]
    assert details["sample_missing_in_legacy"] == ["A3"]
