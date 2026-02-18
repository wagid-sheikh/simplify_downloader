from __future__ import annotations

from datetime import date

from app.crm_downloader.uc_orders_sync import main as uc_main


def test_gst_derived_filenames_use_single_uc_gst_prefix_without_legacy_source_overlap() -> None:
    from_date = date(2025, 1, 1)
    to_date = date(2025, 1, 2)

    base = uc_main._format_gst_derived_filename(
        "A100", from_date, to_date, artifact_type="base_order_info", run_id="run-api-1"
    )
    details = uc_main._format_gst_derived_filename(
        "A100", from_date, to_date, artifact_type="order_details", run_id="run-api-1"
    )
    payments = uc_main._format_gst_derived_filename(
        "A100", from_date, to_date, artifact_type="payment_details", run_id="run-api-1"
    )

    assert base == "A100-uc_gst-base_order_info_20250101_20250102_run-api-1.xlsx"
    assert details == "A100-uc_gst-order_details_20250101_20250102_run-api-1.xlsx"
    assert payments == "A100-uc_gst-payment_details_20250101_20250102_run-api-1.xlsx"

    for filename in (base, details, payments):
        assert "archive_api" not in filename
        assert "gst_api" not in filename
