from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from app.crm_downloader.td_orders_sync.main import (
    StoreOutcome,
    StoreReport,
    TdOrdersDiscoverySummary,
    _build_dataset_order_set_verdict,
    _build_sales_order_row_count_verdict,
    _compare_row_count_diagnostics,
    _filter_non_order_summary_rows,
    _resolve_compare_rows,
)


def test_summary_text_lists_ingest_remarks() -> None:
    summary = TdOrdersDiscoverySummary(
        run_id="run-1", run_env="test", report_date=date(2024, 1, 1), report_end_date=date(2024, 1, 1)
    )
    summary.started_at = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    summary.add_ingest_remarks(
        [
            {"store_code": "a1", "order_number": "123", "ingest_remarks": "invalid phone"},
            {"store_code": "a2", "order_number": "456", "ingest_remarks": "missing date"},
        ]
    )

    finished_at = summary.started_at + timedelta(minutes=5)
    text = summary.summary_text(finished_at=finished_at)

    assert "TD Orders & Sales Run Summary" in text
    assert "Overall Status:" in text

    record = summary.build_record(finished_at=finished_at)
    ingest_metrics = record["metrics_json"]["ingest_remarks"]
    assert ingest_metrics["total"] == 2
    assert ingest_metrics["rows"][0]["store_code"] == "A1"
    payload = record["metrics_json"]["notification_payload"]["ingest_warnings"]
    assert payload["total"] == 2


def test_summary_ingest_remarks_truncate_notice() -> None:
    summary = TdOrdersDiscoverySummary(
        run_id="run-2", run_env="test", report_date=date(2024, 1, 2), report_end_date=date(2024, 1, 2)
    )
    long_remark = "x" * 205
    remarks = [{"store_code": "a1", "order_number": "1", "ingest_remarks": long_remark}]
    remarks.extend(
        {"store_code": f"a{i}", "order_number": str(i), "ingest_remarks": "note"} for i in range(2, 56)
    )
    summary.add_ingest_remarks(remarks)

    record = summary.build_record(finished_at=datetime(2024, 1, 2, tzinfo=timezone.utc))

    ingest_metrics = record["metrics_json"]["ingest_remarks"]
    ingest_payload = record["metrics_json"]["notification_payload"]["ingest_warnings"]
    assert ingest_metrics["total"] == 55
    assert ingest_payload["total"] == 55
    assert ingest_payload["truncated"] is True
    assert ingest_payload["rows"][0]["ingest_remarks"].endswith("…")


def test_summary_records_orders_and_sales_results() -> None:
    summary = TdOrdersDiscoverySummary(
        run_id="run-3", run_env="test", report_date=date(2024, 1, 3), report_end_date=date(2024, 1, 3)
    )
    orders_report = StoreReport(
        status="warning",
        filenames=["orders_A1.xlsx"],
        staging_rows=2,
        final_rows=2,
        warnings=["duplicate rows dropped"],
    )
    sales_report = StoreReport(
        status="ok",
        filenames=["sales_A1.xlsx"],
        staging_rows=3,
        final_rows=3,
        message="Sales ingested: staging=3, final=3",
    )

    summary.record_store(
        "A1",
        StoreOutcome(status="warning", message="Orders downloaded with ingest warnings"),
        orders_result=orders_report,
        sales_result=sales_report,
    )
    record = summary.build_record(finished_at=datetime(2024, 1, 3, tzinfo=timezone.utc))

    orders_metrics = record["metrics_json"]["orders"]
    sales_metrics = record["metrics_json"]["sales"]
    assert orders_metrics["overall_status"] == "success_with_warnings"
    assert sales_metrics["overall_status"] == "success"
    assert orders_metrics["stores"]["A1"]["filenames"] == ["orders_A1.xlsx"]
    assert sales_metrics["stores"]["A1"]["final_rows"] == 3

    text_lines = summary.summary_text(finished_at=datetime(2024, 1, 3, tzinfo=timezone.utc)).splitlines()
    assert "**Per Store Orders Metrics:**" in text_lines
    assert any(line.startswith("- A1 — SUCCESS_WITH_WARNINGS") for line in text_lines)
    assert any("warning_count: 1" in line for line in text_lines)
    assert any(line.startswith("- A1 — SUCCESS") for line in text_lines)


def test_sales_warnings_reflected_in_summary() -> None:
    summary = TdOrdersDiscoverySummary(
        run_id="run-4", run_env="test", report_date=date(2024, 1, 4), report_end_date=date(2024, 1, 4)
    )
    sales_report = StoreReport(
        status="ok",
        filenames=["sales_A2.xlsx"],
        staging_rows=4,
        final_rows=4,
        warnings=["Phone value '12345' is invalid and was dropped"],
    )
    summary.record_store("A2", StoreOutcome(status="ok", message="Store run completed"), sales_result=sales_report)

    record = summary.build_record(finished_at=datetime(2024, 1, 4, tzinfo=timezone.utc))
    sales_metrics = record["metrics_json"]["sales"]
    assert sales_metrics["overall_status"] == "success_with_warnings"

    text_lines = summary.summary_text(finished_at=datetime(2024, 1, 4, tzinfo=timezone.utc)).splitlines()
    assert any(line.startswith("- A2 — SUCCESS") for line in text_lines)
    assert any("warning_count: 1" in line for line in text_lines)


def test_summary_text_filters_row_fields_and_truncates_samples() -> None:
    summary = TdOrdersDiscoverySummary(
        run_id="run-5", run_env="test", report_date=date(2024, 1, 5), report_end_date=date(2024, 1, 5)
    )
    warning_rows = [
        {"values": {"store_code": "A1", "order_number": "W1", "ingest_remarks": "bad W1", "phone": "111"}},
        {"values": {"store_code": "A1", "order_number": "W2", "ingest_remarks": "bad W2", "email": "x@example.com"}},
        {"values": {"store_code": "A1", "order_number": "W3", "ingest_remarks": "bad W3", "customer": "Alice"}},
        {"values": {"store_code": "A1", "order_number": "W4", "ingest_remarks": "bad W4", "customer": "Bob"}},
    ]
    dropped_rows = [
        {"values": {"store_code": "A1", "order_number": "D1", "ingest_remarks": "drop 1", "address": "hidden"}},
        {"values": {"store_code": "A1", "order_number": "D2", "ingest_remarks": "drop 2", "mobile_number": "000"}},
        {"values": {"store_code": "A1", "order_number": "D3", "ingest_remarks": "drop 3", "fax": "111"}},
        {"values": {"store_code": "A1", "order_number": "D4", "ingest_remarks": "drop 4", "carrier": "555"}},
    ]
    edited_rows = [
        {"values": {"store_code": "A1", "order_number": "E1", "payment_mode": "cash"}},
        {"values": {"store_code": "A1", "order_number": "E2", "adjustment": "1.23"}},
        {"values": {"store_code": "A1", "order_number": "E3", "refund": "2.00"}},
        {"values": {"store_code": "A1", "order_number": "E4", "notes": "fix"}},
    ]
    summary.record_store(
        "A1",
        StoreOutcome(status="warning", message="Data warnings"),
        orders_result=StoreReport(status="warning", warning_rows=warning_rows, dropped_rows=dropped_rows),
        sales_result=StoreReport(status="warning", edited_rows=edited_rows),
    )

    finished_at = datetime(2024, 1, 5, tzinfo=timezone.utc)
    text_lines = summary.summary_text(finished_at=finished_at).splitlines()

    def _collect_row_lines(marker: str) -> list[str]:
        start = text_lines.index(marker) + 1
        collected: list[str] = []
        for line in text_lines[start:]:
            if line.startswith("    "):
                collected.append(line.strip())
            else:
                break
        return collected

    warning_lines = _collect_row_lines("  warning rows:")
    dropped_lines = _collect_row_lines("  dropped rows:")
    edited_lines = _collect_row_lines("  edited rows:")
    assert len(warning_lines) == len(warning_rows)
    assert len(dropped_lines) == len(dropped_rows)
    assert len(edited_lines) == len(edited_rows)
    assert all("ingest_remarks=" in line for line in warning_lines)
    assert all("phone" not in line and "email" not in line for line in warning_lines)
    assert all("…truncated" not in line for line in warning_lines)

    assert all("ingest_remarks=" in line for line in dropped_lines)
    assert all("address" not in line for line in dropped_lines)
    assert all("…truncated" not in line for line in dropped_lines)

    assert all("payment_mode" not in line and "adjustment" not in line for line in edited_lines)
    assert all("ingest_remarks" not in line for line in edited_lines)
    assert all("store_code=" in line and "order_number=" in line for line in edited_lines)
    assert all("…truncated" not in line for line in edited_lines)


def test_daily_reconciliation_summary_groups_pass_and_fail() -> None:
    summary = TdOrdersDiscoverySummary(
        run_id="run-6", run_env="test", report_date=date(2024, 1, 6), report_end_date=date(2024, 1, 6)
    )
    summary.record_store(
        "A1",
        StoreOutcome(status="ok", message="done"),
        orders_result=StoreReport(
            status="ok",
            threshold_verdict={"pass": True, "reason_codes": []},
            api_ready=True,
            consecutive_pass_windows=3,
        ),
    )
    summary.record_store(
        "A2",
        StoreOutcome(status="warning", message="mismatch"),
        orders_result=StoreReport(
            status="warning",
            threshold_verdict={
                "pass": False,
                "reason_codes": ["orders:row_count_delta_exceeded", "sales:status_mismatch_exceeded"],
            },
            api_ready=False,
            consecutive_pass_windows=0,
        ),
    )

    record = summary.build_record(finished_at=datetime(2024, 1, 6, tzinfo=timezone.utc))
    daily = record["metrics_json"]["daily_reconciliation"]
    assert daily["passed_stores"] == ["A1"]
    assert daily["failed_stores"][0]["store_code"] == "A2"
    assert daily["stores_passed"] == ["A1"]
    assert daily["stores_failed"][0]["store_code"] == "A2"
    assert "orders:row_count_delta_exceeded" in daily["top_mismatch_reasons"]


def test_compare_rows_use_transaction_rows_not_warning_rows() -> None:
    report = StoreReport(
        status="ok",
        warning_rows=[{"order_number": "W1"}],
        compare_rows_orders=[{"order_number": "O1"}, {"order_number": "O2"}, {"order_number": "O3"}],
    )

    compare_rows = _resolve_compare_rows(report, dataset="orders")

    assert len(compare_rows) == 3
    assert len(report.warning_rows) == 1


def test_compare_row_count_diagnostics_reports_compare_and_warning_counts() -> None:
    orders_report = StoreReport(
        status="ok",
        warning_rows=[{"order_number": "W1"}],
        compare_rows_orders=[{"order_number": "O1"}, {"order_number": "O2"}],
    )
    sales_report = StoreReport(
        status="ok",
        warning_rows=[{"order_number": "SW1"}, {"order_number": "SW2"}],
        compare_rows_sales=[{"order_number": "S1"}, {"order_number": "S2"}, {"order_number": "S3"}],
    )

    diagnostics = _compare_row_count_diagnostics(orders_report, sales_report)

    assert diagnostics == {
        "orders_rows_for_compare": 2,
        "orders_warning_rows": 1,
        "sales_rows_for_compare": 3,
        "sales_warning_rows": 2,
    }



def test_filter_non_order_summary_rows_removes_total_order_footer_rows() -> None:
    rows = [
        {"Order Number": "1001", "Payment Date": "2026-02-01"},
        {"Order Number": "1002", "Payment Date": "2026-02-01"},
        {"Order Number": "Total Order", "Payment Date": "2026-02-01", "Amount": "200"},
        {"Order Number": "", "label": "Total order summary", "Amount": "200"},
    ]

    filtered, filtered_count = _filter_non_order_summary_rows(rows)

    assert filtered_count == 2
    assert [row["Order Number"] for row in filtered] == ["1001", "1002"]


def test_normalized_orders_and_sales_verdicts_pass_when_only_summary_rows_differ() -> None:
    orders_ui = [
        {"order_number": "1001"},
        {"order_number": "1002"},
        {"order_number": "Total Order", "amount": "200"},
    ]
    orders_api = [
        {"order_number": "1001"},
        {"order_number": "1002"},
    ]
    sales_ui = [
        {"order_number": "1001", "payment_mode": "Cash"},
        {"order_number": "1001", "payment_mode": "UPI"},
        {"order_number": "1002", "payment_mode": "Cash"},
        {"order_number": "total order", "amount": "300"},
    ]
    sales_api = [
        {"order_number": "1001", "payment_mode": "Cash"},
        {"order_number": "1001", "payment_mode": "UPI"},
        {"order_number": "1002", "payment_mode": "Cash"},
    ]

    normalized_orders_ui, _ = _filter_non_order_summary_rows(orders_ui)
    normalized_orders_api, _ = _filter_non_order_summary_rows(orders_api)
    normalized_sales_ui, _ = _filter_non_order_summary_rows(sales_ui)
    normalized_sales_api, _ = _filter_non_order_summary_rows(sales_api)

    orders_verdict = _build_dataset_order_set_verdict(
        dataset="orders",
        ui_rows=normalized_orders_ui,
        api_rows=normalized_orders_api,
    )
    sales_verdict = _build_sales_order_row_count_verdict(
        ui_rows=normalized_sales_ui,
        api_rows=normalized_sales_api,
    )

    assert orders_verdict["pass"] is True
    assert orders_verdict["order_number_set_equal"] is True
    assert sales_verdict["pass"] is True
    assert sales_verdict["order_number_set_equal"] is True
    assert sales_verdict["per_order_row_count_equal"] is True


def test_sales_verdict_fails_when_per_order_row_counts_differ() -> None:
    ui_rows = [
        {"order_number": "1001", "payment_mode": "Cash"},
        {"order_number": "1001", "payment_mode": "UPI"},
    ]
    api_rows = [
        {"order_number": "1001", "payment_mode": "Cash"},
    ]

    verdict = _build_sales_order_row_count_verdict(ui_rows=ui_rows, api_rows=api_rows)

    assert verdict["pass"] is False
    assert verdict["order_number_set_equal"] is True
    assert verdict["per_order_row_count_equal"] is False
    assert "sales:per_order_row_count_mismatch" in verdict["reason_codes"]
