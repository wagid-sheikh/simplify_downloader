from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from app.crm_downloader.td_orders_sync.main import ROW_SAMPLE_LIMIT, StoreOutcome, StoreReport, TdOrdersDiscoverySummary


def test_summary_text_lists_ingest_remarks() -> None:
    summary = TdOrdersDiscoverySummary(run_id="run-1", run_env="test", report_date=date(2024, 1, 1))
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
    summary = TdOrdersDiscoverySummary(run_id="run-2", run_env="test", report_date=date(2024, 1, 2))
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
    summary = TdOrdersDiscoverySummary(run_id="run-3", run_env="test", report_date=date(2024, 1, 3))
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
    assert orders_metrics["overall_status"] == "warning"
    assert sales_metrics["overall_status"] == "ok"
    assert orders_metrics["stores"]["A1"]["filenames"] == ["orders_A1.xlsx"]
    assert sales_metrics["stores"]["A1"]["final_rows"] == 3

    text_lines = summary.summary_text(finished_at=datetime(2024, 1, 3, tzinfo=timezone.utc)).splitlines()
    assert "**Per Store Orders Metrics:**" in text_lines
    assert any(line.startswith("- A1 — WARNING") for line in text_lines)
    assert any("warning_count: 1" in line for line in text_lines)
    assert any(line.startswith("- A1 — OK") for line in text_lines)


def test_sales_warnings_reflected_in_summary() -> None:
    summary = TdOrdersDiscoverySummary(run_id="run-4", run_env="test", report_date=date(2024, 1, 4))
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
    assert sales_metrics["overall_status"] == "warning"

    text_lines = summary.summary_text(finished_at=datetime(2024, 1, 4, tzinfo=timezone.utc)).splitlines()
    assert any(line.startswith("- A2 — OK") for line in text_lines)
    assert any("warning_count: 1" in line for line in text_lines)


def test_summary_text_filters_row_fields_and_truncates_samples() -> None:
    summary = TdOrdersDiscoverySummary(run_id="run-5", run_env="test", report_date=date(2024, 1, 5))
    warning_rows = [
        {"values": {"store_code": "A1", "order_number": "W1", "ingest_remarks": "bad W1", "phone": "111"}},
        {"values": {"store_code": "A1", "order_number": "W2", "ingest_remarks": "bad W2", "email": "x@example.com"}},
        {"values": {"store_code": "A1", "order_number": "W3", "ingest_remarks": "bad W3", "customer": "Alice"}},
        {"values": {"store_code": "A1", "order_number": "W4", "ingest_remarks": "bad W4", "customer": "Bob"}},
    ]
    dropped_rows = [
        {"values": {"store_code": "A1", "order_number": "D1", "ingest_remarks": "drop 1", "address": "hidden"}},
        {"values": {"store_code": "A1", "order_number": "D2", "ingest_remarks": "drop 2", "mobile_number": "000"}},
    ]
    edited_rows = [
        {"values": {"store_code": "A1", "order_number": "E1", "payment_mode": "cash"}},
        {"values": {"store_code": "A1", "order_number": "E2", "adjustment": "1.23"}},
    ]
    duplicate_rows = [
        {"values": {"store_code": "A1", "order_number": "DU1", "notes": "dup 1"}},
        {"values": {"store_code": "A1", "order_number": "DU2", "notes": "dup 2"}},
        {"values": {"store_code": "A1", "order_number": "DU3", "notes": "dup 3"}},
        {"values": {"store_code": "A1", "order_number": "DU4", "notes": "dup 4"}},
    ]
    summary.record_store(
        "A1",
        StoreOutcome(status="warning", message="Data warnings"),
        orders_result=StoreReport(status="warning", warning_rows=warning_rows, dropped_rows=dropped_rows),
        sales_result=StoreReport(status="warning", edited_rows=edited_rows, duplicate_rows=duplicate_rows),
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
    duplicate_lines = _collect_row_lines("  duplicate rows:")

    assert len(warning_lines) == ROW_SAMPLE_LIMIT + 1
    assert warning_lines[-1] == "- …truncated"
    assert all("ingest_remarks=" in line for line in warning_lines[:-1])
    assert all("phone" not in line and "email" not in line and "customer" not in line for line in warning_lines)

    assert all("ingest_remarks=" in line for line in dropped_lines)
    assert all("mobile_number" not in line and "address" not in line for line in dropped_lines)

    assert all("payment_mode" not in line and "adjustment" not in line for line in edited_lines)
    assert all("ingest_remarks" not in line for line in edited_lines)
    assert all("store_code=" in line and "order_number=" in line for line in edited_lines)

    assert len(duplicate_lines) == ROW_SAMPLE_LIMIT + 1
    assert duplicate_lines[-1] == "- …truncated"
    assert all("notes" not in line for line in duplicate_lines)
    assert all("ingest_remarks" not in line for line in duplicate_lines)
    assert all("store_code=" in line and "order_number=" in line for line in duplicate_lines[:-1])
