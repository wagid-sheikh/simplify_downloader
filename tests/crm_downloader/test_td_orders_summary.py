from __future__ import annotations

from datetime import date, datetime, timezone

from app.crm_downloader.td_orders_sync.main import StoreOutcome, StoreReport, TdOrdersDiscoverySummary


def test_summary_text_lists_ingest_remarks() -> None:
    summary = TdOrdersDiscoverySummary(run_id="run-1", run_env="test", report_date=date(2024, 1, 1))
    summary.add_ingest_remarks(
        [
            {"store_code": "a1", "order_number": "123", "ingest_remarks": "invalid phone"},
            {"store_code": "a2", "order_number": "456", "ingest_remarks": "missing date"},
        ]
    )

    text = summary.summary_text()

    assert "Ingest remarks:" in text
    assert "- A1 123: invalid phone" in text

    record = summary.build_record(finished_at=datetime(2024, 1, 1, tzinfo=timezone.utc))
    ingest_metrics = record["metrics_json"]["ingest_remarks"]
    assert ingest_metrics["total"] == 2
    assert ingest_metrics["rows"][0]["store_code"] == "A1"


def test_summary_ingest_remarks_truncate_notice() -> None:
    summary = TdOrdersDiscoverySummary(run_id="run-2", run_env="test", report_date=date(2024, 1, 2))
    long_remark = "x" * 205
    remarks = [{"store_code": "a1", "order_number": "1", "ingest_remarks": long_remark}]
    remarks.extend(
        {"store_code": f"a{i}", "order_number": str(i), "ingest_remarks": "note"} for i in range(2, 56)
    )
    summary.add_ingest_remarks(remarks)

    text = summary.summary_text()

    assert "... additional 5 remarks truncated" in text
    assert "…" in text.splitlines()[text.splitlines().index("Ingest remarks:") + 1]


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

    text = summary.summary_text()
    assert "Orders results:" in text
    assert "- A1: WARNING | rows: staging=2, final=2 | files: orders_A1.xlsx | warnings: duplicate rows dropped" in text
    assert "- A1: OK | rows: staging=3, final=3 | files: sales_A1.xlsx | Sales ingested: staging=3, final=3" in text


def test_sales_warnings_do_not_downgrade_status() -> None:
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
    assert sales_metrics["overall_status"] == "ok"

    text_lines = summary.summary_text().splitlines()
    sales_section_index = text_lines.index("Sales results:")
    sales_lines = [line for line in text_lines[sales_section_index + 1 :] if line.startswith("- A2:")]
    sales_line = sales_lines[0]
    assert "warnings: Phone value '12345' is invalid and was dropped" in sales_line
