from __future__ import annotations

from datetime import date, datetime, timezone

from app.crm_downloader.td_orders_sync.main import TdOrdersDiscoverySummary


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
