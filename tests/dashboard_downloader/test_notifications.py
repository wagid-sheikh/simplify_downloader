from __future__ import annotations

from app.dashboard_downloader.notifications import _build_uc_orders_context, _prepare_ingest_remarks


def test_prepare_ingest_remarks_truncates_rows_and_length() -> None:
    rows = [
        {"store_code": "a001", "order_number": "123", "ingest_remarks": "x" * 15},
        {"store_code": "a002", "order_number": "456", "ingest_remarks": "ok"},
    ]

    cleaned, truncated_rows, truncated_length, ingest_text = _prepare_ingest_remarks(
        rows, max_rows=1, max_chars=10
    )

    assert truncated_rows is True
    assert truncated_length is True
    assert cleaned == [{"store_code": "A001", "order_number": "123", "ingest_remarks": "xxxxxxxxx…"}]
    assert "- A001 123: xxxxxxxxx…" in ingest_text
    assert "... additional 1 remarks truncated" in ingest_text


def test_uc_orders_warning_count_ignores_row_totals() -> None:
    run_data = {
        "metrics_json": {
            "notification_payload": {
                "stores": [
                    {
                        "store_code": "UC100",
                        "status": "ok",
                        "staging_rows": 250,
                        "final_rows": 250,
                        "staging_inserted": 250,
                        "final_inserted": 250,
                    }
                ]
            },
            "ingest_remarks": {"rows": []},
            "stores_summary": {"stores": {"UC100": {"warning_count": None}}},
            "window_audit": [],
        }
    }

    context = _build_uc_orders_context(run_data)

    assert context["stores"][0]["warning_count"] == 0
