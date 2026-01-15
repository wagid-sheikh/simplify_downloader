from __future__ import annotations

from datetime import date, datetime, timezone

from jinja2 import Template

from app.dashboard_downloader.notifications import _build_td_orders_context


TEMPLATE_BODY = """
{{ (summary_text or td_summary_text) }}
{% if not (summary_text or td_summary_text) %}
TD Orders & Sales Run Summary
Run ID: {{ run_id }} | Env: {{ run_env }}
Report Date: {{ report_date }}
Started (Asia/Kolkata): {{ started_at_formatted }}
Finished (Asia/Kolkata): {{ finished_at_formatted }}
{% if total_time_taken %}Total Duration: {{ total_time_taken }}{% endif %}
Overall Status: {{ td_overall_status or overall_status }} (Orders: {{ orders_status }}, Sales: {{ sales_status }})

**Per Store Orders Metrics:**
{% for store in stores %}
- {{ store.store_code or 'UNKNOWN' }} — {{ (store.orders_status or 'unknown')|upper }}
  rows_downloaded: {{ store.orders_rows_downloaded or 0 }}
  rows_ingested: {{ store.orders_rows_ingested or store.orders_final_rows or store.orders_staging_rows or 0 }}
  warning_count: {{ store.orders_warning_count or 0 }}
  dropped_count: {{ store.orders_dropped_rows_count or 0 }}
{% endfor %}

**Per Store Sales Metrics:**
{% for store in stores %}
- {{ store.store_code or 'UNKNOWN' }} — {{ (store.sales_status or 'unknown')|upper }}
  rows_downloaded: {{ store.sales_rows_downloaded or 0 }}
  rows_ingested: {{ store.sales_rows_ingested or store.sales_final_rows or store.sales_staging_rows or 0 }}
  warning_count: {{ store.sales_warning_count or 0 }}
  dropped_count: {{ store.sales_dropped_rows_count or 0 }}
  edited_count: {{ store.sales_rows_edited or 0 }}
  duplicate_count: {{ store.sales_rows_duplicate or 0 }}
{% endfor %}
{% if td_all_stores_failed %}
All TD stores failed for Orders and Sales.
{% endif %}
{% endif %}
"""


def _sample_run_data(*, summary_text: str = "") -> dict[str, object]:
    started_at = datetime(2024, 1, 5, 5, 0, tzinfo=timezone.utc)
    finished_at = datetime(2024, 1, 5, 5, 5, tzinfo=timezone.utc)
    notification_payload = {
        "overall_status": "warning",
        "orders_status": "warning",
        "sales_status": "warning",
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "total_time_taken": "00:05:00",
        "stores": [
            {
                "store_code": "A1",
                "status": "ok",
                "orders": {
                    "status": "ok",
                    "rows_downloaded": 12,
                    "rows_ingested": 10,
                    "warning_count": 1,
                    "dropped_rows_count": 0,
                    "warning_rows": [
                        {
                            "store_code": "A1",
                            "order_number": "ORD-1",
                            "ingest_remarks": "warning row remark",
                            "values": {"Order No.": "ORD-1"},
                        }
                    ],
                    "dropped_rows": [
                        {
                            "store_code": "A1",
                            "order_number": "ORD-2",
                            "ingest_remarks": "drop row remark",
                            "values": {"Order No.": "ORD-2"},
                        }
                    ],
                },
                "sales": {
                    "status": "warning",
                    "rows_downloaded": 8,
                    "rows_ingested": 7,
                    "warning_count": 2,
                    "dropped_rows_count": 1,
                    "edited_rows_count": 1,
                    "duplicate_rows_count": 0,
                    "warning_rows": [
                        {
                            "store_code": "A1",
                            "order_number": "S-1",
                            "ingest_remarks": "sales warning row remark",
                            "values": {"Order Number": "S-1"},
                        }
                    ],
                    "dropped_rows": [
                        {
                            "store_code": "A1",
                            "order_number": "S-2",
                            "ingest_remarks": "sales drop row remark",
                            "values": {"Order Number": "S-2"},
                        }
                    ],
                    "edited_rows": [
                        {
                            "store_code": "A1",
                            "order_number": "S-3",
                            "values": {"Order Number": "S-3"},
                        }
                    ],
                    "duplicate_rows": [
                        {
                            "store_code": "A1",
                            "order_number": "S-4",
                            "values": {"Order Number": "S-4"},
                        }
                    ],
                },
            },
            {
                "store_code": "A2",
                "status": "error",
                "orders": {
                    "status": "error",
                    "rows_downloaded": 0,
                    "rows_ingested": 0,
                    "warning_count": 0,
                    "dropped_rows_count": 0,
                },
                "sales": {
                    "status": "error",
                    "rows_downloaded": 0,
                    "rows_ingested": 0,
                    "warning_count": 0,
                    "dropped_rows_count": 0,
                },
            },
        ],
    }
    return {
        "run_id": "run-td-1",
        "run_env": "test",
        "report_date": date(2024, 1, 5),
        "overall_status": "warning",
        "total_time_taken": "00:05:00",
        "metrics_json": {"notification_payload": notification_payload},
        "started_at": started_at,
        "finished_at": finished_at,
        "summary_text": summary_text,
    }


def test_td_context_prefers_structured_summary_text() -> None:
    summary_text = "TD structured summary text"
    run_data = _sample_run_data(summary_text=summary_text)

    context = _build_td_orders_context(run_data)

    assert context["summary_text"] == summary_text
    assert context["td_all_stores_failed"] is False
    assert context["started_at_formatted"] == "05-01-2024 10:30:00"
    assert context["orders_status"] == "success_with_warnings"
    assert context["sales_status"] == "success_with_warnings"


def test_td_template_renders_payload_without_false_failure_note() -> None:
    run_data = _sample_run_data(summary_text="")
    context = _build_td_orders_context(run_data)
    render_context = {
        **context,
        "run_id": run_data["run_id"],
        "run_env": run_data["run_env"],
        "report_date": run_data["report_date"].isoformat(),
    }

    body = Template(TEMPLATE_BODY).render(**render_context)

    assert "TD Orders & Sales Run Summary" in body
    assert "rows_downloaded: 12" in body
    assert "rows_ingested: 10" in body
    assert "warning_count: 1" in body
    assert "edited_count: 1" in body
    assert "duplicate_count: 0" in body
    assert "05-01-2024 10:30:00" in body
    assert "All TD stores failed" not in body


def test_td_context_includes_store_metadata_on_row_details() -> None:
    run_data = _sample_run_data(summary_text="")

    context = _build_td_orders_context(run_data)
    store = context["stores"][0]

    orders_warning_row = store["orders_warning_rows"][0]
    orders_dropped_row = store["orders_dropped_rows"][0]
    sales_warning_row = store["sales_warning_rows"][0]
    sales_dropped_row = store["sales_dropped_rows"][0]
    sales_edited_row = store["sales_edited_rows"][0]
    sales_duplicate_row = store["sales_duplicate_rows"][0]

    assert orders_warning_row["store_code"] == "A1"
    assert orders_warning_row["order_number"] == "ORD-1"
    assert orders_warning_row["ingest_remarks"] == "warning row remark"

    assert orders_dropped_row["store_code"] == "A1"
    assert orders_dropped_row["order_number"] == "ORD-2"
    assert orders_dropped_row["ingest_remarks"] == "drop row remark"

    assert sales_warning_row["store_code"] == "A1"
    assert sales_warning_row["order_number"] == "S-1"
    assert sales_warning_row["ingest_remarks"] == "sales warning row remark"

    assert sales_dropped_row["store_code"] == "A1"
    assert sales_dropped_row["order_number"] == "S-2"
    assert sales_dropped_row["ingest_remarks"] == "sales drop row remark"

    assert sales_edited_row["store_code"] == "A1"
    assert sales_edited_row["order_number"] == "S-3"

    assert sales_duplicate_row["store_code"] == "A1"
    assert sales_duplicate_row["order_number"] == "S-4"
