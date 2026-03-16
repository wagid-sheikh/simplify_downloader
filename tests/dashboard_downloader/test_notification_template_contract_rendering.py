from __future__ import annotations

from datetime import date

from jinja2 import Template

from app.dashboard_downloader.notifications import _build_td_orders_context, _build_uc_orders_context


FINAL_SUBJECT_TEMPLATE = (
    "ETL - [{{ env_upper }}][{{ overall_status_upper }}][{{ store_code }}] "
    "{{ pipeline_display_name }} – {{ run_date_display }}"
)

FINAL_BODY_TEMPLATE = """PIPELINE RUN SUMMARY
- pipeline: {{ pipeline_display_name or pipeline_code or 'n/a' }}
- run_id: {{ run_id or 'n/a' }}
- env: {{ env_upper or run_env or env or 'n/a' }}
- report_date: {{ run_date_display or report_date or 'n/a' }}
- started_at: {{ started_at_ist or started_at_formatted or started_at or 'n/a' }}
- finished_at: {{ finished_at_ist or finished_at_formatted or finished_at or 'n/a' }}
- duration: {{ total_time_taken or 'n/a' }}
- overall_status: {{ overall_status_upper or overall_status_label or overall_status or 'unknown' }}

WINDOW STATUS
- windows_completed: {{ completed_windows or 0 }} / {{ expected_windows or 0 }}
- missing_windows: {{ missing_windows or 0 }}
{% if missing_window_stores %}- missing_window_stores: {{ missing_window_stores | join(', ') }}
{% endif %}

STORE PROCESSING SUMMARY
{{ store_processing_summary_block or '- (none)' }}

FILES PROCESSED
{{ files_processed_block or '- (none)' }}

WARNINGS
{{ warnings_block or '- (none)' }}{% if optional_notes_block %}

NOTES
{{ optional_notes_block }}{% endif %}
"""


def _render(context: dict[str, object]) -> tuple[str, str]:
    subject = Template(FINAL_SUBJECT_TEMPLATE).render(**context)
    body = Template(FINAL_BODY_TEMPLATE).render(**context)
    return subject, body


def test_final_contract_renders_uc_success() -> None:
    run_data = {
        "run_id": "run-uc-success",
        "run_env": "prod",
        "report_date": "2024-01-05",
        "overall_status": "success",
        "total_time_taken": "00:03:00",
        "metrics_json": {
            "window_summary": {"completed_windows": 1, "expected_windows": 1, "missing_windows": 0},
            "window_audit": [],
            "notification_payload": {
                "overall_status": "success",
                "started_at": "2024-01-05T05:00:00+00:00",
                "finished_at": "2024-01-05T05:03:00+00:00",
                "stores": [{"store_code": "UC01", "status": "ok", "filename": "UC01.xlsx"}],
            },
        },
    }
    context = _build_uc_orders_context(run_data)
    subject, body = _render(context)

    assert subject == "ETL - [PROD][SUCCESS][UC01] UC Orders Sync – 05-Jan-2024"
    assert "PIPELINE RUN SUMMARY" in body
    assert "WINDOW STATUS" in body
    assert "STORE PROCESSING SUMMARY" in body
    assert "FILES PROCESSED" in body
    assert "WARNINGS" in body
    assert "NOTES" in body


def test_final_contract_renders_td_success() -> None:
    run_data = {
        "run_id": "run-td-success",
        "run_env": "stage",
        "report_date": date(2024, 1, 5),
        "overall_status": "success",
        "total_time_taken": "00:05:00",
        "metrics_json": {
            "window_summary": {"completed_windows": 1, "expected_windows": 1, "missing_windows": 0},
            "notification_payload": {
                "overall_status": "success",
                "orders_status": "ok",
                "sales_status": "ok",
                "started_at": "2024-01-05T05:00:00+00:00",
                "finished_at": "2024-01-05T05:05:00+00:00",
                "stores": [{"store_code": "TD01", "status": "ok", "orders": {"status": "ok"}, "sales": {"status": "ok"}}],
            },
        },
    }
    context = _build_td_orders_context(run_data)
    subject, body = _render(context)

    assert subject == "ETL - [STAGE][SUCCESS][TD01] TD Orders Sync – 05-Jan-2024"
    assert "- started_at: 05-Jan-2024 10:30:00 IST" in body
    assert body.index("PIPELINE RUN SUMMARY") < body.index("WINDOW STATUS") < body.index("STORE PROCESSING SUMMARY")


def test_final_contract_renders_warning_case() -> None:
    run_data = {
        "run_id": "run-uc-warning",
        "run_env": "qa",
        "report_date": "2024-01-05",
        "overall_status": "warning",
        "metrics_json": {
            "window_summary": {"completed_windows": 1, "expected_windows": 2, "missing_windows": 1},
            "window_audit": [],
            "notification_payload": {
                "overall_status": "warning",
                "stores": [{"store_code": "UC01", "status": "warning", "warning_rows": [{"order_number": "U1"}]}],
            },
        },
    }
    context = _build_uc_orders_context(run_data)
    subject, body = _render(context)

    assert "[SUCCESS WITH WARNINGS]" in subject
    assert "WARNINGS" in body
    assert "UC_STORE_WARNINGS" in body


def test_final_contract_renders_failed_case_without_debug_rows() -> None:
    run_data = {
        "run_id": "run-td-failed",
        "run_env": "dev",
        "report_date": "2024-01-05",
        "overall_status": "failed",
        "metrics_json": {
            "window_summary": {"completed_windows": 0, "expected_windows": 1, "missing_windows": 1},
            "notification_payload": {
                "overall_status": "failed",
                "orders_status": "error",
                "sales_status": "error",
                "stores": [{"store_code": "TD01", "status": "error", "orders": {"status": "error"}, "sales": {"status": "error"}}],
            },
        },
    }
    context = _build_td_orders_context(run_data)
    subject, body = _render(context)

    assert "[FAILED]" in subject
    assert "ROW_WARNINGS" not in body
    assert "row_level_facts_available" not in body
