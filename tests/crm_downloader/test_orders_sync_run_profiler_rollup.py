from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

import app.crm_downloader.orders_sync_run_profiler.main as profiler
from app.crm_downloader.orders_sync_run_profiler.main import (
    _accumulate_ingestion_totals,
    _build_profiler_summary_text,
    _extract_ingestion_counts_from_log,
    _extract_ingestion_counts_from_summary,
    _extract_uc_warning_count_from_summary,
    _init_row_facts,
    _init_status_counts,
    _merge_ingestion_counts,
    _merge_status_counts,
    _resolve_window_outcome_status,
    _rollup_overall_status,
    _select_summary_overall_status,
    _should_fail_cli_for_status,
    OrdersSyncProfilerFailedStatus,
    StoreProfile,
    StoreRunResult,
)



def test_fail_on_failed_status_flag_defaults_to_non_breaking(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ORDERS_SYNC_PROFILER_FAIL_ON_FAILED_STATUS", raising=False)

    assert _should_fail_cli_for_status("failed") is False


def test_fail_on_failed_status_flag_only_breaks_failed_status(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORDERS_SYNC_PROFILER_FAIL_ON_FAILED_STATUS", "1")

    assert _should_fail_cli_for_status("failed") is True
    assert _should_fail_cli_for_status("success_with_warnings") is False
    assert _should_fail_cli_for_status("success") is False


async def _run_profiler_with_failed_store(
    monkeypatch: pytest.MonkeyPatch, calls: dict[str, list]
) -> None:
    persisted_summaries = calls["persisted_summaries"]
    missing_log_calls = calls["missing_log_calls"]
    notification_calls = calls["notification_calls"]

    monkeypatch.setattr(
        profiler,
        "config",
        SimpleNamespace(database_url="sqlite+aiosqlite:///:memory:", run_env="test"),
    )
    monkeypatch.setattr(
        profiler,
        "get_logger",
        lambda **kwargs: profiler.JsonLogger(run_id=kwargs.get("run_id"), log_file_path=None),
    )

    async def fake_fetch_pipeline_id(**_kwargs: object) -> int:
        return 101

    async def fake_load_store_profiles(**_kwargs: object) -> list[StoreProfile]:
        return [
            StoreProfile(
                store_code="S001",
                store_name="Store 001",
                cost_center="CC001",
                sync_config={},
                start_date=None,
            )
        ]

    async def fake_process_store(**_kwargs: object) -> StoreRunResult:
        status_counts = profiler._init_status_counts()
        status_counts["failed"] = 1
        return StoreRunResult(
            store_code="S001",
            pipeline_group="TD",
            pipeline_name="td_orders_sync",
            cost_center="CC001",
            overall_status="failed",
            window_count=1,
            windows=[(date(2024, 1, 1), date(2024, 1, 2))],
            status_counts=status_counts,
            window_audit=[
                {
                    "from_date": "2024-01-01",
                    "to_date": "2024-01-02",
                    "status": "failed",
                    "status_note": "window execution failed",
                }
            ],
            ingestion_totals=profiler._init_ingestion_totals(),
            row_facts=_init_row_facts(),
        )

    async def fake_insert_run_summary(_database_url: str, summary_record: dict) -> None:
        persisted_summaries.append(summary_record)

    async def fake_persist_missing_windows_log_rows(**kwargs: object) -> None:
        missing_log_calls.append(dict(kwargs))

    async def fake_send_notifications_for_run(pipeline_name: str, run_id: str) -> dict:
        assert persisted_summaries, "notifications must run after summary persistence"
        notification_calls.append((pipeline_name, run_id))
        return {"emails_planned": 1, "emails_sent": 1, "errors": []}

    monkeypatch.setattr(profiler, "_fetch_pipeline_id", fake_fetch_pipeline_id)
    monkeypatch.setattr(profiler, "_load_store_profiles", fake_load_store_profiles)
    monkeypatch.setattr(profiler, "_process_store", fake_process_store)
    monkeypatch.setattr(profiler, "insert_run_summary", fake_insert_run_summary)
    monkeypatch.setattr(
        profiler, "_persist_missing_windows_log_rows", fake_persist_missing_windows_log_rows
    )
    monkeypatch.setattr(profiler, "send_notifications_for_run", fake_send_notifications_for_run)

    await profiler.main(
        sync_group="TD",
        store_codes=None,
        max_workers=1,
        run_env="test",
        run_id="profiler-failed-run",
    )


@pytest.mark.asyncio
async def test_failed_profiler_status_remains_non_breaking_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ORDERS_SYNC_PROFILER_FAIL_ON_FAILED_STATUS", raising=False)

    calls = {"persisted_summaries": [], "missing_log_calls": [], "notification_calls": []}
    await _run_profiler_with_failed_store(monkeypatch, calls)

    assert calls["persisted_summaries"][0]["overall_status"] == "failed"
    assert calls["notification_calls"] == [("orders_sync_run_profiler", "profiler-failed-run")]


@pytest.mark.asyncio
async def test_failed_profiler_status_can_fail_cli_after_persistence_and_notifications(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ORDERS_SYNC_PROFILER_FAIL_ON_FAILED_STATUS", "1")

    calls = {"persisted_summaries": [], "missing_log_calls": [], "notification_calls": []}
    with pytest.raises(OrdersSyncProfilerFailedStatus):
        await _run_profiler_with_failed_store(monkeypatch, calls)

    assert calls["persisted_summaries"][0]["overall_status"] == "failed"
    assert calls["notification_calls"] == [("orders_sync_run_profiler", "profiler-failed-run")]


def test_window_status_prefers_summary_success_with_warnings() -> None:
    status, note = _resolve_window_outcome_status(
        raw_status="success",
        summary_overall_status="success_with_warnings",
    )

    assert status == "success_with_warnings"
    assert "overall_status=success_with_warnings" in note


def test_status_counts_roll_up_success_with_warnings_for_mixed_statuses() -> None:
    td_counts = _init_status_counts()
    uc_counts = _init_status_counts()
    total_counts = _init_status_counts()

    _merge_status_counts(td_counts, {"success": 1, "warning": 2, "failed": 1})
    _merge_status_counts(uc_counts, {"success_with_warnings": 1, "success": 3})

    _merge_status_counts(total_counts, td_counts)
    _merge_status_counts(total_counts, uc_counts)

    assert td_counts["success_with_warnings"] == 2
    assert uc_counts["success_with_warnings"] == 1
    assert total_counts == {
        "success": 4,
        "success_with_warnings": 3,
        "partial": 0,
        "failed": 1,
        "skipped": 0,
    }


def test_rollup_status_precedence_for_mixed_statuses() -> None:
    with_failure = {
        "success": 2,
        "success_with_warnings": 2,
        "partial": 0,
        "failed": 1,
        "skipped": 0,
    }
    warning_only = {
        "success": 2,
        "success_with_warnings": 2,
        "partial": 0,
        "failed": 0,
        "skipped": 0,
    }

    assert _rollup_overall_status(with_failure) == "failed"
    assert _rollup_overall_status(warning_only) == "success_with_warnings"


def test_ingestion_counts_merge_uses_summary_final_rows_for_totals() -> None:
    log_row = {
        "primary_rows_downloaded": 120,
        "primary_rows_ingested": 0,
        "primary_staging_rows": 50,
        "primary_staging_inserted": 10,
        "primary_staging_updated": 5,
        "primary_final_inserted": 7,
        "primary_final_updated": 11,
        "secondary_rows_downloaded": 60,
        "secondary_rows_ingested": 0,
        "secondary_staging_rows": 20,
        "secondary_staging_inserted": 3,
        "secondary_staging_updated": 2,
        "secondary_final_inserted": 4,
        "secondary_final_updated": 6,
    }
    summary = {
        "metrics_json": {
            "orders": {
                "stores": {
                    "TEST": {
                        "rows_downloaded": 120,
                        "rows_ingested": 18,
                        "staging_rows": 50,
                        "final_rows": 18,
                        "staging_inserted": 10,
                        "staging_updated": 5,
                        "final_inserted": 7,
                        "final_updated": 11,
                    }
                }
            },
            "sales": {
                "stores": {
                    "TEST": {
                        "rows_downloaded": 60,
                        "rows_ingested": 10,
                        "staging_rows": 20,
                        "final_rows": 10,
                        "staging_inserted": 3,
                        "staging_updated": 2,
                        "final_inserted": 4,
                        "final_updated": 6,
                    }
                }
            },
        }
    }

    ingestion_from_log = _extract_ingestion_counts_from_log(log_row, pipeline_name="td_orders_sync")
    ingestion_from_summary = _extract_ingestion_counts_from_summary(
        summary, store_code="test", pipeline_name="td_orders_sync"
    )
    merged = _merge_ingestion_counts(ingestion_from_log, ingestion_from_summary)

    totals = {
        "rows_downloaded": 0,
        "rows_ingested": 0,
        "staging_rows": 0,
        "final_rows": 0,
        "staging_inserted": 0,
        "staging_updated": 0,
        "final_inserted": 0,
        "final_updated": 0,
    }
    _accumulate_ingestion_totals(totals, merged)

    assert totals["staging_rows"] == 70
    assert totals["final_rows"] == 28
    assert totals["final_inserted"] == 11
    assert totals["final_updated"] == 17


def test_summary_overall_status_is_success_when_all_windows_clean() -> None:
    status_counts = {
        "success": 5,
        "success_with_warnings": 0,
        "partial": 0,
        "failed": 0,
        "skipped": 0,
    }

    assert _select_summary_overall_status(status_counts) == "success"


def test_summary_overall_status_promotes_warning_windows() -> None:
    status_counts = {
        "success": 4,
        "success_with_warnings": 2,
        "partial": 0,
        "failed": 0,
        "skipped": 0,
    }

    assert _select_summary_overall_status(status_counts) == "success_with_warnings"


def test_uc_ingestion_totals_do_not_double_count_gst_and_include_final_rows() -> None:
    summary_window_1 = {
        "metrics_json": {
            "stores_summary": {
                "stores": {
                    "TEST": {
                        "rows_downloaded": 9,
                        "rows_ingested": 9,
                        "final_rows": 9,
                    }
                }
            }
        }
    }
    summary_window_2 = {
        "metrics_json": {
            "stores_summary": {
                "stores": {
                    "TEST": {
                        "rows_downloaded": 14,
                        "rows_ingested": 14,
                        "final_rows": 14,
                    }
                }
            }
        }
    }

    totals = {
        "rows_downloaded": 0,
        "rows_ingested": 0,
        "staging_rows": 0,
        "final_rows": 0,
        "staging_inserted": 0,
        "staging_updated": 0,
        "final_inserted": 0,
        "final_updated": 0,
    }

    for summary in (summary_window_1, summary_window_2):
        counts = _extract_ingestion_counts_from_summary(
            summary, store_code="test", pipeline_name="uc_orders_sync"
        )
        _accumulate_ingestion_totals(totals, counts)

    assert totals["rows_downloaded"] == 23
    assert totals["rows_ingested"] == 23
    assert totals["final_rows"] == 23
    assert totals["rows_ingested"] <= totals["rows_downloaded"]


def test_extract_uc_warning_count_from_summary() -> None:
    summary = {
        "metrics_json": {
            "stores_summary": {
                "stores": {
                    "TEST": {
                        "warning_count": 7,
                    }
                }
            }
        }
    }

    assert _extract_uc_warning_count_from_summary(summary, store_code="test") == 7
    assert _extract_uc_warning_count_from_summary(summary, store_code="missing") == 0


def test_accumulate_ingestion_totals_supports_flat_totals_payload() -> None:
    totals = {
        "rows_downloaded": 0,
        "rows_ingested": 0,
        "staging_rows": 0,
        "final_rows": 0,
        "staging_inserted": 0,
        "staging_updated": 0,
        "final_inserted": 0,
        "final_updated": 0,
    }

    _accumulate_ingestion_totals(
        totals,
        {
            "rows_downloaded": 73,
            "rows_ingested": 73,
            "staging_rows": 73,
            "final_rows": 73,
            "staging_inserted": 0,
            "staging_updated": 0,
            "final_inserted": 0,
            "final_updated": 0,
        },
    )

    assert totals == {
        "rows_downloaded": 73,
        "rows_ingested": 73,
        "staging_rows": 73,
        "final_rows": 73,
        "staging_inserted": 0,
        "staging_updated": 0,
        "final_inserted": 0,
        "final_updated": 0,
    }


def test_profiler_top_level_status_failed_when_any_window_fails() -> None:
    status_counts = {
        "success": 3,
        "success_with_warnings": 1,
        "partial": 0,
        "failed": 1,
        "skipped": 0,
    }

    assert _rollup_overall_status(status_counts) == "failed"
    assert _select_summary_overall_status(status_counts) == "failed"


def test_profiler_summary_text_includes_failed_uc_window_reason() -> None:
    cert_error = "Page.goto: net::ERR_CERT_DATE_INVALID at https://example.test/orders\n    stack details"

    summary = _build_profiler_summary_text(
        run_id="profiler-run-1",
        run_env="test",
        started_at=datetime(2024, 1, 5, 5, 0, tzinfo=timezone.utc),
        finished_at=datetime(2024, 1, 5, 5, 1, tzinfo=timezone.utc),
        overall_status="failed",
        store_entries=[
            {
                "store_code": "UC01",
                "pipeline_name": "uc_orders_sync",
                "status": "failed",
                "window_count": 1,
                "primary_metrics": {},
                "secondary_metrics": {},
                "window_audit": [
                    {
                        "from_date": "2024-01-01",
                        "to_date": "2024-01-02",
                        "status": "failed",
                        "status_note": "window execution failed",
                        "error_message": cert_error,
                    }
                ],
            }
        ],
        window_summary={"completed_windows": 0, "expected_windows": 1, "missing_windows": 0},
        warnings=[],
    )

    assert "failed_windows:" in summary
    assert "2024-01-01 to 2024-01-02" in summary
    assert "status=failed" in summary
    assert "status_note=window execution failed" in summary
    assert "Page.goto: net::ERR_CERT_DATE_INVALID" in summary
    assert "stack details" in summary
