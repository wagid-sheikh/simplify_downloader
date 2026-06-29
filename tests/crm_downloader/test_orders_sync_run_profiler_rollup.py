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



def test_profiler_summary_includes_success_with_warnings_window_details() -> None:
    summary = _build_profiler_summary_text(
        run_id="profiler-warning-run",
        run_env="test",
        started_at=datetime(2024, 2, 1, 5, 0, tzinfo=timezone.utc),
        finished_at=datetime(2024, 2, 1, 5, 1, tzinfo=timezone.utc),
        overall_status="success_with_warnings",
        store_entries=[
            {
                "store_code": "UC01",
                "pipeline_name": "uc_orders_sync",
                "status": "success_with_warnings",
                "window_count": 1,
                "primary_metrics": {},
                "secondary_metrics": {},
                "window_audit": [
                    {
                        "store_code": "UC01",
                        "from_date": "2024-02-01",
                        "to_date": "2024-02-02",
                        "status": "success_with_warnings",
                        "status_note": "GSTIN missing for 2 row(s)",
                        "error_message": "non-fatal validation warnings",
                        "attempt_no": 2,
                        "warning_count": 2,
                    }
                ],
            }
        ],
        window_summary={"completed_windows": 1, "expected_windows": 1, "missing_windows": 0},
        warnings=[],
    )

    assert "  warning_windows:" in summary
    assert "warning_reason=non-fatal validation warnings" in summary
    assert "status_note=GSTIN missing for 2 row(s)" in summary
    assert "error_message=non-fatal validation warnings" not in summary
    assert "attempt_no=2" in summary
    assert "warning_count=2" in summary


def test_profiler_summary_renders_td_api_info_as_status_message_not_error() -> None:
    summary = _build_profiler_summary_text(
        run_id="profiler-td-info-run",
        run_env="test",
        started_at=datetime(2024, 2, 1, 5, 0, tzinfo=timezone.utc),
        finished_at=datetime(2024, 2, 1, 5, 1, tzinfo=timezone.utc),
        overall_status="success_with_warnings",
        store_entries=[
            {
                "store_code": "TD01",
                "pipeline_name": "td_orders_sync",
                "status": "success_with_warnings",
                "window_count": 1,
                "primary_metrics": {},
                "secondary_metrics": {},
                "window_audit": [
                    {
                        "store_code": "TD01",
                        "from_date": "2024-02-01",
                        "to_date": "2024-02-02",
                        "status": "success_with_warnings",
                        "status_note": "summary overall_status=success_with_warnings",
                        "status_message": "Sales sourced from API and ingested; API primary path executed",
                        "error_message": None,
                        "attempt_no": 1,
                        "warning_count": 1,
                    }
                ],
            }
        ],
        window_summary={"completed_windows": 1, "expected_windows": 1, "missing_windows": 0},
        warnings=[],
    )

    assert "status_message=Sales sourced from API and ingested; API primary path executed" in summary
    assert "error_message=Sales sourced from API and ingested; API primary path executed" not in summary

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


@pytest.mark.asyncio
async def test_profiler_summary_rolls_store_ingestion_totals_into_uc_pipeline_and_grand_totals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    persisted_summaries: list[dict] = []
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
        return 202

    async def fake_load_store_profiles(**_kwargs: object) -> list[StoreProfile]:
        return [
            StoreProfile(
                store_code="UC01",
                store_name="UC Store 01",
                cost_center="CC-UC01",
                sync_config={},
                start_date=None,
            )
        ]

    async def fake_process_store(**_kwargs: object) -> StoreRunResult:
        status_counts = profiler._init_status_counts()
        status_counts["success"] = 1
        ingestion_totals = profiler._init_ingestion_totals()
        ingestion_totals.update(
            {
                "rows_downloaded": 41,
                "rows_ingested": 39,
                "staging_rows": 39,
                "final_rows": 37,
                "staging_inserted": 31,
                "staging_updated": 8,
                "final_inserted": 29,
                "final_updated": 8,
            }
        )
        return StoreRunResult(
            store_code="UC01",
            pipeline_group="UC",
            pipeline_name="uc_orders_sync",
            cost_center="CC-UC01",
            overall_status="success",
            window_count=1,
            windows=[(date(2024, 3, 1), date(2024, 3, 2))],
            status_counts=status_counts,
            window_audit=[
                {
                    "store_code": "UC01",
                    "from_date": "2024-03-01",
                    "to_date": "2024-03-02",
                    "status": "success",
                    "ingestion_counts": {},
                }
            ],
            ingestion_totals=ingestion_totals,
            row_facts=_init_row_facts(),
        )

    async def fake_insert_run_summary(_database_url: str, summary_record: dict) -> None:
        persisted_summaries.append(summary_record)

    async def fake_persist_missing_windows_log_rows(**_kwargs: object) -> None:
        return None

    async def fake_send_notifications_for_run(_pipeline_name: str, _run_id: str) -> dict:
        return {"emails_planned": 1, "emails_sent": 1, "errors": []}

    monkeypatch.setattr(profiler, "_fetch_pipeline_id", fake_fetch_pipeline_id)
    monkeypatch.setattr(profiler, "_load_store_profiles", fake_load_store_profiles)
    monkeypatch.setattr(profiler, "_process_store", fake_process_store)
    monkeypatch.setattr(profiler, "insert_run_summary", fake_insert_run_summary)
    monkeypatch.setattr(profiler, "_persist_missing_windows_log_rows", fake_persist_missing_windows_log_rows)
    monkeypatch.setattr(profiler, "send_notifications_for_run", fake_send_notifications_for_run)

    await profiler.main(sync_group="UC", max_workers=1, run_env="test", run_id="profiler-uc-rollup")

    metrics = persisted_summaries[0]["metrics_json"]
    expected_totals = {
        "rows_downloaded": 41,
        "rows_ingested": 39,
        "staging_rows": 39,
        "final_rows": 37,
        "staging_inserted": 31,
        "staging_updated": 8,
        "final_inserted": 29,
        "final_updated": 8,
    }

    assert metrics["pipeline_totals"]["UC"]["ingestion_totals"] == expected_totals
    assert metrics["ingestion_grand_totals"] == expected_totals


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


def test_extract_td_garment_warning_from_summary_flags_incomplete() -> None:
    summary = {
        "metrics_json": {
            "orders": {
                "stores": {
                    "TEST": {
                        "garments_fetch_completeness": "incomplete",
                        "garments_final_row_count": 42,
                        "garments_budget_state": "near_limit",
                        "garments_incomplete_reason": {"code": "pagination_budget_exhausted", "message": "pagination budget exhausted"},
                        "page_size_requested": 100,
                        "pages_attempted": 5,
                        "pages_succeeded": 4,
                        "last_successful_page": 4,
                        "reported_total_rows": 600,
                        "reported_total_pages": 6,
                        "parsed_row_count": 400,
                        "unique_row_id_count": 399,
                        "rows_without_identity_count": 1,
                        "identity_strategy": "composite",
                        "stop_reason": "max_pages",
                        "garments_attempted_page_count": 5,
                        "garments_completed_page_count": 4,
                        "garments_expected_page_count": 6,
                        "garments_timeout_count": 1,
                        "garments_retry_count": 2,
                    }
                }
            }
        }
    }

    warning = profiler._extract_td_garment_warning_from_summary(summary, store_code="test")

    assert warning == {
        "garments_fetch_completeness": "incomplete",
        "garments_final_row_count": 42,
        "garments_budget_state": "near_limit",
        "garments_incomplete_reason": {"code": "pagination_budget_exhausted", "message": "pagination budget exhausted"},
        "page_size_requested": 100,
        "pages_attempted": 5,
        "pages_succeeded": 4,
        "last_successful_page": 4,
        "reported_total_rows": 600,
        "reported_total_pages": 6,
        "parsed_row_count": 400,
        "unique_row_id_count": 399,
        "rows_without_identity_count": 1,
        "identity_strategy": "composite",
        "stop_reason": "max_pages",
        "garments_attempted_page_count": 5,
        "garments_completed_page_count": 4,
        "garments_expected_page_count": 6,
        "garments_timeout_count": 1,
        "garments_retry_count": 2,
        "is_incomplete": True,
    }


@pytest.mark.asyncio
async def test_td_profiler_keeps_valid_empty_orders_window_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        profiler,
        "config",
        SimpleNamespace(database_url="sqlite+aiosqlite:///:memory:", run_env="test"),
    )

    async def fake_fetch_last_success_window_end(**_kwargs: object) -> None:
        return None

    async def fake_pipeline_fn(**_kwargs: object) -> None:
        return None

    async def fake_fetch_summary_for_run(_database_url: str, _run_id: str) -> dict:
        return {
            "overall_status": "success",
            "metrics_json": {
                "orders": {
                    "stores": {
                        "TD01": {
                            "rows_downloaded": 0,
                            "rows_ingested": 0,
                            "final_rows": 0,
                            "warnings": [],
                            "warning_rows": [],
                            "garments_fetch_completeness": "complete",
                            "garments_final_row_count": 0,
                            "garments_budget_state": "within_budget",
                        }
                    }
                },
                "sales": {
                    "stores": {
                        "TD01": {
                            "rows_downloaded": 1,
                            "rows_ingested": 1,
                            "final_rows": 1,
                            "warnings": [],
                            "warning_rows": [],
                        }
                    }
                },
                "stores_summary": {
                    "stores": {
                        "TD01": {
                            "status": "ok",
                            "data_ingest_status": "success",
                            "observability_warnings": [],
                        }
                    }
                },
            },
        }

    async def fake_fetch_latest_log_row(**_kwargs: object) -> dict:
        return {
            "id": 1,
            "status": "success",
            "error_message": None,
            "primary_rows_downloaded": 0,
            "primary_rows_ingested": 0,
            "primary_final_rows": 0,
            "secondary_rows_downloaded": 1,
            "secondary_rows_ingested": 1,
            "secondary_final_rows": 1,
        }

    inserted_summaries: list[dict] = []

    async def fake_insert_run_summary(_database_url: str, summary_record: dict) -> None:
        inserted_summaries.append(summary_record)

    monkeypatch.setattr(profiler, "fetch_last_success_window_end", fake_fetch_last_success_window_end)
    monkeypatch.setattr(profiler, "fetch_summary_for_run", fake_fetch_summary_for_run)
    monkeypatch.setattr(profiler, "_fetch_latest_log_row", fake_fetch_latest_log_row)
    monkeypatch.setattr(profiler, "insert_run_summary", fake_insert_run_summary)

    overall_status, _windows, _details, status_counts, window_audit, *_ = await profiler._run_store_windows(
        logger=profiler.JsonLogger(run_id="profiler-run", log_file_path=None),
        store=StoreProfile(
            store_code="TD01",
            store_name="TD Store",
            cost_center="CC-TD01",
            sync_config={},
            start_date=None,
        ),
        pipeline_name="td_orders_sync",
        pipeline_id=101,
        pipeline_fn=fake_pipeline_fn,
        run_env="test",
        run_id="profiler-run",
        backfill_days=1,
        window_days=1,
        overlap_days=0,
        from_date=date(2024, 2, 1),
        to_date=date(2024, 2, 1),
    )

    assert overall_status == "success"
    assert status_counts["success"] == 1
    assert status_counts["success_with_warnings"] == 0
    assert window_audit[0]["status"] == "success"
    assert window_audit[0]["status_note"] is None
    assert window_audit[0]["status_message"] is None
    assert window_audit[0]["error_message"] is None
    assert window_audit[0]["warning_count"] == 0
    assert window_audit[0]["ingestion_counts"]["primary"]["rows_ingested"] == 0
    assert window_audit[0]["ingestion_counts"]["secondary"]["rows_ingested"] == 1
    assert inserted_summaries[0]["overall_status"] == "success"


@pytest.mark.asyncio
async def test_td_profiler_rolls_up_api_primary_sales_ingest_as_clean_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        profiler,
        "config",
        SimpleNamespace(database_url="sqlite+aiosqlite:///:memory:", run_env="test"),
    )

    async def fake_fetch_last_success_window_end(**_kwargs: object) -> None:
        return None

    async def fake_pipeline_fn(**_kwargs: object) -> None:
        return None

    async def fake_fetch_summary_for_run(_database_url: str, _run_id: str) -> dict:
        return {
            "overall_status": "success",
            "metrics_json": {
                "orders": {
                    "overall_status": "success",
                    "stores": {
                        "TD01": {
                            "status": "ok",
                            "message": "Orders sourced from API and ingested",
                            "source_mode": "api_primary",
                            "rows_downloaded": 1,
                            "rows_ingested": 1,
                            "final_rows": 1,
                            "warnings": [],
                            "warning_rows": [],
                            "garments_fetch_completeness": "complete",
                            "garments_final_row_count": 1,
                            "garments_budget_state": "within_budget",
                        }
                    },
                },
                "sales": {
                    "overall_status": "success",
                    "stores": {
                        "TD01": {
                            "status": "ok",
                            "message": "Sales sourced from API and ingested",
                            "source_mode": "api_primary",
                            "rows_downloaded": 1,
                            "rows_ingested": 1,
                            "final_rows": 1,
                            "warnings": [],
                            "warning_rows": [],
                        }
                    },
                },
                "notification_payload": {
                    "overall_status": "success",
                    "orders_status": "success",
                    "sales_status": "success",
                    "stores": [
                        {
                            "store_code": "TD01",
                            "status": "ok",
                            "message": "API primary path executed",
                            "observability_warnings": [],
                            "orders": {
                                "status": "ok",
                                "message": "Orders sourced from API and ingested",
                                "warning_rows": [],
                                "warnings": [],
                            },
                            "sales": {
                                "status": "ok",
                                "message": "Sales sourced from API and ingested",
                                "warning_rows": [],
                                "warnings": [],
                            },
                        }
                    ],
                },
                "stores_summary": {
                    "stores": {
                        "TD01": {
                            "status": "ok",
                            "message": "API primary path executed",
                            "data_ingest_status": "success",
                            "observability_warnings": [],
                        }
                    }
                },
            },
        }

    async def fake_fetch_latest_log_row(**_kwargs: object) -> dict:
        return {
            "id": 1,
            "status": "success",
            "error_message": "Sales sourced from API and ingested; API primary path executed",
            "primary_rows_downloaded": 1,
            "primary_rows_ingested": 1,
            "primary_final_rows": 1,
            "secondary_rows_downloaded": 1,
            "secondary_rows_ingested": 1,
            "secondary_final_rows": 1,
        }

    inserted_summaries: list[dict] = []

    async def fake_insert_run_summary(_database_url: str, summary_record: dict) -> None:
        inserted_summaries.append(summary_record)

    monkeypatch.setattr(profiler, "fetch_last_success_window_end", fake_fetch_last_success_window_end)
    monkeypatch.setattr(profiler, "fetch_summary_for_run", fake_fetch_summary_for_run)
    monkeypatch.setattr(profiler, "_fetch_latest_log_row", fake_fetch_latest_log_row)
    monkeypatch.setattr(profiler, "insert_run_summary", fake_insert_run_summary)

    (
        overall_status,
        _windows,
        _details,
        status_counts,
        window_audit,
        _totals,
        row_facts,
    ) = await profiler._run_store_windows(
        logger=profiler.JsonLogger(run_id="profiler-run", log_file_path=None),
        store=StoreProfile(
            store_code="TD01",
            store_name="TD Store",
            cost_center="CC-TD01",
            sync_config={},
            start_date=None,
        ),
        pipeline_name="td_orders_sync",
        pipeline_id=101,
        pipeline_fn=fake_pipeline_fn,
        run_env="test",
        run_id="profiler-run",
        backfill_days=1,
        window_days=1,
        overlap_days=0,
        from_date=date(2024, 2, 1),
        to_date=date(2024, 2, 1),
    )

    assert overall_status == "success"
    assert status_counts["success"] == 1
    assert status_counts["success_with_warnings"] == 0
    assert window_audit[0]["status"] == "success"
    assert window_audit[0]["status_note"] is None
    assert window_audit[0]["status_message"] == "Sales sourced from API and ingested; API primary path executed"
    assert window_audit[0]["error_message"] is None
    assert window_audit[0]["warning_count"] == 0
    assert row_facts["warning_rows"] == []
    assert inserted_summaries[0]["overall_status"] == "success"


@pytest.mark.asyncio
async def test_td_profiler_normalizes_benign_api_primary_success_with_warnings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        profiler,
        "config",
        SimpleNamespace(database_url="sqlite+aiosqlite:///:memory:", run_env="test"),
    )

    async def fake_fetch_last_success_window_end(**_kwargs: object) -> None:
        return None

    async def fake_pipeline_fn(**_kwargs: object) -> None:
        return None

    async def fake_fetch_summary_for_run(_database_url: str, _run_id: str) -> dict:
        return {
            "overall_status": "success_with_warnings",
            "metrics_json": {
                "orders": {
                    "overall_status": "success",
                    "stores": {
                        "TD01": {
                            "status": "ok",
                            "message": "Orders sourced from API and ingested",
                            "warning_count": 0,
                            "warnings": [],
                            "warning_rows": [],
                            "error_rows": [],
                            "dropped_rows": [],
                            "garments_fetch_completeness": "complete",
                        }
                    },
                },
                "sales": {
                    "overall_status": "success",
                    "stores": {
                        "TD01": {
                            "status": "ok",
                            "message": "Sales sourced from API and ingested",
                            "warning_count": 0,
                            "warnings": [],
                            "warning_rows": [],
                            "error_rows": [],
                            "dropped_rows": [],
                        }
                    },
                },
                "notification_payload": {
                    "overall_status": "success_with_warnings",
                    "stores": [
                        {
                            "store_code": "TD01",
                            "status": "ok",
                            "message": "API primary path executed",
                            "warning_count": 0,
                            "observability_warnings": [],
                            "orders": {
                                "status": "ok",
                                "message": "Orders sourced from API and ingested",
                                "warning_count": 0,
                                "warnings": [],
                                "warning_rows": [],
                            },
                            "sales": {
                                "status": "ok",
                                "message": "Sales sourced from API and ingested",
                                "warning_count": 0,
                                "warnings": [],
                                "warning_rows": [],
                            },
                        }
                    ],
                },
                "stores_summary": {
                    "stores": {
                        "TD01": {
                            "status": "ok",
                            "message": "API primary path executed",
                            "warning_count": 0,
                            "observability_warnings": [],
                        }
                    }
                },
            },
        }

    async def fake_fetch_latest_log_row(**_kwargs: object) -> dict:
        return {
            "id": 1,
            "status": "success_with_warnings",
            "error_message": "Sales sourced from API and ingested; API primary path executed; warning_count=0",
            "primary_rows_downloaded": 1,
            "primary_rows_ingested": 1,
            "primary_final_rows": 1,
            "secondary_rows_downloaded": 1,
            "secondary_rows_ingested": 1,
            "secondary_final_rows": 1,
        }

    inserted_summaries: list[dict] = []

    async def fake_insert_run_summary(_database_url: str, summary_record: dict) -> None:
        inserted_summaries.append(summary_record)

    monkeypatch.setattr(profiler, "fetch_last_success_window_end", fake_fetch_last_success_window_end)
    monkeypatch.setattr(profiler, "fetch_summary_for_run", fake_fetch_summary_for_run)
    monkeypatch.setattr(profiler, "_fetch_latest_log_row", fake_fetch_latest_log_row)
    monkeypatch.setattr(profiler, "insert_run_summary", fake_insert_run_summary)

    overall_status, _windows, _details, status_counts, window_audit, *_ = await profiler._run_store_windows(
        logger=profiler.JsonLogger(run_id="profiler-run", log_file_path=None),
        store=StoreProfile(
            store_code="TD01",
            store_name="TD Store",
            cost_center="CC-TD01",
            sync_config={},
            start_date=None,
        ),
        pipeline_name="td_orders_sync",
        pipeline_id=101,
        pipeline_fn=fake_pipeline_fn,
        run_env="test",
        run_id="profiler-run",
        backfill_days=1,
        window_days=1,
        overlap_days=0,
        from_date=date(2024, 2, 1),
        to_date=date(2024, 2, 1),
    )

    assert overall_status == "success"
    assert status_counts["success"] == 1
    assert status_counts["success_with_warnings"] == 0
    assert window_audit[0]["status"] == "success"
    assert window_audit[0]["status_note"] is None
    assert window_audit[0]["status_message"] == (
        "Sales sourced from API and ingested; API primary path executed; warning_count=0"
    )
    assert window_audit[0]["error_message"] is None
    assert window_audit[0]["warning_count"] == 0
    assert window_audit[0]["td_benign_warning_info"] == {
        "normalized_from_status": "success_with_warnings",
        "message": "Sales sourced from API and ingested; API primary path executed; warning_count=0",
        "reason": "benign_td_api_primary_success_with_zero_warnings",
    }
    assert inserted_summaries[0]["overall_status"] == "success"


@pytest.mark.asyncio
async def test_td_garment_incomplete_degrades_success_without_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        profiler,
        "config",
        SimpleNamespace(database_url="sqlite+aiosqlite:///:memory:", run_env="test"),
    )

    async def fake_fetch_last_success_window_end(**_kwargs: object) -> None:
        return None

    async def fake_pipeline_fn(**_kwargs: object) -> None:
        return None

    async def fake_fetch_summary_for_run(_database_url: str, _run_id: str) -> dict:
        return {
            "overall_status": "success",
            "metrics_json": {
                "orders": {
                    "stores": {
                        "TD01": {
                            "rows_downloaded": 12,
                            "rows_ingested": 12,
                            "final_rows": 12,
                            "garments_fetch_completeness": "incomplete",
                            "garments_final_row_count": 17,
                            "garments_budget_state": "near_limit",
                        }
                    }
                },
                "sales": {"stores": {"TD01": {"rows_downloaded": 7, "rows_ingested": 7, "final_rows": 7}}},
            },
        }

    async def fake_fetch_latest_log_row(**_kwargs: object) -> dict:
        return {"id": 1, "status": "success", "error_message": None}

    inserted_summaries: list[dict] = []

    async def fake_insert_run_summary(_database_url: str, summary_record: dict) -> None:
        inserted_summaries.append(summary_record)

    monkeypatch.setattr(profiler, "fetch_last_success_window_end", fake_fetch_last_success_window_end)
    monkeypatch.setattr(profiler, "fetch_summary_for_run", fake_fetch_summary_for_run)
    monkeypatch.setattr(profiler, "_fetch_latest_log_row", fake_fetch_latest_log_row)
    monkeypatch.setattr(profiler, "insert_run_summary", fake_insert_run_summary)

    overall_status, _windows, _details, status_counts, window_audit, *_ = await profiler._run_store_windows(
        logger=profiler.JsonLogger(run_id="profiler-run", log_file_path=None),
        store=StoreProfile(
            store_code="TD01",
            store_name="TD Store",
            cost_center="CC-TD01",
            sync_config={},
            start_date=None,
        ),
        pipeline_name="td_orders_sync",
        pipeline_id=101,
        pipeline_fn=fake_pipeline_fn,
        run_env="test",
        run_id="profiler-run",
        backfill_days=1,
        window_days=1,
        overlap_days=0,
        from_date=date(2024, 2, 1),
        to_date=date(2024, 2, 1),
    )

    assert overall_status == "success_with_warnings"
    assert status_counts["success_with_warnings"] == 1
    assert status_counts["success"] == 0
    assert window_audit[0]["status"] == "success_with_warnings"
    assert window_audit[0]["attempt_no"] == 1
    assert "current data incomplete for garment-dependent reports" in window_audit[0]["status_note"]
    assert "recovered navigation failure" not in window_audit[0]["status_note"]
    assert window_audit[0]["warning_count"] == 1
    assert window_audit[0]["ingestion_counts"]["primary"]["rows_ingested"] == 12
    assert window_audit[0]["ingestion_counts"]["secondary"]["rows_ingested"] == 7
    assert inserted_summaries[0]["overall_status"] == "success_with_warnings"


@pytest.mark.asyncio
async def test_td_garment_incomplete_after_retry_remains_degraded_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        profiler,
        "config",
        SimpleNamespace(database_url="sqlite+aiosqlite:///:memory:", run_env="test"),
    )

    async def fake_fetch_last_success_window_end(**_kwargs: object) -> None:
        return None

    async def fake_pipeline_fn(**_kwargs: object) -> None:
        return None

    async def fake_fetch_summary_for_run(_database_url: str, _run_id: str) -> dict:
        return {
            "overall_status": "success",
            "metrics_json": {
                "orders": {
                    "stores": {
                        "TD01": {
                            "garments_fetch_completeness": "incomplete",
                            "garments_final_row_count": 19,
                            "garments_budget_state": "exhausted",
                        }
                    }
                },
                "sales": {"stores": {"TD01": {}}},
            },
        }

    log_rows = iter(
        [
            {"id": 1, "status": "skipped", "error_message": "session load timeout"},
            {"id": 2, "status": "success", "error_message": None},
        ]
    )

    async def fake_fetch_latest_log_row(**_kwargs: object) -> dict:
        return next(log_rows)

    inserted_summaries: list[dict] = []

    async def fake_insert_run_summary(_database_url: str, summary_record: dict) -> None:
        inserted_summaries.append(summary_record)

    monkeypatch.setattr(profiler, "fetch_last_success_window_end", fake_fetch_last_success_window_end)
    monkeypatch.setattr(profiler, "fetch_summary_for_run", fake_fetch_summary_for_run)
    monkeypatch.setattr(profiler, "_fetch_latest_log_row", fake_fetch_latest_log_row)
    monkeypatch.setattr(profiler, "insert_run_summary", fake_insert_run_summary)

    overall_status, _windows, _details, status_counts, window_audit, *_ = await profiler._run_store_windows(
        logger=profiler.JsonLogger(run_id="profiler-run", log_file_path=None),
        store=StoreProfile(
            store_code="TD01",
            store_name="TD Store",
            cost_center="CC-TD01",
            sync_config={},
            start_date=None,
        ),
        pipeline_name="td_orders_sync",
        pipeline_id=101,
        pipeline_fn=fake_pipeline_fn,
        run_env="test",
        run_id="profiler-run",
        backfill_days=1,
        window_days=1,
        overlap_days=0,
        from_date=date(2024, 2, 1),
        to_date=date(2024, 2, 1),
    )

    assert overall_status == "success_with_warnings"
    assert status_counts["success_with_warnings"] == 1
    assert window_audit[0]["attempt_no"] == 2
    assert [attempt["status"] for attempt in window_audit[0]["attempts"]] == [
        "skipped",
        "success_with_warnings",
    ]
    assert "recovered navigation failure on previous attempt" in window_audit[0]["status_note"]
    assert "current data incomplete for garment-dependent reports" in window_audit[0]["status_note"]
    assert "after retry" in window_audit[0]["status_note"]
    assert window_audit[0]["warning_count"] == 1
    assert inserted_summaries[0]["overall_status"] == "success_with_warnings"


@pytest.mark.asyncio
async def test_td_navigation_failure_retries_and_recovers_without_garment_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        profiler,
        "config",
        SimpleNamespace(database_url="sqlite+aiosqlite:///:memory:", run_env="test"),
    )

    async def fake_fetch_last_success_window_end(**_kwargs: object) -> None:
        return None

    pipeline_run_ids: list[str] = []

    async def fake_pipeline_fn(**kwargs: object) -> None:
        pipeline_run_ids.append(str(kwargs["run_id"]))

    async def fake_fetch_summary_for_run(_database_url: str, _run_id: str) -> dict:
        return {
            "overall_status": "success",
            "metrics_json": {
                "orders": {
                    "stores": {
                        "TD01": {
                            "rows_downloaded": 10,
                            "rows_ingested": 10,
                            "garments_fetch_completeness": "complete",
                            "garments_final_row_count": 10,
                        }
                    }
                },
                "sales": {"stores": {"TD01": {"rows_downloaded": 5, "rows_ingested": 5}}},
            },
        }

    log_rows = iter(
        [
            {
                "id": 1,
                "status": "skipped",
                "error_message": "Navigation failed: timeout loading TD reports",
            },
            {"id": 2, "status": "success", "error_message": None},
        ]
    )

    async def fake_fetch_latest_log_row(**_kwargs: object) -> dict:
        return next(log_rows)

    inserted_summaries: list[dict] = []

    async def fake_insert_run_summary(_database_url: str, summary_record: dict) -> None:
        inserted_summaries.append(summary_record)

    monkeypatch.setattr(profiler, "fetch_last_success_window_end", fake_fetch_last_success_window_end)
    monkeypatch.setattr(profiler, "fetch_summary_for_run", fake_fetch_summary_for_run)
    monkeypatch.setattr(profiler, "_fetch_latest_log_row", fake_fetch_latest_log_row)
    monkeypatch.setattr(profiler, "insert_run_summary", fake_insert_run_summary)

    overall_status, _windows, _details, status_counts, window_audit, *_ = await profiler._run_store_windows(
        logger=profiler.JsonLogger(run_id="profiler-run", log_file_path=None),
        store=StoreProfile(
            store_code="TD01",
            store_name="TD Store",
            cost_center="CC-TD01",
            sync_config={},
            start_date=None,
        ),
        pipeline_name="td_orders_sync",
        pipeline_id=101,
        pipeline_fn=fake_pipeline_fn,
        run_env="test",
        run_id="profiler-run",
        backfill_days=1,
        window_days=1,
        overlap_days=0,
        from_date=date(2024, 2, 1),
        to_date=date(2024, 2, 1),
    )

    assert pipeline_run_ids == [
        "profiler-run_TD01_001",
        "profiler-run_TD01_001",
    ]
    assert overall_status == "success"
    assert status_counts["success"] == 1
    assert window_audit[0]["attempt_no"] == 2
    assert [attempt["status"] for attempt in window_audit[0]["attempts"]] == ["skipped", "success"]
    assert "recovered navigation failure on previous attempt" in window_audit[0]["status_note"]
    assert "current data incomplete" not in window_audit[0]["status_note"]
    assert inserted_summaries[0]["overall_status"] == "success"


def test_td_garment_completeness_unavailable_is_not_incomplete_warning() -> None:
    assert profiler._extract_td_garment_warning_from_summary(
        {"metrics_json": {"orders": {"stores": {"TD01": {}}}}}, store_code="TD01"
    ) is None

    warning = profiler._extract_td_garment_warning_from_summary(
        {
            "metrics_json": {
                "orders": {
                    "stores": {
                        "TD01": {
                            "garments_fetch_completeness": "unknown",
                            "garments_final_row_count": 0,
                        }
                    }
                }
            }
        },
        store_code="TD01",
    )

    assert warning is not None
    assert warning["garments_fetch_completeness"] == "unknown"
    assert warning["is_incomplete"] is False
    assert profiler._td_garment_warning_entries(
        [
            {
                "store_code": "TD01",
                "from_date": "2024-02-01",
                "to_date": "2024-02-01",
                "td_garment_warning": warning,
            }
        ]
    ) == []


@pytest.mark.asyncio
async def test_profiler_payload_surfaces_td_garment_incomplete_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    persisted_summaries: list[dict] = []
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
                store_code="TD01",
                store_name="TD Store",
                cost_center="CC-TD01",
                sync_config={},
                start_date=None,
            )
        ]

    async def fake_process_store(**_kwargs: object) -> StoreRunResult:
        status_counts = profiler._init_status_counts()
        status_counts["success_with_warnings"] = 1
        return StoreRunResult(
            store_code="TD01",
            pipeline_group="TD",
            pipeline_name="td_orders_sync",
            cost_center="CC-TD01",
            overall_status="success_with_warnings",
            window_count=1,
            windows=[(date(2024, 2, 1), date(2024, 2, 2))],
            status_counts=status_counts,
            window_audit=[
                {
                    "store_code": "TD01",
                    "from_date": "2024-02-01",
                    "to_date": "2024-02-02",
                    "status": "success_with_warnings",
                    "td_garment_warning": {
                        "garments_fetch_completeness": "incomplete",
                        "garments_final_row_count": 17,
                        "garments_budget_state": "near_limit",
                        "garments_incomplete_reason": {"code": "pagination_budget_exhausted", "message": "pagination budget exhausted"},
                        "page_size_requested": 100,
                        "pages_attempted": 5,
                        "pages_succeeded": 4,
                        "last_successful_page": 4,
                        "reported_total_rows": 600,
                        "reported_total_pages": 6,
                        "parsed_row_count": 400,
                        "unique_row_id_count": 399,
                        "rows_without_identity_count": 1,
                        "identity_strategy": "composite",
                        "stop_reason": "max_pages",
                        "garments_attempted_page_count": 5,
                        "garments_completed_page_count": 4,
                        "garments_expected_page_count": 6,
                        "garments_timeout_count": 1,
                        "garments_retry_count": 2,
                        "is_incomplete": True,
                    },
                    "warning_count": 1,
                    "ingestion_counts": {},
                }
            ],
            ingestion_totals=profiler._init_ingestion_totals(),
            row_facts=_init_row_facts(),
        )

    async def fake_insert_run_summary(_database_url: str, summary_record: dict) -> None:
        persisted_summaries.append(summary_record)

    async def fake_persist_missing_windows_log_rows(**_kwargs: object) -> None:
        return None

    async def fake_send_notifications_for_run(_pipeline_name: str, _run_id: str) -> dict:
        return {"emails_planned": 1, "emails_sent": 1, "errors": []}

    monkeypatch.setattr(profiler, "_fetch_pipeline_id", fake_fetch_pipeline_id)
    monkeypatch.setattr(profiler, "_load_store_profiles", fake_load_store_profiles)
    monkeypatch.setattr(profiler, "_process_store", fake_process_store)
    monkeypatch.setattr(profiler, "insert_run_summary", fake_insert_run_summary)
    monkeypatch.setattr(profiler, "_persist_missing_windows_log_rows", fake_persist_missing_windows_log_rows)
    monkeypatch.setattr(profiler, "send_notifications_for_run", fake_send_notifications_for_run)

    await profiler.main(sync_group="TD", max_workers=1, run_env="test", run_id="profiler-td-garment")

    summary = persisted_summaries[0]
    payload = summary["metrics_json"]["notification_payload"]
    store = payload["stores"][0]

    assert summary["overall_status"] == "success_with_warnings"
    assert store["warning_windows"] == [
        {
            "store_code": "TD01",
            "from_date": "2024-02-01",
            "to_date": "2024-02-02",
            "status": "success_with_warnings",
            "status_note": "",
            "status_message": "",
            "warning_reason": "",
            "error_message": "",
            "attempt_no": None,
            "warning_count": 1,
        }
    ]
    assert store["td_garment_warning_count"] == 1
    assert store["td_garment_incomplete_windows"] == [
        {
            "store_code": "TD01",
            "from_date": "2024-02-01",
            "to_date": "2024-02-02",
            "garments_fetch_completeness": "incomplete",
            "garments_final_row_count": 17,
            "garments_budget_state": "near_limit",
            "garments_incomplete_reason": {"code": "pagination_budget_exhausted", "message": "pagination budget exhausted"},
            "page_size_requested": 100,
            "pages_attempted": 5,
            "pages_succeeded": 4,
            "last_successful_page": 4,
            "reported_total_rows": 600,
            "reported_total_pages": 6,
            "parsed_row_count": 400,
            "unique_row_id_count": 399,
            "rows_without_identity_count": 1,
            "identity_strategy": "composite",
            "stop_reason": "max_pages",
            "garments_attempted_page_count": 5,
            "garments_completed_page_count": 4,
            "garments_expected_page_count": 6,
            "garments_timeout_count": 1,
            "garments_retry_count": 2,
        }
    ]
    assert any("TD_GARMENT_DATA_INCOMPLETE: TD01" in warning for warning in payload["warnings"])
    assert "final_garment_rows=17" in summary["summary_text"]
    assert "reason=pagination budget exhausted" in summary["summary_text"]
    assert "stop_reason=max_pages" in summary["summary_text"]
    assert "reason=unknown" not in summary["summary_text"]


@pytest.mark.asyncio
async def test_uc_timeout_skipped_window_retries_and_can_recover(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        profiler,
        "config",
        SimpleNamespace(database_url="sqlite+aiosqlite:///:memory:", run_env="test"),
    )

    async def fake_fetch_last_success_window_end(**_kwargs: object) -> None:
        return None

    inserted_summaries: list[dict] = []
    pipeline_run_ids: list[str] = []

    async def fake_pipeline_fn(**kwargs: object) -> None:
        pipeline_run_ids.append(str(kwargs["run_id"]))

    async def fake_fetch_summary_for_run(_database_url: str, _run_id: str) -> dict:
        return {}

    async def fake_fetch_latest_log_row(**kwargs: object) -> dict:
        run_id = str(kwargs["run_id"])
        if run_id.endswith("attempt1"):
            return {
                "id": 1,
                "status": "skipped",
                "error_message": "Timeout while loading Archive Orders page",
            }
        return {"id": 2, "status": "success", "error_message": None}

    async def fake_insert_run_summary(_database_url: str, summary_record: dict) -> None:
        inserted_summaries.append(summary_record)

    monkeypatch.setattr(
        profiler, "fetch_last_success_window_end", fake_fetch_last_success_window_end
    )
    monkeypatch.setattr(profiler, "fetch_summary_for_run", fake_fetch_summary_for_run)
    monkeypatch.setattr(profiler, "_fetch_latest_log_row", fake_fetch_latest_log_row)
    monkeypatch.setattr(profiler, "insert_run_summary", fake_insert_run_summary)

    overall_status, _windows, _details, status_counts, window_audit, *_ = await profiler._run_store_windows(
        logger=profiler.JsonLogger(run_id="profiler-run", log_file_path=None),
        store=StoreProfile(
            store_code="UC01",
            store_name="UC Store",
            cost_center="CC-UC01",
            sync_config={},
            start_date=None,
        ),
        pipeline_name="uc_orders_sync",
        pipeline_id=101,
        pipeline_fn=fake_pipeline_fn,
        run_env="test",
        run_id="profiler-run",
        backfill_days=1,
        window_days=1,
        overlap_days=0,
        from_date=date(2024, 1, 1),
        to_date=date(2024, 1, 1),
    )

    assert pipeline_run_ids == [
        "profiler-run_UC01_001_attempt1",
        "profiler-run_UC01_001_attempt2",
    ]
    assert overall_status == "success"
    assert status_counts["success"] == 1
    assert window_audit[0]["attempt_no"] == 2
    assert [attempt["status"] for attempt in window_audit[0]["attempts"]] == [
        "skipped",
        "success",
    ]
    assert inserted_summaries[0]["overall_status"] == "success"


@pytest.mark.asyncio
async def test_uc_window_log_warns_on_row_warnings_without_failing_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        profiler,
        "config",
        SimpleNamespace(database_url="sqlite+aiosqlite:///:memory:", run_env="test"),
    )

    async def fake_fetch_last_success_window_end(**_kwargs: object) -> None:
        return None

    async def fake_pipeline_fn(**_kwargs: object) -> None:
        return None

    async def fake_fetch_summary_for_run(_database_url: str, _run_id: str) -> dict:
        return {
            "overall_status": "success",
            "metrics_json": {
                "stores_summary": {
                    "stores": {
                        "UC01": {
                            "status": "ok",
                            "data_ingest_status": "success",
                            "download_path": "/tmp/uc01.csv",
                            "rows_downloaded": 3,
                            "rows_ingested": 3,
                            "staging_rows": 3,
                            "final_rows": 3,
                            "warning_count": 2,
                            "warning_rows": [
                                {
                                    "store_code": "UC01",
                                    "order_number": "U-1",
                                    "ingest_remarks": "Customer GSTIN missing",
                                    "customer_name": "Sensitive Name",
                                },
                                {
                                    "store_code": "UC01",
                                    "order_number": "U-2",
                                    "reason_code": "amount_mismatch",
                                    "mobile_number": "9999999999",
                                },
                            ],
                        }
                    }
                }
            },
        }

    async def fake_fetch_latest_log_row(**_kwargs: object) -> dict:
        return {
            "id": 11,
            "status": "success",
            "error_message": None,
            "primary_rows_downloaded": 3,
            "primary_rows_ingested": 3,
            "primary_staging_rows": 3,
            "primary_final_rows": 3,
        }

    inserted_summaries: list[dict] = []
    events: list[dict] = []

    async def fake_insert_run_summary(_database_url: str, summary_record: dict) -> None:
        inserted_summaries.append(summary_record)

    monkeypatch.setattr(profiler, "fetch_last_success_window_end", fake_fetch_last_success_window_end)
    monkeypatch.setattr(profiler, "fetch_summary_for_run", fake_fetch_summary_for_run)
    monkeypatch.setattr(profiler, "_fetch_latest_log_row", fake_fetch_latest_log_row)
    monkeypatch.setattr(profiler, "insert_run_summary", fake_insert_run_summary)
    monkeypatch.setattr(profiler, "log_event", lambda **kwargs: events.append(kwargs))

    overall_status, _windows, _details, status_counts, window_audit, *_ = await profiler._run_store_windows(
        logger=profiler.JsonLogger(run_id="profiler-run", log_file_path=None),
        store=StoreProfile(
            store_code="UC01",
            store_name="UC Store",
            cost_center="CC-UC01",
            sync_config={},
            start_date=None,
        ),
        pipeline_name="uc_orders_sync",
        pipeline_id=101,
        pipeline_fn=fake_pipeline_fn,
        run_env="test",
        run_id="profiler-run",
        backfill_days=1,
        window_days=1,
        overlap_days=0,
        from_date=date(2024, 2, 1),
        to_date=date(2024, 2, 1),
    )

    uc_window_events = [event for event in events if event.get("phase") == "uc_window_log"]

    assert overall_status == "success"
    assert status_counts["success"] == 1
    assert status_counts["success_with_warnings"] == 0
    assert window_audit[0]["status"] == "success"
    assert window_audit[0]["warning_count"] == 2
    assert window_audit[0]["warning_categories"] == {
        "Customer GSTIN missing": 1,
        "amount_mismatch": 1,
    }
    assert len(uc_window_events) == 1
    uc_event = uc_window_events[0]
    assert uc_event["status"] == "warning"
    assert uc_event["ingest_success"] is True
    assert uc_event["warning_count"] == 2
    assert uc_event["warning_categories"] == {
        "Customer GSTIN missing": 1,
        "amount_mismatch": 1,
    }
    assert all(
        "customer_name" not in row and "mobile_number" not in row
        for row in uc_event["warning_rows"]
    )
    assert (
        profiler._select_summary_overall_status(
            status_counts, uc_warning_count=window_audit[0]["warning_count"]
        )
        == "success_with_warnings"
    )
    assert inserted_summaries[0]["overall_status"] == "success"


@pytest.mark.asyncio
async def test_profiler_promotes_all_timeout_uc_windows_to_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        profiler,
        "config",
        SimpleNamespace(database_url="sqlite+aiosqlite:///:memory:", run_env="test"),
    )

    async def fake_fetch_last_success_window_end(**_kwargs: object) -> None:
        return None

    inserted_summaries: list[dict] = []

    async def fake_pipeline_fn(**_kwargs: object) -> None:
        return None

    async def fake_fetch_summary_for_run(_database_url: str, _run_id: str) -> dict:
        return {}

    async def fake_fetch_latest_log_row(**_kwargs: object) -> dict:
        return {
            "id": 1,
            "status": "skipped",
            "error_message": "session load timeout before Archive Orders navigation",
        }

    async def fake_insert_run_summary(_database_url: str, summary_record: dict) -> None:
        inserted_summaries.append(summary_record)

    monkeypatch.setattr(
        profiler, "fetch_last_success_window_end", fake_fetch_last_success_window_end
    )
    monkeypatch.setattr(profiler, "fetch_summary_for_run", fake_fetch_summary_for_run)
    monkeypatch.setattr(profiler, "_fetch_latest_log_row", fake_fetch_latest_log_row)
    monkeypatch.setattr(profiler, "insert_run_summary", fake_insert_run_summary)

    overall_status, _windows, _details, status_counts, window_audit, *_ = await profiler._run_store_windows(
        logger=profiler.JsonLogger(run_id="profiler-run", log_file_path=None),
        store=StoreProfile(
            store_code="UC01",
            store_name="UC Store",
            cost_center="CC-UC01",
            sync_config={},
            start_date=None,
        ),
        pipeline_name="uc_orders_sync",
        pipeline_id=101,
        pipeline_fn=fake_pipeline_fn,
        run_env="test",
        run_id="profiler-run",
        backfill_days=1,
        window_days=1,
        overlap_days=0,
        from_date=date(2024, 1, 1),
        to_date=date(2024, 1, 1),
    )

    assert overall_status == "failed"
    assert status_counts["failed"] == 1
    assert status_counts["skipped"] == 0
    assert window_audit[0]["status"] == "failed"
    assert window_audit[0]["attempt_no"] == 2
    assert "timeout/navigation failure promoted to failed" in window_audit[0]["status_note"]
    assert inserted_summaries[0]["overall_status"] == "failed"


def test_timeout_retry_gate_checks_error_status_note_and_skip_reason() -> None:
    assert profiler._should_retry_window_status(
        status="skipped",
        error_message="Timeout while loading Archive Orders page",
        status_note=None,
    )
    assert profiler._should_retry_window_status(
        status="partial",
        error_message=None,
        status_note="Archive Orders navigation failed",
    )
    assert profiler._should_retry_window_status(
        status="skipped",
        error_message=None,
        status_note=None,
        skip_reason="session load timeout",
    )
    assert not profiler._should_retry_window_status(
        status="skipped",
        error_message=None,
        status_note="no data",
    )


def test_cancel_pending_tasks_is_bounded_for_cancellation_resistant_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio
    import time

    events: list[dict[str, object]] = []
    release = asyncio.Event()

    async def cancellation_resistant_task() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await release.wait()

    monkeypatch.setattr(profiler, "log_event", lambda **kwargs: events.append(kwargs))
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(cancellation_resistant_task(), name="resistant-browser-cleanup")
    loop.run_until_complete(asyncio.sleep(0))

    started = time.monotonic()
    forced_cleanup_required = profiler._cancel_pending_tasks(
        loop=loop,
        logger=SimpleNamespace(),
        timeout_seconds=0.05,
    )
    elapsed = time.monotonic() - started

    assert forced_cleanup_required is True
    assert elapsed < 0.5
    assert events[-1]["pending_task_count"] == 1
    assert events[-1]["cancellation_timeout_seconds"] == 0.05
    assert events[-1]["forced_cleanup_required"] is True
    assert "resistant-browser-cleanup" in str(events[-1]["pending_tasks"])

    release.set()
    loop.run_until_complete(task)
    asyncio.set_event_loop(None)
    loop.close()


def test_profiler_main_fatal_error_forces_bounded_exit_when_task_resists_cancellation() -> None:
    import os
    import subprocess
    import sys
    import textwrap

    script = textwrap.dedent(
        """
        import asyncio
        import app.crm_downloader.orders_sync_run_profiler.main as profiler

        class Logger:
            def close(self):
                pass

        async def resistant_cleanup():
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await asyncio.Event().wait()

        async def fatal_main(**kwargs):
            asyncio.create_task(resistant_cleanup(), name="stalled-browser-context-cleanup")
            await asyncio.sleep(0)
            raise RuntimeError("simulated fatal application exception")

        profiler.main = fatal_main
        profiler.get_logger = lambda **kwargs: Logger()
        profiler.log_event = lambda **kwargs: None
        profiler._main()
        """
    )
    env = os.environ.copy()
    env["ORDERS_SYNC_PROFILER_SHUTDOWN_TIMEOUT_SECONDS"] = "0.1"

    result = subprocess.run(
        [sys.executable, "-c", script],
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=3,
    )

    assert result.returncode == 1


def test_profiler_rolls_amount_metrics_into_notification_context() -> None:
    summary = {
        "metrics_json": {
            "orders": {
                "stores": {
                    "TD01": {
                        "rows_downloaded": 2,
                        "final_rows": 2,
                        "amount_metrics": {
                            "explicit_zero_value_order_count": 1,
                            "missing_amount_field_count": 2,
                            "malformed_amount_field_count": 1,
                            "canonical_zero_value_order_count": 2,
                            "parsed_source_gross_amount_sum": 125.5,
                            "parsed_source_net_amount_sum": 100,
                        },
                    }
                }
            },
            "sales": {"stores": {"TD01": {}}},
        }
    }

    counts = profiler._extract_ingestion_counts_from_summary(
        summary, store_code="TD01", pipeline_name="td_orders_sync"
    )
    unified = profiler._build_unified_metrics(counts["primary"])

    assert unified["explicit_zero_value_order_count"] == 1
    assert unified["missing_amount_field_count"] == 2
    assert unified["malformed_amount_field_count"] == 1
    assert unified["canonical_zero_value_order_count"] == 2
    assert unified["parsed_source_gross_amount_sum"] == 125.5
    assert unified["parsed_source_net_amount_sum"] == 100.0
    assert "missing_amount_fields=2" in profiler._format_unified_metrics(unified)


def test_all_store_navigation_failures_classified_as_browser_runtime_incident() -> None:
    def result(store_code: str) -> StoreRunResult:
        status_counts = profiler._init_status_counts()
        status_counts["failed"] = 1
        return StoreRunResult(
            store_code=store_code,
            pipeline_group="UC",
            pipeline_name="uc_orders_sync",
            cost_center=None,
            overall_status="failed",
            window_count=1,
            windows=[(date(2024, 1, 1), date(2024, 1, 1))],
            status_counts=status_counts,
            window_audit=[
                {
                    "status": "failed",
                    "status_note": "Archive Orders navigation failed after timeout/navigation failure promoted to failed",
                    "attempts": [
                        {"status": "skipped", "error_message": "Timed out waiting for home URL"},
                        {"status": "failed", "error_message": "UC dashboard shell loaded but card selector is missing"},
                    ],
                }
            ],
            ingestion_totals=profiler._init_ingestion_totals(),
            row_facts=_init_row_facts(),
        )

    incident = profiler._all_store_navigation_infrastructure_incident(
        [result("UC567"), result("UC610")]
    )

    assert incident == {
        "failure_scope": "all_attempted_stores",
        "failure_reason": "navigation_timeout_or_browser_runtime",
        "failed_store_codes": ["UC567", "UC610"],
        "store_count": 2,
        "pipeline_groups": ["UC"],
    }


def test_mixed_navigation_and_success_is_not_browser_runtime_incident() -> None:
    failed_counts = profiler._init_status_counts()
    failed_counts["failed"] = 1
    success_counts = profiler._init_status_counts()
    success_counts["success"] = 1

    failed = StoreRunResult(
        store_code="UC567",
        pipeline_group="UC",
        pipeline_name="uc_orders_sync",
        cost_center=None,
        overall_status="failed",
        window_count=1,
        windows=[(date(2024, 1, 1), date(2024, 1, 1))],
        status_counts=failed_counts,
        window_audit=[{"status": "failed", "error_message": "navigation failed timeout"}],
        ingestion_totals=profiler._init_ingestion_totals(),
        row_facts=_init_row_facts(),
    )
    succeeded = StoreRunResult(
        store_code="UC610",
        pipeline_group="UC",
        pipeline_name="uc_orders_sync",
        cost_center=None,
        overall_status="success",
        window_count=1,
        windows=[(date(2024, 1, 1), date(2024, 1, 1))],
        status_counts=success_counts,
        window_audit=[{"status": "success"}],
        ingestion_totals=profiler._init_ingestion_totals(),
        row_facts=_init_row_facts(),
    )

    assert profiler._all_store_navigation_infrastructure_incident([failed, succeeded]) is None


def test_extract_row_facts_preserves_sanitized_uc_warning_rows_from_summary() -> None:
    summary = {
        "metrics_json": {
            "stores_summary": {
                "stores": {
                    "UC01": {
                        "warning_count": 1,
                        "warning_rows": [
                            {
                                "store_code": "UC01",
                                "order_number": "U-1",
                                "customer_name": "Sensitive Name",
                                "mobile_number": "9999999999",
                                "values": {"Customer GSTIN": ""},
                                "ingest_remarks": "Customer GSTIN missing",
                            }
                        ],
                    }
                }
            }
        }
    }

    row_facts = profiler._extract_row_facts_from_summary(summary)

    assert row_facts["warning_rows"] == [
        {
            "store_code": "UC01",
            "order_number": "U-1",
            "ingest_remarks": "Customer GSTIN missing",
            "ingestion_remarks": "Customer GSTIN missing",
            "remarks": "Customer GSTIN missing",
        }
    ]


def test_extract_uc_warning_details_from_summary_categorizes_warning_rows() -> None:
    summary = {
        "metrics_json": {
            "stores_summary": {
                "stores": {
                    "UC01": {
                        "warning_count": 2,
                        "warning_rows": [
                            {"order_number": "U-1", "ingest_remarks": "Customer GSTIN missing"},
                            {"order_number": "U-2", "reason_code": "amount_mismatch"},
                        ],
                    }
                }
            }
        }
    }

    details = profiler._extract_uc_warning_details_from_summary(summary, store_code="uc01")

    assert details["warning_count"] == 2
    assert details["warning_categories"] == {
        "Customer GSTIN missing": 1,
        "amount_mismatch": 1,
    }
    assert details["warning_rows"][0]["store_code"] == "UC01"
    assert details["warning_rows"][0] == {
        "store_code": "UC01",
        "order_number": "U-1",
        "ingest_remarks": "Customer GSTIN missing",
        "ingestion_remarks": "Customer GSTIN missing",
        "remarks": "Customer GSTIN missing",
    }
    assert all("mobile_number" not in row and "customer_name" not in row for row in details["warning_rows"])


def test_build_uc_window_log_carries_actionable_sanitized_warning_details() -> None:
    payload = profiler._build_uc_window_log(
        download_paths={"gst": {"download_path": "/tmp/uc.csv"}},
        ingestion_counts={"primary": {"staging_rows": 3, "final_rows": 3}},
        error_message=None,
        warning_count=2,
        warning_categories={"Customer GSTIN missing": 1, "amount_mismatch": 1},
        warning_rows=[
            {"store_code": "UC01", "order_number": "U-1", "ingest_remarks": "Customer GSTIN missing"},
            {"store_code": "UC01", "order_number": "U-2", "ingest_remarks": "amount_mismatch"},
        ],
    )

    assert payload["warning_count"] == 2
    assert payload["warning_categories"] == {"Customer GSTIN missing": 1, "amount_mismatch": 1}
    assert payload["warning_rows"] == [
        {
            "store_code": "UC01",
            "order_number": "U-1",
            "ingest_remarks": "Customer GSTIN missing",
            "ingestion_remarks": "Customer GSTIN missing",
            "remarks": "Customer GSTIN missing",
        },
        {
            "store_code": "UC01",
            "order_number": "U-2",
            "ingest_remarks": "amount_mismatch",
            "ingestion_remarks": "amount_mismatch",
            "remarks": "amount_mismatch",
        },
    ]


def test_summary_status_promotes_warning_windows_before_success() -> None:
    status_counts = _init_status_counts()
    status_counts["success"] = 3
    status_counts["success_with_warnings"] = 1

    assert _select_summary_overall_status(status_counts, uc_warning_count=0) == "success_with_warnings"


def test_summary_status_promotes_uc_row_warnings_when_windows_succeed() -> None:
    status_counts = _init_status_counts()
    status_counts["success"] = 2

    assert _select_summary_overall_status(status_counts, uc_warning_count=4) == "success_with_warnings"


def test_summary_status_uc_row_warnings_do_not_override_failures_or_partials() -> None:
    failed_counts = _init_status_counts()
    failed_counts["success"] = 1
    failed_counts["failed"] = 1
    partial_counts = _init_status_counts()
    partial_counts["success"] = 1
    partial_counts["partial"] = 1

    assert _select_summary_overall_status(failed_counts, uc_warning_count=4) == "failed"
    assert _select_summary_overall_status(partial_counts, uc_warning_count=4) == "partial"


@pytest.mark.asyncio
async def test_profiler_uc_row_warnings_promote_top_level_status_after_aggregation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    persisted_summaries: list[dict] = []

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
        return 202

    async def fake_load_store_profiles(**_kwargs: object) -> list[StoreProfile]:
        return [
            StoreProfile(
                store_code="UC01",
                store_name="UC 01",
                cost_center="CC-UC01",
                sync_config={},
                start_date=None,
            )
        ]

    async def fake_process_store(**_kwargs: object) -> StoreRunResult:
        status_counts = profiler._init_status_counts()
        status_counts["success"] = 1
        return StoreRunResult(
            store_code="UC01",
            pipeline_group="UC",
            pipeline_name="uc_orders_sync",
            cost_center="CC-UC01",
            overall_status="success",
            window_count=1,
            windows=[(date(2024, 2, 1), date(2024, 2, 2))],
            status_counts=status_counts,
            window_audit=[
                {
                    "store_code": "UC01",
                    "from_date": "2024-02-01",
                    "to_date": "2024-02-02",
                    "status": "success",
                    "warning_count": 2,
                    "warning_categories": {"Customer GSTIN missing": 2},
                }
            ],
            ingestion_totals=profiler._init_ingestion_totals(),
            row_facts=_init_row_facts(),
        )

    async def fake_insert_run_summary(_database_url: str, summary_record: dict) -> None:
        persisted_summaries.append(summary_record)

    async def fake_persist_missing_windows_log_rows(**_kwargs: object) -> None:
        return None

    async def fake_send_notifications_for_run(_pipeline_name: str, _run_id: str) -> dict:
        return {"emails_planned": 1, "emails_sent": 1, "errors": []}

    monkeypatch.setattr(profiler, "_fetch_pipeline_id", fake_fetch_pipeline_id)
    monkeypatch.setattr(profiler, "_load_store_profiles", fake_load_store_profiles)
    monkeypatch.setattr(profiler, "_process_store", fake_process_store)
    monkeypatch.setattr(profiler, "insert_run_summary", fake_insert_run_summary)
    monkeypatch.setattr(profiler, "_persist_missing_windows_log_rows", fake_persist_missing_windows_log_rows)
    monkeypatch.setattr(profiler, "send_notifications_for_run", fake_send_notifications_for_run)

    await profiler.main(sync_group="UC", max_workers=1, run_env="test", run_id="profiler-uc-warning-run")

    summary = persisted_summaries[0]
    assert summary["overall_status"] == "success_with_warnings"
    assert summary["metrics_json"]["uc_warning_count"] == 2
    assert any(
        warning.startswith("UC_STORE_WARNINGS: 2 row-level warning(s)")
        for warning in summary["metrics_json"]["notification_payload"]["warnings"]
    )
    assert "Policy: warning windows and UC row-level warnings are non-fatal" in summary["summary_text"]


def test_extract_uc_warning_details_uses_archive_ingest_metrics_without_warning_rows() -> None:
    summary = {
        "metrics_json": {
            "stores_summary": {
                "stores": {
                    "UC567": {
                        "warning_count": 2,
                        "warning_rows": [],
                        "stage_metrics": {
                            "archive_ingest": {
                                "files": {
                                    "base": {
                                        "warnings": 2,
                                        "warning_breakdown": {"status_normalized": 2},
                                        "warning_samples": {
                                            "status_normalized": [
                                                {
                                                    "warning_code": "status_normalized:UPI/Wallet App->UPI_WALLET_APP",
                                                    "store_code": "UC567",
                                                    "source_file": "base.xlsx",
                                                    "row_locator": "row:2",
                                                }
                                            ]
                                        },
                                    }
                                }
                            }
                        },
                    }
                }
            }
        }
    }

    details = profiler._extract_uc_warning_details_from_summary(summary, store_code="uc567")

    assert details["warning_count"] == 2
    assert details["warning_categories"] == {"status_normalized": 2}
    assert details["warning_samples"] == {
        "status_normalized": ["status_normalized:UPI/Wallet App->UPI_WALLET_APP"]
    }
    assert details["warning_rows"] == []
