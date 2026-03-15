from __future__ import annotations

from app.crm_downloader.orders_sync_run_profiler.main import (
    _accumulate_ingestion_totals,
    _extract_ingestion_counts_from_log,
    _extract_ingestion_counts_from_summary,
    _init_status_counts,
    _merge_ingestion_counts,
    _merge_status_counts,
    _resolve_window_outcome_status,
    _rollup_overall_status,
    _select_summary_overall_status,
)


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
