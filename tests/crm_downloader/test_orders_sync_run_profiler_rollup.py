from __future__ import annotations

from app.crm_downloader.orders_sync_run_profiler.main import (
    _init_status_counts,
    _merge_status_counts,
    _resolve_window_outcome_status,
    _rollup_overall_status,
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
