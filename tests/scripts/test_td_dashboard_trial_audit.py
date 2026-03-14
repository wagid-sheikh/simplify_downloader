from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def _write_log(path: Path, events: list[dict[str, object]]) -> None:
    path.write_text("\n".join(json.dumps(event) for event in events) + "\n", encoding="utf-8")


def test_audit_summarizes_multiple_runs_and_passes_gate(tmp_path: Path) -> None:
    run_log = tmp_path / "run_log.txt"
    events = [
        {
            "run_id": "run-1",
            "store_code": "A668",
            "phase": "api",
            "source_mode": "api_only",
            "trial_attempted": True,
            "trial_success": True,
            "fallback_used": False,
            "runtime_delta_ms": 120,
            "context_source": "dashboard_only",
        },
        {
            "run_id": "run-1",
            "store_code": "A668",
            "phase": "garment_ingest",
            "source_mode": "api_only",
            "garments_health": {
                "pages_attempted": 12,
                "timeout_count": 0,
                "retry_success_count": 0,
                "final_row_count": 1750,
                "orphan_rows": 2,
            },
        },
        {
            "run_id": "run-2",
            "store_code": "A817",
            "phase": "api",
            "source_mode": "api_only",
            "trial_attempted": True,
            "trial_success": True,
            "fallback_used": False,
            "runtime_delta_ms": 135,
            "context_source": "dashboard_only",
        },
        {
            "run_id": "run-2",
            "store_code": "A817",
            "phase": "garment_ingest",
            "source_mode": "api_only",
            "garments_health": {
                "pages_attempted": 11,
                "timeout_count": 0,
                "retry_success_count": 1,
                "final_row_count": 1748,
                "orphan_rows": 3,
            },
        },
    ]
    _write_log(run_log, events)

    result = subprocess.run(
        [
            sys.executable,
            "scripts/td_dashboard_trial_audit.py",
            "--run-log",
            str(run_log),
            "--max-runs",
            "2",
            "--min-successful-stores",
            "2",
            "--min-store-success-rate",
            "80",
            "--min-health-samples",
            "1",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "Gate met: eligible for default-path promotion." in result.stdout
    assert "| A668 |" in result.stdout
    assert "| A817 |" in result.stdout


def test_audit_blocks_promotion_when_success_rate_gate_fails(tmp_path: Path) -> None:
    run_log = tmp_path / "run_log.txt"
    events = [
        {
            "run_id": "run-1",
            "store_code": "A668",
            "phase": "api",
            "source_mode": "api_only",
            "trial_attempted": True,
            "trial_success": False,
            "fallback_used": True,
            "runtime_delta_ms": 190,
            "context_source": "iframe_fallback",
        },
        {
            "run_id": "run-1",
            "store_code": "A668",
            "phase": "api",
            "source_mode": "api_only",
            "endpoint_errors": {"/garments/details": "garments_wall_time_budget_cutoff"},
        },
        {
            "run_id": "run-1",
            "store_code": "A668",
            "phase": "garment_ingest",
            "source_mode": "api_only",
            "garments_health": {
                "pages_attempted": 12,
                "timeout_count": 2,
                "retry_success_count": 0,
                "final_row_count": 1750,
                "orphan_rows": 20,
            },
        },
    ]
    _write_log(run_log, events)

    result = subprocess.run(
        [
            sys.executable,
            "scripts/td_dashboard_trial_audit.py",
            "--run-log",
            str(run_log),
            "--max-runs",
            "1",
            "--min-successful-stores",
            "1",
            "--min-store-success-rate",
            "80",
            "--min-health-samples",
            "1",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "Gate NOT met: keep default path unchanged." in result.stdout


def test_audit_blocks_promotion_when_garments_stability_gate_fails(tmp_path: Path) -> None:
    run_log = tmp_path / "run_log.txt"
    events = [
        {
            "run_id": "run-1",
            "store_code": "A668",
            "phase": "api",
            "source_mode": "api_only",
            "trial_attempted": True,
            "trial_success": True,
            "fallback_used": False,
            "runtime_delta_ms": 120,
            "context_source": "dashboard_only",
            "endpoint_errors": {"/garments/details": "garments_wall_time_budget_cutoff"},
        },
        {
            "run_id": "run-1",
            "store_code": "A668",
            "phase": "garment_ingest",
            "source_mode": "api_only",
            "garments_health": {
                "pages_attempted": 12,
                "timeout_count": 1,
                "retry_success_count": 0,
                "final_row_count": 1750,
                "orphan_rows": 1,
            },
        },
        {
            "run_id": "run-2",
            "store_code": "A668",
            "phase": "api",
            "source_mode": "api_only",
            "trial_attempted": True,
            "trial_success": True,
            "fallback_used": False,
            "runtime_delta_ms": 130,
            "context_source": "dashboard_only",
            "endpoint_errors": {"/garments/details": "garments_wall_time_budget_cutoff"},
        },
        {
            "run_id": "run-2",
            "store_code": "A668",
            "phase": "garment_ingest",
            "source_mode": "api_only",
            "garments_health": {
                "pages_attempted": 12,
                "timeout_count": 1,
                "retry_success_count": 0,
                "final_row_count": 1750,
                "orphan_rows": 25,
            },
        },
    ]
    _write_log(run_log, events)

    result = subprocess.run(
        [
            sys.executable,
            "scripts/td_dashboard_trial_audit.py",
            "--run-log",
            str(run_log),
            "--max-runs",
            "2",
            "--min-successful-stores",
            "1",
            "--min-store-success-rate",
            "80",
            "--max-repeated-garments-degrade-events",
            "0",
            "--max-orphan-drift",
            "5",
            "--min-health-samples",
            "2",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "Gate NOT met: garments stability criteria failed for stores A668." in result.stdout
    assert "garments_wall_time_budget_cutoff" in result.stdout
