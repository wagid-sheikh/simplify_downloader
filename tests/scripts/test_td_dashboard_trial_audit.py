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
        }
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
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "Gate NOT met: keep default path unchanged." in result.stdout
