from __future__ import annotations

import json
import os
import signal
import stat
import subprocess
import time
from pathlib import Path

import pytest


def _write_executable(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def _write_successful_preflight(scripts_dir: Path) -> None:
    _write_executable(
        scripts_dir / "orders_sync_connectivity_preflight.sh",
        "#!/usr/bin/env bash\n"
        "echo 'orders_sync_preflight_summary classification=tcp_ok_app_ok exit_code=0'\n"
        "echo 'tcp_connectivity_preflight_succeeded classification=tcp_ok_app_ok'\n"
        "exit 0\n",
    )


def _run_pending_deliveries_wrapper(tmp_path: Path, *args: str) -> list[str]:
    repo_root = tmp_path
    scripts_dir = repo_root / "scripts"
    bin_dir = repo_root / "bin"
    scripts_dir.mkdir()
    bin_dir.mkdir()

    source_wrapper = Path("scripts/run_local_reports_pending_deliveries.sh").read_text(
        encoding="utf-8"
    )
    wrapper = scripts_dir / "run_local_reports_pending_deliveries.sh"
    _write_executable(wrapper, source_wrapper)
    _write_executable(
        bin_dir / "poetry",
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        "printf '%s\\n' \"$*\" >> \"${POETRY_ARGS_LOG}\"\n",
    )

    args_log = tmp_path / "poetry-args.log"
    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{bin_dir}:{env['PATH']}",
            "POETRY_ARGS_LOG": str(args_log),
        }
    )

    result = subprocess.run(
        [str(wrapper), *args],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr + result.stdout
    return args_log.read_text(encoding="utf-8").splitlines()


def test_pending_deliveries_wrapper_keeps_upstream_flags_out_of_recovery_step(
    tmp_path: Path,
) -> None:
    invocations = _run_pending_deliveries_wrapper(
        tmp_path,
        "--orders-sync-upstream-status",
        "success",
        "--orders-sync-upstream-run-id",
        "profiler-123",
    )

    assert invocations == [
        "run python -m app recovery mark-aged-pending-deliveries --env prod",
        "run python -m app report pending-deliveries --env prod "
        "--orders-sync-upstream-status success "
        "--orders-sync-upstream-run-id profiler-123",
    ]


def test_pending_deliveries_wrapper_runs_with_zero_recovery_args_under_strict_shell_mode(
    tmp_path: Path,
) -> None:
    invocations = _run_pending_deliveries_wrapper(tmp_path)

    assert invocations == [
        "run python -m app recovery mark-aged-pending-deliveries --env prod",
        "run python -m app report pending-deliveries --env prod",
    ]


_CRON_UPSTREAM_ARGS = (
    "--orders-sync-upstream-status",
    "success_with_warnings",
    "--orders-sync-upstream-run-id",
    "profiler-cron-smoke",
)


def _run_report_wrapper_through_app(
    tmp_path: Path, wrapper_name: str, *args: str
) -> tuple[subprocess.CompletedProcess[str], list[dict[str, object]]]:
    bin_dir = tmp_path / "bin"
    python_path = tmp_path / "pythonpath"
    bin_dir.mkdir()
    python_path.mkdir()
    invocation_log = tmp_path / "report-adapter-invocations.jsonl"

    _write_executable(
        bin_dir / "poetry",
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        "[[ \"$1\" == \"run\" ]]\n"
        "shift\n"
        "exec \"$@\"\n",
    )
    (python_path / "sitecustomize.py").write_text(
        """
import json
import os
from pathlib import Path

from app.recovery import main as recovery_main
from app.reports.daily_sales_report import pipeline as daily_pipeline
from app.reports.pending_deliveries import pipeline as pending_pipeline


def _record(pipeline, **kwargs):
    path = Path(os.environ["REPORT_ADAPTER_INVOCATION_LOG"])
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"pipeline": pipeline, **kwargs}, default=str) + "\\n")


async def _fake_recovery_run(report_date, env):
    _record("recovery", report_date=report_date, env=env)


async def _fake_pending_run(
    report_date,
    env,
    force,
    orders_sync_upstream_status=None,
    orders_sync_upstream_run_id=None,
):
    _record(
        "pending-deliveries",
        report_date=report_date,
        env=env,
        force=force,
        orders_sync_upstream_status=orders_sync_upstream_status,
        orders_sync_upstream_run_id=orders_sync_upstream_run_id,
    )


async def _fake_daily_run(
    report_date,
    env,
    force,
    orders_sync_upstream_status=None,
    orders_sync_upstream_run_id=None,
):
    _record(
        "daily-sales",
        report_date=report_date,
        env=env,
        force=force,
        orders_sync_upstream_status=orders_sync_upstream_status,
        orders_sync_upstream_run_id=orders_sync_upstream_run_id,
    )


recovery_main._run = _fake_recovery_run
pending_pipeline._run = _fake_pending_run
daily_pipeline._run = _fake_daily_run
""".lstrip(),
        encoding="utf-8",
    )

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{bin_dir}:{env['PATH']}",
            "PYTHONPATH": os.pathsep.join(
                filter(None, [str(python_path), str(Path.cwd()), env.get("PYTHONPATH")])
            ),
            "REPORT_ADAPTER_INVOCATION_LOG": str(invocation_log),
        }
    )
    result = subprocess.run(
        [str(Path("scripts") / wrapper_name), *args],
        cwd=Path.cwd(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    invocations = (
        [json.loads(line) for line in invocation_log.read_text(encoding="utf-8").splitlines()]
        if invocation_log.exists()
        else []
    )
    return result, invocations


def _assert_wrapper_smoke_succeeded(result: subprocess.CompletedProcess[str]) -> None:
    output = f"{result.stdout}\n{result.stderr}"
    assert result.returncode == 0, output
    assert "TypeError" not in output
    assert "usage: app" not in output
    assert "unrecognized arguments" not in output
    assert "unbound variable" not in output


def test_pending_deliveries_wrapper_reaches_async_pipeline_with_cron_upstream_args(
    tmp_path: Path,
) -> None:
    result, invocations = _run_report_wrapper_through_app(
        tmp_path, "run_local_reports_pending_deliveries.sh", *_CRON_UPSTREAM_ARGS
    )

    _assert_wrapper_smoke_succeeded(result)
    assert [invocation["pipeline"] for invocation in invocations] == [
        "recovery",
        "pending-deliveries",
    ]
    pending_invocation = invocations[1]
    assert pending_invocation.pop("report_date")
    assert pending_invocation == {
        "pipeline": "pending-deliveries",
        "env": "prod",
        "force": False,
        "orders_sync_upstream_status": "success_with_warnings",
        "orders_sync_upstream_run_id": "profiler-cron-smoke",
    }


def test_daily_sales_wrapper_reaches_async_pipeline_with_cron_upstream_args(
    tmp_path: Path,
) -> None:
    result, invocations = _run_report_wrapper_through_app(
        tmp_path, "run_local_reports_daily_sales.sh", *_CRON_UPSTREAM_ARGS
    )

    _assert_wrapper_smoke_succeeded(result)
    assert len(invocations) == 1
    daily_invocation = invocations[0]
    assert daily_invocation.pop("report_date")
    assert daily_invocation == {
        "pipeline": "daily-sales",
        "env": "prod",
        "force": False,
        "orders_sync_upstream_status": "success_with_warnings",
        "orders_sync_upstream_run_id": "profiler-cron-smoke",
    }


def test_pending_deliveries_cron_path_always_regenerates_without_force_gate() -> None:
    cron_source = Path("scripts/cron_run_orders_and_reports.sh").read_text(encoding="utf-8")
    local_pending_source = Path("scripts/run_local_reports_pending_deliveries.sh").read_text(
        encoding="utf-8"
    )

    assert "report CLIs always regenerate" in cron_source
    assert 'ORDERS_MAX_ATTEMPTS="${ORDERS_MAX_ATTEMPTS:-3}"' in cron_source
    assert 'ORDERS_RETRY_DELAY_SECONDS="${ORDERS_RETRY_DELAY_SECONDS:-30}"' in cron_source
    assert "PENDING_DELIVERIES_REGENERATE_ARGS" not in cron_source
    assert 'run_local_reports_pending_deliveries.sh"' in cron_source
    assert "pending-deliveries --env prod --force" not in local_pending_source
    assert "pending-deliveries --env prod" in local_pending_source
    assert '${recovery_args[@]+"${recovery_args[@]}"}' in local_pending_source
    assert '--env prod "${recovery_args[@]}"' not in local_pending_source


def test_cron_marks_environment_and_cli_errors_as_deterministic() -> None:
    cron_source = Path("scripts/cron_run_orders_and_reports.sh").read_text(encoding="utf-8")

    assert "unbound variable" in cron_source
    assert "usage: app" in cron_source
    assert "error: unrecognized arguments" in cron_source
    assert "No such file or directory" in cron_source
    assert "Poetry could not find a pyproject\\.toml" in cron_source


def test_cron_returns_non_zero_when_daily_fails_even_if_rescue_succeeds(tmp_path: Path) -> None:
    repo_root = tmp_path
    scripts_dir = repo_root / "scripts"
    logs_dir = repo_root / "logs"
    tmp_dir = repo_root / "tmp"
    scripts_dir.mkdir(parents=True)
    logs_dir.mkdir()
    tmp_dir.mkdir()

    source_cron = Path("scripts/cron_run_orders_and_reports.sh").read_text(encoding="utf-8")
    _write_executable(scripts_dir / "cron_run_orders_and_reports.sh", source_cron)
    _write_successful_preflight(scripts_dir)

    _write_executable(
        scripts_dir / "orders_sync_run_profiler.sh",
        "#!/usr/bin/env bash\nexit 0\n",
    )
    _write_executable(
        scripts_dir / "run_local_reports_mtd_same_day_fulfillment.sh",
        "#!/usr/bin/env bash\nexit 0\n",
    )
    _write_executable(
        scripts_dir / "run_local_reports_pending_deliveries.sh",
        "#!/usr/bin/env bash\nexit 0\n",
    )
    _write_executable(
        scripts_dir / "run_local_reports_daily_sales.sh",
        "#!/usr/bin/env bash\n"
        "COUNT_FILE=\"${TMPDIR:-/tmp}/daily-call-count\"\n"
        "count=0\n"
        "[[ -f \"${COUNT_FILE}\" ]] && count=$(cat \"${COUNT_FILE}\")\n"
        "count=$((count + 1))\n"
        "printf '%s' \"${count}\" > \"${COUNT_FILE}\"\n"
        "printf '%s\\n' \"$*\" >> \"${TMPDIR:-/tmp}/daily-args.log\"\n"
        "if [[ \"${count}\" -le 3 ]]; then exit 1; fi\n"
        "exit 0\n",
    )

    env = os.environ.copy()
    env.update(
        {
            "ORDERS_MAX_ATTEMPTS": "1",
            "DAILY_MAX_ATTEMPTS": "3",
            "MTD_SAME_DAY_MAX_ATTEMPTS": "1",
            "PENDING_MAX_ATTEMPTS": "1",
            "DAILY_RESCUE_AFTER_PENDING_SUCCESS": "1",
            "DAILY_RESCUE_MAX_ATTEMPTS": "1",
            "TMPDIR": str(tmp_path),
        }
    )

    result = subprocess.run(
        [str(scripts_dir / "cron_run_orders_and_reports.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1

    log_files = sorted(logs_dir.glob("cron_run_orders_and_reports_*.log"))
    assert log_files
    log_text = log_files[-1].read_text(encoding="utf-8")
    assert "daily_sales_report_rc=1" in log_text
    assert "daily_sales_report_rescue_rc=0" in log_text
    assert "ERROR: One or more required cron steps failed" in log_text

    args_lines = (tmp_path / "daily-args.log").read_text(encoding="utf-8").splitlines()
    assert len(args_lines) == 4
    assert all("--force" not in line for line in args_lines)


def test_cron_fail_fast_on_deterministic_code_error(tmp_path: Path) -> None:
    repo_root = tmp_path
    scripts_dir = repo_root / "scripts"
    logs_dir = repo_root / "logs"
    tmp_dir = repo_root / "tmp"
    scripts_dir.mkdir(parents=True)
    logs_dir.mkdir()
    tmp_dir.mkdir()

    source_cron = Path("scripts/cron_run_orders_and_reports.sh").read_text(encoding="utf-8")
    _write_executable(scripts_dir / "cron_run_orders_and_reports.sh", source_cron)
    _write_successful_preflight(scripts_dir)

    _write_executable(scripts_dir / "orders_sync_run_profiler.sh", "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(
        scripts_dir / "run_local_reports_mtd_same_day_fulfillment.sh",
        "#!/usr/bin/env bash\nexit 0\n",
    )
    _write_executable(scripts_dir / "run_local_reports_pending_deliveries.sh", "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(
        scripts_dir / "run_local_reports_daily_sales.sh",
        "#!/usr/bin/env bash\n"
        "COUNT_FILE=\"${TMPDIR:-/tmp}/daily-call-count\"\n"
        "count=0\n"
        "[[ -f \"${COUNT_FILE}\" ]] && count=$(cat \"${COUNT_FILE}\")\n"
        "count=$((count + 1))\n"
        "printf '%s' \"${count}\" > \"${COUNT_FILE}\"\n"
        "echo 'TypeError: unsupported operand type(s) for +: int and str' >&2\n"
        "exit 1\n",
    )

    env = os.environ.copy()
    env.update(
        {
            "DAILY_MAX_ATTEMPTS": "4",
            "DAILY_RETRY_DELAY_SECONDS": "0",
            "ORDERS_MAX_ATTEMPTS": "1",
            "MTD_SAME_DAY_MAX_ATTEMPTS": "1",
            "PENDING_MAX_ATTEMPTS": "1",
            "DAILY_RESCUE_AFTER_PENDING_SUCCESS": "0",
            "TMPDIR": str(tmp_path),
        }
    )

    result = subprocess.run(
        [str(scripts_dir / "cron_run_orders_and_reports.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert (tmp_path / "daily-call-count").read_text(encoding="utf-8") == "1"

    log_files = sorted(logs_dir.glob("cron_run_orders_and_reports_*.log"))
    assert log_files
    log_text = log_files[-1].read_text(encoding="utf-8")
    assert "deterministic code, environment, or CLI error detected; failing fast without retries" in log_text
    assert "retry_skipped_reason=deterministic_environment_or_cli_error" in log_text
    assert "failure_class=deterministic_environment_or_cli_error; retry_skipped=true" in log_text
    assert "attempt 2/4 starting" not in log_text


def test_cron_preflight_failure_skips_orders_sync_but_runs_reports(tmp_path: Path) -> None:
    repo_root = tmp_path
    scripts_dir = repo_root / "scripts"
    logs_dir = repo_root / "logs"
    tmp_dir = repo_root / "tmp"
    scripts_dir.mkdir(parents=True)
    logs_dir.mkdir()
    tmp_dir.mkdir()

    source_cron = Path("scripts/cron_run_orders_and_reports.sh").read_text(encoding="utf-8")
    _write_executable(scripts_dir / "cron_run_orders_and_reports.sh", source_cron)
    _write_executable(
        scripts_dir / "orders_sync_connectivity_preflight.sh",
        """#!/usr/bin/env bash
echo 'orders_sync_preflight_summary classification=tcp_failed exit_code=7' >&2
echo 'tcp_connectivity_preflight_failed_summary exit_code=7' >&2
exit 7
""",
    )
    _write_executable(
        scripts_dir / "orders_sync_run_profiler.sh",
        """#!/usr/bin/env bash
printf 'orders sync should not run\n' >> "${TMPDIR:-/tmp}/orders-sync-invoked.log"
exit 0
""",
    )
    _write_executable(
        scripts_dir / "run_local_reports_pending_deliveries.sh",
        """#!/usr/bin/env bash
printf 'pending ran\n' >> "${TMPDIR:-/tmp}/pending-ran.log"
printf '%s\n' "$*" >> "${TMPDIR:-/tmp}/pending-args.log"
exit 0
""",
    )
    _write_executable(
        scripts_dir / "run_local_reports_daily_sales.sh",
        """#!/usr/bin/env bash
printf 'daily ran\n' >> "${TMPDIR:-/tmp}/daily-ran.log"
printf '%s\n' "$*" >> "${TMPDIR:-/tmp}/daily-args.log"
exit 0
""",
    )

    env = os.environ.copy()
    env.update(
        {
            "ORDERS_MAX_ATTEMPTS": "1",
            "ORDERS_PREFLIGHT_MAX_ATTEMPTS": "3",
            "ORDERS_PREFLIGHT_RETRY_DELAY_SECONDS": "0",
            "DAILY_MAX_ATTEMPTS": "1",
            "PENDING_MAX_ATTEMPTS": "1",
            "DAILY_RESCUE_AFTER_PENDING_SUCCESS": "0",
            "TMPDIR": str(tmp_path),
        }
    )

    result = subprocess.run(
        [str(scripts_dir / "cron_run_orders_and_reports.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert not (tmp_path / "orders-sync-invoked.log").exists()
    assert (tmp_path / "daily-ran.log").read_text(encoding="utf-8") == "daily ran\n"
    assert (tmp_path / "pending-ran.log").read_text(encoding="utf-8") == "pending ran\n"
    daily_args = (tmp_path / "daily-args.log").read_text(encoding="utf-8")
    pending_args = (tmp_path / "pending-args.log").read_text(encoding="utf-8")
    assert "--orders-sync-upstream-status failed" in daily_args
    assert "--orders-sync-upstream-status failed" in pending_args

    log_files = sorted(logs_dir.glob("cron_run_orders_and_reports_*.log"))
    assert log_files
    log_text = log_files[-1].read_text(encoding="utf-8")
    assert log_text.count("tcp_connectivity_preflight_failed_summary exit_code=7") == 3
    assert "failure_class=connectivity_preflight_failure" in log_text
    assert "retry_decision=exhausted" in log_text
    assert "orders_sync_run_profiler skipped because tcp_connectivity_preflight failed" in log_text
    assert "orders_sync_run_profiler_rc=7" in log_text
    assert "Running Script 1: orders_sync_run_profiler" not in log_text


def test_cron_runs_reports_without_force_flags(tmp_path: Path) -> None:
    repo_root = tmp_path
    scripts_dir = repo_root / "scripts"
    logs_dir = repo_root / "logs"
    tmp_dir = repo_root / "tmp"
    scripts_dir.mkdir(parents=True)
    logs_dir.mkdir()
    tmp_dir.mkdir()

    source_cron = Path("scripts/cron_run_orders_and_reports.sh").read_text(encoding="utf-8")
    _write_executable(scripts_dir / "cron_run_orders_and_reports.sh", source_cron)
    _write_successful_preflight(scripts_dir)

    _write_executable(scripts_dir / "orders_sync_run_profiler.sh", "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(
        scripts_dir / "run_local_reports_mtd_same_day_fulfillment.sh",
        "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >> \"${TMPDIR:-/tmp}/mtd-args.log\"\nexit 0\n",
    )
    _write_executable(
        scripts_dir / "run_local_reports_pending_deliveries.sh",
        "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >> \"${TMPDIR:-/tmp}/pending-args.log\"\nexit 0\n",
    )
    _write_executable(
        scripts_dir / "run_local_reports_daily_sales.sh",
        "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >> \"${TMPDIR:-/tmp}/daily-args.log\"\nexit 0\n",
    )

    env = os.environ.copy()
    env.update(
        {
            "ORDERS_MAX_ATTEMPTS": "1",
            "DAILY_MAX_ATTEMPTS": "1",
            "MTD_SAME_DAY_MAX_ATTEMPTS": "1",
            "PENDING_MAX_ATTEMPTS": "1",
            "DAILY_RESCUE_AFTER_PENDING_SUCCESS": "0",
            "TMPDIR": str(tmp_path),
        }
    )

    result = subprocess.run(
        [str(scripts_dir / "cron_run_orders_and_reports.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    daily_invocations = (tmp_path / "daily-args.log").read_text(encoding="utf-8").splitlines()
    pending_invocations = (tmp_path / "pending-args.log").read_text(encoding="utf-8").splitlines()
    assert daily_invocations == [""]
    assert pending_invocations == [""]


def test_cron_retries_preserve_mandatory_regeneration_without_force(tmp_path: Path) -> None:
    repo_root = tmp_path
    scripts_dir = repo_root / "scripts"
    logs_dir = repo_root / "logs"
    tmp_dir = repo_root / "tmp"
    scripts_dir.mkdir(parents=True)
    logs_dir.mkdir()
    tmp_dir.mkdir()

    source_cron = Path("scripts/cron_run_orders_and_reports.sh").read_text(encoding="utf-8")
    _write_executable(scripts_dir / "cron_run_orders_and_reports.sh", source_cron)
    _write_successful_preflight(scripts_dir)

    _write_executable(scripts_dir / "orders_sync_run_profiler.sh", "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(scripts_dir / "run_local_reports_mtd_same_day_fulfillment.sh", "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(scripts_dir / "run_local_reports_pending_deliveries.sh", "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(
        scripts_dir / "run_local_reports_daily_sales.sh",
        """#!/usr/bin/env bash
COUNT_FILE="${TMPDIR:-/tmp}/daily-count"
count=0
[[ -f "${COUNT_FILE}" ]] && count=$(cat "${COUNT_FILE}")
count=$((count + 1))
printf '%s' "${count}" > "${COUNT_FILE}"
printf '%s\n' "$*" >> "${TMPDIR:-/tmp}/daily-args.log"
if [[ "${count}" -eq 1 ]]; then exit 1; fi
exit 0
""",
    )

    env = os.environ.copy()
    env.update(
        {
            "ORDERS_MAX_ATTEMPTS": "1",
            "DAILY_MAX_ATTEMPTS": "2",
            "DAILY_RETRY_DELAY_SECONDS": "0",
            "MTD_SAME_DAY_MAX_ATTEMPTS": "1",
            "PENDING_MAX_ATTEMPTS": "1",
            "DAILY_RESCUE_AFTER_PENDING_SUCCESS": "0",
            "TMPDIR": str(tmp_path),
        }
    )

    result = subprocess.run(
        [str(scripts_dir / "cron_run_orders_and_reports.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    args_lines = (tmp_path / "daily-args.log").read_text(encoding="utf-8").splitlines()
    assert len(args_lines) == 2
    assert all("--force" not in line for line in args_lines)


def test_cron_logs_app_layer_preflight_failure_classification(tmp_path: Path) -> None:
    repo_root = tmp_path
    scripts_dir = repo_root / "scripts"
    logs_dir = repo_root / "logs"
    tmp_dir = repo_root / "tmp"
    scripts_dir.mkdir(parents=True)
    logs_dir.mkdir()
    tmp_dir.mkdir()

    source_cron = Path("scripts/cron_run_orders_and_reports.sh").read_text(encoding="utf-8")
    _write_executable(scripts_dir / "cron_run_orders_and_reports.sh", source_cron)
    _write_executable(
        scripts_dir / "orders_sync_connectivity_preflight.sh",
        """#!/usr/bin/env bash
echo 'tcp_connectivity_preflight_dns_ok target_host=example.test latency_ms=1'
echo 'tcp_connectivity_preflight_tcp_ok target_host=example.test latency_ms=2'
echo 'app_layer_preflight_http_failed target_host=example.test status_code=500 response_class=5xx expected_classes=2xx,3xx,4xx latency_ms=3' >&2
echo 'orders_sync_preflight_summary classification=app_layer_failed exit_code=1' >&2
echo 'app_layer_preflight_failed_summary exit_code=1' >&2
exit 1
""",
    )
    _write_executable(
        scripts_dir / "orders_sync_run_profiler.sh",
        """#!/usr/bin/env bash
printf 'orders sync should not run\n' >> "${TMPDIR:-/tmp}/orders-sync-invoked.log"
exit 0
""",
    )
    _write_executable(scripts_dir / "run_local_reports_pending_deliveries.sh", "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(scripts_dir / "run_local_reports_daily_sales.sh", "#!/usr/bin/env bash\nexit 0\n")

    env = os.environ.copy()
    env.update(
        {
            "ORDERS_MAX_ATTEMPTS": "1",
            "ORDERS_PREFLIGHT_MAX_ATTEMPTS": "1",
            "ORDERS_PREFLIGHT_RETRY_DELAY_SECONDS": "0",
            "DAILY_MAX_ATTEMPTS": "1",
            "PENDING_MAX_ATTEMPTS": "1",
            "DAILY_RESCUE_AFTER_PENDING_SUCCESS": "0",
            "TMPDIR": str(tmp_path),
        }
    )

    result = subprocess.run(
        [str(scripts_dir / "cron_run_orders_and_reports.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert not (tmp_path / "orders-sync-invoked.log").exists()

    log_files = sorted(logs_dir.glob("cron_run_orders_and_reports_*.log"))
    assert log_files
    log_text = log_files[-1].read_text(encoding="utf-8")
    assert "tcp_connectivity_preflight_tcp_ok" in log_text
    assert "app_layer_preflight_http_failed" in log_text
    assert "orders_sync_preflight_classification=app_layer_failed" in log_text
    assert "orders sync tcp_connectivity_preflight completed" not in log_text
    assert "tcp_connectivity_preflight_succeeded" not in log_text


@pytest.mark.parametrize(
    "failure_output",
    [
        "./scripts/orders_sync_run_profiler.sh: line 4: REQUIRED_PATH: unbound variable",
        "usage: app [-h]\\napp: error: unrecognized arguments: --obsolete-option",
    ],
    ids=["unbound-variable", "argparse-unrecognized-arguments"],
)
def test_cron_does_not_retry_deterministic_environment_or_cli_errors(
    tmp_path: Path, failure_output: str
) -> None:
    repo_root = tmp_path
    scripts_dir = repo_root / "scripts"
    logs_dir = repo_root / "logs"
    tmp_dir = repo_root / "tmp"
    scripts_dir.mkdir(parents=True)
    logs_dir.mkdir()
    tmp_dir.mkdir()

    source_cron = Path("scripts/cron_run_orders_and_reports.sh").read_text(encoding="utf-8")
    _write_executable(scripts_dir / "cron_run_orders_and_reports.sh", source_cron)
    _write_successful_preflight(scripts_dir)
    _write_executable(
        scripts_dir / "orders_sync_run_profiler.sh",
        "#!/usr/bin/env bash\n"
        "COUNT_FILE=\"${TMPDIR:-/tmp}/orders-call-count\"\n"
        "count=0\n"
        "[[ -f \"${COUNT_FILE}\" ]] && count=$(cat \"${COUNT_FILE}\")\n"
        "count=$((count + 1))\n"
        "printf '%s' \"${count}\" > \"${COUNT_FILE}\"\n"
        f"printf '%b\\n' '{failure_output}' >&2\n"
        "exit 1\n",
    )
    _write_executable(
        scripts_dir / "run_local_reports_pending_deliveries.sh", "#!/usr/bin/env bash\nexit 0\n"
    )
    _write_executable(
        scripts_dir / "run_local_reports_daily_sales.sh", "#!/usr/bin/env bash\nexit 0\n"
    )

    env = os.environ.copy()
    env.update(
        {
            "ORDERS_MAX_ATTEMPTS": "3",
            "ORDERS_RETRY_DELAY_SECONDS": "0",
            "ORDERS_RETRY_JITTER_SECONDS": "0",
            "DAILY_MAX_ATTEMPTS": "1",
            "PENDING_MAX_ATTEMPTS": "1",
            "DAILY_RESCUE_AFTER_PENDING_SUCCESS": "0",
            "TMPDIR": str(tmp_path),
        }
    )

    result = subprocess.run(
        [str(scripts_dir / "cron_run_orders_and_reports.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert (tmp_path / "orders-call-count").read_text(encoding="utf-8") == "1"

    log_files = sorted(logs_dir.glob("cron_run_orders_and_reports_*.log"))
    assert log_files
    log_text = log_files[-1].read_text(encoding="utf-8")
    assert "failure_class=deterministic_environment_or_cli_error; retry_skipped=true" in log_text
    assert "Script 1: orders_sync_run_profiler: attempt 2/3 starting" not in log_text


def test_cron_retries_transient_orders_profiler_failure(tmp_path: Path) -> None:
    repo_root = tmp_path
    scripts_dir = repo_root / "scripts"
    logs_dir = repo_root / "logs"
    tmp_dir = repo_root / "tmp"
    scripts_dir.mkdir(parents=True)
    logs_dir.mkdir()
    tmp_dir.mkdir()

    source_cron = Path("scripts/cron_run_orders_and_reports.sh").read_text(encoding="utf-8")
    _write_executable(scripts_dir / "cron_run_orders_and_reports.sh", source_cron)
    _write_successful_preflight(scripts_dir)
    _write_executable(
        scripts_dir / "orders_sync_run_profiler.sh",
        """#!/usr/bin/env bash
COUNT_FILE="${TMPDIR:-/tmp}/orders-call-count"
count=0
[[ -f "${COUNT_FILE}" ]] && count=$(cat "${COUNT_FILE}")
count=$((count + 1))
printf '%s' "${count}" > "${COUNT_FILE}"
if [[ "${count}" -le 2 ]]; then
  echo 'playwright._impl._errors.TimeoutError: Navigation timeout of 30000 ms exceeded' >&2
  exit 1
fi
echo '{"run_id":"orders-transient-success"}'
exit 0
""",
    )
    _write_executable(scripts_dir / "run_local_reports_pending_deliveries.sh", "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(scripts_dir / "run_local_reports_daily_sales.sh", "#!/usr/bin/env bash\nexit 0\n")

    env = os.environ.copy()
    env.update(
        {
            "ORDERS_MAX_ATTEMPTS": "3",
            "ORDERS_RETRY_DELAY_SECONDS": "1",
            "ORDERS_RETRY_JITTER_SECONDS": "0",
            "ORDERS_RETRY_BACKOFF_MULTIPLIER": "2",
            "ORDERS_RETRY_MAX_DELAY_SECONDS": "2",
            "DAILY_MAX_ATTEMPTS": "1",
            "PENDING_MAX_ATTEMPTS": "1",
            "DAILY_RESCUE_AFTER_PENDING_SUCCESS": "0",
            "TMPDIR": str(tmp_path),
        }
    )

    result = subprocess.run(
        [str(scripts_dir / "cron_run_orders_and_reports.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert (tmp_path / "orders-call-count").read_text(encoding="utf-8") == "3"

    log_files = sorted(logs_dir.glob("cron_run_orders_and_reports_*.log"))
    assert log_files
    log_text = log_files[-1].read_text(encoding="utf-8")
    assert "failure_class=transient_playwright_navigation_failure" in log_text
    assert "transient Playwright/navigation failure detected" in log_text
    assert "sleeping 1s before retry (base_delay_seconds=1, jitter_seconds=0, next_base_delay_seconds=2)" in log_text
    assert "sleeping 2s before retry (base_delay_seconds=2, jitter_seconds=0, next_base_delay_seconds=4)" in log_text
    assert "Script 1: orders_sync_run_profiler: attempt 3/3 starting" in log_text
    assert "orders_sync_run_profiler_rc=0" in log_text


def test_cron_does_not_retry_deterministic_orders_profiler_error(tmp_path: Path) -> None:
    repo_root = tmp_path
    scripts_dir = repo_root / "scripts"
    logs_dir = repo_root / "logs"
    tmp_dir = repo_root / "tmp"
    scripts_dir.mkdir(parents=True)
    logs_dir.mkdir()
    tmp_dir.mkdir()

    source_cron = Path("scripts/cron_run_orders_and_reports.sh").read_text(encoding="utf-8")
    _write_executable(scripts_dir / "cron_run_orders_and_reports.sh", source_cron)
    _write_successful_preflight(scripts_dir)
    _write_executable(
        scripts_dir / "orders_sync_run_profiler.sh",
        """#!/usr/bin/env bash
COUNT_FILE="${TMPDIR:-/tmp}/orders-call-count"
count=0
[[ -f "${COUNT_FILE}" ]] && count=$(cat "${COUNT_FILE}")
count=$((count + 1))
printf '%s' "${count}" > "${COUNT_FILE}"
echo 'SyntaxError: invalid syntax' >&2
exit 1
""",
    )
    _write_executable(scripts_dir / "run_local_reports_pending_deliveries.sh", "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(scripts_dir / "run_local_reports_daily_sales.sh", "#!/usr/bin/env bash\nexit 0\n")

    env = os.environ.copy()
    env.update(
        {
            "ORDERS_MAX_ATTEMPTS": "3",
            "ORDERS_RETRY_DELAY_SECONDS": "0",
            "ORDERS_RETRY_JITTER_SECONDS": "0",
            "DAILY_MAX_ATTEMPTS": "1",
            "PENDING_MAX_ATTEMPTS": "1",
            "DAILY_RESCUE_AFTER_PENDING_SUCCESS": "0",
            "TMPDIR": str(tmp_path),
        }
    )

    result = subprocess.run(
        [str(scripts_dir / "cron_run_orders_and_reports.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert (tmp_path / "orders-call-count").read_text(encoding="utf-8") == "1"

    log_files = sorted(logs_dir.glob("cron_run_orders_and_reports_*.log"))
    assert log_files
    log_text = log_files[-1].read_text(encoding="utf-8")
    assert "failure_class=deterministic_environment_or_cli_error; retry_skipped=true" in log_text
    assert "retry_skipped_reason=deterministic_environment_or_cli_error" in log_text
    assert "Script 1: orders_sync_run_profiler: attempt 2/3 starting" not in log_text


def test_cron_retries_persisted_profiler_failed_status(tmp_path: Path) -> None:
    repo_root = tmp_path
    scripts_dir = repo_root / "scripts"
    logs_dir = repo_root / "logs"
    tmp_dir = repo_root / "tmp"
    scripts_dir.mkdir(parents=True)
    logs_dir.mkdir()
    tmp_dir.mkdir()

    source_cron = Path("scripts/cron_run_orders_and_reports.sh").read_text(encoding="utf-8")
    _write_executable(scripts_dir / "cron_run_orders_and_reports.sh", source_cron)
    _write_successful_preflight(scripts_dir)
    _write_executable(
        scripts_dir / "orders_sync_run_profiler.sh",
        """#!/usr/bin/env bash
COUNT_FILE="${TMPDIR:-/tmp}/orders-call-count"
count=0
[[ -f "${COUNT_FILE}" ]] && count=$(cat "${COUNT_FILE}")
count=$((count + 1))
printf '%s' "${count}" > "${COUNT_FILE}"
if [[ "${count}" -eq 1 ]]; then
  echo 'orders sync final profiler overall_status=failed' >&2
  exit 1
fi
exit 0
""",
    )
    _write_executable(scripts_dir / "run_local_reports_pending_deliveries.sh", "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(scripts_dir / "run_local_reports_daily_sales.sh", "#!/usr/bin/env bash\nexit 0\n")

    env = os.environ.copy()
    env.update(
        {
            "ORDERS_MAX_ATTEMPTS": "2",
            "ORDERS_RETRY_DELAY_SECONDS": "0",
            "ORDERS_RETRY_JITTER_SECONDS": "0",
            "DAILY_MAX_ATTEMPTS": "1",
            "PENDING_MAX_ATTEMPTS": "1",
            "DAILY_RESCUE_AFTER_PENDING_SUCCESS": "0",
            "TMPDIR": str(tmp_path),
        }
    )

    result = subprocess.run(
        [str(scripts_dir / "cron_run_orders_and_reports.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert (tmp_path / "orders-call-count").read_text(encoding="utf-8") == "2"

    log_files = sorted(logs_dir.glob("cron_run_orders_and_reports_*.log"))
    assert log_files
    log_text = log_files[-1].read_text(encoding="utf-8")
    assert "failure_class=persisted_profiler_failed_status" in log_text
    assert "persisted profiler overall_status=failed" in log_text
    assert "Script 1: orders_sync_run_profiler: attempt 2/2 starting" in log_text


def _run_cron_with_profiler_events(tmp_path: Path, events: list[dict[str, object]]) -> str:
    repo_root = tmp_path
    scripts_dir = repo_root / "scripts"
    logs_dir = repo_root / "logs"
    tmp_dir = repo_root / "tmp"
    scripts_dir.mkdir(parents=True)
    logs_dir.mkdir()
    tmp_dir.mkdir()

    source_cron = Path("scripts/cron_run_orders_and_reports.sh").read_text(encoding="utf-8")
    _write_executable(scripts_dir / "cron_run_orders_and_reports.sh", source_cron)
    _write_successful_preflight(scripts_dir)

    event_lines = "\n".join(json.dumps(event) for event in events)
    _write_executable(
        scripts_dir / "orders_sync_run_profiler.sh",
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        "cat <<'JSON_EVENTS' | tee -a \"${JSON_LOG_FILE}\"\n"
        f"{event_lines}\n"
        "JSON_EVENTS\n"
        "exit 0\n",
    )
    _write_executable(
        scripts_dir / "run_local_reports_pending_deliveries.sh",
        "#!/usr/bin/env bash\n"
        "printf '%s\\n' \"$*\" >> \"${TMPDIR:-/tmp}/pending-args.log\"\n"
        "exit 0\n",
    )
    _write_executable(
        scripts_dir / "run_local_reports_daily_sales.sh",
        "#!/usr/bin/env bash\n"
        "printf '%s\\n' \"$*\" >> \"${TMPDIR:-/tmp}/daily-args.log\"\n"
        "exit 0\n",
    )

    env = os.environ.copy()
    env.update(
        {
            "ORDERS_MAX_ATTEMPTS": "1",
            "DAILY_MAX_ATTEMPTS": "1",
            "PENDING_MAX_ATTEMPTS": "1",
            "DAILY_RESCUE_AFTER_PENDING_SUCCESS": "0",
            "JSON_LOG_FILE": str(logs_dir / "simplify_downloader.jsonl"),
            "TMPDIR": str(tmp_path),
        }
    )

    result = subprocess.run(
        [str(scripts_dir / "cron_run_orders_and_reports.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr + result.stdout
    log_files = sorted(logs_dir.glob("cron_run_orders_and_reports_*.log"))
    assert log_files
    return log_files[-1].read_text(encoding="utf-8")


def test_orders_sync_observability_extracts_failed_td_stores_from_profiler_summary(
    tmp_path: Path,
) -> None:
    log_text = _run_cron_with_profiler_events(
        tmp_path,
        [
            {
                "phase": "store",
                "message": "child store failed",
                "run_id": "profiler-td-failed_TD001",
                "store_code": "TD001",
                "store_outcome": "failed",
            },
            {
                "phase": "summary",
                "message": "Orders sync profiler summary",
                "run_id": "profiler-td-failed",
                "overall_status": "failed",
                "status_counts": {"success": 2, "success_with_warnings": 0, "partial": 0, "failed": 1, "skipped": 0},
                "store_totals": {
                    "TD001": {
                        "pipeline_name": "td_orders_sync",
                        "overall_status": "failed",
                        "status_counts": {"success": 0, "success_with_warnings": 0, "partial": 0, "failed": 1, "skipped": 0},
                    },
                    "TD002": {
                        "pipeline_name": "td_orders_sync",
                        "overall_status": "success",
                        "status_counts": {"success": 2, "success_with_warnings": 0, "partial": 0, "failed": 0, "skipped": 0},
                    },
                },
            },
        ],
    )

    assert "orders_sync_profiler_run_id=profiler-td-failed" in log_text
    assert "orders_sync_overall_status=failed" in log_text
    assert "orders_sync_failed_stores=[TD001]" in log_text
    assert "profiler run_id=profiler-td-failed overall_status=failed failed_stores=[TD001]" in log_text


def test_orders_sync_observability_preserves_uc_skipped_timeout_windows(
    tmp_path: Path,
) -> None:
    log_text = _run_cron_with_profiler_events(
        tmp_path,
        [
            {
                "phase": "summary",
                "message": "Orders sync profiler summary",
                "run_id": "profiler-uc-skipped",
                "overall_status": "skipped",
                "status_counts": {"success": 0, "success_with_warnings": 0, "partial": 0, "failed": 0, "skipped": 1},
                "store_totals": {
                    "UC001": {
                        "pipeline_name": "uc_orders_sync",
                        "overall_status": "skipped",
                        "status_counts": {"success": 0, "success_with_warnings": 0, "partial": 0, "failed": 0, "skipped": 1},
                        "window_audit": [
                            {
                                "status": "skipped",
                                "error": "TimeoutError: Navigation timeout of 30000 ms exceeded",
                            }
                        ],
                    }
                },
            }
        ],
    )

    assert "orders_sync_profiler_run_id=profiler-uc-skipped" in log_text
    assert "orders_sync_overall_status=skipped" in log_text
    assert "orders_sync_failed_stores=[]" in log_text
    assert "--orders-sync-upstream-status skipped --orders-sync-upstream-run-id profiler-uc-skipped" in log_text


def test_orders_sync_observability_preserves_success_with_warnings_garment_summary(
    tmp_path: Path,
) -> None:
    log_text = _run_cron_with_profiler_events(
        tmp_path,
        [
            {
                "phase": "summary",
                "message": "Orders sync profiler summary",
                "run_id": "profiler-garment-warnings",
                "overall_status": "success_with_warnings",
                "status_counts": {"success": 2, "success_with_warnings": 1, "partial": 0, "failed": 0, "skipped": 0},
                "store_totals": {
                    "TD003": {
                        "pipeline_name": "td_orders_sync",
                        "overall_status": "success_with_warnings",
                        "status_counts": {"success": 0, "success_with_warnings": 1, "partial": 0, "failed": 0, "skipped": 0},
                        "td_garment_warning_count": 1,
                        "td_garment_incomplete_windows": [
                            {"from_date": "2026-05-01", "to_date": "2026-05-02", "garments_final_row_count": 42}
                        ],
                    }
                },
            }
        ],
    )

    assert "orders_sync_profiler_run_id=profiler-garment-warnings" in log_text
    assert "orders_sync_overall_status=success_with_warnings" in log_text
    assert "orders_sync_overall_status=unknown" not in log_text
    assert "orders_sync_failed_stores=[]" in log_text
    assert "--orders-sync-upstream-status success_with_warnings --orders-sync-upstream-run-id profiler-garment-warnings" in log_text


def test_orders_sync_observability_reports_unknown_when_no_profiler_summary_found(
    tmp_path: Path,
) -> None:
    log_text = _run_cron_with_profiler_events(
        tmp_path,
        [
            {"phase": "startup", "message": "profiler started", "run_id": "profiler-no-summary"},
            {
                "phase": "summary",
                "message": "Some other summary",
                "run_id": "profiler-no-summary",
                "overall_status": "success",
            },
        ],
    )

    assert "orders_sync_profiler_run_id=profiler-no-summary" in log_text
    assert "orders_sync_overall_status=unknown" in log_text
    assert "orders_sync_failed_stores=[]" in log_text


def test_pending_deliveries_wrapper_keeps_upstream_args_out_of_recovery(tmp_path: Path) -> None:
    repo_root = tmp_path
    scripts_dir = repo_root / "scripts"
    fake_bin = repo_root / "bin"
    scripts_dir.mkdir(parents=True)
    fake_bin.mkdir()

    wrapper_source = Path("scripts/run_local_reports_pending_deliveries.sh").read_text(encoding="utf-8")
    _write_executable(scripts_dir / "run_local_reports_pending_deliveries.sh", wrapper_source)
    _write_executable(
        fake_bin / "poetry",
        "#!/usr/bin/env bash\n"
        "printf '%s\n' \"$*\" >> \"${TMPDIR}/poetry-args.log\"\n"
        "exit 0\n",
    )

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{fake_bin}:{env.get('PATH', '')}",
            "TMPDIR": str(tmp_path),
        }
    )

    result = subprocess.run(
        [
            str(scripts_dir / "run_local_reports_pending_deliveries.sh"),
            "--report-date",
            "2026-04-29",
            "--orders-sync-upstream-status",
            "failed",
            "--orders-sync-upstream-run-id",
            "orders-run-1",
        ],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    args_lines = (tmp_path / "poetry-args.log").read_text(encoding="utf-8").splitlines()
    assert len(args_lines) == 2
    recovery_args, report_args = args_lines
    assert "recovery mark-aged-pending-deliveries" in recovery_args
    assert "--report-date 2026-04-29" in recovery_args
    assert "--orders-sync-upstream-status" not in recovery_args
    assert "--orders-sync-upstream-run-id" not in recovery_args
    assert "report pending-deliveries" in report_args
    assert "--orders-sync-upstream-status failed" in report_args
    assert "--orders-sync-upstream-run-id orders-run-1" in report_args


def _pid_is_non_zombie_alive(pid: int) -> bool:
    result = subprocess.run(
        ["ps", "-o", "state=", "-p", str(pid)],
        capture_output=True,
        text=True,
        check=False,
    )
    return bool(result.stdout.strip()) and not result.stdout.strip().startswith("Z")


def _replace_orders_process_group_helper(source: str, replacement: str) -> str:
    start = source.index("process_group_is_alive() {")
    end = source.index("\n}\n", start) + len("\n}\n")
    return source[:start] + replacement + source[end:]


def test_cron_terminates_timed_out_orders_group_releases_lock_and_runs_reports(
    tmp_path: Path,
) -> None:
    repo_root = tmp_path
    scripts_dir = repo_root / "scripts"
    logs_dir = repo_root / "logs"
    tmp_dir = repo_root / "tmp"
    scripts_dir.mkdir(parents=True)
    logs_dir.mkdir()
    tmp_dir.mkdir()

    source_cron = Path("scripts/cron_run_orders_and_reports.sh").read_text(encoding="utf-8")
    _write_executable(scripts_dir / "cron_run_orders_and_reports.sh", source_cron)
    _write_successful_preflight(scripts_dir)
    _write_executable(
        scripts_dir / "orders_sync_run_profiler.sh",
        "#!/usr/bin/env bash\n"
        "trap '' TERM\n"
        "(trap '' TERM; while :; do sleep 1; done) &\n"
        "printf '%s\n' \"$!\" > \"${TMPDIR:-/tmp}/orders-descendant-pid\"\n"
        "while :; do sleep 1; done\n",
    )
    _write_executable(
        scripts_dir / "run_local_reports_daily_sales.sh",
        "#!/usr/bin/env bash\nprintf 'daily ran\\n' > \"${TMPDIR:-/tmp}/daily-ran.log\"\nexit 0\n",
    )
    _write_executable(
        scripts_dir / "run_local_reports_pending_deliveries.sh",
        "#!/usr/bin/env bash\nprintf 'pending ran\\n' > \"${TMPDIR:-/tmp}/pending-ran.log\"\nexit 0\n",
    )

    env = os.environ.copy()
    env.update(
        {
            "ORDERS_MAX_ATTEMPTS": "1",
            "DAILY_MAX_ATTEMPTS": "1",
            "PENDING_MAX_ATTEMPTS": "1",
            "DAILY_RESCUE_AFTER_PENDING_SUCCESS": "0",
            "ORDERS_STEP_TIMEOUT_SECONDS": "1",
            "DAILY_SALES_STEP_TIMEOUT_SECONDS": "5",
            "PENDING_DELIVERIES_STEP_TIMEOUT_SECONDS": "5",
            "KILL_WAIT_SECONDS": "1",
            "TMPDIR": str(tmp_path),
        }
    )

    result = subprocess.run(
        [str(scripts_dir / "cron_run_orders_and_reports.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )

    assert result.returncode == 1
    assert (tmp_path / "daily-ran.log").read_text(encoding="utf-8").strip() == "daily ran"
    assert (tmp_path / "pending-ran.log").read_text(encoding="utf-8").strip() == "pending ran"
    descendant_pid = int((tmp_path / "orders-descendant-pid").read_text(encoding="utf-8").strip())
    assert not _pid_is_non_zombie_alive(descendant_pid)
    assert not (tmp_dir / "cron_run_orders_and_reports.lock").exists()
    assert not (tmp_dir / "cron_heavy_pipelines.lock").exists()

    log_files = sorted(logs_dir.glob("cron_run_orders_and_reports_*.log"))
    assert log_files
    log_text = log_files[-1].read_text(encoding="utf-8")
    assert "exceeded runtime_limit_seconds=1" in log_text
    assert "failure_class=step_runtime_timeout" in log_text
    assert "Script 1: orders_sync_run_profiler failed after 1 attempts" in log_text
    assert "orders_sync_run_profiler_rc=124" in log_text
    assert "Script 2: daily_sales_report: attempt 1/1 succeeded" in log_text


def test_cron_preserves_lock_and_aborts_when_timeout_group_verification_fails(
    tmp_path: Path,
) -> None:
    repo_root, scripts_dir = _prepare_minimal_orders_cron(tmp_path)
    source_path = scripts_dir / "cron_run_orders_and_reports.sh"
    source = source_path.read_text(encoding="utf-8")
    source = _replace_orders_process_group_helper(
        source,
        "process_group_is_alive() {\n  return 0\n}\n",
    )
    _write_executable(source_path, source)
    _write_executable(
        scripts_dir / "orders_sync_run_profiler.sh",
        "#!/usr/bin/env bash\nexec sleep 30\n",
    )
    _write_executable(
        scripts_dir / "run_local_reports_daily_sales.sh",
        "#!/usr/bin/env bash\nprintf 'daily ran\n' > \"${TMPDIR:-/tmp}/daily-ran.log\"\n",
    )
    _write_executable(
        scripts_dir / "run_local_reports_pending_deliveries.sh",
        "#!/usr/bin/env bash\nprintf 'pending ran\n' > \"${TMPDIR:-/tmp}/pending-ran.log\"\n",
    )
    env = os.environ.copy()
    env.update(
        {
            "ORDERS_MAX_ATTEMPTS": "1",
            "ORDERS_STEP_TIMEOUT_SECONDS": "1",
            "KILL_WAIT_SECONDS": "1",
            "TMPDIR": str(repo_root),
        }
    )

    result = subprocess.run(
        [str(source_path)],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )

    assert result.returncode == 1
    assert (repo_root / "tmp" / "cron_run_orders_and_reports.lock").is_dir()
    assert not (repo_root / "daily-ran.log").exists()
    assert not (repo_root / "pending-ran.log").exists()
    log_text = _latest_orders_log_text(repo_root)
    assert "still has non-zombie members after KILL" in log_text
    assert (
        "preserving local lock for explicit operator recovery and aborting wrapper safely" in log_text
    )
    assert "Timeout process-group handling is incomplete or failed verification" in log_text


def test_zombie_only_process_group_remnants_do_not_retain_orders_reports_lock(
    tmp_path: Path,
) -> None:
    repo_root, scripts_dir = _prepare_minimal_orders_cron(tmp_path)
    _write_executable(
        scripts_dir / "orders_sync_run_profiler.sh",
        "#!/usr/bin/env bash\n"
        "trap '' TERM\n"
        "(exit 0) &\n"
        "while :; do sleep 1; done\n",
    )
    env = os.environ.copy()
    env.update(
        {
            "ORDERS_MAX_ATTEMPTS": "1",
            "DAILY_MAX_ATTEMPTS": "1",
            "PENDING_MAX_ATTEMPTS": "1",
            "DAILY_RESCUE_AFTER_PENDING_SUCCESS": "0",
            "ORDERS_STEP_TIMEOUT_SECONDS": "1",
            "KILL_WAIT_SECONDS": "1",
            "TMPDIR": str(repo_root),
        }
    )

    result = subprocess.run(
        [str(scripts_dir / "cron_run_orders_and_reports.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )

    assert result.returncode == 1
    assert not (repo_root / "tmp" / "cron_run_orders_and_reports.lock").exists()
    assert "disappeared after KILL" in _latest_orders_log_text(repo_root)


def _write_orders_lock_metadata(
    lock_dir: Path,
    *,
    pid: int | str,
    pgid: int | str,
    started_epoch: int | None = None,
    command: str = "test-owner",
) -> None:
    lock_dir.mkdir(exist_ok=True)
    epoch = int(time.time()) if started_epoch is None else started_epoch
    (lock_dir / "pid").write_text(f"{pid}\n", encoding="utf-8")
    (lock_dir / "pgid").write_text(f"{pgid}\n", encoding="utf-8")
    (lock_dir / "started_at").write_text("2026-06-01 00:00:00 UTC\n", encoding="utf-8")
    (lock_dir / "started_at_epoch").write_text(f"{epoch}\n", encoding="utf-8")
    (lock_dir / "host").write_text("test-host\n", encoding="utf-8")
    (lock_dir / "command").write_text(f"{command}\n", encoding="utf-8")


def _wait_for_orders_path(path: Path, timeout_seconds: float = 5) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if path.exists():
            return
        time.sleep(0.05)
    raise AssertionError(f"Timed out waiting for {path}")


def _prepare_minimal_orders_cron(tmp_path: Path) -> tuple[Path, Path]:
    repo_root = tmp_path
    scripts_dir = repo_root / "scripts"
    (repo_root / "logs").mkdir()
    (repo_root / "tmp").mkdir()
    scripts_dir.mkdir()
    source = Path("scripts/cron_run_orders_and_reports.sh").read_text(encoding="utf-8")
    _write_executable(scripts_dir / "cron_run_orders_and_reports.sh", source)
    _write_successful_preflight(scripts_dir)
    for name in (
        "orders_sync_run_profiler.sh",
        "run_local_reports_daily_sales.sh",
        "run_local_reports_mtd_same_day_fulfillment.sh",
        "run_local_reports_pending_deliveries.sh",
    ):
        _write_executable(scripts_dir / name, "#!/usr/bin/env bash\nexit 0\n")
    return repo_root, scripts_dir


def _run_minimal_orders_cron(repo_root: Path, scripts_dir: Path) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.update(
        {
            "ORDERS_MAX_ATTEMPTS": "1",
            "DAILY_MAX_ATTEMPTS": "1",
            "MTD_SAME_DAY_MAX_ATTEMPTS": "1",
            "PENDING_MAX_ATTEMPTS": "1",
            "DAILY_RESCUE_AFTER_PENDING_SUCCESS": "0",
            "TMPDIR": str(repo_root),
        }
    )
    return subprocess.run(
        [str(scripts_dir / "cron_run_orders_and_reports.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_active_td_leads_lock_does_not_block_orders_reports(tmp_path: Path) -> None:
    repo_root, scripts_dir = _prepare_minimal_orders_cron(tmp_path)
    unrelated_lock = repo_root / "tmp" / "cron_run_td_leads_sync.lock"
    unrelated_lock.mkdir()
    (unrelated_lock / "pid").write_text(f"{os.getpid()}\n", encoding="utf-8")

    result = _run_minimal_orders_cron(repo_root, scripts_dir)

    assert result.returncode == 0, result.stderr + result.stdout
    assert unrelated_lock.is_dir()
    assert not (repo_root / "tmp" / "cron_heavy_pipelines.lock").exists()


def test_active_orders_reports_lock_blocks_second_orders_reports_instance(tmp_path: Path) -> None:
    repo_root, scripts_dir = _prepare_minimal_orders_cron(tmp_path)
    local_lock = repo_root / "tmp" / "cron_run_orders_and_reports.lock"
    _write_orders_lock_metadata(local_lock, pid=os.getpid(), pgid=os.getpgid(os.getpid()))

    result = _run_minimal_orders_cron(repo_root, scripts_dir)

    assert result.returncode == 0
    log_files = sorted((repo_root / "logs").glob("cron_run_orders_and_reports_*.log"))
    assert log_files
    assert "status=skipped_due_to_active_same_pipeline_owner" in log_files[-1].read_text(encoding="utf-8")
    assert local_lock.is_dir()


def test_orders_reports_wrapper_does_not_reference_retired_global_lock() -> None:
    source = Path("scripts/cron_run_orders_and_reports.sh").read_text(encoding="utf-8")

    assert "cron_heavy_pipelines.lock" not in source
    assert "GLOBAL_LOCK" not in source



def _latest_orders_log_text(repo_root: Path) -> str:
    log_files = sorted((repo_root / "logs").glob("cron_run_orders_and_reports_*.log"))
    assert log_files
    return log_files[-1].read_text(encoding="utf-8")


def test_dead_orders_reports_owner_lock_is_cleaned_up_and_reacquired(tmp_path: Path) -> None:
    repo_root, scripts_dir = _prepare_minimal_orders_cron(tmp_path)
    lock_dir = repo_root / "tmp" / "cron_run_orders_and_reports.lock"
    _write_orders_lock_metadata(lock_dir, pid=999999, pgid=999999)

    result = _run_minimal_orders_cron(repo_root, scripts_dir)

    assert result.returncode == 0, result.stderr + result.stdout
    assert "Owner PID=999999 and PGID=999999 are gone; removing stale lock" in _latest_orders_log_text(repo_root)


def test_malformed_orders_reports_lock_metadata_fails_safe_without_deletion(tmp_path: Path) -> None:
    repo_root, scripts_dir = _prepare_minimal_orders_cron(tmp_path)
    lock_dir = repo_root / "tmp" / "cron_run_orders_and_reports.lock"
    _write_orders_lock_metadata(lock_dir, pid="bad-pid", pgid="bad-pgid")

    result = _run_minimal_orders_cron(repo_root, scripts_dir)

    assert result.returncode == 1
    assert lock_dir.is_dir()
    assert "metadata is missing or malformed. Leaving lock untouched" in _latest_orders_log_text(repo_root)


def test_orders_reports_stale_unrelated_live_process_is_refused(tmp_path: Path) -> None:
    repo_root, scripts_dir = _prepare_minimal_orders_cron(tmp_path)
    lock_dir = repo_root / "tmp" / "cron_run_orders_and_reports.lock"
    owner = subprocess.Popen(["sleep", "30"], start_new_session=True)
    try:
        _write_orders_lock_metadata(lock_dir, pid=owner.pid, pgid=os.getpgid(owner.pid), started_epoch=int(time.time()) - 10)
        env = os.environ.copy()
        env.update({"ORDERS_REPORTS_STALE_OWNER_SECONDS": "0", "TMPDIR": str(repo_root)})

        result = subprocess.run([str(scripts_dir / "cron_run_orders_and_reports.sh")], cwd=repo_root, env=env, check=False)

        assert result.returncode == 1
        assert owner.poll() is None
        assert lock_dir.is_dir()
        assert "command does not belong to expected repository wrapper" in _latest_orders_log_text(repo_root)
    finally:
        os.killpg(owner.pid, signal.SIGKILL)
        owner.wait()


def test_orders_reports_stale_pid_pgid_mismatch_is_refused(tmp_path: Path) -> None:
    repo_root, scripts_dir = _prepare_minimal_orders_cron(tmp_path)
    lock_dir = repo_root / "tmp" / "cron_run_orders_and_reports.lock"
    owner = subprocess.Popen(["sleep", "30"], start_new_session=True)
    try:
        expected_wrapper = scripts_dir / "cron_run_orders_and_reports.sh"
        _write_orders_lock_metadata(lock_dir, pid=owner.pid, pgid=owner.pid + 1, started_epoch=int(time.time()) - 10, command=str(expected_wrapper))
        env = os.environ.copy()
        env.update({"ORDERS_REPORTS_STALE_OWNER_SECONDS": "0", "TMPDIR": str(repo_root)})

        result = subprocess.run([str(expected_wrapper)], cwd=repo_root, env=env, check=False)

        assert result.returncode == 1
        assert owner.poll() is None
        assert lock_dir.is_dir()
        assert "PID/PGID mismatch" in _latest_orders_log_text(repo_root)
    finally:
        os.killpg(owner.pid, signal.SIGKILL)
        owner.wait()


def test_orders_reports_stale_owner_group_is_terminated_and_lock_reacquired(tmp_path: Path) -> None:
    repo_root, scripts_dir = _prepare_minimal_orders_cron(tmp_path)
    lock_dir = repo_root / "tmp" / "cron_run_orders_and_reports.lock"
    _write_executable(scripts_dir / "orders_sync_run_profiler.sh", "#!/usr/bin/env bash\nif mkdir \"${TMPDIR:-/tmp}/hold-once\" 2>/dev/null; then exec sleep 30; fi\nexit 0\n")
    env = os.environ.copy()
    env.update({"ORDERS_REPORTS_STALE_OWNER_SECONDS": "0", "STALE_OWNER_TERM_WAIT_SECONDS": "2", "STALE_OWNER_KILL_WAIT_SECONDS": "2", "ORDERS_MAX_ATTEMPTS": "1", "DAILY_MAX_ATTEMPTS": "1", "MTD_SAME_DAY_MAX_ATTEMPTS": "1", "PENDING_MAX_ATTEMPTS": "1", "DAILY_RESCUE_AFTER_PENDING_SUCCESS": "0", "TMPDIR": str(repo_root)})
    owner = subprocess.Popen([str(scripts_dir / "cron_run_orders_and_reports.sh")], cwd=repo_root, env=env, start_new_session=True)
    try:
        _wait_for_orders_path(lock_dir / "pid")
        result = subprocess.run([str(scripts_dir / "cron_run_orders_and_reports.sh")], cwd=repo_root, env=env, check=False, timeout=10)
        owner.wait(timeout=5)

        assert result.returncode == 0
        assert not lock_dir.exists()
        assert "Confirmed stale-owner process group is gone" in _latest_orders_log_text(repo_root)
    finally:
        if owner.poll() is None:
            os.killpg(owner.pid, signal.SIGKILL)
            owner.wait()


def test_rapid_orders_reports_invocations_do_not_run_concurrently(tmp_path: Path) -> None:
    repo_root, scripts_dir = _prepare_minimal_orders_cron(tmp_path)
    lock_dir = repo_root / "tmp" / "cron_run_orders_and_reports.lock"
    _write_executable(scripts_dir / "orders_sync_run_profiler.sh", "#!/usr/bin/env bash\nexec sleep 30\n")
    env = os.environ.copy()
    env.update({"ORDERS_MAX_ATTEMPTS": "1", "TMPDIR": str(repo_root)})
    owner = subprocess.Popen([str(scripts_dir / "cron_run_orders_and_reports.sh")], cwd=repo_root, env=env, start_new_session=True)
    try:
        _wait_for_orders_path(lock_dir / "pid")
        results = [subprocess.run([str(scripts_dir / "cron_run_orders_and_reports.sh")], cwd=repo_root, env=env, check=False) for _ in range(3)]
        assert [result.returncode for result in results] == [0, 0, 0]
        assert owner.poll() is None
        assert lock_dir.is_dir()
        assert "status=skipped_due_to_active_same_pipeline_owner" in _latest_orders_log_text(repo_root)
    finally:
        os.killpg(owner.pid, signal.SIGKILL)
        owner.wait()


def _run_cron_with_preflight(
    tmp_path: Path,
    preflight_body: str,
    *,
    preflight_attempts: str = "3",
) -> tuple[subprocess.CompletedProcess[str], str]:
    scripts_dir = tmp_path / "scripts"
    logs_dir = tmp_path / "logs"
    tmp_dir = tmp_path / "tmp"
    scripts_dir.mkdir(parents=True)
    logs_dir.mkdir()
    tmp_dir.mkdir()

    source_cron = Path("scripts/cron_run_orders_and_reports.sh").read_text(encoding="utf-8")
    _write_executable(scripts_dir / "cron_run_orders_and_reports.sh", source_cron)
    _write_executable(scripts_dir / "orders_sync_connectivity_preflight.sh", preflight_body)
    _write_executable(
        scripts_dir / "orders_sync_run_profiler.sh",
        "#!/usr/bin/env bash\nprintf 'orders profiler ran\\n' >> \"${TMPDIR:-/tmp}/orders-ran.log\"\nexit 0\n",
    )
    _write_executable(
        scripts_dir / "run_local_reports_daily_sales.sh",
        "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >> \"${TMPDIR:-/tmp}/daily-args.log\"\nexit 0\n",
    )
    _write_executable(
        scripts_dir / "run_local_reports_pending_deliveries.sh",
        "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >> \"${TMPDIR:-/tmp}/pending-args.log\"\nexit 0\n",
    )

    env = os.environ.copy()
    env.update(
        {
            "ORDERS_MAX_ATTEMPTS": "1",
            "ORDERS_PREFLIGHT_MAX_ATTEMPTS": preflight_attempts,
            "ORDERS_PREFLIGHT_RETRY_DELAY_SECONDS": "0",
            "DAILY_MAX_ATTEMPTS": "1",
            "PENDING_MAX_ATTEMPTS": "1",
            "DAILY_RESCUE_AFTER_PENDING_SUCCESS": "0",
            "TMPDIR": str(tmp_path),
        }
    )
    result = subprocess.run(
        [str(scripts_dir / "cron_run_orders_and_reports.sh")],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    log_files = sorted(logs_dir.glob("cron_run_orders_and_reports_*.log"))
    assert log_files
    return result, log_files[-1].read_text(encoding="utf-8")


def _transient_then_success_preflight(classification: str, failure_class: str) -> str:
    return f'''#!/usr/bin/env bash
COUNT_FILE="${{TMPDIR:-/tmp}}/preflight-count"
count=0
[[ -f "${{COUNT_FILE}}" ]] && count=$(cat "${{COUNT_FILE}}")
count=$((count + 1))
printf '%s' "${{count}}" > "${{COUNT_FILE}}"
if [[ "${{count}}" -eq 1 ]]; then
  echo 'orders_sync_preflight_host_result target_host=example.test status=failed failure_class={failure_class} exit_code=1'
  echo 'orders_sync_preflight_summary classification={classification} failure_class={failure_class} exit_code=1'
  exit 1
fi
echo 'orders_sync_preflight_host_result target_host=example.test status=passed failure_class=none exit_code=0'
echo 'orders_sync_preflight_summary classification=tcp_ok_app_ok failure_class=none exit_code=0'
exit 0
'''


def test_cron_retries_dns_preflight_failure_then_runs_profiler(tmp_path: Path) -> None:
    result, log_text = _run_cron_with_preflight(
        tmp_path,
        _transient_then_success_preflight("dns_resolution_failed", "dns_resolution_failure"),
    )

    assert result.returncode == 0
    assert (tmp_path / "preflight-count").read_text(encoding="utf-8") == "2"
    assert (tmp_path / "orders-ran.log").read_text(encoding="utf-8") == "orders profiler ran\n"
    assert "classification=dns_resolution_failed failure_class=dns_resolution_failed exit_code=1 retry_decision=retry" in log_text
    assert "attempt=2/3 status=passed classification=tcp_ok_app_ok retry_decision=not_needed" in log_text


def test_cron_retries_tcp_timeout_preflight_failure_then_runs_profiler(tmp_path: Path) -> None:
    result, log_text = _run_cron_with_preflight(
        tmp_path,
        _transient_then_success_preflight("tcp_connection_timeout", "tcp_connection_timeout"),
    )

    assert result.returncode == 0
    assert (tmp_path / "preflight-count").read_text(encoding="utf-8") == "2"
    assert (tmp_path / "orders-ran.log").read_text(encoding="utf-8") == "orders profiler ran\n"
    assert "classification=tcp_connection_timeout failure_class=tcp_connection_timeout exit_code=1 retry_decision=retry" in log_text


def test_cron_does_not_retry_deterministic_preflight_failure(tmp_path: Path) -> None:
    result, log_text = _run_cron_with_preflight(
        tmp_path,
        "#!/usr/bin/env bash\n"
        "printf 'called\\n' >> \"${TMPDIR:-/tmp}/preflight-calls.log\"\n"
        "echo 'orders_sync_preflight_summary classification=deterministic_configuration_failed failure_class=deterministic_configuration_failure detail=invalid_http_scheme exit_code=2'\n"
        "exit 2\n",
    )

    assert result.returncode == 1
    assert (tmp_path / "preflight-calls.log").read_text(encoding="utf-8") == "called\n"
    assert not (tmp_path / "orders-ran.log").exists()
    assert "classification=deterministic_configuration_failed" in log_text
    assert "retry_decision=fail_fast" in log_text
    assert "attempt=2/3" not in log_text
