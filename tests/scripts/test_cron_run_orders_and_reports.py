from __future__ import annotations

import json
import os
import stat
import subprocess
from pathlib import Path


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
    assert "deterministic code error detected; failing fast without retries" in log_text
    assert "retry_skipped_reason=deterministic_code_error" in log_text
    assert "failure_class=deterministic_code_failure" in log_text
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
    assert "tcp_connectivity_preflight_failed_summary exit_code=7" in log_text
    assert "failure_class=connectivity_preflight_failure" in log_text
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
if [[ "${count}" -eq 1 ]]; then
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
    assert "failure_class=transient_playwright_navigation_failure" in log_text
    assert "transient Playwright/navigation failure detected" in log_text
    assert "Script 1: orders_sync_run_profiler: attempt 2/2 starting" in log_text
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
    assert "failure_class=deterministic_code_failure" in log_text
    assert "retry_skipped_reason=deterministic_code_error" in log_text
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
