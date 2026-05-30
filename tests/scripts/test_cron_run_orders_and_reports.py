from __future__ import annotations

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


def test_pending_deliveries_cron_path_always_regenerates_without_force_gate() -> None:
    cron_source = Path("scripts/cron_run_orders_and_reports.sh").read_text(encoding="utf-8")
    local_pending_source = Path("scripts/run_local_reports_pending_deliveries.sh").read_text(
        encoding="utf-8"
    )

    assert "report CLIs always regenerate" in cron_source
    assert "PENDING_DELIVERIES_REGENERATE_ARGS" not in cron_source
    assert 'run_local_reports_pending_deliveries.sh"' in cron_source
    assert "pending-deliveries --env prod --force" not in local_pending_source
    assert "pending-deliveries --env prod" in local_pending_source


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
exit 0
""",
    )
    _write_executable(
        scripts_dir / "run_local_reports_daily_sales.sh",
        """#!/usr/bin/env bash
printf 'daily ran\n' >> "${TMPDIR:-/tmp}/daily-ran.log"
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

    log_files = sorted(logs_dir.glob("cron_run_orders_and_reports_*.log"))
    assert log_files
    log_text = log_files[-1].read_text(encoding="utf-8")
    assert "tcp_connectivity_preflight_failed_summary exit_code=7" in log_text
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
