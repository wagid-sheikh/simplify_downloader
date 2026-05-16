from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path


def _write_executable(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def test_pending_deliveries_cron_path_is_explicitly_forced() -> None:
    cron_source = Path("scripts/cron_run_orders_and_reports.sh").read_text(encoding="utf-8")
    local_pending_source = Path("scripts/run_local_reports_pending_deliveries.sh").read_text(
        encoding="utf-8"
    )

    assert "successful run summary cannot skip regeneration" in cron_source
    assert 'PENDING_DELIVERIES_REGENERATE_ARGS=("--force")' in cron_source
    assert (
        'run_local_reports_pending_deliveries.sh ${PENDING_DELIVERIES_REGENERATE_ARGS[*]}'
        in cron_source
    )
    assert "pending-deliveries --env prod --force" in local_pending_source


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
    assert "ERROR: One or more required report pipelines failed" in log_text

    args_lines = (tmp_path / "daily-args.log").read_text(encoding="utf-8").splitlines()
    assert len(args_lines) == 4
    assert all("--force" in line for line in args_lines)


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


def test_cron_always_appends_force_flag_to_daily_sales(tmp_path: Path) -> None:
    repo_root = tmp_path
    scripts_dir = repo_root / "scripts"
    logs_dir = repo_root / "logs"
    tmp_dir = repo_root / "tmp"
    scripts_dir.mkdir(parents=True)
    logs_dir.mkdir()
    tmp_dir.mkdir()

    source_cron = Path("scripts/cron_run_orders_and_reports.sh").read_text(encoding="utf-8")
    _write_executable(scripts_dir / "cron_run_orders_and_reports.sh", source_cron)

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
    assert daily_invocations == ["--force"]
    assert pending_invocations == ["--force"]


def test_cron_retries_preserve_mandatory_regeneration(tmp_path: Path) -> None:
    repo_root = tmp_path
    scripts_dir = repo_root / "scripts"
    logs_dir = repo_root / "logs"
    tmp_dir = repo_root / "tmp"
    scripts_dir.mkdir(parents=True)
    logs_dir.mkdir()
    tmp_dir.mkdir()

    source_cron = Path("scripts/cron_run_orders_and_reports.sh").read_text(encoding="utf-8")
    _write_executable(scripts_dir / "cron_run_orders_and_reports.sh", source_cron)

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
    assert all("--force" in line for line in args_lines)
