from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path


def _write_executable(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def test_cron_reports_fail_when_daily_sales_fails(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir(parents=True, exist_ok=True)
    fake_poetry = fake_bin / "poetry"
    fake_poetry.write_text(
        "#!/usr/bin/env bash\n"
        "if [[ \"$*\" == *\"report daily-sales\"* ]]; then exit 1; fi\n"
        "exit 0\n"
    )
    fake_poetry.chmod(0o755)

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{fake_bin}:{env.get('PATH', '')}",
            "CRON_HOME": str(tmp_path),
            "CRON_PATH": str(fake_bin),
            "DAILY_MAX_ATTEMPTS": "1",
            "MTD_SAME_DAY_MAX_ATTEMPTS": "1",
            "PENDING_MAX_ATTEMPTS": "1",
            "ORDERS_MAX_ATTEMPTS": "1",
            "DAILY_RESCUE_AFTER_PENDING_SUCCESS": "0",
        }
    )

    result = subprocess.run(
        ["bash", "scripts/cron_run_orders_and_reports.sh"],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0


def test_cron_does_not_retry_for_undefined_function_error(tmp_path: Path) -> None:
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
        "#!/usr/bin/env bash\n"
        "COUNT_FILE=\"${TMPDIR:-/tmp}/daily-call-count\"\n"
        "count=0\n"
        "[[ -f \"${COUNT_FILE}\" ]] && count=$(cat \"${COUNT_FILE}\")\n"
        "count=$((count + 1))\n"
        "printf '%s' \"${count}\" > \"${COUNT_FILE}\"\n"
        "echo 'psycopg2.errors.UndefinedFunction: function does not exist' >&2\n"
        "exit 1\n",
    )

    env = os.environ.copy()
    env.update(
        {
            "DAILY_MAX_ATTEMPTS": "5",
            "DAILY_RETRY_DELAY_SECONDS": "0",
            "ORDERS_MAX_ATTEMPTS": "1",
            "MTD_SAME_DAY_MAX_ATTEMPTS": "1",
            "PENDING_MAX_ATTEMPTS": "1",
            "DAILY_RESCUE_AFTER_PENDING_SUCCESS": "0",
            "TMPDIR": str(tmp_path),
        }
    )

    result = subprocess.run(
        ["bash", str(scripts_dir / "cron_run_orders_and_reports.sh")],
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
    assert "attempt 2/5 starting" not in log_text


def test_cron_mtd_same_day_command_regenerates_without_force_static() -> None:
    source_cron = Path("scripts/cron_run_orders_and_reports.sh").read_text(encoding="utf-8")

    mtd_lines = [
        line
        for line in source_cron.splitlines()
        if "mtd_same_day_fulfillment_report" in line
        and "run_local_reports_mtd_same_day_fulfillment.sh" in line
    ]

    assert len(mtd_lines) == 1
    mtd_command_line = mtd_lines[0]
    assert "MTD_SAME_DAY_REGENERATE_ARGS" not in mtd_command_line
    assert 'MTD_SAME_DAY_REGENERATE_ARGS=("--force")' not in source_cron
    assert "report CLIs always regenerate; --force is not required" in source_cron


def test_cron_mtd_same_day_receives_no_force_flag(tmp_path: Path) -> None:
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
    _write_executable(scripts_dir / "run_local_reports_daily_sales.sh", "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(scripts_dir / "run_local_reports_pending_deliveries.sh", "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(
        scripts_dir / "run_local_reports_mtd_same_day_fulfillment.sh",
        "#!/usr/bin/env bash\n"
        "printf '%s\n' \"$*\" > \"${TMPDIR}/mtd-args\"\n"
        "exit 0\n",
    )

    env = os.environ.copy()
    env.update(
        {
            "DAILY_MAX_ATTEMPTS": "1",
            "ORDERS_MAX_ATTEMPTS": "1",
            "MTD_SAME_DAY_MAX_ATTEMPTS": "1",
            "PENDING_MAX_ATTEMPTS": "1",
            "DAILY_RESCUE_AFTER_PENDING_SUCCESS": "0",
            "TMPDIR": str(tmp_path),
        }
    )

    result = subprocess.run(
        ["bash", str(scripts_dir / "cron_run_orders_and_reports.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert (tmp_path / "mtd-args").read_text(encoding="utf-8").strip() == ""

    log_files = sorted(logs_dir.glob("cron_run_orders_and_reports_*.log"))
    assert log_files
    log_text = log_files[-1].read_text(encoding="utf-8")
    assert "Script 3: mtd_same_day_fulfillment_report: attempt 1/1 starting" in log_text
    assert "regenerate=true" in log_text
    assert "report CLIs always regenerate; --force is not required" in log_text
