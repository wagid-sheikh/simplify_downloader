from __future__ import annotations

import os
import shutil
import signal
import stat
import subprocess
import time
from pathlib import Path

import pytest

_SOURCE_SCRIPT = Path("scripts/kill_orders_and_reports_stale.sh")
_LOCK_NAME = "cron_run_orders_and_reports.lock"


def _write_executable(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def _prepare_script(tmp_path: Path) -> tuple[Path, Path, Path]:
    scripts_dir = tmp_path / "scripts"
    lock_dir = tmp_path / "tmp" / _LOCK_NAME
    scripts_dir.mkdir()
    lock_dir.parent.mkdir()
    recovery_script = scripts_dir / _SOURCE_SCRIPT.name
    shutil.copy2(_SOURCE_SCRIPT, recovery_script)
    return recovery_script, scripts_dir, lock_dir


def _start_repo_process(scripts_dir: Path) -> subprocess.Popen[str]:
    controlled_process = scripts_dir / "controlled_orders_and_reports.sh"
    _write_executable(
        controlled_process,
        "#!/usr/bin/env bash\n"
        "trap 'exit 0' TERM INT\n"
        "while true; do sleep 1; done\n",
    )
    process = subprocess.Popen(
        [str(controlled_process)],
        start_new_session=True,
        text=True,
    )
    return process


def _write_lock(
    lock_dir: Path,
    *,
    pid: int | str,
    pgid: int | str,
    command: str,
    started_at: str = "2026-05-31 12:00:00 UTC",
) -> None:
    lock_dir.mkdir()
    (lock_dir / "pid").write_text(f"{pid}\n", encoding="utf-8")
    (lock_dir / "pgid").write_text(f"{pgid}\n", encoding="utf-8")
    (lock_dir / "command").write_text(f"{command}\n", encoding="utf-8")
    (lock_dir / "started_at").write_text(f"{started_at}\n", encoding="utf-8")


def _run_recovery(
    recovery_script: Path, **env_overrides: str
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.update(env_overrides)
    return subprocess.run(
        [str(recovery_script)],
        cwd=recovery_script.parent.parent,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def _stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is None:
        os.killpg(process.pid, signal.SIGKILL)
    process.wait(timeout=5)


def test_dry_run_inspects_valid_lock_directory_without_terminating_process(
    tmp_path: Path,
) -> None:
    recovery_script, scripts_dir, lock_dir = _prepare_script(tmp_path)
    process = _start_repo_process(scripts_dir)
    try:
        _write_lock(
            lock_dir, pid=process.pid, pgid=process.pid, command=str(process.args[0])
        )

        result = _run_recovery(recovery_script)

        assert result.returncode == 0, result.stderr + result.stdout
        assert "DRY_RUN=1 FORCE=0" in result.stdout
        assert f"Before snapshot for PGID {process.pid}:" in result.stdout
        assert "Dry run only: FORCE!=1, no termination executed." in result.stdout
        assert process.poll() is None
        assert lock_dir.is_dir()
    finally:
        _stop_process(process)


def test_force_terminates_controlled_repo_process_group_and_removes_lock(
    tmp_path: Path,
) -> None:
    recovery_script, scripts_dir, lock_dir = _prepare_script(tmp_path)
    process = _start_repo_process(scripts_dir)
    try:
        _write_lock(
            lock_dir, pid=process.pid, pgid=process.pid, command=str(process.args[0])
        )

        result = _run_recovery(
            recovery_script,
            FORCE="1",
            TERM_WAIT_SECONDS="0",
            KILL_WAIT_SECONDS="0",
        )
        process.wait(timeout=5)

        assert result.returncode == 0, result.stderr + result.stdout
        assert f"Sending TERM to process group -{process.pid}" in result.stdout
        assert f"After snapshot for PGID {process.pid}:" in result.stdout
        assert "is gone; removing lock directory." in result.stdout
        assert not lock_dir.exists()
    finally:
        _stop_process(process)


def test_rejects_unrelated_lock_command_without_terminating_process(
    tmp_path: Path,
) -> None:
    recovery_script, scripts_dir, lock_dir = _prepare_script(tmp_path)
    process = _start_repo_process(scripts_dir)
    try:
        _write_lock(
            lock_dir, pid=process.pid, pgid=process.pid, command="/usr/bin/sleep 999"
        )

        result = _run_recovery(recovery_script, FORCE="1")

        assert result.returncode == 0, result.stderr + result.stdout
        assert "lock command does not belong to repository" in result.stdout
        assert "Sending TERM" not in result.stdout
        assert process.poll() is None
        assert lock_dir.is_dir()
    finally:
        _stop_process(process)


@pytest.mark.parametrize(
    ("metadata_name", "metadata_value", "message"),
    [
        ("pid", "not-a-pid", "Skipping: non-numeric PID [not-a-pid]."),
        ("pgid", "not-a-pgid", "Skipping: non-numeric PGID [not-a-pgid]."),
    ],
)
def test_rejects_non_numeric_process_metadata(
    tmp_path: Path, metadata_name: str, metadata_value: str, message: str
) -> None:
    recovery_script, scripts_dir, lock_dir = _prepare_script(tmp_path)
    process = _start_repo_process(scripts_dir)
    try:
        metadata = {"pid": process.pid, "pgid": process.pid}
        metadata[metadata_name] = metadata_value
        _write_lock(lock_dir, command=str(process.args[0]), **metadata)

        result = _run_recovery(recovery_script, FORCE="1")

        assert result.returncode == 0, result.stderr + result.stdout
        assert message in result.stdout
        assert "Sending TERM" not in result.stdout
        assert process.poll() is None
        assert lock_dir.is_dir()
    finally:
        _stop_process(process)


@pytest.mark.parametrize("missing_name", ["pid", "pgid", "command", "started_at"])
def test_rejects_lock_directory_with_missing_metadata(
    tmp_path: Path, missing_name: str
) -> None:
    recovery_script, scripts_dir, lock_dir = _prepare_script(tmp_path)
    process = _start_repo_process(scripts_dir)
    try:
        _write_lock(
            lock_dir, pid=process.pid, pgid=process.pid, command=str(process.args[0])
        )
        (lock_dir / missing_name).unlink()

        result = _run_recovery(recovery_script, FORCE="1")

        assert result.returncode == 0, result.stderr + result.stdout
        assert f"missing metadata file {lock_dir / missing_name}" in result.stderr
        assert "Sending TERM" not in result.stdout
        assert process.poll() is None
        assert lock_dir.is_dir()
    finally:
        _stop_process(process)


def test_dry_run_retains_stale_lock_after_process_group_exits(tmp_path: Path) -> None:
    recovery_script, scripts_dir, lock_dir = _prepare_script(tmp_path)
    process = _start_repo_process(scripts_dir)
    command = str(process.args[0])
    pgid = process.pid
    _stop_process(process)
    time.sleep(0.05)
    _write_lock(lock_dir, pid=pgid, pgid=pgid, command=command)

    result = _run_recovery(recovery_script)

    assert result.returncode == 0, result.stderr + result.stdout
    assert f"Before snapshot for PGID {pgid}:" in result.stdout
    assert (
        f"Dry run only: process group {pgid} is gone; stale lock directory retained."
        in result.stdout
    )
    assert f"After snapshot for PGID {pgid}:" in result.stdout
    assert lock_dir.is_dir()


def test_removes_stale_lock_after_process_group_exits(tmp_path: Path) -> None:
    recovery_script, scripts_dir, lock_dir = _prepare_script(tmp_path)
    process = _start_repo_process(scripts_dir)
    command = str(process.args[0])
    pgid = process.pid
    _stop_process(process)
    time.sleep(0.05)
    _write_lock(lock_dir, pid=pgid, pgid=pgid, command=command)

    result = _run_recovery(recovery_script, FORCE="1")

    assert result.returncode == 0, result.stderr + result.stdout
    assert f"Before snapshot for PGID {pgid}:" in result.stdout
    assert f"Process group {pgid} is gone; removing lock directory." in result.stdout
    assert f"After snapshot for PGID {pgid}:" in result.stdout
    assert not lock_dir.exists()
