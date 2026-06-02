from __future__ import annotations

import os
import shutil
import signal
import stat
import subprocess
import time
from pathlib import Path

import pytest

_SOURCE_SCRIPT = Path("scripts/inspect_or_kill_pipeline_stale.sh")
_LEGACY_WRAPPER = Path("scripts/kill_orders_and_reports_stale.sh")
_PIPELINE_LOCKS = {
    "td-leads": "cron_run_td_leads_sync.lock",
    "orders-reports": "cron_run_orders_and_reports.lock",
}


def _write_executable(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def _prepare_scripts(tmp_path: Path) -> tuple[Path, Path, Path]:
    scripts_dir = tmp_path / "scripts"
    scripts_dir.mkdir()
    (tmp_path / "tmp").mkdir()
    recovery_script = scripts_dir / _SOURCE_SCRIPT.name
    legacy_wrapper = scripts_dir / _LEGACY_WRAPPER.name
    shutil.copy2(_SOURCE_SCRIPT, recovery_script)
    shutil.copy2(_LEGACY_WRAPPER, legacy_wrapper)
    return recovery_script, legacy_wrapper, scripts_dir


def _lock_dir(tmp_path: Path, pipeline: str) -> Path:
    return tmp_path / "tmp" / _PIPELINE_LOCKS[pipeline]


def _start_repo_process(
    scripts_dir: Path, *, ignore_term: bool = False
) -> subprocess.Popen[str]:
    controlled_process = scripts_dir / "controlled_pipeline.sh"
    term_trap = "trap '' TERM INT" if ignore_term else "trap 'exit 0' TERM INT"
    _write_executable(
        controlled_process,
        f"#!/usr/bin/env bash\n{term_trap}\nwhile true; do sleep 1; done\n",
    )
    return subprocess.Popen(
        [str(controlled_process)],
        start_new_session=True,
        text=True,
    )


def _write_lock(
    lock_dir: Path,
    *,
    pid: int | str,
    pgid: int | str,
    command: str,
    started_at: str = "2026-05-31 12:00:00 UTC",
    host: str = "test-host",
    cwd: str | None = None,
) -> None:
    lock_dir.mkdir()
    metadata = {
        "pid": pid,
        "pgid": pgid,
        "command": command,
        "started_at": started_at,
        "host": host,
        "cwd": cwd or str(lock_dir.parent.parent),
    }
    for name, value in metadata.items():
        (lock_dir / name).write_text(f"{value}\n", encoding="utf-8")


def _run_recovery(
    recovery_script: Path, pipeline: str | None = None, **env_overrides: str
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.update(env_overrides)
    args = [str(recovery_script)]
    if pipeline is not None:
        args.append(pipeline)
    return subprocess.run(
        args,
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


@pytest.mark.parametrize("pipeline", ["td-leads", "orders-reports"])
def test_dry_run_inspects_pipeline_lock_without_terminating_process(
    tmp_path: Path, pipeline: str
) -> None:
    recovery_script, _, scripts_dir = _prepare_scripts(tmp_path)
    lock_dir = _lock_dir(tmp_path, pipeline)
    process = _start_repo_process(scripts_dir)
    try:
        _write_lock(lock_dir, pid=process.pid, pgid=process.pid, command=str(process.args[0]))

        result = _run_recovery(recovery_script, pipeline)

        assert result.returncode == 0, result.stderr + result.stdout
        assert f"Pipeline={pipeline} DRY_RUN=1 FORCE=0" in result.stdout
        assert f"Lock: {lock_dir}" in result.stdout
        assert "host=test-host" in result.stdout
        assert f"cwd={tmp_path}" in result.stdout
        assert f"Before snapshot for PGID {process.pid}:" in result.stdout
        assert f"After snapshot for PGID {process.pid}:" in result.stdout
        assert "Dry run only: FORCE!=1, no termination executed." in result.stdout
        assert process.poll() is None
        assert lock_dir.is_dir()
    finally:
        _stop_process(process)


@pytest.mark.parametrize("pipeline", ["td-leads", "orders-reports"])
def test_force_terminates_pipeline_process_group_and_removes_lock(
    tmp_path: Path, pipeline: str
) -> None:
    recovery_script, _, scripts_dir = _prepare_scripts(tmp_path)
    lock_dir = _lock_dir(tmp_path, pipeline)
    process = _start_repo_process(scripts_dir)
    try:
        _write_lock(lock_dir, pid=process.pid, pgid=process.pid, command=str(process.args[0]))

        result = _run_recovery(
            recovery_script,
            pipeline,
            FORCE="1",
            TERM_WAIT_SECONDS="0",
            KILL_WAIT_SECONDS="0",
        )
        process.wait(timeout=5)

        assert result.returncode == 0, result.stderr + result.stdout
        assert f"Sending TERM to process group -{process.pid}" in result.stdout
        assert "is gone; removing lock directory." in result.stdout
        assert not lock_dir.exists()
    finally:
        _stop_process(process)


def test_force_escalates_to_kill_after_bounded_term_wait(tmp_path: Path) -> None:
    recovery_script, _, scripts_dir = _prepare_scripts(tmp_path)
    lock_dir = _lock_dir(tmp_path, "td-leads")
    process = _start_repo_process(scripts_dir, ignore_term=True)
    try:
        _write_lock(lock_dir, pid=process.pid, pgid=process.pid, command=str(process.args[0]))

        result = _run_recovery(
            recovery_script,
            "td-leads",
            FORCE="1",
            TERM_WAIT_SECONDS="0",
            KILL_WAIT_SECONDS="0",
        )
        process.wait(timeout=5)

        assert result.returncode == 0, result.stderr + result.stdout
        assert f"PGID {process.pid} still alive; sending KILL" in result.stdout
        assert not lock_dir.exists()
    finally:
        _stop_process(process)


def test_rejects_unrelated_lock_command_without_terminating_process(tmp_path: Path) -> None:
    recovery_script, _, scripts_dir = _prepare_scripts(tmp_path)
    lock_dir = _lock_dir(tmp_path, "orders-reports")
    process = _start_repo_process(scripts_dir)
    try:
        _write_lock(lock_dir, pid=process.pid, pgid=process.pid, command="/usr/bin/sleep 999")

        result = _run_recovery(recovery_script, "orders-reports", FORCE="1")

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
    recovery_script, _, scripts_dir = _prepare_scripts(tmp_path)
    lock_dir = _lock_dir(tmp_path, "orders-reports")
    process = _start_repo_process(scripts_dir)
    try:
        metadata: dict[str, int | str] = {"pid": process.pid, "pgid": process.pid}
        metadata[metadata_name] = metadata_value
        _write_lock(lock_dir, command=str(process.args[0]), **metadata)

        result = _run_recovery(recovery_script, "orders-reports", FORCE="1")

        assert result.returncode == 0, result.stderr + result.stdout
        assert message in result.stdout
        assert "Sending TERM" not in result.stdout
        assert process.poll() is None
        assert lock_dir.is_dir()
    finally:
        _stop_process(process)


@pytest.mark.parametrize(
    ("metadata_name", "metadata_value", "message"),
    [
        ("started_at", "not-a-time", "Skipping: malformed start time [not-a-time]."),
        ("host", "not a host", "Skipping: malformed host [not a host]."),
        ("cwd", "relative/path", "Skipping: malformed working directory [relative/path]."),
    ],
)
def test_rejects_malformed_descriptive_metadata(
    tmp_path: Path, metadata_name: str, metadata_value: str, message: str
) -> None:
    recovery_script, _, scripts_dir = _prepare_scripts(tmp_path)
    lock_dir = _lock_dir(tmp_path, "orders-reports")
    process = _start_repo_process(scripts_dir)
    try:
        metadata = {metadata_name: metadata_value}
        _write_lock(
            lock_dir,
            pid=process.pid,
            pgid=process.pid,
            command=str(process.args[0]),
            **metadata,
        )

        result = _run_recovery(recovery_script, "orders-reports", FORCE="1")

        assert result.returncode == 0, result.stderr + result.stdout
        assert message in result.stdout
        assert "Sending TERM" not in result.stdout
        assert process.poll() is None
        assert lock_dir.is_dir()
    finally:
        _stop_process(process)


@pytest.mark.parametrize("missing_name", ["pid", "pgid", "command", "started_at", "host", "cwd"])
def test_rejects_lock_directory_with_missing_metadata(tmp_path: Path, missing_name: str) -> None:
    recovery_script, _, scripts_dir = _prepare_scripts(tmp_path)
    lock_dir = _lock_dir(tmp_path, "orders-reports")
    process = _start_repo_process(scripts_dir)
    try:
        _write_lock(lock_dir, pid=process.pid, pgid=process.pid, command=str(process.args[0]))
        (lock_dir / missing_name).unlink()

        result = _run_recovery(recovery_script, "orders-reports", FORCE="1")

        assert result.returncode == 0, result.stderr + result.stdout
        assert f"missing metadata file {lock_dir / missing_name}" in result.stderr
        assert "Sending TERM" not in result.stdout
        assert process.poll() is None
        assert lock_dir.is_dir()
    finally:
        _stop_process(process)


def test_force_removes_dead_owner_lock(tmp_path: Path) -> None:
    recovery_script, _, scripts_dir = _prepare_scripts(tmp_path)
    lock_dir = _lock_dir(tmp_path, "td-leads")
    process = _start_repo_process(scripts_dir)
    command = str(process.args[0])
    pgid = process.pid
    _stop_process(process)
    time.sleep(0.05)
    _write_lock(lock_dir, pid=pgid, pgid=pgid, command=command)

    result = _run_recovery(recovery_script, "td-leads", FORCE="1")

    assert result.returncode == 0, result.stderr + result.stdout
    assert f"Process group {pgid} is gone; removing lock directory." in result.stdout
    assert not lock_dir.exists()


def test_force_removes_obsolete_global_lock_after_recorded_group_is_gone(tmp_path: Path) -> None:
    recovery_script, _, scripts_dir = _prepare_scripts(tmp_path)
    process = _start_repo_process(scripts_dir)
    command = str(process.args[0])
    pgid = process.pid
    _stop_process(process)
    time.sleep(0.05)
    obsolete_lock = tmp_path / "tmp" / "cron_heavy_pipelines.lock"
    _write_lock(obsolete_lock, pid=pgid, pgid=pgid, command=command)

    result = _run_recovery(recovery_script, "orders-reports", FORCE="1")

    assert result.returncode == 0, result.stderr + result.stdout
    assert f"Obsolete global lock rollout cleanup: {obsolete_lock}" in result.stdout
    assert not obsolete_lock.exists()


def test_legacy_orders_reports_wrapper_forwards_to_general_helper(tmp_path: Path) -> None:
    _, legacy_wrapper, scripts_dir = _prepare_scripts(tmp_path)
    lock_dir = _lock_dir(tmp_path, "orders-reports")
    process = _start_repo_process(scripts_dir)
    try:
        _write_lock(lock_dir, pid=process.pid, pgid=process.pid, command=str(process.args[0]))

        result = _run_recovery(legacy_wrapper, PIPELINE="td-leads", Pipeline="td-leads")

        assert result.returncode == 0, result.stderr + result.stdout
        assert "Legacy helper: inspecting orders-reports only." in result.stdout
        assert (
            "For TD leads, run: ./scripts/inspect_or_kill_pipeline_stale.sh td-leads"
            in result.stdout
        )
        assert (
            "For explicit orders/reports inspection, run: "
            "./scripts/inspect_or_kill_pipeline_stale.sh orders-reports"
            in result.stdout
        )
        assert "Pipeline=orders-reports DRY_RUN=1 FORCE=0" in result.stdout
        assert f"Lock: {lock_dir}" in result.stdout
        assert process.poll() is None
    finally:
        _stop_process(process)


def test_rejects_unknown_pipeline(tmp_path: Path) -> None:
    recovery_script, _, _ = _prepare_scripts(tmp_path)

    result = _run_recovery(recovery_script, "unknown")

    assert result.returncode == 2
    assert "Unknown pipeline: unknown" in result.stderr
