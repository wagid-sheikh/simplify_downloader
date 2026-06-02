from __future__ import annotations

import os
import signal
import stat
import subprocess
import time
from pathlib import Path


def _write_executable(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def _prepare_repo(tmp_path: Path) -> tuple[Path, Path]:
    repo_root = tmp_path
    scripts_dir = repo_root / "scripts"
    logs_dir = repo_root / "logs"
    tmp_dir = repo_root / "tmp"
    scripts_dir.mkdir(parents=True)
    logs_dir.mkdir()
    tmp_dir.mkdir()

    source_cron = Path("scripts/cron_run_td_leads_sync.sh").read_text(encoding="utf-8")
    _write_executable(scripts_dir / "cron_run_td_leads_sync.sh", source_cron)
    return repo_root, scripts_dir


def _run_cron(repo_root: Path, scripts_dir: Path, *args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.update({"TMPDIR": str(repo_root)})
    return subprocess.run(
        [str(scripts_dir / "cron_run_td_leads_sync.sh"), *args],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def _latest_log_text(repo_root: Path) -> str:
    log_files = sorted((repo_root / "logs").glob("cron_run_td_leads_sync_*.log"))
    assert log_files
    return log_files[-1].read_text(encoding="utf-8")



def _write_lock_metadata(
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


def _wait_for_path(path: Path, timeout_seconds: float = 5) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if path.exists():
            return
        time.sleep(0.05)
    raise AssertionError(f"Timed out waiting for {path}")


def _pid_is_non_zombie_alive(pid: int) -> bool:
    result = subprocess.run(
        ["ps", "-o", "state=", "-p", str(pid)],
        capture_output=True,
        text=True,
        check=False,
    )
    return bool(result.stdout.strip()) and not result.stdout.strip().startswith("Z")


def test_td_leads_cron_no_cli_args_is_nounset_safe(tmp_path: Path) -> None:
    repo_root, scripts_dir = _prepare_repo(tmp_path)
    _write_executable(
        scripts_dir / "run_local_td_leads_sync.sh",
        "#!/usr/bin/env bash\nprintf '%s\\n' \"$#\" > \"${TMPDIR:-/tmp}/td_leads_arg_count\"\nexit 0\n",
    )

    result = _run_cron(repo_root, scripts_dir)

    assert result.returncode == 0
    assert "unbound variable" not in (result.stderr + result.stdout).lower()
    assert (tmp_path / "td_leads_arg_count").read_text(encoding="utf-8").strip() == "0"

    log_text = _latest_log_text(repo_root)
    assert "Parsed td_leads args count=0" in log_text


def test_td_leads_cron_passes_one_non_reporting_arg(tmp_path: Path) -> None:
    repo_root, scripts_dir = _prepare_repo(tmp_path)
    _write_executable(
        scripts_dir / "run_local_td_leads_sync.sh",
        "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" > \"${TMPDIR:-/tmp}/td_leads_args\"\nexit 0\n",
    )

    result = _run_cron(repo_root, scripts_dir, "--foo")

    assert result.returncode == 0
    assert "unbound variable" not in (result.stderr + result.stdout).lower()
    assert (tmp_path / "td_leads_args").read_text(encoding="utf-8").strip() == "--foo"


def test_td_leads_cron_maps_reporting_mode_meeting(tmp_path: Path) -> None:
    repo_root, scripts_dir = _prepare_repo(tmp_path)
    _write_executable(
        scripts_dir / "run_local_td_leads_sync.sh",
        "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" > \"${TMPDIR:-/tmp}/td_leads_args\"\nexit 0\n",
    )

    result = _run_cron(repo_root, scripts_dir, "reporting_mode=meeting")

    assert result.returncode == 0
    assert (tmp_path / "td_leads_args").read_text(encoding="utf-8").strip() == "--reporting-mode meeting"


def test_td_leads_cron_ignores_invalid_reporting_mode(tmp_path: Path) -> None:
    repo_root, scripts_dir = _prepare_repo(tmp_path)
    _write_executable(
        scripts_dir / "run_local_td_leads_sync.sh",
        "#!/usr/bin/env bash\nprintf '%s\\n' \"$#\" > \"${TMPDIR:-/tmp}/td_leads_arg_count\"\nexit 0\n",
    )

    result = _run_cron(repo_root, scripts_dir, "reporting_mode=invalid")

    assert result.returncode == 0
    assert (tmp_path / "td_leads_arg_count").read_text(encoding="utf-8").strip() == "0"

    log_text = _latest_log_text(repo_root)
    assert "Invalid reporting_mode 'invalid' ignored" in log_text


def test_td_leads_cron_enforces_deprecated_max_runtime_seconds_override(tmp_path: Path) -> None:
    repo_root, scripts_dir = _prepare_repo(tmp_path)
    _write_executable(
        scripts_dir / "run_local_td_leads_sync.sh",
        "#!/usr/bin/env bash\nexec sleep 30\n",
    )

    env = os.environ.copy()
    env.update({"TMPDIR": str(repo_root), "TD_LEADS_MAX_RUNTIME_SECONDS": "30", "MAX_RUNTIME_SECONDS": "1"})
    result = subprocess.run(
        [str(scripts_dir / "cron_run_td_leads_sync.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )

    assert result.returncode == 124
    log_text = _latest_log_text(repo_root)
    assert "exceeded TD_LEADS_MAX_RUNTIME_SECONDS=1s" in log_text


def test_td_leads_watchdog_terminates_descendant_process_group(tmp_path: Path) -> None:
    repo_root, scripts_dir = _prepare_repo(tmp_path)
    descendant_pid_file = tmp_path / "td_leads_descendant_pid"
    _write_executable(
        scripts_dir / "run_local_td_leads_sync.sh",
        """#!/usr/bin/env bash
trap '' TERM
(
  trap '' TERM
  while :; do sleep 1; done
) &
printf '%s\n' "$!" > "${TMPDIR:-/tmp}/td_leads_descendant_pid"
while :; do sleep 1; done
""",
    )

    env = os.environ.copy()
    env.update({"TMPDIR": str(repo_root), "TD_LEADS_MAX_RUNTIME_SECONDS": "1", "KILL_WAIT_SECONDS": "1"})
    result = subprocess.run(
        [str(scripts_dir / "cron_run_td_leads_sync.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )

    assert result.returncode == 124
    descendant_pid = int(descendant_pid_file.read_text(encoding="utf-8").strip())
    assert not _pid_is_non_zombie_alive(descendant_pid)
    assert not (repo_root / "tmp" / "cron_run_td_leads_sync.lock").exists()
    log_text = _latest_log_text(repo_root)
    assert "child_pid=" in log_text and "child_pgid=" in log_text
    assert "sending KILL" in log_text
    assert "disappeared after KILL" in log_text


def test_active_orders_reports_lock_does_not_block_td_leads(tmp_path: Path) -> None:
    repo_root, scripts_dir = _prepare_repo(tmp_path)
    unrelated_lock = repo_root / "tmp" / "cron_run_orders_and_reports.lock"
    unrelated_lock.mkdir()
    (unrelated_lock / "pid").write_text(f"{os.getpid()}\n", encoding="utf-8")
    _write_executable(
        scripts_dir / "run_local_td_leads_sync.sh",
        "#!/usr/bin/env bash\nprintf 'ran\\n' > \"${TMPDIR:-/tmp}/td-leads-ran\"\nexit 0\n",
    )

    result = _run_cron(repo_root, scripts_dir)

    assert result.returncode == 0, result.stderr + result.stdout
    assert (tmp_path / "td-leads-ran").read_text(encoding="utf-8") == "ran\n"
    assert unrelated_lock.is_dir()
    assert not (repo_root / "tmp" / "cron_heavy_pipelines.lock").exists()


def test_active_td_leads_lock_suppresses_second_td_leads_instance(tmp_path: Path) -> None:
    repo_root, scripts_dir = _prepare_repo(tmp_path)
    local_lock = repo_root / "tmp" / "cron_run_td_leads_sync.lock"
    _write_lock_metadata(local_lock, pid=os.getpid(), pgid=os.getpgid(os.getpid()))
    _write_executable(
        scripts_dir / "run_local_td_leads_sync.sh",
        "#!/usr/bin/env bash\nprintf 'unexpected\\n' > \"${TMPDIR:-/tmp}/td-leads-ran\"\nexit 0\n",
    )

    result = _run_cron(repo_root, scripts_dir)

    assert result.returncode == 0, result.stderr + result.stdout
    assert not (tmp_path / "td-leads-ran").exists()
    assert "status=skipped_due_to_active_same_pipeline_owner" in _latest_log_text(repo_root)


def test_td_leads_wrapper_does_not_reference_retired_global_lock() -> None:
    source = Path("scripts/cron_run_td_leads_sync.sh").read_text(encoding="utf-8")

    assert "cron_heavy_pipelines.lock" not in source
    assert "GLOBAL_LOCK" not in source


def test_td_leads_crontab_does_not_document_retired_global_lock_wait_policy() -> None:
    source = Path("scripts/crontab_entries.txt").read_text(encoding="utf-8")

    assert "cron_heavy_pipelines.lock" not in source
    assert "900" not in source


def test_dead_td_leads_owner_lock_is_cleaned_up_and_reacquired(tmp_path: Path) -> None:
    repo_root, scripts_dir = _prepare_repo(tmp_path)
    lock_dir = repo_root / "tmp" / "cron_run_td_leads_sync.lock"
    _write_lock_metadata(lock_dir, pid=999999, pgid=999999)
    _write_executable(scripts_dir / "run_local_td_leads_sync.sh", "#!/usr/bin/env bash\nprintf 'ran\\n' > \"${TMPDIR:-/tmp}/ran\"\n")

    result = _run_cron(repo_root, scripts_dir)

    assert result.returncode == 0, result.stderr + result.stdout
    assert (tmp_path / "ran").exists()
    assert "Owner PID=999999 and PGID=999999 are gone; removing stale lock" in _latest_log_text(repo_root)


def test_malformed_td_leads_lock_metadata_fails_safe_without_deletion(tmp_path: Path) -> None:
    repo_root, scripts_dir = _prepare_repo(tmp_path)
    lock_dir = repo_root / "tmp" / "cron_run_td_leads_sync.lock"
    _write_lock_metadata(lock_dir, pid="bad-pid", pgid="bad-pgid")
    _write_executable(scripts_dir / "run_local_td_leads_sync.sh", "#!/usr/bin/env bash\nexit 0\n")

    result = _run_cron(repo_root, scripts_dir)

    assert result.returncode == 1
    assert lock_dir.is_dir()
    assert "metadata is missing or malformed. Leaving lock untouched" in _latest_log_text(repo_root)


def test_td_leads_stale_unrelated_live_process_is_refused(tmp_path: Path) -> None:
    repo_root, scripts_dir = _prepare_repo(tmp_path)
    lock_dir = repo_root / "tmp" / "cron_run_td_leads_sync.lock"
    owner = subprocess.Popen(["sleep", "30"], start_new_session=True)
    try:
        _write_lock_metadata(lock_dir, pid=owner.pid, pgid=os.getpgid(owner.pid), started_epoch=int(time.time()) - 10)
        _write_executable(scripts_dir / "run_local_td_leads_sync.sh", "#!/usr/bin/env bash\nexit 0\n")
        env = os.environ.copy()
        env.update({"TD_LEADS_STALE_OWNER_SECONDS": "0", "TMPDIR": str(repo_root)})

        result = subprocess.run([str(scripts_dir / "cron_run_td_leads_sync.sh")], cwd=repo_root, env=env, check=False)

        assert result.returncode == 1
        assert owner.poll() is None
        assert lock_dir.is_dir()
        assert "command does not belong to expected repository wrapper" in _latest_log_text(repo_root)
    finally:
        os.killpg(owner.pid, signal.SIGTERM)
        owner.wait()


def test_td_leads_stale_pid_pgid_mismatch_is_refused(tmp_path: Path) -> None:
    repo_root, scripts_dir = _prepare_repo(tmp_path)
    lock_dir = repo_root / "tmp" / "cron_run_td_leads_sync.lock"
    owner = subprocess.Popen(["sleep", "30"], start_new_session=True)
    try:
        expected_wrapper = scripts_dir / "cron_run_td_leads_sync.sh"
        _write_lock_metadata(lock_dir, pid=owner.pid, pgid=owner.pid + 1, started_epoch=int(time.time()) - 10, command=str(expected_wrapper))
        _write_executable(scripts_dir / "run_local_td_leads_sync.sh", "#!/usr/bin/env bash\nexit 0\n")
        env = os.environ.copy()
        env.update({"TD_LEADS_STALE_OWNER_SECONDS": "0", "TMPDIR": str(repo_root)})

        result = subprocess.run([str(expected_wrapper)], cwd=repo_root, env=env, check=False)

        assert result.returncode == 1
        assert owner.poll() is None
        assert lock_dir.is_dir()
        assert "PID/PGID mismatch" in _latest_log_text(repo_root)
    finally:
        os.killpg(owner.pid, signal.SIGTERM)
        owner.wait()


def test_td_leads_stale_owner_group_is_terminated_and_lock_reacquired(tmp_path: Path) -> None:
    repo_root, scripts_dir = _prepare_repo(tmp_path)
    lock_dir = repo_root / "tmp" / "cron_run_td_leads_sync.lock"
    _write_executable(scripts_dir / "run_local_td_leads_sync.sh", "#!/usr/bin/env bash\nif mkdir \"${TMPDIR:-/tmp}/hold-once\" 2>/dev/null; then exec sleep 30; fi\nexit 0\n")
    env = os.environ.copy()
    env.update({"TD_LEADS_STALE_OWNER_SECONDS": "0", "STALE_OWNER_TERM_WAIT_SECONDS": "2", "STALE_OWNER_KILL_WAIT_SECONDS": "2", "TMPDIR": str(repo_root)})
    owner = subprocess.Popen([str(scripts_dir / "cron_run_td_leads_sync.sh")], cwd=repo_root, env=env, start_new_session=True)
    try:
        _wait_for_path(lock_dir / "pid")
        result = subprocess.run([str(scripts_dir / "cron_run_td_leads_sync.sh")], cwd=repo_root, env=env, check=False, timeout=10)
        owner.wait(timeout=5)

        assert result.returncode == 0
        assert not lock_dir.exists()
        assert "Confirmed stale-owner process group is gone" in _latest_log_text(repo_root)
    finally:
        if owner.poll() is None:
            os.killpg(owner.pid, signal.SIGTERM)
            owner.wait()


def test_rapid_td_leads_invocations_do_not_run_concurrently(tmp_path: Path) -> None:
    repo_root, scripts_dir = _prepare_repo(tmp_path)
    lock_dir = repo_root / "tmp" / "cron_run_td_leads_sync.lock"
    _write_executable(scripts_dir / "run_local_td_leads_sync.sh", "#!/usr/bin/env bash\nexec sleep 30\n")
    env = os.environ.copy()
    env.update({"TMPDIR": str(repo_root)})
    owner = subprocess.Popen([str(scripts_dir / "cron_run_td_leads_sync.sh")], cwd=repo_root, env=env, start_new_session=True)
    try:
        _wait_for_path(lock_dir / "pid")
        results = [subprocess.run([str(scripts_dir / "cron_run_td_leads_sync.sh")], cwd=repo_root, env=env, check=False) for _ in range(3)]
        assert [result.returncode for result in results] == [0, 0, 0]
        assert owner.poll() is None
        assert lock_dir.is_dir()
        assert "status=skipped_due_to_active_same_pipeline_owner" in _latest_log_text(repo_root)
    finally:
        os.killpg(owner.pid, signal.SIGTERM)
        owner.wait()


def test_td_leads_wrapper_logs_operational_notification_delivery_success(tmp_path: Path) -> None:
    repo_root, scripts_dir = _prepare_repo(tmp_path)
    _write_executable(
        scripts_dir / "run_local_td_leads_sync.sh", "#!/usr/bin/env bash\nexec sleep 30\n"
    )
    helper_path = (
        repo_root
        / "app"
        / "crm_downloader"
        / "td_leads_sync"
        / "wrapper_notifications.py"
    )
    helper_path.parent.mkdir(parents=True)
    helper_path.write_text(
        "# marker file for wrapper helper availability\n", encoding="utf-8"
    )
    bin_dir = repo_root / "bin"
    bin_dir.mkdir()
    _write_executable(
        bin_dir / "poetry", "#!/usr/bin/env bash\nprintf '{\"emails_sent\": 1}\\n'\n"
    )
    env = os.environ.copy()
    env.update(
        {
            "TMPDIR": str(repo_root),
            "CRON_PATH": str(bin_dir),
            "TD_LEADS_MAX_RUNTIME_SECONDS": "1",
            "KILL_WAIT_SECONDS": "1",
        }
    )

    result = subprocess.run(
        [str(scripts_dir / "cron_run_td_leads_sync.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )

    assert result.returncode == 124
    log_text = _latest_log_text(repo_root)
    assert "[wrapper notification] status=watchdog_timeout delivery=success" in log_text
    assert '{"emails_sent": 1}' in log_text


def test_td_leads_stale_recovery_refuses_inherited_process_group(tmp_path: Path) -> None:
    repo_root, scripts_dir = _prepare_repo(tmp_path)
    lock_dir = repo_root / "tmp" / "cron_run_td_leads_sync.lock"
    expected_wrapper = scripts_dir / "cron_run_td_leads_sync.sh"
    owner = subprocess.Popen(
        ["bash", "-c", "while :; do sleep 1; done", str(expected_wrapper)]
    )
    try:
        inherited_pgid = os.getpgid(owner.pid)
        assert inherited_pgid != owner.pid
        _write_lock_metadata(
            lock_dir,
            pid=owner.pid,
            pgid=inherited_pgid,
            started_epoch=int(time.time()) - 10,
            command=str(expected_wrapper),
        )
        _write_executable(scripts_dir / "run_local_td_leads_sync.sh", "#!/usr/bin/env bash\nexit 0\n")
        env = os.environ.copy()
        env.update({"TD_LEADS_STALE_OWNER_SECONDS": "0", "TMPDIR": str(repo_root)})

        result = subprocess.run([str(expected_wrapper)], cwd=repo_root, env=env, check=False)

        assert result.returncode == 1
        assert owner.poll() is None
        assert lock_dir.is_dir()
        assert "is not an isolated wrapper-owned process group" in _latest_log_text(repo_root)
    finally:
        owner.terminate()
        owner.wait(timeout=5)
