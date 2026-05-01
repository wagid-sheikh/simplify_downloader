from __future__ import annotations

import os
import stat
import subprocess
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
    env.update({"LOCK_WAIT_SECONDS": "0", "SAFE_MODE": "0", "TMPDIR": str(repo_root)})
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
