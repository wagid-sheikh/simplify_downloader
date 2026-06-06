from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path


def _write_executable(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def _prepare_repo(tmp_path: Path, *, preflight_exit: int) -> tuple[Path, Path]:
    repo_root = tmp_path
    scripts_dir = repo_root / "scripts"
    bin_dir = repo_root / "bin"
    logs_dir = repo_root / "logs"
    app_dir = repo_root / "app" / "crm_downloader" / "td_leads_sync"
    scripts_dir.mkdir(parents=True)
    bin_dir.mkdir()
    logs_dir.mkdir()
    app_dir.mkdir(parents=True)
    (app_dir / "wrapper_notifications.py").write_text("", encoding="utf-8")
    _write_executable(
        scripts_dir / "cron_run_td_leads_sync.sh",
        Path("scripts/cron_run_td_leads_sync.sh").read_text(encoding="utf-8"),
    )
    _write_executable(
        scripts_dir / "run_local_td_leads_sync.sh",
        "#!/usr/bin/env bash\n"
        'printf \'td_leads_child_launched degraded=%s reason=%s\\n\' "${TD_LEADS_NOTIFICATION_PREFLIGHT_DEGRADED:-0}" "${TD_LEADS_NOTIFICATION_PREFLIGHT_DEGRADED_REASON:-none}" >> launched.log\n'
        "exit 0\n",
    )
    _write_executable(
        bin_dir / "poetry",
        "#!/usr/bin/env bash\n"
        "printf 'fake poetry %s\\n' \"$*\"\n"
        "if [[ \"$*\" == *'app.crm_downloader.td_leads_sync.network_preflight'* ]]; then\n"
        f"  echo 'td_leads_network_preflight_summary classification=test exit_code={preflight_exit}'\n"
        f"  exit {preflight_exit}\n"
        "fi\n"
        "exit 0\n",
    )
    return repo_root, bin_dir


def _run_cron(repo_root: Path, bin_dir: Path) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{bin_dir}:{env['PATH']}",
            "CRON_PATH": str(bin_dir),
            "TD_LEADS_MAX_RUNTIME_SECONDS": "10",
            "TD_LEADS_WRAPPER_NOTIFICATION_TIMEOUT_SECONDS": "1",
        }
    )
    return subprocess.run(
        [str(repo_root / "scripts" / "cron_run_td_leads_sync.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_td_leads_cron_preflight_crm_failure_skips_browser_launch(
    tmp_path: Path,
) -> None:
    repo_root, bin_dir = _prepare_repo(tmp_path, preflight_exit=20)

    result = _run_cron(repo_root, bin_dir)

    assert result.returncode == 0, result.stderr + result.stdout
    assert not (repo_root / "launched.log").exists()
    log_text = sorted((repo_root / "logs").glob("cron_run_td_leads_sync_*.log"))[
        -1
    ].read_text(encoding="utf-8")
    assert "CRM DNS/TCP preflight failed" in log_text
    assert "browser launch skipped after operational summary persistence" in log_text


def test_td_leads_cron_preflight_smtp_failure_continues_degraded(
    tmp_path: Path,
) -> None:
    repo_root, bin_dir = _prepare_repo(tmp_path, preflight_exit=30)

    result = _run_cron(repo_root, bin_dir)

    assert result.returncode == 0, result.stderr + result.stdout
    launched = (repo_root / "launched.log").read_text(encoding="utf-8")
    assert (
        "td_leads_child_launched degraded=1 reason=smtp_dns_tcp_preflight_failed"
        in launched
    )
    log_text = sorted((repo_root / "logs").glob("cron_run_td_leads_sync_*.log"))[
        -1
    ].read_text(encoding="utf-8")
    assert "SMTP DNS/TCP preflight failed; continuing data sync" in log_text


def test_td_leads_cron_preflight_success_passes_through_to_sync(tmp_path: Path) -> None:
    repo_root, bin_dir = _prepare_repo(tmp_path, preflight_exit=0)

    result = _run_cron(repo_root, bin_dir)

    assert result.returncode == 0, result.stderr + result.stdout
    launched = (repo_root / "launched.log").read_text(encoding="utf-8")
    assert "td_leads_child_launched degraded=0 reason=none" in launched
    log_text = sorted((repo_root / "logs").glob("cron_run_td_leads_sync_*.log"))[
        -1
    ].read_text(encoding="utf-8")
    assert "TD leads network preflight passed" in log_text
