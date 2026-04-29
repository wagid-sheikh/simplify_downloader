from __future__ import annotations

import os
import subprocess
from pathlib import Path


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
