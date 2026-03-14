from __future__ import annotations

from pathlib import Path


def test_uc_orders_sync_has_no_merge_conflict_markers() -> None:
    root = Path(__file__).resolve().parents[2]
    target_dir = root / "app" / "crm_downloader" / "uc_orders_sync"
    marker_prefixes = ("<<<<<<<", "=======", ">>>>>>>")

    offenders: list[str] = []
    for py_file in sorted(target_dir.rglob("*.py")):
        with py_file.open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, start=1):
                if line.startswith(marker_prefixes):
                    offenders.append(f"{py_file.relative_to(root)}:{line_no}: {line.rstrip()}")

    assert not offenders, (
        "Merge conflict markers found in app/crm_downloader/uc_orders_sync Python files:\n"
        + "\n".join(offenders)
    )
