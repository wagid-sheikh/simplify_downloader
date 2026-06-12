"""Input discovery and deterministic archive helpers for Phase 2 ingestion."""

from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass
from pathlib import Path

from app.dashboard_downloader.json_logger import JsonLogger, log_event

from .types import DiscoveredInputFile

SUPPORTED_EXTERNAL_EXTENSIONS = {".csv", ".xlsx"}
SUPPORTED_WORKBOOK_EXTENSIONS = {".xlsx"}
ARCHIVE_DIR_NAMES = {"archive", "archived"}


@dataclass(frozen=True)
class CustomerFollowupPaths:
    input_dir: Path
    external_input_dir: Path
    archive_dir: Path


def get_customer_followup_paths() -> CustomerFollowupPaths:
    """Resolve folders through the repository config singleton at call time."""

    from app.config import config

    return CustomerFollowupPaths(
        input_dir=Path(config.customer_followup_input_dir).expanduser(),
        external_input_dir=Path(config.customer_followup_external_input_dir).expanduser(),
        archive_dir=Path(config.customer_followup_archive_dir).expanduser(),
    )


def _is_ignored(path: Path) -> bool:
    if any(part.startswith(".") for part in path.parts):
        return True
    if path.name.startswith(("~$", ".")):
        return True
    if any(part.lower() in ARCHIVE_DIR_NAMES for part in path.parts):
        return True
    if path.suffix.lower() in {".tmp", ".temp", ".bak", ".partial", ".crdownload"}:
        return True
    return False


def _discover_files(base_dir: Path, *, extensions: set[str]) -> list[DiscoveredInputFile]:
    if not base_dir.exists():
        return []
    files: list[DiscoveredInputFile] = []
    for path in sorted(base_dir.rglob("*")):
        if not path.is_file() or _is_ignored(path.relative_to(base_dir)):
            continue
        if path.suffix.lower() not in extensions:
            continue
        stat = path.stat()
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        rel = path.relative_to(base_dir).as_posix()
        files.append(
            DiscoveredInputFile(
                path=path,
                relative_path=rel,
                file_name=path.name,
                file_size=stat.st_size,
                content_sha256=digest,
                identity_key=f"{rel}:{stat.st_size}:{digest}",
                file_type=path.suffix.lower().lstrip("."),
            )
        )
    return files


def discover_external_lead_files(*, external_input_dir: Path | None = None, logger: JsonLogger | None = None) -> list[DiscoveredInputFile]:
    paths = get_customer_followup_paths() if external_input_dir is None else None
    base = external_input_dir or paths.external_input_dir  # type: ignore[union-attr]
    files = _discover_files(Path(base), extensions=SUPPORTED_EXTERNAL_EXTENSIONS)
    if logger:
        log_event(logger=logger, phase="input_discovery", message="external_lead_files_discovered", count=len(files))
    return files


def discover_returned_workbooks(*, input_dir: Path | None = None, logger: JsonLogger | None = None) -> list[DiscoveredInputFile]:
    paths = get_customer_followup_paths() if input_dir is None else None
    base = input_dir or paths.input_dir  # type: ignore[union-attr]
    external_dir = (get_customer_followup_paths().external_input_dir if input_dir is None else Path(base) / "external_leads").resolve()
    files = [f for f in _discover_files(Path(base), extensions=SUPPORTED_WORKBOOK_EXTENSIONS) if external_dir not in f.path.resolve().parents]
    if logger:
        log_event(logger=logger, phase="input_discovery", message="returned_workbooks_discovered", count=len(files))
    return files


def archive_processed_file(source: Path, *, archive_dir: Path | None = None, run_id: str, result_metadata: dict[str, object] | None = None, logger: JsonLogger | None = None) -> Path:
    """Copy a processed file into a deterministic archive path without overwriting."""

    paths = get_customer_followup_paths() if archive_dir is None else None
    base = Path(archive_dir or paths.archive_dir)  # type: ignore[union-attr]
    digest = hashlib.sha256(source.read_bytes()).hexdigest()[:16]
    target_dir = base / digest[:2]
    target_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{source.stem}__{run_id}__{digest}"
    target = target_dir / f"{stem}{source.suffix.lower()}"
    counter = 1
    while target.exists():
        if hashlib.sha256(target.read_bytes()).hexdigest() == hashlib.sha256(source.read_bytes()).hexdigest():
            break
        target = target_dir / f"{stem}__dup{counter}{source.suffix.lower()}"
        counter += 1
    if not target.exists():
        shutil.copy2(source, target)
    metadata_path = target.with_suffix(target.suffix + ".json")
    metadata = {"source_file": source.name, "run_id": run_id, "content_sha256": hashlib.sha256(source.read_bytes()).hexdigest(), **(result_metadata or {})}
    if not metadata_path.exists():
        metadata_path.write_text(json.dumps(metadata, default=str, sort_keys=True, indent=2), encoding="utf-8")
    if logger:
        log_event(logger=logger, phase="archive", message="file_archived", source_file=source.name, archived_file=str(target), run_id=run_id)
    return target
