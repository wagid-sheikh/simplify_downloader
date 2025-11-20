from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from app.dashboard_downloader.config import (
    DATA_DIR,
    MERGE_BUCKET_DB_SPECS,
    MERGED_NAMES,
    fetch_store_codes,
    global_credentials,
    stores_from_list,
)
from app.config import config


GLOBAL_CREDENTIAL_ERROR = "Global CRM credentials (username/password) are missing/invalid."


def _default_global_username() -> str:
    username, _ = global_credentials()
    return username or ""


def _default_global_password() -> str:
    _, password = global_credentials()
    return password or ""


@dataclass
class PipelineSettings:
    run_id: str
    data_dir: Path = DATA_DIR
    stores: Dict[str, dict] = field(default_factory=dict)
    raw_store_env: str = ""
    merged_names: Dict[str, str] = field(default_factory=lambda: MERGED_NAMES)
    merge_bucket_specs: Dict[str, dict] = field(default_factory=lambda: MERGE_BUCKET_DB_SPECS)
    dry_run: bool = False
    ingest_batch_size: int = field(default_factory=lambda: config.ingest_batch_size)
    database_url: Optional[str] = field(default_factory=lambda: config.database_url)
    global_username: str = field(default_factory=_default_global_username)
    global_password: str = field(default_factory=_default_global_password)


async def _ensure_report_store_alignment(selected: Dict[str, dict]) -> None:
    report_codes = await fetch_store_codes(database_url=config.database_url, report_flag=True)
    if not report_codes:
        return

    selected_codes = {code.strip().upper() for code in selected.keys()}
    missing = sorted(code for code in report_codes if code not in selected_codes)
    if missing:
        raise ValueError(
            "Report-eligible stores are missing from the scraping run: %s" % ",".join(missing)
        )


async def _resolve_store_codes() -> List[str]:
    stores = await fetch_store_codes(database_url=config.database_url, etl_flag=True)
    if not stores:
        raise ValueError("No stores are flagged for ETL")
    return stores


async def load_settings(*, dry_run: bool, run_id: str) -> PipelineSettings:
    store_codes = await _resolve_store_codes()
    selected = stores_from_list(store_codes)

    await _ensure_report_store_alignment(selected)
    raw_source = "store_master.etl_flag"
    return PipelineSettings(run_id=run_id, stores=selected, raw_store_env=raw_source, dry_run=dry_run)
