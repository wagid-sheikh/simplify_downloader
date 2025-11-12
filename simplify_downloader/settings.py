from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from downloader.config import DATA_DIR, MERGE_BUCKET_DB_SPECS, MERGED_NAMES, STORES, env_stores_list, stores_from_list


@dataclass
class PipelineSettings:
    run_id: str
    data_dir: Path = DATA_DIR
    stores: Dict[str, dict] = field(default_factory=dict)
    merged_names: Dict[str, str] = field(default_factory=lambda: MERGED_NAMES)
    merge_bucket_specs: Dict[str, dict] = field(default_factory=lambda: MERGE_BUCKET_DB_SPECS)
    dry_run: bool = False
    ingest_batch_size: int = field(default_factory=lambda: int(os.getenv("INGEST_BATCH_SIZE", "3000")))
    database_url: Optional[str] = field(default_factory=lambda: os.getenv("DATABASE_URL"))


def load_settings(*, stores_list: Optional[str], dry_run: bool, run_id: str) -> PipelineSettings:
    env_list = env_stores_list()
    cli_list = [s.strip() for s in (stores_list.split(",") if stores_list else []) if s.strip()]
    final_list: List[str] = cli_list or env_list or list(STORES.keys())
    selected = stores_from_list(final_list)
    if not selected:
        selected = STORES
    return PipelineSettings(run_id=run_id, stores=selected, dry_run=dry_run)
