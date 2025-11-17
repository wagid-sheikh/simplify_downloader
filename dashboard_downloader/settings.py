from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from dashboard_downloader.config import (
    DATA_DIR,
    MERGE_BUCKET_DB_SPECS,
    MERGED_NAMES,
    env_stores_list,
    global_credentials,
    stores_from_list,
)
from simplify_downloader.config import config


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


def _split_codes(raw: str | None) -> List[str]:
    if not raw:
        return []
    return [token.strip() for token in raw.split(",") if token.strip()]


def _normalized(values: Iterable[str]) -> List[str]:
    return sorted({token.strip().upper() for token in values if token and token.strip()})


def _validate_store_selector_sources(*, cli: List[str], env_upper: str | None) -> None:
    sources: List[tuple[str, List[str]]] = []
    if cli:
        sources.append(("CLI --stores_list", _normalized(cli)))
    if env_upper:
        sources.append(("STORES_LIST env", _normalized(_split_codes(env_upper))))

    if not sources:
        return

    baseline_codes = sources[0][1]
    for label, codes in sources[1:]:
        if codes != baseline_codes:
            raise ValueError(
                "Conflicting store selections detected between %s and %s: %s vs %s"
                % (sources[0][0], label, ",".join(baseline_codes), ",".join(codes))
            )


def _ensure_report_store_alignment(selected: Dict[str, dict]) -> None:
    report_codes = _normalized(config.report_stores_list)
    if not report_codes:
        return

    selected_codes = {code.strip().upper() for code in selected.keys()}
    missing = sorted(code for code in report_codes if code not in selected_codes)
    if missing:
        raise ValueError(
            "REPORT_STORES_LIST includes stores not present in the scraping run: %s"
            % ",".join(missing)
        )


def load_settings(*, stores_list: Optional[str], dry_run: bool, run_id: str) -> PipelineSettings:
    raw_upper_env = ",".join(config.stores_list)
    cli_values = _split_codes(stores_list)
    _validate_store_selector_sources(cli=cli_values, env_upper=raw_upper_env)

    env_list = env_stores_list()
    cli_list = [s.strip() for s in (stores_list.split(",") if stores_list else []) if s.strip()]
    final_list: List[str] = cli_list or env_list
    if not final_list:
        raise ValueError("At least one store code must be provided via --stores_list or STORES_LIST")

    raw_env = raw_upper_env or ""
    selected = stores_from_list(final_list)

    _ensure_report_store_alignment(selected)
    return PipelineSettings(run_id=run_id, stores=selected, raw_store_env=raw_env, dry_run=dry_run)
