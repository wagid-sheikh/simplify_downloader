from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence

from app.crm_downloader.td_orders_sync.td_api_compare import CompareDiffReport

logger = logging.getLogger(__name__)


@dataclass
class TdApiArtifactPersistResult:
    artifact_paths: dict[str, str] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)



def _window_token(value: date) -> str:
    return value.strftime("%Y%m%d")



def _raw_filename(store_code: str, dataset: str, from_date: date, to_date: date) -> str:
    return f"{store_code}_td_api_{dataset}_{_window_token(from_date)}_{_window_token(to_date)}.json"



def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")



def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(dict(row), ensure_ascii=False, sort_keys=True))
            handle.write("\n")



def _window_dir(download_dir: Path, store_code: str, from_date: date, to_date: date) -> Path:
    store = (store_code or "").strip().upper() or "UNKNOWN"
    return download_dir / f"{store}_td_api_{_window_token(from_date)}_{_window_token(to_date)}"


def _window_file_prefix(store_code: str, from_date: date, to_date: date) -> str:
    store = (store_code or "").strip().upper() or "UNKNOWN"
    return f"{store}_td_api_{_window_token(from_date)}_{_window_token(to_date)}"


def persist_td_compare_artifacts(
    *,
    download_dir: Path,
    store_code: str,
    from_date: date,
    to_date: date,
    dataset: str,
    diff_report: CompareDiffReport,
    key_fields: Sequence[str],
    row_sample_cap: int,
) -> TdApiArtifactPersistResult:
    result = TdApiArtifactPersistResult()
    window_dir = _window_dir(download_dir, store_code, from_date, to_date)
    prefix = f"{_window_file_prefix(store_code, from_date, to_date)}_{dataset}"

    missing_in_api_rows = [row.as_dict() for row in diff_report.missing_in_api_rows[:row_sample_cap]]
    missing_in_ui_rows = [row.as_dict() for row in diff_report.missing_in_ui_rows[:row_sample_cap]]
    value_mismatch_rows = [row.as_dict() for row in diff_report.value_mismatch_rows[:row_sample_cap]]

    artifact_targets: list[tuple[str, Path, Any, str]] = [
        (
            f"{dataset}_compare_summary",
            window_dir / f"{prefix}_compare_summary.json",
            diff_report.summary_dict(dataset=dataset, key_fields=key_fields, row_sample_cap=row_sample_cap),
            "json",
        ),
        (f"{dataset}_missing_in_api", window_dir / f"{prefix}_missing_in_api.jsonl", missing_in_api_rows, "jsonl"),
        (f"{dataset}_missing_in_ui", window_dir / f"{prefix}_missing_in_ui.jsonl", missing_in_ui_rows, "jsonl"),
        (
            f"{dataset}_value_mismatches",
            window_dir / f"{prefix}_value_mismatches.jsonl",
            value_mismatch_rows,
            "jsonl",
        ),
    ]

    for key, path, payload, kind in artifact_targets:
        try:
            if kind == "jsonl":
                _write_jsonl(path, payload)
            else:
                _write_json(path, payload)
            result.artifact_paths[key] = str(path)
        except Exception as exc:  # pragma: no cover - defensive guard
            warning = f"Failed to persist TD compare artifact '{key}' at {path}: {exc}"
            result.warnings.append(warning)
            logger.warning(warning)

    return result


def persist_td_api_artifacts(
    *,
    download_dir: Path,
    store_code: str,
    from_date: date,
    to_date: date,
    raw_orders: Any,
    raw_sales: Any,
    raw_garments: Any,
    canonical_orders: Sequence[Mapping[str, Any]],
    canonical_sales: Sequence[Mapping[str, Any]],
    canonical_garments: Sequence[Mapping[str, Any]],
) -> TdApiArtifactPersistResult:
    result = TdApiArtifactPersistResult()
    store = (store_code or "").strip().upper() or "UNKNOWN"
    window_dir = _window_dir(download_dir, store, from_date, to_date)

    artifact_targets: list[tuple[str, Path, Any, str]] = [
        ("orders_raw", download_dir / _raw_filename(store, "orders", from_date, to_date), raw_orders, "json"),
        ("sales_raw", download_dir / _raw_filename(store, "sales", from_date, to_date), raw_sales, "json"),
        ("garments_raw", download_dir / _raw_filename(store, "garments", from_date, to_date), raw_garments, "json"),
        ("orders_raw_alias", window_dir / "orders_raw.json", raw_orders, "json"),
        ("sales_raw_alias", window_dir / "sales_raw.json", raw_sales, "json"),
        ("garments_raw_alias", window_dir / "garments_raw.json", raw_garments, "json"),
        ("orders_canonical", window_dir / "orders_canonical.jsonl", canonical_orders, "jsonl"),
        ("sales_canonical", window_dir / "sales_canonical.jsonl", canonical_sales, "jsonl"),
        ("garments_canonical", window_dir / "garments_canonical.jsonl", canonical_garments, "jsonl"),
    ]

    for key, path, payload, kind in artifact_targets:
        try:
            if kind == "jsonl":
                _write_jsonl(path, payload)
            else:
                _write_json(path, payload)
            result.artifact_paths[key] = str(path)
        except Exception as exc:  # pragma: no cover - defensive guard
            warning = f"Failed to persist TD API artifact '{key}' at {path}: {exc}"
            result.warnings.append(warning)
            logger.warning(warning)

    return result
