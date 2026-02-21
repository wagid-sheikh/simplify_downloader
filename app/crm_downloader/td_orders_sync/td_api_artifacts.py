from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence

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
    window_dir = download_dir / f"{store}_td_api_{_window_token(from_date)}_{_window_token(to_date)}"

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


def persist_td_compare_artifacts(
    *,
    download_dir: Path,
    store_code: str,
    from_date: date,
    to_date: date,
    orders_compare_metrics: Mapping[str, Any],
    sales_compare_metrics: Mapping[str, Any],
) -> TdApiArtifactPersistResult:
    result = TdApiArtifactPersistResult()
    store = (store_code or "").strip().upper() or "UNKNOWN"
    window_dir = download_dir / f"{store}_td_api_{_window_token(from_date)}_{_window_token(to_date)}"

    artifact_targets: list[tuple[str, Path, Any]] = [
        (
            "orders_compare_mismatches",
            window_dir / "orders_compare_mismatches.json",
            (orders_compare_metrics or {}).get("mismatch_artifacts") or {},
        ),
        (
            "sales_compare_mismatches",
            window_dir / "sales_compare_mismatches.json",
            (sales_compare_metrics or {}).get("mismatch_artifacts") or {},
        ),
    ]

    for key, path, payload in artifact_targets:
        try:
            _write_json(path, payload)
            result.artifact_paths[key] = str(path)
        except Exception as exc:  # pragma: no cover - defensive guard
            warning = f"Failed to persist TD compare artifact '{key}' at {path}: {exc}"
            result.warnings.append(warning)
            logger.warning(warning)

    return result
