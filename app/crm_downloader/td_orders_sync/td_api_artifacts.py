from __future__ import annotations

import json
import logging
import os
import re

import openpyxl
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence

logger = logging.getLogger(__name__)

_ILLEGAL_XLSX_CHARS_RE = re.compile(r"[\000-\010]|[\013-\014]|[\016-\037]")

_SENSITIVE_FIELD_NAMES = {"token", "authorization", "cookie", "set-cookie"}
_REDACTED = "***REDACTED***"


def redact_sensitive_fields(payload: Any) -> Any:
    def _is_sensitive_field(key: Any) -> bool:
        return str(key).strip().lower() in _SENSITIVE_FIELD_NAMES

    if isinstance(payload, Mapping):
        redacted: dict[str, Any] = {}
        for key, value in payload.items():
            key_text = str(key)
            redacted[key_text] = _REDACTED if _is_sensitive_field(key) else redact_sensitive_fields(value)
        return redacted
    if isinstance(payload, list):
        return [redact_sensitive_fields(item) for item in payload]
    if isinstance(payload, tuple):
        return tuple(redact_sensitive_fields(item) for item in payload)
    if isinstance(payload, set):
        return {redact_sensitive_fields(item) for item in payload}
    return payload


@dataclass
class TdApiArtifactPersistResult:
    artifact_paths: dict[str, str] = field(default_factory=dict)
    human_readable_export_enabled: bool = False
    human_readable_artifact_paths: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)



def _window_token(value: date) -> str:
    return value.strftime("%Y%m%d")



def _raw_filename(store_code: str, dataset: str, from_date: date, to_date: date) -> str:
    return f"{store_code}_td_api_{dataset}_{_window_token(from_date)}_{_window_token(to_date)}.json"



def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(redact_sensitive_fields(payload), ensure_ascii=False, indent=2), encoding="utf-8")



def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(redact_sensitive_fields(dict(row)), ensure_ascii=False, sort_keys=True))
            handle.write("\n")


def _write_excel(path: Path, rows: Sequence[Mapping[str, Any]], *, sheet_name: str = "rows") -> None:
    workbook = openpyxl.Workbook()
    worksheet = workbook.active
    worksheet.title = sheet_name

    def _json_compatible(value: Any) -> Any:
        if isinstance(value, Mapping):
            return {str(key): _json_compatible(nested_value) for key, nested_value in value.items()}
        if isinstance(value, (list, tuple)):
            return [_json_compatible(item) for item in value]
        if isinstance(value, set):
            normalized_items = [_json_compatible(item) for item in value]
            return sorted(normalized_items, key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True))
        return value

    def _excel_safe_cell_value(value: Any) -> Any:
        if isinstance(value, (Mapping, list, tuple, set)):
            return json.dumps(_json_compatible(value), ensure_ascii=False, sort_keys=True)
        if isinstance(value, str):
            return _ILLEGAL_XLSX_CHARS_RE.sub("", value)
        return value

    if not rows:
        worksheet.append(["status"])
        worksheet.append(["no rows"])
    else:
        columns: list[str] = []
        for row in rows:
            for key in row.keys():
                key_text = str(key)
                if key_text not in columns:
                    columns.append(key_text)
        worksheet.append(columns)
        for row in rows:
            worksheet.append([_excel_safe_cell_value(row.get(column)) for column in columns])
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(path)


def _excel_filename(store_code: str, dataset: str, from_date: date, to_date: date) -> str:
    return f"{store_code}_td_api_{dataset}_{_window_token(from_date)}_{_window_token(to_date)}.xlsx"


def _human_readable_export_enabled() -> bool:
    return (os.environ.get("TD_API_HUMAN_READABLE_EXPORT") or "true").strip().lower() in {"1", "true", "yes", "on"}



def persist_td_api_artifacts(
    *,
    download_dir: Path,
    store_code: str,
    from_date: date,
    to_date: date,
    raw_orders: Any,
    raw_sales: Any,
    raw_garments: Any,
    order_rows: Sequence[Mapping[str, Any]],
    sale_rows: Sequence[Mapping[str, Any]],
    garments_rows: Sequence[Mapping[str, Any]],
) -> TdApiArtifactPersistResult:
    human_readable_export_enabled = _human_readable_export_enabled()
    result = TdApiArtifactPersistResult(human_readable_export_enabled=human_readable_export_enabled)
    store = (store_code or "").strip().upper() or "UNKNOWN"
    artifact_targets: list[tuple[str, Path, Any, str]] = [
        ("orders_raw", download_dir / _raw_filename(store, "orders", from_date, to_date), raw_orders, "json"),
        ("sales_raw", download_dir / _raw_filename(store, "sales", from_date, to_date), raw_sales, "json"),
        ("garments_raw", download_dir / _raw_filename(store, "garments", from_date, to_date), raw_garments, "json"),
    ]

    for key, path, payload, kind in artifact_targets:
        try:
            if kind == "jsonl":
                _write_jsonl(path, payload)
            elif kind == "xlsx":
                _write_excel(path, payload, sheet_name=key)
            else:
                _write_json(path, payload)
            result.artifact_paths[key] = str(path)
        except Exception as exc:  # pragma: no cover - defensive guard
            warning = f"Failed to persist TD API artifact '{key}' at {path}: {exc}"
            result.warnings.append(warning)
            logger.warning(warning)

    if human_readable_export_enabled:
        diagnostic_targets: list[tuple[str, Path, Sequence[Mapping[str, Any]], str]] = [
            ("orders_excel", download_dir / _excel_filename(store, "orders", from_date, to_date), order_rows, "orders"),
            ("sales_excel", download_dir / _excel_filename(store, "sales", from_date, to_date), sale_rows, "sales"),
            (
                "garments_excel",
                download_dir / _excel_filename(store, "garments", from_date, to_date),
                garments_rows,
                "garments",
            ),
        ]
        for key, path, rows, sheet_name in diagnostic_targets:
            try:
                _write_excel(path, rows, sheet_name=sheet_name)
                result.artifact_paths[key] = str(path)
                result.human_readable_artifact_paths.append(str(path))
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
    endpoint_health_summary: Mapping[str, Any] | None = None,
) -> TdApiArtifactPersistResult:
    _ = (
        download_dir,
        store_code,
        from_date,
        to_date,
        orders_compare_metrics,
        sales_compare_metrics,
        endpoint_health_summary,
    )
    return TdApiArtifactPersistResult()
