from __future__ import annotations

from datetime import date, datetime
from functools import lru_cache
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, ValidationError, create_model

from downloader.config import MERGE_BUCKET_DB_SPECS


TYPE_MAP = {
    "int": int,
    "str": str,
    "float": float,
    "bool": bool,
    "date": date,
}


class BucketRow(BaseModel):
    model_config = ConfigDict(extra="ignore", arbitrary_types_allowed=True)


def _coerce_value(kind: str, value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        raw = value.strip()
    else:
        raw = str(value).strip()
    if raw == "":
        return None

    if kind == "int":
        try:
            return int(float(raw))
        except ValueError:
            return int(raw)
    if kind == "float":
        return float(raw)
    if kind == "bool":
        lowered = raw.lower()
        if lowered in {"1", "true"}:
            return True
        if lowered in {"0", "false"}:
            return False
        return False
    if kind == "date":
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                return datetime.strptime(raw, fmt).date()
            except ValueError:
                continue
        return None
    return raw


@lru_cache(maxsize=None)
def bucket_model(bucket: str) -> type[BucketRow]:
    spec = MERGE_BUCKET_DB_SPECS[bucket]
    fields: Dict[str, tuple[type | None, Any]] = {}
    for column, type_name in spec["coerce"].items():
        py_type = TYPE_MAP[type_name]
        fields[column] = (Optional[py_type], None)
    return create_model(f"{bucket.title()}Row", __base=BucketRow, **fields)


def normalize_headers(headers: list[str]) -> Dict[str, str]:
    return {header.lower(): header for header in headers}


def coerce_csv_row(bucket: str, row: Dict[str, Any], header_map: Dict[str, str]) -> Dict[str, Any]:
    spec = MERGE_BUCKET_DB_SPECS[bucket]
    column_map = spec["column_map"]
    coerced: Dict[str, Any] = {}
    for csv_key, dest_key in column_map.items():
        lookup = header_map.get(csv_key.lower())
        value = row.get(lookup) if lookup else row.get(csv_key)
        kind = spec["coerce"].get(dest_key, "str")
        coerced[dest_key] = _coerce_value(kind, value)
    model = bucket_model(bucket)
    try:
        validated = model(**coerced)
    except ValidationError as err:
        raise ValueError(str(err)) from err
    return validated.model_dump()
