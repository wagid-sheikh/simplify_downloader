from __future__ import annotations

from datetime import date, datetime
from functools import lru_cache
from typing import Any, Dict, Iterable, Optional

from pydantic import BaseModel, ConfigDict, ValidationError, create_model

try:
    from dashboard_downloader.config import MERGE_BUCKET_DB_SPECS
except ModuleNotFoundError:
    import importlib.util
    import sys
    from pathlib import Path

    project_root = Path(__file__).resolve().parents[2]
    package_dir = project_root / "dashboard_downloader"
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    package_spec = importlib.util.spec_from_file_location(
        "dashboard_downloader", package_dir / "__init__.py"
    )
    if package_spec and package_spec.loader:
        package = importlib.util.module_from_spec(package_spec)
        package_spec.loader.exec_module(package)
        package.__path__ = [str(package_dir)]
        sys.modules["dashboard_downloader"] = package

    config_spec = importlib.util.spec_from_file_location(
        "dashboard_downloader.config", package_dir / "config.py"
    )
    if not config_spec or not config_spec.loader:
        raise
    config_module = importlib.util.module_from_spec(config_spec)
    config_spec.loader.exec_module(config_module)
    sys.modules["dashboard_downloader.config"] = config_module
    MERGE_BUCKET_DB_SPECS = config_module.MERGE_BUCKET_DB_SPECS


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


def _normalize_variants(header: str) -> Iterable[str]:
    """Yield common normalized variants for a header string."""

    normalized = header.strip().lower()
    if not normalized:
        return []

    variants = {normalized}
    # Replace spaces/underscores/dashes with alternate forms to improve matching.
    variants.add(normalized.replace(" ", ""))
    variants.add(normalized.replace(" ", "_"))
    variants.add(normalized.replace("_", ""))
    variants.add(normalized.replace("_", " "))
    variants.add(normalized.replace("-", ""))
    variants.add(normalized.replace("-", "_"))
    variants.add(normalized.replace("-", " "))

    return variants


def normalize_headers(headers: list[str]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for header in headers:
        for variant in _normalize_variants(header):
            mapping.setdefault(variant, header)
    return mapping


def _header_lookup(header_map: Dict[str, str], key: str) -> str | None:
    """Return the canonical header for a provided lookup key using variants."""

    for variant in _normalize_variants(key):
        if variant in header_map:
            return header_map[variant]
    return None


def coerce_csv_row(bucket: str, row: Dict[str, Any], header_map: Dict[str, str]) -> Dict[str, Any]:
    spec = MERGE_BUCKET_DB_SPECS[bucket]
    column_map = spec["column_map"]
    coerced: Dict[str, Any] = {}
    for csv_key, dest_key in column_map.items():
        keys = (csv_key,) if isinstance(csv_key, str) else tuple(csv_key)
        value = None
        for key in keys:
            lookup = _header_lookup(header_map, key)
            if lookup is not None:
                value = row.get(lookup)
            if value is None:
                value = row.get(key)
            if value is not None:
                break
        kind = spec["coerce"].get(dest_key, "str")
        coerced[dest_key] = _coerce_value(kind, value)
    model = bucket_model(bucket)
    try:
        validated = model(**coerced)
    except ValidationError as err:
        raise ValueError(str(err)) from err
    data = validated.model_dump()

    required_columns = set(spec.get("required_columns", []))
    required_columns.update(spec.get("dedupe_keys", []))

    missing = [column for column in required_columns if data.get(column) is None]
    if missing:
        raise ValueError(f"Missing required values for: {', '.join(sorted(missing))}")

    return data
