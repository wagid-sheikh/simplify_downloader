from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

from dateutil import parser

from app.common.date_utils import get_timezone

MISSING_KEY_PART = "<MISSING>"

COMPARE_KEY_FIELDS_BY_DATASET: dict[str, tuple[str, ...]] = {
    "orders": ("store_code", "order_number", "order_date"),
    "sales": ("store_code", "order_number", "payment_date"),
    "garments": ("store_code", "order_number", "line_item_key"),
}

KEY_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "store_code": ("store_code", "store", "cost_center"),
    "order_number": ("order_number", "order_no", "orderNo", "orderNumber"),
    "order_date": ("order_date", "orderDate"),
    "payment_date": ("payment_date", "paymentDate"),
    "payment_mode": ("payment_mode", "paymentMode", "mode"),
    "line_item_key": ("line_item_key", "lineItemKey", "itemKey", "line_identifier", "api_line_item_id"),
}


@dataclass(frozen=True)
class CorrelationContext:
    run_id: str
    store_code: str
    window_start: str
    window_end: str
    source_mode: str

    def as_dict(self) -> dict[str, str]:
        return {
            "run_id": self.run_id,
            "store_code": self.store_code,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "source_mode": self.source_mode,
        }


@dataclass(frozen=True)
class ApiRequestMetadata:
    endpoint: str
    method: str
    query_params: dict[str, list[str]]
    status: int | None
    latency_ms: int | None
    retry_count: int
    token_refresh_attempted: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "endpoint": self.endpoint,
            "method": self.method,
            "query_params": self.query_params,
            "status": self.status,
            "latency_ms": self.latency_ms,
            "retry_count": self.retry_count,
            "token_refresh_attempted": self.token_refresh_attempted,
        }


@dataclass(frozen=True)
class AuthDiagnostics:
    cookies_found: bool
    token_found: bool
    token_expiry: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "cookies_found": self.cookies_found,
            "token_found": self.token_found,
            "token_expiry": self.token_expiry,
        }


@dataclass(frozen=True)
class CompareMetrics:
    total_rows: int
    matched_rows: int
    missing_in_api: int
    missing_in_ui: int
    amount_mismatches: int
    status_mismatches: int
    sample_mismatch_keys: list[str]
    mismatch_artifacts: dict[str, list[dict[str, Any]]]

    def as_dict(self) -> dict[str, Any]:
        return {
            "total_rows": self.total_rows,
            "matched_rows": self.matched_rows,
            "missing_in_api": self.missing_in_api,
            "missing_in_ui": self.missing_in_ui,
            "amount_mismatches": self.amount_mismatches,
            "status_mismatches": self.status_mismatches,
            "sample_mismatch_keys": self.sample_mismatch_keys,
            "mismatch_artifacts": self.mismatch_artifacts,
        }


@dataclass(frozen=True)
class DecisionLog:
    decision: str
    reason: str

    def as_dict(self) -> dict[str, str]:
        return {"decision": self.decision, "reason": self.reason}


def _safe_b64decode(value: str) -> bytes | None:
    try:
        padding = "=" * ((4 - len(value) % 4) % 4)
        return base64.urlsafe_b64decode(value + padding)
    except Exception:
        return None


def parse_token_expiry(token: str | None) -> str | None:
    if not token or token.count(".") < 2:
        return None
    payload = _safe_b64decode(token.split(".")[1])
    if not payload:
        return None
    try:
        payload_json = json.loads(payload.decode("utf-8"))
    except Exception:
        return None
    exp = payload_json.get("exp")
    if not isinstance(exp, (int, float)):
        return None
    return datetime.fromtimestamp(exp, timezone.utc).isoformat()


def collect_auth_diagnostics(storage_state_path: Path | None) -> AuthDiagnostics:
    cookies_found = False
    token_found = False
    token_expiry: str | None = None
    if not storage_state_path or not storage_state_path.exists():
        return AuthDiagnostics(cookies_found=False, token_found=False, token_expiry=None)
    try:
        state = json.loads(storage_state_path.read_text(encoding="utf-8"))
    except Exception:
        return AuthDiagnostics(cookies_found=False, token_found=False, token_expiry=None)

    cookies = state.get("cookies") if isinstance(state, Mapping) else None
    cookies_found = isinstance(cookies, list) and bool(cookies)

    candidate_token: str | None = None
    origins = state.get("origins") if isinstance(state, Mapping) else None
    if isinstance(origins, list):
        for origin in origins:
            if not isinstance(origin, Mapping):
                continue
            local_storage = origin.get("localStorage")
            if not isinstance(local_storage, list):
                continue
            for entry in local_storage:
                if not isinstance(entry, Mapping):
                    continue
                name = str(entry.get("name") or "").lower()
                value = entry.get("value")
                if value and any(key in name for key in ("token", "auth", "jwt", "bearer")):
                    token_found = True
                    candidate_token = str(value)
                    break
            if token_found:
                break

    token_expiry = parse_token_expiry(candidate_token)
    return AuthDiagnostics(cookies_found=cookies_found, token_found=token_found, token_expiry=token_expiry)


def build_api_request_metadata(
    *,
    url: str,
    method: str,
    status: int | None,
    latency_ms: int | None,
    retry_count: int = 0,
    token_refresh_attempted: bool = False,
) -> ApiRequestMetadata:
    parsed = urlparse(url)
    endpoint = parsed.path
    query_params = {key: sorted(values) for key, values in parse_qs(parsed.query, keep_blank_values=True).items()}
    return ApiRequestMetadata(
        endpoint=endpoint,
        method=method.upper(),
        query_params=query_params,
        status=status,
        latency_ms=latency_ms,
        retry_count=max(retry_count, 0),
        token_refresh_attempted=token_refresh_attempted,
    )


def _normalized_key_part(value: Any) -> str:
    normalized = str(value).strip() if value is not None else ""
    return normalized if normalized else MISSING_KEY_PART


def _resolve_row_field(row: Mapping[str, Any], field: str, *, default_store_code: str | None = None) -> Any:
    if field == "store_code" and default_store_code:
        candidates = (*KEY_FIELD_ALIASES.get(field, (field,)),)
        for candidate in candidates:
            value = row.get(candidate)
            if value is not None and str(value).strip():
                return value
        return default_store_code
    for candidate in KEY_FIELD_ALIASES.get(field, (field,)):
        value = row.get(candidate)
        if value is not None and str(value).strip():
            return value
    return row.get(field)


VALUE_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "order_number": ("Order No.", "Order Number"),
    "order_date": ("Order Date / Time", "Order Date"),
    "payment_date": ("Payment Date",),
    "payment_mode": ("Payment Mode",),
    "amount": ("Payment Received", "Amount", "Total"),
    "status": ("Status",),
}


def _resolve_nested_value(row: Mapping[str, Any], field: str) -> Any:
    values = row.get("values")
    if not isinstance(values, Mapping):
        return None
    for candidate in (*KEY_FIELD_ALIASES.get(field, ()), *VALUE_FIELD_ALIASES.get(field, ())):
        value = values.get(candidate)
        if value is not None and str(value).strip():
            return value
    return None


def _resolve_field_value(row: Mapping[str, Any], field: str, *, default_store_code: str | None = None) -> Any:
    resolved = _resolve_row_field(row, field, default_store_code=default_store_code)
    if resolved is not None and str(resolved).strip():
        return resolved
    return _resolve_nested_value(row, field)


def _normalize_datetime_value(value: Any) -> str:
    if value in (None, ""):
        return ""
    tz = _safe_timezone()
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = parser.parse(str(value))
        except Exception:
            return str(value).strip()
    normalized = parsed if parsed.tzinfo else parsed.replace(tzinfo=tz)
    return normalized.isoformat()


def _safe_timezone() -> ZoneInfo:
    try:
        return get_timezone()
    except Exception:
        return ZoneInfo("Asia/Kolkata")


def _normalize_numeric_value(value: Any) -> str:
    if value in (None, ""):
        return ""
    try:
        if isinstance(value, (int, float, Decimal)):
            numeric = Decimal(str(value))
        else:
            numeric = Decimal(str(value).replace(",", "").strip())
    except (InvalidOperation, ValueError):
        return str(value).strip()
    return f"{numeric.quantize(Decimal('0.01'))}"


def _normalized_compare_value(field: str, value: Any) -> str:
    if field in {"order_date", "payment_date"}:
        return _normalize_datetime_value(value)
    if field in {"amount", "total", "net_amount", "payment_received"}:
        return _normalize_numeric_value(value)
    return str(value).strip()


def _infer_default_store_code(ui_rows: Sequence[Mapping[str, Any]], api_rows: Sequence[Mapping[str, Any]]) -> str | None:
    stores = {
        str(_resolve_row_field(row, "store_code")).strip().upper()
        for row in [*ui_rows, *api_rows]
        if _resolve_row_field(row, "store_code") is not None and str(_resolve_row_field(row, "store_code")).strip()
    }
    return next(iter(stores)) if len(stores) == 1 else None


def _canonical_key(
    row: Mapping[str, Any],
    key_fields: Sequence[str],
    *,
    default_store_code: str | None = None,
) -> tuple[str, dict[str, str]]:
    key_parts = {
        field: _normalized_key_part(
            _normalized_compare_value(field, _resolve_field_value(row, field, default_store_code=default_store_code))
        )
        for field in key_fields
    }
    key = "|".join(key_parts[field] for field in key_fields)
    return key, key_parts


def _format_key_parts(key_parts: Mapping[str, str]) -> str:
    return ", ".join(f"{field}={value}" for field, value in key_parts.items())


def _normalized_order_number(row: Mapping[str, Any], *, default_store_code: str | None = None) -> str:
    return _normalized_key_part(
        _normalized_compare_value("order_number", _resolve_field_value(row, "order_number", default_store_code=default_store_code))
    )


def _is_sales_compare(key_fields: Sequence[str]) -> bool:
    return "payment_date" in key_fields or "payment_mode" in key_fields


def project_api_rows_for_compare(
    *,
    dataset: str,
    api_rows: Iterable[Mapping[str, Any]],
    store_code: str | None = None,
) -> list[dict[str, Any]]:
    """Project raw API rows into compare-only canonical fields."""
    normalized_store = (store_code or "").strip().upper()
    projected_rows: list[dict[str, Any]] = []
    for row in api_rows:
        projected: dict[str, Any] = {}
        if normalized_store:
            projected["store_code"] = normalized_store
        for field in ("order_number", "order_date", "payment_date", "payment_mode", "line_item_key", "amount", "status"):
            value = _resolve_field_value(row, field, default_store_code=normalized_store or None)
            if value is not None and str(value).strip() != "":
                projected[field] = value
        if dataset == "orders":
            projected.pop("payment_date", None)
            projected.pop("payment_mode", None)
            projected.pop("line_item_key", None)
        elif dataset == "sales":
            projected.pop("order_date", None)
            projected.pop("line_item_key", None)
        elif dataset == "garments":
            projected.pop("payment_date", None)
            projected.pop("payment_mode", None)
        projected_rows.append(projected)
    return projected_rows


def compare_canonical_rows(
    *,
    ui_rows: Iterable[Mapping[str, Any]],
    api_rows: Iterable[Mapping[str, Any]],
    key_fields: Sequence[str],
    amount_fields: Sequence[str] = ("amount", "total", "net_amount"),
    status_fields: Sequence[str] = ("status", "order_status"),
    sample_limit: int = 20,
) -> CompareMetrics:
    ui_rows_seq = list(ui_rows)
    api_rows_seq = list(api_rows)
    default_store_code = _infer_default_store_code(ui_rows_seq, api_rows_seq)

    ui_index: dict[str, Mapping[str, Any]] = {}
    api_index: dict[str, Mapping[str, Any]] = {}
    key_breakdown: dict[str, dict[str, str]] = {}

    for row in ui_rows_seq:
        key, parts = _canonical_key(row, key_fields, default_store_code=default_store_code)
        ui_index[key] = row
        key_breakdown.setdefault(key, parts)

    for row in api_rows_seq:
        key, parts = _canonical_key(row, key_fields, default_store_code=default_store_code)
        api_index[key] = row
        key_breakdown.setdefault(key, parts)

    keys = set(ui_index) | set(api_index)
    sales_compare = _is_sales_compare(key_fields)
    ui_order_numbers = {_normalized_order_number(row, default_store_code=default_store_code) for row in ui_rows_seq}
    api_order_numbers = {_normalized_order_number(row, default_store_code=default_store_code) for row in api_rows_seq}

    missing_in_api = sorted(ui_order_numbers - api_order_numbers)
    missing_in_ui = sorted(api_order_numbers - ui_order_numbers)
    shared = sorted(key for key in keys if key in ui_index and key in api_index)
    shared_order_numbers = sorted(ui_order_numbers & api_order_numbers)

    amount_mismatches = 0
    status_mismatches = 0
    mismatch_samples: list[str] = []
    value_mismatch_keys: list[str] = []
    sales_order_row_count_mismatches: list[dict[str, Any]] = []

    for key in shared:
        ui_row = ui_index[key]
        api_row = api_index[key]
        amount_mismatch = False
        status_mismatch = False

        for field in amount_fields:
            ui_val = _resolve_field_value(ui_row, field, default_store_code=default_store_code)
            api_val = _resolve_field_value(api_row, field, default_store_code=default_store_code)
            if ui_val is not None or api_val is not None:
                if _normalized_compare_value(field, ui_val) != _normalized_compare_value(field, api_val):
                    amount_mismatch = True
                break
        for field in status_fields:
            ui_val = _resolve_field_value(ui_row, field, default_store_code=default_store_code)
            api_val = _resolve_field_value(api_row, field, default_store_code=default_store_code)
            if ui_val is not None or api_val is not None:
                if str(ui_val).strip().lower() != str(api_val).strip().lower():
                    status_mismatch = True
                break

        if amount_mismatch:
            amount_mismatches += 1
        if status_mismatch:
            status_mismatches += 1
        if amount_mismatch or status_mismatch:
            value_mismatch_keys.append(key)
        if (amount_mismatch or status_mismatch) and len(mismatch_samples) < sample_limit:
            mismatch_samples.append(f"shared_mismatch ({_format_key_parts(key_breakdown.get(key, {}))})")

    if sales_compare:
        ui_sales_order_counts: dict[str, int] = {}
        api_sales_order_counts: dict[str, int] = {}
        for row in ui_rows_seq:
            order_no = _normalized_order_number(row, default_store_code=default_store_code)
            ui_sales_order_counts[order_no] = ui_sales_order_counts.get(order_no, 0) + 1
        for row in api_rows_seq:
            order_no = _normalized_order_number(row, default_store_code=default_store_code)
            api_sales_order_counts[order_no] = api_sales_order_counts.get(order_no, 0) + 1

        for order_no in shared_order_numbers:
            ui_count = ui_sales_order_counts.get(order_no, 0)
            api_count = api_sales_order_counts.get(order_no, 0)
            if ui_count != api_count:
                sales_order_row_count_mismatches.append(
                    {"order_number": order_no, "ui_row_count": ui_count, "api_row_count": api_count}
                )
                if len(mismatch_samples) < sample_limit:
                    mismatch_samples.append(
                        f"sales_row_count_mismatch (order_number={order_no}, ui_rows={ui_count}, api_rows={api_count})"
                    )

    matched_rows = len(shared_order_numbers) - len(sales_order_row_count_mismatches)
    missing_api_samples = [
        f"missing_in_api_order_number (order_number={order_no})" for order_no in missing_in_api
    ]
    missing_ui_samples = [
        f"missing_in_ui_order_number (order_number={order_no})" for order_no in missing_in_ui
    ]
    sample_keys = (missing_api_samples + missing_ui_samples + mismatch_samples)[:sample_limit]

    mismatch_artifacts = {
        "missing_in_api": [
            {"order_number": order_no, "key": order_no, "key_components": {"order_number": order_no}}
            for order_no in missing_in_api
        ],
        "missing_in_ui": [
            {"order_number": order_no, "key": order_no, "key_components": {"order_number": order_no}}
            for order_no in missing_in_ui
        ],
        "value_mismatches": [
            {"key": key, "key_components": key_breakdown.get(key, {})} for key in value_mismatch_keys
        ],
    }
    if sales_compare:
        mismatch_artifacts["sales_order_row_count_mismatches"] = sales_order_row_count_mismatches

    return CompareMetrics(
        total_rows=len(ui_order_numbers | api_order_numbers),
        matched_rows=max(matched_rows, 0),
        missing_in_api=len(missing_in_api),
        missing_in_ui=len(missing_in_ui),
        amount_mismatches=amount_mismatches,
        status_mismatches=status_mismatches,
        sample_mismatch_keys=sample_keys,
        mismatch_artifacts=mismatch_artifacts,
    )
