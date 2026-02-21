from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import parse_qs, urlparse

MISSING_KEY_PART = "<MISSING>"

COMPARE_KEY_FIELDS_BY_DATASET: dict[str, tuple[str, ...]] = {
    "orders": ("store_code", "order_number", "order_date"),
    "sales": ("store_code", "order_number", "payment_date", "payment_mode"),
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

    def as_dict(self) -> dict[str, Any]:
        return {
            "total_rows": self.total_rows,
            "matched_rows": self.matched_rows,
            "missing_in_api": self.missing_in_api,
            "missing_in_ui": self.missing_in_ui,
            "amount_mismatches": self.amount_mismatches,
            "status_mismatches": self.status_mismatches,
            "sample_mismatch_keys": self.sample_mismatch_keys,
        }


@dataclass(frozen=True)
class CompareDiffRow:
    key: str
    key_fields: dict[str, str]
    reason_code: str
    ui_row: dict[str, Any] | None = None
    api_row: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "key": self.key,
            "key_fields": dict(self.key_fields),
            "reason_code": self.reason_code,
        }
        if self.ui_row is not None:
            payload["ui_row"] = dict(self.ui_row)
        if self.api_row is not None:
            payload["api_row"] = dict(self.api_row)
        return payload


@dataclass(frozen=True)
class CompareDiffReport:
    metrics: CompareMetrics
    missing_in_api_rows: list[CompareDiffRow]
    missing_in_ui_rows: list[CompareDiffRow]
    value_mismatch_rows: list[CompareDiffRow]

    def summary_dict(self, *, dataset: str, key_fields: Sequence[str], row_sample_cap: int) -> dict[str, Any]:
        return {
            "dataset": dataset,
            "key_fields": list(key_fields),
            "counts": {
                "total_rows": self.metrics.total_rows,
                "matched_rows": self.metrics.matched_rows,
                "missing_in_api": self.metrics.missing_in_api,
                "missing_in_ui": self.metrics.missing_in_ui,
                "amount_mismatches": self.metrics.amount_mismatches,
                "status_mismatches": self.metrics.status_mismatches,
                "value_mismatches": len(self.value_mismatch_rows),
            },
            "row_sample_cap": row_sample_cap,
            "truncated": {
                "missing_in_api": len(self.missing_in_api_rows) > row_sample_cap,
                "missing_in_ui": len(self.missing_in_ui_rows) > row_sample_cap,
                "value_mismatches": len(self.value_mismatch_rows) > row_sample_cap,
            },
            "sample_mismatch_keys": list(self.metrics.sample_mismatch_keys),
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
        field: _normalized_key_part(_resolve_row_field(row, field, default_store_code=default_store_code))
        for field in key_fields
    }
    key = "|".join(key_parts[field] for field in key_fields)
    return key, key_parts


def _format_key_parts(key_parts: Mapping[str, str]) -> str:
    return ", ".join(f"{field}={value}" for field, value in key_parts.items())


def compare_canonical_rows(
    *,
    ui_rows: Iterable[Mapping[str, Any]],
    api_rows: Iterable[Mapping[str, Any]],
    key_fields: Sequence[str],
    amount_fields: Sequence[str] = ("amount", "total", "net_amount"),
    status_fields: Sequence[str] = ("status", "order_status"),
    sample_limit: int = 20,
) -> CompareMetrics:
    return compare_canonical_rows_detailed(
        ui_rows=ui_rows,
        api_rows=api_rows,
        key_fields=key_fields,
        amount_fields=amount_fields,
        status_fields=status_fields,
        sample_limit=sample_limit,
    ).metrics


def compare_canonical_rows_detailed(
    *,
    ui_rows: Iterable[Mapping[str, Any]],
    api_rows: Iterable[Mapping[str, Any]],
    key_fields: Sequence[str],
    amount_fields: Sequence[str] = ("amount", "total", "net_amount"),
    status_fields: Sequence[str] = ("status", "order_status"),
    sample_limit: int = 20,
) -> CompareDiffReport:
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

    missing_in_api = sorted(key for key in keys if key in ui_index and key not in api_index)
    missing_in_ui = sorted(key for key in keys if key in api_index and key not in ui_index)
    shared = sorted(key for key in keys if key in ui_index and key in api_index)

    amount_mismatches = 0
    status_mismatches = 0
    mismatch_samples: list[str] = []
    mismatched_shared = 0
    value_mismatch_rows: list[CompareDiffRow] = []

    for key in shared:
        ui_row = ui_index[key]
        api_row = api_index[key]
        amount_mismatch = False
        status_mismatch = False

        for field in amount_fields:
            ui_val = ui_row.get(field)
            api_val = api_row.get(field)
            if ui_val is not None or api_val is not None:
                if str(ui_val) != str(api_val):
                    amount_mismatch = True
                break
        for field in status_fields:
            ui_val = ui_row.get(field)
            api_val = api_row.get(field)
            if ui_val is not None or api_val is not None:
                if str(ui_val).strip().lower() != str(api_val).strip().lower():
                    status_mismatch = True
                break

        reason_code = ""
        if amount_mismatch:
            amount_mismatches += 1
            reason_code = "amount_mismatch"
        if status_mismatch:
            status_mismatches += 1
            reason_code = "status_mismatch" if not reason_code else "amount_and_status_mismatch"
        if amount_mismatch or status_mismatch:
            mismatched_shared += 1
            value_mismatch_rows.append(
                CompareDiffRow(
                    key=key,
                    key_fields=dict(key_breakdown.get(key, {})),
                    reason_code=reason_code,
                    ui_row=dict(ui_row),
                    api_row=dict(api_row),
                )
            )
        if (amount_mismatch or status_mismatch) and len(mismatch_samples) < sample_limit:
            mismatch_samples.append(f"shared_mismatch ({_format_key_parts(key_breakdown.get(key, {}))})")

    matched_rows = len(shared) - mismatched_shared
    missing_api_samples = [
        f"missing_in_api ({_format_key_parts(key_breakdown.get(key, {}))})" for key in missing_in_api
    ]
    missing_ui_samples = [
        f"missing_in_ui ({_format_key_parts(key_breakdown.get(key, {}))})" for key in missing_in_ui
    ]
    sample_keys = (missing_api_samples + missing_ui_samples + mismatch_samples)[:sample_limit]
    metrics = CompareMetrics(
        total_rows=len(keys),
        matched_rows=max(matched_rows, 0),
        missing_in_api=len(missing_in_api),
        missing_in_ui=len(missing_in_ui),
        amount_mismatches=amount_mismatches,
        status_mismatches=status_mismatches,
        sample_mismatch_keys=sample_keys,
    )
    return CompareDiffReport(
        metrics=metrics,
        missing_in_api_rows=[
            CompareDiffRow(
                key=key,
                key_fields=dict(key_breakdown.get(key, {})),
                reason_code="key_missing",
                ui_row=dict(ui_index[key]),
                api_row=None,
            )
            for key in missing_in_api
        ],
        missing_in_ui_rows=[
            CompareDiffRow(
                key=key,
                key_fields=dict(key_breakdown.get(key, {})),
                reason_code="key_missing",
                ui_row=None,
                api_row=dict(api_index[key]),
            )
            for key in missing_in_ui
        ],
        value_mismatch_rows=value_mismatch_rows,
    )
