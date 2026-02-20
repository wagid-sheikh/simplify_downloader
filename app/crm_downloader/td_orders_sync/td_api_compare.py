from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import parse_qs, urlparse


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

    def as_dict(self) -> dict[str, Any]:
        return {
            "endpoint": self.endpoint,
            "method": self.method,
            "query_params": self.query_params,
            "status": self.status,
            "latency_ms": self.latency_ms,
            "retry_count": self.retry_count,
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
    )


def _canonical_key(row: Mapping[str, Any], key_fields: Sequence[str]) -> str:
    return "|".join(str(row.get(field) or "").strip() for field in key_fields)


def compare_canonical_rows(
    *,
    ui_rows: Iterable[Mapping[str, Any]],
    api_rows: Iterable[Mapping[str, Any]],
    key_fields: Sequence[str],
    amount_fields: Sequence[str] = ("amount", "total", "net_amount"),
    status_fields: Sequence[str] = ("status", "order_status"),
    sample_limit: int = 20,
) -> CompareMetrics:
    ui_index = {_canonical_key(row, key_fields): row for row in ui_rows}
    api_index = {_canonical_key(row, key_fields): row for row in api_rows}
    keys = set(ui_index) | set(api_index)

    missing_in_api = sorted(key for key in keys if key and key in ui_index and key not in api_index)
    missing_in_ui = sorted(key for key in keys if key and key in api_index and key not in ui_index)
    shared = sorted(key for key in keys if key in ui_index and key in api_index)

    amount_mismatches = 0
    status_mismatches = 0
    mismatch_samples: list[str] = []

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

        if amount_mismatch:
            amount_mismatches += 1
        if status_mismatch:
            status_mismatches += 1
        if (amount_mismatch or status_mismatch) and len(mismatch_samples) < sample_limit:
            mismatch_samples.append(key)

    matched_rows = len(shared) - len({*mismatch_samples})
    sample_keys = (missing_in_api + missing_in_ui + mismatch_samples)[:sample_limit]
    return CompareMetrics(
        total_rows=len(keys),
        matched_rows=max(matched_rows, 0),
        missing_in_api=len(missing_in_api),
        missing_in_ui=len(missing_in_ui),
        amount_mismatches=amount_mismatches,
        status_mismatches=status_mismatches,
        sample_mismatch_keys=sample_keys,
    )
