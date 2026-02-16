from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping


@dataclass
class ExtractComparisonSummary:
    legacy_base_rows: int
    candidate_base_rows: int
    legacy_order_detail_rows: int
    candidate_order_detail_rows: int
    legacy_payment_rows: int
    candidate_payment_rows: int
    missing_in_candidate_base: int
    missing_in_legacy_base: int
    common_base: int


def _build_keyed_map(rows: Iterable[Mapping[str, Any]], *, key_field: str) -> dict[str, Mapping[str, Any]]:
    keyed: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        key = str(row.get(key_field) or "").strip()
        if not key:
            continue
        keyed[key] = row
    return keyed


def compare_extracts(
    *,
    legacy_base_rows: list[Mapping[str, Any]],
    legacy_order_detail_rows: list[Mapping[str, Any]],
    legacy_payment_rows: list[Mapping[str, Any]],
    candidate_base_rows: list[Mapping[str, Any]],
    candidate_order_detail_rows: list[Mapping[str, Any]],
    candidate_payment_rows: list[Mapping[str, Any]],
) -> tuple[ExtractComparisonSummary, dict[str, Any]]:
    legacy_base = _build_keyed_map(legacy_base_rows, key_field="order_code")
    candidate_base = _build_keyed_map(candidate_base_rows, key_field="order_code")

    legacy_codes = set(legacy_base.keys())
    candidate_codes = set(candidate_base.keys())
    common_codes = legacy_codes & candidate_codes

    missing_in_candidate = sorted(legacy_codes - candidate_codes)
    missing_in_legacy = sorted(candidate_codes - legacy_codes)

    field_checks = ("customer_name", "customer_phone", "address", "payment_text", "status")
    field_mismatch_counts: dict[str, int] = {field: 0 for field in field_checks}
    for code in common_codes:
        left = legacy_base.get(code) or {}
        right = candidate_base.get(code) or {}
        for field in field_checks:
            if str(left.get(field) or "").strip() != str(right.get(field) or "").strip():
                field_mismatch_counts[field] += 1

    summary = ExtractComparisonSummary(
        legacy_base_rows=len(legacy_base_rows),
        candidate_base_rows=len(candidate_base_rows),
        legacy_order_detail_rows=len(legacy_order_detail_rows),
        candidate_order_detail_rows=len(candidate_order_detail_rows),
        legacy_payment_rows=len(legacy_payment_rows),
        candidate_payment_rows=len(candidate_payment_rows),
        missing_in_candidate_base=len(missing_in_candidate),
        missing_in_legacy_base=len(missing_in_legacy),
        common_base=len(common_codes),
    )
    details = {
        "field_mismatch_counts": field_mismatch_counts,
        "sample_missing_in_candidate": missing_in_candidate[:20],
        "sample_missing_in_legacy": missing_in_legacy[:20],
    }
    return summary, details
