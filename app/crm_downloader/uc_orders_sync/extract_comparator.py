from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping


@dataclass
class ExtractComparisonSummary:
    legacy_gst_rows: int
    candidate_gst_rows: int
    legacy_base_rows: int
    candidate_base_rows: int
    legacy_order_detail_rows: int
    candidate_order_detail_rows: int
    legacy_payment_rows: int
    candidate_payment_rows: int
    missing_in_candidate_gst: int
    missing_in_legacy_gst: int
    common_gst: int
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


def _compute_field_mismatch_counts(
    *,
    left_rows: dict[str, Mapping[str, Any]],
    right_rows: dict[str, Mapping[str, Any]],
    common_codes: set[str],
    fields: tuple[str, ...],
) -> dict[str, int]:
    mismatch_counts: dict[str, int] = {field: 0 for field in fields}
    for code in common_codes:
        left = left_rows.get(code) or {}
        right = right_rows.get(code) or {}
        for field in fields:
            if str(left.get(field) or "").strip() != str(right.get(field) or "").strip():
                mismatch_counts[field] += 1
    return mismatch_counts


def compare_extracts(
    *,
    legacy_gst_rows: list[Mapping[str, Any]],
    candidate_gst_rows: list[Mapping[str, Any]],
    legacy_base_rows: list[Mapping[str, Any]],
    legacy_order_detail_rows: list[Mapping[str, Any]],
    legacy_payment_rows: list[Mapping[str, Any]],
    candidate_base_rows: list[Mapping[str, Any]],
    candidate_order_detail_rows: list[Mapping[str, Any]],
    candidate_payment_rows: list[Mapping[str, Any]],
) -> tuple[ExtractComparisonSummary, dict[str, Any]]:
    legacy_gst = _build_keyed_map(legacy_gst_rows, key_field="order_number")
    candidate_gst = _build_keyed_map(candidate_gst_rows, key_field="order_number")
    legacy_gst_codes = set(legacy_gst.keys())
    candidate_gst_codes = set(candidate_gst.keys())
    common_gst_codes = legacy_gst_codes & candidate_gst_codes

    legacy_base = _build_keyed_map(legacy_base_rows, key_field="order_code")
    candidate_base = _build_keyed_map(candidate_base_rows, key_field="order_code")
    legacy_codes = set(legacy_base.keys())
    candidate_codes = set(candidate_base.keys())
    common_codes = legacy_codes & candidate_codes

    missing_in_candidate_gst = sorted(legacy_gst_codes - candidate_gst_codes)
    missing_in_legacy_gst = sorted(candidate_gst_codes - legacy_gst_codes)
    missing_in_candidate = sorted(legacy_codes - candidate_codes)
    missing_in_legacy = sorted(candidate_codes - legacy_codes)

    gst_field_checks = (
        "invoice_number",
        "invoice_date",
        "name",
        "customer_phone",
        "address",
        "final_amount",
        "payment_status",
    )
    base_field_checks = ("customer_name", "customer_phone", "address", "payment_text", "status")
    payment_field_checks = ("payment_mode", "amount", "payment_date")

    legacy_payments = _build_keyed_map(legacy_payment_rows, key_field="order_code")
    candidate_payments = _build_keyed_map(candidate_payment_rows, key_field="order_code")
    common_payment_codes = set(legacy_payments.keys()) & set(candidate_payments.keys())

    summary = ExtractComparisonSummary(
        legacy_gst_rows=len(legacy_gst_rows),
        candidate_gst_rows=len(candidate_gst_rows),
        legacy_base_rows=len(legacy_base_rows),
        candidate_base_rows=len(candidate_base_rows),
        legacy_order_detail_rows=len(legacy_order_detail_rows),
        candidate_order_detail_rows=len(candidate_order_detail_rows),
        legacy_payment_rows=len(legacy_payment_rows),
        candidate_payment_rows=len(candidate_payment_rows),
        missing_in_candidate_gst=len(missing_in_candidate_gst),
        missing_in_legacy_gst=len(missing_in_legacy_gst),
        common_gst=len(common_gst_codes),
        missing_in_candidate_base=len(missing_in_candidate),
        missing_in_legacy_base=len(missing_in_legacy),
        common_base=len(common_codes),
    )
    details = {
        "gst_field_mismatch_counts": _compute_field_mismatch_counts(
            left_rows=legacy_gst,
            right_rows=candidate_gst,
            common_codes=common_gst_codes,
            fields=gst_field_checks,
        ),
        "base_field_mismatch_counts": _compute_field_mismatch_counts(
            left_rows=legacy_base,
            right_rows=candidate_base,
            common_codes=common_codes,
            fields=base_field_checks,
        ),
        "payment_field_mismatch_counts": _compute_field_mismatch_counts(
            left_rows=legacy_payments,
            right_rows=candidate_payments,
            common_codes=common_payment_codes,
            fields=payment_field_checks,
        ),
        "sample_missing_in_candidate_gst": missing_in_candidate_gst[:20],
        "sample_missing_in_legacy_gst": missing_in_legacy_gst[:20],
        "sample_missing_in_candidate_base": missing_in_candidate[:20],
        "sample_missing_in_legacy_base": missing_in_legacy[:20],
        "payment_coverage": {
            "legacy_order_codes": len(legacy_payments),
            "candidate_order_codes": len(candidate_payments),
            "common_order_codes": len(common_payment_codes),
            "missing_in_candidate": sorted(set(legacy_payments) - set(candidate_payments))[:20],
            "missing_in_legacy": sorted(set(candidate_payments) - set(legacy_payments))[:20],
        },
    }
    return summary, details
