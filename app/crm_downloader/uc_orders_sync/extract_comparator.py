from __future__ import annotations

from dataclasses import dataclass
from collections import defaultdict
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


@dataclass(frozen=True)
class MigrationThresholds:
    gst_key_parity_min_pct: float = 95.0
    payment_coverage_min_pct: float = 90.0
    payment_field_mismatch_max_pct: float = 10.0


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


def _safe_pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 100.0
    return round((numerator / denominator) * 100.0, 2)


def _normalize_token(value: Any) -> str:
    return str(value or "").strip()


def _build_payment_multimap(rows: Iterable[Mapping[str, Any]]) -> dict[str, list[Mapping[str, Any]]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        order_code = _normalize_token(row.get("order_code"))
        if not order_code:
            continue
        grouped[order_code].append(row)
    return dict(grouped)


def _payment_row_sort_key(row: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        _normalize_token(row.get("payment_date")),
        _normalize_token(row.get("payment_mode")),
        _normalize_token(row.get("amount")),
        _normalize_token(row.get("transaction_id")),
    )


def _compute_payment_field_mismatch_counts(
    *,
    left_rows: dict[str, list[Mapping[str, Any]]],
    right_rows: dict[str, list[Mapping[str, Any]]],
    common_codes: set[str],
    fields: tuple[str, ...],
) -> tuple[dict[str, int], int]:
    mismatch_counts: dict[str, int] = {field: 0 for field in fields}
    comparison_count = 0
    for code in common_codes:
        left = sorted(left_rows.get(code) or [], key=_payment_row_sort_key)
        right = sorted(right_rows.get(code) or [], key=_payment_row_sort_key)
        compare_len = min(len(left), len(right))
        for idx in range(compare_len):
            comparison_count += len(fields)
            for field in fields:
                if _normalize_token(left[idx].get(field)) != _normalize_token(right[idx].get(field)):
                    mismatch_counts[field] += 1
    return mismatch_counts, comparison_count


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
    thresholds: MigrationThresholds | None = None,
) -> tuple[ExtractComparisonSummary, dict[str, Any]]:
    active_thresholds = thresholds or MigrationThresholds()
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

    legacy_payments = _build_payment_multimap(legacy_payment_rows)
    candidate_payments = _build_payment_multimap(candidate_payment_rows)
    common_payment_codes = set(legacy_payments.keys()) & set(candidate_payments.keys())
    payment_field_mismatch_counts, total_payment_field_comparisons = _compute_payment_field_mismatch_counts(
        left_rows=legacy_payments,
        right_rows=candidate_payments,
        common_codes=common_payment_codes,
        fields=payment_field_checks,
    )
    total_payment_field_mismatches = sum(payment_field_mismatch_counts.values())

    gst_key_parity_pct = _safe_pct(
        len(common_gst_codes), max(len(legacy_gst_codes), len(candidate_gst_codes))
    )
    compared_payment_rows = total_payment_field_comparisons // len(payment_field_checks) if payment_field_checks else 0
    payment_coverage_pct = _safe_pct(compared_payment_rows, len(legacy_payment_rows))
    payment_field_mismatch_pct = _safe_pct(
        total_payment_field_mismatches, total_payment_field_comparisons
    )

    gst_key_parity_pass = gst_key_parity_pct >= active_thresholds.gst_key_parity_min_pct
    payment_coverage_pass = (
        payment_coverage_pct >= active_thresholds.payment_coverage_min_pct
    )
    payment_mismatch_pass = (
        payment_field_mismatch_pct
        <= active_thresholds.payment_field_mismatch_max_pct
    )

    migration_ready = (
        gst_key_parity_pass and payment_coverage_pass and payment_mismatch_pass
    )
    migration_reason_codes: list[str] = []
    if not gst_key_parity_pass:
        migration_reason_codes.append("below_gst_key_parity_threshold")
    if not payment_coverage_pass:
        migration_reason_codes.append("below_payment_coverage_threshold")
    if not payment_mismatch_pass:
        migration_reason_codes.append("above_payment_field_mismatch_threshold")

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
        "payment_field_mismatch_counts": payment_field_mismatch_counts,
        "sample_missing_in_candidate_gst": missing_in_candidate_gst[:20],
        "sample_missing_in_legacy_gst": missing_in_legacy_gst[:20],
        "sample_missing_in_candidate_base": missing_in_candidate[:20],
        "sample_missing_in_legacy_base": missing_in_legacy[:20],
        "payment_coverage": {
            "legacy_order_codes": len(legacy_payments),
            "candidate_order_codes": len(candidate_payments),
            "common_order_codes": len(common_payment_codes),
            "legacy_payment_rows": len(legacy_payment_rows),
            "candidate_payment_rows": len(candidate_payment_rows),
            "common_payment_rows_compared": compared_payment_rows,
            "missing_in_candidate": sorted(set(legacy_payments) - set(candidate_payments))[:20],
            "missing_in_legacy": sorted(set(candidate_payments) - set(legacy_payments))[:20],
        },
        "threshold_evaluation": {
            "thresholds": {
                "gst_key_parity_min_pct": active_thresholds.gst_key_parity_min_pct,
                "payment_coverage_min_pct": active_thresholds.payment_coverage_min_pct,
                "payment_field_mismatch_max_pct": active_thresholds.payment_field_mismatch_max_pct,
            },
            "metrics": {
                "gst_key_parity_pct": gst_key_parity_pct,
                "payment_coverage_pct": payment_coverage_pct,
                "payment_field_mismatch_pct": payment_field_mismatch_pct,
            },
            "checks": {
                "gst_key_parity_pass": gst_key_parity_pass,
                "payment_coverage_pass": payment_coverage_pass,
                "payment_field_mismatch_pass": payment_mismatch_pass,
            },
            "migration_ready": migration_ready,
            "reason_codes": migration_reason_codes,
        },
        "migration_ready": migration_ready,
        "migration_reason_codes": migration_reason_codes,
    }
    return summary, details
