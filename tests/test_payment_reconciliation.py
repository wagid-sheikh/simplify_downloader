from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from app.reports.shared.payment_reconciliation import (
    build_payment_evidence_audit_rows,
    normalize_order_number,
    reconcile_payments,
    split_payment_order_numbers,
)


def _order(
    order_number: str,
    amount: str,
    order_date: str = "2026-05-01T10:00:00",
    cost_center: str = "CC1",
    recovery_status: str = "",
    recovery_category: str = "",
) -> dict[str, object]:
    return {
        "cost_center": cost_center,
        "order_number": order_number,
        "order_date": datetime.fromisoformat(order_date),
        "order_amount": Decimal(amount),
        "recovery_status": recovery_status,
        "recovery_category": recovery_category,
    }


def _sale(
    order_number: str, amount: str, cost_center: str = "CC1"
) -> dict[str, object]:
    return {
        "cost_center": cost_center,
        "order_number": order_number,
        "payment_received": Decimal(amount),
    }


def _proof(
    order_number: str,
    amount: str,
    source_type: str = "google_sheet",
    cost_center: str = "CC1",
    bank_row_id: str = "ignored",
) -> dict[str, object]:
    return {
        "cost_center": cost_center,
        "order_number": order_number,
        "amount": Decimal(amount),
        "source_type": source_type,
        "bank_row_id": bank_row_id,
    }


def test_normalizes_and_splits_payment_collection_order_numbers() -> None:
    assert normalize_order_number(" uc567-1688 ") == "UC567-1688"
    assert split_payment_order_numbers("UC567-1688, UC567-1702 / uc567-1703") == (
        "UC567-1688",
        "UC567-1702",
        "UC567-1703",
    )


def test_single_order_group_is_paid_with_one_rupee_tolerance_and_ignores_bank_row_id() -> (
    None
):
    result = reconcile_payments(
        order_rows=[_order(" TD 123 ", "100")],
        sales_rows=[_sale("TD123", "100")],
        payment_evidence_rows=[_proof("TD123", "99", bank_row_id="BANK-1")],
    )

    assert len(result.groups) == 1
    group = result.groups[0]
    assert group.group_type == "single_order"
    assert group.status == "paid"
    assert group.evidence_amount == Decimal("99")
    assert group.short_amount == Decimal("0")
    assert result.orders[0].status == "paid"
    assert result.recovery_auto_clear_orders == ()


def test_to_be_recovered_full_single_order_proof_is_recovery_auto_clearable_without_paid_status() -> (
    None
):
    result = reconcile_payments(
        order_rows=[_order("REC-1", "100", recovery_status="TO_BE_RECOVERED")],
        sales_rows=[],
        payment_evidence_rows=[_proof("REC-1", "100")],
    )

    order = result.orders[0]
    assert order.status == "recovery_excluded"
    assert order.has_recovery_auto_clear_proof is True
    assert [
        candidate.order_number for candidate in result.recovery_auto_clear_orders
    ] == ["REC-1"]


def test_to_be_recovered_grouped_comma_slash_proof_clears_when_allocation_covers_order() -> (
    None
):
    result = reconcile_payments(
        order_rows=[
            _order(
                "REC-1", "100", "2026-05-01T10:00:00", recovery_status="TO_BE_RECOVERED"
            ),
            _order(
                "REC-2", "150", "2026-05-01T11:00:00", recovery_status="TO_BE_RECOVERED"
            ),
            _order(
                "REC-3", "200", "2026-05-01T12:00:00", recovery_status="TO_BE_RECOVERED"
            ),
        ],
        sales_rows=[],
        payment_evidence_rows=[_proof("REC-1, REC-2 / REC-3", "450")],
    )

    assert result.groups[0].status == "paid"
    assert {order.status for order in result.orders} == {"recovery_excluded"}
    assert [order.order_number for order in result.recovery_auto_clear_orders] == [
        "REC-1",
        "REC-2",
        "REC-3",
    ]


def test_to_be_recovered_insufficient_or_unsupported_proof_is_not_auto_clearable() -> (
    None
):
    insufficient = reconcile_payments(
        order_rows=[_order("REC-SHORT", "100", recovery_status="TO_BE_RECOVERED")],
        sales_rows=[],
        payment_evidence_rows=[_proof("REC-SHORT", "98")],
    )
    unsupported = reconcile_payments(
        order_rows=[_order("REC-BANK", "100", recovery_status="TO_BE_RECOVERED")],
        sales_rows=[],
        payment_evidence_rows=[_proof("REC-BANK", "100", source_type="bank_upload")],
    )

    assert insufficient.orders[0].has_recovery_auto_clear_proof is False
    assert insufficient.recovery_auto_clear_orders == ()
    assert len(unsupported.invalid_evidence_rows) == 1
    assert unsupported.orders[0].has_recovery_auto_clear_proof is False
    assert unsupported.recovery_auto_clear_orders == ()


def test_terminal_recovery_statuses_are_not_auto_cleared_again() -> None:
    result = reconcile_payments(
        order_rows=[
            _order("WRITTEN", "100", recovery_status="WRITE_OFF"),
            _order("DONE", "100", recovery_status="RECOVERED"),
        ],
        sales_rows=[],
        payment_evidence_rows=[_proof("WRITTEN/DONE", "200")],
    )

    assert [order.status for order in result.orders] == [
        "recovery_excluded",
        "recovery_excluded",
    ]
    assert [order.has_recovery_auto_clear_proof for order in result.orders] == [
        False,
        False,
    ]
    assert result.recovery_auto_clear_orders == ()


def test_grouped_payment_sums_each_payment_row_once_for_comma_order_example() -> None:
    result = reconcile_payments(
        order_rows=[
            _order("UC567-1688", "100", "2026-05-01T10:00:00"),
            _order("UC567-1702", "200", "2026-05-01T11:00:00"),
        ],
        sales_rows=[_sale("UC567-1688", "100"), _sale("UC567-1702", "200")],
        payment_evidence_rows=[
            _proof("UC567-1688,UC567-1702", "300", source_type="legacy_sales")
        ],
    )

    assert len(result.groups) == 1
    group = result.groups[0]
    assert group.normalized_order_numbers == ("UC567-1688", "UC567-1702")
    assert group.group_type == "multi_order"
    assert group.status == "paid"
    assert group.expected_order_amount == Decimal("300")
    assert group.evidence_amount == Decimal("300")
    assert [order.allocated_payment_amount for order in group.orders] == [
        Decimal("100"),
        Decimal("200"),
    ]


def test_grouped_payment_splits_on_slash_and_allocates_short_groups_by_order_date_then_number() -> (
    None
):
    result = reconcile_payments(
        order_rows=[
            _order("B-2", "100", "2026-05-02T10:00:00"),
            _order("A-1", "100", "2026-05-01T10:00:00"),
            _order("C-3", "100", "2026-05-03T10:00:00"),
        ],
        sales_rows=[_sale("A-1", "100"), _sale("B-2", "100"), _sale("C-3", "100")],
        payment_evidence_rows=[_proof("B-2/A-1/C-3", "250")],
    )

    group = result.groups[0]
    assert group.status == "short"
    assert group.short_amount == Decimal("50")
    assert [
        (
            order.order_number,
            order.allocated_payment_amount,
            order.status,
            order.short_amount,
        )
        for order in group.orders
    ] == [
        ("A-1", Decimal("100"), "paid", Decimal("0")),
        ("B-2", Decimal("100"), "paid", Decimal("0")),
        ("C-3", Decimal("50"), "short", Decimal("50")),
    ]
    assert [order.order_number for order in result.short_payment_orders] == []
    assert group.sales_evidence_mismatch is True


def test_exact_order_token_matching_distinguishes_ord1_from_ord10() -> None:
    result = reconcile_payments(
        order_rows=[
            _order("ORD1", "100", "2026-05-01T10:00:00"),
            _order("ORD10", "100", "2026-05-01T11:00:00"),
        ],
        sales_rows=[_sale("ORD1", "80"), _sale("ORD10", "100")],
        payment_evidence_rows=[_proof("ORD10", "100")],
    )

    groups_by_token = {group.normalized_order_numbers: group for group in result.groups}
    assert groups_by_token[("ORD10",)].status == "paid"
    assert groups_by_token[("ORD1",)].status == "proof_missing"
    assert [order.order_number for order in result.actual_payments_not_found] == [
        "ORD1"
    ]
    assert result.short_payment_orders == ()


def test_slash_and_comma_grouped_tokens_match_exact_orders_without_ord10_false_positive() -> (
    None
):
    result = reconcile_payments(
        order_rows=[
            _order("ORD1", "100", "2026-05-01T10:00:00"),
            _order("ORD2", "100", "2026-05-01T11:00:00"),
            _order("ORD3", "100", "2026-05-01T12:00:00"),
            _order("ORD4", "100", "2026-05-01T13:00:00"),
            _order("ORD10", "100", "2026-05-01T14:00:00"),
        ],
        sales_rows=[
            _sale("ORD1", "100"),
            _sale("ORD2", "100"),
            _sale("ORD3", "100"),
            _sale("ORD4", "100"),
            _sale("ORD10", "100"),
        ],
        payment_evidence_rows=[
            _proof("ORD1/ORD2", "200"),
            _proof("ORD3, ORD4", "200"),
        ],
    )

    assert [group.normalized_order_numbers for group in result.groups] == [
        ("ORD1", "ORD2"),
        ("ORD3", "ORD4"),
        ("ORD10",),
    ]
    assert [group.status for group in result.groups] == [
        "paid",
        "paid",
        "proof_missing",
    ]
    assert [order.order_number for order in result.actual_payments_not_found] == [
        "ORD10"
    ]


def test_proof_missing_report_includes_sales_payments_without_valid_collection_proof() -> (
    None
):
    result = reconcile_payments(
        order_rows=[_order("TD-1", "150")],
        sales_rows=[_sale("TD-1", "150")],
        payment_evidence_rows=[_proof("TD-1", "150", source_type="bank_upload")],
    )

    assert len(result.invalid_evidence_rows) == 1
    assert result.groups[0].status == "proof_missing"
    assert result.actual_payments_not_found[0].order_number == "TD-1"


def test_zero_value_sales_row_without_proof_is_not_apnf_or_short_payment() -> None:
    result = reconcile_payments(
        order_rows=[_order("ZERO-SALE-NO-PROOF", "0")],
        sales_rows=[_sale("ZERO-SALE-NO-PROOF", "0")],
        payment_evidence_rows=[],
    )

    assert result.groups[0].status == "proof_missing"
    assert result.actual_payments_not_found == ()
    assert result.short_payment_orders == ()
    assert result.recovery_auto_clear_orders == ()


def test_overlapping_group_and_single_top_up_reconcile_consistently() -> None:
    result = reconcile_payments(
        order_rows=[
            _order("ORD1", "100", "2026-05-01T10:00:00"),
            _order("ORD2", "100", "2026-05-01T11:00:00"),
        ],
        sales_rows=[_sale("ORD1", "100"), _sale("ORD2", "100")],
        payment_evidence_rows=[
            _proof("ORD1,ORD2", "150"),
            _proof("ORD2", "50"),
        ],
    )

    assert len(result.groups) == 1
    group = result.groups[0]
    assert group.normalized_order_numbers == ("ORD1", "ORD2")
    assert group.status == "paid"
    assert group.evidence_amount == Decimal("200")
    assert group.expected_order_amount == Decimal("200")
    assert result.actual_payments_not_found == ()
    assert result.short_payment_orders == ()
    assert result.recovery_auto_clear_orders == ()

    audit_rows = build_payment_evidence_audit_rows(
        order_rows=[
            _order("ORD1", "100", "2026-05-01T10:00:00"),
            _order("ORD2", "100", "2026-05-01T11:00:00"),
        ],
        sales_rows=[_sale("ORD1", "100"), _sale("ORD2", "100")],
        payment_evidence_rows=[
            _proof("ORD1,ORD2", "150"),
            _proof("ORD2", "50"),
        ],
    )
    assert [row.reconciliation_result for row in audit_rows] == [
        "grouped paid",
        "grouped paid",
    ]
    assert {row.group_key for row in audit_rows} == {"ORD1|ORD2"}
    assert {row.grouped_amount for row in audit_rows} == {Decimal("200")}


def test_grouped_payment_and_single_order_top_up_are_paid_against_total_amount() -> (
    None
):
    result = reconcile_payments(
        order_rows=[
            _order("ORD1", "100", "2026-05-01T10:00:00"),
            _order("ORD2", "200", "2026-05-01T11:00:00"),
        ],
        sales_rows=[_sale("ORD1", "100"), _sale("ORD2", "200")],
        payment_evidence_rows=[
            _proof("ORD1,ORD2", "250"),
            _proof("ORD2", "50"),
        ],
    )

    assert len(result.groups) == 1
    group = result.groups[0]
    assert group.normalized_order_numbers == ("ORD1", "ORD2")
    assert group.expected_order_amount == Decimal("300")
    assert group.evidence_amount == Decimal("300")
    assert [row.amount for row in group.evidence_rows] == [
        Decimal("250"),
        Decimal("50"),
    ]
    assert group.status == "paid"
    assert result.short_payment_orders == ()


def test_grouped_payment_and_single_order_top_up_report_only_sequential_shortage() -> (
    None
):
    result = reconcile_payments(
        order_rows=[
            _order("ORD1", "100", "2026-05-01T10:00:00"),
            _order("ORD2", "200", "2026-05-01T11:00:00"),
        ],
        sales_rows=[_sale("ORD1", "100"), _sale("ORD2", "200")],
        payment_evidence_rows=[
            _proof("ORD1,ORD2", "230"),
            _proof("ORD2", "50"),
        ],
    )

    assert len(result.groups) == 1
    group = result.groups[0]
    assert group.expected_order_amount == Decimal("300")
    assert group.evidence_amount == Decimal("280")
    assert group.short_amount == Decimal("20")
    assert [
        (
            order.order_number,
            order.allocated_payment_amount,
            order.short_amount,
            order.status,
        )
        for order in group.orders
    ] == [
        ("ORD1", Decimal("100"), Decimal("0"), "paid"),
        ("ORD2", Decimal("180"), Decimal("20"), "short"),
    ]
    assert [
        (order.order_number, order.short_amount)
        for order in result.short_payment_orders
    ] == []
    assert group.sales_evidence_mismatch is True


def test_sales_equals_evidence_has_no_sales_evidence_mismatch() -> None:
    result = reconcile_payments(
        order_rows=[_order("ORD-MATCH", "100")],
        sales_rows=[_sale("ORD-MATCH", "100")],
        payment_evidence_rows=[_proof("ORD-MATCH", "100")],
    )

    group = result.groups[0]
    order = result.orders[0]
    assert group.status == "paid"
    assert group.sales_payment_received == Decimal("100")
    assert group.evidence_amount == Decimal("100")
    assert group.sales_evidence_difference == Decimal("0")
    assert group.sales_evidence_mismatch is False
    assert order.sales_evidence_mismatch is False


def test_sales_greater_than_evidence_sets_mismatch_without_missing_proof() -> None:
    result = reconcile_payments(
        order_rows=[_order("ORD-SALES-HIGH", "100")],
        sales_rows=[_sale("ORD-SALES-HIGH", "120")],
        payment_evidence_rows=[_proof("ORD-SALES-HIGH", "100")],
    )

    group = result.groups[0]
    assert group.status == "paid"
    assert group.sales_payment_received == Decimal("120")
    assert group.evidence_amount == Decimal("100")
    assert group.sales_evidence_difference == Decimal("20")
    assert group.sales_evidence_mismatch is True
    assert result.actual_payments_not_found == ()
    assert result.short_payment_orders == ()


def test_evidence_greater_than_sales_sets_mismatch_without_short_payment() -> None:
    result = reconcile_payments(
        order_rows=[_order("ORD-EVID-HIGH", "100")],
        sales_rows=[_sale("ORD-EVID-HIGH", "80")],
        payment_evidence_rows=[_proof("ORD-EVID-HIGH", "100")],
    )

    group = result.groups[0]
    assert group.status == "paid"
    assert group.sales_payment_received == Decimal("80")
    assert group.evidence_amount == Decimal("100")
    assert group.sales_evidence_difference == Decimal("-20")
    assert group.sales_evidence_mismatch is True
    assert result.actual_payments_not_found == ()
    assert result.short_payment_orders == ()


def test_both_sales_and_evidence_short_against_order_amount_remains_short_only() -> (
    None
):
    result = reconcile_payments(
        order_rows=[_order("ORD-BOTH-SHORT", "100")],
        sales_rows=[_sale("ORD-BOTH-SHORT", "50")],
        payment_evidence_rows=[_proof("ORD-BOTH-SHORT", "50")],
    )

    group = result.groups[0]
    order = result.orders[0]
    assert group.status == "short"
    assert group.sales_payment_received == Decimal("50")
    assert group.evidence_amount == Decimal("50")
    assert group.sales_evidence_difference == Decimal("0")
    assert group.sales_evidence_mismatch is False
    assert order.short_amount == Decimal("50")
    assert result.actual_payments_not_found == ()
    assert result.short_payment_orders == (order,)


def test_transitive_grouped_payment_edges_reconcile_as_one_component() -> None:
    result = reconcile_payments(
        order_rows=[
            _order("ORD1", "100", "2026-05-01T10:00:00"),
            _order("ORD2", "100", "2026-05-01T11:00:00"),
            _order("ORD3", "100", "2026-05-01T12:00:00"),
        ],
        sales_rows=[_sale("ORD1", "100"), _sale("ORD2", "100"), _sale("ORD3", "100")],
        payment_evidence_rows=[
            _proof("ORD1,ORD2", "200"),
            _proof("ORD2,ORD3", "100"),
        ],
    )

    assert len(result.groups) == 1
    group = result.groups[0]
    assert group.normalized_order_numbers == ("ORD1", "ORD2", "ORD3")
    assert group.status == "paid"
    assert group.expected_order_amount == Decimal("300")
    assert group.evidence_amount == Decimal("300")
    assert result.short_payment_orders == ()
    assert result.actual_payments_not_found == ()


def test_exact_token_grouping_does_not_create_conflicting_audit_outcomes() -> None:
    audit_rows = build_payment_evidence_audit_rows(
        order_rows=[
            _order("ORD1", "100", "2026-05-01T10:00:00"),
            _order("ORD2", "100", "2026-05-01T11:00:00"),
            _order("ORD3", "100", "2026-05-01T12:00:00"),
        ],
        sales_rows=[_sale("ORD1", "100"), _sale("ORD2", "100"), _sale("ORD3", "100")],
        payment_evidence_rows=[
            _proof("ORD1,ORD2", "200"),
            _proof("ORD2,ORD3", "100"),
        ],
    )

    assert [row.reconciliation_result for row in audit_rows] == [
        "grouped paid",
        "grouped paid",
    ]
    assert {row.group_key for row in audit_rows} == {"ORD1|ORD2|ORD3"}
    assert {row.grouped_amount for row in audit_rows} == {Decimal("300")}
    assert {row.grouped_order_amount for row in audit_rows} == {Decimal("300")}


def test_write_off_short_payment_evidence_is_recovery_excluded_for_audit() -> None:
    result = reconcile_payments(
        order_rows=[
            _order(
                "UN3668",
                "1570",
                recovery_status="WRITE_OFF",
                recovery_category="write off",
            )
        ],
        sales_rows=[_sale("UN3668", "1178")],
        payment_evidence_rows=[_proof("UN3668", "1178")],
    )

    group = result.groups[0]
    order = result.orders[0]
    assert group.status == "recovery_excluded"
    assert group.short_amount == Decimal("0")
    assert order.status == "recovery_excluded"
    assert order.short_amount == Decimal("0")
    assert result.short_payment_orders == ()

    audit_rows = build_payment_evidence_audit_rows(
        order_rows=[
            _order(
                "UN3668",
                "1570",
                recovery_status="WRITE_OFF",
                recovery_category="write off",
            )
        ],
        sales_rows=[_sale("UN3668", "1178")],
        payment_evidence_rows=[_proof("UN3668", "1178")],
    )

    assert len(audit_rows) == 1
    audit_row = audit_rows[0]
    assert audit_row.reconciliation_result == "recovery_excluded"
    assert audit_row.reconciliation_result != "short"
    assert (
        audit_row.operator_actionable_payment_status == "non_actionable_recovery_status"
    )
    assert audit_row.operator_actionable_payment_status != "actionable_short_payment"
    assert audit_row.recovery_statuses_csv == "WRITE_OFF"
    assert audit_row.recovery_categories_csv == "write off"


def test_grouped_short_with_mixed_recovery_status_is_not_grouped_short() -> None:
    audit_rows = build_payment_evidence_audit_rows(
        order_rows=[
            _order("ORD-ACTION", "100", "2026-05-01T10:00:00"),
            _order(
                "ORD-WRITEOFF",
                "100",
                "2026-05-01T11:00:00",
                recovery_status="WRITE_OFF",
            ),
        ],
        sales_rows=[_sale("ORD-ACTION", "100"), _sale("ORD-WRITEOFF", "50")],
        payment_evidence_rows=[_proof("ORD-ACTION,ORD-WRITEOFF", "100")],
    )

    assert len(audit_rows) == 1
    assert audit_rows[0].reconciliation_result == "mixed_recovery_status"
    assert audit_rows[0].reconciliation_result != "grouped short"
    assert (
        audit_rows[0].operator_actionable_payment_status
        == "non_actionable_recovery_status"
    )
    assert (
        audit_rows[0].operator_actionable_payment_status != "actionable_short_payment"
    )
    assert audit_rows[0].recovery_statuses_csv == "WRITE_OFF"


def test_overpayment_counts_as_paid_and_short_proof_remains_short_payment() -> None:
    overpaid = reconcile_payments(
        order_rows=[_order("OVERPAID", "100")],
        sales_rows=[_sale("OVERPAID", "125")],
        payment_evidence_rows=[_proof("OVERPAID", "125")],
    )
    short = reconcile_payments(
        order_rows=[_order("SHORTPROOF", "100")],
        sales_rows=[_sale("SHORTPROOF", "50")],
        payment_evidence_rows=[_proof("SHORTPROOF", "50")],
    )

    assert overpaid.groups[0].status == "paid"
    assert overpaid.orders[0].status == "paid"
    assert overpaid.short_payment_orders == ()
    assert short.groups[0].status == "short"
    assert short.short_payment_orders == (short.orders[0],)


def test_mixed_comma_slash_tokens_match_and_unrelated_tokens_do_not_match() -> None:
    result = reconcile_payments(
        order_rows=[
            _order("MIX1", "50", "2026-05-01T10:00:00"),
            _order("MIX2", "30", "2026-05-01T11:00:00"),
            _order("MIX3", "20", "2026-05-01T12:00:00"),
            _order("ORD1", "100", "2026-05-01T13:00:00"),
        ],
        sales_rows=[
            _sale("MIX1", "50"),
            _sale("MIX2", "30"),
            _sale("MIX3", "20"),
            _sale("ORD1", "100"),
        ],
        payment_evidence_rows=[
            _proof("MIX1, MIX2/MIX3", "100"),
            _proof("ORD10", "100"),
        ],
    )

    groups_by_tokens = {
        group.normalized_order_numbers: group for group in result.groups
    }
    assert groups_by_tokens[("MIX1", "MIX2", "MIX3")].status == "paid"
    assert groups_by_tokens[("ORD1",)].status == "proof_missing"
    assert ("ORD10",) not in groups_by_tokens
    assert [order.order_number for order in result.actual_payments_not_found] == [
        "ORD1"
    ]


def test_to_be_recovered_grouped_proof_ignores_unmatched_token_when_matched_orders_are_covered() -> (
    None
):
    result = reconcile_payments(
        order_rows=[
            _order(
                "T766", "100", "2026-05-01T10:00:00", recovery_status="TO_BE_RECOVERED"
            ),
            _order(
                "T767", "150", "2026-05-01T11:00:00", recovery_status="TO_BE_RECOVERED"
            ),
        ],
        sales_rows=[],
        payment_evidence_rows=[_proof("T766, T767, BADTOKEN", "250")],
    )

    group = result.groups[0]
    assert group.status == "data_quality_exception"
    assert group.unmatched_order_numbers == ("BADTOKEN",)
    assert group.data_quality_exception is True
    assert [
        (
            order.order_number,
            order.allocated_payment_amount,
            order.has_recovery_auto_clear_proof,
        )
        for order in result.orders
    ] == [("T766", Decimal("100"), True), ("T767", Decimal("150"), True)]
    assert [order.order_number for order in result.recovery_auto_clear_orders] == [
        "T766",
        "T767",
    ]

    audit_rows = build_payment_evidence_audit_rows(
        order_rows=[
            _order(
                "T766", "100", "2026-05-01T10:00:00", recovery_status="TO_BE_RECOVERED"
            ),
            _order(
                "T767", "150", "2026-05-01T11:00:00", recovery_status="TO_BE_RECOVERED"
            ),
        ],
        sales_rows=[],
        payment_evidence_rows=[_proof("T766, T767, BADTOKEN", "250")],
    )
    assert audit_rows[0].reconciliation_result == "unmatched order token"
    assert audit_rows[0].normalized_order_tokens_csv == "T766,T767,BADTOKEN"


def test_package_sales_proof_suppresses_actual_payments_not_found_without_collection() -> (
    None
):
    result = reconcile_payments(
        order_rows=[_order("PKG-PAID", "100")],
        sales_rows=[
            {
                "cost_center": "CC1",
                "order_number": "PKG-PAID",
                "payment_received": "100",
                "payment_mode": " Package ",
                "id": 10,
            }
        ],
        payment_evidence_rows=[],
    )

    assert result.actual_payments_not_found == ()
    assert result.orders[0].status == "paid"
    assert result.orders[0].package_sales_total == Decimal("100")


def test_insufficient_package_sales_proof_remains_actual_payments_not_found() -> None:
    result = reconcile_payments(
        order_rows=[_order("PKG-SHORT", "100")],
        sales_rows=[
            {
                "cost_center": "CC1",
                "order_number": "PKG-SHORT",
                "payment_received": "98",
                "payment_mode": "package",
            }
        ],
        payment_evidence_rows=[],
    )

    assert [order.order_number for order in result.actual_payments_not_found] == [
        "PKG-SHORT"
    ]


def test_non_package_sales_does_not_suppress_actual_payments_not_found() -> None:
    result = reconcile_payments(
        order_rows=[_order("CASH", "100")],
        sales_rows=[
            {
                "cost_center": "CC1",
                "order_number": "CASH",
                "payment_received": "100",
                "payment_mode": "Cash",
            }
        ],
        payment_evidence_rows=[],
    )

    assert [order.order_number for order in result.actual_payments_not_found] == [
        "CASH"
    ]


def test_package_sales_auto_clears_to_be_recovered_within_tolerance() -> None:
    result = reconcile_payments(
        order_rows=[_order("PKG-REC", "100", recovery_status="TO_BE_RECOVERED")],
        sales_rows=[
            {
                "cost_center": "CC1",
                "order_number": "PKG-REC",
                "payment_received": "99",
                "payment_mode": "package",
                "id": 44,
            }
        ],
        payment_evidence_rows=[],
    )

    assert [order.order_number for order in result.recovery_auto_clear_orders] == [
        "PKG-REC"
    ]
    assert result.orders[0].package_sales_total == Decimal("99")


def test_package_sales_short_by_more_than_tolerance_does_not_auto_clear() -> None:
    result = reconcile_payments(
        order_rows=[_order("PKG-REC-SHORT", "100", recovery_status="TO_BE_RECOVERED")],
        sales_rows=[
            {
                "cost_center": "CC1",
                "order_number": "PKG-REC-SHORT",
                "payment_received": "98.99",
                "payment_mode": "package",
            }
        ],
        payment_evidence_rows=[],
    )

    assert result.recovery_auto_clear_orders == ()
