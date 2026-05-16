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
    assert result.recovery_auto_clear_orders[0].order_number == "TD 123"


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
    assert [order.order_number for order in result.short_payment_orders] == ["C-3"]


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
    assert [order.order_number for order in result.recovery_auto_clear_orders] == [
        "ORD1",
        "ORD2",
    ]

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
    ] == [
        ("ORD2", Decimal("20")),
    ]


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
