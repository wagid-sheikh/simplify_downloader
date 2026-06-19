"""Canonical payment reconciliation logic for report data loading.

The SQL missing-payment view is maintained as a compatibility/audit read model;
Python reports should use this module so missing proof, short payment, grouped
payment, top-up, sales, and recovery-status rules stay in one place.

``payment_collections.order_number`` can be either one order number or a grouped
list separated by ``/`` or ``,``. Matching is canonicalized by splitting those
delimiters, normalizing each token, and comparing whole tokens exactly; ``ORD1``
therefore never matches ``ORD10`` unless a grouped evidence row explicitly names
``ORD1`` as its own token.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping, Sequence
import re

VALID_PAYMENT_SOURCE_TYPES = frozenset({"google_sheet", "legacy_sales"})
RECOVERY_EXCLUDED_STATUSES = frozenset(
    {
        "TO_BE_RECOVERED",
        "TO_BE_COMPENSATED",
        "RECOVERED",
        "COMPENSATED",
        "WRITE_OFF",
    }
)
ACTIONABLE_SHORT_PAYMENT_STATUS = "actionable_short_payment"
NON_ACTIONABLE_RECOVERY_STATUS = "non_actionable_recovery_status"
AUDIT_ONLY_PAYMENT_STATUS = "audit_only"
DEFAULT_PAYMENT_TOLERANCE = Decimal("1")
_ORDER_TOKEN_SPLIT_RE = re.compile(r"[,/]")
_WHITESPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class ReconciliationOrder:
    """Canonical order row from vw_orders."""

    cost_center: str
    order_number: str
    normalized_order_number: str
    order_date: datetime | date | None
    order_amount: Decimal
    recovery_status: str = ""
    recovery_category: str = ""
    raw: Any = None


@dataclass(frozen=True)
class ReconciliationSalesPayment:
    """Canonical sales payment row from sales."""

    cost_center: str
    order_number: str
    normalized_order_number: str
    payment_received: Decimal
    raw: Any = None


@dataclass(frozen=True)
class ReconciliationPaymentEvidence:
    """Canonical payment proof row from payment_collections."""

    evidence_id: str
    cost_center: str
    order_number: str
    normalized_order_numbers: tuple[str, ...]
    amount: Decimal
    source_type: str
    raw: Any = None


@dataclass(frozen=True)
class ReconciledOrderPayment:
    cost_center: str
    order_number: str
    normalized_order_number: str
    order_date: datetime | date | None
    order_amount: Decimal
    sales_payment_received: Decimal
    allocated_payment_amount: Decimal
    evidence_amount: Decimal
    sales_evidence_difference: Decimal
    sales_evidence_mismatch: bool
    sales_evidence_consistent: bool
    has_sales_payment_data: bool
    short_amount: Decimal
    status: str
    has_payment_proof: bool
    data_quality_exception: bool = False
    recovery_status: str = ""
    recovery_category: str = ""
    raw_order: Any = None

    def has_sufficient_recovery_auto_clear_proof(
        self, tolerance: Decimal = DEFAULT_PAYMENT_TOLERANCE
    ) -> bool:
        """Return whether payment proof can auto-clear an active recovery row.

        Recovery auto-clear is intentionally proof-sufficiency based. It does
        not depend on the operator-facing reconciliation ``status`` because
        active recovery rows are excluded from Short Payments / Actual Payments
        Not Found action lists and can therefore carry ``recovery_excluded``
        even when valid payment evidence fully covers the order.
        """

        return (
            self.recovery_status.strip().upper() == "TO_BE_RECOVERED"
            and self.order_amount > 0
            and self.has_payment_proof is True
            and self.allocated_payment_amount + tolerance >= self.order_amount
        )

    @property
    def has_recovery_auto_clear_proof(self) -> bool:
        return self.has_sufficient_recovery_auto_clear_proof()


@dataclass(frozen=True)
class ReconciledPaymentGroup:
    group_key: tuple[str, tuple[str, ...]]
    cost_center: str
    normalized_order_numbers: tuple[str, ...]
    group_type: str
    status: str
    expected_order_amount: Decimal
    sales_payment_received: Decimal
    evidence_amount: Decimal
    sales_evidence_difference: Decimal
    sales_evidence_mismatch: bool
    sales_evidence_consistent: bool
    has_sales_payment_data: bool
    short_amount: Decimal
    overpayment_amount: Decimal
    evidence_rows: tuple[ReconciliationPaymentEvidence, ...]
    orders: tuple[ReconciledOrderPayment, ...]
    token_count: int = 0
    matched_order_count: int = 0
    unmatched_order_numbers: tuple[str, ...] = ()
    data_quality_exception: bool = False
    recovery_statuses_csv: str = ""
    recovery_categories_csv: str = ""


@dataclass(frozen=True)
class PaymentReconciliationResult:
    groups: tuple[ReconciledPaymentGroup, ...]
    orders: tuple[ReconciledOrderPayment, ...]
    invalid_evidence_rows: tuple[Any, ...] = field(default_factory=tuple)
    unmatched_evidence_rows: tuple[ReconciliationPaymentEvidence, ...] = field(
        default_factory=tuple
    )

    @property
    def actual_payments_not_found(self) -> tuple[ReconciledOrderPayment, ...]:
        """Orders with sales payment recorded but no valid payment-collection proof."""

        return tuple(
            order
            for order in self.orders
            if order.status == "proof_missing" and order.sales_payment_received > 0
        )

    @property
    def short_payment_orders(self) -> tuple[ReconciledOrderPayment, ...]:
        """Clean operator Short Payments.

        Short Payments require all canonical payment inputs to reconcile cleanly:
        a sales row exists, qualifying payment evidence exists, sales and evidence
        match within the shared ₹1 tolerance, and the evidence is still short
        against ``vw_orders.order_amount`` by more than that tolerance. Raw short
        evidence with missing sales or sales/evidence mismatches remains available
        in audit classifications, but is not part of the operator Short Payments
        action list.
        """

        return tuple(
            order
            for order in self.orders
            if (
                order.status == "short"
                and order.has_sales_payment_data
                and order.has_payment_proof
                and order.sales_evidence_consistent
                and order.short_amount > DEFAULT_PAYMENT_TOLERANCE
            )
        )

    @property
    def recovery_auto_clear_orders(self) -> tuple[ReconciledOrderPayment, ...]:
        return tuple(
            order for order in self.orders if order.has_recovery_auto_clear_proof
        )

    @property
    def data_quality_exception_groups(self) -> tuple[ReconciledPaymentGroup, ...]:
        """Payment components quarantined because at least one token was unmatched."""

        return tuple(group for group in self.groups if group.data_quality_exception)


@dataclass(frozen=True)
class PaymentEvidenceAuditRow:
    payment_id: Any
    source_type: str
    source_sheet_row: Any
    cost_center: str
    payment_date: Any
    payment_timestamp: Any
    order_number: str
    normalized_order_tokens_csv: str
    amount: Decimal
    order_amount: Decimal
    payment_received: Decimal
    reconciliation_result: str
    operator_actionable_payment_status: str
    is_grouped: bool
    bank_row_id: Any
    group_key: str
    grouped_amount: Decimal
    grouped_order_amount: Decimal
    grouped_payment_received: Decimal
    sales_evidence_difference: Decimal
    sales_evidence_mismatch: bool
    sales_evidence_classification: str
    component_id: str
    recovery_statuses_csv: str
    recovery_categories_csv: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "payment_id": self.payment_id,
            "source_type": self.source_type,
            "source_sheet_row": self.source_sheet_row,
            "cost_center": self.cost_center,
            "payment_date": self.payment_date,
            "payment_timestamp": self.payment_timestamp,
            "order_number": self.order_number,
            "normalized_order_tokens_csv": self.normalized_order_tokens_csv,
            "amount": self.amount,
            "order_amount": self.order_amount,
            "payment_received": self.payment_received,
            "reconciliation_result": self.reconciliation_result,
            "operator_actionable_payment_status": (
                self.operator_actionable_payment_status
            ),
            "is_grouped": self.is_grouped,
            "bank_row_id": self.bank_row_id,
            "group_key": self.group_key,
            "grouped_amount": self.grouped_amount,
            "grouped_order_amount": self.grouped_order_amount,
            "grouped_payment_received": self.grouped_payment_received,
            "sales_evidence_difference": self.sales_evidence_difference,
            "sales_evidence_mismatch": self.sales_evidence_mismatch,
            "sales_evidence_classification": self.sales_evidence_classification,
            "component_id": self.component_id,
            "recovery_statuses_csv": self.recovery_statuses_csv,
            "recovery_categories_csv": self.recovery_categories_csv,
        }


def operator_actionable_payment_status(
    reconciliation_result: str, recovery_statuses_csv: str
) -> str:
    """Classify whether an audit row is an operator Short Payment action.

    ``reconciliation_result`` is an audit classification. The Daily Sales Short
    Payments PDF is the operator action list and excludes recovery workflow
    statuses even when the audit classification would otherwise look short.
    """

    recovery_statuses = {
        status.strip().upper()
        for status in str(recovery_statuses_csv or "").split(",")
        if status.strip()
    }
    if recovery_statuses & RECOVERY_EXCLUDED_STATUSES:
        return NON_ACTIONABLE_RECOVERY_STATUS
    if str(reconciliation_result or "").strip().lower() in {"short", "grouped short"}:
        return ACTIONABLE_SHORT_PAYMENT_STATUS
    return AUDIT_ONLY_PAYMENT_STATUS


def normalize_order_number(order_number: Any) -> str:
    """Normalize order numbers consistently across vw_orders, sales, and proofs."""

    return _WHITESPACE_RE.sub("", str(order_number or "")).upper()


def split_payment_order_numbers(order_number: Any) -> tuple[str, ...]:
    """Return canonical exact-match tokens from payment collection order text.

    Grouped payment evidence is supported by splitting on comma and slash.
    Downstream reconciliation compares the resulting normalized tokens exactly,
    not by substring, to avoid false positives such as ``ORD1`` matching
    ``ORD10``.
    """

    tokens: list[str] = []
    seen: set[str] = set()
    for token in _ORDER_TOKEN_SPLIT_RE.split(str(order_number or "")):
        normalized = normalize_order_number(token)
        if normalized and normalized not in seen:
            seen.add(normalized)
            tokens.append(normalized)
    return tuple(tokens)


def reconcile_payments(
    *,
    order_rows: Sequence[Any],
    sales_rows: Sequence[Any] = (),
    payment_evidence_rows: Sequence[Any] = (),
    tolerance: Decimal | int | str = DEFAULT_PAYMENT_TOLERANCE,
    valid_source_types: Iterable[str] = VALID_PAYMENT_SOURCE_TYPES,
) -> PaymentReconciliationResult:
    """Reconcile vw_orders, sales, and payment_collections rows.

    The helper expects order rows to expose ``order_amount`` from ``vw_orders``,
    sales rows to expose ``payment_received``, and payment evidence rows to expose
    ``amount`` from ``payment_collections``. Payment collection ``bank_row_id`` is
    intentionally ignored because proof matching is based on source type, cost
    center, exact normalized order-number tokens, and amount.
    """

    tolerance_amount = _decimal(tolerance)
    valid_sources = {str(source).strip().lower() for source in valid_source_types}

    orders_by_key: dict[tuple[str, str], ReconciliationOrder] = {}
    for row in order_rows:
        order = ReconciliationOrder(
            cost_center=_text(_field(row, "cost_center")),
            order_number=_text(_field(row, "order_number")),
            normalized_order_number=normalize_order_number(_field(row, "order_number")),
            order_date=_field(row, "order_date"),
            order_amount=_decimal(_field(row, "order_amount")),
            recovery_status=_text(_field(row, "recovery_status")),
            recovery_category=_text(_field(row, "recovery_category")),
            raw=row,
        )
        if not order.normalized_order_number:
            continue
        orders_by_key[(order.cost_center, order.normalized_order_number)] = order

    sales_totals: dict[tuple[str, str], Decimal] = defaultdict(lambda: Decimal("0"))
    sales_row_counts: dict[tuple[str, str], int] = defaultdict(int)
    for row in sales_rows:
        cost_center = _text(_field(row, "cost_center"))
        normalized_order_number = normalize_order_number(_field(row, "order_number"))
        if normalized_order_number:
            key = (cost_center, normalized_order_number)
            sales_totals[key] += _decimal(_field(row, "payment_received"))
            sales_row_counts[key] += 1

    evidence_rows: list[ReconciliationPaymentEvidence] = []
    invalid_rows: list[Any] = []
    for index, row in enumerate(payment_evidence_rows):
        source_type = _text(_field(row, "source_type")).lower()
        tokens = split_payment_order_numbers(_field(row, "order_number"))
        if source_type not in valid_sources or not tokens:
            invalid_rows.append(row)
            continue
        evidence_rows.append(
            ReconciliationPaymentEvidence(
                evidence_id=_evidence_id(row, index),
                cost_center=_text(_field(row, "cost_center")),
                order_number=_text(_field(row, "order_number")),
                normalized_order_numbers=tokens,
                amount=_decimal(_field(row, "amount")),
                source_type=source_type,
                raw=row,
            )
        )

    grouped_evidence, unmatched_evidence = _build_evidence_components(
        evidence_rows, orders_by_key
    )

    groups: list[ReconciledPaymentGroup] = []
    reconciled_orders: dict[tuple[str, str], ReconciledOrderPayment] = {}

    for cost_center, normalized_numbers, rows in grouped_evidence:
        group_orders = [
            orders_by_key[(cost_center, number)]
            for number in normalized_numbers
            if (cost_center, number) in orders_by_key
        ]
        if not group_orders:
            unmatched_evidence.extend(rows)
            continue
        unmatched_numbers = tuple(
            number
            for number in normalized_numbers
            if (cost_center, number) not in orders_by_key
        )
        group = _reconcile_group(
            cost_center=cost_center,
            normalized_numbers=tuple(normalized_numbers),
            orders=group_orders,
            evidence_rows=tuple(rows),
            sales_totals=sales_totals,
            sales_row_counts=sales_row_counts,
            tolerance=tolerance_amount,
            unmatched_numbers=unmatched_numbers,
        )
        groups.append(group)
        for order in group.orders:
            reconciled_orders[(order.cost_center, order.normalized_order_number)] = (
                order
            )

    for key, order in sorted(
        orders_by_key.items(), key=lambda item: _order_sort_key(item[1])
    ):
        if key in reconciled_orders:
            continue
        sales_received = sales_totals.get(key, Decimal("0"))
        has_sales_payment_data = sales_row_counts.get(key, 0) > 0
        sales_evidence_mismatch = abs(sales_received) > tolerance_amount
        recovery_excluded = _is_recovery_excluded_status(order.recovery_status)
        status = "recovery_excluded" if recovery_excluded else "proof_missing"
        short_amount = Decimal("0") if recovery_excluded else order.order_amount
        reconciled = ReconciledOrderPayment(
            cost_center=order.cost_center,
            order_number=order.order_number,
            normalized_order_number=order.normalized_order_number,
            order_date=order.order_date,
            order_amount=order.order_amount,
            sales_payment_received=sales_received,
            allocated_payment_amount=Decimal("0"),
            evidence_amount=Decimal("0"),
            sales_evidence_difference=sales_received,
            sales_evidence_mismatch=sales_evidence_mismatch,
            sales_evidence_consistent=has_sales_payment_data
            and not sales_evidence_mismatch,
            has_sales_payment_data=has_sales_payment_data,
            short_amount=short_amount,
            status=status,
            has_payment_proof=False,
            data_quality_exception=False,
            recovery_status=order.recovery_status,
            recovery_category=order.recovery_category,
            raw_order=order.raw,
        )
        group = ReconciledPaymentGroup(
            group_key=(order.cost_center, (order.normalized_order_number,)),
            cost_center=order.cost_center,
            normalized_order_numbers=(order.normalized_order_number,),
            group_type="single_order",
            status=status,
            expected_order_amount=order.order_amount,
            sales_payment_received=sales_received,
            evidence_amount=Decimal("0"),
            sales_evidence_difference=sales_received,
            sales_evidence_mismatch=sales_evidence_mismatch,
            sales_evidence_consistent=has_sales_payment_data
            and not sales_evidence_mismatch,
            has_sales_payment_data=has_sales_payment_data,
            short_amount=short_amount,
            overpayment_amount=Decimal("0"),
            evidence_rows=(),
            orders=(reconciled,),
            token_count=1,
            matched_order_count=1,
            recovery_statuses_csv=order.recovery_status,
            recovery_categories_csv=order.recovery_category,
        )
        groups.append(group)
        reconciled_orders[key] = reconciled

    sorted_groups = tuple(
        sorted(
            groups,
            key=lambda group: (
                group.cost_center,
                _group_order_date(group),
                group.normalized_order_numbers,
            ),
        )
    )
    sorted_orders = tuple(
        sorted(
            reconciled_orders.values(),
            key=lambda order: (
                order.cost_center,
                _date_sort_value(order.order_date),
                order.order_number,
            ),
        )
    )
    return PaymentReconciliationResult(
        groups=sorted_groups,
        orders=sorted_orders,
        invalid_evidence_rows=tuple(invalid_rows),
        unmatched_evidence_rows=tuple(unmatched_evidence),
    )


def _reconcile_group(
    *,
    cost_center: str,
    normalized_numbers: tuple[str, ...],
    orders: Sequence[ReconciliationOrder],
    evidence_rows: tuple[ReconciliationPaymentEvidence, ...],
    sales_totals: Mapping[tuple[str, str], Decimal],
    sales_row_counts: Mapping[tuple[str, str], int],
    tolerance: Decimal,
    unmatched_numbers: tuple[str, ...] = (),
) -> ReconciledPaymentGroup:
    sorted_orders = sorted(orders, key=_order_sort_key)
    expected_amount = sum((order.order_amount for order in sorted_orders), Decimal("0"))
    sales_received = sum(
        (
            sales_totals.get((cost_center, order.normalized_order_number), Decimal("0"))
            for order in sorted_orders
        ),
        Decimal("0"),
    )
    evidence_amount = sum((row.amount for row in evidence_rows), Decimal("0"))
    sales_evidence_difference = sales_received - evidence_amount
    sales_evidence_mismatch = abs(sales_evidence_difference) > tolerance
    has_sales_payment_data = all(
        sales_row_counts.get((cost_center, order.normalized_order_number), 0) > 0
        for order in sorted_orders
    )
    sales_evidence_consistent = has_sales_payment_data and not sales_evidence_mismatch

    recovery_excluded_numbers = {
        order.normalized_order_number
        for order in sorted_orders
        if _is_recovery_excluded_status(order.recovery_status)
    }
    all_recovery_excluded = len(recovery_excluded_numbers) == len(sorted_orders)
    has_recovery_excluded = bool(recovery_excluded_numbers)

    data_quality_exception = bool(unmatched_numbers)
    if data_quality_exception:
        status = "data_quality_exception"
    elif not evidence_rows:
        status = "recovery_excluded" if all_recovery_excluded else "proof_missing"
    elif evidence_amount + tolerance >= expected_amount:
        status = "paid"
    elif all_recovery_excluded:
        status = "recovery_excluded"
    elif has_recovery_excluded:
        status = "mixed_recovery_status"
    else:
        status = "short"

    remaining = Decimal("0") if data_quality_exception else evidence_amount
    allocation_order = [
        *[
            order
            for order in sorted_orders
            if order.normalized_order_number not in recovery_excluded_numbers
        ],
        *[
            order
            for order in sorted_orders
            if order.normalized_order_number in recovery_excluded_numbers
        ],
    ]
    allocations: dict[str, Decimal] = {}
    for order in allocation_order:
        allocated = Decimal("0")
        if remaining > 0:
            allocated = min(remaining, order.order_amount)
            remaining -= allocated
        allocations[order.normalized_order_number] = allocated

    reconciled_orders: list[ReconciledOrderPayment] = []
    for order in sorted_orders:
        allocated = allocations.get(order.normalized_order_number, Decimal("0"))
        short_amount = max(order.order_amount - allocated, Decimal("0"))
        order_key = (cost_center, order.normalized_order_number)
        order_sales_received = sales_totals.get(order_key, Decimal("0"))
        order_has_sales_payment_data = sales_row_counts.get(order_key, 0) > 0
        order_sales_evidence_difference = order_sales_received - allocated
        order_sales_evidence_mismatch = abs(order_sales_evidence_difference) > tolerance
        order_sales_evidence_consistent = (
            order_has_sales_payment_data and not order_sales_evidence_mismatch
        )
        if data_quality_exception:
            order_status = "data_quality_exception"
            short_amount = Decimal("0")
        elif order.normalized_order_number in recovery_excluded_numbers:
            order_status = "recovery_excluded"
            short_amount = Decimal("0")
        elif not evidence_rows:
            order_status = "proof_missing"
        elif allocated + tolerance >= order.order_amount:
            order_status = "paid"
            short_amount = Decimal("0")
        else:
            order_status = "short"
        reconciled_orders.append(
            ReconciledOrderPayment(
                cost_center=order.cost_center,
                order_number=order.order_number,
                normalized_order_number=order.normalized_order_number,
                order_date=order.order_date,
                order_amount=order.order_amount,
                sales_payment_received=order_sales_received,
                allocated_payment_amount=allocated,
                evidence_amount=allocated,
                sales_evidence_difference=order_sales_evidence_difference,
                sales_evidence_mismatch=order_sales_evidence_mismatch,
                sales_evidence_consistent=order_sales_evidence_consistent,
                has_sales_payment_data=order_has_sales_payment_data,
                short_amount=short_amount,
                status=order_status,
                has_payment_proof=bool(evidence_rows) and not data_quality_exception,
                data_quality_exception=data_quality_exception,
                recovery_status=order.recovery_status,
                recovery_category=order.recovery_category,
                raw_order=order.raw,
            )
        )

    actionable_expected_amount = sum(
        (
            order.order_amount
            for order in sorted_orders
            if order.normalized_order_number not in recovery_excluded_numbers
        ),
        Decimal("0"),
    )
    group_short = (
        Decimal("0")
        if status in {"paid", "data_quality_exception", "recovery_excluded"}
        else max(actionable_expected_amount - evidence_amount, Decimal("0"))
    )
    return ReconciledPaymentGroup(
        group_key=(cost_center, normalized_numbers),
        cost_center=cost_center,
        normalized_order_numbers=normalized_numbers,
        group_type="multi_order" if len(normalized_numbers) > 1 else "single_order",
        status=status,
        expected_order_amount=expected_amount,
        sales_payment_received=sales_received,
        evidence_amount=evidence_amount,
        sales_evidence_difference=sales_evidence_difference,
        sales_evidence_mismatch=sales_evidence_mismatch,
        sales_evidence_consistent=sales_evidence_consistent,
        has_sales_payment_data=has_sales_payment_data,
        short_amount=group_short,
        overpayment_amount=max(evidence_amount - expected_amount, Decimal("0")),
        evidence_rows=evidence_rows,
        orders=tuple(reconciled_orders),
        token_count=len(normalized_numbers),
        matched_order_count=len(sorted_orders),
        unmatched_order_numbers=unmatched_numbers,
        data_quality_exception=data_quality_exception,
        recovery_statuses_csv=",".join(
            _unique_nonempty(order.recovery_status for order in sorted_orders)
        ),
        recovery_categories_csv=",".join(
            _unique_nonempty(order.recovery_category for order in sorted_orders)
        ),
    )


def _is_recovery_excluded_status(status: str) -> bool:
    return status.strip().upper() in RECOVERY_EXCLUDED_STATUSES


def _build_evidence_components(
    evidence_rows: Sequence[ReconciliationPaymentEvidence],
    orders_by_key: Mapping[tuple[str, str], ReconciliationOrder],
) -> tuple[
    list[tuple[str, tuple[str, ...], list[ReconciliationPaymentEvidence]]],
    list[ReconciliationPaymentEvidence],
]:
    rows_by_cost_center: dict[str, list[ReconciliationPaymentEvidence]] = defaultdict(
        list
    )
    for row in evidence_rows:
        rows_by_cost_center[row.cost_center].append(row)

    grouped: list[tuple[str, tuple[str, ...], list[ReconciliationPaymentEvidence]]] = []
    unmatched: list[ReconciliationPaymentEvidence] = []
    for cost_center, rows in rows_by_cost_center.items():
        parent: dict[str, str] = {}

        def find(value: str) -> str:
            parent.setdefault(value, value)
            if parent[value] != value:
                parent[value] = find(parent[value])
            return parent[value]

        def union(left: str, right: str) -> None:
            parent[find(right)] = find(left)

        # Model payment evidence as graph edges scoped to one cost center. Each
        # evidence row connects every normalized order token it names; overlapping
        # rows such as ``ORD1,ORD2`` and ``ORD2,ORD3`` therefore collapse into a
        # single component. The component is reconciled once so each payment row
        # contributes its amount once and each matched vw_orders.order_amount is
        # compared against the component total with the shared ₹1 tolerance.
        for index, row in enumerate(rows):
            row_node = f"row:{index}:{row.evidence_id}"
            find(row_node)
            for token in row.normalized_order_numbers:
                token_node = f"token:{token}"
                find(token_node)
                union(row_node, token_node)

        tokens_by_root: dict[str, set[str]] = defaultdict(set)
        rows_by_root: dict[str, list[ReconciliationPaymentEvidence]] = defaultdict(list)
        for index, row in enumerate(rows):
            row_node = f"row:{index}:{row.evidence_id}"
            root = find(row_node)
            tokens_by_root[root].update(row.normalized_order_numbers)
            rows_by_root[root].append(row)

        for root, tokens in tokens_by_root.items():
            normalized_numbers = tuple(
                sorted(
                    tokens,
                    key=lambda token: _token_sort_key(
                        cost_center, token, orders_by_key
                    ),
                )
            )
            component_rows = rows_by_root[root]
            if any(
                (cost_center, token) in orders_by_key for token in normalized_numbers
            ):
                grouped.append((cost_center, normalized_numbers, component_rows))
            else:
                unmatched.extend(component_rows)
    return grouped, unmatched


def build_payment_evidence_audit_rows(
    *,
    order_rows: Sequence[Any],
    sales_rows: Sequence[Any] = (),
    payment_evidence_rows: Sequence[Any] = (),
    tolerance: Decimal | int | str = DEFAULT_PAYMENT_TOLERANCE,
    valid_source_types: Iterable[str] | None = None,
) -> tuple[PaymentEvidenceAuditRow, ...]:
    """Build audit rows from the same reconciliation graph used by reports.

    Payment evidence review is intentionally derived from ``reconcile_payments``
    so overlapping group rows (for example ``ORD1,ORD2`` plus an ``ORD2`` top-up)
    use the same connected-component totals as short-payment reporting and
    recovery auto-clear.
    """

    source_types = (
        {_text(_field(row, "source_type")).lower() for row in payment_evidence_rows}
        if valid_source_types is None
        else {str(source).strip().lower() for source in valid_source_types}
    )
    result = reconcile_payments(
        order_rows=order_rows,
        sales_rows=sales_rows,
        payment_evidence_rows=payment_evidence_rows,
        tolerance=tolerance,
        valid_source_types=source_types,
    )
    groups_by_evidence_id = {
        evidence.evidence_id: group
        for group in result.groups
        for evidence in group.evidence_rows
    }
    unmatched_by_evidence_id = {
        evidence.evidence_id: evidence for evidence in result.unmatched_evidence_rows
    }

    rows: list[PaymentEvidenceAuditRow] = []
    for index, raw_row in enumerate(payment_evidence_rows):
        evidence_id = _evidence_id(raw_row, index)
        source_type = _text(_field(raw_row, "source_type")).lower()
        tokens = split_payment_order_numbers(_field(raw_row, "order_number"))
        group = groups_by_evidence_id.get(evidence_id)
        unmatched = unmatched_by_evidence_id.get(evidence_id)

        if group is not None:
            payment_received = group.sales_payment_received
            order_amount = group.expected_order_amount
            grouped_amount = group.evidence_amount
            grouped_order_amount = group.expected_order_amount
            grouped_payment_received = group.sales_payment_received
            sales_evidence_difference = group.sales_evidence_difference
            sales_evidence_mismatch = group.sales_evidence_mismatch
            normalized_tokens = group.normalized_order_numbers
            is_grouped = len(normalized_tokens) > 1
            group_key = _audit_group_key(group.cost_center, normalized_tokens)
            recovery_statuses_csv = group.recovery_statuses_csv
            recovery_categories_csv = group.recovery_categories_csv
            component_id = _audit_component_id(group.cost_center, normalized_tokens)
            if group.data_quality_exception:
                reconciliation_result = "unmatched order token"
            elif group.status in {"recovery_excluded", "mixed_recovery_status"}:
                reconciliation_result = group.status
            elif group.sales_payment_received <= 0:
                reconciliation_result = "missing sales"
            elif group.status == "paid":
                reconciliation_result = "grouped paid" if is_grouped else "paid"
            elif group.status == "short" and group.sales_evidence_mismatch:
                reconciliation_result = (
                    "grouped sales/evidence mismatch short"
                    if is_grouped
                    else "sales/evidence mismatch short"
                )
            elif group.status == "short":
                reconciliation_result = "grouped short" if is_grouped else "short"
            else:
                reconciliation_result = group.status
        else:
            normalized_tokens = (
                unmatched.normalized_order_numbers if unmatched is not None else tokens
            )
            is_grouped = len(normalized_tokens) > 1
            group_key = (
                _audit_group_key(
                    _text(_field(raw_row, "cost_center")), normalized_tokens
                )
                if normalized_tokens
                else ""
            )
            payment_received = Decimal("0")
            order_amount = Decimal("0")
            grouped_amount = _decimal(_field(raw_row, "amount"))
            grouped_order_amount = Decimal("0")
            grouped_payment_received = Decimal("0")
            sales_evidence_difference = Decimal("0") - grouped_amount
            sales_evidence_mismatch = abs(sales_evidence_difference) > _decimal(
                tolerance
            )
            reconciliation_result = "missing order token"
            recovery_statuses_csv = ""
            recovery_categories_csv = ""
            component_id = _audit_component_id(
                _text(_field(raw_row, "cost_center")), normalized_tokens
            )

        rows.append(
            PaymentEvidenceAuditRow(
                payment_id=_field(raw_row, "payment_id"),
                source_type=source_type,
                source_sheet_row=_field(raw_row, "source_sheet_row"),
                cost_center=_text(_field(raw_row, "cost_center")),
                payment_date=_field(raw_row, "payment_date"),
                payment_timestamp=_field(raw_row, "payment_timestamp"),
                order_number=_text(_field(raw_row, "order_number")),
                normalized_order_tokens_csv=",".join(normalized_tokens),
                amount=_decimal(_field(raw_row, "amount")),
                order_amount=order_amount,
                payment_received=payment_received,
                reconciliation_result=reconciliation_result,
                operator_actionable_payment_status=operator_actionable_payment_status(
                    reconciliation_result, recovery_statuses_csv
                ),
                is_grouped=is_grouped,
                bank_row_id=_field(raw_row, "bank_row_id"),
                group_key=group_key,
                grouped_amount=grouped_amount,
                grouped_order_amount=grouped_order_amount,
                grouped_payment_received=grouped_payment_received,
                sales_evidence_difference=sales_evidence_difference,
                sales_evidence_mismatch=sales_evidence_mismatch,
                sales_evidence_classification=_sales_evidence_classification(
                    sales_evidence_difference, _decimal(tolerance)
                ),
                component_id=component_id,
                recovery_statuses_csv=recovery_statuses_csv,
                recovery_categories_csv=recovery_categories_csv,
            )
        )
    return tuple(rows)


def _audit_group_key(cost_center: str, normalized_numbers: tuple[str, ...]) -> str:
    if not normalized_numbers:
        return ""
    return "|".join(normalized_numbers)


def _audit_component_id(cost_center: str, normalized_numbers: tuple[str, ...]) -> str:
    group_key = _audit_group_key(cost_center, normalized_numbers)
    return f"{cost_center}|{group_key}" if group_key else ""


def _sales_evidence_classification(difference: Decimal, tolerance: Decimal) -> str:
    if abs(difference) <= tolerance:
        return "matched"
    if difference > 0:
        return "sales higher"
    return "evidence higher"


def _unique_nonempty(values: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    unique: list[str] = []
    for value in values:
        text = _text(value)
        if text and text not in seen:
            seen.add(text)
            unique.append(text)
    return tuple(sorted(unique))


def _field(row: Any, name: str) -> Any:
    if isinstance(row, Mapping):
        return row.get(name)
    return getattr(row, name, None)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if value is None or value == "":
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _evidence_id(row: Any, index: int) -> str:
    for field_name in ("payment_id", "id", "source_sheet_row"):
        value = _field(row, field_name)
        if value not in (None, ""):
            return str(value)
    return f"row-{index}"


def _order_sort_key(order: ReconciliationOrder) -> tuple[str, str, str]:
    return (order.cost_center, _date_sort_value(order.order_date), order.order_number)


def _token_sort_key(
    cost_center: str,
    token: str,
    orders_by_key: Mapping[tuple[str, str], ReconciliationOrder],
) -> tuple[str, str]:
    order = orders_by_key.get((cost_center, token))
    if order is None:
        return ("9999-12-31T23:59:59", token)
    return (_date_sort_value(order.order_date), order.order_number)


def _group_order_date(group: ReconciledPaymentGroup) -> str:
    dates = [_date_sort_value(order.order_date) for order in group.orders]
    return min(dates) if dates else ""


def _date_sort_value(value: datetime | date | None) -> str:
    if value is None:
        return ""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)
