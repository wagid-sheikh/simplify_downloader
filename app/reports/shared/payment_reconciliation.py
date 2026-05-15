from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping, Sequence
import re

VALID_PAYMENT_SOURCE_TYPES = frozenset({"google_sheet", "legacy_sales"})
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
    short_amount: Decimal
    status: str
    has_payment_proof: bool
    raw_order: Any = None


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
    short_amount: Decimal
    overpayment_amount: Decimal
    evidence_rows: tuple[ReconciliationPaymentEvidence, ...]
    orders: tuple[ReconciledOrderPayment, ...]


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
        return tuple(order for order in self.orders if order.status == "short")

    @property
    def recovery_auto_clear_orders(self) -> tuple[ReconciledOrderPayment, ...]:
        return tuple(
            order
            for order in self.orders
            if order.status == "paid" and order.has_payment_proof
        )


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
    is_grouped: bool
    bank_row_id: Any
    group_key: str
    grouped_amount: Decimal
    grouped_order_amount: Decimal
    grouped_payment_received: Decimal

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
            "is_grouped": self.is_grouped,
            "bank_row_id": self.bank_row_id,
            "group_key": self.group_key,
            "grouped_amount": self.grouped_amount,
            "grouped_order_amount": self.grouped_order_amount,
            "grouped_payment_received": self.grouped_payment_received,
        }


def normalize_order_number(order_number: Any) -> str:
    """Normalize order numbers consistently across vw_orders, sales, and proofs."""

    return _WHITESPACE_RE.sub("", str(order_number or "")).upper()


def split_payment_order_numbers(order_number: Any) -> tuple[str, ...]:
    """Split payment_collections.order_number on comma and slash and normalize tokens."""

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
    center, order-number tokens, and amount.
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
            raw=row,
        )
        if not order.normalized_order_number:
            continue
        orders_by_key[(order.cost_center, order.normalized_order_number)] = order

    sales_totals: dict[tuple[str, str], Decimal] = defaultdict(lambda: Decimal("0"))
    for row in sales_rows:
        cost_center = _text(_field(row, "cost_center"))
        normalized_order_number = normalize_order_number(_field(row, "order_number"))
        if normalized_order_number:
            sales_totals[(cost_center, normalized_order_number)] += _decimal(
                _field(row, "payment_received")
            )

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
        group = _reconcile_group(
            cost_center=cost_center,
            normalized_numbers=tuple(normalized_numbers),
            orders=group_orders,
            evidence_rows=tuple(rows),
            sales_totals=sales_totals,
            tolerance=tolerance_amount,
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
        status = "proof_missing"
        reconciled = ReconciledOrderPayment(
            cost_center=order.cost_center,
            order_number=order.order_number,
            normalized_order_number=order.normalized_order_number,
            order_date=order.order_date,
            order_amount=order.order_amount,
            sales_payment_received=sales_received,
            allocated_payment_amount=Decimal("0"),
            short_amount=order.order_amount,
            status=status,
            has_payment_proof=False,
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
            short_amount=order.order_amount,
            overpayment_amount=Decimal("0"),
            evidence_rows=(),
            orders=(reconciled,),
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
    tolerance: Decimal,
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

    if not evidence_rows:
        status = "proof_missing"
    elif evidence_amount + tolerance >= expected_amount:
        status = "paid"
    else:
        status = "short"

    remaining = evidence_amount
    reconciled_orders: list[ReconciledOrderPayment] = []
    for order in sorted_orders:
        allocated = Decimal("0")
        if remaining > 0:
            allocated = min(remaining, order.order_amount)
            remaining -= allocated
        short_amount = max(order.order_amount - allocated, Decimal("0"))
        if not evidence_rows:
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
                sales_payment_received=sales_totals.get(
                    (cost_center, order.normalized_order_number), Decimal("0")
                ),
                allocated_payment_amount=allocated,
                short_amount=short_amount,
                status=order_status,
                has_payment_proof=bool(evidence_rows),
                raw_order=order.raw,
            )
        )

    group_short = (
        Decimal("0")
        if status == "paid"
        else max(expected_amount - evidence_amount, Decimal("0"))
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
        short_amount=group_short,
        overpayment_amount=max(evidence_amount - expected_amount, Decimal("0")),
        evidence_rows=evidence_rows,
        orders=tuple(reconciled_orders),
    )


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

        for row in rows:
            for token in row.normalized_order_numbers:
                find(token)
            first = row.normalized_order_numbers[0]
            for token in row.normalized_order_numbers[1:]:
                union(first, token)

        tokens_by_root: dict[str, set[str]] = defaultdict(set)
        rows_by_root: dict[str, list[ReconciliationPaymentEvidence]] = defaultdict(list)
        for row in rows:
            root = find(row.normalized_order_numbers[0])
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
            normalized_tokens = group.normalized_order_numbers
            is_grouped = len(normalized_tokens) > 1
            group_key = _audit_group_key(group.cost_center, normalized_tokens)
            if group.sales_payment_received <= 0:
                reconciliation_result = "missing sales"
            elif group.status == "paid":
                reconciliation_result = "grouped paid" if is_grouped else "paid"
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
            reconciliation_result = "missing order token"

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
                is_grouped=is_grouped,
                bank_row_id=_field(raw_row, "bank_row_id"),
                group_key=group_key,
                grouped_amount=grouped_amount,
                grouped_order_amount=grouped_order_amount,
                grouped_payment_received=grouped_payment_received,
            )
        )
    return tuple(rows)


def _audit_group_key(cost_center: str, normalized_numbers: tuple[str, ...]) -> str:
    if not normalized_numbers:
        return ""
    return "|".join(normalized_numbers)


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
