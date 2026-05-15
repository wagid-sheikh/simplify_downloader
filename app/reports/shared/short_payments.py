"""Short-payment and missing-payment report loaders.

Python reconciliation is canonical for runnable reports.  The
``vw_orders_missing_in_payment_collections`` SQL view is retained as a
compatibility/audit projection and must mirror the missing-proof subset exposed
by ``fetch_missing_payment_rows_without_proof``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

import sqlalchemy as sa
from sqlalchemy.exc import OperationalError

from app.reports.shared.payment_reconciliation import (
    ReconciledOrderPayment,
    normalize_order_number,
    reconcile_payments,
    split_payment_order_numbers,
)

RECOVERY_PAYMENT_EXCLUSIONS = (
    "TO_BE_RECOVERED",
    "TO_BE_COMPENSATED",
    "RECOVERED",
    "COMPENSATED",
    "WRITE_OFF",
)
QUALIFYING_PAYMENT_SOURCE_TYPES = ("google_sheet", "legacy_sales")


@dataclass
class ShortPaymentRow:
    cost_center: str
    order_number: str
    order_date: datetime | None
    customer_name: str | None
    mobile_number: str | None
    order_amount: Decimal
    paid_amount: Decimal
    shortage_amount: Decimal
    group_key: str | None = None


def payment_collection_order_tokens(order_number: object | None) -> tuple[str, ...]:
    return split_payment_order_numbers(order_number)


def _decimal(value: object | None) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _normalized_order_number(value: object | None) -> str:
    return normalize_order_number(value)


def _as_datetime(value: object | None) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    if hasattr(value, "year") and hasattr(value, "month") and hasattr(value, "day"):
        return datetime(value.year, value.month, value.day)
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


async def fetch_short_payment_rows(
    *,
    session: Any,
    orders: Any,
    payment_collections: Any,
    sales: Any,
    start_datetime: datetime | None = None,
    end_datetime: datetime | None = None,
) -> list[ShortPaymentRow]:
    """Return clean, sales-backed short payments across all order dates.

    Short Payments are a global/current operator action list, matching the
    open-ended behavior of manual recovery buckets such as ``TO_BE_RECOVERED``.
    ``start_datetime`` and ``end_datetime`` are accepted for call-site
    compatibility but do not limit candidate orders.

    Short Payment rows are the normal operator-facing bucket for orders where
    all three payment-truth inputs agree that money was received but is still
    short of ``vw_orders.order_amount``: ``sales.payment_received`` exists,
    qualifying ``payment_collections.amount`` proof exists, and sales/proof are
    consistent within the shared ₹1 tolerance. Proof-only shorts and
    sales/evidence mismatches are left out of this clean bucket and remain
    visible through the payment-evidence audit classification instead of being
    silently presented as normal short payments.

    Payment evidence is grouped in ``payment_reconciliation`` by connected
    cost-center/order-token components.  This keeps overlapping grouped rows
    (for example ``ORD1,ORD2`` plus ``ORD2,ORD3``) in one component, sums every
    payment row once, and applies the ₹1 tolerance before this report selects
    component-level short orders.
    """

    reconciliation = await _fetch_reconciliation(
        session=session,
        orders=orders,
        payment_collections=payment_collections,
        sales=sales,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        filter_order_date=False,
    )

    rows = [
        ShortPaymentRow(
            cost_center=order.cost_center,
            order_number=order.order_number,
            order_date=_as_datetime(order.order_date),
            customer_name=_raw_text(order, "customer_name"),
            mobile_number=_raw_text(order, "mobile_number"),
            order_amount=order.order_amount,
            paid_amount=order.evidence_amount,
            shortage_amount=order.short_amount,
            group_key=_group_key_for_order(order=order, reconciliation=reconciliation),
        )
        for order in reconciliation.short_payment_orders
        if (
            order.order_amount > 0
            and order.has_sales_payment_data
            and order.sales_evidence_consistent
        )
    ]
    rows.sort(
        key=lambda row: (
            row.cost_center,
            row.order_date or datetime.min,
            row.order_number,
        )
    )
    return rows


async def fetch_missing_payment_rows_without_proof(
    *,
    session: Any,
    orders: Any,
    payment_collections: Any,
    start_datetime: datetime,
    end_datetime: datetime,
    row_factory: Any,
    sales: Any | None = None,
) -> list[Any]:
    """Return sales-paid orders whose valid payment proof is absent.

    Evidence components with unmatched order tokens are reported through the
    payment-evidence audit bucket instead of Actual Payments Not Found.
    """

    reconciliation = await _fetch_reconciliation(
        session=session,
        orders=orders,
        payment_collections=payment_collections,
        sales=sales,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
    )

    rows: list[Any] = []
    for order in reconciliation.actual_payments_not_found:
        if order.order_amount <= 0:
            continue
        rows.append(
            row_factory(
                cost_center=order.cost_center,
                order_number=order.order_number,
                order_date=_as_datetime(order.order_date),
                customer_name=_raw_text(order, "customer_name"),
                mobile_number=_raw_text(order, "mobile_number"),
                order_amount=order.order_amount,
            )
        )
    return rows


async def _fetch_reconciliation(
    *,
    session: Any,
    orders: Any,
    payment_collections: Any,
    sales: Any | None,
    start_datetime: datetime | None,
    end_datetime: datetime | None,
    filter_order_date: bool = True,
):
    order_stmt = (
        sa.select(
            orders.c.cost_center,
            orders.c.order_number,
            orders.c.order_date,
            orders.c.customer_name,
            orders.c.mobile_number,
            orders.c.order_amount,
        )
        .where(orders.c.order_amount > 0)
        .where(
            sa.func.coalesce(orders.c.recovery_status, "NONE").not_in(
                RECOVERY_PAYMENT_EXCLUSIONS
            )
        )
        .order_by(orders.c.cost_center, orders.c.order_date, orders.c.order_number)
    )
    if filter_order_date:
        if start_datetime is None or end_datetime is None:
            raise ValueError(
                "start_datetime and end_datetime are required when filtering by order date"
            )
        order_stmt = order_stmt.where(orders.c.order_date >= start_datetime).where(
            orders.c.order_date < end_datetime
        )
    try:
        order_result = await session.execute(order_stmt)
    except OperationalError as exc:
        if "recovery_status" not in str(exc):
            raise
        order_stmt = (
            sa.select(
                orders.c.cost_center,
                orders.c.order_number,
                orders.c.order_date,
                orders.c.customer_name,
                orders.c.mobile_number,
                orders.c.order_amount,
            )
            .where(orders.c.order_amount > 0)
            .order_by(orders.c.cost_center, orders.c.order_date, orders.c.order_number)
        )
        if filter_order_date:
            order_stmt = order_stmt.where(orders.c.order_date >= start_datetime).where(
                orders.c.order_date < end_datetime
            )
        order_result = await session.execute(order_stmt)
    order_rows = [dict(record) for record in order_result.mappings()]
    payment_rows = await _fetch_payment_rows_for_orders(
        session=session,
        payment_collections=payment_collections,
        order_rows=order_rows,
    )

    sales_rows: list[dict[str, Any]] = []
    if sales is not None:
        sales_rows = await _fetch_sales_rows_for_orders(
            session=session,
            sales=sales,
            order_rows=order_rows,
        )

    return reconcile_payments(
        order_rows=order_rows,
        sales_rows=sales_rows,
        payment_evidence_rows=payment_rows,
        valid_source_types=QUALIFYING_PAYMENT_SOURCE_TYPES,
    )


def _raw_text(order: ReconciledOrderPayment, field_name: str) -> str:
    raw = order.raw_order
    if isinstance(raw, dict):
        return str(raw.get(field_name) or "")
    return str(getattr(raw, field_name, "") or "")


def _group_key_for_order(
    *, order: ReconciledOrderPayment, reconciliation: Any
) -> str | None:
    for group in reconciliation.groups:
        if any(
            group_order.cost_center == order.cost_center
            and group_order.normalized_order_number == order.normalized_order_number
            for group_order in group.orders
        ):
            if len(group.normalized_order_numbers) <= 1:
                return None
            return "|".join(group.normalized_order_numbers)
    return None


def _normalised_order_sql(column: Any) -> Any:
    return sa.func.upper(sa.func.replace(sa.func.coalesce(column, ""), " ", ""))


async def _fetch_payment_rows_for_orders(
    *, session: Any, payment_collections: Any, order_rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Fetch only payment evidence that can affect the candidate order set.

    Payment collections are historical and can grow much larger than the daily
    or MTD order windows.  Reconciliation first selects candidate orders, then
    constrains evidence by candidate cost center and, when the token set is a
    safe size for SQL parameters, by normalized order-token containment.
    """

    candidate_cost_centers = sorted(
        {
            str(row.get("cost_center") or "")
            for row in order_rows
            if row.get("cost_center")
        }
    )
    if not candidate_cost_centers:
        return []

    candidate_order_tokens = sorted(
        {
            normalize_order_number(row.get("order_number"))
            for row in order_rows
            if normalize_order_number(row.get("order_number"))
        }
    )
    payment_stmt = (
        sa.select(
            payment_collections.c.cost_center,
            payment_collections.c.order_number,
            payment_collections.c.amount,
            payment_collections.c.source_type,
        )
        .where(payment_collections.c.cost_center.in_(candidate_cost_centers))
        .where(
            sa.func.lower(payment_collections.c.source_type).in_(
                QUALIFYING_PAYMENT_SOURCE_TYPES
            )
        )
    )

    # Keep the SQL shape bounded for large MTD windows; cost center filtering is
    # still mandatory, and token filtering is applied when feasible.
    if 0 < len(candidate_order_tokens) <= 500:
        normalized_payment_order = _normalised_order_sql(
            payment_collections.c.order_number
        )
        payment_stmt = payment_stmt.where(
            sa.or_(
                *[
                    normalized_payment_order.contains(token, autoescape=True)
                    for token in candidate_order_tokens
                ]
            )
        )

    try:
        payment_result = await session.execute(payment_stmt)
        return [dict(record) for record in payment_result.mappings()]
    except OperationalError as exc:
        if "payment_collections.amount" not in str(
            exc
        ) and "payment_collections.source_type" not in str(exc):
            raise
        return []


async def _fetch_sales_rows_for_orders(
    *, session: Any, sales: Any, order_rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    order_keys = {
        (row["cost_center"], normalize_order_number(row["order_number"]))
        for row in order_rows
    }
    if not order_keys:
        return []

    order_numbers_by_cost_center: dict[str, set[str]] = {}
    for cost_center, order_number in order_keys:
        order_numbers_by_cost_center.setdefault(cost_center, set()).add(order_number)

    predicates = [
        sa.and_(
            sales.c.cost_center == cost_center,
            _normalised_order_sql(sales.c.order_number).in_(tuple(order_numbers)),
        )
        for cost_center, order_numbers in order_numbers_by_cost_center.items()
    ]
    sales_stmt = (
        sa.select(
            sales.c.cost_center,
            sales.c.order_number,
            sa.func.sum(sa.func.coalesce(sales.c.payment_received, 0)).label(
                "payment_received"
            ),
        )
        .where(sa.or_(*predicates))
        .group_by(sales.c.cost_center, sales.c.order_number)
    )
    sales_result = await session.execute(sales_stmt)
    return [
        dict(record)
        for record in sales_result.mappings()
        if (record["cost_center"], normalize_order_number(record["order_number"]))
        in order_keys
    ]
