from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import re
from typing import Any

import sqlalchemy as sa


RECOVERY_PAYMENT_EXCLUSIONS = (
    "TO_BE_RECOVERED",
    "TO_BE_COMPENSATED",
    "RECOVERED",
    "COMPENSATED",
    "WRITE_OFF",
)
QUALIFYING_PAYMENT_SOURCE_TYPES = ("google_sheet", "legacy_sales")
_PAYMENT_TOKEN_RE = re.compile(r"[,/]")


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
    if order_number is None:
        return ()
    return tuple(
        token.upper().replace(" ", "")
        for token in (
            raw_token.strip()
            for raw_token in _PAYMENT_TOKEN_RE.split(str(order_number))
        )
        if token
    )


def _decimal(value: object | None) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _normalized_order_number(value: object | None) -> str:
    return str(value or "").upper().replace(" ", "")


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
    start_datetime: datetime,
    end_datetime: datetime,
) -> list[ShortPaymentRow]:
    """Return partially paid order rows from payment proof records.

    Single-payment proofs are compared order-by-order. Multi-order payment proofs
    are grouped by their normalized token set and allocated to matching orders in
    order_date/order_number sequence so early orders consume the available proof
    amount first.
    """

    order_stmt = (
        sa.select(
            orders.c.cost_center,
            orders.c.order_number,
            orders.c.order_date,
            orders.c.customer_name,
            orders.c.mobile_number,
            orders.c.order_amount,
        )
        .where(orders.c.order_date >= start_datetime)
        .where(orders.c.order_date < end_datetime)
        .where(orders.c.order_amount > 0)
        .where(
            sa.func.coalesce(orders.c.recovery_status, "NONE").not_in(
                RECOVERY_PAYMENT_EXCLUSIONS
            )
        )
        .order_by(orders.c.cost_center, orders.c.order_date, orders.c.order_number)
    )
    payment_stmt = sa.select(
        payment_collections.c.cost_center,
        payment_collections.c.order_number,
        payment_collections.c.amount,
    ).where(payment_collections.c.source_type.in_(QUALIFYING_PAYMENT_SOURCE_TYPES))

    order_result = await session.execute(order_stmt)
    payment_result = await session.execute(payment_stmt)

    orders_by_key: dict[tuple[str, str], dict[str, object]] = {}
    for record in order_result.mappings():
        cost_center = str(record["cost_center"] or "")
        token = _normalized_order_number(record["order_number"])
        orders_by_key[(cost_center, token)] = dict(record)

    single_paid: dict[tuple[str, str], Decimal] = {}
    group_paid: dict[tuple[str, str], Decimal] = {}
    group_tokens: dict[tuple[str, str], tuple[str, ...]] = {}

    for record in payment_result.mappings():
        cost_center = str(record["cost_center"] or "")
        tokens = tuple(dict.fromkeys(payment_collection_order_tokens(record["order_number"])))
        if not tokens:
            continue
        amount = _decimal(record["amount"])
        if len(tokens) == 1:
            key = (cost_center, tokens[0])
            single_paid[key] = single_paid.get(key, Decimal("0")) + amount
            continue
        group_key = "|".join(sorted(tokens))
        key = (cost_center, group_key)
        group_paid[key] = group_paid.get(key, Decimal("0")) + amount
        group_tokens[key] = tuple(sorted(tokens))

    short_rows: list[ShortPaymentRow] = []
    grouped_order_keys: set[tuple[str, str]] = set()

    for (cost_center, group_key), paid_amount in group_paid.items():
        matching_orders = [
            orders_by_key[(cost_center, token)]
            for token in group_tokens[(cost_center, group_key)]
            if (cost_center, token) in orders_by_key
        ]
        if not matching_orders:
            continue
        matching_orders.sort(
            key=lambda row: (
                _as_datetime(row.get("order_date")) or datetime.min,
                str(row.get("order_number") or ""),
            )
        )
        expected_amount = sum((_decimal(row.get("order_amount")) for row in matching_orders), Decimal("0"))
        if expected_amount > 0 and paid_amount + Decimal("1") >= expected_amount:
            continue
        remaining_paid = paid_amount
        for row in matching_orders:
            order_amount = _decimal(row.get("order_amount"))
            allocated_paid = min(order_amount, max(remaining_paid, Decimal("0")))
            remaining_paid -= allocated_paid
            shortage = order_amount - allocated_paid
            grouped_order_keys.add((cost_center, _normalized_order_number(row.get("order_number"))))
            if shortage > Decimal("1"):
                short_rows.append(
                    ShortPaymentRow(
                        cost_center=cost_center,
                        order_number=str(row.get("order_number") or ""),
                        order_date=_as_datetime(row.get("order_date")),
                        customer_name=str(row.get("customer_name") or ""),
                        mobile_number=str(row.get("mobile_number") or ""),
                        order_amount=order_amount,
                        paid_amount=allocated_paid,
                        shortage_amount=shortage,
                        group_key=group_key,
                    )
                )

    for key, paid_amount in single_paid.items():
        if key in grouped_order_keys or key not in orders_by_key:
            continue
        row = orders_by_key[key]
        order_amount = _decimal(row.get("order_amount"))
        shortage = order_amount - paid_amount
        if shortage > Decimal("1"):
            short_rows.append(
                ShortPaymentRow(
                    cost_center=key[0],
                    order_number=str(row.get("order_number") or ""),
                    order_date=_as_datetime(row.get("order_date")),
                    customer_name=str(row.get("customer_name") or ""),
                    mobile_number=str(row.get("mobile_number") or ""),
                    order_amount=order_amount,
                    paid_amount=paid_amount,
                    shortage_amount=shortage,
                    group_key=None,
                )
            )

    short_rows.sort(key=lambda row: (row.cost_center, row.order_date or datetime.min, row.order_number))
    return short_rows


async def fetch_missing_payment_rows_without_proof(
    *,
    session: Any,
    orders: Any,
    payment_collections: Any,
    start_datetime: datetime,
    end_datetime: datetime,
    row_factory: Any,
) -> list[Any]:
    order_stmt = (
        sa.select(
            orders.c.cost_center,
            orders.c.order_number,
            orders.c.order_date,
            orders.c.customer_name,
            orders.c.mobile_number,
            orders.c.order_amount,
        )
        .where(orders.c.order_date >= start_datetime)
        .where(orders.c.order_date < end_datetime)
        .where(orders.c.order_amount > 0)
        .where(
            sa.func.coalesce(orders.c.recovery_status, "NONE").not_in(
                RECOVERY_PAYMENT_EXCLUSIONS
            )
        )
        .order_by(orders.c.cost_center, orders.c.order_date, orders.c.order_number)
    )
    payment_stmt = sa.select(
        payment_collections.c.cost_center,
        payment_collections.c.order_number,
    ).where(payment_collections.c.source_type.in_(QUALIFYING_PAYMENT_SOURCE_TYPES))

    order_result = await session.execute(order_stmt)
    payment_result = await session.execute(payment_stmt)

    proof_tokens = {
        (str(record["cost_center"] or ""), token)
        for record in payment_result.mappings()
        for token in payment_collection_order_tokens(record["order_number"])
    }

    rows: list[Any] = []
    for record in order_result.mappings():
        key = (str(record["cost_center"] or ""), _normalized_order_number(record["order_number"]))
        if key in proof_tokens:
            continue
        rows.append(
            row_factory(
                cost_center=str(record["cost_center"] or ""),
                order_number=str(record["order_number"] or ""),
                order_date=_as_datetime(record["order_date"]),
                customer_name=str(record["customer_name"] or ""),
                mobile_number=str(record["mobile_number"] or ""),
                order_amount=_decimal(record["order_amount"]),
            )
        )
    return rows
