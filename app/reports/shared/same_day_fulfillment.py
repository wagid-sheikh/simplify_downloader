from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

import sqlalchemy as sa


@dataclass
class SameDayFulfillmentRecord:
    cost_center: str
    store_code: str
    order_number: str
    order_date: datetime | None
    customer_name: str | None
    mobile_number: str | None
    line_items: str | None
    line_item_rows: list[dict[str, str]]
    payment_date: datetime | None
    payment_mode: str | None
    net_amount: Any
    payment_received: Any


def format_duration_minutes(total_minutes: int | None) -> str:
    if total_minutes is None:
        return "--"
    if total_minutes < 60:
        return f"{total_minutes} min"
    duration_hours, duration_minutes = divmod(total_minutes, 60)
    hour_label = "hr" if duration_hours == 1 else "hrs"
    return f"{duration_hours} {hour_label} {duration_minutes} min"


def format_duration_hours(hours: float | Decimal | None) -> str:
    if hours is None:
        return "--"
    return format_duration_minutes(round(float(hours) * 60))


def group_rows_by_store(rows: list[Any]) -> list[tuple[str, list[Any]]]:
    grouped: dict[str, list[Any]] = defaultdict(list)
    for row in rows:
        store_code = str(getattr(row, "store_code", "") or "").strip() or "--"
        grouped[store_code].append(row)

    sorted_groups: list[tuple[str, list[Any]]] = []
    for store_code in sorted(grouped):
        store_rows = sorted(
            grouped[store_code],
            key=lambda row: (
                getattr(row, "order_date", None) or datetime.min,
                str(getattr(row, "order_number", "") or ""),
            ),
            reverse=True,
        )
        sorted_groups.append((store_code, store_rows))
    return sorted_groups


def build_store_summary(rows: list[Any]) -> list[dict[str, Any]]:
    summary_rows: list[dict[str, Any]] = []
    for store_code, store_rows in group_rows_by_store(rows):
        durations = [round(float(value) * 60) for value in (getattr(row, "hours", None) for row in store_rows) if value is not None]
        summary_rows.append(
            {
                "store_code": store_code,
                "total_orders": len(store_rows),
                "total_net_amount": sum((getattr(row, "net_amount", Decimal("0")) or Decimal("0")) for row in store_rows),
                "total_payment_received": sum((getattr(row, "payment_received", Decimal("0")) or Decimal("0")) for row in store_rows),
                "avg_minutes": (round(sum(durations) / len(durations)) if durations else None),
                "fastest_minutes": (min(durations) if durations else None),
                "slowest_minutes": (max(durations) if durations else None),
            }
        )
    return summary_rows




def _line_item_struct(service_name: object | None, garment_name: object | None) -> dict[str, str]:
    return {
        "service_name": str(service_name) if service_name is not None else "",
        "garment_name": str(garment_name) if garment_name is not None else "",
    }
def string_list_agg(*, dialect_name: str, value_expr: Any, separator: str):
    if dialect_name == "postgresql":
        return sa.func.string_agg(value_expr, sa.literal(separator))
    return sa.func.group_concat(value_expr, separator)


def build_line_items_agg(*, order_line_items: sa.Table, dialect_name: str) -> sa.Subquery:
    line_item_name = sa.func.trim(
        sa.func.coalesce(order_line_items.c.service_name, "")
        + sa.literal(" ")
        + sa.func.coalesce(order_line_items.c.garment_name, "")
    )
    return (
        sa.select(
            order_line_items.c.cost_center.label("cost_center"),
            order_line_items.c.order_number.label("order_number"),
            string_list_agg(dialect_name=dialect_name, value_expr=line_item_name, separator=", ").label("line_items"),
        )
        .group_by(order_line_items.c.cost_center, order_line_items.c.order_number)
        .subquery()
    )


def same_day_date_expr(*, dialect_name: str, dt_expr: Any, timezone_name: str):
    if dialect_name == "postgresql":
        return sa.cast(sa.func.timezone(timezone_name, dt_expr), sa.Date)
    return sa.func.substr(sa.cast(dt_expr, sa.String), 1, 10)


def coerce_datetime(value: datetime | str | None) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


async def fetch_same_day_fulfillment_rows(
    *,
    session,
    orders: sa.Table,
    sales: sa.Table,
    order_line_items: sa.Table,
    store_master: sa.Table,
    start_datetime: datetime,
    end_datetime: datetime,
    timezone_name: str,
) -> list[SameDayFulfillmentRecord]:
    bind = getattr(session, "bind", None)
    dialect_name = bind.dialect.name if bind is not None else ""
    line_items_agg = build_line_items_agg(order_line_items=order_line_items, dialect_name=dialect_name)
    payment_collections = sa.table(
        "payment_collections",
        sa.column("cost_center"),
        sa.column("order_number"),
    )

    normalized_order_number = sa.func.lower(sa.func.replace(sa.func.coalesce(orders.c.order_number, ""), " ", ""))

    if dialect_name == "postgresql":
        split_tokens = (
            sa.select(
                sa.func.regexp_split_to_table(
                    sa.func.coalesce(payment_collections.c.order_number, ""),
                    r"[,/]",
                ).label("token")
            )
            .lateral()
            .alias("split_tokens")
        )
        payment_proof_exists = sa.exists(
            sa.select(sa.literal(1))
            .select_from(payment_collections.join(split_tokens, sa.true()))
            .where(payment_collections.c.cost_center == orders.c.cost_center)
            .where(sa.func.length(sa.func.trim(split_tokens.c.token)) > 0)
            .where(
                sa.func.lower(sa.func.replace(sa.func.trim(split_tokens.c.token), " ", ""))
                == normalized_order_number
            )
        )
    else:
        normalized_collection_orders = sa.func.lower(
            sa.func.replace(
                sa.func.replace(sa.func.coalesce(payment_collections.c.order_number, ""), "/", ","),
                " ",
                "",
            )
        )

        payment_proof_exists = sa.exists(
            sa.select(sa.literal(1))
            .select_from(payment_collections)
            .where(payment_collections.c.cost_center == orders.c.cost_center)
            .where(
                sa.func.instr(
                    sa.literal(",") + normalized_collection_orders + sa.literal(","),
                    sa.literal(",") + normalized_order_number + sa.literal(","),
                )
                > 0
            )
        )


    stmt = (
        sa.select(
            orders.c.cost_center,
            sa.func.coalesce(store_master.c.store_code, "").label("store_code"),
            orders.c.order_number,
            orders.c.order_date,
            orders.c.customer_name,
            orders.c.mobile_number,
            orders.c.net_amount,
            line_items_agg.c.line_items,
            sa.func.max(sales.c.payment_date).label("payment_date"),
            sa.func.sum(sa.func.coalesce(sales.c.payment_received, 0)).label("payment_received"),
            string_list_agg(
                dialect_name=dialect_name,
                value_expr=sa.func.coalesce(sales.c.payment_mode, ""),
                separator=", ",
            ).label("payment_mode"),
        )
        .select_from(
            orders.join(
                sales,
                sa.and_(orders.c.cost_center == sales.c.cost_center, orders.c.order_number == sales.c.order_number),
            )
            .outerjoin(store_master, store_master.c.cost_center == orders.c.cost_center)
            .outerjoin(
                line_items_agg,
                sa.and_(orders.c.cost_center == line_items_agg.c.cost_center, orders.c.order_number == line_items_agg.c.order_number),
            )
        )
        .where(orders.c.order_date >= start_datetime)
        .where(orders.c.order_date < end_datetime)
        .where(
            same_day_date_expr(dialect_name=dialect_name, dt_expr=orders.c.order_date, timezone_name=timezone_name)
            == same_day_date_expr(dialect_name=dialect_name, dt_expr=sales.c.payment_date, timezone_name=timezone_name)
        )
        .where(~payment_proof_exists)
        .group_by(
            orders.c.cost_center,
            store_master.c.store_code,
            orders.c.order_number,
            orders.c.order_date,
            orders.c.customer_name,
            orders.c.mobile_number,
            orders.c.net_amount,
            line_items_agg.c.line_items,
        )
        .order_by(orders.c.order_date, orders.c.order_number)
    )

    result = await session.execute(stmt)
    entries = list(result.mappings())

    order_keys = {(str(entry.get("cost_center") or ""), str(entry.get("order_number") or "")) for entry in entries}
    line_items_by_order: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    if order_keys:
        keys_filter = sa.tuple_(order_line_items.c.cost_center, order_line_items.c.order_number).in_(list(order_keys))
        line_items_stmt = (
            sa.select(
                order_line_items.c.cost_center,
                order_line_items.c.order_number,
                order_line_items.c.service_name,
                order_line_items.c.garment_name,
            )
            .where(keys_filter)
            .order_by(order_line_items.c.cost_center, order_line_items.c.order_number)
        )
        line_items_result = await session.execute(line_items_stmt)
        for item in line_items_result.mappings():
            key = (str(item.get("cost_center") or ""), str(item.get("order_number") or ""))
            line_items_by_order[key].append(
                _line_item_struct(item.get("service_name"), item.get("garment_name"))
            )

    rows: list[SameDayFulfillmentRecord] = []
    for entry in entries:
        cost_center = str(entry.get("cost_center") or "")
        order_number = str(entry.get("order_number") or "")
        rows.append(
            SameDayFulfillmentRecord(
                cost_center=cost_center,
                store_code=str(entry["store_code"] or ""),
                order_number=order_number,
                order_date=coerce_datetime(entry["order_date"]),
                customer_name=str(entry["customer_name"]) if entry["customer_name"] is not None else None,
                mobile_number=str(entry["mobile_number"]) if entry["mobile_number"] is not None else None,
                line_items=str(entry["line_items"]) if entry["line_items"] is not None else None,
                line_item_rows=line_items_by_order.get((cost_center, order_number), []),
                payment_date=coerce_datetime(entry["payment_date"]),
                payment_mode=str(entry["payment_mode"]) if entry["payment_mode"] is not None else None,
                net_amount=entry["net_amount"],
                payment_received=entry["payment_received"],
            )
        )
    return rows
