from __future__ import annotations

from datetime import datetime
from typing import Any

import sqlalchemy as sa


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
