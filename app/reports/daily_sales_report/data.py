from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal
from typing import Iterable, List, Mapping

import sqlalchemy as sa

from app.common.date_utils import get_timezone
from app.common.db import session_scope


@dataclass
class DailySalesRow:
    cost_center: str
    cost_center_name: str
    target_type: str
    sales_ftd: Decimal
    sales_mtd: Decimal
    sales_lmtd: Decimal
    orders_count_ftd: int
    orders_count_mtd: int
    orders_count_lmtd: int
    collections_ftd: Decimal
    collections_mtd: Decimal
    collections_lmtd: Decimal
    target: Decimal
    achieved: Decimal
    ttd: Decimal
    delta: Decimal
    reqd_per_day: Decimal
    orders_sync_time: str | None


@dataclass
class EditedOrderRow:
    cost_center: str
    order_number: str
    original_value: Decimal
    new_value: Decimal
    loss: Decimal


@dataclass
class EditedOrdersSummary:
    distinct_order_count_total: int
    store_count: int
    sum_orig_distinct: Decimal
    sum_new_distinct: Decimal
    net_loss_distinct: Decimal
    per_store_counts: List[str]


@dataclass
class DailySalesReportData:
    report_date: date
    rows: List[DailySalesRow]
    totals: DailySalesRow
    edited_orders: List[EditedOrderRow]
    edited_orders_totals: EditedOrderRow | None
    edited_orders_summary: EditedOrdersSummary | None
    missed_leads: List[Mapping[str, str]]


def _decimal(value: object | None) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _date_range(report_date: date, tz) -> dict[str, datetime]:
    start_day = datetime.combine(report_date, time.min, tzinfo=tz)
    next_day = start_day + timedelta(days=1)

    month_start = report_date.replace(day=1)
    start_month = datetime.combine(month_start, time.min, tzinfo=tz)

    prev_month_end = month_start - timedelta(days=1)
    prev_month_start = prev_month_end.replace(day=1)
    day_target = report_date.day
    last_prev_day = prev_month_end.day
    lmt_end_day = day_target if day_target <= last_prev_day else last_prev_day
    lmt_end_date = prev_month_end.replace(day=lmt_end_day)

    lmt_start = datetime.combine(prev_month_start, time.min, tzinfo=tz)
    lmt_end = datetime.combine(lmt_end_date, time.min, tzinfo=tz) + timedelta(days=1)

    return {
        "start_day": start_day,
        "next_day": next_day,
        "start_month": start_month,
        "lmt_start": lmt_start,
        "lmt_end": lmt_end,
    }


def _remaining_days(report_date: date) -> int:
    next_month = report_date.replace(day=1) + timedelta(days=32)
    last_day = next_month.replace(day=1) - timedelta(days=1)
    return max(0, (last_day - report_date).days)


def _days_in_month(report_date: date) -> int:
    next_month = report_date.replace(day=1) + timedelta(days=32)
    last_day = next_month.replace(day=1) - timedelta(days=1)
    return last_day.day


def _round_amount(value: Decimal) -> Decimal:
    return value.to_integral_value(rounding=ROUND_HALF_UP)


def _truncate_amount(value: Decimal) -> Decimal:
    return value.to_integral_value(rounding=ROUND_DOWN)


def _calculate_ttd(target: Decimal, achieved: Decimal, day_of_month: int, days_in_month: int) -> Decimal:
    if days_in_month <= 0:
        return Decimal("0")
    expected_mtd = _truncate_amount((target / Decimal(str(days_in_month))) * Decimal(str(day_of_month)))
    return _round_amount(achieved - expected_mtd)


def _build_orders_agg(orders: sa.Table, ranges: dict[str, datetime]) -> sa.Subquery:
    def _sum_when(condition: sa.ColumnElement[bool]) -> sa.ColumnElement:
        return sa.func.coalesce(sa.func.sum(sa.case((condition, orders.c.net_amount), else_=0)), 0)

    return (
        sa.select(
            orders.c.cost_center.label("cost_center"),
            _sum_when(sa.and_(orders.c.order_date >= ranges["start_day"], orders.c.order_date < ranges["next_day"]))
            .label("sales_ftd"),
            _sum_when(sa.and_(orders.c.order_date >= ranges["start_month"], orders.c.order_date < ranges["next_day"]))
            .label("sales_mtd"),
            _sum_when(sa.and_(orders.c.order_date >= ranges["lmt_start"], orders.c.order_date < ranges["lmt_end"]))
            .label("sales_lmtd"),
        )
        .group_by(orders.c.cost_center)
        .subquery()
    )


def _build_orders_count_agg(orders: sa.Table, ranges: dict[str, datetime]) -> sa.Subquery:
    def _count_when(condition: sa.ColumnElement[bool]) -> sa.ColumnElement:
        return sa.func.coalesce(sa.func.sum(sa.case((condition, 1), else_=0)), 0)

    return (
        sa.select(
            orders.c.cost_center.label("cost_center"),
            _count_when(sa.and_(orders.c.order_date >= ranges["start_day"], orders.c.order_date < ranges["next_day"]))
            .label("orders_count_ftd"),
            _count_when(sa.and_(orders.c.order_date >= ranges["start_month"], orders.c.order_date < ranges["next_day"]))
            .label("orders_count_mtd"),
            _count_when(sa.and_(orders.c.order_date >= ranges["lmt_start"], orders.c.order_date < ranges["lmt_end"]))
            .label("orders_count_lmtd"),
        )
        .group_by(orders.c.cost_center)
        .subquery()
    )


def _build_orders_sync_agg(orders_sync_log: sa.Table) -> sa.Subquery:
    return (
        sa.select(
            orders_sync_log.c.cost_center.label("cost_center"),
            sa.func.max(orders_sync_log.c.orders_pulled_at).label("orders_pulled_at"),
        )
        .group_by(orders_sync_log.c.cost_center)
        .subquery()
    )


def _build_sales_agg(sales: sa.Table, ranges: dict[str, datetime]) -> sa.Subquery:
    def _sum_when(condition: sa.ColumnElement[bool]) -> sa.ColumnElement:
        return sa.func.coalesce(sa.func.sum(sa.case((condition, sales.c.payment_received), else_=0)), 0)

    return (
        sa.select(
            sales.c.cost_center.label("cost_center"),
            _sum_when(sa.and_(sales.c.payment_date >= ranges["start_day"], sales.c.payment_date < ranges["next_day"]))
            .label("collections_ftd"),
            _sum_when(sa.and_(sales.c.payment_date >= ranges["start_month"], sales.c.payment_date < ranges["next_day"]))
            .label("collections_mtd"),
            _sum_when(sa.and_(sales.c.payment_date >= ranges["lmt_start"], sales.c.payment_date < ranges["lmt_end"]))
            .label("collections_lmtd"),
        )
        .group_by(sales.c.cost_center)
        .subquery()
    )


def _totals_row(rows: Iterable[DailySalesRow]) -> DailySalesRow:
    totals = DailySalesRow(
        cost_center="TOTAL",
        cost_center_name="Total",
        target_type="value",
        sales_ftd=Decimal("0"),
        sales_mtd=Decimal("0"),
        sales_lmtd=Decimal("0"),
        orders_count_ftd=0,
        orders_count_mtd=0,
        orders_count_lmtd=0,
        collections_ftd=Decimal("0"),
        collections_mtd=Decimal("0"),
        collections_lmtd=Decimal("0"),
        target=Decimal("0"),
        achieved=Decimal("0"),
        ttd=Decimal("0"),
        delta=Decimal("0"),
        reqd_per_day=Decimal("0"),
        orders_sync_time=None,
    )
    for row in rows:
        totals.sales_ftd += row.sales_ftd
        totals.sales_mtd += row.sales_mtd
        totals.sales_lmtd += row.sales_lmtd
        totals.orders_count_ftd += row.orders_count_ftd
        totals.orders_count_mtd += row.orders_count_mtd
        totals.orders_count_lmtd += row.orders_count_lmtd
        totals.collections_ftd += row.collections_ftd
        totals.collections_mtd += row.collections_mtd
        totals.collections_lmtd += row.collections_lmtd
        totals.target += row.target
        totals.achieved += row.achieved
        totals.delta += row.delta
        totals.reqd_per_day += row.reqd_per_day
    return totals


def _edited_totals(rows: Iterable[EditedOrderRow]) -> EditedOrderRow | None:
    rows = list(rows)
    if not rows:
        return None
    totals = EditedOrderRow(
        cost_center="Total",
        order_number="",
        original_value=Decimal("0"),
        new_value=Decimal("0"),
        loss=Decimal("0"),
    )
    for row in rows:
        totals.original_value += row.original_value
        totals.new_value += row.new_value
        totals.loss += row.loss
    return totals


async def fetch_daily_sales_report(
    *, database_url: str, report_date: date
) -> DailySalesReportData:
    tz = get_timezone()
    ranges = _date_range(report_date, tz)
    remaining_days = _remaining_days(report_date)
    day_of_month = report_date.day
    days_in_month = _days_in_month(report_date)

    cost_center = sa.table(
        "cost_center",
        sa.column("cost_center"),
        sa.column("description"),
        sa.column("target_type"),
        sa.column("is_active"),
    )
    targets = sa.table(
        "cost_center_targets",
        sa.column("month"),
        sa.column("year"),
        sa.column("cost_center"),
        sa.column("sale_target"),
    )
    orders = sa.table(
        "orders",
        sa.column("cost_center"),
        sa.column("order_number"),
        sa.column("order_date"),
        sa.column("net_amount"),
    )
    orders_sync_log = sa.table(
        "orders_sync_log",
        sa.column("cost_center"),
        sa.column("orders_pulled_at"),
    )
    sales = sa.table(
        "sales",
        sa.column("cost_center"),
        sa.column("payment_date"),
        sa.column("payment_received"),
        sa.column("adjustments"),
        sa.column("order_number"),
        sa.column("is_edited_order"),
    )

    orders_agg = _build_orders_agg(orders, ranges)
    orders_count_agg = _build_orders_count_agg(orders, ranges)
    sales_agg = _build_sales_agg(sales, ranges)
    orders_sync_agg = _build_orders_sync_agg(orders_sync_log)

    stmt = (
        sa.select(
            cost_center.c.cost_center,
            cost_center.c.description,
            cost_center.c.target_type,
            orders_agg.c.sales_ftd,
            orders_agg.c.sales_mtd,
            orders_agg.c.sales_lmtd,
            orders_count_agg.c.orders_count_ftd,
            orders_count_agg.c.orders_count_mtd,
            orders_count_agg.c.orders_count_lmtd,
            sales_agg.c.collections_ftd,
            sales_agg.c.collections_mtd,
            sales_agg.c.collections_lmtd,
            targets.c.sale_target,
            orders_sync_agg.c.orders_pulled_at,
        )
        .select_from(
            cost_center
            .outerjoin(orders_agg, orders_agg.c.cost_center == cost_center.c.cost_center)
            .outerjoin(orders_count_agg, orders_count_agg.c.cost_center == cost_center.c.cost_center)
            .outerjoin(sales_agg, sales_agg.c.cost_center == cost_center.c.cost_center)
            .outerjoin(orders_sync_agg, orders_sync_agg.c.cost_center == cost_center.c.cost_center)
            .outerjoin(
                targets,
                sa.and_(
                    targets.c.cost_center == cost_center.c.cost_center,
                    targets.c.month == report_date.month,
                    targets.c.year == report_date.year,
                ),
            )
        )
        .where(cost_center.c.is_active.is_(True))
        .order_by(cost_center.c.description)
    )

    rows: list[DailySalesRow] = []
    async with session_scope(database_url) as session:
        result = await session.execute(stmt)
        for entry in result.mappings():
            target_type = (entry["target_type"] or "value").lower()
            sales_ftd = _decimal(entry["sales_ftd"])
            sales_mtd = _decimal(entry["sales_mtd"])
            sales_lmtd = _decimal(entry["sales_lmtd"])
            orders_count_ftd = int(entry["orders_count_ftd"] or 0)
            orders_count_mtd = int(entry["orders_count_mtd"] or 0)
            orders_count_lmtd = int(entry["orders_count_lmtd"] or 0)
            collections_ftd = _decimal(entry["collections_ftd"])
            collections_mtd = _decimal(entry["collections_mtd"])
            collections_lmtd = _decimal(entry["collections_lmtd"])
            target = _decimal(entry["sale_target"])
            achieved = sales_mtd
            if target_type == "none":
                target = Decimal("0")
                achieved = Decimal("0")
            ttd = _calculate_ttd(target, achieved, day_of_month, days_in_month)
            delta = achieved - target
            reqd_per_day = Decimal("0")
            if target_type == "none":
                delta = Decimal("0")
            elif remaining_days:
                reqd_per_day = abs(delta) / Decimal(str(remaining_days))

            orders_pulled_at = entry["orders_pulled_at"]
            orders_sync_time = None
            if orders_pulled_at:
                orders_sync_time = orders_pulled_at.astimezone(tz).strftime("%H:%M")

            rows.append(
                DailySalesRow(
                    cost_center=str(entry["cost_center"]),
                    cost_center_name=str(entry["description"]),
                    target_type=target_type,
                    sales_ftd=sales_ftd,
                    sales_mtd=sales_mtd,
                    sales_lmtd=sales_lmtd,
                    orders_count_ftd=orders_count_ftd,
                    orders_count_mtd=orders_count_mtd,
                    orders_count_lmtd=orders_count_lmtd,
                    collections_ftd=collections_ftd,
                    collections_mtd=collections_mtd,
                    collections_lmtd=collections_lmtd,
                    target=target,
                    achieved=achieved,
                    ttd=ttd,
                    delta=delta,
                    reqd_per_day=reqd_per_day,
                    orders_sync_time=orders_sync_time,
                )
            )

        edited_start = datetime.combine(report_date - timedelta(days=1), time.min, tzinfo=tz)
        edited_end = datetime.combine(report_date + timedelta(days=1), time.min, tzinfo=tz)
        edited_stmt = (
            sa.select(
                sales.c.cost_center,
                sales.c.order_number,
                sales.c.payment_received,
                orders.c.net_amount,
            )
            .select_from(
                sales.outerjoin(
                    orders,
                    sa.and_(
                        orders.c.order_number == sales.c.order_number,
                        orders.c.cost_center == sales.c.cost_center,
                    ),
                )
            )
            .where(sales.c.is_edited_order.is_(True))
            .where(sales.c.payment_date >= edited_start)
            .where(sales.c.payment_date < edited_end)
            .order_by(sales.c.cost_center, sales.c.order_number)
        )
        edited_rows: list[EditedOrderRow] = []
        edited_result = await session.execute(edited_stmt)
        for entry in edited_result.mappings():
            payment_received = _decimal(entry["payment_received"])
            net_amount = _decimal(entry["net_amount"])
            original_value = net_amount
            new_value = payment_received
            loss = net_amount - payment_received
            edited_rows.append(
                EditedOrderRow(
                    cost_center=str(entry["cost_center"]),
                    order_number=str(entry["order_number"]),
                    original_value=original_value,
                    new_value=new_value,
                    loss=loss,
                )
            )

    totals = _totals_row(rows)
    totals.ttd = _calculate_ttd(totals.target, totals.achieved, day_of_month, days_in_month)
    edited_totals = _edited_totals(edited_rows)
    edited_orders_summary = None
    if edited_rows:
        distinct_map: dict[tuple[str, str], dict[str, Decimal]] = {}
        for row in edited_rows:
            key = (row.cost_center, row.order_number)
            if key not in distinct_map:
                distinct_map[key] = {
                    "orig_value": row.original_value,
                    "new_value": row.new_value,
                }
                continue
            distinct_map[key]["orig_value"] = max(distinct_map[key]["orig_value"], row.original_value)
            distinct_map[key]["new_value"] = min(distinct_map[key]["new_value"], row.new_value)

        per_store_counts: dict[str, int] = {}
        sum_orig_distinct = Decimal("0")
        sum_new_distinct = Decimal("0")
        net_loss_distinct = Decimal("0")
        for (cost_center, _order_number), values in distinct_map.items():
            per_store_counts[cost_center] = per_store_counts.get(cost_center, 0) + 1
            sum_orig_distinct += values["orig_value"]
            sum_new_distinct += values["new_value"]
            net_loss_distinct += values["orig_value"] - values["new_value"]

        edited_orders_summary = EditedOrdersSummary(
            distinct_order_count_total=len(distinct_map),
            store_count=len(per_store_counts),
            sum_orig_distinct=sum_orig_distinct,
            sum_new_distinct=sum_new_distinct,
            net_loss_distinct=net_loss_distinct,
            per_store_counts=[f"{store}: {count}" for store, count in sorted(per_store_counts.items())],
        )

    return DailySalesReportData(
        report_date=report_date,
        rows=rows,
        totals=totals,
        edited_orders=edited_rows,
        edited_orders_totals=edited_totals,
        edited_orders_summary=edited_orders_summary,
        missed_leads=[],
    )
