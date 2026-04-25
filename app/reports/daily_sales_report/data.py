from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal
from typing import Iterable, List, Mapping

import sqlalchemy as sa

from app.common.date_utils import get_timezone
from app.common.db import session_scope
from app.common.lead_rules import resolve_cancelled_flag


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
    collections_count_ftd: int
    collections_count_mtd: int
    collections_count_lmtd: int
    target: Decimal
    achieved: Decimal
    ttd: Decimal
    delta: Decimal
    reqd_per_day: Decimal
    orders_sync_time: str | None
    pickup_new_conv_pct: Decimal | None
    pickup_existing_conv_pct: Decimal | None
    pickup_total_count: int | None
    pickup_total_conv_pct: Decimal | None
    delivery_tat_pct: Decimal | None
    kpi_snapshot_label: str


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
class RecoveryAgingSplit:
    label: str
    order_count: int
    total_amount_at_risk: Decimal


@dataclass
class RecoverySectionRow:
    cost_center: str
    cost_center_name: str
    order_count: int
    total_amount_at_risk: Decimal
    aging_split: List[RecoveryAgingSplit]


@dataclass
class RecoveryLifecycleRow:
    cost_center: str
    cost_center_name: str
    order_number: str
    recovery_status: str
    recovery_opened_at: date | None
    recovery_closed_at: date | None
    recovery_expected_resolution_date: date | None
    open_age_days: int
    is_overdue: bool
    closure_turnaround_days: int | None


@dataclass
class RecoveryBacklogTotals:
    opening_backlog: int = 0
    newly_tagged: int = 0
    closed_today: int = 0
    net_backlog_movement: int = 0


@dataclass
class DailySalesReportData:
    report_date: date
    rows: List[DailySalesRow]
    totals: DailySalesRow
    edited_orders: List[EditedOrderRow]
    edited_orders_totals: EditedOrderRow | None
    edited_orders_summary: EditedOrdersSummary | None
    missed_leads: List[Mapping[str, object]]
    cancelled_leads: List[Mapping[str, object]]
    lead_performance_summary: List[Mapping[str, object]]
    td_leads_sync_metrics: Mapping[str, object]
    td_leads_sync_lead_changes: Mapping[str, object]
    to_be_recovered: List[RecoverySectionRow] = field(default_factory=list)
    to_be_compensated: List[RecoverySectionRow] = field(default_factory=list)
    recovery_lifecycle_closed: List[RecoveryLifecycleRow] = field(default_factory=list)
    recovery_backlog_totals: RecoveryBacklogTotals = field(default_factory=RecoveryBacklogTotals)


LEAD_BENCHMARKS = {
    "conversion_target": Decimal("85"),
    "conversion_min": Decimal("70"),
    "cancelled_target": Decimal("10"),
    "cancelled_max": Decimal("20"),
    "pending_max": Decimal("5"),
}

MANUAL_RECOVERY_STATUSES = ("TO_BE_RECOVERED", "TO_BE_COMPENSATED")
RECOVERY_CLOSED_STATUSES = ("RECOVERED", "COMPENSATED", "WRITE_OFF")
MANUAL_RECOVERY_AGING_BUCKETS = ("0-30", "31-60", "61-90", ">90")


def _decimal(value: object | None) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _to_local_date(value: object | None, tz) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    parsed_dt = _parse_orders_sync_timestamp(value, tz=tz)
    if parsed_dt is None:
        return None
    return parsed_dt.date()


def _manual_recovery_age_bucket(age_days: int) -> str:
    if age_days <= 30:
        return "0-30"
    if age_days <= 60:
        return "31-60"
    if age_days <= 90:
        return "61-90"
    return ">90"


def _new_recovery_aging_split() -> dict[str, RecoveryAgingSplit]:
    return {
        bucket: RecoveryAgingSplit(
            label=bucket,
            order_count=0,
            total_amount_at_risk=Decimal("0"),
        )
        for bucket in MANUAL_RECOVERY_AGING_BUCKETS
    }


def _build_manual_recovery_sections(
    records: Iterable[Mapping[str, object]],
    *,
    report_date: date,
    tz,
) -> tuple[
    list[RecoverySectionRow],
    list[RecoverySectionRow],
    list[RecoveryLifecycleRow],
    RecoveryBacklogTotals,
]:
    grouped: dict[str, dict[str, object]] = {}
    closed_lifecycle_rows: list[RecoveryLifecycleRow] = []
    opening_backlog = 0
    newly_tagged = 0
    closed_today = 0

    for record in records:
        recovery_status = str(record.get("recovery_status") or "").upper()
        if recovery_status not in MANUAL_RECOVERY_STATUSES and recovery_status not in RECOVERY_CLOSED_STATUSES:
            continue

        recovery_opened_at = _to_local_date(record.get("recovery_opened_at"), tz)
        recovery_closed_at = _to_local_date(record.get("recovery_closed_at"), tz)
        recovery_expected_resolution_date = _to_local_date(record.get("recovery_expected_resolution_date"), tz)

        if recovery_status in MANUAL_RECOVERY_STATUSES:
            if recovery_opened_at is not None and recovery_opened_at < report_date:
                opening_backlog += 1
            if recovery_opened_at == report_date:
                newly_tagged += 1
        if recovery_status in RECOVERY_CLOSED_STATUSES and recovery_closed_at == report_date:
            closed_today += 1

        cost_center = str(record.get("cost_center") or "")
        cost_center_name = str(record.get("description") or cost_center)

        source_system = str(record.get("source_system") or "")
        net_amount = _decimal(record.get("net_amount"))
        gross_amount = _decimal(record.get("gross_amount"))
        amount_at_risk = net_amount if source_system == "TumbleDry" else gross_amount
        if amount_at_risk < 0:
            amount_at_risk = Decimal("0")

        default_due_date = _to_local_date(record.get("default_due_date"), tz)
        order_date = _to_local_date(record.get("order_date"), tz)
        age_anchor_date = recovery_opened_at or default_due_date or order_date or report_date
        age_days = max(0, (report_date - age_anchor_date).days)
        is_overdue = (
            recovery_expected_resolution_date is not None
            and recovery_expected_resolution_date < report_date
            and recovery_status in MANUAL_RECOVERY_STATUSES
        )

        if recovery_status in MANUAL_RECOVERY_STATUSES:
            group = grouped.setdefault(
                f"{recovery_status}:{cost_center}",
                {
                    "status": recovery_status,
                    "cost_center": cost_center,
                    "cost_center_name": cost_center_name,
                    "order_count": 0,
                    "total_amount_at_risk": Decimal("0"),
                    "aging_split": _new_recovery_aging_split(),
                },
            )
            bucket = _manual_recovery_age_bucket(age_days)
            aging_split = group["aging_split"][bucket]
            aging_split.order_count += 1
            aging_split.total_amount_at_risk += amount_at_risk

            group["order_count"] += 1
            group["total_amount_at_risk"] += amount_at_risk
            continue

        closure_turnaround_days: int | None = None
        if recovery_opened_at is not None and recovery_closed_at is not None:
            closure_turnaround_days = max(0, (recovery_closed_at - recovery_opened_at).days)
        closed_lifecycle_rows.append(
            RecoveryLifecycleRow(
                cost_center=cost_center,
                cost_center_name=cost_center_name,
                order_number=str(record.get("order_number") or ""),
                recovery_status=recovery_status,
                recovery_opened_at=recovery_opened_at,
                recovery_closed_at=recovery_closed_at,
                recovery_expected_resolution_date=recovery_expected_resolution_date,
                open_age_days=age_days,
                is_overdue=is_overdue,
                closure_turnaround_days=closure_turnaround_days,
            )
        )

    recovered: list[RecoverySectionRow] = []
    compensated: list[RecoverySectionRow] = []

    for key in sorted(grouped.keys(), key=lambda value: (value.split(":", 1)[0], value.split(":", 1)[1])):
        item = grouped[key]
        section_row = RecoverySectionRow(
            cost_center=str(item["cost_center"]),
            cost_center_name=str(item["cost_center_name"]),
            order_count=int(item["order_count"]),
            total_amount_at_risk=item["total_amount_at_risk"],
            aging_split=[item["aging_split"][bucket] for bucket in MANUAL_RECOVERY_AGING_BUCKETS],
        )
        if item["status"] == "TO_BE_RECOVERED":
            recovered.append(section_row)
        else:
            compensated.append(section_row)

    closed_lifecycle_rows.sort(key=lambda row: (row.cost_center, row.recovery_closed_at or report_date, row.order_number))

    backlog_totals = RecoveryBacklogTotals(
        opening_backlog=opening_backlog,
        newly_tagged=newly_tagged,
        closed_today=closed_today,
        net_backlog_movement=newly_tagged - closed_today,
    )
    return recovered, compensated, closed_lifecycle_rows, backlog_totals


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


def _round_percentage(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _lead_metric_status_color(
    *, metric: str, value: Decimal, total_leads: int
) -> tuple[str, str]:
    if total_leads <= 0:
        return ("NEUTRAL", "NEUTRAL")

    if metric == "conversion":
        if value >= LEAD_BENCHMARKS["conversion_target"]:
            return ("EXCELLENT", "GREEN")
        if value >= LEAD_BENCHMARKS["conversion_min"]:
            return ("HEALTHY", "YELLOW")
        return ("POOR", "RED")

    if metric == "cancelled":
        if value <= LEAD_BENCHMARKS["cancelled_target"]:
            return ("EXCELLENT", "GREEN")
        if value <= LEAD_BENCHMARKS["cancelled_max"]:
            return ("ACCEPTABLE", "YELLOW")
        return ("HIGH_LEAKAGE", "RED")

    if metric == "pending":
        if value <= LEAD_BENCHMARKS["pending_max"]:
            return ("CONTROLLED", "GREEN")
        return ("FOLLOW_UP_GAP", "RED")

    return ("NEUTRAL", "NEUTRAL")


def _lead_metric_payload(*, metric: str, value: Decimal, total_leads: int) -> dict[str, object]:
    status, color = _lead_metric_status_color(metric=metric, value=value, total_leads=total_leads)
    return {
        "value": float(value),
        "color": color,
        "status": status,
    }


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
    sync_ts = sa.func.coalesce(
        orders_sync_log.c.orders_pulled_at,
        orders_sync_log.c.updated_at,
        orders_sync_log.c.created_at,
    )
    return (
        sa.select(
            orders_sync_log.c.cost_center.label("cost_center"),
            sa.func.max(sync_ts).label("orders_pulled_at"),
        )
        .group_by(orders_sync_log.c.cost_center)
        .subquery()
    )


def _parse_orders_sync_timestamp(value: object | None, *, tz) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    return dt.astimezone(tz)


def _build_sales_agg(sales: sa.Table, ranges: dict[str, datetime]) -> sa.Subquery:
    normalized_order_number = sa.func.upper(sa.func.trim(sales.c.order_number))
    valid_order_number = sa.and_(
        sales.c.order_number.is_not(None),
        normalized_order_number != "",
    )
    ftd_condition = sa.and_(sales.c.payment_date >= ranges["start_day"], sales.c.payment_date < ranges["next_day"])
    mtd_condition = sa.and_(sales.c.payment_date >= ranges["start_month"], sales.c.payment_date < ranges["next_day"])
    lmtd_condition = sa.and_(sales.c.payment_date >= ranges["lmt_start"], sales.c.payment_date < ranges["lmt_end"])

    periodized_sales = sa.union_all(
        sa.select(
            sales.c.cost_center.label("cost_center"),
            normalized_order_number.label("order_number"),
            sa.literal("ftd").label("period_bucket"),
            sales.c.payment_received.label("payment_received"),
        ).where(sa.and_(valid_order_number, ftd_condition)),
        sa.select(
            sales.c.cost_center.label("cost_center"),
            normalized_order_number.label("order_number"),
            sa.literal("mtd").label("period_bucket"),
            sales.c.payment_received.label("payment_received"),
        ).where(sa.and_(valid_order_number, mtd_condition)),
        sa.select(
            sales.c.cost_center.label("cost_center"),
            normalized_order_number.label("order_number"),
            sa.literal("lmtd").label("period_bucket"),
            sales.c.payment_received.label("payment_received"),
        ).where(sa.and_(valid_order_number, lmtd_condition)),
    ).subquery()

    sales_per_order = (
        sa.select(
            periodized_sales.c.cost_center,
            periodized_sales.c.order_number,
            periodized_sales.c.period_bucket,
            sa.func.coalesce(sa.func.sum(periodized_sales.c.payment_received), 0).label("order_amount"),
        )
        .group_by(
            periodized_sales.c.cost_center,
            periodized_sales.c.order_number,
            periodized_sales.c.period_bucket,
        )
        .subquery()
    )

    def _sum_when_period(period_bucket: str) -> sa.ColumnElement:
        return sa.func.coalesce(
            sa.func.sum(sa.case((sales_per_order.c.period_bucket == period_bucket, sales_per_order.c.order_amount), else_=0)),
            0,
        )

    def _count_when_period(period_bucket: str) -> sa.ColumnElement:
        return sa.func.coalesce(
            sa.func.sum(sa.case((sales_per_order.c.period_bucket == period_bucket, 1), else_=0)),
            0,
        )

    return (
        sa.select(
            sales_per_order.c.cost_center.label("cost_center"),
            _sum_when_period("ftd").label("collections_ftd"),
            _sum_when_period("mtd").label("collections_mtd"),
            _sum_when_period("lmtd").label("collections_lmtd"),
            _count_when_period("ftd").label("collections_count_ftd"),
            _count_when_period("mtd").label("collections_count_mtd"),
            _count_when_period("lmtd").label("collections_count_lmtd"),
        )
        .group_by(sales_per_order.c.cost_center)
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
        collections_count_ftd=0,
        collections_count_mtd=0,
        collections_count_lmtd=0,
        target=Decimal("0"),
        achieved=Decimal("0"),
        ttd=Decimal("0"),
        delta=Decimal("0"),
        reqd_per_day=Decimal("0"),
        orders_sync_time=None,
        pickup_new_conv_pct=None,
        pickup_existing_conv_pct=None,
        pickup_total_count=None,
        pickup_total_conv_pct=None,
        delivery_tat_pct=None,
        kpi_snapshot_label="--",
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
        totals.collections_count_ftd += row.collections_count_ftd
        totals.collections_count_mtd += row.collections_count_mtd
        totals.collections_count_lmtd += row.collections_count_lmtd
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
    edited_ranges = _date_range(report_date - timedelta(days=1), tz)
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
        sa.column("collection_target"),
        sa.column("sales_mtd"),
        sa.column("collection_mtd"),
        sa.column("sales_target_met"),
        sa.column("collection_target_met"),
    )
    orders = sa.table(
        "orders",
        sa.column("cost_center"),
        sa.column("order_number"),
        sa.column("order_date"),
        sa.column("net_amount"),
        sa.column("gross_amount"),
        sa.column("default_due_date"),
        sa.column("source_system"),
        sa.column("recovery_status"),
        sa.column("recovery_opened_at"),
        sa.column("recovery_closed_at"),
        sa.column("recovery_expected_resolution_date"),
    )
    orders_sync_log = sa.table(
        "orders_sync_log",
        sa.column("cost_center"),
        sa.column("orders_pulled_at"),
        sa.column("updated_at"),
        sa.column("created_at"),
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
    store_master = sa.table(
        "store_master",
        sa.column("id"),
        sa.column("cost_center"),
        sa.column("store_code"),
        sa.column("store_name"),
        sa.column("sync_group"),
    )
    store_dashboard_summary = sa.table(
        "store_dashboard_summary",
        sa.column("store_id"),
        sa.column("dashboard_date"),
        sa.column("run_date_time"),
        sa.column("pickup_new_conv_pct"),
        sa.column("pickup_existing_conv_pct"),
        sa.column("pickup_total_count"),
        sa.column("pickup_total_conv_pct"),
        sa.column("delivery_tat_pct"),
    )
    missed_leads = sa.table(
        "missed_leads",
        sa.column("store_code"),
        sa.column("mobile_number"),
        sa.column("customer_name"),
        sa.column("customer_type"),
        sa.column("pickup_date"),
        sa.column("is_order_placed"),
    )
    crm_leads_current = sa.table(
        "crm_leads_current",
        sa.column("lead_uid"),
        sa.column("store_code"),
        sa.column("status_bucket"),
        sa.column("customer_name"),
        sa.column("mobile"),
        sa.column("pickup_created_at"),
        sa.column("reason"),
        sa.column("cancelled_flag"),
    )
    crm_leads_status_events = sa.table(
        "crm_leads_status_events",
        sa.column("lead_uid"),
        sa.column("status_bucket"),
    )
    previous_day = report_date - timedelta(days=1)

    store_master_candidates = (
        sa.select(
            store_master.c.id.label("id"),
            store_master.c.cost_center.label("cost_center"),
            store_master.c.store_code.label("store_code"),
            store_master.c.store_name.label("store_name"),
            store_master.c.sync_group.label("sync_group"),
            sa.func.row_number()
            .over(
                partition_by=store_master.c.cost_center,
                order_by=store_master.c.id.asc(),
            )
            .label("store_row_number"),
        )
        .subquery()
    )

    store_master_primary = (
        sa.select(
            store_master_candidates.c.id,
            store_master_candidates.c.cost_center,
            store_master_candidates.c.store_code,
            store_master_candidates.c.store_name,
            store_master_candidates.c.sync_group,
        )
        .where(store_master_candidates.c.store_row_number == 1)
        .subquery()
    )

    summary_candidates = (
        sa.select(
            store_dashboard_summary.c.store_id.label("store_id"),
            store_dashboard_summary.c.dashboard_date.label("dashboard_date"),
            store_dashboard_summary.c.pickup_new_conv_pct.label("pickup_new_conv_pct"),
            store_dashboard_summary.c.pickup_existing_conv_pct.label("pickup_existing_conv_pct"),
            store_dashboard_summary.c.pickup_total_count.label("pickup_total_count"),
            store_dashboard_summary.c.pickup_total_conv_pct.label("pickup_total_conv_pct"),
            store_dashboard_summary.c.delivery_tat_pct.label("delivery_tat_pct"),
            sa.func.row_number()
            .over(
                partition_by=store_dashboard_summary.c.store_id,
                order_by=(
                    store_dashboard_summary.c.dashboard_date.desc(),
                    store_dashboard_summary.c.run_date_time.desc(),
                ),
            )
            .label("summary_row_number"),
        )
        .where(store_dashboard_summary.c.dashboard_date.in_([report_date, previous_day]))
        .subquery()
    )

    selected_kpi = (
        sa.select(
            summary_candidates.c.store_id,
            summary_candidates.c.dashboard_date,
            summary_candidates.c.pickup_new_conv_pct,
            summary_candidates.c.pickup_existing_conv_pct,
            summary_candidates.c.pickup_total_count,
            summary_candidates.c.pickup_total_conv_pct,
            summary_candidates.c.delivery_tat_pct,
        )
        .where(summary_candidates.c.summary_row_number == 1)
        .subquery()
    )

    orders_agg = _build_orders_agg(orders, ranges)
    orders_count_agg = _build_orders_count_agg(orders, ranges)
    sales_agg = _build_sales_agg(sales, ranges)
    orders_sync_agg = _build_orders_sync_agg(orders_sync_log)

    stmt = (
        sa.select(
            cost_center.c.cost_center,
            sa.func.coalesce(store_master_primary.c.store_name, cost_center.c.description).label("description"),
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
            sales_agg.c.collections_count_ftd,
            sales_agg.c.collections_count_mtd,
            sales_agg.c.collections_count_lmtd,
            targets.c.sale_target,
            targets.c.collection_target,
            orders_sync_agg.c.orders_pulled_at,
            sa.case(
                (
                    store_master_primary.c.sync_group == "TD",
                    sa.case(
                        (selected_kpi.c.dashboard_date == report_date, sa.literal("D")),
                        (selected_kpi.c.dashboard_date == previous_day, sa.literal("D-1")),
                        else_=sa.literal("--"),
                    ),
                ),
                else_=None,
            ).label("kpi_snapshot_label"),
            sa.case(
                (store_master_primary.c.sync_group == "TD", selected_kpi.c.pickup_new_conv_pct),
                else_=None,
            ).label("pickup_new_conv_pct"),
            sa.case(
                (store_master_primary.c.sync_group == "TD", selected_kpi.c.pickup_existing_conv_pct),
                else_=None,
            ).label("pickup_existing_conv_pct"),
            sa.case(
                (store_master_primary.c.sync_group == "TD", selected_kpi.c.pickup_total_count),
                else_=None,
            ).label("pickup_total_count"),
            sa.case(
                (store_master_primary.c.sync_group == "TD", selected_kpi.c.pickup_total_conv_pct),
                else_=None,
            ).label("pickup_total_conv_pct"),
            sa.case(
                (store_master_primary.c.sync_group == "TD", selected_kpi.c.delivery_tat_pct),
                else_=None,
            ).label("delivery_tat_pct"),
        )
        .select_from(
            cost_center
            .outerjoin(store_master_primary, store_master_primary.c.cost_center == cost_center.c.cost_center)
            .outerjoin(orders_agg, orders_agg.c.cost_center == cost_center.c.cost_center)
            .outerjoin(orders_count_agg, orders_count_agg.c.cost_center == cost_center.c.cost_center)
            .outerjoin(sales_agg, sales_agg.c.cost_center == cost_center.c.cost_center)
            .outerjoin(orders_sync_agg, orders_sync_agg.c.cost_center == cost_center.c.cost_center)
            .outerjoin(selected_kpi, selected_kpi.c.store_id == store_master_primary.c.id)
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
        target_updates: list[dict[str, object]] = []
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
            collections_count_ftd = int(entry["collections_count_ftd"] or 0)
            collections_count_mtd = int(entry["collections_count_mtd"] or 0)
            collections_count_lmtd = int(entry["collections_count_lmtd"] or 0)
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

            orders_pulled_at = _parse_orders_sync_timestamp(entry["orders_pulled_at"], tz=tz)
            orders_sync_time = orders_pulled_at.strftime("%H:%M") if orders_pulled_at else None

            sale_target = _decimal(entry["sale_target"]) if entry["sale_target"] is not None else None
            collection_target = (
                _decimal(entry["collection_target"]) if entry["collection_target"] is not None else None
            )
            sales_target_met = None if sale_target is None else bool(sales_mtd >= sale_target)
            collection_target_met = (
                None if collection_target is None else bool(collections_mtd >= collection_target)
            )
            target_updates.append(
                {
                    "b_month": report_date.month,
                    "b_year": report_date.year,
                    "b_cost_center": str(entry["cost_center"]),
                    "sales_mtd": str(sales_mtd),
                    "collection_mtd": str(collections_mtd),
                    "sales_target_met": sales_target_met,
                    "collection_target_met": collection_target_met,
                }
            )

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
                    collections_count_ftd=collections_count_ftd,
                    collections_count_mtd=collections_count_mtd,
                    collections_count_lmtd=collections_count_lmtd,
                    target=target,
                    achieved=achieved,
                    ttd=ttd,
                    delta=delta,
                    reqd_per_day=reqd_per_day,
                    orders_sync_time=orders_sync_time,
                    pickup_new_conv_pct=_decimal(entry["pickup_new_conv_pct"]) if entry["pickup_new_conv_pct"] is not None else None,
                    pickup_existing_conv_pct=_decimal(entry["pickup_existing_conv_pct"]) if entry["pickup_existing_conv_pct"] is not None else None,
                    pickup_total_count=int(entry["pickup_total_count"]) if entry["pickup_total_count"] is not None else None,
                    pickup_total_conv_pct=_decimal(entry["pickup_total_conv_pct"]) if entry["pickup_total_conv_pct"] is not None else None,
                    delivery_tat_pct=_decimal(entry["delivery_tat_pct"]) if entry["delivery_tat_pct"] is not None else None,
                    kpi_snapshot_label=str(entry["kpi_snapshot_label"] or "--"),
                )
            )

        if target_updates:
            update_stmt = (
                sa.update(targets)
                .where(
                    sa.and_(
                        targets.c.month == sa.bindparam("b_month"),
                        targets.c.year == sa.bindparam("b_year"),
                        targets.c.cost_center == sa.bindparam("b_cost_center"),
                    )
                )
                .values(
                    sales_mtd=sa.bindparam("sales_mtd"),
                    collection_mtd=sa.bindparam("collection_mtd"),
                    sales_target_met=sa.bindparam("sales_target_met"),
                    collection_target_met=sa.bindparam("collection_target_met"),
                )
            )
            await session.execute(update_stmt, target_updates)
            await session.commit()

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
            .where(sales.c.payment_date >= edited_ranges["start_day"])
            .where(sales.c.payment_date < ranges["next_day"])
            .order_by(sales.c.cost_center, sales.c.order_number)
        )
        edited_rows: list[EditedOrderRow] = []
        edited_result = await session.execute(edited_stmt)
        for entry in edited_result.mappings():
            payment_received = _decimal(entry["payment_received"])
            net_amount = _decimal(entry["net_amount"])
            loss = net_amount - payment_received
            edited_rows.append(
                EditedOrderRow(
                    cost_center=str(entry["cost_center"]),
                    order_number=str(entry["order_number"]),
                    original_value=net_amount,
                    new_value=payment_received,
                    loss=loss,
                )
            )

        manual_recovery_stmt = (
            sa.select(
                orders.c.cost_center,
                sa.func.coalesce(store_master_primary.c.store_name, cost_center.c.description).label("description"),
                orders.c.order_number,
                orders.c.source_system,
                orders.c.net_amount,
                orders.c.gross_amount,
                orders.c.order_date,
                orders.c.default_due_date,
                orders.c.recovery_status,
                orders.c.recovery_opened_at,
                orders.c.recovery_closed_at,
                orders.c.recovery_expected_resolution_date,
            )
            .select_from(
                orders.join(cost_center, cost_center.c.cost_center == orders.c.cost_center).outerjoin(
                    store_master_primary,
                    store_master_primary.c.cost_center == orders.c.cost_center,
                )
            )
            .where(cost_center.c.is_active.is_(True))
            .where(
                sa.or_(
                    orders.c.recovery_status.in_(MANUAL_RECOVERY_STATUSES + RECOVERY_CLOSED_STATUSES),
                    orders.c.recovery_opened_at.is_not(None),
                )
            )
            .order_by(orders.c.cost_center, orders.c.order_date, orders.c.order_number)
        )
        manual_recovery_result = await session.execute(manual_recovery_stmt)
        to_be_recovered, to_be_compensated, recovery_lifecycle_closed, recovery_backlog_totals = (
            _build_manual_recovery_sections(
            manual_recovery_result.mappings(),
            report_date=report_date,
            tz=tz,
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
        for (cost_center_code, _order_number), values in distinct_map.items():
            per_store_counts[cost_center_code] = per_store_counts.get(cost_center_code, 0) + 1
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

    report_month_start = report_date.replace(day=1)
    report_next_month_start = (report_month_start + timedelta(days=32)).replace(day=1)
    monthly_lead_period_end = datetime.combine(report_next_month_start, time.min, tzinfo=tz)
    lead_period_start = datetime.combine(report_month_start, time.min, tzinfo=tz)
    lead_period_end = monthly_lead_period_end

    td_store_master_primary = (
        sa.select(
            store_master_primary.c.store_code,
            store_master_primary.c.store_name,
        )
        .where(store_master_primary.c.sync_group == "TD")
        .subquery()
    )

    async with session_scope(database_url) as session:
        missed_leads_stmt = (
            sa.select(
                td_store_master_primary.c.store_name.label("store_name"),
                missed_leads.c.customer_type,
                missed_leads.c.customer_name,
                missed_leads.c.mobile_number,
            )
            .select_from(
                missed_leads.join(
                    td_store_master_primary,
                    td_store_master_primary.c.store_code == missed_leads.c.store_code,
                )
            )
            .where(missed_leads.c.is_order_placed.is_(False))
            .where(missed_leads.c.pickup_date >= report_month_start)
            .where(missed_leads.c.pickup_date < report_next_month_start)
            .order_by(
                td_store_master_primary.c.store_name,
                missed_leads.c.customer_type,
                missed_leads.c.pickup_date,
                missed_leads.c.customer_name,
            )
        )

        missed_leads_grouped: list[Mapping[str, object]] = []
        grouped_map: dict[tuple[str, str], list[dict[str, str]]] = {}
        missed_leads_result = await session.execute(missed_leads_stmt)
        for entry in missed_leads_result.mappings():
            store_name = str(entry["store_name"] or "--")
            customer_type = str(entry["customer_type"] or "Unknown")
            key = (store_name, customer_type)
            grouped_map.setdefault(key, []).append(
                {
                    "customer_name": str(entry["customer_name"] or "--"),
                    "mobile_number": str(entry["mobile_number"] or "--"),
                }
            )

        for (store_name, customer_type), leads in sorted(grouped_map.items()):
            missed_leads_grouped.append(
                {
                    "store_name": store_name,
                    "customer_type": customer_type,
                    "leads": leads,
                }
            )

        # Deprecated from daily sales report context:
        # keep these payload slots empty and avoid unnecessary pipeline summary reads.
        td_leads_sync_metrics: Mapping[str, object] = {}
        td_leads_sync_lead_changes: Mapping[str, object] = {}

        normalized_store_code_expr = sa.func.upper(sa.func.trim(crm_leads_current.c.store_code))
        normalized_status_bucket_expr = sa.func.lower(sa.func.trim(crm_leads_current.c.status_bucket))
        normalized_store_master_code_expr = sa.func.upper(sa.func.trim(store_master_primary.c.store_code))
        normalized_cancelled_flag_expr = sa.func.lower(sa.func.trim(crm_leads_current.c.cancelled_flag))
        # Current-state KPI policy:
        # final_status_bucket comes only from crm_leads_current.status_bucket.
        # Historical events are transition analytics input and must not override
        # the current status for KPI/cancelled cohort classification.
        is_cancelled_expr = normalized_status_bucket_expr == "cancelled"
        final_status_bucket_expr = sa.case(
            (is_cancelled_expr, "cancelled"),
            else_=normalized_status_bucket_expr,
        )

        lead_base = (
            sa.select(
                normalized_store_code_expr.label("store_code"),
                td_store_master_primary.c.store_name.label("store_name"),
                crm_leads_current.c.customer_name.label("customer_name"),
                crm_leads_current.c.mobile.label("mobile"),
                crm_leads_current.c.reason.label("reason"),
                normalized_cancelled_flag_expr.label("cancelled_flag"),
                crm_leads_current.c.pickup_created_at.label("pickup_created_at"),
                is_cancelled_expr.label("is_cancelled"),
                normalized_status_bucket_expr.label("status_bucket"),
                final_status_bucket_expr.label("final_status_bucket"),
            )
            .select_from(
                crm_leads_current.join(
                    td_store_master_primary,
                    sa.func.upper(sa.func.trim(td_store_master_primary.c.store_code))
                    == normalized_store_code_expr,
                )
            )
            .where(crm_leads_current.c.pickup_created_at >= lead_period_start)
            .where(crm_leads_current.c.pickup_created_at < lead_period_end)
            .subquery()
        )

        cancelled_leads_stmt = (
            sa.select(
                lead_base.c.store_name,
                lead_base.c.customer_name,
                lead_base.c.mobile,
                lead_base.c.cancelled_flag,
                lead_base.c.reason,
            )
            # Cancelled monthly table policy:
            # use current-cancelled cohort only (final status at report time),
            # not "ever hit cancelled" transition cohort.
            .where(lead_base.c.final_status_bucket == "cancelled")
            .order_by(
                lead_base.c.store_name,
                lead_base.c.pickup_created_at,
                lead_base.c.customer_name,
            )
        )
        cancelled_grouped_map: dict[str, dict[str, object]] = {}
        cancelled_leads_result = await session.execute(cancelled_leads_stmt)
        for entry in cancelled_leads_result.mappings():
            store_name = str(entry["store_name"] or "--")
            group = cancelled_grouped_map.setdefault(
                store_name,
                {
                    "store_name": store_name,
                    "total_cancelled_count": 0,
                    "customer_cancelled_count": 0,
                    "store_cancelled_rows": [],
                },
            )
            group["total_cancelled_count"] = int(group["total_cancelled_count"]) + 1
            cancelled_flag = resolve_cancelled_flag(cancelled_flag=entry["cancelled_flag"], reason=entry["reason"])
            if cancelled_flag == "customer":
                group["customer_cancelled_count"] = int(group["customer_cancelled_count"]) + 1
                continue
            cast_rows = group["store_cancelled_rows"]
            if isinstance(cast_rows, list):
                cast_rows.append(
                    {
                        "customer_name": str(entry["customer_name"] or "--"),
                        "mobile": str(entry["mobile"] or "--"),
                        "reason": (str(entry["reason"] or "").strip() or "--"),
                    }
                )
        cancelled_leads_grouped = [
            cancelled_grouped_map[store_name] for store_name in sorted(cancelled_grouped_map.keys())
        ]

        lead_agg = (
            sa.select(
                lead_base.c.store_code.label("store_code"),
                sa.func.count().label("total_leads"),
                sa.func.coalesce(
                    sa.func.sum(
                        sa.case(
                            (
                                sa.and_(
                                    lead_base.c.final_status_bucket == "completed",
                                ),
                                1,
                            ),
                            else_=0,
                        )
                    ),
                    0,
                ).label("completed_leads"),
                sa.func.coalesce(
                    sa.func.sum(sa.case((lead_base.c.final_status_bucket == "cancelled", 1), else_=0)),
                    0,
                ).label("cancelled_leads"),
                sa.func.coalesce(
                    sa.func.sum(
                        sa.case(
                            (
                                sa.and_(
                                    lead_base.c.final_status_bucket == "pending",
                                ),
                                1,
                            ),
                            else_=0,
                        )
                    ),
                    0,
                ).label("pending_leads"),
            )
            .select_from(lead_base)
            .group_by(lead_base.c.store_code)
            .subquery()
        )

        lead_summary_stmt = (
            sa.select(
                normalized_store_master_code_expr.label("store_code"),
                store_master_primary.c.store_name.label("store_name"),
                sa.func.coalesce(lead_agg.c.total_leads, 0).label("total_leads"),
                sa.func.coalesce(lead_agg.c.completed_leads, 0).label("completed_leads"),
                sa.func.coalesce(lead_agg.c.cancelled_leads, 0).label("cancelled_leads"),
                sa.func.coalesce(lead_agg.c.pending_leads, 0).label("pending_leads"),
            )
            .select_from(
                cost_center.join(
                    store_master_primary,
                    store_master_primary.c.cost_center == cost_center.c.cost_center,
                ).outerjoin(lead_agg, lead_agg.c.store_code == normalized_store_master_code_expr)
            )
            .where(cost_center.c.is_active.is_(True))
            .order_by(store_master_primary.c.store_name)
        )

        lead_performance_summary: list[Mapping[str, object]] = []
        lead_summary_result = await session.execute(lead_summary_stmt)
        for entry in lead_summary_result.mappings():
            total_leads = int(entry["total_leads"] or 0)
            completed_leads = int(entry["completed_leads"] or 0)
            cancelled_leads = int(entry["cancelled_leads"] or 0)
            pending_leads = int(entry["pending_leads"] or 0)

            if total_leads == 0:
                conversion_pct = Decimal("0")
                cancelled_pct = Decimal("0")
                pending_pct = Decimal("0")
            else:
                total = Decimal(str(total_leads))
                conversion_pct = _round_percentage((Decimal(str(completed_leads)) / total) * Decimal("100"))
                cancelled_pct = _round_percentage((Decimal(str(cancelled_leads)) / total) * Decimal("100"))
                pending_pct = _round_percentage((Decimal(str(pending_leads)) / total) * Decimal("100"))

            lead_performance_summary.append(
                {
                    "store": str(entry["store_code"] or "--"),
                    "store_name": str(entry["store_name"] or "--"),
                    "period_type": "MTD",
                    "period_start": report_month_start.isoformat(),
                    "period_end": (report_next_month_start - timedelta(days=1)).isoformat(),
                    "total_leads": total_leads,
                    "completed_leads": completed_leads,
                    "cancelled_leads": cancelled_leads,
                    "pending_leads": pending_leads,
                    "conversion_pct": _lead_metric_payload(
                        metric="conversion",
                        value=conversion_pct,
                        total_leads=total_leads,
                    ),
                    "cancelled_pct": _lead_metric_payload(
                        metric="cancelled",
                        value=cancelled_pct,
                        total_leads=total_leads,
                    ),
                    "pending_pct": _lead_metric_payload(
                        metric="pending",
                        value=pending_pct,
                        total_leads=total_leads,
                    ),
                    "conversion_gap": float(_round_percentage(conversion_pct - LEAD_BENCHMARKS["conversion_target"])),
                    "cancelled_gap": float(_round_percentage(cancelled_pct - LEAD_BENCHMARKS["cancelled_target"])),
                    "pending_gap": float(_round_percentage(pending_pct - LEAD_BENCHMARKS["pending_max"])),
                    "benchmark": {
                        "conversion_target": float(LEAD_BENCHMARKS["conversion_target"]),
                        "conversion_min": float(LEAD_BENCHMARKS["conversion_min"]),
                        "cancelled_target": float(LEAD_BENCHMARKS["cancelled_target"]),
                        "cancelled_max": float(LEAD_BENCHMARKS["cancelled_max"]),
                        "pending_max": float(LEAD_BENCHMARKS["pending_max"]),
                    },
                }
            )

    return DailySalesReportData(
        report_date=report_date,
        rows=rows,
        totals=totals,
        edited_orders=edited_rows,
        edited_orders_totals=edited_totals,
        edited_orders_summary=edited_orders_summary,
        missed_leads=missed_leads_grouped,
        cancelled_leads=cancelled_leads_grouped,
        lead_performance_summary=lead_performance_summary,
        td_leads_sync_metrics=td_leads_sync_metrics,
        td_leads_sync_lead_changes=td_leads_sync_lead_changes,
        to_be_recovered=to_be_recovered,
        to_be_compensated=to_be_compensated,
        recovery_lifecycle_closed=recovery_lifecycle_closed,
        recovery_backlog_totals=recovery_backlog_totals,
    )
