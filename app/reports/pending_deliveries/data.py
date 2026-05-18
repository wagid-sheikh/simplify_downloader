from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Iterable, List

import sqlalchemy as sa

from app.common.date_utils import get_timezone
from app.common.db import session_scope
from app.crm_downloader.td_orders_sync.sales_ingest import _sales_table


PENDING_DELIVERY_EXCLUDED_RECOVERY_STATUSES = (
    "TO_BE_RECOVERED",
    "TO_BE_COMPENSATED",
    "RECOVERED",
    "COMPENSATED",
    "WRITE_OFF",
)
ACTIVE_MANUAL_RECOVERY_STATUSES = ("TO_BE_RECOVERED", "TO_BE_COMPENSATED")


@dataclass
class PendingDeliveryRow:
    cost_center: str
    store_code: str
    order_number: str
    customer_name: str
    order_date: date
    default_due_date: date
    age_days: int
    order_amount: Decimal
    paid_amount: Decimal
    pending_amount: Decimal
    adjustments: Decimal
    is_edited_order: bool
    is_duplicate: bool
    source_system: str
    recovery_status: str | None = None

    @property
    def gross_amount(self) -> Decimal:
        return self.order_amount


@dataclass
class PendingDeliveriesBucket:
    label: str
    min_days: int
    max_days: int | None
    rows: List[PendingDeliveryRow]
    total_count: int
    total_pending_amount: Decimal


@dataclass
class PendingDeliveriesSummarySection:
    cost_center: str
    buckets: List[PendingDeliveriesBucket]
    total_pending_amount: Decimal
    total_count: int


@dataclass
class PendingDeliveriesCostCenterSection:
    cost_center: str
    buckets: List[PendingDeliveriesBucket]
    total_pending_amount: Decimal
    total_count: int


@dataclass
class PendingDeliveriesReportData:
    report_date: date
    summary_sections: List[PendingDeliveriesSummarySection]
    cost_center_sections: List[PendingDeliveriesCostCenterSection]
    total_pending_amount: Decimal
    total_count: int
    manual_recovery_rows: List[PendingDeliveryRow]
    manual_recovery_total_amount_at_risk: Decimal


def _decimal(value: object | None) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        numeric = value
    else:
        numeric = Decimal(str(value))
    return numeric.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _coerce_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def _resolve_order_date(order_date: datetime, tz) -> date:
    if order_date.tzinfo is None:
        order_date = order_date.replace(tzinfo=tz)
    return order_date.astimezone(tz).date()


def _resolve_business_date(value: datetime, tz) -> date:
    if value.tzinfo is None:
        value = value.replace(tzinfo=tz)
    return value.astimezone(tz).date()


def _bucket_age(age_days: int) -> str:
    if age_days <= 5:
        return "0-5"
    if age_days <= 15:
        return "6-15"
    return ">15"


def _build_buckets(rows: Iterable[PendingDeliveryRow]) -> List[PendingDeliveriesBucket]:
    buckets_map = {
        "0-5": PendingDeliveriesBucket(label="0-5 days", min_days=0, max_days=5, rows=[], total_count=0, total_pending_amount=Decimal("0")),
        "6-15": PendingDeliveriesBucket(label="6-15 days", min_days=6, max_days=15, rows=[], total_count=0, total_pending_amount=Decimal("0")),
        ">15": PendingDeliveriesBucket(label=">15 days", min_days=16, max_days=None, rows=[], total_count=0, total_pending_amount=Decimal("0")),
    }

    for row in rows:
        bucket_key = _bucket_age(row.age_days)
        buckets_map[bucket_key].rows.append(row)

    for bucket in buckets_map.values():
        bucket.rows.sort(key=lambda item: (-item.age_days, item.order_date, item.order_number))
        bucket.total_count = len(bucket.rows)
        bucket.total_pending_amount = sum((row.pending_amount for row in bucket.rows), Decimal("0"))

    return [buckets_map["0-5"], buckets_map["6-15"], buckets_map[">15"]]


def _build_summary_sections(rows: Iterable[PendingDeliveryRow]) -> List[PendingDeliveriesSummarySection]:
    grouped_rows: dict[str, list[PendingDeliveryRow]] = {}
    for row in rows:
        grouped_rows.setdefault(row.cost_center, []).append(row)

    summary_sections: list[PendingDeliveriesSummarySection] = []
    for cost_center, cost_center_rows in sorted(grouped_rows.items()):
        buckets = _build_buckets(cost_center_rows)
        total_pending_amount = sum((bucket.total_pending_amount for bucket in buckets), Decimal("0"))
        total_count = sum(bucket.total_count for bucket in buckets)
        summary_sections.append(
            PendingDeliveriesSummarySection(
                cost_center=cost_center,
                buckets=buckets,
                total_pending_amount=total_pending_amount,
                total_count=total_count,
            )
        )
    return summary_sections


def _build_cost_center_sections(
    rows: Iterable[PendingDeliveryRow],
) -> List[PendingDeliveriesCostCenterSection]:
    grouped_rows: dict[str, list[PendingDeliveryRow]] = {}
    for row in rows:
        grouped_rows.setdefault(row.cost_center, []).append(row)

    cost_center_sections: list[PendingDeliveriesCostCenterSection] = []
    for cost_center, cost_center_rows in sorted(grouped_rows.items()):
        buckets = _build_buckets(cost_center_rows)
        total_pending_amount = sum((bucket.total_pending_amount for bucket in buckets), Decimal("0"))
        total_count = sum(bucket.total_count for bucket in buckets)
        cost_center_sections.append(
            PendingDeliveriesCostCenterSection(
                cost_center=cost_center,
                buckets=buckets,
                total_pending_amount=total_pending_amount,
                total_count=total_count,
            )
        )
    return cost_center_sections


async def fetch_pending_deliveries_report(
    *,
    database_url: str,
    report_date: date,
    include_aged_unresolved_recovery_rows: bool = False,
) -> PendingDeliveriesReportData:
    metadata = sa.MetaData()
    orders = sa.table(
        "vw_orders",
        sa.column("cost_center"),
        sa.column("store_code"),
        sa.column("order_number"),
        sa.column("customer_name"),
        sa.column("order_date"),
        sa.column("default_due_date"),
        sa.column("source_system"),
        sa.column("order_amount"),
        sa.column("order_status"),
        sa.column("recovery_status"),
    )
    sales = _sales_table(metadata)

    amount_expr = sa.func.coalesce(orders.c.order_amount, 0)

    def _normalized_key(column: sa.ColumnElement[object]) -> sa.ColumnElement[str]:
        return sa.func.upper(sa.func.trim(sa.func.coalesce(column, "")))

    matching_sale_exists = sa.exists().where(
        sa.and_(
            _normalized_key(sales.c.cost_center) == _normalized_key(orders.c.cost_center),
            _normalized_key(sales.c.order_number) == _normalized_key(orders.c.order_number),
        )
    )

    stmt = (
        sa.select(
            orders.c.cost_center,
            orders.c.store_code,
            orders.c.order_number,
            orders.c.customer_name,
            orders.c.order_date,
            orders.c.default_due_date,
            orders.c.source_system,
            amount_expr.label("order_amount"),
            sa.literal(0).label("paid_amount"),
            amount_expr.label("pending_amount"),
            sa.literal(0).label("adjustments"),
            sa.literal(0).label("is_edited_order"),
            sa.literal(0).label("is_duplicate"),
        )
        .select_from(orders)
        .where(orders.c.order_status == "Pending")
        .where(orders.c.recovery_status == "NONE")
        .where(sa.not_(matching_sale_exists))
        .where(amount_expr > 0)
    )

    tz = get_timezone()
    rows: list[PendingDeliveryRow] = []
    manual_recovery_rows: list[PendingDeliveryRow] = []

    async with session_scope(database_url) as session:
        results = await session.execute(stmt)
        for record in results.mappings():
            order_date = _coerce_datetime(record.get("order_date"))
            if order_date is None:
                continue
            default_due_date = _coerce_datetime(record.get("default_due_date"))
            if default_due_date is None:
                continue
            order_date_local = _resolve_order_date(order_date, tz)
            default_due_date_local = _resolve_business_date(default_due_date, tz)
            age_days = max(0, (report_date - default_due_date_local).days)
            order_amount = _decimal(record.get("order_amount"))
            paid_amount = _decimal(record.get("paid_amount"))
            pending_amount = _decimal(record.get("pending_amount"))
            adjustments = _decimal(record.get("adjustments"))
            is_edited_order = bool(record.get("is_edited_order"))
            is_duplicate = bool(record.get("is_duplicate"))
            rows.append(
                PendingDeliveryRow(
                    cost_center=str(record.get("cost_center") or ""),
                    store_code=str(record.get("store_code") or ""),
                    order_number=str(record.get("order_number") or ""),
                    customer_name=str(record.get("customer_name") or ""),
                    order_date=order_date_local,
                    default_due_date=default_due_date_local,
                    age_days=age_days,
                    order_amount=order_amount,
                    paid_amount=paid_amount,
                    pending_amount=pending_amount,
                    adjustments=adjustments,
                    is_edited_order=is_edited_order,
                    is_duplicate=is_duplicate,
                    source_system=str(record.get("source_system") or ""),
                )
            )

        recovery_orders = sa.table(
            "vw_orders",
            sa.column("cost_center"),
            sa.column("store_code"),
            sa.column("order_number"),
            sa.column("customer_name"),
            sa.column("order_date"),
            sa.column("default_due_date"),
            sa.column("source_system"),
            sa.column("order_amount"),
            sa.column("recovery_status"),
        )
        manual_recovery_filter = recovery_orders.c.recovery_status.in_(
            ACTIVE_MANUAL_RECOVERY_STATUSES
        )
        # Historical callers may still pass include_aged_unresolved_recovery_rows=True,
        # but pending-deliveries recovery visibility is deliberately limited to the two
        # active manual-action statuses. Closed statuses and other custom statuses must
        # not leak into this report section.
        _ = include_aged_unresolved_recovery_rows

        manual_recovery_amount_expr = sa.func.coalesce(recovery_orders.c.order_amount, 0)
        manual_recovery_stmt = (
            sa.select(
                recovery_orders.c.cost_center,
                recovery_orders.c.store_code,
                recovery_orders.c.order_number,
                recovery_orders.c.customer_name,
                recovery_orders.c.order_date,
                recovery_orders.c.default_due_date,
                recovery_orders.c.source_system,
                manual_recovery_amount_expr.label("amount_at_risk"),
                recovery_orders.c.recovery_status,
            )
            .where(manual_recovery_filter)
            .where(manual_recovery_amount_expr > 0)
            .order_by(
                recovery_orders.c.cost_center,
                recovery_orders.c.default_due_date,
                recovery_orders.c.order_date,
                recovery_orders.c.order_number,
            )
        )
        try:
            manual_recovery_results = await session.execute(manual_recovery_stmt)
        except sa.exc.DBAPIError:
            manual_recovery_results = None

        if manual_recovery_results is not None:
            for record in manual_recovery_results.mappings():
                order_date = _coerce_datetime(record.get("order_date"))
                if order_date is None:
                    continue
                default_due_date = _coerce_datetime(record.get("default_due_date"))
                if default_due_date is None:
                    default_due_date = order_date
                order_date_local = _resolve_order_date(order_date, tz)
                default_due_date_local = _resolve_business_date(default_due_date, tz)
                age_days = max(0, (report_date - default_due_date_local).days)
                amount_at_risk = _decimal(record.get("amount_at_risk"))
                manual_recovery_rows.append(
                    PendingDeliveryRow(
                        cost_center=str(record.get("cost_center") or ""),
                        store_code=str(record.get("store_code") or ""),
                        order_number=str(record.get("order_number") or ""),
                        customer_name=str(record.get("customer_name") or ""),
                        order_date=order_date_local,
                        default_due_date=default_due_date_local,
                        age_days=age_days,
                        order_amount=amount_at_risk,
                        paid_amount=Decimal("0"),
                        pending_amount=amount_at_risk,
                        adjustments=Decimal("0"),
                        is_edited_order=False,
                        is_duplicate=False,
                        source_system=str(record.get("source_system") or ""),
                        recovery_status=str(record.get("recovery_status") or ""),
                    )
                )

    summary_sections = _build_summary_sections(rows)
    cost_center_sections = _build_cost_center_sections(rows)
    total_pending_amount = sum(
        (section.total_pending_amount for section in summary_sections), Decimal("0")
    )
    total_count = sum(section.total_count for section in summary_sections)
    manual_recovery_total_amount_at_risk = sum(
        (row.pending_amount for row in manual_recovery_rows), Decimal("0")
    )
    return PendingDeliveriesReportData(
        report_date=report_date,
        summary_sections=summary_sections,
        cost_center_sections=cost_center_sections,
        total_pending_amount=total_pending_amount,
        total_count=total_count,
        manual_recovery_rows=manual_recovery_rows,
        manual_recovery_total_amount_at_risk=manual_recovery_total_amount_at_risk,
    )
