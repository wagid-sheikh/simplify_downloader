from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Iterable, List

import sqlalchemy as sa

from app.common.date_utils import get_timezone
from app.common.db import session_scope
from app.crm_downloader.td_orders_sync.ingest import _orders_table
from app.crm_downloader.td_orders_sync.sales_ingest import _sales_table


@dataclass
class PendingDeliveryRow:
    cost_center: str
    store_code: str
    order_number: str
    customer_name: str
    order_date: date
    age_days: int
    gross_amount: Decimal
    paid_amount: Decimal
    pending_amount: Decimal
    adjustments: Decimal
    is_edited_order: bool
    is_duplicate: bool
    source_system: str


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


def _decimal(value: object | None) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _resolve_order_date(order_date: datetime, tz) -> date:
    if order_date.tzinfo is None:
        order_date = order_date.replace(tzinfo=tz)
    return order_date.astimezone(tz).date()


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
    skip_uc_pending_delivery: bool,
) -> PendingDeliveriesReportData:
    metadata = sa.MetaData()
    orders = _orders_table(metadata)
    sales = _sales_table(metadata)

    paid_amount_expr = sa.func.coalesce(
        sa.func.sum(
            sa.case(
                (
                    orders.c.source_system == "TumbleDry",
                    sa.func.coalesce(sales.c.payment_received, 0)
                    + sa.func.coalesce(sales.c.adjustments, 0),
                ),
                else_=sa.func.coalesce(sales.c.payment_received, 0),
            )
        ),
        0,
    )
    amount_expr = sa.func.coalesce(
        sa.case(
            (orders.c.source_system == "TumbleDry", orders.c.net_amount),
            else_=orders.c.gross_amount,
        ),
        0,
    )
    adjustments_expr = sa.func.coalesce(sa.func.sum(sales.c.adjustments), 0)
    is_edited_order_expr = sa.func.max(
        sa.case((sales.c.is_edited_order.is_(True), 1), else_=0)
    )
    is_duplicate_expr = sa.func.max(
        sa.case((sales.c.is_duplicate.is_(True), 1), else_=0)
    )
    pending_amount_expr = sa.func.greatest(amount_expr - paid_amount_expr, 0)

    stmt = (
        sa.select(
            orders.c.cost_center,
            orders.c.store_code,
            orders.c.order_number,
            orders.c.customer_name,
            orders.c.order_date,
            orders.c.source_system,
            amount_expr.label("gross_amount"),
            paid_amount_expr.label("paid_amount"),
            pending_amount_expr.label("pending_amount"),
            adjustments_expr.label("adjustments"),
            is_edited_order_expr.label("is_edited_order"),
            is_duplicate_expr.label("is_duplicate"),
        )
        .select_from(
            orders.outerjoin(
                sales,
                sa.and_(
                    orders.c.cost_center == sales.c.cost_center,
                    orders.c.order_number == sales.c.order_number,
                ),
            )
        )
        .where(orders.c.order_status == "Pending")
        .group_by(
            orders.c.cost_center,
            orders.c.store_code,
            orders.c.order_number,
            orders.c.customer_name,
            orders.c.order_date,
            orders.c.source_system,
            amount_expr,
        )
        .having(pending_amount_expr > 0)
    )

    if skip_uc_pending_delivery:
        stmt = stmt.where(orders.c.source_system != "UClean")

    tz = get_timezone()
    rows: list[PendingDeliveryRow] = []

    async with session_scope(database_url) as session:
        results = await session.execute(stmt)
        for record in results.mappings():
            order_date = record.get("order_date")
            if not isinstance(order_date, datetime):
                continue
            order_date_local = _resolve_order_date(order_date, tz)
            age_days = max(0, (report_date - order_date_local).days)
            gross_amount = _decimal(record.get("gross_amount"))
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
                    age_days=age_days,
                    gross_amount=gross_amount,
                    paid_amount=paid_amount,
                    pending_amount=pending_amount,
                    adjustments=adjustments,
                    is_edited_order=is_edited_order,
                    is_duplicate=is_duplicate,
                    source_system=str(record.get("source_system") or ""),
                )
            )

    summary_sections = _build_summary_sections(rows)
    cost_center_sections = _build_cost_center_sections(rows)
    total_pending_amount = sum(
        (section.total_pending_amount for section in summary_sections), Decimal("0")
    )
    total_count = sum(section.total_count for section in summary_sections)
    return PendingDeliveriesReportData(
        report_date=report_date,
        summary_sections=summary_sections,
        cost_center_sections=cost_center_sections,
        total_pending_amount=total_pending_amount,
        total_count=total_count,
    )
