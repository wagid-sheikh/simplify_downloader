from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Iterable, List

import sqlalchemy as sa

from app.common.date_utils import get_timezone
from app.common.db import session_scope
from app.common.order_recovery import (
    transition_order_recovery_status,
)
from app.crm_downloader.td_orders_sync.sales_ingest import _sales_table
from app.reports.shared.payment_reconciliation import (
    DEFAULT_PAYMENT_TOLERANCE,
    reconcile_payments,
)
from app.reports.shared.short_payments import fetch_payment_rows_for_orders

PENDING_DELIVERY_MAIN_RECOVERY_STATUS = "NONE"

PENDING_DELIVERY_EXCLUDED_RECOVERY_STATUSES = (
    "TO_BE_RECOVERED",
    "TO_BE_COMPENSATED",
    "RECOVERED",
    "COMPENSATED",
    "WRITE_OFF",
)


AGED_PENDING_DELIVERY_RECOVERY_STATUS = "TO_BE_RECOVERED"
AGED_PENDING_DELIVERY_THRESHOLD_DAYS = 30


def _system_recovery_note(marked_at: datetime) -> str:
    readable_date = marked_at.strftime("%d-%b-%Y")
    technical_timestamp = marked_at.isoformat()
    return (
        "Auto marked as TO_BE_RECOVERED by system "
        f"on {readable_date} [{technical_timestamp}]"
    )


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
    missing_default_due_date_count: int = 0


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


def _bucket_age(age_days: int) -> str | None:
    if age_days <= 5:
        return "0-5"
    if age_days <= 15:
        return "6-15"
    if age_days <= AGED_PENDING_DELIVERY_THRESHOLD_DAYS:
        return "16-30"
    return None


def _build_buckets(rows: Iterable[PendingDeliveryRow]) -> List[PendingDeliveriesBucket]:
    buckets_map = {
        "0-5": PendingDeliveriesBucket(
            label="0-5 days",
            min_days=0,
            max_days=5,
            rows=[],
            total_count=0,
            total_pending_amount=Decimal("0"),
        ),
        "6-15": PendingDeliveriesBucket(
            label="6-15 days",
            min_days=6,
            max_days=15,
            rows=[],
            total_count=0,
            total_pending_amount=Decimal("0"),
        ),
        "16-30": PendingDeliveriesBucket(
            label="16-30 days",
            min_days=16,
            max_days=AGED_PENDING_DELIVERY_THRESHOLD_DAYS,
            rows=[],
            total_count=0,
            total_pending_amount=Decimal("0"),
        ),
    }

    for row in rows:
        bucket_key = _bucket_age(row.age_days)
        if bucket_key is None:
            continue
        buckets_map[bucket_key].rows.append(row)

    for bucket in buckets_map.values():
        bucket.rows.sort(
            key=lambda item: (-item.age_days, item.order_date, item.order_number)
        )
        bucket.total_count = len(bucket.rows)
        bucket.total_pending_amount = sum(
            (row.pending_amount for row in bucket.rows), Decimal("0")
        )

    return [buckets_map["0-5"], buckets_map["6-15"], buckets_map["16-30"]]


def _build_summary_sections(
    rows: Iterable[PendingDeliveryRow],
) -> List[PendingDeliveriesSummarySection]:
    grouped_rows: dict[str, list[PendingDeliveryRow]] = {}
    for row in rows:
        grouped_rows.setdefault(row.cost_center, []).append(row)

    summary_sections: list[PendingDeliveriesSummarySection] = []
    for cost_center, cost_center_rows in sorted(grouped_rows.items()):
        buckets = _build_buckets(cost_center_rows)
        total_pending_amount = sum(
            (bucket.total_pending_amount for bucket in buckets), Decimal("0")
        )
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
        total_pending_amount = sum(
            (bucket.total_pending_amount for bucket in buckets), Decimal("0")
        )
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


@dataclass
class PendingDeliveryTransitionMetrics:
    scanned_count: int = 0
    eligible_count: int = 0
    transitioned_count: int = 0
    skipped_due_to_sales_row: int = 0
    skipped_due_to_non_none_status: int = 0
    skipped_due_to_missing_due_date: int = 0
    skipped_due_to_age: int = 0

    def to_dict(self) -> dict[str, int]:
        return {k: int(v) for k, v in asdict(self).items()}


async def transition_aged_pending_deliveries_to_recovery_metrics(
    *,
    database_url: str,
    report_date: date,
) -> PendingDeliveryTransitionMetrics:
    """Mark aged pending-delivery orders for recovery on the base orders table and return metrics."""

    metadata = sa.MetaData()
    orders = sa.table(
        "vw_orders",
        sa.column("cost_center"),
        sa.column("order_number"),
        sa.column("order_date"),
        sa.column("default_due_date"),
        sa.column("order_amount"),
        sa.column("recovery_status"),
    )
    payment_collections = sa.table(
        "payment_collections",
        sa.column("cost_center"),
        sa.column("order_number"),
        sa.column("amount"),
        sa.column("source_type"),
    )
    sales = _sales_table(metadata)

    def _normalized_key(column: sa.ColumnElement[object]) -> sa.ColumnElement[str]:
        return sa.func.upper(sa.func.trim(sa.func.coalesce(column, "")))

    matching_sale_exists = sa.exists().where(
        sa.and_(
            _normalized_key(sales.c.cost_center)
            == _normalized_key(orders.c.cost_center),
            _normalized_key(sales.c.order_number)
            == _normalized_key(orders.c.order_number),
        )
    )
    amount_expr = sa.func.coalesce(orders.c.order_amount, 0)
    base_stmt = (
        sa.select(
            orders.c.cost_center,
            orders.c.order_number,
            orders.c.order_date,
            orders.c.default_due_date,
            orders.c.recovery_status,
            amount_expr.label("order_amount"),
            matching_sale_exists.label("has_sale"),
        )
        .select_from(orders)
        .where(amount_expr > 0)
        .order_by(orders.c.cost_center, orders.c.order_number)
    )

    tz = get_timezone()
    note = _system_recovery_note(
        datetime.combine(report_date, datetime.min.time(), tzinfo=tz)
    )
    metrics = PendingDeliveryTransitionMetrics()

    async with session_scope(database_url) as session:
        results = await session.execute(base_stmt)
        candidate_order_rows: list[dict[str, object]] = []
        for record in results.mappings():
            metrics.scanned_count += 1
            has_sale = bool(record.get("has_sale"))
            if has_sale:
                metrics.skipped_due_to_sales_row += 1
                continue
            if str(record.get("recovery_status") or "") != PENDING_DELIVERY_MAIN_RECOVERY_STATUS:
                metrics.skipped_due_to_non_none_status += 1
                continue
            default_due_date = _coerce_datetime(record.get("default_due_date"))
            if default_due_date is None:
                metrics.skipped_due_to_missing_due_date += 1
                order_date = _coerce_datetime(record.get("order_date"))
                default_due_date = order_date + timedelta(days=2) if order_date is not None else None
            if default_due_date is None:
                metrics.skipped_due_to_age += 1
                continue
            default_due_date_local = _resolve_business_date(default_due_date, tz)
            age_days = max(0, (report_date - default_due_date_local).days)
            if age_days <= AGED_PENDING_DELIVERY_THRESHOLD_DAYS:
                metrics.skipped_due_to_age += 1
                continue
            cost_center = str(record.get("cost_center") or "")
            order_number = str(record.get("order_number") or "")
            if not cost_center or not order_number:
                continue
            metrics.eligible_count += 1
            candidate_order_rows.append(
                {
                    "cost_center": cost_center,
                    "order_number": order_number,
                    "order_date": record.get("order_date"),
                    "order_amount": record.get("order_amount"),
                    "recovery_status": record.get("recovery_status"),
                }
            )

        payment_rows = await fetch_payment_rows_for_orders(
            session=session,
            payment_collections=payment_collections,
            order_rows=candidate_order_rows,
        )
        reconciliation = reconcile_payments(
            order_rows=candidate_order_rows,
            payment_evidence_rows=payment_rows,
        )
        candidate_keys: list[tuple[str, str]] = []
        for order in reconciliation.orders:
            has_sufficient_payment_proof = (
                order.order_amount > 0
                and order.has_payment_proof
                and order.allocated_payment_amount + DEFAULT_PAYMENT_TOLERANCE
                >= order.order_amount
            )
            if has_sufficient_payment_proof:
                continue
            candidate_keys.append((order.cost_center, order.order_number))

        for cost_center, order_number in candidate_keys:
            metrics.transitioned_count += await transition_order_recovery_status(
                session=session,
                cost_center=cost_center,
                order_number=order_number,
                from_status=PENDING_DELIVERY_MAIN_RECOVERY_STATUS,
                to_status=AGED_PENDING_DELIVERY_RECOVERY_STATUS,
                recovery_category=None,
                recovery_note=note,
            )

        await session.commit()

    return metrics


async def transition_aged_pending_deliveries_to_recovery(
    *,
    database_url: str,
    report_date: date,
) -> int:
    metrics = await transition_aged_pending_deliveries_to_recovery_metrics(
        database_url=database_url,
        report_date=report_date,
    )
    return metrics.transitioned_count


async def fetch_pending_deliveries_report(
    *,
    database_url: str,
    report_date: date,
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
        sa.column("recovery_status"),
    )
    payment_collections = sa.table(
        "payment_collections",
        sa.column("cost_center"),
        sa.column("order_number"),
        sa.column("amount"),
        sa.column("source_type"),
    )
    sales = _sales_table(metadata)

    amount_expr = sa.func.coalesce(orders.c.order_amount, 0)

    def _normalized_key(column: sa.ColumnElement[object]) -> sa.ColumnElement[str]:
        return sa.func.upper(sa.func.trim(sa.func.coalesce(column, "")))

    matching_sale_exists = sa.exists().where(
        sa.and_(
            _normalized_key(sales.c.cost_center)
            == _normalized_key(orders.c.cost_center),
            _normalized_key(sales.c.order_number)
            == _normalized_key(orders.c.order_number),
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
            sa.literal(0).label("adjustments"),
            sa.literal(0).label("is_edited_order"),
            sa.literal(0).label("is_duplicate"),
        )
        .select_from(orders)
        .where(orders.c.recovery_status == PENDING_DELIVERY_MAIN_RECOVERY_STATUS)
        .where(sa.not_(matching_sale_exists))
        .where(amount_expr > 0)
    )

    tz = get_timezone()
    rows: list[PendingDeliveryRow] = []
    missing_default_due_date_count = 0

    async with session_scope(database_url) as session:
        results = await session.execute(stmt)
        candidate_order_rows: list[dict[str, object]] = []
        prepared_rows: dict[tuple[str, str], dict[str, object]] = {}
        for record in results.mappings():
            order_date = _coerce_datetime(record.get("order_date"))
            if order_date is None:
                continue
            default_due_date = _coerce_datetime(record.get("default_due_date"))
            if default_due_date is None:
                missing_default_due_date_count += 1
                default_due_date = order_date + timedelta(days=2)
            order_date_local = _resolve_order_date(order_date, tz)
            default_due_date_local = _resolve_business_date(default_due_date, tz)
            age_days = max(0, (report_date - default_due_date_local).days)
            if age_days > AGED_PENDING_DELIVERY_THRESHOLD_DAYS:
                continue

            row = dict(record)
            cost_center = str(row.get("cost_center") or "")
            order_number = str(row.get("order_number") or "")
            if not cost_center or not order_number:
                continue
            key = (cost_center, order_number)
            row["order_date_local"] = order_date_local
            row["default_due_date_local"] = default_due_date_local
            row["age_days"] = age_days
            prepared_rows[key] = row
            candidate_order_rows.append(row)

        payment_rows = await fetch_payment_rows_for_orders(
            session=session,
            payment_collections=payment_collections,
            order_rows=candidate_order_rows,
        )
        reconciliation = reconcile_payments(
            order_rows=candidate_order_rows,
            payment_evidence_rows=payment_rows,
        )

        for order in reconciliation.orders:
            if order.order_amount <= 0:
                continue
            if (
                order.has_payment_proof
                and order.allocated_payment_amount + DEFAULT_PAYMENT_TOLERANCE
                >= order.order_amount
            ):
                continue
            record = prepared_rows.get((order.cost_center, order.order_number))
            if record is None:
                continue
            order_amount = _decimal(order.order_amount)
            paid_amount = _decimal(order.allocated_payment_amount)
            pending_amount = max(Decimal("0"), order_amount - paid_amount)
            adjustments = _decimal(record.get("adjustments"))
            is_edited_order = bool(record.get("is_edited_order"))
            is_duplicate = bool(record.get("is_duplicate"))
            rows.append(
                PendingDeliveryRow(
                    cost_center=order.cost_center,
                    store_code=str(record.get("store_code") or ""),
                    order_number=order.order_number,
                    customer_name=str(record.get("customer_name") or ""),
                    order_date=record["order_date_local"],
                    default_due_date=record["default_due_date_local"],
                    age_days=int(record["age_days"]),
                    order_amount=order_amount,
                    paid_amount=paid_amount,
                    pending_amount=pending_amount,
                    adjustments=adjustments,
                    is_edited_order=is_edited_order,
                    is_duplicate=is_duplicate,
                    source_system=str(record.get("source_system") or ""),
                    recovery_status=order.recovery_status,
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
        missing_default_due_date_count=missing_default_due_date_count,
    )
