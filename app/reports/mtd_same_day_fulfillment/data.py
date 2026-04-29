from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal

import sqlalchemy as sa

from app.common.date_utils import get_timezone
from app.common.db import session_scope
from app.reports.shared.same_day_fulfillment import fetch_same_day_fulfillment_rows


@dataclass
class MTDSameDayFulfillmentRow:
    store_code: str
    order_number: str
    order_date: datetime | None
    customer_name: str | None
    mobile_number: str | None
    line_items: str | None
    delivery_or_payment_date: datetime | None
    payment_mode: str | None
    hours: float | None
    net_amount: Decimal | None
    payment_received: Decimal | None


async def fetch_mtd_same_day_fulfillment(*, database_url: str, report_date: date) -> list[MTDSameDayFulfillmentRow]:
    orders = sa.table(
        "orders",
        sa.column("cost_center"),
        sa.column("order_number"),
        sa.column("order_date"),
        sa.column("net_amount"),
        sa.column("customer_name"),
        sa.column("mobile_number"),
    )
    order_line_items = sa.table(
        "order_line_items",
        sa.column("cost_center"),
        sa.column("order_number"),
        sa.column("service_name"),
        sa.column("garment_name"),
    )
    sales = sa.table(
        "sales",
        sa.column("cost_center"),
        sa.column("order_number"),
        sa.column("payment_date"),
        sa.column("payment_received"),
        sa.column("payment_mode"),
    )
    store_master = sa.table(
        "store_master",
        sa.column("cost_center"),
        sa.column("store_code"),
    )

    tz = get_timezone()
    start_month = datetime.combine(report_date.replace(day=1), time.min, tzinfo=tz)
    next_day = datetime.combine(report_date, time.min, tzinfo=tz) + timedelta(days=1)

    rows: list[MTDSameDayFulfillmentRow] = []
    async with session_scope(database_url) as session:
        records = await fetch_same_day_fulfillment_rows(
            session=session,
            orders=orders,
            sales=sales,
            order_line_items=order_line_items,
            store_master=store_master,
            start_datetime=start_month,
            end_datetime=next_day,
            timezone_name=str(tz),
        )
        for record in records:
            hours = None
            if record.order_date is not None and record.payment_date is not None:
                hours = round((record.payment_date - record.order_date).total_seconds() / 3600, 2)

            rows.append(MTDSameDayFulfillmentRow(
                store_code=record.store_code,
                order_number=record.order_number,
                order_date=record.order_date,
                customer_name=record.customer_name,
                mobile_number=record.mobile_number,
                line_items=record.line_items,
                delivery_or_payment_date=record.payment_date,
                payment_mode=record.payment_mode,
                hours=hours,
                net_amount=Decimal(str(record.net_amount)) if record.net_amount is not None else None,
                payment_received=Decimal(str(record.payment_received)) if record.payment_received is not None else None,
            ))
    return rows
