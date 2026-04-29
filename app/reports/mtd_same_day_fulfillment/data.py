from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal

import sqlalchemy as sa

from app.common.date_utils import get_timezone
from app.common.db import session_scope


@dataclass
class MTDSameDayFulfillmentRow:
    store_code: str
    order_number: str
    order_date: datetime | None
    delivery_or_payment_date: datetime | None
    net_amount: Decimal | None
    payment_received: Decimal | None


async def fetch_mtd_same_day_fulfillment(*, database_url: str, report_date: date) -> list[MTDSameDayFulfillmentRow]:
    orders = sa.table(
        "orders",
        sa.column("cost_center"),
        sa.column("order_number"),
        sa.column("order_date"),
        sa.column("net_amount"),
    )
    sales = sa.table(
        "sales",
        sa.column("cost_center"),
        sa.column("order_number"),
        sa.column("payment_date"),
        sa.column("payment_received"),
    )
    store_master = sa.table(
        "store_master",
        sa.column("cost_center"),
        sa.column("store_code"),
    )

    tz = get_timezone()
    start_month = datetime.combine(report_date.replace(day=1), time.min, tzinfo=tz)
    next_day = datetime.combine(report_date, time.min, tzinfo=tz) + timedelta(days=1)

    stmt = (
        sa.select(
            sa.func.coalesce(store_master.c.store_code, "").label("store_code"),
            orders.c.order_number,
            orders.c.order_date,
            sa.func.max(sales.c.payment_date).label("payment_date"),
            orders.c.net_amount,
            sa.func.sum(sa.func.coalesce(sales.c.payment_received, 0)).label("payment_received"),
        )
        .select_from(
            orders.join(
                sales,
                sa.and_(orders.c.cost_center == sales.c.cost_center, orders.c.order_number == sales.c.order_number),
            ).outerjoin(store_master, store_master.c.cost_center == orders.c.cost_center)
        )
        .where(orders.c.order_date >= start_month)
        .where(orders.c.order_date < next_day)
        .where(sales.c.payment_date >= start_month)
        .where(sales.c.payment_date < next_day)
        .group_by(store_master.c.store_code, orders.c.order_number, orders.c.order_date, orders.c.net_amount)
        .order_by(orders.c.order_date, orders.c.order_number)
    )

    rows: list[MTDSameDayFulfillmentRow] = []
    async with session_scope(database_url) as session:
        result = await session.execute(stmt)
        for entry in result.mappings():
            rows.append(MTDSameDayFulfillmentRow(
                store_code=str(entry["store_code"] or ""),
                order_number=str(entry["order_number"] or ""),
                order_date=entry["order_date"],
                delivery_or_payment_date=entry["payment_date"],
                net_amount=Decimal(str(entry["net_amount"])) if entry["net_amount"] is not None else None,
                payment_received=Decimal(str(entry["payment_received"])) if entry["payment_received"] is not None else None,
            ))
    return rows
