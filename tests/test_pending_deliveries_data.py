from datetime import date, datetime
from decimal import Decimal

import pytest
import sqlalchemy as sa
from sqlalchemy import event
from zoneinfo import ZoneInfo

from app.common.db import _ensure_async_engine, session_scope
from app.crm_downloader.td_orders_sync.ingest import _orders_table
from app.crm_downloader.td_orders_sync.sales_ingest import _sales_table
from app.reports.pending_deliveries.data import fetch_pending_deliveries_report


async def _register_sqlite_greatest(database_url: str) -> None:
    engine = _ensure_async_engine(database_url)

    @event.listens_for(engine.sync_engine, "connect")
    def _add_sqlite_functions(dbapi_connection, _connection_record):
        dbapi_connection.create_function("greatest", -1, lambda *args: max(args))

    async with engine.begin() as conn:
        await conn.execute(sa.text("SELECT 1"))


def _create_tables(database_url: str) -> None:
    metadata = sa.MetaData()
    _orders_table(metadata)
    _sales_table(metadata)
    engine = sa.create_engine(database_url.replace("+aiosqlite", ""))
    metadata.create_all(engine)
    engine.dispose()


@pytest.mark.asyncio
async def test_fetch_pending_deliveries_package_pending_tolerance(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "pending_deliveries.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr("app.reports.pending_deliveries.data.get_timezone", lambda: tz)
    order_date = datetime(2025, 5, 10, 10, 0, tzinfo=tz)
    now = datetime(2025, 5, 20, 10, 0, tzinfo=tz)

    amount = Decimal("2165.00")
    pending_by_order = {
        "ORD-EXACT": Decimal("216.50"),
        "ORD-NEAR-LOW": Decimal("216.49"),
        "ORD-NEAR-HIGH": Decimal("217.40"),
        "ORD-BOUNDARY": Decimal("217.50"),
        "ORD-NOT-COVERED": Decimal("218.00"),
    }

    async with session_scope(database_url) as session:
        metadata = sa.MetaData()
        orders = _orders_table(metadata)
        sales = _sales_table(metadata)

        for index, (order_number, pending_amount) in enumerate(pending_by_order.items(), start=1):
            await session.execute(
                sa.insert(orders).values(
                    run_id="test-run",
                    run_date=now,
                    cost_center="UN3668",
                    store_code="A668",
                    source_system="TumbleDry",
                    order_number=order_number,
                    invoice_number=f"INV-{index}",
                    order_date=order_date,
                    customer_name=f"Customer {index}",
                    mobile_number=f"99999999{index:02d}",
                    package_flag=True,
                    due_date=order_date,
                    default_due_date=order_date,
                    complete_processing_by=order_date,
                    gross_amount=amount,
                    net_amount=amount,
                    order_status="Pending",
                    created_at=now,
                )
            )

            await session.execute(
                sa.insert(sales).values(
                    run_id="test-run",
                    run_date=now,
                    cost_center="UN3668",
                    store_code="A668",
                    order_date=order_date,
                    payment_date=now,
                    order_number=order_number,
                    customer_name=f"Customer {index}",
                    payment_received=amount - pending_amount,
                    adjustments=Decimal("0"),
                    payment_mode="Package",
                    is_duplicate=False,
                    is_edited_order=False,
                )
            )

        await session.commit()

    data = await fetch_pending_deliveries_report(
        database_url=database_url,
        report_date=date(2025, 5, 20),
        skip_uc_pending_delivery=False,
    )

    assert data.total_count == 1
    assert data.total_pending_amount == Decimal("218.00")
    assert len(data.summary_sections) == 1
    assert {row.order_number for bucket in data.summary_sections[0].buckets for row in bucket.rows} == {
        "ORD-NOT-COVERED"
    }
