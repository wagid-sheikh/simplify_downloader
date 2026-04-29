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
    with engine.begin() as connection:
        connection.execute(sa.text("ALTER TABLE orders ADD COLUMN recovery_status TEXT"))
    engine.dispose()


async def _insert_order_and_sale(
    *,
    database_url: str,
    now: datetime,
    order_date: datetime,
    default_due_date: datetime,
    source_system: str,
    order_number: str,
    gross_amount: Decimal,
    net_amount: Decimal,
    payment_received: Decimal,
    adjustments: Decimal,
    recovery_status: str | None = None,
) -> None:
    async with session_scope(database_url) as session:
        metadata = sa.MetaData()
        orders = _orders_table(metadata)
        sales = _sales_table(metadata)
        order_values = {
            "run_id": "test-run",
            "run_date": now,
            "cost_center": "UN3668",
            "store_code": "A668",
            "source_system": source_system,
            "order_number": order_number,
            "invoice_number": f"INV-{order_number}",
            "order_date": order_date,
            "customer_name": f"Customer-{order_number}",
            "mobile_number": "9999999999",
            "package_flag": False,
            "due_date": default_due_date,
            "default_due_date": default_due_date,
            "complete_processing_by": default_due_date,
            "gross_amount": gross_amount,
            "net_amount": net_amount,
            "order_status": "Pending",
            "created_at": now,
        }
        await session.execute(sa.insert(orders).values(**order_values))
        if recovery_status is not None:
            await session.execute(
                sa.text(
                    "UPDATE orders SET recovery_status = :recovery_status "
                    "WHERE cost_center = :cost_center AND order_number = :order_number"
                ),
                {
                    "recovery_status": recovery_status,
                    "cost_center": "UN3668",
                    "order_number": order_number,
                },
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
                customer_name=f"Customer-{order_number}",
                payment_received=payment_received,
                adjustments=adjustments,
                payment_mode="Cash",
                is_duplicate=False,
                is_edited_order=False,
            )
        )
        await session.commit()


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
    )

    assert data.total_count == 1
    assert data.total_pending_amount == Decimal("218.00")
    assert len(data.summary_sections) == 1
    assert {row.order_number for bucket in data.summary_sections[0].buckets for row in bucket.rows} == {
        "ORD-NOT-COVERED"
    }
    included_row = next(
        row
        for bucket in data.summary_sections[0].buckets
        for row in bucket.rows
        if row.order_number == "ORD-NOT-COVERED"
    )
    assert included_row.default_due_date == date(2025, 5, 10)


@pytest.mark.asyncio
async def test_fetch_pending_deliveries_includes_td_and_uc_orders(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "pending_deliveries_mixed.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr("app.reports.pending_deliveries.data.get_timezone", lambda: tz)
    now = datetime(2025, 5, 20, 10, 0, tzinfo=tz)
    order_date = datetime(2025, 5, 1, 10, 0, tzinfo=tz)
    default_due_date = datetime(2025, 5, 1, 10, 0, tzinfo=tz)

    await _insert_order_and_sale(
        database_url=database_url,
        now=now,
        order_date=order_date,
        default_due_date=default_due_date,
        source_system="TumbleDry",
        order_number="TD-001",
        gross_amount=Decimal("100.00"),
        net_amount=Decimal("100.00"),
        payment_received=Decimal("60.00"),
        adjustments=Decimal("10.00"),
    )
    await _insert_order_and_sale(
        database_url=database_url,
        now=now,
        order_date=order_date,
        default_due_date=default_due_date,
        source_system="UClean",
        order_number="UC-001",
        gross_amount=Decimal("200.00"),
        net_amount=Decimal("200.00"),
        payment_received=Decimal("50.00"),
        adjustments=Decimal("25.00"),
    )

    data = await fetch_pending_deliveries_report(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    assert data.total_count == 2
    assert data.total_pending_amount == Decimal("180.00")
    assert len(data.summary_sections) == 1
    assert data.summary_sections[0].total_count == 2
    assert data.summary_sections[0].total_pending_amount == Decimal("180.00")

    detail_rows = [row for bucket in data.cost_center_sections[0].buckets for row in bucket.rows]
    assert {row.order_number for row in detail_rows} == {"TD-001", "UC-001"}
    assert {row.source_system for row in detail_rows} == {"TumbleDry", "UClean"}


@pytest.mark.asyncio
async def test_fetch_pending_deliveries_excludes_recovery_statuses_from_main_buckets(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "pending_deliveries_recovery_statuses.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr("app.reports.pending_deliveries.data.get_timezone", lambda: tz)
    now = datetime(2025, 5, 20, 10, 0, tzinfo=tz)
    order_date = datetime(2025, 5, 1, 10, 0, tzinfo=tz)
    default_due_date = datetime(2025, 5, 1, 10, 0, tzinfo=tz)

    excluded_statuses = [
        "TO_BE_RECOVERED",
        "TO_BE_COMPENSATED",
        "RECOVERED",
        "COMPENSATED",
        "WRITE_OFF",
    ]
    for status in excluded_statuses:
        await _insert_order_and_sale(
            database_url=database_url,
            now=now,
            order_date=order_date,
            default_due_date=default_due_date,
            source_system="UClean",
            order_number=f"EX-{status}",
            gross_amount=Decimal("100.00"),
            net_amount=Decimal("100.00"),
            payment_received=Decimal("50.00"),
            adjustments=Decimal("0.00"),
            recovery_status=status,
        )

    await _insert_order_and_sale(
        database_url=database_url,
        now=now,
        order_date=order_date,
        default_due_date=default_due_date,
        source_system="TumbleDry",
        order_number="ALLOWED-NULL",
        gross_amount=Decimal("120.00"),
        net_amount=Decimal("120.00"),
        payment_received=Decimal("20.00"),
        adjustments=Decimal("0.00"),
        recovery_status=None,
    )
    await _insert_order_and_sale(
        database_url=database_url,
        now=now,
        order_date=order_date,
        default_due_date=default_due_date,
        source_system="UClean",
        order_number="ALLOWED-CUSTOM",
        gross_amount=Decimal("130.00"),
        net_amount=Decimal("130.00"),
        payment_received=Decimal("30.00"),
        adjustments=Decimal("0.00"),
        recovery_status="IN_PROGRESS",
    )

    data = await fetch_pending_deliveries_report(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    summary_rows = [
        row
        for section in data.summary_sections
        for bucket in section.buckets
        for row in bucket.rows
    ]
    detail_rows = [
        row
        for section in data.cost_center_sections
        for bucket in section.buckets
        for row in bucket.rows
    ]

    assert {row.order_number for row in summary_rows} == {"ALLOWED-NULL", "ALLOWED-CUSTOM"}
    assert {row.order_number for row in detail_rows} == {"ALLOWED-NULL", "ALLOWED-CUSTOM"}
    assert data.total_count == 2
    assert data.total_pending_amount == Decimal("200.00")
