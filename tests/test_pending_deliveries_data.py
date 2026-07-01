from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest
import sqlalchemy as sa
from sqlalchemy import event
from zoneinfo import ZoneInfo

from app.common.db import _ensure_async_engine, session_scope
from app.crm_downloader.td_orders_sync.ingest import _orders_table
from app.crm_downloader.td_orders_sync.sales_ingest import _sales_table
from app.reports.pending_deliveries.data import (
    PENDING_DELIVERY_EXCLUDED_RECOVERY_STATUSES,
    PENDING_DELIVERY_MAIN_RECOVERY_STATUS,
    fetch_pending_deliveries_report,
    transition_aged_pending_deliveries_to_recovery_metrics,
    transition_aged_pending_deliveries_to_recovery,
)


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
        connection.execute(
            sa.text("ALTER TABLE orders ADD COLUMN recovery_status TEXT")
        )
        connection.execute(
            sa.text("ALTER TABLE orders ADD COLUMN recovery_category TEXT")
        )
        connection.execute(sa.text("ALTER TABLE orders ADD COLUMN recovery_notes TEXT"))
        connection.execute(sa.text("""
                CREATE TABLE payment_collections (
                    payment_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cost_center TEXT,
                    order_number TEXT,
                    amount NUMERIC DEFAULT 0,
                    source_type TEXT DEFAULT 'google_sheet'
                )
                """))
        connection.execute(sa.text("""
                CREATE VIEW vw_orders AS
                SELECT
                    cost_center,
                    store_code,
                    order_number,
                    customer_name,
                    order_date,
                    default_due_date,
                    source_system,
                    order_status,
                    COALESCE(recovery_status, 'NONE') AS recovery_status,
                    CASE
                        WHEN (
                            CASE
                                WHEN COALESCE(adjustment, 0) > 0 THEN
                                    COALESCE(
                                        CASE
                                            WHEN source_system = 'TumbleDry'
                                                 AND net_amount IS NOT NULL
                                                 AND net_amount <> 0
                                                THEN net_amount
                                            WHEN source_system = 'TumbleDry'
                                                THEN gross_amount
                                            ELSE gross_amount
                                        END,
                                        0
                                    ) - COALESCE(adjustment, 0)
                                ELSE
                                    COALESCE(
                                        CASE
                                            WHEN source_system = 'TumbleDry'
                                                 AND net_amount IS NOT NULL
                                                 AND net_amount <> 0
                                                THEN net_amount
                                            WHEN source_system = 'TumbleDry'
                                                THEN gross_amount
                                            ELSE gross_amount
                                        END,
                                        0
                                    )
                            END
                        ) <= 0 THEN 0
                        ELSE (
                            CASE
                                WHEN COALESCE(adjustment, 0) > 0 THEN
                                    COALESCE(
                                        CASE
                                            WHEN source_system = 'TumbleDry'
                                                 AND net_amount IS NOT NULL
                                                 AND net_amount <> 0
                                                THEN net_amount
                                            WHEN source_system = 'TumbleDry'
                                                THEN gross_amount
                                            ELSE gross_amount
                                        END,
                                        0
                                    ) - COALESCE(adjustment, 0)
                                ELSE
                                    COALESCE(
                                        CASE
                                            WHEN source_system = 'TumbleDry'
                                                 AND net_amount IS NOT NULL
                                                 AND net_amount <> 0
                                                THEN net_amount
                                            WHEN source_system = 'TumbleDry'
                                                THEN gross_amount
                                            ELSE gross_amount
                                        END,
                                        0
                                    )
                            END
                        )
                    END AS order_amount
                FROM orders
                """))
    engine.dispose()


_DEFAULT_RECOVERY_STATUS = PENDING_DELIVERY_MAIN_RECOVERY_STATUS


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
    recovery_status: str | None = _DEFAULT_RECOVERY_STATUS,
    order_status: str = "Pending",
    insert_sale: bool = True,
    sale_order_number: str | None = None,
    sale_cost_center: str = "UN3668",
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
            "order_status": order_status,
            "created_at": now,
        }
        await session.execute(sa.insert(orders).values(**order_values))
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
        if insert_sale:
            await session.execute(
                sa.insert(sales).values(
                    run_id="test-run",
                    run_date=now,
                    cost_center=sale_cost_center,
                    store_code="A668",
                    order_date=order_date,
                    payment_date=now,
                    order_number=sale_order_number or order_number,
                    customer_name=f"Customer-{order_number}",
                    payment_received=payment_received,
                    adjustments=adjustments,
                    payment_mode="Cash",
                    is_duplicate=False,
                    is_edited_order=False,
                )
            )
        await session.commit()


async def _insert_payment_collection(
    *,
    database_url: str,
    cost_center: str = "UN3668",
    order_number: str,
    amount: Decimal,
    source_type: str = "google_sheet",
) -> None:
    async with session_scope(database_url) as session:
        await session.execute(
            sa.text(
                "INSERT INTO payment_collections "
                "(cost_center, order_number, amount, source_type) "
                "VALUES (:cost_center, :order_number, :amount, :source_type)"
            ),
            {
                "cost_center": cost_center,
                "order_number": order_number,
                "amount": str(amount),
                "source_type": source_type,
            },
        )
        await session.commit()


async def _set_view_default_due_date_null_for_order(
    *, database_url: str, order_number: str
) -> None:
    escaped_order_number = order_number.replace("'", "''")
    async with session_scope(database_url) as session:
        await session.execute(sa.text("DROP VIEW IF EXISTS vw_orders"))
        await session.execute(sa.text(f"""
                CREATE VIEW vw_orders AS
                SELECT
                    cost_center,
                    store_code,
                    order_number,
                    customer_name,
                    order_date,
                    CASE WHEN order_number = '{escaped_order_number}' THEN NULL ELSE default_due_date END AS default_due_date,
                    source_system,
                    order_status,
                    COALESCE(recovery_status, 'NONE') AS recovery_status,
                    CASE
                        WHEN (
                            CASE
                                WHEN COALESCE(adjustment, 0) > 0 THEN
                                    COALESCE(
                                        CASE
                                            WHEN source_system = 'TumbleDry'
                                                 AND net_amount IS NOT NULL
                                                 AND net_amount <> 0
                                                THEN net_amount
                                            WHEN source_system = 'TumbleDry'
                                                THEN gross_amount
                                            ELSE gross_amount
                                        END,
                                        0
                                    ) - COALESCE(adjustment, 0)
                                ELSE
                                    COALESCE(
                                        CASE
                                            WHEN source_system = 'TumbleDry'
                                                 AND net_amount IS NOT NULL
                                                 AND net_amount <> 0
                                                THEN net_amount
                                            WHEN source_system = 'TumbleDry'
                                                THEN gross_amount
                                            ELSE gross_amount
                                        END,
                                        0
                                    )
                            END
                        ) <= 0 THEN 0
                        ELSE (
                            CASE
                                WHEN COALESCE(adjustment, 0) > 0 THEN
                                    COALESCE(
                                        CASE
                                            WHEN source_system = 'TumbleDry'
                                                 AND net_amount IS NOT NULL
                                                 AND net_amount <> 0
                                                THEN net_amount
                                            WHEN source_system = 'TumbleDry'
                                                THEN gross_amount
                                            ELSE gross_amount
                                        END,
                                        0
                                    ) - COALESCE(adjustment, 0)
                                ELSE
                                    COALESCE(
                                        CASE
                                            WHEN source_system = 'TumbleDry'
                                                 AND net_amount IS NOT NULL
                                                 AND net_amount <> 0
                                                THEN net_amount
                                            WHEN source_system = 'TumbleDry'
                                                THEN gross_amount
                                            ELSE gross_amount
                                        END,
                                        0
                                    )
                            END
                        )
                    END AS order_amount
                FROM orders
                """))
        await session.commit()


@pytest.mark.asyncio
async def test_fetch_pending_deliveries_includes_pending_order_with_no_sales_row(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "pending_deliveries_no_sale.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr("app.reports.pending_deliveries.data.get_timezone", lambda: tz)
    order_date = datetime(2025, 5, 18, 10, 0, tzinfo=tz)
    now = datetime(2025, 5, 20, 10, 0, tzinfo=tz)

    await _insert_order_and_sale(
        database_url=database_url,
        now=now,
        order_date=order_date,
        default_due_date=order_date,
        source_system="TumbleDry",
        order_number="NO-SALE",
        gross_amount=Decimal("2165.00"),
        net_amount=Decimal("2165.00"),
        payment_received=Decimal("0.00"),
        adjustments=Decimal("0.00"),
        insert_sale=False,
    )

    data = await fetch_pending_deliveries_report(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    assert data.total_count == 1
    assert data.total_pending_amount == Decimal("2165.00")
    assert len(data.summary_sections) == 1
    included_row = next(
        row
        for bucket in data.summary_sections[0].buckets
        for row in bucket.rows
        if row.order_number == "NO-SALE"
    )
    assert included_row.paid_amount == Decimal("0.00")
    assert included_row.pending_amount == Decimal("2165.00")
    assert included_row.default_due_date == date(2025, 5, 18)


@pytest.mark.asyncio
async def test_fetch_pending_deliveries_includes_non_pending_order_status_when_unpaid(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "pending_deliveries_non_pending_status.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr("app.reports.pending_deliveries.data.get_timezone", lambda: tz)
    order_date = datetime(2025, 5, 18, 10, 0, tzinfo=tz)
    now = datetime(2025, 5, 20, 10, 0, tzinfo=tz)

    await _insert_order_and_sale(
        database_url=database_url,
        now=now,
        order_date=order_date,
        default_due_date=order_date,
        source_system="TumbleDry",
        order_number="READY-NO-PROOF",
        gross_amount=Decimal("2165.00"),
        net_amount=Decimal("2165.00"),
        payment_received=Decimal("0.00"),
        adjustments=Decimal("0.00"),
        order_status="Ready",
        insert_sale=False,
    )

    data = await fetch_pending_deliveries_report(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    reported_orders = {
        row.order_number
        for section in data.summary_sections
        for bucket in section.buckets
        for row in bucket.rows
    }
    assert reported_orders == {"READY-NO-PROOF"}
    assert data.total_pending_amount == Decimal("2165.00")


@pytest.mark.asyncio
async def test_fetch_pending_deliveries_excludes_sufficient_payment_collection_proof(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "pending_deliveries_sufficient_proof.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr("app.reports.pending_deliveries.data.get_timezone", lambda: tz)
    order_date = datetime(2025, 5, 18, 10, 0, tzinfo=tz)
    now = datetime(2025, 5, 20, 10, 0, tzinfo=tz)

    await _insert_order_and_sale(
        database_url=database_url,
        now=now,
        order_date=order_date,
        default_due_date=order_date,
        source_system="TumbleDry",
        order_number="ENOUGH-PROOF",
        gross_amount=Decimal("100.00"),
        net_amount=Decimal("100.00"),
        payment_received=Decimal("0.00"),
        adjustments=Decimal("0.00"),
        insert_sale=False,
    )
    await _insert_payment_collection(
        database_url=database_url,
        order_number=" enough-proof ",
        amount=Decimal("99.00"),
    )

    data = await fetch_pending_deliveries_report(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    assert data.total_count == 0
    assert data.total_pending_amount == Decimal("0")


@pytest.mark.asyncio
async def test_fetch_pending_deliveries_excludes_orders_older_than_30_days(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "pending_deliveries_older_than_30.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr("app.reports.pending_deliveries.data.get_timezone", lambda: tz)
    report_date = date(2025, 5, 20)
    now = datetime(2025, 5, 20, 10, 0, tzinfo=tz)
    old_due_date = datetime(2025, 4, 19, 10, 0, tzinfo=tz)

    await _insert_order_and_sale(
        database_url=database_url,
        now=now,
        order_date=old_due_date,
        default_due_date=old_due_date,
        source_system="TumbleDry",
        order_number="AGE-31-FETCH",
        gross_amount=Decimal("100.00"),
        net_amount=Decimal("100.00"),
        payment_received=Decimal("0.00"),
        adjustments=Decimal("0.00"),
        insert_sale=False,
    )

    data = await fetch_pending_deliveries_report(
        database_url=database_url,
        report_date=report_date,
    )

    assert data.total_count == 0
    assert data.summary_sections == []


@pytest.mark.asyncio
async def test_fetch_pending_deliveries_falls_back_to_order_date_plus_two_days_when_default_due_date_missing(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "pending_deliveries_missing_due_date.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr("app.reports.pending_deliveries.data.get_timezone", lambda: tz)
    order_date = datetime(2025, 5, 18, 10, 0, tzinfo=tz)
    now = datetime(2025, 5, 20, 10, 0, tzinfo=tz)

    await _insert_order_and_sale(
        database_url=database_url,
        now=now,
        order_date=order_date,
        default_due_date=order_date,
        source_system="TumbleDry",
        order_number="NO-DUE-DATE",
        gross_amount=Decimal("2165.00"),
        net_amount=Decimal("2165.00"),
        payment_received=Decimal("0.00"),
        adjustments=Decimal("0.00"),
        insert_sale=False,
    )
    await _set_view_default_due_date_null_for_order(
        database_url=database_url,
        order_number="NO-DUE-DATE",
    )

    data = await fetch_pending_deliveries_report(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    assert data.total_count == 1
    assert data.missing_default_due_date_count == 1
    included_row = next(
        row
        for bucket in data.summary_sections[0].buckets
        for row in bucket.rows
        if row.order_number == "NO-DUE-DATE"
    )
    assert included_row.default_due_date == date(2025, 5, 20)
    assert included_row.age_days == 2


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
    order_date = datetime(2025, 5, 18, 10, 0, tzinfo=tz)
    default_due_date = datetime(2025, 5, 18, 10, 0, tzinfo=tz)

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
        insert_sale=False,
    )
    await _insert_order_and_sale(
        database_url=database_url,
        now=now,
        order_date=order_date,
        default_due_date=default_due_date,
        source_system="UClean",
        order_number="UC-001",
        gross_amount=Decimal("200.00"),
        net_amount=Decimal("150.00"),
        payment_received=Decimal("50.00"),
        adjustments=Decimal("25.00"),
        insert_sale=False,
    )

    data = await fetch_pending_deliveries_report(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    assert data.total_count == 2
    assert data.total_pending_amount == Decimal("300.00")
    assert len(data.summary_sections) == 1
    assert data.summary_sections[0].total_count == 2
    assert data.summary_sections[0].total_pending_amount == Decimal("300.00")

    detail_rows = [
        row for bucket in data.cost_center_sections[0].buckets for row in bucket.rows
    ]
    assert {row.order_number for row in detail_rows} == {"TD-001", "UC-001"}
    assert {row.source_system for row in detail_rows} == {"TumbleDry", "UClean"}
    assert {row.order_number: row.order_amount for row in detail_rows}[
        "UC-001"
    ] == Decimal("200.00")
    assert {row.order_number: row.pending_amount for row in detail_rows}[
        "UC-001"
    ] == Decimal("200.00")


@pytest.mark.asyncio
async def test_fetch_pending_deliveries_includes_zero_value_order_without_payment_proof(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "pending_deliveries_zero_no_proof.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr("app.reports.pending_deliveries.data.get_timezone", lambda: tz)
    now = datetime(2025, 5, 20, 10, 0, tzinfo=tz)
    order_date = datetime(2025, 5, 20, 10, 0, tzinfo=tz)

    await _insert_order_and_sale(
        database_url=database_url,
        now=now,
        order_date=order_date,
        default_due_date=order_date,
        source_system="TumbleDry",
        order_number="ZERO-NO-PROOF",
        gross_amount=Decimal("0.00"),
        net_amount=Decimal("0.00"),
        payment_received=Decimal("0.00"),
        adjustments=Decimal("0.00"),
        insert_sale=False,
    )

    data = await fetch_pending_deliveries_report(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    rows = [
        row
        for section in data.summary_sections
        for bucket in section.buckets
        for row in bucket.rows
    ]
    assert {row.order_number for row in rows} == {"ZERO-NO-PROOF"}
    assert rows[0].order_amount == Decimal("0.00")
    assert rows[0].pending_amount == Decimal("0")


@pytest.mark.asyncio
async def test_fetch_pending_deliveries_excludes_zero_value_order_with_zero_amount_proof(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "pending_deliveries_zero_with_proof.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr("app.reports.pending_deliveries.data.get_timezone", lambda: tz)
    now = datetime(2025, 5, 20, 10, 0, tzinfo=tz)
    order_date = datetime(2025, 5, 20, 10, 0, tzinfo=tz)

    await _insert_order_and_sale(
        database_url=database_url,
        now=now,
        order_date=order_date,
        default_due_date=order_date,
        source_system="TumbleDry",
        order_number="ZERO-WITH-PROOF",
        gross_amount=Decimal("0.00"),
        net_amount=Decimal("0.00"),
        payment_received=Decimal("0.00"),
        adjustments=Decimal("0.00"),
        insert_sale=False,
    )
    await _insert_payment_collection(
        database_url=database_url,
        order_number=" zero-with-proof ",
        amount=Decimal("0.00"),
    )

    data = await fetch_pending_deliveries_report(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    assert data.total_count == 0
    assert data.summary_sections == []


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
    order_date = datetime(2025, 5, 18, 10, 0, tzinfo=tz)
    default_due_date = datetime(2025, 5, 18, 10, 0, tzinfo=tz)

    excluded_statuses = [
        "IN_PROGRESS",
        *PENDING_DELIVERY_EXCLUDED_RECOVERY_STATUSES,
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
            insert_sale=False,
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
        insert_sale=False,
    )
    await _insert_order_and_sale(
        database_url=database_url,
        now=now,
        order_date=order_date,
        default_due_date=default_due_date,
        source_system="UClean",
        order_number="ALLOWED-NONE",
        gross_amount=Decimal("130.00"),
        net_amount=Decimal("130.00"),
        payment_received=Decimal("30.00"),
        adjustments=Decimal("0.00"),
        recovery_status=PENDING_DELIVERY_MAIN_RECOVERY_STATUS,
        insert_sale=False,
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

    assert {row.order_number for row in summary_rows} == {
        "ALLOWED-NULL",
        "ALLOWED-NONE",
    }
    assert {row.order_number for row in detail_rows} == {"ALLOWED-NULL", "ALLOWED-NONE"}
    assert not {row.order_number for row in detail_rows} & {
        f"EX-{status}" for status in excluded_statuses
    }
    assert data.total_count == 2
    assert data.total_pending_amount == Decimal("250.00")


@pytest.mark.asyncio
@pytest.mark.parametrize("recovery_status", PENDING_DELIVERY_EXCLUDED_RECOVERY_STATUSES)
async def test_each_recovery_status_is_excluded_from_pending_delivery_action_buckets(
    tmp_path, monkeypatch, recovery_status: str
) -> None:
    db_path = tmp_path / f"pending_deliveries_excludes_{recovery_status.lower()}.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr("app.reports.pending_deliveries.data.get_timezone", lambda: tz)
    now = datetime(2025, 5, 20, 10, 0, tzinfo=tz)
    order_date = datetime(2025, 5, 18, 10, 0, tzinfo=tz)

    await _insert_order_and_sale(
        database_url=database_url,
        now=now,
        order_date=order_date,
        default_due_date=order_date,
        source_system="UClean",
        order_number=f"EXCLUDED-{recovery_status}",
        gross_amount=Decimal("100.00"),
        net_amount=Decimal("100.00"),
        payment_received=Decimal("0.00"),
        adjustments=Decimal("0.00"),
        recovery_status=recovery_status,
    )
    await _insert_order_and_sale(
        database_url=database_url,
        now=now,
        order_date=order_date,
        default_due_date=order_date,
        source_system="TumbleDry",
        order_number="VISIBLE-PENDING",
        gross_amount=Decimal("100.00"),
        net_amount=Decimal("100.00"),
        payment_received=Decimal("0.00"),
        adjustments=Decimal("0.00"),
        recovery_status=None,
        insert_sale=False,
    )

    data = await fetch_pending_deliveries_report(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    action_bucket_orders = {
        row.order_number
        for section in data.cost_center_sections
        for bucket in section.buckets
        for row in bucket.rows
    }
    assert action_bucket_orders == {"VISIBLE-PENDING"}
    assert f"EXCLUDED-{recovery_status}" not in action_bucket_orders


@pytest.mark.asyncio
async def test_fetch_pending_deliveries_excludes_orders_with_any_matching_sales_row(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "pending_deliveries_sales_anti_join.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr("app.reports.pending_deliveries.data.get_timezone", lambda: tz)
    now = datetime(2025, 5, 20, 10, 0, tzinfo=tz)
    order_date = datetime(2025, 5, 18, 10, 0, tzinfo=tz)

    await _insert_order_and_sale(
        database_url=database_url,
        now=now,
        order_date=order_date,
        default_due_date=order_date,
        source_system="TumbleDry",
        order_number="NO-SALE",
        gross_amount=Decimal("100.00"),
        net_amount=Decimal("100.00"),
        payment_received=Decimal("0.00"),
        adjustments=Decimal("0.00"),
        insert_sale=False,
    )
    await _insert_order_and_sale(
        database_url=database_url,
        now=now,
        order_date=order_date,
        default_due_date=order_date,
        source_system="TumbleDry",
        order_number="FULL-PAY",
        gross_amount=Decimal("100.00"),
        net_amount=Decimal("100.00"),
        payment_received=Decimal("100.00"),
        adjustments=Decimal("0.00"),
    )
    await _insert_order_and_sale(
        database_url=database_url,
        now=now,
        order_date=order_date,
        default_due_date=order_date,
        source_system="TumbleDry",
        order_number="SHORT-PAY",
        gross_amount=Decimal("100.00"),
        net_amount=Decimal("100.00"),
        payment_received=Decimal("25.00"),
        adjustments=Decimal("0.00"),
        sale_order_number=" short-pay ",
        sale_cost_center=" un3668 ",
    )

    data = await fetch_pending_deliveries_report(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    reported_orders = {
        row.order_number
        for section in data.summary_sections
        for bucket in section.buckets
        for row in bucket.rows
    }
    assert reported_orders == {"NO-SALE"}
    assert data.total_pending_amount == Decimal("100.00")


async def _fetch_recovery_rows(database_url: str) -> dict[str, dict[str, object]]:
    async with session_scope(database_url) as session:
        rows = (await session.execute(sa.text("""
                        SELECT order_number, recovery_status, recovery_category, recovery_notes
                        FROM orders
                        ORDER BY order_number
                        """))).mappings().all()
    return {str(row["order_number"]): dict(row) for row in rows}


async def _seed_transition_order(
    *,
    database_url: str,
    monkeypatch,
    order_number: str,
    age_days: int,
    recovery_status: str | None = PENDING_DELIVERY_MAIN_RECOVERY_STATUS,
    insert_sale: bool = False,
    recovery_notes: str | None = None,
    order_status: str = "Pending",
) -> None:
    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr("app.reports.pending_deliveries.data.get_timezone", lambda: tz)
    report_date = date(2025, 5, 20)
    now = datetime(2025, 5, 20, 10, 0, tzinfo=tz)
    due_date = datetime.combine(
        report_date - timedelta(days=age_days),
        datetime.min.time(),
        tzinfo=tz,
    ).replace(hour=10)
    await _insert_order_and_sale(
        database_url=database_url,
        now=now,
        order_date=due_date,
        default_due_date=due_date,
        source_system="TumbleDry",
        order_number=order_number,
        gross_amount=Decimal("100.00"),
        net_amount=Decimal("100.00"),
        payment_received=Decimal("0.00"),
        adjustments=Decimal("0.00"),
        recovery_status=recovery_status,
        order_status=order_status,
        insert_sale=insert_sale,
    )
    if recovery_notes is not None:
        async with session_scope(database_url) as session:
            await session.execute(
                sa.text(
                    "UPDATE orders SET recovery_notes = :notes "
                    "WHERE order_number = :order_number"
                ),
                {"notes": recovery_notes, "order_number": order_number},
            )
            await session.commit()


@pytest.mark.asyncio
async def test_transition_aged_unresolved_non_pending_order_marked(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "pending_transition_non_pending_status.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    await _seed_transition_order(
        database_url=database_url,
        monkeypatch=monkeypatch,
        order_number="READY-AGED",
        age_days=31,
        order_status="Ready",
    )

    metrics = await transition_aged_pending_deliveries_to_recovery_metrics(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    rows = await _fetch_recovery_rows(database_url)
    assert metrics.scanned_count == 1
    assert metrics.eligible_count == 1
    assert metrics.transitioned_count == 1
    assert rows["READY-AGED"]["recovery_status"] == "TO_BE_RECOVERED"
    assert rows["READY-AGED"]["recovery_category"] is None


@pytest.mark.asyncio
async def test_transition_aged_order_with_sufficient_payment_proof_not_marked(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "pending_transition_sufficient_proof.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    await _seed_transition_order(
        database_url=database_url,
        monkeypatch=monkeypatch,
        order_number="AGED-PAID",
        age_days=31,
    )
    await _insert_payment_collection(
        database_url=database_url,
        order_number=" aged-paid ",
        amount=Decimal("100.00"),
    )

    metrics = await transition_aged_pending_deliveries_to_recovery_metrics(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    rows = await _fetch_recovery_rows(database_url)
    assert metrics.eligible_count == 1
    assert metrics.transitioned_count == 0
    assert rows["AGED-PAID"]["recovery_status"] == "NONE"
    assert rows["AGED-PAID"]["recovery_category"] is None


@pytest.mark.asyncio
async def test_transition_aged_order_with_matching_sales_row_not_marked(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "pending_transition_matching_sale.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    await _seed_transition_order(
        database_url=database_url,
        monkeypatch=monkeypatch,
        order_number="AGED-HAS-SALE",
        age_days=31,
        insert_sale=True,
    )

    metrics = await transition_aged_pending_deliveries_to_recovery_metrics(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    rows = await _fetch_recovery_rows(database_url)
    assert metrics.scanned_count == 1
    assert metrics.skipped_due_to_sales_row == 1
    assert metrics.transitioned_count == 0
    assert rows["AGED-HAS-SALE"]["recovery_status"] == "NONE"
    assert rows["AGED-HAS-SALE"]["recovery_category"] is None


@pytest.mark.asyncio
async def test_transition_age_at_or_below_threshold_not_marked(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "pending_transition_age_threshold.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    for age_days in (29, 30):
        await _seed_transition_order(
            database_url=database_url,
            monkeypatch=monkeypatch,
            order_number=f"AGE-{age_days}-THRESHOLD",
            age_days=age_days,
        )

    metrics = await transition_aged_pending_deliveries_to_recovery_metrics(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    rows = await _fetch_recovery_rows(database_url)
    assert metrics.scanned_count == 2
    assert metrics.skipped_due_to_age == 2
    assert metrics.transitioned_count == 0
    assert {row["recovery_status"] for row in rows.values()} == {"NONE"}
    assert {row["recovery_category"] for row in rows.values()} == {None}


@pytest.mark.asyncio
async def test_transition_age_30_not_marked(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "pending_transition_age_30.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    await _seed_transition_order(
        database_url=database_url,
        monkeypatch=monkeypatch,
        order_number="AGE-30",
        age_days=30,
    )

    transitioned_count = await transition_aged_pending_deliveries_to_recovery(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    rows = await _fetch_recovery_rows(database_url)
    assert transitioned_count == 0
    assert rows["AGE-30"]["recovery_status"] == "NONE"
    assert rows["AGE-30"]["recovery_category"] is None
    assert rows["AGE-30"]["recovery_notes"] is None


@pytest.mark.asyncio
async def test_transition_age_31_marked_without_recovery_category(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "pending_transition_age_31.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    await _seed_transition_order(
        database_url=database_url,
        monkeypatch=monkeypatch,
        order_number="AGE-31",
        age_days=31,
    )

    transitioned_count = await transition_aged_pending_deliveries_to_recovery(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    rows = await _fetch_recovery_rows(database_url)
    assert transitioned_count == 1
    assert rows["AGE-31"]["recovery_status"] == "TO_BE_RECOVERED"
    assert rows["AGE-31"]["recovery_category"] is None
    assert (
        rows["AGE-31"]["recovery_notes"]
        == "Auto marked as TO_BE_RECOVERED by system on 20-May-2025 [2025-05-20T00:00:00+05:30]"
    )


@pytest.mark.asyncio
async def test_transition_no_sales_no_payment_proof_marked(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "pending_transition_no_proof.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    await _seed_transition_order(
        database_url=database_url,
        monkeypatch=monkeypatch,
        order_number="NO-PROOF",
        age_days=31,
    )

    metrics = await transition_aged_pending_deliveries_to_recovery_metrics(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    rows = await _fetch_recovery_rows(database_url)
    assert metrics.eligible_count == 1
    assert metrics.transitioned_count == 1
    assert rows["NO-PROOF"]["recovery_status"] == "TO_BE_RECOVERED"


@pytest.mark.asyncio
async def test_transition_no_sales_with_full_verified_payment_proof_not_marked(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "pending_transition_verified_proof.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    await _seed_transition_order(
        database_url=database_url,
        monkeypatch=monkeypatch,
        order_number="PAID-PROOF",
        age_days=31,
    )
    await _insert_payment_collection(
        database_url=database_url,
        order_number=" paid-proof ",
        amount=Decimal("100.00"),
    )

    metrics = await transition_aged_pending_deliveries_to_recovery_metrics(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    rows = await _fetch_recovery_rows(database_url)
    assert metrics.eligible_count == 1
    assert metrics.transitioned_count == 0
    assert rows["PAID-PROOF"]["recovery_status"] == "NONE"


@pytest.mark.asyncio
async def test_transition_grouped_payment_proof_uses_comma_slash_tokens(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "pending_transition_grouped_proof.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    for order_number in ("GROUP-A", "GROUP-B", "GROUP-C"):
        await _seed_transition_order(
            database_url=database_url,
            monkeypatch=monkeypatch,
            order_number=order_number,
            age_days=31,
        )
    await _insert_payment_collection(
        database_url=database_url,
        order_number="group-a, group-b/group-c",
        amount=Decimal("300.00"),
    )

    metrics = await transition_aged_pending_deliveries_to_recovery_metrics(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    rows = await _fetch_recovery_rows(database_url)
    assert metrics.eligible_count == 3
    assert metrics.transitioned_count == 0
    assert {rows[order]["recovery_status"] for order in rows} == {"NONE"}


@pytest.mark.asyncio
async def test_transition_mixed_unmatched_grouped_payment_tokens_do_not_block_recovery(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "pending_transition_unmatched_grouped_proof.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    for order_number in ("MATCHED-A", "MATCHED-B"):
        await _seed_transition_order(
            database_url=database_url,
            monkeypatch=monkeypatch,
            order_number=order_number,
            age_days=31,
        )
    await _insert_payment_collection(
        database_url=database_url,
        order_number="matched-a, unmatched-token / matched-b",
        amount=Decimal("200.00"),
    )

    metrics = await transition_aged_pending_deliveries_to_recovery_metrics(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    rows = await _fetch_recovery_rows(database_url)
    assert metrics.eligible_count == 2
    assert metrics.transitioned_count == 2
    recovery_statuses = {
        rows[order]["recovery_status"] for order in ("MATCHED-A", "MATCHED-B")
    }
    assert recovery_statuses == {"TO_BE_RECOVERED"}


@pytest.mark.asyncio
async def test_transition_proof_only_without_sales_still_requires_sufficient_amount(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "pending_transition_proof_only_short.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    await _seed_transition_order(
        database_url=database_url,
        monkeypatch=monkeypatch,
        order_number="PROOF-ONLY-SHORT",
        age_days=31,
    )
    await _insert_payment_collection(
        database_url=database_url,
        order_number="PROOF-ONLY-SHORT",
        amount=Decimal("98.00"),
    )

    metrics = await transition_aged_pending_deliveries_to_recovery_metrics(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    rows = await _fetch_recovery_rows(database_url)
    assert metrics.eligible_count == 1
    assert metrics.transitioned_count == 1
    assert rows["PROOF-ONLY-SHORT"]["recovery_status"] == "TO_BE_RECOVERED"


@pytest.mark.asyncio
async def test_transition_unsupported_payment_source_type_does_not_block_recovery(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "pending_transition_unsupported_source.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    await _seed_transition_order(
        database_url=database_url,
        monkeypatch=monkeypatch,
        order_number="UNSUPPORTED-PROOF",
        age_days=31,
    )
    await _insert_payment_collection(
        database_url=database_url,
        order_number="UNSUPPORTED-PROOF",
        amount=Decimal("100.00"),
        source_type="manual_upload",
    )

    metrics = await transition_aged_pending_deliveries_to_recovery_metrics(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    rows = await _fetch_recovery_rows(database_url)
    assert metrics.eligible_count == 1
    assert metrics.transitioned_count == 1
    assert rows["UNSUPPORTED-PROOF"]["recovery_status"] == "TO_BE_RECOVERED"


@pytest.mark.asyncio
async def test_fetch_after_transition_keeps_ages_0_and_3_in_0_3_bucket_and_excludes_4(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "pending_transition_bucket_boundaries.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    for age_days in (0, 3, 4):
        await _seed_transition_order(
            database_url=database_url,
            monkeypatch=monkeypatch,
            order_number=f"AGE-{age_days}",
            age_days=age_days,
        )

    transitioned_count = await transition_aged_pending_deliveries_to_recovery(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )
    data = await fetch_pending_deliveries_report(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    assert transitioned_count == 0
    assert [bucket.label for bucket in data.summary_sections[0].buckets] == [
        "0-3 days",
    ]
    pending_bucket = data.summary_sections[0].buckets[0]
    assert {row.order_number for row in pending_bucket.rows} == {"AGE-0", "AGE-3"}
    assert {row.age_days for row in pending_bucket.rows} == {0, 3}
    assert {
        row.order_number
        for section in data.cost_center_sections
        for bucket in section.buckets
        for row in bucket.rows
    } == {"AGE-0", "AGE-3"}


@pytest.mark.asyncio
async def test_transition_existing_sales_row_not_marked(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "pending_transition_sales_row.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    await _seed_transition_order(
        database_url=database_url,
        monkeypatch=monkeypatch,
        order_number="HAS-SALE",
        age_days=31,
        insert_sale=True,
    )

    transitioned_count = await transition_aged_pending_deliveries_to_recovery(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    rows = await _fetch_recovery_rows(database_url)
    assert transitioned_count == 0
    assert rows["HAS-SALE"]["recovery_status"] == "NONE"
    assert rows["HAS-SALE"]["recovery_category"] is None
    assert rows["HAS-SALE"]["recovery_notes"] is None


@pytest.mark.asyncio
async def test_transition_missing_default_due_date_uses_order_date_plus_two_days_fallback(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "pending_transition_missing_due_date.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr("app.reports.pending_deliveries.data.get_timezone", lambda: tz)
    report_date = date(2025, 5, 20)
    now = datetime(2025, 5, 20, 10, 0, tzinfo=tz)
    order_date = datetime(2025, 4, 10, 10, 0, tzinfo=tz)
    await _insert_order_and_sale(
        database_url=database_url,
        now=now,
        order_date=order_date,
        default_due_date=order_date,
        source_system="TumbleDry",
        order_number="AGE-BY-ORDER-DATE",
        gross_amount=Decimal("100.00"),
        net_amount=Decimal("100.00"),
        payment_received=Decimal("0.00"),
        adjustments=Decimal("0.00"),
        insert_sale=False,
    )
    await _set_view_default_due_date_null_for_order(
        database_url=database_url,
        order_number="AGE-BY-ORDER-DATE",
    )

    metrics = await transition_aged_pending_deliveries_to_recovery_metrics(
        database_url=database_url,
        report_date=report_date,
    )

    rows = await _fetch_recovery_rows(database_url)
    assert metrics.skipped_due_to_missing_due_date == 1
    assert metrics.transitioned_count == 1
    assert rows["AGE-BY-ORDER-DATE"]["recovery_status"] == "TO_BE_RECOVERED"


@pytest.mark.asyncio
async def test_transition_non_none_status_not_overwritten(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "pending_transition_non_none.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    await _seed_transition_order(
        database_url=database_url,
        monkeypatch=monkeypatch,
        order_number="WRITE-OFF",
        age_days=31,
        recovery_status="WRITE_OFF",
        recovery_notes="manual decision",
    )

    transitioned_count = await transition_aged_pending_deliveries_to_recovery(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    rows = await _fetch_recovery_rows(database_url)
    assert transitioned_count == 0
    assert rows["WRITE-OFF"]["recovery_status"] == "WRITE_OFF"
    assert rows["WRITE-OFF"]["recovery_category"] is None
    assert rows["WRITE-OFF"]["recovery_notes"] == "manual decision"


@pytest.mark.asyncio
async def test_transition_existing_note_remains_intact_after_auto_marking(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "pending_transition_preserve_note.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    existing_note = "operator note\nfollow-up promised"
    await _seed_transition_order(
        database_url=database_url,
        monkeypatch=monkeypatch,
        order_number="PRESERVE-NOTE",
        age_days=31,
        recovery_notes=existing_note,
    )

    transitioned_count = await transition_aged_pending_deliveries_to_recovery(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    rows = await _fetch_recovery_rows(database_url)
    expected_note = "Auto marked as TO_BE_RECOVERED by system on 20-May-2025 [2025-05-20T00:00:00+05:30]"
    assert transitioned_count == 1
    assert rows["PRESERVE-NOTE"]["recovery_status"] == "TO_BE_RECOVERED"
    assert (
        rows["PRESERVE-NOTE"]["recovery_notes"] == f"{existing_note}\n{expected_note}"
    )


@pytest.mark.asyncio
async def test_transition_repeated_run_does_not_duplicate_notes(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "pending_transition_idempotent.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    await _register_sqlite_greatest(database_url)

    await _seed_transition_order(
        database_url=database_url,
        monkeypatch=monkeypatch,
        order_number="IDEMPOTENT",
        age_days=31,
        recovery_notes="operator note",
    )

    first_count = await transition_aged_pending_deliveries_to_recovery(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )
    second_count = await transition_aged_pending_deliveries_to_recovery(
        database_url=database_url,
        report_date=date(2025, 5, 20),
    )

    rows = await _fetch_recovery_rows(database_url)
    expected_note = "Auto marked as TO_BE_RECOVERED by system on 20-May-2025 [2025-05-20T00:00:00+05:30]"
    assert first_count == 1
    assert second_count == 0
    assert rows["IDEMPOTENT"]["recovery_status"] == "TO_BE_RECOVERED"
    assert rows["IDEMPOTENT"]["recovery_notes"] == f"operator note\n{expected_note}"
    assert rows["IDEMPOTENT"]["recovery_notes"].count(expected_note) == 1
