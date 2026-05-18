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
        connection.execute(sa.text("ALTER TABLE orders ADD COLUMN recovery_status TEXT"))
        connection.execute(sa.text("ALTER TABLE orders ADD COLUMN recovery_category TEXT"))
        connection.execute(sa.text("ALTER TABLE orders ADD COLUMN recovery_notes TEXT"))
        connection.execute(
            sa.text(
                """
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
                """
            )
        )
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
            "order_status": "Pending",
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
    order_date = datetime(2025, 5, 10, 10, 0, tzinfo=tz)
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

    detail_rows = [row for bucket in data.cost_center_sections[0].buckets for row in bucket.rows]
    assert {row.order_number for row in detail_rows} == {"TD-001", "UC-001"}
    assert {row.source_system for row in detail_rows} == {"TumbleDry", "UClean"}
    assert {row.order_number: row.order_amount for row in detail_rows}["UC-001"] == Decimal("200.00")
    assert {row.order_number: row.pending_amount for row in detail_rows}["UC-001"] == Decimal("200.00")


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

    assert {row.order_number for row in summary_rows} == {"ALLOWED-NULL", "ALLOWED-NONE"}
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
    order_date = datetime(2025, 5, 1, 10, 0, tzinfo=tz)

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
    order_date = datetime(2025, 5, 1, 10, 0, tzinfo=tz)

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
        rows = (
            (
                await session.execute(
                    sa.text(
                        """
                        SELECT order_number, recovery_status, recovery_category, recovery_notes
                        FROM orders
                        ORDER BY order_number
                        """
                    )
                )
            )
            .mappings()
            .all()
        )
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
async def test_transition_age_31_marked(tmp_path, monkeypatch) -> None:
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
    assert rows["AGE-31"]["recovery_category"] == "OTHER"
    assert (
        rows["AGE-31"]["recovery_notes"]
        == "Auto marked as TO_BE_RECOVERED by system on 20-May-2025"
    )


@pytest.mark.asyncio
async def test_transition_existing_sales_row_not_marked(
    tmp_path, monkeypatch
) -> None:
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
    expected_note = "Auto marked as TO_BE_RECOVERED by system on 20-May-2025"
    assert first_count == 1
    assert second_count == 0
    assert rows["IDEMPOTENT"]["recovery_status"] == "TO_BE_RECOVERED"
    assert rows["IDEMPOTENT"]["recovery_notes"] == f"operator note\n{expected_note}"
    assert rows["IDEMPOTENT"]["recovery_notes"].count(expected_note) == 1
