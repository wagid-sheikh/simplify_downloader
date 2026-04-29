from contextlib import asynccontextmanager
from datetime import date, datetime
from zoneinfo import ZoneInfo

from sqlalchemy.dialects import postgresql

import pytest
import sqlalchemy as sa

from app.common.db import session_scope
from app.reports.mtd_same_day_fulfillment.data import fetch_mtd_same_day_fulfillment
from app.reports.mtd_same_day_fulfillment.data import MTDSameDayFulfillmentRow
from app.reports.mtd_same_day_fulfillment.render import render_html
import app.reports.mtd_same_day_fulfillment.data as mtd_data


def _create_tables(database_url: str) -> None:
    engine = sa.create_engine(database_url.replace('+aiosqlite', ''))
    with engine.begin() as conn:
        conn.execute(sa.text("CREATE TABLE orders (cost_center TEXT, order_number TEXT, order_date TIMESTAMP, customer_name TEXT, mobile_number TEXT, net_amount NUMERIC)"))
        conn.execute(sa.text("CREATE TABLE order_line_items (cost_center TEXT, order_number TEXT, service_name TEXT, garment_name TEXT)"))
        conn.execute(sa.text("CREATE TABLE sales (cost_center TEXT, order_number TEXT, payment_date TIMESTAMP, payment_mode TEXT, payment_received NUMERIC)"))
        conn.execute(sa.text("CREATE TABLE store_master (cost_center TEXT, store_code TEXT)"))
    engine.dispose()


@pytest.mark.asyncio
async def test_fetch_mtd_same_day_fulfillment_filters_and_aggregates(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / 'mtd_same_day.db'
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    monkeypatch.setattr(mtd_data, 'get_timezone', lambda: ZoneInfo('Asia/Kolkata'))

    async with session_scope(database_url) as session:
        await session.execute(sa.text("INSERT INTO store_master (cost_center, store_code) VALUES ('CC1', 'S1')"))
        await session.execute(sa.text("INSERT INTO orders (cost_center, order_number, order_date, customer_name, mobile_number, net_amount) VALUES ('CC1','O1','2026-04-10T09:00:00+05:30','Alice','9999999999',800),('CC1','O2','2026-03-30T09:00:00+05:30','Bob','8888888888',700)"))
        await session.execute(sa.text("INSERT INTO order_line_items (cost_center, order_number, service_name, garment_name) VALUES ('CC1','O1','Wash','Shirt'),('CC1','O1','Iron','Pant'),('CC1','O2','Dry','Coat')"))
        await session.execute(sa.text("INSERT INTO sales (cost_center, order_number, payment_date, payment_mode, payment_received) VALUES ('CC1','O1','2026-04-10T10:00:00+05:30','UPI',500),('CC1','O1','2026-04-10T11:00:00+05:30','UPI',300),('CC1','O2','2026-04-10T11:00:00+05:30','CARD',700),('CC1','O1','2026-04-11T00:10:00+05:30','CASH',50)"))
        await session.commit()

    rows = await fetch_mtd_same_day_fulfillment(database_url=database_url, report_date=date(2026,4,29))
    assert len(rows) == 1
    assert rows[0].order_number == 'O1'
    assert rows[0].line_items == "Iron Pant × 1 | Wash Shirt × 1"
    assert rows[0].net_amount == 800
    assert rows[0].payment_received == 800
    assert rows[0].hours == 2.0


@pytest.mark.asyncio
async def test_fetch_mtd_same_day_fulfillment_does_not_use_create_engine(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / 'mtd_same_day_no_reflection.db'
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    monkeypatch.setattr(mtd_data, 'get_timezone', lambda: ZoneInfo('Asia/Kolkata'))

    def _fail_create_engine(*args, **kwargs):
        raise AssertionError("create_engine should not be used in fetch_mtd_same_day_fulfillment")

    monkeypatch.setattr(mtd_data.sa, "create_engine", _fail_create_engine)

    async with session_scope(database_url) as session:
        await session.execute(sa.text("INSERT INTO store_master (cost_center, store_code) VALUES ('CC1', 'S1')"))
        await session.execute(sa.text("INSERT INTO orders (cost_center, order_number, order_date, customer_name, mobile_number, net_amount) VALUES ('CC1','O1','2026-04-10T09:00:00+05:30','Alice','9999999999',800)"))
        await session.execute(sa.text("INSERT INTO order_line_items (cost_center, order_number, service_name, garment_name) VALUES ('CC1','O1','Wash',NULL),('CC1','O1',NULL,'Trouser')"))
        await session.execute(sa.text("INSERT INTO sales (cost_center, order_number, payment_date, payment_mode, payment_received) VALUES ('CC1','O1','2026-04-10T10:00:00+05:30','UPI',800)"))
        await session.commit()

    rows = await fetch_mtd_same_day_fulfillment(database_url=database_url, report_date=date(2026, 4, 29))
    assert len(rows) == 1
    assert rows[0].order_number == "O1"
    assert rows[0].line_items == "Trouser × 1 | Wash × 1"


def test_render_html_includes_financial_columns() -> None:
    html = render_html(rows=[], report_date_display='29-Apr-2026', mtd_start_display='01-Apr-2026', mtd_end_display='29-Apr-2026')
    assert 'The Shaw Ventures' in html
    assert 'MTD Same-Day Orders (Delivered within same calendar day)' in html
    assert 'Payment Date' in html
    assert 'Store: ' not in html
    assert 'Net Amount' in html
    assert 'Payment Received' in html


def test_render_html_groups_store_and_formats_duration() -> None:
    rows = [
        MTDSameDayFulfillmentRow("S1", "A2", datetime(2026, 4, 10, 10), "Alice", "999", "Wash", datetime(2026, 4, 10, 10, 2), "UPI", 0.04, 10, 10),
        MTDSameDayFulfillmentRow("S1", "A1", datetime(2026, 4, 10, 9), "Bob", None, "Iron", datetime(2026, 4, 10, 9, 14), "CARD", 0.23, 20, 20),
        MTDSameDayFulfillmentRow("S2", "B1", datetime(2026, 4, 10, 8), "Cara", "888", "Dry", datetime(2026, 4, 10, 13, 23), "CASH", 5.39, 30, 30),
        MTDSameDayFulfillmentRow("S2", "B2", datetime(2026, 4, 10, 7), "Dan", "777", "Steam", datetime(2026, 4, 10, 7), "UPI", 0.00, 40, 40),
    ]
    html = render_html(rows=rows, report_date_display='29-Apr-2026', mtd_start_display='01-Apr-2026', mtd_end_display='29-Apr-2026')
    assert "Store: S1" in html and "Store: S2" in html
    assert "2 min" in html and "14 min" in html and "5h 23m" in html and "0 min" in html
    assert "Store Code</th>" in html  # summary only
    assert "Customer</th>" in html
    assert "Store Code</th>" in html and "Order Number" in html


@pytest.mark.asyncio
async def test_fetch_mtd_same_day_fulfillment_postgres_sql_has_no_strftime_and_hours_from_python(monkeypatch) -> None:
    monkeypatch.setattr(mtd_data, 'get_timezone', lambda: ZoneInfo('Asia/Kolkata'))

    captured = {}

    class _Result:
        def mappings(self):
            return [
                {
                    "store_code": "S1",
                    "order_number": "O1",
                    "order_date": datetime(2026, 4, 10, 9, 0),
                    "customer_name": "Alice",
                    "mobile_number": "9999999999",
                    "line_items": "Wash Shirt",
                    "payment_date": datetime(2026, 4, 10, 11, 30),
                    "payment_mode": "UPI",
                    "net_amount": 800,
                    "payment_received": 800,
                }
            ]

    class _Session:
        async def execute(self, stmt):
            captured["stmt"] = stmt
            return _Result()

    @asynccontextmanager
    async def _fake_session_scope(_database_url: str):
        yield _Session()

    monkeypatch.setattr(mtd_data, 'session_scope', _fake_session_scope)

    rows = await fetch_mtd_same_day_fulfillment(
        database_url='postgresql+asyncpg://user:pass@localhost/db',
        report_date=date(2026, 4, 29),
    )

    compiled = str(captured["stmt"].compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    assert "strftime" not in compiled.lower()
    assert rows[0].hours == 2.5


@pytest.mark.asyncio
async def test_fetch_mtd_same_day_fulfillment_date_only_and_mtd_window(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / 'mtd_same_day_window.db'
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    monkeypatch.setattr(mtd_data, 'get_timezone', lambda: ZoneInfo('Asia/Kolkata'))

    async with session_scope(database_url) as session:
        await session.execute(sa.text("INSERT INTO store_master (cost_center, store_code) VALUES ('CC1', 'S1')"))
        await session.execute(sa.text("INSERT INTO orders (cost_center, order_number, order_date, customer_name, mobile_number, net_amount) VALUES ('CC1','O3','2026-04-29 23:50:00','Late','777',600),('CC1','O4','2026-03-31 23:50:00','Old','666',400)"))
        await session.execute(sa.text("INSERT INTO sales (cost_center, order_number, payment_date, payment_mode, payment_received) VALUES ('CC1','O3','2026-04-30 00:05:00','UPI',600),('CC1','O4','2026-04-01 00:05:00','UPI',400)"))
        await session.commit()

    rows = await fetch_mtd_same_day_fulfillment(database_url=database_url, report_date=date(2026, 4, 29))
    order_numbers = {row.order_number for row in rows}
    assert 'O3' not in order_numbers
    assert 'O4' not in order_numbers
