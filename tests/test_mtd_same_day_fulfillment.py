from contextlib import asynccontextmanager
from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from sqlalchemy.dialects import postgresql

import pytest
import sqlalchemy as sa

from app.common.db import session_scope
from app.reports.mtd_same_day_fulfillment.data import fetch_mtd_same_day_fulfillment, fetch_short_payments_mtd
from app.reports.mtd_same_day_fulfillment.data import MTDSameDayFulfillmentRow
from app.reports.mtd_same_day_fulfillment.render import render_html
from app.reports.shared.same_day_fulfillment import format_duration_minutes
import app.reports.mtd_same_day_fulfillment.data as mtd_data
import app.reports.mtd_same_day_fulfillment.pipeline as mtd_pipeline


def _create_tables(database_url: str) -> None:
    engine = sa.create_engine(database_url.replace('+aiosqlite', ''))
    with engine.begin() as conn:
        conn.execute(sa.text("CREATE TABLE orders (cost_center TEXT, order_number TEXT, order_date TIMESTAMP, customer_name TEXT, mobile_number TEXT, net_amount NUMERIC, gross_amount NUMERIC, adjustment NUMERIC, source_system TEXT, recovery_status TEXT)"))
        conn.execute(sa.text("CREATE VIEW vw_orders AS SELECT *, CASE WHEN (CASE WHEN COALESCE(adjustment, 0) > 0 THEN COALESCE(CASE WHEN source_system = 'TumbleDry' AND net_amount IS NOT NULL AND net_amount <> 0 THEN net_amount WHEN source_system = 'TumbleDry' THEN gross_amount ELSE gross_amount END, 0) - COALESCE(adjustment, 0) ELSE COALESCE(CASE WHEN source_system = 'TumbleDry' AND net_amount IS NOT NULL AND net_amount <> 0 THEN net_amount WHEN source_system = 'TumbleDry' THEN gross_amount ELSE gross_amount END, 0) END) <= 0 THEN 0 ELSE (CASE WHEN COALESCE(adjustment, 0) > 0 THEN COALESCE(CASE WHEN source_system = 'TumbleDry' AND net_amount IS NOT NULL AND net_amount <> 0 THEN net_amount WHEN source_system = 'TumbleDry' THEN gross_amount ELSE gross_amount END, 0) - COALESCE(adjustment, 0) ELSE COALESCE(CASE WHEN source_system = 'TumbleDry' AND net_amount IS NOT NULL AND net_amount <> 0 THEN net_amount WHEN source_system = 'TumbleDry' THEN gross_amount ELSE gross_amount END, 0) END) END AS order_amount FROM orders"))
        conn.execute(sa.text("CREATE TABLE order_line_items (cost_center TEXT, order_number TEXT, service_name TEXT, garment_name TEXT)"))
        conn.execute(sa.text("CREATE TABLE sales (cost_center TEXT, order_number TEXT, payment_date TIMESTAMP, payment_mode TEXT, payment_received NUMERIC)"))
        conn.execute(sa.text("CREATE TABLE store_master (cost_center TEXT, store_code TEXT)"))
        conn.execute(sa.text("CREATE TABLE payment_collections (cost_center TEXT, order_number TEXT, amount NUMERIC DEFAULT 0, source_type TEXT DEFAULT 'google_sheet')"))
    engine.dispose()


@pytest.mark.asyncio
async def test_fetch_mtd_same_day_fulfillment_filters_and_aggregates(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / 'mtd_same_day.db'
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    monkeypatch.setattr(mtd_data, 'get_timezone', lambda: ZoneInfo('Asia/Kolkata'))

    async with session_scope(database_url) as session:
        await session.execute(sa.text("INSERT INTO store_master (cost_center, store_code) VALUES ('CC1', 'S1')"))
        await session.execute(sa.text("INSERT INTO orders (cost_center, order_number, order_date, customer_name, mobile_number, net_amount, source_system) VALUES ('CC1','O1','2026-04-10T09:00:00+05:30','Alice','9999999999',800,'TumbleDry'),('CC1','O2','2026-03-30T09:00:00+05:30','Bob','8888888888',700,'TumbleDry')"))
        await session.execute(sa.text("INSERT INTO order_line_items (cost_center, order_number, service_name, garment_name) VALUES ('CC1','O1','Wash','Shirt'),('CC1','O1','Iron','Pant'),('CC1','O2','Dry','Coat')"))
        await session.execute(sa.text("INSERT INTO sales (cost_center, order_number, payment_date, payment_mode, payment_received) VALUES ('CC1','O1','2026-04-10T10:00:00+05:30','UPI',500),('CC1','O1','2026-04-10T11:00:00+05:30','UPI',300),('CC1','O2','2026-04-10T11:00:00+05:30','CARD',700),('CC1','O1','2026-04-11T00:10:00+05:30','CASH',50)"))
        await session.commit()

    rows = await fetch_mtd_same_day_fulfillment(database_url=database_url, report_date=date(2026,4,29))
    assert len(rows) == 1
    assert rows[0].order_number == 'O1'
    assert rows[0].line_items == "Iron Pant × 1 | Wash Shirt × 1"
    assert rows[0].order_amount == 800
    assert rows[0].payment_received == 800
    assert rows[0].hours == 2.0
    assert not hasattr(rows[0], 'net_amount')


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
        await session.execute(sa.text("INSERT INTO orders (cost_center, order_number, order_date, customer_name, mobile_number, net_amount, source_system) VALUES ('CC1','O1','2026-04-10T09:00:00+05:30','Alice','9999999999',800,'TumbleDry')"))
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
    assert 'Order Amount' in html
    assert 'Net Amount' not in html
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
    assert "2 min" in html and "14 min" in html and "5 hrs 23 min" in html and "0 min" in html
    assert "10-04-2026<br><span class=\"micro-font\">10:00 AM</span>" in html
    assert "10-04-2026<br><span class=\"micro-font\">10:02 AM</span>" in html
    assert "Store Code</th>" in html  # summary only
    assert "Customer</th>" in html
    assert "Store Code</th>" in html and "Order Number" in html


@pytest.mark.asyncio
async def test_fetch_mtd_same_day_fulfillment_postgres_sql_has_no_strftime_and_hours_from_python(monkeypatch) -> None:
    monkeypatch.setattr(mtd_data, 'get_timezone', lambda: ZoneInfo('Asia/Kolkata'))

    captured = {'stmts': []}

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
                    "order_amount": 800,
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
        await session.execute(sa.text("INSERT INTO orders (cost_center, order_number, order_date, customer_name, mobile_number, net_amount, source_system) VALUES ('CC1','O3','2026-04-29 23:50:00','Late','777',600,'TumbleDry'),('CC1','O4','2026-03-31 23:50:00','Old','666',400,'TumbleDry')"))
        await session.execute(sa.text("INSERT INTO sales (cost_center, order_number, payment_date, payment_mode, payment_received) VALUES ('CC1','O3','2026-04-30 00:05:00','UPI',600),('CC1','O4','2026-04-01 00:05:00','UPI',400)"))
        await session.commit()

    rows = await fetch_mtd_same_day_fulfillment(database_url=database_url, report_date=date(2026, 4, 29))
    order_numbers = {row.order_number for row in rows}
    assert 'O3' not in order_numbers
    assert 'O4' not in order_numbers


@pytest.mark.asyncio
async def test_mtd_pipeline_has_no_missing_payment_fetch_dependency(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "mtd_pipeline_no_missing_dependency.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"

    monkeypatch.setattr(
        mtd_pipeline,
        "config",
        type("Cfg", (), {"database_url": database_url, "pdf_render_timeout_seconds": 30})(),
    )
    monkeypatch.setattr(mtd_pipeline, "resolve_run_env", lambda env: "test")
    monkeypatch.setattr(mtd_pipeline, "new_run_id", lambda: "mtd-no-missing-dep")
    monkeypatch.setattr(mtd_pipeline, "get_timezone", lambda: ZoneInfo("Asia/Kolkata"))

    async def _fake_fetch_rows(*args, **kwargs):
        return []

    async def _fake_render_pdf(_html, output_path, pdf_options=None, logger=None):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"pdf")

    async def _fake_summary(*args, **kwargs):
        return None

    async def _fake_notify(*args, **kwargs):
        return {"emails_planned": 0, "emails_sent": 0, "errors": []}

    monkeypatch.setattr(mtd_pipeline, "fetch_mtd_same_day_fulfillment", _fake_fetch_rows)
    monkeypatch.setattr(mtd_pipeline, "render_pdf_with_configured_browser", _fake_render_pdf)
    monkeypatch.setattr(mtd_pipeline, "persist_summary_record", _fake_summary)
    monkeypatch.setattr(mtd_pipeline, "update_summary_record", _fake_summary)
    monkeypatch.setattr(mtd_pipeline, "send_notifications_for_run", _fake_notify)

    await mtd_pipeline._run(report_date=date(2026, 4, 29), env="test", force=True)



@pytest.mark.asyncio
async def test_fetch_short_payments_mtd_uses_global_orders_for_grouped_reconciliation(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / 'mtd_short_payments.db'
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    monkeypatch.setattr(mtd_data, 'get_timezone', lambda: ZoneInfo('Asia/Kolkata'))

    async with session_scope(database_url) as session:
        await session.execute(sa.text("""
            INSERT INTO orders (cost_center, order_number, order_date, customer_name, mobile_number, net_amount, source_system, recovery_status) VALUES
                ('CC1','A1','2026-04-10T09:00:00+05:30','Alice','999',200,'TumbleDry','NONE'),
                ('CC1','A2','2026-03-31T10:00:00+05:30','Bob','888',100,'TumbleDry','NONE'),
                ('CC1','B1','2026-04-11T09:00:00+05:30','Cara','777',100,'TumbleDry','NONE'),
                ('CC1','B2','2026-03-31T10:00:00+05:30','Dan','666',200,'TumbleDry','NONE'),
                ('CC1','S1','2026-04-12T09:00:00+05:30','Eve','555',150,'TumbleDry','NONE'),
                ('CC1','PONLY','2026-04-12T09:30:00+05:30','Pam','554',100,'TumbleDry','NONE'),
                ('CC1','MIS','2026-04-12T09:45:00+05:30','Max','553',100,'TumbleDry','NONE'),
                ('CC1','ZERO','2026-04-12T09:50:00+05:30','Zoe','552',0,'TumbleDry','NONE'),
                ('CC1','RECOVERY','2026-04-12T09:55:00+05:30','Ria','551',150,'TumbleDry','TO_BE_RECOVERED'),
                ('CC1','COMP','2026-04-12T09:57:00+05:30','Cia','550',150,'TumbleDry','TO_BE_COMPENSATED'),
                ('CC1','REC','2026-04-12T10:00:00+05:30','Ron','444',150,'TumbleDry','WRITE_OFF')
        """))
        await session.execute(sa.text("""
            INSERT INTO sales (cost_center, order_number, payment_date, payment_mode, payment_received) VALUES
                ('CC1','A1','2026-04-10T09:30:00+05:30','UPI',50),
                ('CC1','A2','2026-03-31T10:30:00+05:30','UPI',100),
                ('CC1','B1','2026-04-11T09:30:00+05:30','UPI',100),
                ('CC1','B2','2026-03-31T10:30:00+05:30','UPI',200),
                ('CC1','S1','2026-04-12T09:30:00+05:30','UPI',140),
                ('CC1','MIS','2026-04-12T09:55:00+05:30','UPI',90),
                ('CC1','ZERO','2026-04-12T09:56:00+05:30','UPI',0),
                ('CC1','RECOVERY','2026-04-12T09:57:00+05:30','UPI',140),
                ('CC1','COMP','2026-04-12T09:58:00+05:30','UPI',140)
        """))
        await session.execute(sa.text("""
            INSERT INTO payment_collections (cost_center, order_number, amount, source_type) VALUES
                ('CC1','A1/A2',150,'google_sheet'),
                ('CC1','B1/B2',300,'google_sheet'),
                ('CC1','S1',140,'google_sheet'),
                ('CC1','PONLY',80,'google_sheet'),
                ('CC1','MIS',80,'google_sheet'),
                ('CC1','ZERO',0,'google_sheet'),
                ('CC1','RECOVERY',140,'google_sheet'),
                ('CC1','COMP',140,'google_sheet'),
                ('CC1','REC',10,'google_sheet')
        """))
        await session.commit()

    rows = await fetch_short_payments_mtd(database_url=database_url, report_date=date(2026, 4, 29))

    assert [(row.order_number, row.paid_amount, row.shortage_amount, row.group_key) for row in rows] == [
        ('A1', Decimal('50'), Decimal('150'), 'A2|A1'),
        ('S1', Decimal('140'), Decimal('10'), None),
    ]


def test_render_html_has_no_apnf_section() -> None:
    html = render_html(
        rows=[],
        report_date_display='29-Apr-2026',
        mtd_start_display='01-Apr-2026',
        mtd_end_display='29-Apr-2026',
    )

    assert 'Actual Payments Not Found' not in html


def test_render_html_does_not_render_short_payments_section() -> None:
    html = render_html(
        rows=[],
        report_date_display='29-Apr-2026',
        mtd_start_display='01-Apr-2026',
        mtd_end_display='29-Apr-2026',
    )

    assert 'Short Payments' not in html
    assert 'Shortage Amount' not in html
    assert 'Group Key' not in html

def test_format_duration_minutes_examples() -> None:
    assert format_duration_minutes(0) == "0 min"
    assert format_duration_minutes(14) == "14 min"
    assert format_duration_minutes(60) == "1 hr 0 min"
    assert format_duration_minutes(181) == "3 hrs 1 min"
    assert format_duration_minutes(566) == "9 hrs 26 min"

@pytest.mark.asyncio
async def test_fetch_mtd_same_day_fulfillment_excludes_orders_with_payment_proof_tokens(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / 'mtd_same_day_payment_proof.db'
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    monkeypatch.setattr(mtd_data, 'get_timezone', lambda: ZoneInfo('Asia/Kolkata'))

    async with session_scope(database_url) as session:
        await session.execute(sa.text("INSERT INTO store_master (cost_center, store_code) VALUES ('CC1', 'S1'), ('CC2', 'S2')"))
        await session.execute(sa.text("INSERT INTO orders (cost_center, order_number, order_date, customer_name, mobile_number, net_amount, source_system) VALUES ('CC1','Ord2','2026-04-10T09:00:00+05:30','Alice','999',800,'TumbleDry'),('CC1','ORDX','2026-04-10T09:00:00+05:30','Bob','888',700,'TumbleDry'),('CC2','ORD3','2026-04-10T09:00:00+05:30','Cara','777',600,'TumbleDry')"))
        await session.execute(sa.text("INSERT INTO sales (cost_center, order_number, payment_date, payment_mode, payment_received) VALUES ('CC1','Ord2','2026-04-10T10:00:00+05:30','UPI',800),('CC1','ORDX','2026-04-10T10:00:00+05:30','CARD',700),('CC2','ORD3','2026-04-10T10:00:00+05:30','UPI',600)"))
        await session.execute(sa.text("INSERT INTO payment_collections (cost_center, order_number) VALUES ('CC1',' ORD1, ord2 / ORD3 ,,')"))
        await session.commit()

    rows = await fetch_mtd_same_day_fulfillment(database_url=database_url, report_date=date(2026, 4, 29))
    assert {row.order_number for row in rows} == {'ORDX', 'ORD3'}
