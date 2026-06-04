from datetime import datetime

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from app.common.db import session_scope
from app.reports.shared.same_day_fulfillment import fetch_same_day_fulfillment_rows


def _create_tables(database_url: str) -> None:
    engine = sa.create_engine(database_url.replace('+aiosqlite', ''))
    with engine.begin() as conn:
        conn.execute(sa.text("CREATE TABLE orders (cost_center TEXT, order_number TEXT, order_date TIMESTAMP, customer_name TEXT, mobile_number TEXT, net_amount NUMERIC, gross_amount NUMERIC, adjustment NUMERIC, source_system TEXT)"))
        conn.execute(sa.text("CREATE VIEW vw_orders AS SELECT *, CASE WHEN (CASE WHEN COALESCE(adjustment, 0) > 0 THEN COALESCE(CASE WHEN source_system = 'TumbleDry' AND net_amount IS NOT NULL AND net_amount <> 0 THEN net_amount WHEN source_system = 'TumbleDry' THEN gross_amount ELSE gross_amount END, 0) - COALESCE(adjustment, 0) ELSE COALESCE(CASE WHEN source_system = 'TumbleDry' AND net_amount IS NOT NULL AND net_amount <> 0 THEN net_amount WHEN source_system = 'TumbleDry' THEN gross_amount ELSE gross_amount END, 0) END) <= 0 THEN 0 ELSE (CASE WHEN COALESCE(adjustment, 0) > 0 THEN COALESCE(CASE WHEN source_system = 'TumbleDry' AND net_amount IS NOT NULL AND net_amount <> 0 THEN net_amount WHEN source_system = 'TumbleDry' THEN gross_amount ELSE gross_amount END, 0) - COALESCE(adjustment, 0) ELSE COALESCE(CASE WHEN source_system = 'TumbleDry' AND net_amount IS NOT NULL AND net_amount <> 0 THEN net_amount WHEN source_system = 'TumbleDry' THEN gross_amount ELSE gross_amount END, 0) END) END AS order_amount FROM orders"))
        conn.execute(sa.text("CREATE TABLE order_line_items (cost_center TEXT, order_number TEXT, service_name TEXT, garment_name TEXT)"))
        conn.execute(sa.text("CREATE TABLE sales (cost_center TEXT, order_number TEXT, payment_date TIMESTAMP, payment_mode TEXT, payment_received NUMERIC)"))
        conn.execute(sa.text("CREATE TABLE store_master (cost_center TEXT, store_code TEXT)"))
        conn.execute(sa.text("CREATE TABLE payment_collections (cost_center TEXT, order_number TEXT, amount NUMERIC DEFAULT 0, source_type TEXT DEFAULT 'google_sheet')"))
    engine.dispose()


@pytest.mark.asyncio
async def test_fetch_same_day_fulfillment_rows_filters_window_and_aggregates(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'same_day_shared.db'}"
    _create_tables(database_url)

    async with session_scope(database_url) as session:
        await session.execute(sa.text("INSERT INTO store_master VALUES ('CC1', 'S1')"))
        await session.execute(sa.text("INSERT INTO orders (cost_center, order_number, order_date, customer_name, mobile_number, net_amount, source_system) VALUES ('CC1','O1','2026-04-10T09:00:00','Alice','999',800,'TumbleDry'),('CC1','O2','2026-04-09T09:00:00','Bob','888',500,'TumbleDry')"))
        await session.execute(sa.text("INSERT INTO order_line_items VALUES ('CC1','O1','Wash','Shirt'),('CC1','O1','Iron','Pant')"))
        await session.execute(sa.text("INSERT INTO sales VALUES ('CC1','O1','2026-04-10T10:00:00','UPI',300),('CC1','O1','2026-04-10T11:00:00','CARD',500),('CC1','O2','2026-04-10T10:00:00','UPI',500)"))
        await session.commit()

    orders = sa.table('vw_orders', sa.column('cost_center'), sa.column('order_number'), sa.column('order_date'), sa.column('customer_name'), sa.column('mobile_number'), sa.column('order_amount'))
    order_line_items = sa.table('order_line_items', sa.column('cost_center'), sa.column('order_number'), sa.column('service_name'), sa.column('garment_name'))
    sales = sa.table('sales', sa.column('cost_center'), sa.column('order_number'), sa.column('payment_date'), sa.column('payment_mode'), sa.column('payment_received'))
    store_master = sa.table('store_master', sa.column('cost_center'), sa.column('store_code'))

    async with session_scope(database_url) as session:
        rows = await fetch_same_day_fulfillment_rows(
            session=session,
            orders=orders,
            sales=sales,
            order_line_items=order_line_items,
            store_master=store_master,
            start_datetime=datetime.fromisoformat('2026-04-10T00:00:00'),
            end_datetime=datetime.fromisoformat('2026-04-11T00:00:00'),
            timezone_name='Asia/Kolkata',
        )

    assert [row.order_number for row in rows] == ['O1']
    assert rows[0].line_items == 'Wash Shirt, Iron Pant'
    assert rows[0].line_item_rows == [
        {'service_name': 'Wash', 'garment_name': 'Shirt'},
        {'service_name': 'Iron', 'garment_name': 'Pant'},
    ]
    assert str(rows[0].order_amount) == '800'
    assert str(rows[0].payment_received) == '800'
    assert not hasattr(rows[0], 'net_amount')


@pytest.mark.asyncio
async def test_fetch_same_day_fulfillment_rows_postgres_sql_uses_timezone(monkeypatch) -> None:
    captured = {}

    class _Result:
        def mappings(self):
            return []

    class _Session:
        bind = type('B', (), {'dialect': type('D', (), {'name': 'postgresql'})()})()

        async def execute(self, stmt):
            captured['stmt'] = stmt
            return _Result()

    orders = sa.table('vw_orders', sa.column('cost_center'), sa.column('order_number'), sa.column('order_date'), sa.column('customer_name'), sa.column('mobile_number'), sa.column('order_amount'))
    order_line_items = sa.table('order_line_items', sa.column('cost_center'), sa.column('order_number'), sa.column('service_name'), sa.column('garment_name'))
    sales = sa.table('sales', sa.column('cost_center'), sa.column('order_number'), sa.column('payment_date'), sa.column('payment_mode'), sa.column('payment_received'))
    store_master = sa.table('store_master', sa.column('cost_center'), sa.column('store_code'))

    await fetch_same_day_fulfillment_rows(
        session=_Session(),
        orders=orders,
        sales=sales,
        order_line_items=order_line_items,
        store_master=store_master,
        start_datetime=datetime(2026, 4, 1),
        end_datetime=datetime(2026, 4, 30),
        timezone_name='Asia/Kolkata',
    )

    compiled = str(captured['stmt'].compile(dialect=postgresql.dialect(), compile_kwargs={'literal_binds': True}))
    assert 'timezone' in compiled.lower()
    assert 'string_agg' in compiled.lower()
    compiled_lower = compiled.lower()
    assert 'regexp_split_to_table' not in compiled_lower
    assert 'vw_orders.order_amount' in compiled_lower
    assert 'net_amount' not in compiled_lower


@pytest.mark.asyncio
async def test_fetch_same_day_fulfillment_rows_uses_full_payment_proof_rules(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'same_day_proof_rules.db'}"
    _create_tables(database_url)

    async with session_scope(database_url) as session:
        await session.execute(sa.text("INSERT INTO store_master VALUES ('CC1', 'S1'), ('CC2', 'S2')"))
        await session.execute(sa.text("""
            INSERT INTO orders (cost_center, order_number, order_date, customer_name, mobile_number, net_amount, source_system) VALUES
                ('CC1','NO_PROOF','2026-04-10T09:00:00','No Proof','900',100,'TumbleDry'),
                ('CC1','FULL','2026-04-10T09:01:00','Full Proof','901',100,'TumbleDry'),
                ('CC1','OVER','2026-04-10T09:02:00','Over Proof','902',100,'TumbleDry'),
                ('CC1','SHORT','2026-04-10T09:03:00','Short Proof','903',100,'TumbleDry'),
                ('CC1','COMMA1','2026-04-10T09:04:00','Comma One','904',60,'TumbleDry'),
                ('CC1','COMMA2','2026-04-10T09:05:00','Comma Two','905',40,'TumbleDry'),
                ('CC1','SLASH1','2026-04-10T09:06:00','Slash One','906',70,'TumbleDry'),
                ('CC1','SLASH2','2026-04-10T09:07:00','Slash Two','907',30,'TumbleDry'),
                ('CC1','MIX1','2026-04-10T09:08:00','Mixed One','908',50,'TumbleDry'),
                ('CC1','MIX2','2026-04-10T09:09:00','Mixed Two','909',30,'TumbleDry'),
                ('CC1','MIX3','2026-04-10T09:10:00','Mixed Three','910',20,'TumbleDry'),
                ('CC1','TOKEN','2026-04-10T09:11:00','Token Exact','911',100,'TumbleDry'),
                ('CC1','TD_ROW','2026-04-10T09:12:00','TD Row','912',100,'TumbleDry'),
                ('CC2','UC_ROW','2026-04-10T09:13:00','UC Row','913',100,'UClean')
        """))
        await session.execute(sa.text("""
            INSERT INTO sales (cost_center, order_number, payment_date, payment_mode, payment_received) VALUES
                ('CC1','NO_PROOF','2026-04-10T10:00:00','UPI',100),
                ('CC1','FULL','2026-04-10T10:01:00','UPI',100),
                ('CC1','OVER','2026-04-10T10:02:00','UPI',101),
                ('CC1','SHORT','2026-04-10T10:03:00','UPI',50),
                ('CC1','COMMA1','2026-04-10T10:04:00','UPI',60),
                ('CC1','COMMA2','2026-04-10T10:05:00','UPI',40),
                ('CC1','SLASH1','2026-04-10T10:06:00','UPI',70),
                ('CC1','SLASH2','2026-04-10T10:07:00','UPI',30),
                ('CC1','MIX1','2026-04-10T10:08:00','UPI',50),
                ('CC1','MIX2','2026-04-10T10:09:00','UPI',30),
                ('CC1','MIX3','2026-04-10T10:10:00','UPI',20),
                ('CC1','TOKEN','2026-04-10T10:11:00','UPI',100),
                ('CC1','TD_ROW','2026-04-10T10:12:00','UPI',100),
                ('CC2','UC_ROW','2026-04-10T10:13:00','UPI',100)
        """))
        await session.execute(sa.text("""
            INSERT INTO payment_collections (cost_center, order_number, amount, source_type) VALUES
                ('CC1','FULL',100,'google_sheet'),
                ('CC1','OVER',101,'google_sheet'),
                ('CC1','SHORT',50,'google_sheet'),
                ('CC1','COMMA1, COMMA2',100,'google_sheet'),
                ('CC1','SLASH1/SLASH2',100,'google_sheet'),
                ('CC1','MIX1, MIX2/MIX3',100,'google_sheet'),
                ('CC1','TOKENX',100,'google_sheet')
        """))
        await session.commit()

    orders = sa.table('vw_orders', sa.column('cost_center'), sa.column('order_number'), sa.column('order_date'), sa.column('customer_name'), sa.column('mobile_number'), sa.column('order_amount'))
    order_line_items = sa.table('order_line_items', sa.column('cost_center'), sa.column('order_number'), sa.column('service_name'), sa.column('garment_name'))
    sales = sa.table('sales', sa.column('cost_center'), sa.column('order_number'), sa.column('payment_date'), sa.column('payment_mode'), sa.column('payment_received'))
    store_master = sa.table('store_master', sa.column('cost_center'), sa.column('store_code'))

    async with session_scope(database_url) as session:
        rows = await fetch_same_day_fulfillment_rows(
            session=session,
            orders=orders,
            sales=sales,
            order_line_items=order_line_items,
            store_master=store_master,
            start_datetime=datetime.fromisoformat('2026-04-10T00:00:00'),
            end_datetime=datetime.fromisoformat('2026-04-11T00:00:00'),
            timezone_name='Asia/Kolkata',
        )

    order_numbers = {row.order_number for row in rows}
    assert {'NO_PROOF', 'SHORT', 'TOKEN', 'TD_ROW', 'UC_ROW'} <= order_numbers
    assert 'FULL' not in order_numbers
    assert 'OVER' not in order_numbers
    assert not {'COMMA1', 'COMMA2'} & order_numbers
    assert not {'SLASH1', 'SLASH2'} & order_numbers
    assert not {'MIX1', 'MIX2', 'MIX3'} & order_numbers
