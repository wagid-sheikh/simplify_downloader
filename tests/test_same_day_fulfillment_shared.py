from datetime import datetime

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from app.common.db import session_scope
from app.reports.shared.same_day_fulfillment import fetch_same_day_fulfillment_rows


def _create_tables(database_url: str) -> None:
    engine = sa.create_engine(database_url.replace('+aiosqlite', ''))
    with engine.begin() as conn:
        conn.execute(sa.text("CREATE TABLE orders (cost_center TEXT, order_number TEXT, order_date TIMESTAMP, customer_name TEXT, mobile_number TEXT, net_amount NUMERIC)"))
        conn.execute(sa.text("CREATE TABLE order_line_items (cost_center TEXT, order_number TEXT, service_name TEXT, garment_name TEXT)"))
        conn.execute(sa.text("CREATE TABLE sales (cost_center TEXT, order_number TEXT, payment_date TIMESTAMP, payment_mode TEXT, payment_received NUMERIC)"))
        conn.execute(sa.text("CREATE TABLE store_master (cost_center TEXT, store_code TEXT)"))
    engine.dispose()


@pytest.mark.asyncio
async def test_fetch_same_day_fulfillment_rows_filters_window_and_aggregates(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'same_day_shared.db'}"
    _create_tables(database_url)

    async with session_scope(database_url) as session:
        await session.execute(sa.text("INSERT INTO store_master VALUES ('CC1', 'S1')"))
        await session.execute(sa.text("INSERT INTO orders VALUES ('CC1','O1','2026-04-10T09:00:00','Alice','999',800),('CC1','O2','2026-04-09T09:00:00','Bob','888',500)"))
        await session.execute(sa.text("INSERT INTO order_line_items VALUES ('CC1','O1','Wash','Shirt'),('CC1','O1','Iron','Pant')"))
        await session.execute(sa.text("INSERT INTO sales VALUES ('CC1','O1','2026-04-10T10:00:00','UPI',300),('CC1','O1','2026-04-10T11:00:00','CARD',500),('CC1','O2','2026-04-10T10:00:00','UPI',500)"))
        await session.commit()

    orders = sa.table('orders', sa.column('cost_center'), sa.column('order_number'), sa.column('order_date'), sa.column('customer_name'), sa.column('mobile_number'), sa.column('net_amount'))
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
    assert str(rows[0].payment_received) == '800'


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

    orders = sa.table('orders', sa.column('cost_center'), sa.column('order_number'), sa.column('order_date'), sa.column('customer_name'), sa.column('mobile_number'), sa.column('net_amount'))
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
