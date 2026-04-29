from datetime import date
from zoneinfo import ZoneInfo

import pytest
import sqlalchemy as sa

from app.common.db import session_scope
from app.reports.mtd_same_day_fulfillment.data import fetch_mtd_same_day_fulfillment
from app.reports.mtd_same_day_fulfillment.render import render_html
import app.reports.mtd_same_day_fulfillment.data as mtd_data


def _create_tables(database_url: str) -> None:
    engine = sa.create_engine(database_url.replace('+aiosqlite', ''))
    with engine.begin() as conn:
        conn.execute(sa.text("CREATE TABLE orders (cost_center TEXT, order_number TEXT, order_date TIMESTAMP, net_amount NUMERIC)"))
        conn.execute(sa.text("CREATE TABLE sales (cost_center TEXT, order_number TEXT, payment_date TIMESTAMP, payment_received NUMERIC)"))
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
        await session.execute(sa.text("INSERT INTO orders (cost_center, order_number, order_date, net_amount) VALUES ('CC1','O1','2026-04-10T09:00:00+05:30',800),('CC1','O2','2026-03-30T09:00:00+05:30',700)"))
        await session.execute(sa.text("INSERT INTO sales (cost_center, order_number, payment_date, payment_received) VALUES ('CC1','O1','2026-04-10T10:00:00+05:30',500),('CC1','O1','2026-04-10T11:00:00+05:30',300),('CC1','O2','2026-04-10T11:00:00+05:30',700)"))
        await session.commit()

    rows = await fetch_mtd_same_day_fulfillment(database_url=database_url, report_date=date(2026,4,29))
    assert len(rows) == 1
    assert rows[0].order_number == 'O1'
    assert rows[0].net_amount == 800
    assert rows[0].payment_received == 800


def test_render_html_includes_financial_columns() -> None:
    html = render_html(rows=[], report_date_display='29-Apr-2026', mtd_start_display='01-Apr-2026', mtd_end_display='29-Apr-2026')
    assert 'Net Amount' in html
    assert 'Payment Received' in html
