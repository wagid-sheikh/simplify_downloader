from datetime import date
from zoneinfo import ZoneInfo

import pytest
import sqlalchemy as sa

from app.common.db import session_scope
import importlib.util
from pathlib import Path
import sys



_DATA_MODULE_PATH = Path(__file__).resolve().parents[1] / "app" / "reports" / "daily_sales_report" / "data.py"
_spec = importlib.util.spec_from_file_location("daily_sales_report_data_module", _DATA_MODULE_PATH)
if _spec is None or _spec.loader is None:
    raise RuntimeError("Unable to load daily sales report data module")
_data_module = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _data_module
_spec.loader.exec_module(_data_module)
fetch_daily_sales_report = _data_module.fetch_daily_sales_report


def _create_tables(database_url: str) -> None:
    engine = sa.create_engine(database_url.replace("+aiosqlite", ""))
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                CREATE TABLE cost_center (
                    cost_center TEXT PRIMARY KEY,
                    description TEXT,
                    target_type TEXT,
                    is_active BOOLEAN
                )
                """
            )
        )
        conn.execute(
            sa.text(
                """
                CREATE TABLE cost_center_targets (
                    month INTEGER,
                    year INTEGER,
                    cost_center TEXT,
                    sale_target NUMERIC
                )
                """
            )
        )
        conn.execute(
            sa.text(
                """
                CREATE TABLE orders (
                    cost_center TEXT,
                    order_number TEXT,
                    order_date TIMESTAMP,
                    net_amount NUMERIC
                )
                """
            )
        )
        conn.execute(
            sa.text(
                """
                CREATE TABLE orders_sync_log (
                    cost_center TEXT,
                    orders_pulled_at TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            sa.text(
                """
                CREATE TABLE sales (
                    cost_center TEXT,
                    payment_date TIMESTAMP,
                    payment_received NUMERIC,
                    adjustments NUMERIC,
                    order_number TEXT,
                    is_edited_order BOOLEAN
                )
                """
            )
        )
        conn.execute(
            sa.text(
                """
                CREATE TABLE store_master (
                    id INTEGER PRIMARY KEY,
                    cost_center TEXT,
                    store_code TEXT,
                    store_name TEXT,
                    sync_group TEXT
                )
                """
            )
        )
        conn.execute(
            sa.text(
                """
                CREATE TABLE store_dashboard_summary (
                    store_id INTEGER,
                    dashboard_date DATE,
                    run_date_time TIMESTAMP,
                    pickup_new_conv_pct NUMERIC,
                    pickup_existing_conv_pct NUMERIC,
                    pickup_total_count INTEGER,
                    pickup_total_conv_pct NUMERIC,
                    delivery_tat_pct NUMERIC
                )
                """
            )
        )
        conn.execute(
            sa.text(
                """
                CREATE TABLE missed_leads (
                    store_code TEXT,
                    mobile_number TEXT,
                    customer_name TEXT,
                    customer_type TEXT,
                    pickup_date DATE,
                    is_order_placed BOOLEAN
                )
                """
            )
        )
    engine.dispose()


@pytest.mark.asyncio
async def test_fetch_daily_sales_report_missed_leads_td_only(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "daily_sales_report.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr(_data_module, "get_timezone", lambda: tz)
    report_date = date(2026, 1, 19)

    async with session_scope(database_url) as session:
        await session.execute(
            sa.text(
                """
                INSERT INTO cost_center (cost_center, description, target_type, is_active)
                VALUES
                    ('CC-TD', 'TD Cost Center', 'value', 1),
                    ('CC-NTD', 'Non-TD Cost Center', 'value', 1)
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO store_master (id, cost_center, store_code, store_name, sync_group)
                VALUES
                    (1, 'CC-TD', 'S-TD', 'TD Store', 'TD'),
                    (2, 'CC-NTD', 'S-NTD', 'Non-TD Store', 'UC')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO missed_leads (
                    store_code, mobile_number, customer_name, customer_type, pickup_date, is_order_placed
                ) VALUES
                    ('S-TD', '9999999999', 'Alice TD', 'New', :report_date, 0),
                    ('S-TD', '8888888888', 'Bob TD', 'New', :report_date, 0),
                    ('S-NTD', '7777777777', 'Charlie NonTD', 'New', :report_date, 0)
                """
            ),
            {"report_date": report_date},
        )
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)

    assert report.missed_leads == [
        {
            "store_name": "TD Store",
            "customer_type": "New",
            "leads": [
                {"customer_name": "Alice TD", "mobile_number": "9999999999"},
                {"customer_name": "Bob TD", "mobile_number": "8888888888"},
            ],
        }
    ]
