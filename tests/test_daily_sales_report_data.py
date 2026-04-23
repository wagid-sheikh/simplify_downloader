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
                    sale_target NUMERIC,
                    collection_target NUMERIC,
                    sales_mtd NUMERIC,
                    collection_mtd NUMERIC,
                    sales_target_met BOOLEAN,
                    collection_target_met BOOLEAN
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
                    orders_pulled_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    created_at TIMESTAMP
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
        conn.execute(
            sa.text(
                """
                CREATE TABLE crm_leads (
                    store_code TEXT,
                    status_bucket TEXT,
                    customer_name TEXT,
                    mobile TEXT,
                    pickup_created_at TIMESTAMP
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


@pytest.mark.asyncio
async def test_fetch_daily_sales_report_lead_performance_summary_mtd_pickup_created_at(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "daily_sales_report_lead_performance.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr(_data_module, "get_timezone", lambda: tz)
    report_date = date(2026, 4, 23)

    async with session_scope(database_url) as session:
        await session.execute(
            sa.text(
                """
                INSERT INTO cost_center (cost_center, description, target_type, is_active)
                VALUES
                    ('CC-UN', 'Uttam Nagar', 'value', 1),
                    ('CC-KN', 'Kirti Nagar', 'value', 1)
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO store_master (id, cost_center, store_code, store_name, sync_group)
                VALUES
                    (1, 'CC-UN', ' un ', 'Uttam Nagar', 'TD'),
                    (2, 'CC-KN', ' kn ', 'Kirti Nagar', 'TD')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO crm_leads (store_code, status_bucket, pickup_created_at) VALUES
                    ('UN', 'completed', '2026-04-01 02:00:00+00:00'),
                    (' un ', ' Completed ', '2026-04-22 12:00:00+00:00'),
                    ('UN ', ' CANCELLED ', '2026-04-23 16:00:00+00:00'),
                    (' un', ' pending ', '2026-04-10 05:00:00+00:00'),
                    ('UN', 'completed', '2026-03-31 23:59:00+00:00'),
                    ('UN', 'cancelled', '2026-04-24 00:10:00+00:00')
                """
            )
        )
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)

    un_summary = next(item for item in report.lead_performance_summary if item["store"] == "UN")
    kn_summary = next(item for item in report.lead_performance_summary if item["store"] == "KN")

    assert un_summary["period_type"] == "MTD"
    assert un_summary["period_start"] == "2026-04-01"
    assert un_summary["period_end"] == "2026-04-23"
    assert un_summary["total_leads"] == 4
    assert un_summary["completed_leads"] == 2
    assert un_summary["cancelled_leads"] == 1
    assert un_summary["pending_leads"] == 1
    assert un_summary["conversion_pct"] == {"value": 50.0, "color": "RED", "status": "POOR"}
    assert un_summary["cancelled_pct"] == {"value": 25.0, "color": "RED", "status": "HIGH_LEAKAGE"}
    assert un_summary["pending_pct"] == {"value": 25.0, "color": "RED", "status": "FOLLOW_UP_GAP"}
    assert un_summary["benchmark"] == {
        "conversion_target": 85.0,
        "conversion_min": 70.0,
        "cancelled_target": 10.0,
        "cancelled_max": 20.0,
        "pending_max": 5.0,
    }
    assert un_summary["conversion_gap"] == -35.0
    assert un_summary["cancelled_gap"] == 15.0
    assert un_summary["pending_gap"] == 20.0

    assert kn_summary["total_leads"] == 0
    assert kn_summary["conversion_pct"] == {"value": 0.0, "color": "NEUTRAL", "status": "NEUTRAL"}
    assert kn_summary["cancelled_pct"] == {"value": 0.0, "color": "NEUTRAL", "status": "NEUTRAL"}
    assert kn_summary["pending_pct"] == {"value": 0.0, "color": "NEUTRAL", "status": "NEUTRAL"}


@pytest.mark.asyncio
async def test_fetch_daily_sales_report_cancelled_leads_month_window_and_formatting(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "daily_sales_report_cancelled_leads.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr(_data_module, "get_timezone", lambda: tz)
    report_date = date(2026, 4, 23)

    async with session_scope(database_url) as session:
        await session.execute(
            sa.text(
                """
                INSERT INTO cost_center (cost_center, description, target_type, is_active)
                VALUES ('CC-TD', 'TD Cost Center', 'value', 1)
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO store_master (id, cost_center, store_code, store_name, sync_group)
                VALUES (1, 'CC-TD', ' un ', 'Uttam Nagar', 'TD')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO crm_leads (store_code, status_bucket, customer_name, mobile, pickup_created_at) VALUES
                    ('UN', 'cancelled', 'April Cancelled', '9000000001', '2026-04-02 00:00:00'),
                    ('UN', ' cancelled ', 'Second Cancelled', '9000000002', '2026-04-30 23:59:00'),
                    ('UN', 'completed', 'Completed Lead', '9000000003', '2026-04-15 10:00:00'),
                    ('UN', 'cancelled', 'March Cancelled', '9000000004', '2026-03-30 23:59:00'),
                    ('UN', 'cancelled', 'May Cancelled', '9000000005', '2026-05-02 00:00:00')
                """
            )
        )
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)

    assert report.cancelled_leads == [
        {
            "store_name": "Uttam Nagar",
            "leads": [
                "April Cancelled (9000000001)",
                "Second Cancelled (9000000002)",
            ],
        }
    ]


@pytest.mark.asyncio
async def test_fetch_daily_sales_report_orders_sync_uses_updated_at_fallback(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "daily_sales_report_sync_fallback.db"
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
                VALUES ('CC-TD', 'TD Cost Center', 'value', 1)
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO orders_sync_log (cost_center, orders_pulled_at, updated_at, created_at)
                VALUES ('CC-TD', NULL, '2026-01-19 07:25:00', '2026-01-19 07:10:00')
                """
            )
        )
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)

    td_row = next(row for row in report.rows if row.cost_center == "CC-TD")
    assert td_row.orders_sync_time == "07:25"


@pytest.mark.asyncio
async def test_fetch_daily_sales_report_orders_sync_uses_created_at_string_fallback(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "daily_sales_report_sync_created_fallback.db"
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
                VALUES ('CC-TD', 'TD Cost Center', 'value', 1)
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO orders_sync_log (cost_center, orders_pulled_at, updated_at, created_at)
                VALUES ('CC-TD', NULL, NULL, '2026-01-19T05:45:00Z')
                """
            )
        )
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)

    td_row = next(row for row in report.rows if row.cost_center == "CC-TD")
    assert td_row.orders_sync_time == "11:15"


@pytest.mark.asyncio
async def test_fetch_daily_sales_report_updates_cost_center_targets_mtd_fields(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "daily_sales_report_targets_update.db"
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
                VALUES ('CC-TD', 'TD Cost Center', 'value', 1)
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO cost_center_targets (
                    month, year, cost_center, sale_target, collection_target
                ) VALUES (1, 2026, 'CC-TD', 150, 90)
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO orders (cost_center, order_number, order_date, net_amount)
                VALUES
                    ('CC-TD', 'ORD-1', '2026-01-05 10:00:00', 100),
                    ('CC-TD', 'ORD-2', '2026-01-10 11:00:00', 60)
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO sales (cost_center, payment_date, payment_received, adjustments, order_number, is_edited_order)
                VALUES ('CC-TD', '2026-01-12 12:00:00', 120, 0, 'ORD-1', 0)
                """
            )
        )
        await session.commit()

    await fetch_daily_sales_report(database_url=database_url, report_date=report_date)

    async with session_scope(database_url) as session:
        updated = await session.execute(
            sa.text(
                """
                SELECT sales_mtd, collection_mtd, sales_target_met, collection_target_met
                FROM cost_center_targets
                WHERE month = 1 AND year = 2026 AND cost_center = 'CC-TD'
                """
            )
        )
        row = updated.mappings().one()

    assert row["sales_mtd"] == 160
    assert row["collection_mtd"] == 120
    assert bool(row["sales_target_met"]) is True
    assert bool(row["collection_target_met"]) is True


@pytest.mark.asyncio
async def test_fetch_daily_sales_report_collections_preaggregated_by_normalized_order(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "daily_sales_report_collections_grain.db"
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
                VALUES ('CC-TD', 'TD Cost Center', 'value', 1)
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO sales (cost_center, payment_date, payment_received, adjustments, order_number, is_edited_order)
                VALUES
                    ('CC-TD', '2026-01-19 09:00:00', 100, 0, 'ORD123', 0),
                    ('CC-TD', '2026-01-19 12:00:00', 50, 0, ' ord123 ', 0),
                    ('CC-TD', '2026-01-19 13:00:00', 25, 0, 'Ord123', 0),
                    ('CC-TD', '2026-01-10 11:00:00', 40, 0, ' ORD-200 ', 0),
                    ('CC-TD', '2026-01-11 14:00:00', 20, 0, 'ORD-200', 0),
                    ('CC-TD', '2026-01-19 15:00:00', 500, 0, '', 0),
                    ('CC-TD', '2026-01-19 16:00:00', 600, 0, NULL, 0)
                """
            )
        )
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)
    td_row = next(row for row in report.rows if row.cost_center == "CC-TD")

    assert td_row.collections_ftd == 175
    assert td_row.collections_mtd == 235
    assert td_row.collections_count_ftd == 1
    assert td_row.collections_count_mtd == 2
