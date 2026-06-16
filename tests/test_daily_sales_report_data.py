from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql, sqlite
from app.reports.same_day_fulfillment import same_day_date_expr

from app.common.db import session_scope
from app.crm_downloader.td_orders_sync.ingest import (
    _orders_table as _td_orders_table,
    _stg_td_orders_table,
    ingest_td_orders_rows,
)
from app.dashboard_downloader.json_logger import get_logger
import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace



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
                    customer_name TEXT,
                    mobile_number TEXT,
                    net_amount NUMERIC,
                    gross_amount NUMERIC,
                    adjustment NUMERIC,
                    default_due_date TIMESTAMP,
                    source_system TEXT,
                    recovery_status TEXT,
                    recovery_category TEXT,
                    recovery_notes TEXT,
                    recovery_opened_at TIMESTAMP,
                    recovery_closed_at TIMESTAMP,
                    recovery_expected_resolution_date DATE
                )
                """
            )
        )
        conn.execute(
            sa.text(
                """
                CREATE VIEW vw_orders AS
                SELECT
                    *,
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
        conn.execute(
            sa.text(
                """
                CREATE TABLE orders_sync_log (
                    cost_center TEXT,
                    orders_pulled_at TIMESTAMP,
                    status TEXT,
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
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cost_center TEXT,
                    payment_date TIMESTAMP,
                    payment_received NUMERIC,
                    payment_mode TEXT,
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
                CREATE TABLE order_line_items (
                    cost_center TEXT,
                    order_number TEXT,
                    service_name TEXT,
                    garment_name TEXT
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
                CREATE TABLE crm_leads_current (
                    lead_uid TEXT,
                    store_code TEXT,
                    status_bucket TEXT,
                    customer_name TEXT,
                    mobile TEXT,
                    customer_type TEXT,
                    pickup_created_at TIMESTAMP,
                    reason TEXT,
                    cancelled_flag TEXT
                )
                """
            )
        )
        conn.execute(
            sa.text(
                """
                CREATE TABLE crm_leads_status_events (
                    lead_uid TEXT,
                    status_bucket TEXT,
                    scraped_at TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            sa.text(
                """
                CREATE TABLE payment_collections (
                    payment_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cost_center TEXT,
                    order_number TEXT,
                    amount NUMERIC DEFAULT 0,
                    source_type TEXT DEFAULT 'google_sheet'
                )
                """
            )
        )
        conn.execute(
            sa.text(
                """
                CREATE TABLE pipeline_run_summaries (
                    pipeline_name TEXT,
                    run_id TEXT,
                    report_date DATE,
                    finished_at TIMESTAMP,
                    created_at TIMESTAMP,
                    metrics_json JSON
                )
                """
            )
        )
        conn.execute(
            sa.text(
                """
                CREATE VIEW vw_orders_missing_in_payment_collections AS
                SELECT
                    o.cost_center,
                    o.order_number,
                    o.order_date,
                    o.customer_name,
                    o.mobile_number,
                    o.order_amount AS net_amount
                FROM vw_orders o
                JOIN sales s
                    ON s.cost_center = o.cost_center
                   AND s.order_number = o.order_number
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM payment_collections pc
                    WHERE pc.cost_center = o.cost_center
                      AND (',' || upper(replace(replace(coalesce(pc.order_number, ''), ' ', ''), '/', ',')) || ',')
                          LIKE ('%,' || upper(replace(coalesce(o.order_number, ''), ' ', '')) || ',%')
                )
                GROUP BY
                    o.cost_center,
                    o.order_number,
                    o.order_date,
                    o.customer_name,
                    o.mobile_number,
                    o.order_amount
                """
            )
        )
    engine.dispose()



@pytest.mark.asyncio
async def test_fetch_daily_sales_report_missing_payments_uses_source_aware_amount(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "daily_sales_report_missing_payments.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    engine = sa.create_engine(database_url.replace("+aiosqlite", ""))
    with engine.begin() as conn:
        conn.execute(sa.text("DROP VIEW vw_orders_missing_in_payment_collections"))
        conn.execute(
            sa.text(
                """
                CREATE VIEW vw_orders_missing_in_payment_collections AS
                SELECT
                    o.cost_center,
                    o.order_number,
                    o.order_date,
                    o.customer_name,
                    o.mobile_number,
                    o.order_amount AS net_amount
                FROM vw_orders o
                JOIN sales s
                    ON s.cost_center = o.cost_center
                   AND s.order_number = o.order_number
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM payment_collections pc
                    WHERE pc.cost_center = o.cost_center
                      AND (',' || upper(replace(replace(coalesce(pc.order_number, ''), ' ', ''), '/', ',')) || ',')
                          LIKE ('%,' || upper(replace(coalesce(o.order_number, ''), ' ', '')) || ',%')
                )
                GROUP BY
                    o.cost_center,
                    o.order_number,
                    o.order_date,
                    o.customer_name,
                    o.mobile_number,
                    o.order_amount
                """
            )
        )
    engine.dispose()

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr(_data_module, "get_timezone", lambda: tz)
    report_date = date(2026, 4, 29)

    async with session_scope(database_url) as session:
        await session.execute(
            sa.text(
                """
                INSERT INTO cost_center (cost_center, description, target_type, is_active)
                VALUES ('CC1', 'Store 1', 'value', 1)
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO store_master (id, cost_center, store_code, store_name, sync_group)
                VALUES (1, 'CC1', 'S1', 'Store One', 'TD')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO orders (
                    cost_center, order_number, order_date, customer_name, mobile_number,
                    net_amount, gross_amount, source_system, recovery_status
                ) VALUES
                    ('CC1', 'TD-MISSING', '2026-04-29T09:00:00+05:30', 'Tara', '9000000001', 500, 650, 'TumbleDry', 'NONE'),
                    ('CC1', 'UC-MISSING', '2026-04-29T10:00:00+05:30', 'Uma', '9000000002', 0, 910, 'UC', 'NONE'),
                ('CC1', 'UC-MATCHED', '2026-04-29T11:00:00+05:30', 'Maya', '9000000003', 300, 390, 'UC', 'NONE'),
                ('CC1', 'OLD-MISSING', '2026-03-20T11:15:00+05:30', 'Omar', '9000000099', 250, 325, 'TumbleDry', 'NONE'),
                ('CC1', 'ZERO-VALUE', '2026-04-29T11:30:00+05:30', 'Zed', '9000000004', 0, 0, 'TumbleDry', 'NONE')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO sales (
                    cost_center, order_number, payment_date, payment_received, payment_mode,
                    adjustments, is_edited_order
                ) VALUES
                    ('CC1', 'TD-MISSING', '2026-04-29T12:00:00+05:30', 500, 'UPI', 0, 0),
                    ('CC1', 'UC-MISSING', '2026-04-29T12:30:00+05:30', 910, 'CARD', 0, 0),
                    ('CC1', 'UC-MATCHED', '2026-04-29T13:00:00+05:30', 390, 'CASH', 0, 0),
                    ('CC1', 'OLD-MISSING', '2026-03-20T12:00:00+05:30', 250, 'UPI', 0, 0),
                    ('CC1', 'ZERO-VALUE', '2026-04-29T13:30:00+05:30', 0, 'CASH', 0, 0)
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO payment_collections (cost_center, order_number)
                VALUES ('CC1', ' ignore / uc-matched ,,')
                """
            )
        )
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)

    assert [(row.order_number, row.order_amount) for row in report.missing_payment_rows] == [
        ("OLD-MISSING", Decimal("250")),
        ("TD-MISSING", Decimal("500")),
        ("UC-MISSING", Decimal("910")),
    ]


@pytest.mark.asyncio
async def test_fetch_daily_sales_report_short_payments_separate_from_missing_payments(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "daily_sales_report_short_payments.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr(_data_module, "get_timezone", lambda: tz)
    report_date = date(2026, 4, 29)

    async with session_scope(database_url) as session:
        await session.execute(sa.text("""
            INSERT INTO cost_center (cost_center, description, target_type, is_active)
            VALUES ('CC1', 'Store 1', 'value', 1)
        """))
        await session.execute(sa.text("""
            INSERT INTO store_master (id, cost_center, store_code, store_name, sync_group)
            VALUES (1, 'CC1', 'S1', 'Store One', 'TD')
        """))
        await session.execute(sa.text("""
            INSERT INTO orders (
                cost_center, order_number, order_date, customer_name, mobile_number,
                net_amount, gross_amount, source_system, recovery_status
            ) VALUES
                ('CC1','OLD-SHORT','2026-04-01T08:00:00+05:30','Olive','111',100,100,'TumbleDry','NONE'),
                ('CC1','ZERO-VALUE','2026-04-15T08:00:00+05:30','Zed','112',0,0,'TumbleDry','NONE'),
                ('CC1','RECOVERY-SHORT','2026-04-28T08:00:00+05:30','Ria','113',100,100,'TumbleDry','TO_BE_RECOVERED'),
                ('CC1','COMP-SHORT','2026-04-28T08:30:00+05:30','Cia','114',100,100,'TumbleDry','TO_BE_COMPENSATED'),
                ('CC1','SINGLE','2026-04-29T09:00:00+05:30','Alice','999',100,100,'TumbleDry','NONE'),
                ('CC1','GROUP1','2026-04-29T10:00:00+05:30','Bob','888',100,100,'TumbleDry','NONE'),
                ('CC1','GROUP2','2026-04-29T11:00:00+05:30','Cara','777',200,200,'TumbleDry','NONE'),
                ('CC1','MISSING','2026-04-29T12:00:00+05:30','Dan','666',120,120,'TumbleDry','NONE'),
                ('CC1','PROOFONLY','2026-04-29T12:15:00+05:30','Polly','665',100,100,'TumbleDry','NONE'),
                ('CC1','MISMATCH','2026-04-29T12:30:00+05:30','Mia','664',100,100,'TumbleDry','NONE'),
                ('CC1','NULL-STATUS-SHORT','2026-04-29T12:40:00+05:30','Nia','663',100,100,'TumbleDry',NULL),
                ('CC1','CUSTOM-STATUS-SHORT','2026-04-29T12:50:00+05:30','Cal','662',100,100,'TumbleDry','CUSTOM_STATUS'),
                ('CC1','REC','2026-04-29T13:00:00+05:30','Eve','555',150,150,'TumbleDry','WRITE_OFF')
        """))
        await session.execute(sa.text("""
            INSERT INTO sales (cost_center, order_number, payment_date, payment_received, payment_mode, adjustments, is_edited_order) VALUES
                ('CC1','OLD-SHORT','2026-04-01T08:30:00+05:30',80,'UPI',0,0),
                ('CC1','ZERO-VALUE','2026-04-15T08:30:00+05:30',0,'UPI',0,0),
                ('CC1','RECOVERY-SHORT','2026-04-28T08:30:00+05:30',80,'UPI',0,0),
                ('CC1','COMP-SHORT','2026-04-28T09:00:00+05:30',80,'UPI',0,0),
                ('CC1','SINGLE','2026-04-29T09:30:00+05:30',80,'UPI',0,0),
                ('CC1','GROUP1','2026-04-29T10:30:00+05:30',100,'UPI',0,0),
                ('CC1','GROUP2','2026-04-29T11:30:00+05:30',50,'UPI',0,0),
                ('CC1','MISSING','2026-04-29T12:30:00+05:30',120,'UPI',0,0),
                ('CC1','MISMATCH','2026-04-29T12:45:00+05:30',90,'UPI',0,0),
                ('CC1','NULL-STATUS-SHORT','2026-04-29T12:50:00+05:30',80,'UPI',0,0),
                ('CC1','CUSTOM-STATUS-SHORT','2026-04-29T12:55:00+05:30',80,'UPI',0,0)
        """))
        await session.execute(sa.text("""
            INSERT INTO payment_collections (cost_center, order_number, amount, source_type) VALUES
                ('CC1','OLD-SHORT',80,'google_sheet'),
                ('CC1','ZERO-VALUE',0,'google_sheet'),
                ('CC1','RECOVERY-SHORT',80,'google_sheet'),
                ('CC1','COMP-SHORT',80,'google_sheet'),
                ('CC1','SINGLE',80,'google_sheet'),
                ('CC1','GROUP1/GROUP2',150,'google_sheet'),
                ('CC1','PROOFONLY',80,'google_sheet'),
                ('CC1','MISMATCH',80,'google_sheet'),
                ('CC1','NULL-STATUS-SHORT',80,'google_sheet'),
                ('CC1','CUSTOM-STATUS-SHORT',80,'google_sheet'),
                ('CC1','REC',10,'google_sheet')
        """))
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)

    assert [(row.order_number, row.paid_amount, row.shortage_amount, row.group_key) for row in report.short_payment_rows] == [
        ("OLD-SHORT", Decimal("80"), Decimal("20"), None),
        ("SINGLE", Decimal("80"), Decimal("20"), None),
        ("GROUP2", Decimal("50"), Decimal("150"), "GROUP1|GROUP2"),
    ]
    assert {row.order_number for row in report.short_payment_rows}.isdisjoint(
        {"NULL-STATUS-SHORT", "CUSTOM-STATUS-SHORT"}
    )
    assert [row.order_number for row in report.missing_payment_rows] == ["MISSING"]


@pytest.mark.asyncio
async def test_fetch_daily_sales_report_actual_payments_not_found_requires_sales_payment_record(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "daily_sales_report_actual_payments_not_found_sales.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr(_data_module, "get_timezone", lambda: tz)
    report_date = date(2026, 4, 29)

    async with session_scope(database_url) as session:
        await session.execute(sa.text("""
            INSERT INTO cost_center (cost_center, description, target_type, is_active)
            VALUES ('CC1', 'Store 1', 'value', 1)
        """))
        await session.execute(sa.text("""
            INSERT INTO store_master (id, cost_center, store_code, store_name, sync_group)
            VALUES (1, 'CC1', 'S1', 'Store One', 'TD')
        """))
        await session.execute(sa.text("""
            INSERT INTO orders (
                cost_center, order_number, order_date, customer_name, mobile_number,
                net_amount, gross_amount, source_system, recovery_status
            ) VALUES
                ('CC1','SALES-NO-PROOF','2026-04-29T09:00:00+05:30','Alice','999',100,100,'TumbleDry','NONE'),
                ('CC1','NO-SALES-NO-PROOF','2026-04-29T10:00:00+05:30','Bob','888',200,200,'TumbleDry','NONE'),
                ('CC1','NO-SALES-WITH-PROOF','2026-04-29T11:00:00+05:30','Cara','777',300,300,'TumbleDry','NONE'),
                ('CC1','ZERO-VALUE','2026-04-29T12:00:00+05:30','Zed','666',0,0,'TumbleDry','NONE'),
                ('CC1','RECOVERY','2026-04-29T13:00:00+05:30','Ron','555',400,400,'TumbleDry','WRITE_OFF')
        """))
        await session.execute(sa.text("""
            INSERT INTO sales (cost_center, order_number, payment_date, payment_received, payment_mode, adjustments, is_edited_order) VALUES
                ('CC1','SALES-NO-PROOF','2026-04-29T09:30:00+05:30',100,'UPI',0,0),
                ('CC1','ZERO-VALUE','2026-04-29T12:30:00+05:30',50,'CASH',0,0),
                ('CC1','RECOVERY','2026-04-29T13:30:00+05:30',400,'CARD',0,0)
        """))
        await session.execute(sa.text("""
            INSERT INTO payment_collections (cost_center, order_number, amount, source_type) VALUES
                ('CC1','NO-SALES-WITH-PROOF',300,'google_sheet')
        """))
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)

    assert [(row.order_number, row.order_amount) for row in report.missing_payment_rows] == [
        ("SALES-NO-PROOF", Decimal("100")),
    ]


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
async def test_fetch_daily_sales_report_same_day_fulfillment_rows(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "daily_sales_report_same_day.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr(_data_module, "get_timezone", lambda: tz)
    report_date = date(2026, 4, 29)
    async with session_scope(database_url) as session:
        await session.execute(sa.text("INSERT INTO cost_center (cost_center, description, target_type, is_active) VALUES ('CC1','Store 1','value',1)"))
        await session.execute(sa.text("INSERT INTO store_master (id, cost_center, store_code, store_name, sync_group) VALUES (1,'CC1','S1','Store One','TD')"))
        await session.execute(
            sa.text(
                "INSERT INTO orders (cost_center, order_number, order_date, customer_name, mobile_number, net_amount, gross_amount, source_system) "
                "VALUES ('CC1','O100', :order_dt, 'Ravi', '9000000000', 500, 500, 'TumbleDry')"
            ),
            {"order_dt": "2026-04-29T09:00:00+05:30"},
        )
        await session.execute(
            sa.text(
                "INSERT INTO sales (cost_center, order_number, payment_date, payment_received, payment_mode, adjustments, is_edited_order) "
                "VALUES ('CC1','O100', :payment_dt, 500, 'UPI', 0, 0)"
            ),
            {"payment_dt": "2026-04-29T13:30:00+05:30"},
        )
        await session.execute(
            sa.text(
                "INSERT INTO order_line_items (cost_center, order_number, service_name, garment_name) VALUES "
                "('CC1','O100','Dryclean','Shirt'),"
                "('CC1','O100','Steam','Trouser')"
            )
        )
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)
    assert len(report.same_day_fulfillment_rows) == 1
    row = report.same_day_fulfillment_rows[0]
    assert row.store_code == "S1"
    assert row.order_number == "O100"
    assert row.payment_mode == "UPI"
    assert row.line_items == "Dryclean Shirt × 1 | Steam Trouser × 1"
    assert row.order_amount == 500
    assert row.payment_received == 500
    assert str(row.hours) == "4.50"




@pytest.mark.asyncio
async def test_fetch_daily_sales_report_same_day_fulfillment_aggregates_multiple_payments(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "daily_sales_report_same_day_multi.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr(_data_module, "get_timezone", lambda: tz)
    report_date = date(2026, 4, 29)
    async with session_scope(database_url) as session:
        await session.execute(sa.text("INSERT INTO cost_center (cost_center, description, target_type, is_active) VALUES ('CC1','Store 1','value',1)"))
        await session.execute(sa.text("INSERT INTO store_master (id, cost_center, store_code, store_name, sync_group) VALUES (1,'CC1','S1','Store One','TD')"))
        await session.execute(sa.text("INSERT INTO orders (cost_center, order_number, order_date, customer_name, mobile_number, net_amount, gross_amount, source_system) VALUES ('CC1','O200', :order_dt, 'Ravi', '9000000000', 500, 500, 'TumbleDry')"), {"order_dt": "2026-04-29T09:00:00+05:30"})
        await session.execute(sa.text("INSERT INTO sales (cost_center, order_number, payment_date, payment_received, payment_mode, adjustments, is_edited_order) VALUES ('CC1','O200', :p1, 300, 'UPI', 0, 0),('CC1','O200', :p2, 200, 'Cash', 0, 0)"), {"p1": "2026-04-29T11:00:00+05:30", "p2": "2026-04-29T12:00:00+05:30"})
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)
    row = report.same_day_fulfillment_rows[0]
    assert row.payment_received == 500
    assert "Cash" in row.payment_mode
    assert "UPI" in row.payment_mode
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
                INSERT INTO crm_leads_current (lead_uid, store_code, status_bucket, pickup_created_at) VALUES
                    ('U1', 'UN', 'completed', '2026-04-01 02:00:00+00:00'),
                    ('U2', ' un ', ' Completed ', '2026-04-22 12:00:00+00:00'),
                    ('U3', 'UN ', ' pending ', '2026-04-23 16:00:00+00:00'),
                    ('U4', ' un', ' pending ', '2026-04-10 05:00:00+00:00'),
                    ('U8', 'UN', 'cancelled', '2026-04-01 12:00:00+00:00'),
                    ('U5', 'UN', 'completed', '2026-03-31 23:59:00+00:00'),
                    ('U6', 'UN', 'cancelled', '2026-04-24 00:10:00+00:00'),
                    ('U7', 'UN', 'completed', '2026-04-11 00:10:00+00:00')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO crm_leads_status_events (lead_uid, status_bucket) VALUES
                    ('U3', 'cancelled'),
                    ('U7', 'cancelled')
                """
            )
        )
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)

    un_summary = next(item for item in report.lead_performance_summary if item["store"] == "UN")
    kn_summary = next(item for item in report.lead_performance_summary if item["store"] == "KN")

    assert un_summary["period_type"] == "MTD"
    assert un_summary["period_start"] == "2026-04-01"
    assert un_summary["period_end"] == "2026-04-30"
    assert un_summary["total_leads"] == 7
    assert un_summary["completed_leads"] == 3
    assert un_summary["cancelled_leads"] == 2
    assert un_summary["pending_leads"] == 2
    assert un_summary["conversion_pct"] == {"value": 42.86, "color": "RED", "status": "POOR"}
    assert un_summary["cancelled_pct"] == {"value": 28.57, "color": "RED", "status": "HIGH_LEAKAGE"}
    assert un_summary["pending_pct"] == {"value": 28.57, "color": "RED", "status": "FOLLOW_UP_GAP"}
    assert un_summary["benchmark"] == {
        "conversion_target": 85.0,
        "conversion_min": 70.0,
        "cancelled_target": 10.0,
        "cancelled_max": 20.0,
        "pending_max": 5.0,
    }
    assert un_summary["conversion_gap"] == -42.14
    assert un_summary["cancelled_gap"] == 18.57
    assert un_summary["pending_gap"] == 23.57

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
                INSERT INTO crm_leads_current (
                    lead_uid, store_code, status_bucket, customer_name, mobile, pickup_created_at, reason, cancelled_flag
                ) VALUES
                    ('L1', 'UN', 'cancelled', 'Customer Cancelled', '9000000001', '2026-04-02 00:00:00', NULL, 'customer'),
                    ('L7', 'UN', 'cancelled', 'Persisted Store Flag', '9000000007', '2026-04-08 00:00:00', '   ', 'store'),
                    ('L2', 'UN', ' completed ', 'Reopened Completed', '9000000002', '2026-04-30 23:59:00', 'Requested defer', 'store'),
                    ('L6', 'UN', ' pending ', 'Reopened Pending', '9000000006', '2026-04-12 11:59:00', NULL, 'store'),
                    ('L3', 'UN', 'completed', 'Completed Lead', '9000000003', '2026-04-15 10:00:00', NULL, NULL),
                    ('L4', 'UN', 'cancelled', 'March Cancelled', '9000000004', '2026-03-30 23:59:00', NULL, 'store'),
                    ('L5', 'UN', 'cancelled', 'May Cancelled', '9000000005', '2026-05-02 00:00:00', NULL, 'store')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO crm_leads_status_events (lead_uid, status_bucket) VALUES
                    ('L2', 'cancelled'),
                    ('L6', 'cancelled')
                """
            )
        )
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)

    assert report.cancelled_leads == [
        {
            "store_name": "Uttam Nagar",
            "total_cancelled_count": 2,
            "customer_cancelled_count": 1,
            "store_cancelled_rows": [
                {
                    "customer_name": "Persisted Store Flag",
                    "mobile": "9000000007",
                    "reason": "--",
                }
            ],
        }
    ]
    un_summary = next(item for item in report.lead_performance_summary if item["store"] == "UN")
    assert un_summary["total_leads"] == 5
    assert un_summary["completed_leads"] == 2
    assert un_summary["cancelled_leads"] == 2
    assert un_summary["pending_leads"] == 1


@pytest.mark.asyncio
async def test_fetch_daily_sales_report_uses_customer_type_when_present(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "daily_sales_report_customer_type_present.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr(_data_module, "get_timezone", lambda: tz)
    report_date = date(2026, 4, 23)

    async with session_scope(database_url) as session:
        await session.execute(sa.text("INSERT INTO cost_center (cost_center, description, target_type, is_active) VALUES ('CC-TD','TD','value',1)"))
        await session.execute(
            sa.text("INSERT INTO store_master (id, cost_center, store_code, store_name, sync_group) VALUES (1, 'CC-TD', 'UN', 'Uttam Nagar', 'TD')")
        )
        await session.execute(
            sa.text(
                "INSERT INTO crm_leads_current (lead_uid, store_code, status_bucket, customer_name, mobile, customer_type, pickup_created_at, reason, cancelled_flag) "
                "VALUES ('L1', 'UN', 'cancelled', 'Customer One', '9000000001', 'Existing', '2026-04-02 00:00:00', 'NA', 'store')"
            )
        )
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)
    assert report.cancelled_leads[0]["store_cancelled_rows"][0]["is_existing_customer_cancelled"] is True


@pytest.mark.asyncio
async def test_fetch_daily_sales_report_falls_back_when_customer_type_missing(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "daily_sales_report_customer_type_missing.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr(_data_module, "get_timezone", lambda: tz)
    report_date = date(2026, 4, 23)

    async with session_scope(database_url) as session:
        await session.execute(sa.text("DROP TABLE crm_leads_current"))
        await session.execute(
            sa.text(
                "CREATE TABLE crm_leads_current (lead_uid TEXT, store_code TEXT, status_bucket TEXT, customer_name TEXT, mobile TEXT, pickup_created_at TIMESTAMP, reason TEXT, cancelled_flag TEXT)"
            )
        )
        await session.execute(sa.text("INSERT INTO cost_center (cost_center, description, target_type, is_active) VALUES ('CC-TD','TD','value',1)"))
        await session.execute(
            sa.text("INSERT INTO store_master (id, cost_center, store_code, store_name, sync_group) VALUES (1, 'CC-TD', 'UN', 'Uttam Nagar', 'TD')")
        )
        await session.execute(
            sa.text(
                "INSERT INTO crm_leads_current (lead_uid, store_code, status_bucket, customer_name, mobile, pickup_created_at, reason, cancelled_flag) "
                "VALUES ('L1', 'UN', 'cancelled', 'Customer One', '9000000001', '2026-04-02 00:00:00', 'NA', 'store')"
            )
        )
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)
    assert report.cancelled_leads[0]["store_cancelled_rows"][0]["is_existing_customer_cancelled"] is False


def test_completed_reconciliation_string_agg_requires_keyword_arguments() -> None:
    with pytest.raises(TypeError):
        _data_module.string_list_agg(sa.literal_column("order_number"))  # type: ignore[call-arg]


@pytest.mark.asyncio
async def test_fetch_daily_sales_report_orders_sync_uses_successful_refresh_timestamp(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "daily_sales_report_sync_success.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr(_data_module, "get_timezone", lambda: tz)
    report_date = date(2026, 1, 19)

    async with session_scope(database_url) as session:
        await session.execute(
            sa.text("INSERT INTO cost_center (cost_center, description, target_type, is_active) VALUES ('CC-TD', 'TD Cost Center', 'value', 1)")
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO orders_sync_log (cost_center, orders_pulled_at, status, updated_at, created_at)
                VALUES ('CC-TD', '2026-01-19 07:20:00', 'success', '2026-01-19 07:25:00', '2026-01-19 07:10:00')
                """
            )
        )
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)

    td_row = next(row for row in report.rows if row.cost_center == "CC-TD")
    assert td_row.orders_sync_time == "07:20"
    assert td_row.last_successful_orders_refresh_at == datetime(2026, 1, 19, 7, 20, tzinfo=tz)
    assert td_row.last_orders_sync_attempt_at == datetime(2026, 1, 19, 7, 25, tzinfo=tz)
    assert td_row.latest_orders_sync_outcome == "success"
    assert td_row.orders_sync_warning is False


@pytest.mark.asyncio
async def test_fetch_daily_sales_report_deprecates_td_leads_sync_payload(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "daily_sales_report_td_leads_sync_metrics.db"
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
                VALUES (1, 'CC-TD', 'UN', 'Uttam Nagar', 'TD')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO pipeline_run_summaries (
                    pipeline_name, run_id, report_date, finished_at, created_at, metrics_json
                ) VALUES (
                    'td_crm_leads_sync', 'run-td-1', '2026-04-23', '2026-04-23 10:00:00', '2026-04-23 10:00:00',
                    :metrics_json
                )
                """
            ),
            {
                "metrics_json": """
                {"stores":[{"store_code":"UN","bucket_write_counts":{"pending":{"created":1,"updated":2},"completed":{"created":3,"updated":4},"cancelled":{"created":5,"updated":6}},"status_transitions":[{"from_status_bucket":"pending","to_status_bucket":"completed"}]}]}
                """
            },
        )
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)

    assert report.td_leads_sync_metrics == {}
    assert report.td_leads_sync_lead_changes == {}


@pytest.mark.asyncio
async def test_fetch_daily_sales_report_deprecates_td_lead_change_details_payload(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "daily_sales_report_td_lead_changes.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr(_data_module, "get_timezone", lambda: tz)
    report_date = date(2026, 4, 23)

    async with session_scope(database_url) as session:
        await session.execute(
            sa.text("INSERT INTO cost_center (cost_center, description, target_type, is_active) VALUES ('CC-TD', 'TD Cost Center', 'value', 1)")
        )
        await session.execute(
            sa.text("INSERT INTO store_master (id, cost_center, store_code, store_name, sync_group) VALUES (1, 'CC-TD', 'UN', 'Uttam Nagar', 'TD')")
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO pipeline_run_summaries (
                    pipeline_name, run_id, report_date, finished_at, created_at, metrics_json
                ) VALUES (
                    'td_crm_leads_sync', 'run-td-2', '2026-04-23', '2026-04-23 11:00:00', '2026-04-23 11:00:00',
                    :metrics_json
                )
                """
            ),
            {
                "metrics_json": """
                {"stores":[{"store_code":"UN","lead_change_details":{"cap_per_group":20,"created_by_bucket":[{"status_bucket":"pending","rows":[{"customer_name":"Nia","mobile":"9000000000","action":"created","current_status_bucket":"pending","previous_status_bucket":null}],"overflow_count":1}],"updated_by_bucket":[],"transitions":[]}}]}
                """
            },
        )
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)
    assert report.td_leads_sync_metrics == {}
    assert report.td_leads_sync_lead_changes == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("latest_status", ["failed", "skipped"])
async def test_fetch_daily_sales_report_orders_sync_warns_after_unsuccessful_newer_attempt(
    tmp_path, monkeypatch, latest_status
) -> None:
    db_path = tmp_path / f"daily_sales_report_sync_{latest_status}.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr(_data_module, "get_timezone", lambda: tz)
    report_date = date(2026, 1, 19)

    async with session_scope(database_url) as session:
        await session.execute(
            sa.text("INSERT INTO cost_center (cost_center, description, target_type, is_active) VALUES ('CC-TD', 'TD Cost Center', 'value', 1)")
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO orders_sync_log (cost_center, orders_pulled_at, status, updated_at, created_at) VALUES
                ('CC-TD', '2026-01-19 07:20:00', 'success', '2026-01-19 07:25:00', '2026-01-19 07:10:00'),
                ('CC-TD', NULL, :latest_status, '2026-01-19 08:05:00', '2026-01-19 08:00:00')
                """
            ),
            {"latest_status": latest_status},
        )
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)

    td_row = next(row for row in report.rows if row.cost_center == "CC-TD")
    assert td_row.orders_sync_time == "07:20"
    assert td_row.last_successful_orders_refresh_at == datetime(2026, 1, 19, 7, 20, tzinfo=tz)
    assert td_row.last_orders_sync_attempt_at == datetime(2026, 1, 19, 8, 5, tzinfo=tz)
    assert td_row.latest_orders_sync_outcome == latest_status
    assert td_row.orders_sync_warning is True


@pytest.mark.asyncio
@pytest.mark.parametrize(("latest_status", "expected_warning"), [("failed", True), ("success", False)])
async def test_fetch_daily_sales_report_orders_sync_does_not_fake_success_from_attempt_timestamps(
    tmp_path, monkeypatch, latest_status, expected_warning
) -> None:
    db_path = tmp_path / f"daily_sales_report_sync_without_refresh_{latest_status}.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr(_data_module, "get_timezone", lambda: tz)
    report_date = date(2026, 1, 19)

    async with session_scope(database_url) as session:
        await session.execute(
            sa.text("INSERT INTO cost_center (cost_center, description, target_type, is_active) VALUES ('CC-TD', 'TD Cost Center', 'value', 1)")
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO orders_sync_log (cost_center, orders_pulled_at, status, updated_at, created_at)
                VALUES ('CC-TD', NULL, :latest_status, '2026-01-19 08:05:00', '2026-01-19 08:00:00')
                """
            ),
            {"latest_status": latest_status},
        )
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)

    td_row = next(row for row in report.rows if row.cost_center == "CC-TD")
    assert td_row.orders_sync_time is None
    assert td_row.last_successful_orders_refresh_at is None
    assert td_row.last_orders_sync_attempt_at == datetime(2026, 1, 19, 8, 5, tzinfo=tz)
    assert td_row.latest_orders_sync_outcome == latest_status
    assert td_row.orders_sync_warning is expected_warning


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("target_compute_type", "expected_title", "expected_targets", "expected_achieved"),
    [
        (
            "SALES",
            "Sales Target",
            {"CC1": Decimal("1000"), "CC2": Decimal("2000")},
            {"CC1": Decimal("400"), "CC2": Decimal("300")},
        ),
        (
            "COLLECTIONS",
            "Collections Target",
            {"CC1": Decimal("700"), "CC2": Decimal("900")},
            {"CC1": Decimal("250"), "CC2": Decimal("200")},
        ),
    ],
)
async def test_fetch_daily_sales_report_target_mode_uses_sales_or_allocated_collections(
    tmp_path, monkeypatch, target_compute_type, expected_title, expected_targets, expected_achieved
) -> None:
    db_path = tmp_path / f"daily_sales_report_target_{target_compute_type.lower()}.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr(_data_module, "get_timezone", lambda: tz)
    monkeypatch.setattr(_data_module, "config", SimpleNamespace(target_compute_type=target_compute_type))
    report_date = date(2026, 1, 19)

    async with session_scope(database_url) as session:
        await session.execute(sa.text("""
            INSERT INTO cost_center (cost_center, description, target_type, is_active)
            VALUES ('CC1', 'Store 1', 'value', 1), ('CC2', 'Store 2', 'value', 1)
        """))
        await session.execute(sa.text("""
            INSERT INTO cost_center_targets (month, year, cost_center, sale_target, collection_target)
            VALUES (1, 2026, 'CC1', 1000, 700), (1, 2026, 'CC2', 2000, 900)
        """))
        await session.execute(sa.text("""
            INSERT INTO orders (cost_center, order_number, order_date, net_amount, gross_amount, source_system, recovery_status)
            VALUES
                ('CC1', 'CC1-MTD-1', '2026-01-05 10:00:00', 300, 300, 'TumbleDry', 'NONE'),
                ('CC1', 'CC1-MTD-2', '2026-01-19 10:00:00', 100, 100, 'TumbleDry', 'NONE'),
                ('CC1', 'CC1-OLD', '2025-12-31 10:00:00', 500, 500, 'TumbleDry', 'NONE'),
                ('CC2', 'CC2-MTD-1', '2026-01-10 10:00:00', 300, 300, 'TumbleDry', 'NONE')
        """))
        await session.execute(sa.text("""
            INSERT INTO sales (cost_center, payment_date, payment_received, adjustments, order_number, is_edited_order)
            VALUES
                ('CC1', '2026-01-05 11:00:00', 999, 0, 'CC1-MTD-1', 0),
                ('CC1', '2026-01-19 11:00:00', 100, 0, 'CC1-MTD-2', 0),
                ('CC1', '2026-01-19 12:00:00', 500, 0, 'CC1-OLD', 0),
                ('CC2', '2026-01-10 11:00:00', 300, 0, 'CC2-MTD-1', 0)
        """))
        await session.execute(sa.text("""
            INSERT INTO payment_collections (cost_center, order_number, amount, source_type)
            VALUES
                ('CC1', 'CC1-MTD-1/CC1-MTD-2', 250, 'google_sheet'),
                ('CC1', 'CC1-OLD', 500, 'google_sheet'),
                ('CC2', 'CC2-MTD-1', 200, 'google_sheet')
        """))
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)
    rows = {row.cost_center: row for row in report.rows}

    assert report.target_compute_type == target_compute_type
    assert report.target_section_title == expected_title
    for cost_center, expected_target in expected_targets.items():
        assert rows[cost_center].target == expected_target
        assert rows[cost_center].achieved == expected_achieved[cost_center]
        assert rows[cost_center].delta == expected_achieved[cost_center] - expected_target
    assert report.totals.target == sum(expected_targets.values(), Decimal("0"))
    assert report.totals.achieved == sum(expected_achieved.values(), Decimal("0"))
    assert report.totals.delta == sum(expected_achieved.values(), Decimal("0")) - sum(expected_targets.values(), Decimal("0"))
    assert rows["CC1"].sales_mtd == Decimal("400")
    assert rows["CC1"].collections_mtd == Decimal("1599")


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
                INSERT INTO orders (cost_center, order_number, order_date, net_amount, source_system)
                VALUES
                    ('CC-TD', 'ORD-1', '2026-01-05 10:00:00', 100, 'TumbleDry'),
                    ('CC-TD', 'ORD-2', '2026-01-10 11:00:00', 60, 'TumbleDry')
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
async def test_fetch_daily_sales_report_order_amount_from_vw_orders_keeps_collections_from_sales(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "daily_sales_report_order_vs_collection.db"
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
                VALUES ('CC-UC', 'UC Cost Center', 'value', 1)
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO orders (
                    cost_center, order_number, order_date, net_amount, gross_amount, source_system
                ) VALUES ('CC-UC', 'UC-1', '2026-01-19 10:00:00', 0, 1000, 'UC')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO sales (
                    cost_center, payment_date, payment_received, adjustments, order_number, is_edited_order
                ) VALUES ('CC-UC', '2026-01-19 12:00:00', 400, 0, 'UC-1', 0)
                """
            )
        )
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)
    uc_row = next(row for row in report.rows if row.cost_center == "CC-UC")

    assert uc_row.sales_ftd == 1000
    assert uc_row.sales_mtd == 1000
    assert uc_row.collections_ftd == 400
    assert uc_row.collections_mtd == 400


@pytest.mark.asyncio
async def test_fetch_daily_sales_report_edited_loss_uses_standardized_order_amount(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "daily_sales_report_edited_order_amount.db"
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
                VALUES ('CC-UC', 'UC Cost Center', 'value', 1)
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO orders (
                    cost_center, order_number, order_date, net_amount, gross_amount, source_system
                ) VALUES ('CC-UC', 'UC-EDIT', '2026-01-18 10:00:00', 0, 1000, 'UC')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO sales (
                    cost_center, payment_date, payment_received, adjustments, order_number, is_edited_order
                ) VALUES ('CC-UC', '2026-01-19 12:00:00', 700, 0, 'UC-EDIT', 1)
                """
            )
        )
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)

    assert [
        (row.order_number, row.original_value, row.new_value, row.loss)
        for row in report.edited_orders
    ] == [("UC-EDIT", Decimal("1000"), Decimal("700"), Decimal("300"))]
    assert report.edited_orders_summary is not None
    assert report.edited_orders_summary.sum_orig_distinct == 1000
    assert report.edited_orders_summary.sum_new_distinct == 700


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


@pytest.mark.asyncio
async def test_fetch_daily_sales_report_manual_recovery_sections(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "daily_sales_report_manual_recovery.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr(_data_module, "get_timezone", lambda: tz)
    report_date = date(2026, 4, 25)

    async with session_scope(database_url) as session:
        await session.execute(
            sa.text(
                """
                INSERT INTO cost_center (cost_center, description, target_type, is_active)
                VALUES ('CC-TD', 'TD Store', 'value', 1), ('CC-UC', 'UC Store', 'value', 1)
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO orders (
                    cost_center, order_number, order_date, customer_name, mobile_number,
                    net_amount, gross_amount, default_due_date, source_system, recovery_status,
                    recovery_opened_at, recovery_closed_at, recovery_expected_resolution_date
                ) VALUES
                    ('CC-TD', 'TD-R1', '2026-04-20 10:00:00', 'Asha', '9000000001', 100, 120, '2026-04-20 10:00:00', 'TumbleDry', 'TO_BE_RECOVERED', '2026-04-20 10:00:00', NULL, '2026-04-23'),
                    ('CC-TD', 'TD-R2', '2026-03-10 10:00:00', 'Ira', '9000000002', 200, 230, '2026-03-10 10:00:00', 'TumbleDry', 'TO_BE_RECOVERED', '2026-03-10 10:00:00', NULL, '2026-04-05'),
                    ('CC-UC', 'UC-C1', '2026-01-10 10:00:00', 'Meera', '9000000003', 50, 300, '2026-01-10 10:00:00', 'UClean', 'TO_BE_COMPENSATED', '2026-01-10 10:00:00', NULL, '2026-02-15'),
                    ('CC-TD', 'TD-X1', '2026-02-10 10:00:00', 'Nia', '9000000004', 90, 100, '2026-02-10 10:00:00', 'TumbleDry', 'COMPENSATED', '2026-04-10 09:00:00', '2026-04-25 11:30:00', '2026-04-22')
                """
            )
        )
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)

    assert report.auto_cleared_order_numbers == []
    assert report.auto_cleared_order_numbers_text == ""
    assert len(report.to_be_recovered) == 2
    assert [row.order_number for row in report.to_be_recovered] == ["TD-R2", "TD-R1"]
    assert report.to_be_recovered[0].customer_name == "Ira"
    assert report.to_be_recovered[1].mobile_number == "9000000001"
    assert report.to_be_recovered_total_order_value == 300

    assert len(report.to_be_compensated) == 1
    compensated = report.to_be_compensated[0]
    assert compensated.cost_center == "CC-UC"
    assert compensated.order_number == "UC-C1"
    assert compensated.customer_name == "Meera"
    assert compensated.mobile_number == "9000000003"
    assert report.to_be_compensated_total_order_value == 300


def test_string_list_agg_helper_compiles_for_postgres_and_sqlite() -> None:
    expr_pg = _data_module.string_list_agg(
        dialect_name="postgresql",
        value_expr=sa.literal_column("payment_mode"),
        separator=", ",
    )
    expr_sqlite = _data_module.string_list_agg(
        dialect_name="sqlite",
        value_expr=sa.literal_column("payment_mode"),
        separator=", ",
    )

    pg_sql = str(sa.select(expr_pg).compile(dialect=postgresql.dialect()))
    sqlite_sql = str(sa.select(expr_sqlite).compile(dialect=sqlite.dialect()))

    assert "string_agg" in pg_sql.lower()
    assert "group_concat" in sqlite_sql.lower()


def test_same_day_aggregation_statements_compile_with_postgres_safe_functions() -> None:
    order_line_items = sa.table(
        "order_line_items",
        sa.column("cost_center"),
        sa.column("order_number"),
        sa.column("service_name"),
        sa.column("garment_name"),
    )
    sales = sa.table(
        "sales",
        sa.column("cost_center"),
        sa.column("order_number"),
        sa.column("payment_mode"),
        sa.column("payment_received"),
        sa.column("payment_date"),
    )
    orders = sa.table(
        "orders",
        sa.column("cost_center"),
        sa.column("order_number"),
        sa.column("order_date"),
    )

    line_items_stmt = sa.select(
        _data_module.string_list_agg(
            dialect_name="postgresql",
            value_expr=sa.func.trim(
                sa.func.coalesce(order_line_items.c.service_name, "")
                + sa.literal(" ")
                + sa.func.coalesce(order_line_items.c.garment_name, "")
            ),
            separator=", ",
        )
    ).group_by(order_line_items.c.cost_center, order_line_items.c.order_number)

    same_day_stmt = (
        sa.select(
            _data_module.string_list_agg(
                dialect_name="postgresql",
                value_expr=sa.func.coalesce(sales.c.payment_mode, ""),
                separator=", ",
            ).label("payment_mode"),
            sa.func.sum(sa.func.coalesce(sales.c.payment_received, 0)).label("payment_received"),
        )
        .select_from(
            orders.join(
                sales,
                sa.and_(orders.c.cost_center == sales.c.cost_center, orders.c.order_number == sales.c.order_number),
            )
        )
        .group_by(orders.c.cost_center, orders.c.order_number, orders.c.order_date)
    )

    line_items_sql = str(line_items_stmt.compile(dialect=postgresql.dialect())).lower()
    same_day_sql = str(same_day_stmt.compile(dialect=postgresql.dialect())).lower()

    assert "string_agg" in line_items_sql
    assert "group_concat" not in line_items_sql
    assert "string_agg" in same_day_sql
    assert "group_concat" not in same_day_sql


def test_report_day_orders_aggregation_compiles_postgres_with_ordered_string_agg() -> None:
    cost_center = sa.table(
        "cost_center",
        sa.column("cost_center"),
        sa.column("is_active"),
    )
    report_day_orders_base = sa.table(
        "report_day_orders_base",
        sa.column("cost_center"),
        sa.column("order_number"),
    )

    report_day_orders_stmt = (
        sa.select(
            cost_center.c.cost_center.label("cost_center"),
            sa.func.coalesce(
                sa.func.string_agg(
                    report_day_orders_base.c.order_number,
                    sa.dialects.postgresql.aggregate_order_by(
                        sa.literal(", "),
                        report_day_orders_base.c.order_number.asc(),
                    ),
                ),
                sa.literal("-"),
            ).label("order_numbers_text"),
        )
        .select_from(
            cost_center.outerjoin(
                report_day_orders_base,
                report_day_orders_base.c.cost_center == cost_center.c.cost_center,
            )
        )
        .where(cost_center.c.is_active.is_(True))
        .group_by(cost_center.c.cost_center)
        .order_by(cost_center.c.cost_center.asc())
    )

    compiled_sql = str(
        report_day_orders_stmt.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    ).lower()
    assert "string_agg" in compiled_sql
    assert "group_concat" not in compiled_sql
    assert (
        "string_agg(report_day_orders_base.order_number, ', ' order by report_day_orders_base.order_number asc)"
        in compiled_sql
    )


@pytest.mark.asyncio
async def test_fetch_daily_sales_report_same_day_fulfillment_ignores_time_component(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "daily_sales_report_same_day_date_only.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr(_data_module, "get_timezone", lambda: tz)
    report_date = date(2026, 4, 29)
    async with session_scope(database_url) as session:
        await session.execute(sa.text("INSERT INTO cost_center (cost_center, description, target_type, is_active) VALUES ('CC1','Store 1','value',1)"))
        await session.execute(sa.text("INSERT INTO store_master (id, cost_center, store_code, store_name, sync_group) VALUES (1,'CC1','S1','Store One','TD')"))
        await session.execute(sa.text("INSERT INTO orders (cost_center, order_number, order_date, customer_name, mobile_number, net_amount, gross_amount, source_system) VALUES ('CC1','O300', '2026-04-29 23:50:00', 'Ravi', '9000000000', 500, 500, 'TumbleDry')"))
        await session.execute(sa.text("INSERT INTO sales (cost_center, order_number, payment_date, payment_received, payment_mode, adjustments, is_edited_order) VALUES ('CC1','O300', '2026-04-30 00:05:00', 500, 'UPI', 0, 0)"))
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)
    assert report.same_day_fulfillment_rows == []


def test_same_day_postgres_expression_has_no_strftime() -> None:
    expr = same_day_date_expr(dialect_name="postgresql", dt_expr=sa.column("order_date"), timezone_name="Asia/Kolkata")
    compiled = str(expr.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    assert "strftime" not in compiled.lower()

@pytest.mark.asyncio
async def test_fetch_daily_sales_report_same_day_fulfillment_payment_proof_filtering(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "daily_sales_report_same_day_payment_proof.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr(_data_module, "get_timezone", lambda: tz)
    report_date = date(2026, 4, 29)

    async with session_scope(database_url) as session:
        await session.execute(sa.text("INSERT INTO cost_center (cost_center, description, target_type, is_active) VALUES ('CC1','Store 1','value',1),('CC2','Store 2','value',1)"))
        await session.execute(sa.text("INSERT INTO store_master (id, cost_center, store_code, store_name, sync_group) VALUES (1,'CC1','S1','Store One','TD'),(2,'CC2','S2','Store Two','TD')"))
        await session.execute(sa.text("INSERT INTO orders (cost_center, order_number, order_date, customer_name, mobile_number, net_amount, gross_amount, source_system) VALUES ('CC1','Ord2', :d1, 'Ravi', '9000000000', 500, 500, 'TumbleDry'),('CC1','OX', :d1, 'Mona', '9000000001', 400, 400, 'TumbleDry'),('CC2','ORD3', :d1, 'Neha', '9000000002', 300, 300, 'TumbleDry')"), {"d1": "2026-04-29T09:00:00+05:30"})
        await session.execute(sa.text("INSERT INTO sales (cost_center, order_number, payment_date, payment_received, payment_mode, adjustments, is_edited_order) VALUES ('CC1','Ord2', :p1, 500, 'UPI', 0, 0),('CC1','OX', :p1, 400, 'Cash', 0, 0),('CC2','ORD3', :p1, 300, 'Card', 0, 0)"), {"p1": "2026-04-29T12:00:00+05:30"})
        await session.execute(sa.text("INSERT INTO payment_collections (cost_center, order_number) VALUES ('CC1', ' ORD1, ord2/ORD3 ,,')"))
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)
    assert {row.order_number for row in report.same_day_fulfillment_rows} == {"OX", "ORD3"}


@pytest.mark.asyncio
async def test_fetch_daily_sales_report_report_day_orders_by_cost_center(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "daily_sales_report_day_orders.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)
    monkeypatch.setattr(_data_module, "get_timezone", lambda: ZoneInfo("Asia/Kolkata"))

    report_date = date(2026, 4, 29)
    async with session_scope(database_url) as session:
        await session.execute(
            sa.text(
                """
                INSERT INTO cost_center(cost_center, description, target_type, is_active)
                VALUES
                    ('AA', 'Alpha', 'value', 1),
                    ('BB', 'Bravo', 'value', 1),
                    ('CC', 'Charlie', 'value', 1),
                    ('ZZ', 'Zulu', 'value', 0)
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO orders(cost_center, order_number, order_date, source_system, gross_amount)
                VALUES
                    ('AA', 'A-002', '2026-04-29T10:00:00+05:30', 'TumbleDry', 100),
                    ('AA', 'A-001', '2026-04-29T09:00:00+05:30', 'TumbleDry', 100),
                    ('AA', 'A-ZERO', '2026-04-29T10:30:00+05:30', 'TumbleDry', 0),
                    ('AA', 'A-003', '2026-04-30T09:00:00+05:30', 'TumbleDry', 100),
                    ('BB', 'B-001', '2026-04-29T11:00:00+05:30', 'TumbleDry', 100),
                    ('ZZ', 'Z-001', '2026-04-29T08:00:00+05:30', 'TumbleDry', 100)
                """
            )
        )
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)
    assert [(row.cost_center, row.order_numbers_text) for row in report.report_day_orders_by_cost_center] == [
        ("AA", "A-001, A-002, A-ZERO"),
        ("BB", "B-001"),
        ("CC", "-"),
    ]
    alpha_population = report.report_day_orders_by_cost_center[0]
    assert alpha_population.order_numbers == ["A-001", "A-002", "A-ZERO"]
    assert alpha_population.orders_count == 3
    assert alpha_population.order_amount == Decimal("200")
    assert alpha_population.zero_value_orders_count == 1
    assert report.integrity_findings == []

    # Keep the reporting regression tied to ingestion semantics: a source-supplied
    # zero remains descriptive data, while absent money fields degrade the window.
    ingest_database_url = f"sqlite+aiosqlite:///{tmp_path / 'daily_sales_missing_amount_ingest.db'}"
    metadata = sa.MetaData()
    _stg_td_orders_table(metadata)
    _td_orders_table(metadata)
    engine = sa.create_engine(ingest_database_url.replace("+aiosqlite", ""))
    metadata.create_all(engine)
    engine.dispose()
    ingest_result = await ingest_td_orders_rows(
        rows=[
            {
                "orderNo": "DAILY-MISSING-AMOUNT",
                "orderDate": "2026-04-29T12:00:00+05:30",
                "customerPhone": "9999999999",
            }
        ],
        store_code="AA",
        cost_center="AA",
        run_id="daily-sales-missing-amount",
        run_date=datetime(2026, 4, 29, 13, 0, tzinfo=ZoneInfo("Asia/Kolkata")),
        database_url=ingest_database_url,
        logger=get_logger("daily-sales-missing-amount"),
    )
    assert ingest_result.final_rows == 1
    assert ingest_result.amount_metrics["missing_amount_field_count"] == 2
    assert len(ingest_result.warning_rows) == 1
    assert any("AMOUNT_INGEST_WARNING" in warning for warning in ingest_result.warnings)


@pytest.mark.asyncio
async def test_to_be_recovered_orders_with_sales_and_payment_evidence_are_cleared(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "daily_sales_report_recovery_resolution.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr(_data_module, "get_timezone", lambda: tz)
    report_date = date(2026, 4, 29)

    order_numbers = [
        "NO-EVID",
        "SALES-ONLY",
        "PC-ONLY",
        "BOTH-1",
        "COMMA-2",
        "SLASH-3",
        "MIXED-4",
        "TD-1",
    ]
    async with session_scope(database_url) as session:
        await session.execute(
            sa.text(
                """
                INSERT INTO cost_center (cost_center, description, target_type, is_active)
                VALUES ('CC1', 'Store 1', 'value', 1)
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO store_master (id, cost_center, store_code, store_name, sync_group)
                VALUES (1, 'CC1', 'S1', 'Store One', 'TD')
                """
            )
        )
        for order_number in order_numbers:
            await session.execute(
                sa.text(
                    """
                    INSERT INTO orders (
                        cost_center, order_number, order_date, customer_name,
                        mobile_number, net_amount, gross_amount, source_system,
                        recovery_status, recovery_category
                    ) VALUES (
                        'CC1', :order_number, '2026-04-29T09:00:00+05:30',
                        'Customer', '9000000000', 500, 500, 'TumbleDry',
                        'TO_BE_RECOVERED', 'MISSING_PAYMENT'
                    )
                    """
                ),
                {"order_number": order_number},
            )
        await session.execute(
            sa.text(
                """
                INSERT INTO sales (cost_center, order_number, payment_date, payment_received, payment_mode)
                VALUES
                    ('CC1', 'SALES-ONLY', '2026-04-29T10:00:00+05:30', 500, 'UPI'),
                    ('CC1', 'BOTH-1', '2026-04-29T10:00:00+05:30', 500, 'UPI'),
                    ('CC1', 'COMMA-2', '2026-04-29T10:00:00+05:30', 500, 'UPI'),
                    ('CC1', 'SLASH-3', '2026-04-29T10:00:00+05:30', 500, 'UPI'),
                    ('CC1', 'MIXED-4', '2026-04-29T10:00:00+05:30', 500, 'UPI'),
                    ('CC1', 'TD-1', '2026-04-29T10:00:00+05:30', 500, 'UPI'),
                    ('CC1', 'BOTH-1', '2026-04-29T10:05:00+05:30', 0, 'UPI')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO payment_collections (cost_center, order_number, amount, source_type)
                VALUES
                    ('CC1', 'PC-ONLY', 0, 'google_sheet'),
                    ('CC1', 'BOTH-1', 500, 'google_sheet'),
                    ('CC1', 'NOPE, COMMA-2', 500, 'legacy_sales'),
                    ('CC1', 'NOPE/SLASH-3', 500, 'google_sheet'),
                    ('CC1', 'NOPE, ALSO-NOPE/MIXED-4', 500, 'legacy_sales'),
                    ('CC1', 'TD-10, TD-11', 500, 'google_sheet'),
                    ('CC1', 'BOTH-1', 0, 'google_sheet')
                """
            )
        )
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)

    assert report.auto_cleared_order_numbers == ["BOTH-1"]
    assert report.auto_cleared_order_numbers_text == "BOTH-1"
    assert "SALES-ONLY" not in report.auto_cleared_order_numbers_text
    assert "PC-ONLY" not in report.auto_cleared_order_numbers_text
    assert "TD-1" not in report.auto_cleared_order_numbers_text

    reported_order_numbers = {row.order_number for row in report.to_be_recovered}
    assert reported_order_numbers == {
        "NO-EVID",
        "SALES-ONLY",
        "PC-ONLY",
        "COMMA-2",
        "SLASH-3",
        "MIXED-4",
        "TD-1",
    }

    async with session_scope(database_url) as session:
        status_rows = (
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

    statuses = {row["order_number"]: row for row in status_rows}
    expected_recovery_notes = {
        "BOTH-1": "AUTO_CLEARED_PAYMENT_PROOF payment_collections.payment_ids=2,7, source_types=google_sheet, total_paid=500, order_amount=500, sales_payment_received=500, evidence_amount=500, sales_evidence_difference=0, group_key=CC1:BOTH-1",
    }
    for order_number, recovery_notes in expected_recovery_notes.items():
        assert statuses[order_number]["recovery_status"] == "RECOVERED"
        assert (
            statuses[order_number]["recovery_category"]
            == "PAYMENT_PROOF_AUTO_RECOVERED"
        )
        assert statuses[order_number]["recovery_notes"] == recovery_notes

    for order_number in [
        "NO-EVID",
        "SALES-ONLY",
        "PC-ONLY",
        "COMMA-2",
        "SLASH-3",
        "MIXED-4",
        "TD-1",
    ]:
        assert statuses[order_number]["recovery_status"] == "TO_BE_RECOVERED"
        assert statuses[order_number]["recovery_category"] == "MISSING_PAYMENT"
        assert statuses[order_number]["recovery_notes"] is None


@pytest.mark.asyncio
async def test_single_auto_cleared_to_be_recovered_order_is_reported(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "daily_sales_report_single_recovery_resolution.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr(_data_module, "get_timezone", lambda: tz)
    report_date = date(2026, 4, 29)

    async with session_scope(database_url) as session:
        await session.execute(
            sa.text(
                """
                INSERT INTO cost_center (cost_center, description, target_type, is_active)
                VALUES ('CC1', 'Store 1', 'value', 1)
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO store_master (id, cost_center, store_code, store_name, sync_group)
                VALUES (1, 'CC1', 'S1', 'Store One', 'TD')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO orders (
                    cost_center, order_number, order_date, customer_name,
                    mobile_number, net_amount, gross_amount, source_system,
                    recovery_status, recovery_category
                ) VALUES (
                    'CC1', 'TD123', '2026-04-29T09:00:00+05:30',
                    'Customer', '9000000000', 500, 500, 'TumbleDry',
                    'TO_BE_RECOVERED', 'MISSING_PAYMENT'
                )
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO sales (
                    cost_center, order_number, payment_date, payment_received, payment_mode
                ) VALUES ('CC1', 'TD123', '2026-04-29T10:00:00+05:30', 500, 'UPI')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO payment_collections (cost_center, order_number, amount, source_type)
                VALUES ('CC1', 'TD123', 499, 'legacy_sales')
                """
            )
        )
        await session.commit()

    report = await fetch_daily_sales_report(
        database_url=database_url, report_date=report_date
    )

    assert report.auto_cleared_order_numbers == ["TD123"]
    assert report.auto_cleared_order_numbers_text == "TD123"
    assert [row.order_number for row in report.to_be_recovered] == []

    async with session_scope(database_url) as session:
        recovery_notes = (
            await session.execute(
                sa.text(
                    """
                    SELECT recovery_notes
                    FROM orders
                    WHERE order_number = 'TD123'
                    """
                )
            )
        ).scalar_one()

    assert recovery_notes == (
        "AUTO_CLEARED_PAYMENT_PROOF "
        "payment_collections.payment_ids=1, source_types=legacy_sales, "
        "total_paid=499, order_amount=500, sales_payment_received=500, evidence_amount=499, sales_evidence_difference=1, group_key=CC1:TD123"
    )


@pytest.mark.asyncio
async def test_to_be_compensated_daily_sales_recovery_section_is_not_auto_cleared(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "daily_sales_report_compensated_unchanged.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)

    tz = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr(_data_module, "get_timezone", lambda: tz)
    report_date = date(2026, 4, 29)

    async with session_scope(database_url) as session:
        await session.execute(
            sa.text(
                """
                INSERT INTO cost_center (cost_center, description, target_type, is_active)
                VALUES ('CC1', 'Store 1', 'value', 1)
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO store_master (id, cost_center, store_code, store_name, sync_group)
                VALUES (1, 'CC1', 'S1', 'Store One', 'TD')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO orders (
                    cost_center, order_number, order_date, customer_name,
                    mobile_number, net_amount, gross_amount, source_system,
                    recovery_status, recovery_category
                ) VALUES (
                    'CC1', 'COMP-1', '2026-04-29T09:00:00+05:30',
                    'Customer', '9000000000', 500, 500, 'TumbleDry',
                    'TO_BE_COMPENSATED', 'OVER_COLLECTION'
                )
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO sales (cost_center, order_number, payment_date, payment_received, payment_mode)
                VALUES ('CC1', 'COMP-1', '2026-04-29T10:00:00+05:30', 500, 'UPI')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO payment_collections (cost_center, order_number)
                VALUES ('CC1', 'COMP-1')
                """
            )
        )
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)

    assert [row.order_number for row in report.to_be_compensated] == ["COMP-1"]
    async with session_scope(database_url) as session:
        status = (
            await session.execute(
                sa.text(
                    """
                    SELECT recovery_status, recovery_category, recovery_notes
                    FROM orders
                    WHERE order_number = 'COMP-1'
                    """
                )
            )
        ).mappings().one()

    assert status["recovery_status"] == "TO_BE_COMPENSATED"
    assert status["recovery_category"] == "OVER_COLLECTION"
    assert status["recovery_notes"] is None


def test_build_manual_recovery_sections_excludes_zero_value_orders() -> None:
    rows, compensated_rows, total, compensated_total = _data_module._build_manual_recovery_sections(
        [
            {
                "cost_center": "CC1",
                "order_number": "RECOVERY-ZERO",
                "order_date": date(2026, 4, 29),
                "customer_name": "Zero",
                "mobile_number": "111",
                "order_amount": Decimal("0"),
                "recovery_status": "TO_BE_RECOVERED",
            },
            {
                "cost_center": "CC1",
                "order_number": "RECOVERY-POSITIVE",
                "order_date": date(2026, 4, 29),
                "customer_name": "Paid",
                "mobile_number": "222",
                "order_amount": Decimal("75"),
                "recovery_status": "TO_BE_RECOVERED",
            },
            {
                "cost_center": "CC1",
                "order_number": "COMPENSATION-ZERO",
                "order_date": date(2026, 4, 29),
                "customer_name": "Comp Zero",
                "mobile_number": "333",
                "order_amount": Decimal("0"),
                "recovery_status": "TO_BE_COMPENSATED",
            },
        ],
        tz=ZoneInfo("Asia/Kolkata"),
    )

    assert [row.order_number for row in rows] == ["RECOVERY-POSITIVE"]
    assert compensated_rows == []
    assert total == Decimal("75")
    assert compensated_total == Decimal("0")


@pytest.mark.asyncio
async def test_grouped_auto_clear_requires_sufficient_grouped_payment_proof(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "daily_sales_report_grouped_recovery_resolution.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)

    monkeypatch.setattr(_data_module, "get_timezone", lambda: ZoneInfo("Asia/Kolkata"))
    report_date = date(2026, 4, 29)

    async with session_scope(database_url) as session:
        await session.execute(sa.text("INSERT INTO cost_center (cost_center, description, target_type, is_active) VALUES ('CC1','Store 1','value',1)"))
        await session.execute(sa.text("INSERT INTO store_master (id, cost_center, store_code, store_name, sync_group) VALUES (1,'CC1','S1','Store One','TD')"))
        await session.execute(sa.text("""
            INSERT INTO orders (
                cost_center, order_number, order_date, customer_name, mobile_number,
                net_amount, gross_amount, source_system, recovery_status, recovery_category
            ) VALUES
                ('CC1', 'GROUP-1', '2026-04-29T09:00:00+05:30', 'Customer', '9000000001', 100, 100, 'TumbleDry', 'TO_BE_RECOVERED', 'MISSING_PAYMENT'),
                ('CC1', 'GROUP-2', '2026-04-29T09:05:00+05:30', 'Customer', '9000000002', 200, 200, 'TumbleDry', 'TO_BE_RECOVERED', 'MISSING_PAYMENT')
        """))
        await session.execute(sa.text("""
            INSERT INTO sales (cost_center, order_number, payment_date, payment_received, payment_mode)
            VALUES
                ('CC1', 'GROUP-1', '2026-04-29T10:00:00+05:30', 100, 'UPI'),
                ('CC1', 'GROUP-2', '2026-04-29T10:05:00+05:30', 200, 'UPI')
        """))
        await session.execute(sa.text("""
            INSERT INTO payment_collections (cost_center, order_number, amount, source_type)
            VALUES ('CC1', 'GROUP-1/GROUP-2', 299, 'google_sheet')
        """))
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)

    assert report.auto_cleared_order_numbers == ["GROUP-1", "GROUP-2"]
    assert report.to_be_recovered == []
    async with session_scope(database_url) as session:
        rows = (await session.execute(sa.text("""
            SELECT order_number, recovery_status, recovery_category, recovery_notes
            FROM orders
            ORDER BY order_number
        """))).mappings().all()

    assert {row["order_number"]: row["recovery_status"] for row in rows} == {
        "GROUP-1": "RECOVERED",
        "GROUP-2": "RECOVERED",
    }
    assert {row["order_number"]: row["recovery_category"] for row in rows} == {
        "GROUP-1": "PAYMENT_PROOF_AUTO_RECOVERED",
        "GROUP-2": "PAYMENT_PROOF_AUTO_RECOVERED",
    }
    for row in rows:
        assert "payment_collections.payment_ids=1" in row["recovery_notes"]
        assert "source_types=google_sheet" in row["recovery_notes"]
        assert "total_paid=299" in row["recovery_notes"]
        assert "group_key=CC1:GROUP-1/GROUP-2" in row["recovery_notes"]


@pytest.mark.asyncio
async def test_auto_clear_keeps_to_be_recovered_when_sales_row_is_missing(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "daily_sales_report_missing_sales_recovery_resolution.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)

    monkeypatch.setattr(_data_module, "get_timezone", lambda: ZoneInfo("Asia/Kolkata"))
    report_date = date(2026, 4, 29)

    async with session_scope(database_url) as session:
        await session.execute(sa.text("INSERT INTO cost_center (cost_center, description, target_type, is_active) VALUES ('CC1','Store 1','value',1)"))
        await session.execute(sa.text("INSERT INTO store_master (id, cost_center, store_code, store_name, sync_group) VALUES (1,'CC1','S1','Store One','TD')"))
        await session.execute(sa.text("""
            INSERT INTO orders (
                cost_center, order_number, order_date, customer_name, mobile_number,
                net_amount, gross_amount, source_system, recovery_status, recovery_category
            ) VALUES (
                'CC1', 'NO-SALES', '2026-04-29T09:00:00+05:30', 'Customer', '9000000001',
                500, 500, 'TumbleDry', 'TO_BE_RECOVERED', 'MISSING_PAYMENT'
            )
        """))
        await session.execute(sa.text("""
            INSERT INTO payment_collections (cost_center, order_number, amount, source_type)
            VALUES ('CC1', 'NO-SALES', 500, 'legacy_sales')
        """))
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)

    assert report.auto_cleared_order_numbers == []
    assert [row.order_number for row in report.to_be_recovered] == ["NO-SALES"]
    async with session_scope(database_url) as session:
        status = (await session.execute(sa.text("""
            SELECT recovery_status, recovery_category, recovery_notes
            FROM orders
            WHERE order_number = 'NO-SALES'
        """))).mappings().one()

    assert status["recovery_status"] == "TO_BE_RECOVERED"
    assert status["recovery_category"] == "MISSING_PAYMENT"
    assert status["recovery_notes"] is None


@pytest.mark.asyncio
async def test_auto_clear_keeps_to_be_recovered_when_sales_and_proof_mismatch(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "daily_sales_report_mismatch_recovery_resolution.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)

    monkeypatch.setattr(_data_module, "get_timezone", lambda: ZoneInfo("Asia/Kolkata"))
    report_date = date(2026, 4, 29)

    async with session_scope(database_url) as session:
        await session.execute(sa.text("INSERT INTO cost_center (cost_center, description, target_type, is_active) VALUES ('CC1','Store 1','value',1)"))
        await session.execute(sa.text("INSERT INTO store_master (id, cost_center, store_code, store_name, sync_group) VALUES (1,'CC1','S1','Store One','TD')"))
        await session.execute(sa.text("""
            INSERT INTO orders (
                cost_center, order_number, order_date, customer_name, mobile_number,
                net_amount, gross_amount, source_system, recovery_status, recovery_category
            ) VALUES (
                'CC1', 'MISMATCH', '2026-04-29T09:00:00+05:30', 'Customer', '9000000001',
                500, 500, 'TumbleDry', 'TO_BE_RECOVERED', 'MISSING_PAYMENT'
            )
        """))
        await session.execute(sa.text("""
            INSERT INTO sales (cost_center, order_number, payment_date, payment_received, payment_mode)
            VALUES ('CC1', 'MISMATCH', '2026-04-29T10:00:00+05:30', 450, 'UPI')
        """))
        await session.execute(sa.text("""
            INSERT INTO payment_collections (cost_center, order_number, amount, source_type)
            VALUES ('CC1', 'MISMATCH', 500, 'legacy_sales')
        """))
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)

    assert report.auto_cleared_order_numbers == []
    assert [row.order_number for row in report.to_be_recovered] == ["MISMATCH"]
    async with session_scope(database_url) as session:
        status = (await session.execute(sa.text("""
            SELECT recovery_status, recovery_category, recovery_notes
            FROM orders
            WHERE order_number = 'MISMATCH'
        """))).mappings().one()

    assert status["recovery_status"] == "TO_BE_RECOVERED"
    assert status["recovery_category"] == "MISSING_PAYMENT"
    assert status["recovery_notes"] is None


@pytest.mark.asyncio
async def test_auto_clear_keeps_to_be_recovered_when_payment_proof_is_insufficient(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "daily_sales_report_insufficient_recovery_resolution.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)

    monkeypatch.setattr(_data_module, "get_timezone", lambda: ZoneInfo("Asia/Kolkata"))
    report_date = date(2026, 4, 29)

    async with session_scope(database_url) as session:
        await session.execute(sa.text("INSERT INTO cost_center (cost_center, description, target_type, is_active) VALUES ('CC1','Store 1','value',1)"))
        await session.execute(sa.text("INSERT INTO store_master (id, cost_center, store_code, store_name, sync_group) VALUES (1,'CC1','S1','Store One','TD')"))
        await session.execute(sa.text("""
            INSERT INTO orders (
                cost_center, order_number, order_date, customer_name, mobile_number,
                net_amount, gross_amount, source_system, recovery_status, recovery_category
            ) VALUES (
                'CC1', 'SHORT-PROOF', '2026-04-29T09:00:00+05:30', 'Customer', '9000000001',
                500, 500, 'TumbleDry', 'TO_BE_RECOVERED', 'MISSING_PAYMENT'
            )
        """))
        await session.execute(sa.text("""
            INSERT INTO payment_collections (cost_center, order_number, amount, source_type)
            VALUES ('CC1', 'SHORT-PROOF', 498, 'legacy_sales')
        """))
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)

    assert report.auto_cleared_order_numbers == []
    assert [row.order_number for row in report.to_be_recovered] == ["SHORT-PROOF"]
    async with session_scope(database_url) as session:
        status = (await session.execute(sa.text("""
            SELECT recovery_status, recovery_category, recovery_notes
            FROM orders
            WHERE order_number = 'SHORT-PROOF'
        """))).mappings().one()

    assert status["recovery_status"] == "TO_BE_RECOVERED"
    assert status["recovery_category"] == "MISSING_PAYMENT"
    assert status["recovery_notes"] is None


@pytest.mark.asyncio
async def test_auto_clear_keeps_to_be_recovered_when_payment_proof_is_missing(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "daily_sales_report_missing_recovery_resolution.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)

    monkeypatch.setattr(_data_module, "get_timezone", lambda: ZoneInfo("Asia/Kolkata"))
    report_date = date(2026, 4, 29)

    async with session_scope(database_url) as session:
        await session.execute(sa.text("INSERT INTO cost_center (cost_center, description, target_type, is_active) VALUES ('CC1','Store 1','value',1)"))
        await session.execute(sa.text("INSERT INTO store_master (id, cost_center, store_code, store_name, sync_group) VALUES (1,'CC1','S1','Store One','TD')"))
        await session.execute(sa.text("""
            INSERT INTO orders (
                cost_center, order_number, order_date, customer_name, mobile_number,
                net_amount, gross_amount, source_system, recovery_status, recovery_category
            ) VALUES (
                'CC1', 'NO-PROOF', '2026-04-29T09:00:00+05:30', 'Customer', '9000000001',
                500, 500, 'TumbleDry', 'TO_BE_RECOVERED', 'MISSING_PAYMENT'
            )
        """))
        await session.execute(sa.text("""
            INSERT INTO sales (cost_center, order_number, payment_date, payment_received, payment_mode)
            VALUES ('CC1', 'NO-PROOF', '2026-04-29T10:00:00+05:30', 500, 'UPI')
        """))
        await session.commit()

    report = await fetch_daily_sales_report(database_url=database_url, report_date=report_date)

    assert report.auto_cleared_order_numbers == []
    assert [row.order_number for row in report.to_be_recovered] == ["NO-PROOF"]
    async with session_scope(database_url) as session:
        status = (await session.execute(sa.text("""
            SELECT recovery_status, recovery_category, recovery_notes
            FROM orders
            WHERE order_number = 'NO-PROOF'
        """))).mappings().one()

    assert status["recovery_status"] == "TO_BE_RECOVERED"
    assert status["recovery_category"] == "MISSING_PAYMENT"
    assert status["recovery_notes"] is None



def _integrity_daily_row(*, count: int, amount: str):
    row = _data_module._totals_row([])
    row.cost_center = "CC1"
    row.cost_center_name = "Store One"
    row.orders_count_ftd = count
    row.sales_ftd = Decimal(amount)
    return row


def _integrity_findings(*, row_count: int, row_amount: str, order_numbers: list[str], population_amount: str, zero_count: int = 0):
    row = _integrity_daily_row(count=row_count, amount=row_amount)
    totals = _data_module._totals_row([row])
    population = _data_module.ReportDayOrdersByCostCenterRow(
        cost_center="CC1",
        order_numbers_text=", ".join(order_numbers) or "-",
        order_numbers=order_numbers,
        orders_count=len(order_numbers),
        order_amount=Decimal(population_amount),
        zero_value_orders_count=zero_count,
    )
    return _data_module._build_integrity_findings(
        rows=[row], totals=totals, report_day_orders_by_cost_center=[population]
    )


def test_daily_sales_integrity_matching_summary_and_order_number_list() -> None:
    findings = _integrity_findings(
        row_count=2, row_amount="125", order_numbers=["ORD-1", "ORD-2"], population_amount="125"
    )
    assert findings == []


def test_daily_sales_integrity_explicit_zero_value_order_remains_listed() -> None:
    findings = _integrity_findings(
        row_count=1, row_amount="0", order_numbers=["ZERO-1"], population_amount="0", zero_count=1
    )
    assert [finding.code for finding in findings] == ["ftd_orders_with_zero_total_amount"]


def test_daily_sales_integrity_multiple_zero_value_orders_generate_warning() -> None:
    findings = _integrity_findings(
        row_count=2, row_amount="0", order_numbers=["ZERO-1", "ZERO-2"], population_amount="0", zero_count=2
    )
    assert [finding.code for finding in findings] == [
        "ftd_orders_with_zero_total_amount",
        "multiple_zero_value_ftd_orders",
    ]
    assert all(finding.severity == "warning" for finding in findings)


def test_daily_sales_integrity_count_mismatch_is_hard_error() -> None:
    findings = _integrity_findings(
        row_count=1, row_amount="125", order_numbers=["ORD-1", "ORD-2"], population_amount="125"
    )
    assert [(finding.code, finding.severity) for finding in findings] == [
        ("store_ftd_count_mismatch", "error")
    ]


def test_daily_sales_integrity_amount_mismatch_is_hard_error() -> None:
    findings = _integrity_findings(
        row_count=2, row_amount="124", order_numbers=["ORD-1", "ORD-2"], population_amount="125"
    )
    assert [(finding.code, finding.severity) for finding in findings] == [
        ("store_ftd_amount_mismatch", "error")
    ]


def test_daily_sales_integrity_duplicate_order_number_is_hard_error() -> None:
    findings = _integrity_findings(
        row_count=2, row_amount="125", order_numbers=["ORD-1", "ORD-1"], population_amount="125"
    )
    assert [(finding.code, finding.severity) for finding in findings] == [
        ("duplicate_ftd_order_number", "error")
    ]


def test_daily_sales_integrity_aggregate_total_mismatch_is_hard_error() -> None:
    row = _integrity_daily_row(count=1, amount="125")
    totals = _data_module._totals_row([row])
    totals.orders_count_ftd = 2
    totals.sales_ftd = Decimal("126")
    population = _data_module.ReportDayOrdersByCostCenterRow(
        cost_center="CC1", order_numbers_text="ORD-1", order_numbers=["ORD-1"], orders_count=1, order_amount=Decimal("125")
    )
    findings = _data_module._build_integrity_findings(rows=[row], totals=totals, report_day_orders_by_cost_center=[population])
    assert [(finding.code, finding.severity) for finding in findings] == [
        ("total_ftd_count_mismatch", "error"),
        ("total_ftd_amount_mismatch", "error"),
    ]
