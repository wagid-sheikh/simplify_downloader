from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest
import sqlalchemy as sa

from app.common.db import session_scope
from app.crm_downloader.td_orders_sync.sales_ingest import _sales_table
from app.crm_downloader.uc_orders_sync.archive_ingest import (
    TABLE_ARCHIVE_BASE,
    TABLE_ARCHIVE_ORDER_DETAILS,
    TABLE_ARCHIVE_PAYMENT_DETAILS,
)
from app.crm_downloader.td_orders_sync.garment_ingest import order_line_items_table
from app.crm_downloader.uc_orders_sync.gst_publish import (
    REASON_GST_LIFECYCLE_PARENT_INGEST_FAILURE,
    REASON_GST_LIFECYCLE_PARENT_ORDER_CONTEXT_MISSING,
    REASON_GST_LIFECYCLE_PARENT_SOURCE_ABSENT,
    REASON_GST_LIFECYCLE_PARENT_COVERAGE_LOW,
    REASON_GST_LIFECYCLE_PARENT_COVERAGE_NEAR_ZERO,
    REASON_GST_LIFECYCLE_UNPARSEABLE_PAYMENT_DATE,
    publish_uc_gst_order_details_to_line_items,
    publish_uc_gst_order_details_to_orders,
    publish_uc_gst_payments_to_sales,
    publish_uc_gst_stage2_stage3,
)
from app.crm_downloader.uc_orders_sync.ingest import _orders_table
from app.crm_downloader.uc_orders_sync.ingest import _stg_uc_orders_table


async def _create_tables(db_url: str) -> None:
    metadata = sa.MetaData()
    _orders_table(metadata)
    _stg_uc_orders_table(metadata)
    _sales_table(metadata)
    order_line_items_table(metadata)
    sa.Table(
        TABLE_ARCHIVE_BASE,
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("run_id", sa.Text),
        sa.Column("run_date", sa.DateTime(timezone=True)),
        sa.Column("cost_center", sa.String(8)),
        sa.Column("store_code", sa.String(8)),
        sa.Column("ingest_remarks", sa.Text),
        sa.Column("order_code", sa.String(24)),
        sa.Column("customer_name", sa.String(128)),
        sa.Column("customer_phone", sa.String(24)),
        sa.Column("address", sa.Text),
        sa.Column("customer_source", sa.String(64)),
    )
    sa.Table(
        TABLE_ARCHIVE_ORDER_DETAILS,
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("run_id", sa.Text),
        sa.Column("run_date", sa.DateTime(timezone=True)),
        sa.Column("cost_center", sa.String(8)),
        sa.Column("store_code", sa.String(8)),
        sa.Column("ingest_remarks", sa.Text),
        sa.Column("order_code", sa.String(24)),
        sa.Column("order_datetime_raw", sa.Text),
        sa.Column("service", sa.Text),
        sa.Column("item_name", sa.Text),
        sa.Column("rate", sa.Numeric(12, 2)),
        sa.Column("quantity", sa.Numeric(12, 2)),
        sa.Column("weight", sa.Numeric(12, 3)),
        sa.Column("amount", sa.Numeric(12, 2)),
        sa.Column("line_hash", sa.String(64)),
    )
    sa.Table(
        TABLE_ARCHIVE_PAYMENT_DETAILS,
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("run_id", sa.Text),
        sa.Column("run_date", sa.DateTime(timezone=True)),
        sa.Column("store_code", sa.String(8)),
        sa.Column("order_code", sa.String(24)),
        sa.Column("payment_mode", sa.String(32)),
        sa.Column("amount", sa.Numeric(12, 2)),
        sa.Column("payment_date_raw", sa.Text),
        sa.Column("transaction_id", sa.String(128)),
        sa.Column("ingest_remarks", sa.Text),
    )
    async with session_scope(db_url) as session:
        bind = session.bind
        assert isinstance(bind, sa.ext.asyncio.AsyncEngine)
        async with bind.begin() as conn:
            await conn.run_sync(metadata.create_all)


@pytest.mark.asyncio
async def test_orders_enrichment_recomputes_and_preserves_protected_columns(tmp_path: Path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'publish_orders.sqlite'}"
    await _create_tables(db_url)

    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                """
                INSERT INTO orders (
                    cost_center, store_code, order_number, order_date, customer_name, mobile_number,
                    gross_amount, discount_amount, tax_amount, net_amount, payment_status, order_status,
                    run_id, run_date, created_at, pieces, weight, service_type
                ) VALUES (
                    'CC01', 'UC567', 'ORD-1', '2025-01-01T00:00:00+00:00', 'Alice', '9999999999',
                    500, 10, 5, 495, 'Paid', 'Delivered', 'orig-run', '2025-01-01T00:00:00+00:00', '2025-01-01T00:00:00+00:00',
                    1, 0.5, 'Legacy'
                )
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO stg_uc_archive_order_details (run_id, store_code, order_code, quantity, weight, service)
                VALUES
                    ('run-1', 'UC567', 'ORD-1', 2, 1.25, 'Dryclean'),
                    ('run-1', 'UC567', 'ORD-1', 3, 0.75, 'Wash'),
                    ('run-1', 'UC567', 'ORD-1', NULL, NULL, 'Dryclean')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO stg_uc_archive_orders_base (id, run_id, run_date, cost_center, store_code, order_code, customer_source, address)
                VALUES
                    (100, 'run-0', '2025-01-01T00:00:00+00:00', 'CC01', 'UC567', 'ORD-1', ' ', ' '),
                    (101, 'run-1', '2025-01-03T00:00:00+00:00', 'CC01', 'UC567', 'ORD-1', 'Web', 'Address 1')
                """
            )
        )
        await session.commit()

    metrics = await publish_uc_gst_order_details_to_orders(database_url=db_url, run_id='run-1', store_code='UC567')
    assert metrics.updated == 1

    async with session_scope(db_url) as session:
        row = (
            await session.execute(
                sa.text(
                    "SELECT pieces, weight, service_type, customer_name, net_amount, payment_status, run_id, customer_source, customer_address FROM orders WHERE store_code='UC567' AND order_number='ORD-1'"
                )
            )
        ).one()
    assert Decimal(str(row.pieces)) == Decimal("5")
    assert Decimal(str(row.weight)) == Decimal("2.0")
    assert row.service_type == "Dryclean, Wash"
    assert row.customer_name == "Alice"
    assert Decimal(str(row.net_amount)) == Decimal("495")
    assert row.payment_status == "Paid"
    assert row.run_id == "orig-run"
    assert row.customer_source == "Web"
    assert row.customer_address == "Address 1"


@pytest.mark.asyncio
async def test_payment_upsert_idempotency_and_null_transaction_collision(tmp_path: Path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'publish_sales.sqlite'}"
    await _create_tables(db_url)

    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                """
                INSERT INTO orders (cost_center, store_code, order_number, order_date, customer_name, mobile_number, created_at)
                VALUES ('CC01', 'UC567', 'ORD-1', '2025-01-01T00:00:00+00:00', 'Alice', '9999999999', '2025-01-01T00:00:00+00:00')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO stg_uc_archive_payment_details (run_id, run_date, store_code, order_code, payment_mode, amount, payment_date_raw, transaction_id, ingest_remarks)
                VALUES
                    ('run-1', '2025-01-02T00:00:00+00:00', 'UC567', 'ORD-1', 'CASH', 100, '02 Jan 2025, 10:30 AM', '' , 'stg-remark'),
                    ('run-1', '2025-01-02T00:00:00+00:00', 'UC567', 'ORD-1', 'CASH', 100, '02 Jan 2025, 10:30 AM', NULL, 'stg-remark')
                """
            )
        )
        await session.commit()

    first = await publish_uc_gst_payments_to_sales(database_url=db_url, run_id='run-1', store_code='UC567')
    second = await publish_uc_gst_payments_to_sales(database_url=db_url, run_id='run-1', store_code='UC567')

    assert first.inserted == 1
    assert first.updated == 0
    assert second.inserted == 0
    assert second.updated == 1

    async with session_scope(db_url) as session:
        count = (await session.execute(sa.text("SELECT COUNT(*) FROM sales"))).scalar_one()
        row = (await session.execute(sa.text("SELECT transaction_id, ingest_remarks, order_type FROM sales"))).one()
    assert count == 1
    assert row.transaction_id is None
    assert "stg-remark" in (row.ingest_remarks or "")
    assert row.order_type is None


@pytest.mark.asyncio
async def test_payment_publish_customer_address_prefers_order_then_archive_base(tmp_path: Path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'publish_sales_customer_address.sqlite'}"
    await _create_tables(db_url)

    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                """
                INSERT INTO orders (
                    cost_center, store_code, order_number, order_date, customer_name, mobile_number, customer_address, created_at
                ) VALUES
                    ('CC01', 'UC567', 'ORD-ADDR-1', '2025-01-01T00:00:00+00:00', 'Alice', '9999999999', 'Order Address 1', '2025-01-01T00:00:00+00:00'),
                    ('CC01', 'UC567', 'ORD-ADDR-2', '2025-01-01T00:00:00+00:00', 'Bob', '8888888888', '   ', '2025-01-01T00:00:00+00:00')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO stg_uc_archive_orders_base (
                    run_id, run_date, cost_center, store_code, order_code, customer_name, customer_phone, address
                ) VALUES
                    ('run-addr', '2025-01-03T00:00:00+00:00', 'CC01', 'UC567', 'ORD-ADDR-1', 'Archive Alice', '7000000001', 'Archive Address 1'),
                    ('run-addr', '2025-01-03T00:00:00+00:00', 'CC01', 'UC567', 'ORD-ADDR-2', 'Archive Bob', '7000000002', 'Archive Address 2')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO stg_uc_archive_payment_details (
                    run_id, run_date, store_code, order_code, payment_mode, amount, payment_date_raw, transaction_id
                ) VALUES
                    ('run-addr', '2025-01-03T00:00:00+00:00', 'UC567', 'ORD-ADDR-1', 'UPI', 100, '03 Jan 2025, 11:00 AM', 'TADDR1'),
                    ('run-addr', '2025-01-03T00:00:00+00:00', 'UC567', 'ORD-ADDR-2', 'UPI', 200, '03 Jan 2025, 11:10 AM', 'TADDR2')
                """
            )
        )
        await session.commit()

    metrics = await publish_uc_gst_payments_to_sales(database_url=db_url, run_id='run-addr', store_code='UC567')
    assert metrics.inserted == 2

    async with session_scope(db_url) as session:
        rows = (
            await session.execute(
                sa.text(
                    "SELECT order_number, customer_address, order_type FROM sales WHERE store_code='UC567' ORDER BY order_number"
                )
            )
        ).all()

    assert rows[0].order_number == 'ORD-ADDR-1'
    assert rows[0].customer_address == 'Order Address 1'
    assert rows[0].order_type is None
    assert rows[1].order_number == 'ORD-ADDR-2'
    assert rows[1].customer_address == 'Archive Address 2'
    assert rows[1].order_type is None


@pytest.mark.asyncio
async def test_payment_skip_reasons_and_metrics_and_orchestrator(tmp_path: Path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'publish_reasons.sqlite'}"
    await _create_tables(db_url)

    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                """
                INSERT INTO stg_uc_archive_payment_details (run_id, run_date, store_code, order_code, payment_mode, amount, payment_date_raw, transaction_id)
                VALUES
                    ('run-2', '2025-01-03T00:00:00+00:00', 'UC999', 'ORD-X', 'UPI', 50, '03 Jan 2025, 11:00 AM', 'T1'),
                    ('run-2', '2025-01-03T00:00:00+00:00', 'UC567', 'ORD-1', 'UPI', 60, 'not-a-date', 'T2'),
                    ('run-2', '2025-01-03T00:00:00+00:00', 'UC567', '', 'UPI', 60, '03 Jan 2025, 11:00 AM', 'T3')
                """
            )
        )
        await session.commit()

    sales_metrics = await publish_uc_gst_payments_to_sales(database_url=db_url, run_id='run-2', store_code='UC567')
    assert sales_metrics.skipped == 2
    assert sales_metrics.warnings == 1
    assert sales_metrics.reason_codes[REASON_GST_LIFECYCLE_PARENT_COVERAGE_NEAR_ZERO] == 1
    assert sales_metrics.publish_parent_match_rate == 0.0
    assert sales_metrics.missing_parent_count == 1

    stage = await publish_uc_gst_stage2_stage3(database_url=db_url, run_id='run-2', store_code='UC567')
    assert isinstance(stage.orders.updated, int)
    assert stage.sales.skipped >= 2


@pytest.mark.asyncio
async def test_payment_publish_uses_historical_orders_when_current_run_stg_missing(tmp_path: Path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'publish_parent_historical.sqlite'}"
    await _create_tables(db_url)

    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                """
                INSERT INTO orders (
                    cost_center, store_code, order_number, order_date, customer_name, mobile_number, run_id, created_at
                ) VALUES (
                    'CC77', 'UC777', 'ORD-HIST', '2025-01-01T00:00:00+00:00', 'Historical User', '7000000000',
                    'older-run', '2025-01-02T00:00:00+00:00'
                )
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO stg_uc_archive_payment_details (
                    run_id, run_date, store_code, order_code, payment_mode, amount, payment_date_raw, transaction_id
                ) VALUES (
                    'run-historical', '2025-01-04T00:00:00+00:00', 'UC777', 'ORD-HIST', 'UPI', 120, '04 Jan 2025, 08:00 AM', 'THIST'
                )
                """
            )
        )
        await session.commit()

    metrics = await publish_uc_gst_payments_to_sales(database_url=db_url, run_id='run-historical', store_code='UC777')

    assert metrics.inserted == 1
    assert metrics.skipped == 0
    assert metrics.reason_codes.get(REASON_GST_LIFECYCLE_PARENT_ORDER_CONTEXT_MISSING, 0) == 0

    async with session_scope(db_url) as session:
        row = (
            await session.execute(
                sa.text(
                    "SELECT cost_center, store_code, order_number, payment_received FROM sales WHERE transaction_id='THIST'"
                )
            )
        ).one()

    assert row.cost_center == 'CC77'
    assert row.store_code == 'UC777'
    assert row.order_number == 'ORD-HIST'
    assert Decimal(str(row.payment_received)) == Decimal('120')


@pytest.mark.asyncio
async def test_payment_publish_reason_codes_distinguish_ingest_failure_vs_absent_source(tmp_path: Path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'publish_parent_reasons.sqlite'}"
    await _create_tables(db_url)

    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                """
                INSERT INTO stg_uc_orders (run_id, run_date, cost_center, store_code, order_number, invoice_date)
                VALUES ('run-reasons', '2025-01-03T00:00:00+00:00', 'CC88', 'UC888', 'ORD-INGEST', '2025-01-02T00:00:00+00:00')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO stg_uc_archive_orders_base (
                    run_id, run_date, cost_center, store_code, order_code, ingest_remarks
                ) VALUES (
                    'run-reasons', '2025-01-03T00:00:00+00:00', '', 'UC888', 'ORD-INGEST', 'gst header mismatch'
                )
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO stg_uc_archive_payment_details (
                    run_id, run_date, store_code, order_code, payment_mode, amount, payment_date_raw, transaction_id
                ) VALUES
                    ('run-reasons', '2025-01-03T00:00:00+00:00', 'UC888', 'ORD-INGEST', 'UPI', 90, '03 Jan 2025, 10:00 AM', 'T-INGEST'),
                    ('run-reasons', '2025-01-03T00:00:00+00:00', 'UC888', 'ORD-ABSENT', 'UPI', 95, '03 Jan 2025, 10:05 AM', 'T-ABSENT')
                """
            )
        )
        await session.commit()

    metrics = await publish_uc_gst_payments_to_sales(database_url=db_url, run_id='run-reasons', store_code='UC888')

    assert metrics.inserted == 0
    assert metrics.reason_codes[REASON_GST_LIFECYCLE_PARENT_INGEST_FAILURE] == 1
    assert metrics.reason_codes[REASON_GST_LIFECYCLE_PARENT_SOURCE_ABSENT] == 1


@pytest.mark.asyncio
async def test_payment_publish_parent_coverage_full(tmp_path: Path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'publish_parent_full.sqlite'}"
    await _create_tables(db_url)

    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                """
                INSERT INTO orders (cost_center, store_code, order_number, order_date, customer_name, mobile_number, created_at)
                VALUES
                    ('CC01', 'UC567', 'ORD-1', '2025-01-01T00:00:00+00:00', 'Alice', '9999999999', '2025-01-01T00:00:00+00:00'),
                    ('CC01', 'UC567', 'ORD-2', '2025-01-01T00:00:00+00:00', 'Bob', '8888888888', '2025-01-01T00:00:00+00:00')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO stg_uc_archive_payment_details (run_id, run_date, store_code, order_code, payment_mode, amount, payment_date_raw, transaction_id)
                VALUES
                    ('run-full', '2025-01-03T00:00:00+00:00', 'UC567', 'ORD-1', 'UPI', 50, '03 Jan 2025, 11:00 AM', 'T1'),
                    ('run-full', '2025-01-03T00:00:00+00:00', 'UC567', 'ORD-2', 'CASH', 60, '03 Jan 2025, 11:10 AM', 'T2')
                """
            )
        )
        await session.commit()

    metrics = await publish_uc_gst_payments_to_sales(database_url=db_url, run_id='run-full', store_code='UC567')
    assert metrics.publish_parent_match_rate == 1.0
    assert metrics.missing_parent_count == 0
    assert metrics.preflight_warning is None
    assert metrics.inserted == 2


@pytest.mark.asyncio
async def test_payment_publish_parent_coverage_near_zero_preflight_skip(tmp_path: Path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'publish_parent_zero.sqlite'}"
    await _create_tables(db_url)

    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                """
                INSERT INTO stg_uc_archive_payment_details (run_id, run_date, store_code, order_code, payment_mode, amount, payment_date_raw, transaction_id)
                VALUES
                    ('run-zero', '2025-01-03T00:00:00+00:00', 'UC999', 'ORD-X', 'UPI', 50, '03 Jan 2025, 11:00 AM', 'T1')
                """
            )
        )
        await session.commit()

    metrics = await publish_uc_gst_payments_to_sales(database_url=db_url, run_id='run-zero', store_code='UC999')
    assert metrics.publish_parent_match_rate == 0.0
    assert metrics.missing_parent_count == 1
    assert metrics.inserted == 0
    assert metrics.skipped == 1
    assert metrics.warnings == 1
    assert metrics.reason_codes[REASON_GST_LIFECYCLE_PARENT_COVERAGE_NEAR_ZERO] == 1
    assert metrics.preflight_warning is not None
    assert "UC999:ORD-X" in metrics.preflight_warning


@pytest.mark.asyncio
async def test_payment_publish_parent_coverage_mixed(tmp_path: Path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'publish_parent_mixed.sqlite'}"
    await _create_tables(db_url)

    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                """
                INSERT INTO orders (cost_center, store_code, order_number, order_date, customer_name, mobile_number, created_at)
                VALUES ('CC01', 'UC567', 'ORD-1', '2025-01-01T00:00:00+00:00', 'Alice', '9999999999', '2025-01-01T00:00:00+00:00')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO stg_uc_archive_payment_details (run_id, run_date, store_code, order_code, payment_mode, amount, payment_date_raw, transaction_id)
                VALUES
                    ('run-mixed', '2025-01-03T00:00:00+00:00', 'UC567', 'ORD-1', 'UPI', 50, '03 Jan 2025, 11:00 AM', 'T1'),
                    ('run-mixed', '2025-01-03T00:00:00+00:00', 'UC567', 'ORD-MISSING', 'UPI', 60, '03 Jan 2025, 11:10 AM', 'T2')
                """
            )
        )
        await session.commit()

    metrics = await publish_uc_gst_payments_to_sales(database_url=db_url, run_id='run-mixed', store_code='UC567')
    assert metrics.publish_parent_match_rate == 0.5
    assert metrics.missing_parent_count == 1
    assert metrics.preflight_warning is not None
    assert metrics.inserted == 1
    assert metrics.reason_codes[REASON_GST_LIFECYCLE_PARENT_COVERAGE_LOW] == 1
    assert metrics.reason_codes[REASON_GST_LIFECYCLE_PARENT_SOURCE_ABSENT] == 1


@pytest.mark.asyncio
async def test_payment_publish_parent_coverage_low_logs_diagnostics(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'publish_parent_low.sqlite'}"
    await _create_tables(db_url)

    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                """
                INSERT INTO orders (cost_center, store_code, order_number, order_date, customer_name, mobile_number, created_at)
                VALUES ('CC01', 'UC567', 'ORD-1', '2025-01-01T00:00:00+00:00', 'Alice', '9999999999', '2025-01-01T00:00:00+00:00')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO stg_uc_archive_payment_details (run_id, run_date, store_code, order_code, payment_mode, amount, payment_date_raw, transaction_id)
                VALUES
                    ('run-low', '2025-01-03T00:00:00+00:00', 'UC567', 'ORD-1', 'UPI', 50, '03 Jan 2025, 11:00 AM', 'T1'),
                    ('run-low', '2025-01-03T00:00:00+00:00', 'UC567', 'ORD-X', 'UPI', 60, '03 Jan 2025, 11:10 AM', 'T2'),
                    ('run-low', '2025-01-03T00:00:00+00:00', 'UC567', 'ORD-Y', 'UPI', 70, '03 Jan 2025, 11:20 AM', 'T3')
                """
            )
        )
        await session.commit()

    caplog.set_level("WARNING")
    metrics = await publish_uc_gst_payments_to_sales(database_url=db_url, run_id='run-low', store_code='UC567')

    assert metrics.inserted == 1
    assert metrics.reason_codes[REASON_GST_LIFECYCLE_PARENT_COVERAGE_LOW] == 1
    assert metrics.reason_codes[REASON_GST_LIFECYCLE_PARENT_SOURCE_ABSENT] == 2
    assert metrics.preflight_warning is not None
    assert "ORD-X" in metrics.preflight_warning
    assert metrics.preflight_diagnostics is not None
    assert metrics.preflight_diagnostics["matched_parent_keys"] == 1

    warning_records = [r for r in caplog.records if "gst_publish_parent_preflight_low" in r.message]
    assert warning_records


@pytest.mark.asyncio
async def test_payment_publish_uses_archive_base_fallback_when_stg_parent_exists(tmp_path: Path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'publish_parent_fallback.sqlite'}"
    await _create_tables(db_url)

    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                """
                INSERT INTO stg_uc_orders (run_id, run_date, cost_center, store_code, order_number, invoice_date)
                VALUES ('run-fallback', '2025-01-03T00:00:00+00:00', 'CC01', 'UC567', 'ORD-FB', '2025-01-02T00:00:00+00:00')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO stg_uc_archive_orders_base (run_id, run_date, cost_center, store_code, order_code, customer_name, customer_phone, ingest_remarks)
                VALUES ('run-fallback', '2025-01-03T00:00:00+00:00', 'CC01', 'UC567', 'ORD-FB', 'Fallback User', '7777777777', 'archive-base-remark')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO stg_uc_archive_payment_details (run_id, run_date, store_code, order_code, payment_mode, amount, payment_date_raw, transaction_id, ingest_remarks)
                VALUES ('run-fallback', '2025-01-03T00:00:00+00:00', 'UC567', 'ORD-FB', 'UPI', 80, '03 Jan 2025, 11:30 AM', 'TFB', 'payment-remark')
                """
            )
        )
        await session.commit()

    metrics = await publish_uc_gst_payments_to_sales(database_url=db_url, run_id='run-fallback', store_code='UC567')
    assert metrics.inserted == 1
    assert metrics.missing_parent_count == 0

    async with session_scope(db_url) as session:
        row = (
            await session.execute(
                sa.text(
                    "SELECT cost_center, customer_name, mobile_number, ingest_remarks FROM sales WHERE store_code='UC567' AND order_number='ORD-FB'"
                )
            )
        ).one()
    assert row.cost_center == "CC01"
    assert row.customer_name == "Fallback User"
    assert row.mobile_number == "7777777777"
    assert "archive-base-remark" in (row.ingest_remarks or "")
    assert "payment-remark" in (row.ingest_remarks or "")


@pytest.mark.asyncio
async def test_gst_publish_normalizes_join_keys_and_improves_parent_coverage(tmp_path: Path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'publish_parent_normalized.sqlite'}"
    await _create_tables(db_url)

    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                """
                INSERT INTO orders (
                    cost_center, store_code, order_number, order_date, customer_name, mobile_number, created_at
                ) VALUES (
                    'CC01', 'UC567', '1234', '2025-01-01T00:00:00+00:00', 'Alice', '9999999999', '2025-01-01T00:00:00+00:00'
                )
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO stg_uc_archive_order_details (run_id, store_code, order_code, quantity, weight, service)
                VALUES ('run-normalized', 'UC567', ' uc567-1234 ', 2, 1.25, 'Dryclean')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO stg_uc_archive_payment_details (
                    run_id, run_date, store_code, order_code, payment_mode, amount, payment_date_raw, transaction_id, ingest_remarks
                ) VALUES (
                    'run-normalized', '2025-01-03T00:00:00+00:00', 'UC567', ' uc567-1234 ', 'UPI', 50,
                    '03 Jan 2025, 11:00 AM', 'TN1', 'normalized-key-test'
                )
                """
            )
        )
        await session.commit()

    order_metrics = await publish_uc_gst_order_details_to_orders(database_url=db_url, run_id='run-normalized', store_code='UC567')
    sales_metrics = await publish_uc_gst_payments_to_sales(database_url=db_url, run_id='run-normalized', store_code='UC567')

    assert order_metrics.updated == 1
    assert order_metrics.reason_codes.get(REASON_GST_LIFECYCLE_PARENT_ORDER_CONTEXT_MISSING, 0) == 0
    assert sales_metrics.publish_parent_match_rate == 1.0
    assert sales_metrics.missing_parent_count == 0
    assert sales_metrics.preflight_warning is None
    assert sales_metrics.reason_codes.get(REASON_GST_LIFECYCLE_PARENT_COVERAGE_NEAR_ZERO, 0) == 0
    assert sales_metrics.reason_codes.get(REASON_GST_LIFECYCLE_PARENT_ORDER_CONTEXT_MISSING, 0) == 0

    async with session_scope(db_url) as session:
        order_row = (
            await session.execute(sa.text("SELECT pieces, weight FROM orders WHERE store_code='UC567' AND order_number='1234'"))
        ).one()
        sales_row = (
            await session.execute(sa.text("SELECT store_code, order_number FROM sales WHERE transaction_id='TN1'"))
        ).one()

    assert Decimal(str(order_row.pieces)) == Decimal("2")
    assert Decimal(str(order_row.weight)) == Decimal("1.25")
    assert sales_row.store_code == "UC567"
    assert sales_row.order_number == "1234"


@pytest.mark.asyncio
async def test_payment_preflight_scoped_to_store_and_run_id(tmp_path: Path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'publish_scope.sqlite'}"
    await _create_tables(db_url)

    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                """
                INSERT INTO orders (cost_center, store_code, order_number, order_date, customer_name, mobile_number, created_at)
                VALUES
                    ('CC10', 'UC610', 'ORD-MATCH', '2025-01-01T00:00:00+00:00', 'Scope User', '9999999999', '2025-01-01T00:00:00+00:00'),
                    ('CC56', 'UC567', 'ORD-EXISTING', '2025-01-01T00:00:00+00:00', 'Other User', '8888888888', '2025-01-01T00:00:00+00:00')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO stg_uc_archive_payment_details (run_id, run_date, store_code, order_code, payment_mode, amount, payment_date_raw, transaction_id)
                VALUES
                    ('run-uc610', '2025-01-03T00:00:00+00:00', 'UC610', 'ORD-MATCH', 'UPI', 50, '03 Jan 2025, 11:00 AM', 'T610-1'),
                    ('run-uc610', '2025-01-03T00:00:00+00:00', 'UC610', 'ORD-MISSING', 'UPI', 60, '03 Jan 2025, 11:10 AM', 'T610-2'),
                    ('run-uc567', '2025-01-03T00:00:00+00:00', 'UC567', 'ORD-OTHER-MISSING', 'UPI', 70, '03 Jan 2025, 11:20 AM', 'T567-1')
                """
            )
        )
        await session.commit()

    metrics = await publish_uc_gst_payments_to_sales(
        database_url=db_url,
        store_code='UC610',
        run_id='run-uc610',
    )

    assert metrics.publish_parent_match_rate == 0.5
    assert metrics.preflight_warning is not None
    assert 'UC610:ORD-MISSING' in metrics.preflight_warning
    assert 'UC567:ORD-OTHER-MISSING' not in metrics.preflight_warning
    assert metrics.preflight_diagnostics is not None
    assert metrics.preflight_diagnostics['sample_missing_keys'] == ['UC610:ORD-MISSING']



@pytest.mark.asyncio
async def test_orders_enrichment_keeps_long_service_type_value(tmp_path: Path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'publish_orders_long_service.sqlite'}"
    await _create_tables(db_url)

    async with session_scope(db_url) as session:
        await session.execute(
            sa.text(
                """
                INSERT INTO orders (
                    cost_center, store_code, order_number, order_date, customer_name, mobile_number,
                    run_id, run_date, created_at
                ) VALUES (
                    'CC01', 'UC610', 'ORD-LONG-1', '2025-01-01T00:00:00+00:00', 'Alice', '9999999999',
                    'orig-run', '2025-01-01T00:00:00+00:00', '2025-01-01T00:00:00+00:00'
                )
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO stg_uc_archive_order_details (run_id, store_code, order_code, quantity, weight, service)
                VALUES ('run-long-service', 'UC610', 'ORD-LONG-1', 1, 1.0, 'Dry cleaning, Laundry - Wash & Fold')
                """
            )
        )
        await session.commit()

    metrics = await publish_uc_gst_order_details_to_orders(
        database_url=db_url, run_id='run-long-service', store_code='UC610'
    )

    assert metrics.updated == 1

    async with session_scope(db_url) as session:
        service_type = (
            await session.execute(
                sa.text(
                    "SELECT service_type FROM orders WHERE store_code='UC610' AND order_number='ORD-LONG-1'"
                )
            )
        ).scalar_one()

    assert service_type == 'Dry cleaning, Laundry - Wash & Fold'


@pytest.mark.asyncio
async def test_line_item_publish_assigns_serials_and_single_row_defaults(tmp_path: Path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'publish_line_items.sqlite'}"
    await _create_tables(db_url)

    async with session_scope(db_url) as session:
        await session.execute(sa.text("""
            INSERT INTO orders (id, cost_center, store_code, order_number, order_date, customer_name, mobile_number, order_status, created_at)
            VALUES
                (101, 'CC01', 'UC567', 'ORD-1', '2025-01-01T00:00:00+00:00', 'Alice', '9999999999', 'Delivered', '2025-01-01T00:00:00+00:00'),
                (102, 'CC01', 'UC567', 'ORD-2', '2025-01-01T00:00:00+00:00', 'Bob', '8888888888', 'Booked', '2025-01-01T00:00:00+00:00')
        """))
        await session.execute(sa.text("""
            INSERT INTO stg_uc_archive_order_details
            (id, run_id, run_date, cost_center, store_code, order_code, service, item_name, rate, quantity, weight, amount, order_datetime_raw, line_hash, ingest_remarks)
            VALUES
                (1, 'run-li', '2025-01-03T00:00:00+00:00', 'CC01', 'UC567', 'ORD-1', 'Dryclean', 'Shirt', 50, 1, 0.5, 50, '03 Jan 2025, 10:30 AM', 'bhash', 'r1'),
                (2, 'run-li', '2025-01-03T00:00:00+00:00', 'CC01', 'UC567', 'ORD-1', 'Wash', 'Pants', 70, 2, 0.75, 140, '03 Jan 2025, 10:30 AM', 'ahash', 'r2'),
                (3, 'run-li', '2025-01-03T00:00:00+00:00', 'CC01', 'UC567', 'ORD-2', 'Iron', 'Kurta', 30, 1, NULL, 30, '03 Jan 2025, 10:30 AM', NULL, 'r3')
        """))
        await session.commit()

    metrics = await publish_uc_gst_order_details_to_line_items(database_url=db_url, run_id='run-li', store_code='UC567')
    assert metrics.inserted == 3

    async with session_scope(db_url) as session:
        rows = (await session.execute(sa.text("""
            SELECT order_number, order_id, line_item_key, line_item_uid, is_orphan, weight
            FROM order_line_items
            ORDER BY order_number, order_id
        """))).all()

    assert [(r.order_number, r.order_id) for r in rows] == [('ORD-1', 1), ('ORD-1', 2), ('ORD-2', 1)]
    assert rows[0].line_item_key == 'ahash'
    assert rows[2].line_item_key == 'Kurta|Iron|30.00'
    assert rows[2].line_item_uid.endswith('|1')
    assert Decimal(str(rows[0].weight)) == Decimal('0.75')
    assert rows[2].weight is None
    assert all(not r.is_orphan for r in rows)


@pytest.mark.asyncio
async def test_line_item_publish_is_deterministic_and_marks_orphans(tmp_path: Path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'publish_line_items_orphan.sqlite'}"
    await _create_tables(db_url)

    async with session_scope(db_url) as session:
        await session.execute(sa.text("""
            INSERT INTO orders (cost_center, store_code, order_number, order_date, customer_name, mobile_number, created_at)
            VALUES ('CC01', 'UC567', 'ORD-1', '2025-01-01T00:00:00+00:00', 'Alice', '9999999999', '2025-01-01T00:00:00+00:00')
        """))
        await session.execute(sa.text("""
            INSERT INTO stg_uc_archive_order_details
            (id, run_id, run_date, cost_center, store_code, order_code, service, item_name, rate, quantity, amount, order_datetime_raw, line_hash, ingest_remarks)
            VALUES
                (1, 'run-det', '2025-01-03T00:00:00+00:00', 'CC01', 'UC567', 'ORD-1', 'Dryclean', 'Shirt', 50, 1, 50, '03 Jan 2025, 10:30 AM', 'h1', 'r1'),
                (2, 'run-det', '2025-01-03T00:00:00+00:00', 'CC01', 'UC567', 'ORD-MISS', 'Wash', 'Pants', 70, 1, 70, '03 Jan 2025, 10:30 AM', 'h2', 'r2')
        """))
        await session.commit()

    first = await publish_uc_gst_order_details_to_line_items(database_url=db_url, run_id='run-det', store_code='UC567')
    second = await publish_uc_gst_order_details_to_line_items(database_url=db_url, run_id='run-det', store_code='UC567')

    assert first.inserted == 2
    assert second.updated == 2

    async with session_scope(db_url) as session:
        count = (await session.execute(sa.text("SELECT COUNT(*) FROM order_line_items"))).scalar_one()
        orphan = (await session.execute(sa.text("""
            SELECT is_orphan, ingest_remarks FROM order_line_items WHERE order_number='ORD-MISS'
        """))).one()
    assert count == 2
    assert orphan.is_orphan
    assert 'parent_order_missing' in (orphan.ingest_remarks or '')
