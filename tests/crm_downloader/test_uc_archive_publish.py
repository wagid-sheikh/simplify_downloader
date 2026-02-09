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
from app.crm_downloader.uc_orders_sync.archive_publish import (
    REASON_MISSING_PARENT_ORDER_CONTEXT,
    REASON_PREFLIGHT_PARENT_COVERAGE_LOW,
    REASON_PREFLIGHT_PARENT_COVERAGE_NEAR_ZERO,
    REASON_UNPARSEABLE_PAYMENT_DATE,
    publish_uc_archive_order_details_to_orders,
    publish_uc_archive_payments_to_sales,
    publish_uc_archive_stage2_stage3,
)
from app.crm_downloader.uc_orders_sync.ingest import _orders_table
from app.crm_downloader.uc_orders_sync.ingest import _stg_uc_orders_table


async def _create_tables(db_url: str) -> None:
    metadata = sa.MetaData()
    _orders_table(metadata)
    _stg_uc_orders_table(metadata)
    _sales_table(metadata)
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
    )
    sa.Table(
        TABLE_ARCHIVE_ORDER_DETAILS,
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("store_code", sa.String(8)),
        sa.Column("order_code", sa.String(24)),
        sa.Column("quantity", sa.Numeric(12, 2)),
        sa.Column("weight", sa.Numeric(12, 3)),
        sa.Column("service", sa.Text),
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
                INSERT INTO stg_uc_archive_order_details (store_code, order_code, quantity, weight, service)
                VALUES
                    ('UC567', 'ORD-1', 2, 1.25, 'Dryclean'),
                    ('UC567', 'ORD-1', 3, 0.75, 'Wash'),
                    ('UC567', 'ORD-1', NULL, NULL, 'Dryclean')
                """
            )
        )
        await session.commit()

    metrics = await publish_uc_archive_order_details_to_orders(database_url=db_url)
    assert metrics.updated == 1

    async with session_scope(db_url) as session:
        row = (
            await session.execute(
                sa.text(
                    "SELECT pieces, weight, service_type, customer_name, net_amount, payment_status, run_id FROM orders WHERE store_code='UC567' AND order_number='ORD-1'"
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

    first = await publish_uc_archive_payments_to_sales(database_url=db_url)
    second = await publish_uc_archive_payments_to_sales(database_url=db_url)

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
    assert row.order_type == "UClean"


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

    sales_metrics = await publish_uc_archive_payments_to_sales(database_url=db_url)
    assert sales_metrics.skipped == 3
    assert sales_metrics.warnings == 1
    assert sales_metrics.reason_codes[REASON_PREFLIGHT_PARENT_COVERAGE_NEAR_ZERO] == 1
    assert sales_metrics.publish_parent_match_rate == 0.0
    assert sales_metrics.missing_parent_count == 2

    stage = await publish_uc_archive_stage2_stage3(database_url=db_url)
    assert isinstance(stage.orders.updated, int)
    assert stage.sales.skipped >= 3


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

    metrics = await publish_uc_archive_payments_to_sales(database_url=db_url)
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

    metrics = await publish_uc_archive_payments_to_sales(database_url=db_url)
    assert metrics.publish_parent_match_rate == 0.0
    assert metrics.missing_parent_count == 1
    assert metrics.inserted == 0
    assert metrics.skipped == 1
    assert metrics.warnings == 1
    assert metrics.reason_codes[REASON_PREFLIGHT_PARENT_COVERAGE_NEAR_ZERO] == 1
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

    metrics = await publish_uc_archive_payments_to_sales(database_url=db_url)
    assert metrics.publish_parent_match_rate == 0.5
    assert metrics.missing_parent_count == 1
    assert metrics.preflight_warning is not None
    assert metrics.inserted == 1
    assert metrics.reason_codes[REASON_PREFLIGHT_PARENT_COVERAGE_LOW] == 1
    assert metrics.reason_codes[REASON_MISSING_PARENT_ORDER_CONTEXT] == 1


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
    metrics = await publish_uc_archive_payments_to_sales(database_url=db_url)

    assert metrics.inserted == 1
    assert metrics.reason_codes[REASON_PREFLIGHT_PARENT_COVERAGE_LOW] == 1
    assert metrics.reason_codes[REASON_MISSING_PARENT_ORDER_CONTEXT] == 2
    assert metrics.preflight_warning is not None
    assert "ORD-X" in metrics.preflight_warning
    assert metrics.preflight_diagnostics is not None
    assert metrics.preflight_diagnostics["matched_parent_keys"] == 1

    warning_records = [r for r in caplog.records if "archive_publish_parent_preflight_low" in r.message]
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

    metrics = await publish_uc_archive_payments_to_sales(database_url=db_url)
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
