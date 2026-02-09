from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import openpyxl
import pytest
import sqlalchemy as sa

from app.common.db import session_scope
from app.crm_downloader.uc_orders_sync.archive_ingest import (
    FILE_BASE,
    FILE_ORDER_DETAILS,
    FILE_PAYMENT_DETAILS,
    TABLE_ARCHIVE_BASE,
    TABLE_ARCHIVE_ORDER_DETAILS,
    TABLE_ARCHIVE_PAYMENT_DETAILS,
    ingest_uc_archive_excels,
    publish_uc_archive_to_orders_and_sales,
)
from app.dashboard_downloader.json_logger import get_logger


def _write_xlsx(path: Path, headers: list[str], rows: list[list[object]]) -> Path:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(headers)
    for row in rows:
        ws.append(row)
    wb.save(path)
    return path


async def _create_tables(database_url: str) -> None:
    metadata = sa.MetaData()
    sa.Table(
        "store_master",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("store_code", sa.String(8)),
        sa.Column("cost_center", sa.String(8)),
    )
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
        sa.Column("pickup_raw", sa.Text),
        sa.Column("delivery_raw", sa.Text),
        sa.Column("customer_name", sa.String(128)),
        sa.Column("customer_phone", sa.String(24)),
        sa.Column("address", sa.Text),
        sa.Column("payment_text", sa.Text),
        sa.Column("instructions", sa.Text),
        sa.Column("customer_source", sa.String(64)),
        sa.Column("status", sa.String(32)),
        sa.Column("status_date_raw", sa.Text),
        sa.Column("source_file", sa.Text),
        sa.UniqueConstraint("store_code", "order_code", name="uq_stg_uc_archive_orders_base_store_order"),
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
        sa.Column("order_mode", sa.String(64)),
        sa.Column("order_datetime_raw", sa.Text),
        sa.Column("pickup_datetime_raw", sa.Text),
        sa.Column("delivery_datetime_raw", sa.Text),
        sa.Column("service", sa.Text),
        sa.Column("hsn_sac", sa.String(32)),
        sa.Column("item_name", sa.Text),
        sa.Column("rate", sa.Numeric(12, 2)),
        sa.Column("quantity", sa.Numeric(12, 2)),
        sa.Column("weight", sa.Numeric(12, 3)),
        sa.Column("addons", sa.Text),
        sa.Column("amount", sa.Numeric(12, 2)),
        sa.Column("line_hash", sa.String(64)),
        sa.Column("source_file", sa.Text),
        sa.UniqueConstraint("store_code", "order_code", "line_hash", name="uq_stg_uc_archive_order_details_store_order_line"),
    )

    sa.Table(
        "orders",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("run_id", sa.Text),
        sa.Column("run_date", sa.DateTime(timezone=True)),
        sa.Column("cost_center", sa.String(8)),
        sa.Column("store_code", sa.String(8)),
        sa.Column("order_number", sa.String(24)),
        sa.Column("order_date", sa.DateTime(timezone=True), nullable=False),
        sa.Column("customer_name", sa.String(128)),
        sa.Column("customer_address", sa.String(256)),
        sa.Column("mobile_number", sa.String(16)),
        sa.Column("service_type", sa.String(64)),
        sa.Column("pieces", sa.Numeric(12, 0)),
        sa.Column("weight", sa.Numeric(12, 2)),
    )
    sa.Table(
        "sales",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("run_id", sa.Text),
        sa.Column("run_date", sa.DateTime(timezone=True)),
        sa.Column("cost_center", sa.String(8)),
        sa.Column("store_code", sa.String(16)),
        sa.Column("order_date", sa.DateTime(timezone=True)),
        sa.Column("payment_date", sa.DateTime(timezone=True)),
        sa.Column("order_number", sa.String(16)),
        sa.Column("customer_code", sa.String(16)),
        sa.Column("customer_name", sa.String(128)),
        sa.Column("customer_address", sa.String(256)),
        sa.Column("mobile_number", sa.String(16)),
        sa.Column("payment_received", sa.Numeric(12, 2)),
        sa.Column("adjustments", sa.Numeric(12, 2)),
        sa.Column("balance", sa.Numeric(12, 2)),
        sa.Column("accepted_by", sa.String(64)),
        sa.Column("payment_mode", sa.String(32)),
        sa.Column("transaction_id", sa.String(64)),
        sa.Column("payment_made_at", sa.String(128)),
        sa.Column("order_type", sa.String(32)),
        sa.Column("is_duplicate", sa.Boolean()),
        sa.Column("is_edited_order", sa.Boolean()),
        sa.Column("ingest_remarks", sa.Text),
        sa.UniqueConstraint("cost_center", "order_number", "payment_date", name="uq_sales_cost_center_order_number_payment_date"),
    )

    payment = sa.Table(
        TABLE_ARCHIVE_PAYMENT_DETAILS,
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("run_id", sa.Text),
        sa.Column("run_date", sa.DateTime(timezone=True)),
        sa.Column("cost_center", sa.String(8)),
        sa.Column("store_code", sa.String(8)),
        sa.Column("ingest_remarks", sa.Text),
        sa.Column("order_code", sa.String(24)),
        sa.Column("payment_mode", sa.String(32)),
        sa.Column("amount", sa.Numeric(12, 2)),
        sa.Column("payment_date_raw", sa.Text),
        sa.Column("transaction_id", sa.String(128)),
        sa.Column("source_file", sa.Text),
    )

    async with session_scope(database_url) as session:
        bind = session.bind
        assert isinstance(bind, sa.ext.asyncio.AsyncEngine)
        async with bind.begin() as conn:
            await conn.run_sync(metadata.create_all)
            await conn.execute(
                sa.text(
                    """
                    CREATE UNIQUE INDEX uq_stg_uc_archive_payment_details_idempotency
                    ON stg_uc_archive_payment_details
                    (store_code, order_code, payment_date_raw, payment_mode, amount, coalesce(transaction_id, ''))
                    """
                )
            )


@pytest.mark.asyncio
async def test_uc_archive_happy_path_and_idempotency(tmp_path: Path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'db.sqlite'}"
    await _create_tables(db_url)
    async with session_scope(db_url) as session:
        await session.execute(sa.text("INSERT INTO store_master (store_code, cost_center) VALUES ('UC567', 'CC01')"))
        await session.commit()

    base = _write_xlsx(
        tmp_path / "UC567-base_order_info_20250101_20250105.xlsx",
        ["store_code", "order_code", "pickup", "delivery", "customer_name", "customer_phone", "address", "payment_text", "instructions", "customer_source", "status", "status_date"],
        [[" uc567 ", "ord-1", "2025-01-01", "2025-01-02", " Alice ", "+91 99999-88888", "addr", "paid", "inst", "Walk in", "Delivered", "2025-01-02"]],
    )
    details = _write_xlsx(
        tmp_path / "UC567-order_details_20250101_20250105.xlsx",
        ["store_code", "order_code", "order_mode", "order_datetime", "pickup_datetime", "delivery_datetime", "service", "hsn_sac", "item_name", "rate", "quantity", "weight", "addons", "amount"],
        [["UC567", "ORD-1", "Online", "raw", "praw", "draw", "Dryclean", "9988", "Shirt", "100", "2", "1.2", "None", "200"]],
    )
    payments = _write_xlsx(
        tmp_path / "UC567-payment_details_20250101_20250105.xlsx",
        ["store_code", "order_code", "payment_mode", "amount", "payment_date", "transaction_id"],
        [["UC567", "ORD-1", "UPI / Wallet", "200", "2025-01-02", "TX1"]],
    )

    logger = get_logger("test_uc_archive_ingest")
    first = await ingest_uc_archive_excels(
        database_url=db_url,
        run_id="run-1",
        run_date=datetime(2025, 1, 5, tzinfo=timezone.utc),
        store_code=None,
        cost_center=None,
        base_order_info_path=base,
        order_details_path=details,
        payment_details_path=payments,
        logger=logger,
    )
    assert first.files[FILE_BASE].inserted == 1
    assert first.files[FILE_ORDER_DETAILS].inserted == 1
    assert first.files[FILE_PAYMENT_DETAILS].inserted == 1

    second = await ingest_uc_archive_excels(
        database_url=db_url,
        run_id="run-2",
        run_date=datetime(2025, 1, 6, tzinfo=timezone.utc),
        store_code=None,
        cost_center=None,
        base_order_info_path=base,
        order_details_path=details,
        payment_details_path=payments,
        logger=logger,
    )
    assert second.files[FILE_BASE].updated == 1
    assert second.files[FILE_ORDER_DETAILS].updated == 1
    assert second.files[FILE_PAYMENT_DETAILS].updated == 1


@pytest.mark.asyncio
async def test_uc_archive_rejects_and_warning_remarks(tmp_path: Path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'db2.sqlite'}"
    await _create_tables(db_url)

    base = _write_xlsx(
        tmp_path / "base.xlsx",
        ["store_code", "order_code", "pickup", "delivery", "customer_name", "customer_phone", "address", "payment_text", "instructions", "customer_source", "status", "status_date"],
        [["", "", "", "", "", "12", "", "", "", "", "x", ""]],
    )
    details = _write_xlsx(
        tmp_path / "details.xlsx",
        ["store_code", "order_code", "order_mode", "order_datetime", "pickup_datetime", "delivery_datetime", "service", "hsn_sac", "item_name", "rate", "quantity", "weight", "addons", "amount"],
        [["", "ORD-2", "", "", "", "", "", "", "", "bad", "", "", "", ""]],
    )
    payments = _write_xlsx(
        tmp_path / "payments.xlsx",
        ["store_code", "order_code", "payment_mode", "amount", "payment_date", "transaction_id"],
        [["UC999", "ORD-3", "", "x", "-", ""]],
    )

    result = await ingest_uc_archive_excels(
        database_url=db_url,
        run_id="run-3",
        run_date=datetime(2025, 1, 7, tzinfo=timezone.utc),
        store_code=None,
        cost_center=None,
        base_order_info_path=base,
        order_details_path=details,
        payment_details_path=payments,
        logger=get_logger("test_uc_archive_ingest2"),
    )
    assert result.files[FILE_BASE].rejected == 1
    assert result.files[FILE_BASE].reject_reasons["missing_store_code"] == 1
    assert result.files[FILE_BASE].reject_reasons["missing_order_code"] == 1
    assert result.files[FILE_ORDER_DETAILS].rejected == 1
    assert result.files[FILE_ORDER_DETAILS].reject_reasons["missing_store_code"] == 1
    assert result.files[FILE_PAYMENT_DETAILS].inserted == 1

    async with session_scope(db_url) as session:
        row = (
            await session.execute(sa.text("SELECT ingest_remarks, cost_center, payment_mode FROM stg_uc_archive_payment_details"))
        ).one()
    assert "missing_cost_center_mapping" in (row.ingest_remarks or "")
    assert "missing_payment_mode" in (row.ingest_remarks or "")
    assert row.cost_center is None
    assert row.payment_mode == "UNKNOWN"


@pytest.mark.asyncio
async def test_uc_archive_payment_idempotency_with_blank_transaction_id(tmp_path: Path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'db3.sqlite'}"
    await _create_tables(db_url)

    base = _write_xlsx(
        tmp_path / "b.xlsx",
        ["store_code", "order_code", "pickup", "delivery", "customer_name", "customer_phone", "address", "payment_text", "instructions", "customer_source", "status", "status_date"],
        [["UC567", "ORD-9", "", "", "", "", "", "", "", "", "", ""]],
    )
    details = _write_xlsx(
        tmp_path / "d.xlsx",
        ["store_code", "order_code", "order_mode", "order_datetime", "pickup_datetime", "delivery_datetime", "service", "hsn_sac", "item_name", "rate", "quantity", "weight", "addons", "amount"],
        [["UC567", "ORD-9", "", "", "", "", "Svc", "", "Item", "1", "1", "", "", "1"]],
    )
    payments = _write_xlsx(
        tmp_path / "p.xlsx",
        ["store_code", "order_code", "payment_mode", "amount", "payment_date", "transaction_id"],
        [
            ["UC567", "ORD-9", "Cash", "50", "2025-01-01", ""],
            ["UC567", "ORD-9", "Cash", "50", "2025-01-01", None],
        ],
    )

    result = await ingest_uc_archive_excels(
        database_url=db_url,
        run_id="run-4",
        run_date=datetime(2025, 1, 7, tzinfo=timezone.utc),
        store_code=None,
        cost_center="CC01",
        base_order_info_path=base,
        order_details_path=details,
        payment_details_path=payments,
        logger=get_logger("test_uc_archive_ingest3"),
    )
    assert result.files[FILE_PAYMENT_DETAILS].inserted == 1

    async with session_scope(db_url) as session:
        count = (await session.execute(sa.text("SELECT COUNT(*) FROM stg_uc_archive_payment_details"))).scalar_one()
    assert count == 1


@pytest.mark.asyncio
async def test_uc_archive_publish_orders_and_sales_idempotent(tmp_path: Path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'db4.sqlite'}"
    await _create_tables(db_url)

    async with session_scope(db_url) as session:
        await session.execute(sa.text("""
            INSERT INTO orders (cost_center, store_code, order_number, order_date, customer_name, customer_address, mobile_number)
            VALUES ('CC01', 'UC567', 'ORD-1', '2025-01-01T00:00:00+00:00', 'Alice', 'Addr1', '9999988888')
        """))
        await session.execute(sa.text("""
            INSERT INTO stg_uc_archive_order_details (run_id, run_date, cost_center, store_code, order_code, service, quantity, weight, line_hash, source_file)
            VALUES
            ('run-pub', '2025-01-07T00:00:00+00:00', 'CC01', 'UC567', 'ORD-1', 'Dryclean', 2, 1.2, 'h1', 'f.xlsx'),
            ('run-pub', '2025-01-07T00:00:00+00:00', 'CC01', 'UC567', 'ORD-1', 'Steam', 1, 0.3, 'h2', 'f.xlsx')
        """))
        await session.execute(sa.text("""
            INSERT INTO stg_uc_archive_payment_details (run_id, run_date, cost_center, store_code, order_code, payment_mode, amount, payment_date_raw, transaction_id, source_file)
            VALUES
            ('run-pub', '2025-01-07T00:00:00+00:00', 'CC01', 'UC567', 'ORD-1', 'UPI', 200, '07-01-2025 10:30', 'TX1', 'p.xlsx'),
            ('run-pub', '2025-01-07T00:00:00+00:00', 'CC01', 'UC567', 'ORD-1', 'CASH', 50, '07-01-2025 10:30', 'TX1B', 'p.xlsx'),
            ('run-pub', '2025-01-07T00:00:00+00:00', 'CC01', 'UC567', 'ORD-MISS', 'UPI', 50, '07-01-2025 11:00', 'TX2', 'p.xlsx'),
            ('run-pub', '2025-01-07T00:00:00+00:00', 'CC01', 'UC567', 'ORD-1', 'CASH', 100, 'bad-date', 'TX3', 'p.xlsx')
        """))
        await session.commit()

    logger = get_logger('test_uc_archive_publish')
    first = await publish_uc_archive_to_orders_and_sales(database_url=db_url, logger=logger, run_id='run-pub')
    second = await publish_uc_archive_to_orders_and_sales(database_url=db_url, logger=logger, run_id='run-pub')

    assert first.orders_updated >= 1
    assert first.sales_inserted == 1
    assert first.skipped == 3
    assert first.skip_reasons['missing_order_context'] == 1
    assert first.skip_reasons['payment_date_parse_failure'] == 1
    assert first.skip_reasons['key_conflict'] == 1
    assert second.sales_updated + second.sales_inserted == 1

    async with session_scope(db_url) as session:
        order_row = (await session.execute(sa.text("SELECT pieces, weight, service_type FROM orders WHERE store_code='UC567' AND order_number='ORD-1'"))).one()
        sales_count = (await session.execute(sa.text("SELECT COUNT(*) FROM sales WHERE cost_center='CC01' AND order_number='ORD-1'"))).scalar_one()
    assert str(order_row.pieces) in {'3', '3.0000000000'}
    assert float(order_row.weight) == pytest.approx(1.5, rel=1e-6)
    assert order_row.service_type == 'Dryclean, Steam'
    assert sales_count == 1
