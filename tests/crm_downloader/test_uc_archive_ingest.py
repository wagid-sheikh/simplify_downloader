from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import openpyxl
import pytest
import sqlalchemy as sa

from app.common.db import session_scope
from app.crm_downloader.uc_orders_sync.archive_ingest import (
    _existing_rows_predicate,
    _stg_uc_archive_payment_details_table,
    FILE_BASE,
    FILE_ORDER_DETAILS,
    FILE_PAYMENT_DETAILS,
    TABLE_ARCHIVE_BASE,
    TABLE_ARCHIVE_ORDER_DETAILS,
    TABLE_ARCHIVE_PAYMENT_DETAILS,
    ingest_uc_archive_excels,
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
async def test_uc_archive_payment_idempotency_mixed_null_keys_and_numeric_amount(tmp_path: Path) -> None:
    db_url = f"sqlite+aiosqlite:///{tmp_path/'db4.sqlite'}"
    await _create_tables(db_url)

    base = _write_xlsx(
        tmp_path / "b2.xlsx",
        ["store_code", "order_code", "pickup", "delivery", "customer_name", "customer_phone", "address", "payment_text", "instructions", "customer_source", "status", "status_date"],
        [
            ["UC567", "ORD-10", "", "", "", "", "", "", "", "", "", ""],
            ["UC567", "ORD-11", "", "", "", "", "", "", "", "", "", ""],
        ],
    )
    details = _write_xlsx(
        tmp_path / "d2.xlsx",
        ["store_code", "order_code", "order_mode", "order_datetime", "pickup_datetime", "delivery_datetime", "service", "hsn_sac", "item_name", "rate", "quantity", "weight", "addons", "amount"],
        [
            ["UC567", "ORD-10", "", "", "", "", "Svc", "", "Item", "1", "1", "", "", "1"],
            ["UC567", "ORD-11", "", "", "", "", "Svc", "", "Item", "1", "1", "", "", "1"],
        ],
    )

    payments_first = _write_xlsx(
        tmp_path / "p2_first.xlsx",
        ["store_code", "order_code", "payment_mode", "amount", "payment_date", "transaction_id"],
        [
            ["UC567", "ORD-10", "Cash", "150", "2025-01-01", ""],
            ["UC567", "ORD-11", "", "200", "2025-01-02", "TX-11"],
        ],
    )

    first = await ingest_uc_archive_excels(
        database_url=db_url,
        run_id="run-5",
        run_date=datetime(2025, 1, 7, tzinfo=timezone.utc),
        store_code=None,
        cost_center="CC01",
        base_order_info_path=base,
        order_details_path=details,
        payment_details_path=payments_first,
        logger=get_logger("test_uc_archive_ingest4_first"),
    )
    assert first.files[FILE_PAYMENT_DETAILS].inserted == 2

    payments_second = _write_xlsx(
        tmp_path / "p2_second.xlsx",
        ["store_code", "order_code", "payment_mode", "amount", "payment_date", "transaction_id"],
        [
            ["UC567", "ORD-10", "Cash", "150.00", "2025-01-01", None],
            ["UC567", "ORD-11", "", "200.00", "2025-01-02", "TX-11"],
        ],
    )

    second = await ingest_uc_archive_excels(
        database_url=db_url,
        run_id="run-6",
        run_date=datetime(2025, 1, 8, tzinfo=timezone.utc),
        store_code=None,
        cost_center="CC01",
        base_order_info_path=base,
        order_details_path=details,
        payment_details_path=payments_second,
        logger=get_logger("test_uc_archive_ingest4_second"),
    )
    assert second.files[FILE_PAYMENT_DETAILS].updated == 2

    async with session_scope(db_url) as session:
        count = (await session.execute(sa.text("SELECT COUNT(*) FROM stg_uc_archive_payment_details"))).scalar_one()
    assert count == 2


def test_uc_archive_payment_match_predicate_compiles_for_postgres_and_sqlite() -> None:
    table = _stg_uc_archive_payment_details_table(sa.MetaData())
    key_names = ["store_code", "order_code", "payment_date_raw", "payment_mode", "amount", "transaction_id"]
    rows = [
        {
            "store_code": "UC567",
            "order_code": "ORD-10",
            "payment_date_raw": None,
            "payment_mode": "CASH",
            "amount": 150,
            "transaction_id": None,
        }
    ]

    predicate = _existing_rows_predicate(table, key_names, rows)
    postgres_sql = str(
        predicate.compile(
            dialect=sa.dialects.postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    sqlite_sql = str(
        predicate.compile(
            dialect=sa.dialects.sqlite.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "IS NOT DISTINCT FROM" in postgres_sql
    assert "coalesce" not in postgres_sql.lower()
    assert "coalesce" not in sqlite_sql.lower()
