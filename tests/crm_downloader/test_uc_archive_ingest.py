from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import openpyxl
import pytest
import sqlalchemy as sa

from app.common.db import session_scope
from app.crm_downloader.uc_orders_sync.archive_ingest import (
    ingest_uc_archive_excels,
    _stg_uc_archive_order_details_table,
    _stg_uc_archive_orders_base_table,
    _stg_uc_archive_payment_details_table,
)


def _write_xlsx(path: Path, headers: list[str], rows: list[list[object]]) -> None:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(headers)
    for row in rows:
        ws.append(row)
    wb.save(path)


async def _setup_db(database_url: str) -> None:
    metadata = sa.MetaData()
    sa.Table(
        "store_master",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("store_code", sa.String(8), unique=True),
        sa.Column("cost_center", sa.String(8)),
        sa.Column("sync_group", sa.String(2)),
    )
    _stg_uc_archive_orders_base_table(metadata)
    _stg_uc_archive_order_details_table(metadata)
    _stg_uc_archive_payment_details_table(metadata)

    async with session_scope(database_url) as session:
        bind = session.bind
        assert isinstance(bind, sa.ext.asyncio.AsyncEngine)
        async with bind.begin() as conn:
            await conn.run_sync(metadata.create_all)
            await conn.execute(sa.text("CREATE UNIQUE INDEX uq_base ON stg_uc_archive_orders_base(store_code, order_code)"))
            await conn.execute(sa.text("CREATE UNIQUE INDEX uq_details ON stg_uc_archive_order_details(store_code, order_code, line_hash)"))
            await conn.execute(
                sa.text(
                    "CREATE UNIQUE INDEX uq_payments ON stg_uc_archive_payment_details(store_code, order_code, payment_date_raw, payment_mode, amount, transaction_id)"
                )
            )

        await session.execute(
            sa.insert(metadata.tables["store_master"]),
            [
                {"store_code": "UC567", "cost_center": "SC3567", "sync_group": "UC"},
                {"store_code": "UC610", "cost_center": None, "sync_group": "UC"},
            ],
        )
        await session.commit()


@pytest.mark.asyncio
async def test_uc_archive_ingest_upserts_and_counts(tmp_path: Path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path/'archive.db'}"
    await _setup_db(database_url)

    _write_xlsx(
        tmp_path / "UC567-base_order_info_20250101_20250101.xlsx",
        ["store_code", "order_code", "pickup", "delivery", "customer_name", "customer_phone", "address", "payment_text", "instructions", "customer_source", "status", "status_date"],
        [["uc567", " ord1 ", "-", "01 Jan", "Alice", "+91 99999 88888", "A-1", "View Payment Details", "", "app", "delivered", "-"]],
    )
    _write_xlsx(
        tmp_path / "UC567-order_details_20250101_20250101.xlsx",
        ["store_code", "order_code", "order_mode", "order_datetime", "pickup_datetime", "delivery_datetime", "service", "hsn_sac", "item_name", "rate", "quantity", "weight", "addons", "amount"],
        [["UC567", "ORD1", "Walk-In", "", "", "", "Dryclean", "001", "Shirt", "₹100", "2", "1.500kg", "-", "200"]],
    )
    _write_xlsx(
        tmp_path / "UC567-payment_details_20250101_20250101.xlsx",
        ["store_code", "order_code", "payment_mode", "amount", "payment_date", "transaction_id"],
        [["UC567", "ORD1", "UPI / Wallet", "₹200", "01 Jan", None]],
    )

    first = await ingest_uc_archive_excels(
        folder=tmp_path,
        run_id="run-1",
        run_date=datetime(2025, 1, 2, tzinfo=timezone.utc),
        database_url=database_url,
    )
    second = await ingest_uc_archive_excels(
        folder=tmp_path,
        run_id="run-2",
        run_date=datetime(2025, 1, 3, tzinfo=timezone.utc),
        database_url=database_url,
    )

    assert len(first.files) == 3
    assert all(f.counters.inserted == 1 for f in first.files)
    assert all(f.counters.updated == 1 for f in second.files)

    metadata = sa.MetaData()
    base = _stg_uc_archive_orders_base_table(metadata)
    async with session_scope(database_url) as session:
        row = (await session.execute(sa.select(base.c.cost_center, base.c.store_code, base.c.order_code, base.c.customer_source))).one()
    assert row.cost_center == "SC3567"
    assert row.store_code == "UC567"
    assert row.order_code == "ORD1"
    assert row.customer_source == "APP"


@pytest.mark.asyncio
async def test_uc_archive_ingest_rejections_and_warnings(tmp_path: Path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path/'archive_warn.db'}"
    await _setup_db(database_url)

    _write_xlsx(
        tmp_path / "UC610-base_order_info_20250101_20250101.xlsx",
        ["store_code", "order_code", "pickup", "delivery", "customer_name", "customer_phone", "address", "payment_text", "instructions", "customer_source", "status", "status_date"],
        [
            ["UC999", "ORD1", "", "", "", "", "", "", "", "", "", ""],
            ["UC610", "", "", "", "", "12", "", "", "", "", "", ""],
            ["UC610", "ORD2", "", "", "", "12", "", "", "", "", "", ""],
        ],
    )

    result = await ingest_uc_archive_excels(
        folder=tmp_path,
        run_id="run-3",
        run_date=datetime(2025, 1, 2, tzinfo=timezone.utc),
        database_url=database_url,
    )
    file_result = result.files[0]
    assert file_result.counters.rejected == 2
    assert file_result.counters.rejected_reasons["missing_store_code"] == 1
    assert file_result.counters.rejected_reasons["missing_order_code"] == 1
    assert file_result.counters.inserted == 1
    assert file_result.counters.warnings >= 1
