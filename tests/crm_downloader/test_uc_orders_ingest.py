from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import openpyxl
import pytest
import sqlalchemy as sa

from app.common.db import session_scope
from app.crm_downloader.uc_orders_sync import ingest as uc_ingest
from app.crm_downloader.uc_orders_sync.ingest import (
    _expected_headers,
    _orders_table,
    _stg_uc_orders_table,
    ingest_uc_orders_workbook,
)
from app.dashboard_downloader.json_logger import get_logger


@pytest.fixture(autouse=True)
def _patch_uc_timezone(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(uc_ingest, "get_timezone", lambda: timezone.utc)


def _build_uc_workbook(path: Path, *, order_number: str = "UC-001") -> Path:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(list(_expected_headers()))
    ws.append(
        [
            1,
            order_number,
            "INV-1",
            "10/01/2025",
            "Alice",
            "+91 99999-88888",
            "Paid",
            "GSTIN-1",
            "KA",
            "100",
            "9",
            "9",
            "118",
        ]
    )
    wb.save(path)
    return path


async def _create_archive_base_table(database_url: str) -> None:
    metadata = sa.MetaData()
    sa.Table(
        "stg_uc_archive_orders_base",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("store_code", sa.String(length=8)),
        sa.Column("order_code", sa.String(length=24)),
        sa.Column("run_date", sa.DateTime(timezone=True)),
        sa.Column("customer_source", sa.String(length=64)),
        sa.Column("address", sa.Text),
    )
    async with session_scope(database_url) as session:
        bind = session.bind
        assert isinstance(bind, sa.ext.asyncio.AsyncEngine)
        async with bind.begin() as conn:
            await conn.run_sync(metadata.create_all)


@pytest.mark.asyncio
async def test_uc_ingest_enriches_source_and_address_from_archive(tmp_path: Path) -> None:
    db_path = tmp_path / "uc_ingest.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    await _create_archive_base_table(database_url)

    async with session_scope(database_url) as session:
        await session.execute(
            sa.text(
                """
                INSERT INTO stg_uc_archive_orders_base (store_code, order_code, run_date, customer_source, address)
                VALUES
                    ('S001', 'UC-001', '2025-01-01T10:00:00+00:00', 'Old Source', 'Old Address'),
                    ('S001', 'UC-001', '2025-01-02T10:00:00+00:00', 'App', '221B Baker Street')
                """
            )
        )
        await session.commit()

    workbook = _build_uc_workbook(tmp_path / "uc.xlsx")
    result = await ingest_uc_orders_workbook(
        workbook_path=workbook,
        store_code="S001",
        cost_center="C001",
        run_id="run-1",
        run_date=datetime(2025, 1, 3),
        database_url=database_url,
        logger=get_logger("test_uc_ingest"),
    )
    assert result.staging_rows == 1
    assert result.final_rows == 1

    metadata = sa.MetaData()
    stg_table = _stg_uc_orders_table(metadata)
    orders_table = _orders_table(metadata)
    async with session_scope(database_url) as session:
        stg_row = (
            await session.execute(
                sa.select(stg_table.c.customer_source, stg_table.c.customer_address).where(stg_table.c.order_number == "UC-001")
            )
        ).one()
        final_row = (
            await session.execute(
                sa.select(orders_table.c.customer_source, orders_table.c.customer_address).where(orders_table.c.order_number == "UC-001")
            )
        ).one()
    assert stg_row.customer_source == "App"
    assert stg_row.customer_address == "221B Baker Street"
    assert final_row.customer_source == "App"
    assert final_row.customer_address == "221B Baker Street"


@pytest.mark.asyncio
async def test_uc_ingest_without_archive_does_not_apply_hardcoded_source(tmp_path: Path) -> None:
    db_path = tmp_path / "uc_ingest_no_archive.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    workbook = _build_uc_workbook(tmp_path / "uc.xlsx")

    await ingest_uc_orders_workbook(
        workbook_path=workbook,
        store_code="S001",
        cost_center="C001",
        run_id="run-2",
        run_date=datetime(2025, 1, 3),
        database_url=database_url,
        logger=get_logger("test_uc_ingest"),
    )

    metadata = sa.MetaData()
    orders_table = _orders_table(metadata)
    async with session_scope(database_url) as session:
        final_row = (
            await session.execute(
                sa.select(orders_table.c.customer_source, orders_table.c.customer_address).where(orders_table.c.order_number == "UC-001")
            )
        ).one()
    assert final_row.customer_source is None
    assert final_row.customer_address is None


@pytest.mark.asyncio
async def test_uc_ingest_blank_archive_values_do_not_overwrite_existing_staging(tmp_path: Path) -> None:
    db_path = tmp_path / "uc_ingest_blank_archive.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    await _create_archive_base_table(database_url)

    workbook = _build_uc_workbook(tmp_path / "uc.xlsx")
    run_date = datetime(2025, 1, 3)

    metadata = sa.MetaData()
    stg_table = _stg_uc_orders_table(metadata)
    async with session_scope(database_url) as session:
        bind = session.bind
        assert isinstance(bind, sa.ext.asyncio.AsyncEngine)
        async with bind.begin() as conn:
            await conn.run_sync(metadata.create_all)
        await session.execute(
            sa.insert(stg_table).values(
                run_id="seed",
                run_date=run_date,
                cost_center="C001",
                store_code="S001",
                s_no=1,
                order_number="UC-001",
                invoice_number="INV-1",
                invoice_date=datetime.fromisoformat("2025-01-10T00:00:00+00:00"),
                customer_name="Alice",
                mobile_number="9999988888",
                payment_status="Paid",
                customer_gstin="GSTIN-1",
                customer_source="Counter",
                customer_address="Existing Address",
                place_of_supply="KA",
                net_amount=100,
                cgst=9,
                sgst=9,
                gross_amount=118,
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO stg_uc_archive_orders_base (store_code, order_code, run_date, customer_source, address)
                VALUES ('S001', 'UC-001', '2025-01-04T10:00:00+00:00', '   ', '')
                """
            )
        )
        await session.commit()

    await ingest_uc_orders_workbook(
        workbook_path=workbook,
        store_code="S001",
        cost_center="C001",
        run_id="run-3",
        run_date=run_date,
        database_url=database_url,
        logger=get_logger("test_uc_ingest"),
    )

    orders_table = _orders_table(sa.MetaData())
    async with session_scope(database_url) as session:
        stg_row = (
            await session.execute(
                sa.select(stg_table.c.customer_source, stg_table.c.customer_address).where(stg_table.c.order_number == "UC-001")
            )
        ).one()
        order_row = (
            await session.execute(
                sa.select(orders_table.c.customer_source, orders_table.c.customer_address).where(orders_table.c.order_number == "UC-001")
            )
        ).one()

    assert stg_row.customer_source == "Counter"
    assert stg_row.customer_address == "Existing Address"
    assert order_row.customer_source == "Counter"
    assert order_row.customer_address == "Existing Address"
