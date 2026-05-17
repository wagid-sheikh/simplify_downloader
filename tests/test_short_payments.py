from decimal import Decimal

import pytest
import sqlalchemy as sa

from app.common.db import session_scope
from app.reports.shared.short_payments import fetch_short_payment_rows


def _create_tables(database_url: str) -> None:
    engine = sa.create_engine(database_url.replace("+aiosqlite", ""))
    with engine.begin() as conn:
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
                    recovery_status TEXT
                )
                """
            )
        )
        conn.execute(
            sa.text(
                """
                CREATE VIEW vw_orders AS
                SELECT
                    cost_center,
                    order_number,
                    order_date,
                    customer_name,
                    mobile_number,
                    net_amount AS order_amount,
                    recovery_status
                FROM orders
                """
            )
        )
        conn.execute(
            sa.text(
                """
                CREATE TABLE sales (
                    cost_center TEXT,
                    order_number TEXT,
                    payment_received NUMERIC
                )
                """
            )
        )
        conn.execute(
            sa.text(
                """
                CREATE TABLE payment_collections (
                    cost_center TEXT,
                    order_number TEXT,
                    amount NUMERIC,
                    source_type TEXT
                )
                """
            )
        )


@pytest.mark.asyncio
async def test_fetch_short_payment_rows_excludes_duplicate_identity_with_recovery_status(
    tmp_path,
) -> None:
    db_path = tmp_path / "short_payments_duplicate_recovery.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)

    orders = sa.table(
        "vw_orders",
        sa.column("cost_center"),
        sa.column("order_number"),
        sa.column("order_date"),
        sa.column("customer_name"),
        sa.column("mobile_number"),
        sa.column("order_amount"),
        sa.column("recovery_status"),
    )
    sales = sa.table(
        "sales",
        sa.column("cost_center"),
        sa.column("order_number"),
        sa.column("payment_received"),
    )
    payment_collections = sa.table(
        "payment_collections",
        sa.column("cost_center"),
        sa.column("order_number"),
        sa.column("amount"),
        sa.column("source_type"),
    )

    async with session_scope(database_url) as session:
        await session.execute(
            sa.text(
                """
                INSERT INTO orders (
                    cost_center, order_number, order_date, customer_name,
                    mobile_number, net_amount, recovery_status
                ) VALUES
                    ('UN3668', 'T2724', '2026-04-29T09:00:00+05:30',
                     'Write Off Customer', '999', 1570, 'WRITE_OFF'),
                    ('UN3668', 'T 2724', '2026-04-29T09:05:00+05:30',
                     'Duplicate Customer', '888', 1570, NULL)
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO sales (cost_center, order_number, payment_received)
                VALUES ('UN3668', 'T2724', 1178)
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO payment_collections (
                    cost_center, order_number, amount, source_type
                ) VALUES ('UN3668', 'T2724', 1178, 'google_sheet')
                """
            )
        )
        await session.commit()

    async with session_scope(database_url) as session:
        rows = await fetch_short_payment_rows(
            session=session,
            orders=orders,
            payment_collections=payment_collections,
            sales=sales,
        )

    assert [row.order_number for row in rows] == []
    assert Decimal("1570") - Decimal("1178") == Decimal("392")
