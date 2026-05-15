from __future__ import annotations

import importlib.util
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Callable

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic.migration import MigrationContext
from alembic.operations import Operations
from app.common.db import session_scope
from app.reports.shared.short_payments import (
    fetch_missing_payment_rows_without_proof,
    fetch_short_payment_rows,
)


def _load_migration_module():
    project_root = Path(__file__).resolve().parents[1]
    module_path = (
        project_root / "alembic" / "versions" / "0115_canon_missing_view.py"
    )
    spec = importlib.util.spec_from_file_location(
        "v0115_canon_missing_view", module_path
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load migration module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


migration = _load_migration_module()


@dataclass
class MissingPaymentReportRow:
    cost_center: str
    order_number: str
    order_date: datetime | None
    customer_name: str | None
    mobile_number: str | None
    order_amount: Decimal


def _run_migration(
    connection: sa.Connection, fn: Callable[[], None], monkeypatch: pytest.MonkeyPatch
) -> None:
    context = MigrationContext.configure(connection)
    operations = Operations(context)
    original_op = migration.op
    monkeypatch.setattr(migration, "op", operations)
    try:
        fn()
    finally:
        monkeypatch.setattr(migration, "op", original_op)


def _create_schema_and_fixture(connection: sa.Connection) -> None:
    connection.execute(sa.text("""
            CREATE TABLE vw_orders (
                cost_center TEXT NOT NULL,
                order_number TEXT NOT NULL,
                order_date TEXT NOT NULL,
                customer_name TEXT,
                mobile_number TEXT,
                order_amount NUMERIC(12, 2) NOT NULL,
                recovery_status TEXT
            )
            """))
    connection.execute(sa.text("""
            CREATE TABLE sales (
                cost_center TEXT NOT NULL,
                order_number TEXT NOT NULL,
                payment_received NUMERIC(12, 2)
            )
            """))
    connection.execute(sa.text("""
            CREATE TABLE payment_collections (
                payment_id INTEGER PRIMARY KEY,
                cost_center TEXT NOT NULL,
                order_number TEXT,
                amount NUMERIC(12, 2) NOT NULL,
                source_type TEXT NOT NULL
            )
            """))
    connection.execute(sa.text("""
            CREATE VIEW vw_orders_missing_in_payment_collections AS
            SELECT cost_center, order_number, date(order_date) AS order_date,
                   customer_name, mobile_number, order_amount AS net_amount
            FROM vw_orders
            """))
    connection.execute(sa.text("""
            INSERT INTO vw_orders (
                cost_center, order_number, order_date, customer_name, mobile_number,
                order_amount, recovery_status
            ) VALUES
                ('CC1', 'GROUP-A', '2026-05-01T09:00:00', 'Grouped A', '9001', 100, NULL),
                ('CC1', 'GROUP-B', '2026-05-01T10:00:00', 'Grouped B', '9002', 100, NULL),
                ('CC1', 'TOPUP-A', '2026-05-01T11:00:00', 'Topup A', '9003', 100, NULL),
                ('CC1', 'TOPUP-B', '2026-05-01T12:00:00', 'Topup B', '9004', 200, NULL),
                ('CC1', 'MISSING-PROOF', '2026-05-01T13:00:00', 'Missing Proof', '9005', 120, NULL),
                ('CC1', 'MISSING-SALES', '2026-05-01T14:00:00', 'Missing Sales', '9006', 130, NULL),
                ('CC1', 'RECOVERY-EXCLUDED', '2026-05-01T15:00:00', 'Recovery', '9007', 140, 'WRITE_OFF'),
                ('CC1', 'UNSUPPORTED-PROOF', '2026-05-01T16:00:00', 'Unsupported', '9008', 150, NULL),
                ('CC1', 'SHORT-A', '2026-05-01T17:00:00', 'Short A', '9009', 100, NULL),
                ('CC1', 'SHORT-B', '2026-05-01T18:00:00', 'Short B', '9010', 200, NULL)
            """))
    connection.execute(sa.text("""
            INSERT INTO sales (cost_center, order_number, payment_received) VALUES
                ('CC1', 'GROUP-A', 100),
                ('CC1', 'GROUP-B', 100),
                ('CC1', 'TOPUP-A', 100),
                ('CC1', 'TOPUP-B', 200),
                ('CC1', 'MISSING-PROOF', 120),
                ('CC1', 'RECOVERY-EXCLUDED', 140),
                ('CC1', 'UNSUPPORTED-PROOF', 150),
                ('CC1', 'SHORT-A', 100),
                ('CC1', 'SHORT-B', 150)
            """))
    connection.execute(sa.text("""
            INSERT INTO payment_collections (
                payment_id, cost_center, order_number, amount, source_type
            ) VALUES
                (1, 'CC1', 'GROUP-A / group-b', 200, 'Google_Sheet'),
                (2, 'CC1', 'TOPUP-A,TOPUP-B', 250, 'google_sheet'),
                (3, 'CC1', 'TOPUP-B', 50, 'google_sheet'),
                (4, 'CC1', 'UNSUPPORTED-PROOF', 150, 'bank_statement'),
                (5, 'CC1', 'SHORT-A/SHORT-B', 250, 'google_sheet'),
                (6, 'CC1', 'UNRELATED-HISTORY', 999999, 'google_sheet'),
                (7, 'CC2', 'SHORT-A/SHORT-B', 999999, 'google_sheet')
            """))


@pytest.mark.asyncio
async def test_sql_missing_payment_view_matches_python_report_helper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "missing_payment_parity.db"
    sync_url = f"sqlite:///{db_path}"
    async_url = f"sqlite+aiosqlite:///{db_path}"
    engine = sa.create_engine(sync_url)

    with engine.begin() as connection:
        _create_schema_and_fixture(connection)
        _run_migration(connection, migration.upgrade, monkeypatch)
        sql_rows = connection.execute(sa.text("""
                SELECT order_number, net_amount
                FROM vw_orders_missing_in_payment_collections
                ORDER BY order_number
                """)).mappings().all()

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
    payment_collections = sa.table(
        "payment_collections",
        sa.column("cost_center"),
        sa.column("order_number"),
        sa.column("amount"),
        sa.column("source_type"),
    )
    sales = sa.table(
        "sales",
        sa.column("cost_center"),
        sa.column("order_number"),
        sa.column("payment_received"),
    )

    async with session_scope(async_url) as session:
        python_rows = await fetch_missing_payment_rows_without_proof(
            session=session,
            orders=orders,
            payment_collections=payment_collections,
            sales=sales,
            start_datetime=datetime(2026, 5, 1),
            end_datetime=datetime(2026, 5, 2),
            row_factory=MissingPaymentReportRow,
        )
        short_rows = await fetch_short_payment_rows(
            session=session,
            orders=orders,
            payment_collections=payment_collections,
            sales=sales,
            start_datetime=datetime(2026, 5, 1),
            end_datetime=datetime(2026, 5, 2),
        )

    assert [dict(row) for row in sql_rows] == [
        {"order_number": "MISSING-PROOF", "net_amount": 120},
        {"order_number": "UNSUPPORTED-PROOF", "net_amount": 150},
    ]
    assert [(row.order_number, row.order_amount) for row in python_rows] == [
        ("MISSING-PROOF", Decimal("120.00")),
        ("UNSUPPORTED-PROOF", Decimal("150.00")),
    ]
    assert [
        (row.order_number, row.paid_amount, row.shortage_amount, row.group_key)
        for row in short_rows
    ] == [
        ("SHORT-B", Decimal("150.00"), Decimal("50.00"), "SHORT-A|SHORT-B"),
    ]


@pytest.mark.asyncio
async def test_payment_reconciliation_limits_payment_collection_query_to_candidates() -> None:
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
    payment_collections = sa.table(
        "payment_collections",
        sa.column("cost_center"),
        sa.column("order_number"),
        sa.column("amount"),
        sa.column("source_type"),
    )
    sales = sa.table(
        "sales",
        sa.column("cost_center"),
        sa.column("order_number"),
        sa.column("payment_received"),
    )

    class _Result:
        def __init__(self, rows: list[dict[str, object]]) -> None:
            self._rows = rows

        def mappings(self) -> list[dict[str, object]]:
            return self._rows

    class _Session:
        def __init__(self) -> None:
            self.statements: list[sa.sql.ClauseElement] = []

        async def execute(self, stmt):
            self.statements.append(stmt)
            compiled = str(
                stmt.compile(
                    dialect=postgresql.dialect(),
                    compile_kwargs={"literal_binds": True},
                )
            )
            if "FROM vw_orders" in compiled:
                return _Result(
                    [
                        {
                            "cost_center": "CC1",
                            "order_number": "ORD123",
                            "order_date": datetime(2026, 5, 1, 9),
                            "customer_name": "Alice",
                            "mobile_number": "999",
                            "order_amount": Decimal("100"),
                        }
                    ]
                )
            if "FROM payment_collections" in compiled:
                return _Result(
                    [
                        {
                            "cost_center": "CC1",
                            "order_number": "ORD123",
                            "amount": Decimal("80"),
                            "source_type": "google_sheet",
                        }
                    ]
                )
            if "FROM sales" in compiled:
                return _Result(
                    [
                        {
                            "cost_center": "CC1",
                            "order_number": "ORD123",
                            "payment_received": Decimal("80"),
                        }
                    ]
                )
            return _Result([])

    session = _Session()
    rows = await fetch_short_payment_rows(
        session=session,
        orders=orders,
        payment_collections=payment_collections,
        sales=sales,
        start_datetime=datetime(2026, 5, 1),
        end_datetime=datetime(2026, 5, 2),
    )

    payment_sql = "\n".join(
        str(
            stmt.compile(
                dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
            )
        )
        for stmt in session.statements
        if "payment_collections" in str(stmt)
    )
    assert [(row.order_number, row.paid_amount, row.shortage_amount) for row in rows] == [
        ("ORD123", Decimal("80"), Decimal("20")),
    ]
    assert "payment_collections.cost_center IN ('CC1')" in payment_sql
    assert "LIKE" in payment_sql
    assert "ORD123" in payment_sql

@pytest.mark.asyncio
async def test_fetch_short_payment_rows_requires_sales_backed_consistent_evidence(tmp_path: Path) -> None:
    db_path = tmp_path / "short_payment_truth_inputs.db"
    sync_url = f"sqlite:///{db_path}"
    async_url = f"sqlite+aiosqlite:///{db_path}"
    engine = sa.create_engine(sync_url)
    with engine.begin() as connection:
        connection.execute(sa.text("""
            CREATE TABLE vw_orders (
                cost_center TEXT NOT NULL,
                order_number TEXT NOT NULL,
                order_date TEXT NOT NULL,
                customer_name TEXT,
                mobile_number TEXT,
                order_amount NUMERIC(12, 2) NOT NULL,
                recovery_status TEXT
            )
        """))
        connection.execute(sa.text("""
            CREATE TABLE sales (
                cost_center TEXT NOT NULL,
                order_number TEXT NOT NULL,
                payment_received NUMERIC(12, 2)
            )
        """))
        connection.execute(sa.text("""
            CREATE TABLE payment_collections (
                cost_center TEXT NOT NULL,
                order_number TEXT,
                amount NUMERIC(12, 2) NOT NULL,
                source_type TEXT NOT NULL
            )
        """))
        connection.execute(sa.text("""
            INSERT INTO vw_orders (
                cost_center, order_number, order_date, customer_name, mobile_number,
                order_amount, recovery_status
            ) VALUES
                ('CC1', 'SALES-PROOF-SHORT', '2026-05-01T09:00:00', 'Alice', '9001', 100, NULL),
                ('CC1', 'PROOF-ONLY-SHORT', '2026-05-01T10:00:00', 'Bob', '9002', 100, NULL),
                ('CC1', 'MISMATCH-SHORT', '2026-05-01T11:00:00', 'Cara', '9003', 100, NULL),
                ('CC1', 'EQUAL-EVIDENCE-SHORT', '2026-05-01T12:00:00', 'Dan', '9004', 200, NULL),
                ('CC1', 'PAID-IN-FULL', '2026-05-01T13:00:00', 'Eve', '9005', 100, NULL)
        """))
        connection.execute(sa.text("""
            INSERT INTO sales (cost_center, order_number, payment_received) VALUES
                ('CC1', 'SALES-PROOF-SHORT', 80),
                ('CC1', 'MISMATCH-SHORT', 90),
                ('CC1', 'EQUAL-EVIDENCE-SHORT', 150),
                ('CC1', 'PAID-IN-FULL', 100)
        """))
        connection.execute(sa.text("""
            INSERT INTO payment_collections (cost_center, order_number, amount, source_type) VALUES
                ('CC1', 'SALES-PROOF-SHORT', 80, 'google_sheet'),
                ('CC1', 'PROOF-ONLY-SHORT', 80, 'google_sheet'),
                ('CC1', 'MISMATCH-SHORT', 80, 'google_sheet'),
                ('CC1', 'EQUAL-EVIDENCE-SHORT', 150, 'google_sheet'),
                ('CC1', 'PAID-IN-FULL', 100, 'google_sheet')
        """))
    engine.dispose()

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
    payment_collections = sa.table(
        "payment_collections",
        sa.column("cost_center"),
        sa.column("order_number"),
        sa.column("amount"),
        sa.column("source_type"),
    )
    sales = sa.table(
        "sales",
        sa.column("cost_center"),
        sa.column("order_number"),
        sa.column("payment_received"),
    )

    async with session_scope(async_url) as session:
        rows = await fetch_short_payment_rows(
            session=session,
            orders=orders,
            payment_collections=payment_collections,
            sales=sales,
            start_datetime=datetime(2026, 5, 1),
            end_datetime=datetime(2026, 5, 2),
        )

    assert [(row.order_number, row.paid_amount, row.shortage_amount) for row in rows] == [
        ("SALES-PROOF-SHORT", Decimal("80.00"), Decimal("20.00")),
        ("EQUAL-EVIDENCE-SHORT", Decimal("150.00"), Decimal("50.00")),
    ]


def test_postgres_view_sql_documents_python_compatibility_contract() -> None:
    assert migration.revision == "0115_canon_missing_view"
    assert migration.down_revision == "0114_payment_audit_canon"

    normalized_sql = " ".join(migration.POSTGRES_VIEW_SQL.split()).lower()

    assert (
        "create or replace view public.vw_orders_missing_in_payment_collections"
        in normalized_sql
    )
    assert "from public.vw_orders as o" in normalized_sql
    assert "sum(coalesce(s.payment_received, 0)) as payment_received" in normalized_sql
    assert "lower(pc.source_type) in ('google_sheet', 'legacy_sales')" in normalized_sql
    assert "regexp_split_to_array" in normalized_sql
    assert "'[/,]+'" in normalized_sql
    assert "sales_payment_received > 0" in normalized_sql
    assert "payment_component_walk" in normalized_sql
    assert "component_status" in normalized_sql
    assert "has_data_quality_exception" in normalized_sql
    assert "+ 1 >=" in normalized_sql
    assert "not has_payment_proof" in normalized_sql
    assert "not has_data_quality_exception" in normalized_sql
    assert "order_amount::numeric(12, 2) as net_amount" in normalized_sql
    assert "to_be_recovered" in normalized_sql
    assert "to_be_compensated" in normalized_sql
    assert "recovered" in normalized_sql
    assert "compensated" in normalized_sql
    assert "write_off" in normalized_sql
    assert "bank_row_id" not in normalized_sql
    assert "payment_status" not in normalized_sql
    assert "payment_amount" not in normalized_sql
