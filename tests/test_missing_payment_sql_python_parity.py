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
    _fetch_payment_rows_for_orders,
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
                ('CC1', 'GROUP-A', '2026-05-01T09:00:00', 'Grouped A', '9001', 100, 'NONE'),
                ('CC1', 'GROUP-B', '2026-05-01T10:00:00', 'Grouped B', '9002', 100, 'NONE'),
                ('CC1', 'TOPUP-A', '2026-05-01T11:00:00', 'Topup A', '9003', 100, 'NONE'),
                ('CC1', 'TOPUP-B', '2026-05-01T12:00:00', 'Topup B', '9004', 200, 'NONE'),
                ('CC1', 'MISSING-PROOF', '2026-05-01T13:00:00', 'Missing Proof', '9005', 120, 'NONE'),
                ('CC1', 'MISSING-SALES', '2026-05-01T14:00:00', 'Missing Sales', '9006', 130, 'NONE'),
                ('CC1', 'RECOVERY-EXCLUDED', '2026-05-01T15:00:00', 'Recovery', '9007', 140, 'WRITE_OFF'),
                ('CC1', 'UNSUPPORTED-PROOF', '2026-05-01T16:00:00', 'Unsupported', '9008', 150, 'NONE'),
                ('CC1', 'MALFORMED-PROOF-TOKEN', '2026-05-01T16:30:00', 'Malformed', '9011', 160, 'NONE'),
                ('CC1', 'AMOUNT-MISMATCH', '2026-05-01T16:45:00', 'Amount', '9012', 100, 'NONE'),
                ('CC1', 'SHORT-A', '2026-05-01T17:00:00', 'Short A', '9009', 100, 'NONE'),
                ('CC1', 'SHORT-B', '2026-05-01T18:00:00', 'Short B', '9010', 200, 'NONE')
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
                ('CC1', 'MALFORMED-PROOF-TOKEN', 160),
                ('CC1', 'AMOUNT-MISMATCH', 80),
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
                (5, 'CC1', 'NOT-MALFORMED-PROOF-TOKEN', 160, 'google_sheet'),
                (6, 'CC1', 'AMOUNT-MISMATCH', 80, 'google_sheet'),
                (7, 'CC1', 'SHORT-A/SHORT-B', 250, 'google_sheet'),
                (8, 'CC1', 'UNRELATED-HISTORY', 999999, 'google_sheet'),
                (9, 'CC2', 'SHORT-A/SHORT-B', 999999, 'google_sheet')
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
        {"order_number": "MALFORMED-PROOF-TOKEN", "net_amount": 160},
        {"order_number": "MISSING-PROOF", "net_amount": 120},
        {"order_number": "UNSUPPORTED-PROOF", "net_amount": 150},
    ]
    assert [(row.order_number, row.order_amount) for row in python_rows] == [
        ("MISSING-PROOF", Decimal("120.00")),
        ("UNSUPPORTED-PROOF", Decimal("150.00")),
        ("MALFORMED-PROOF-TOKEN", Decimal("160.00")),
    ]
    assert [
        (row.order_number, row.paid_amount, row.shortage_amount, row.group_key)
        for row in short_rows
    ] == [
        ("AMOUNT-MISMATCH", Decimal("80.00"), Decimal("20.00"), None),
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
    order_sql = "\n".join(
        str(
            stmt.compile(
                dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
            )
        )
        for stmt in session.statements
        if "vw_orders" in str(stmt)
    )
    assert "vw_orders.recovery_status = 'NONE'" in order_sql
    assert "payment_collections.cost_center IN ('CC1')" in payment_sql
    assert "LIKE" in payment_sql
    assert "ORD123" in payment_sql


@pytest.mark.asyncio
async def test_payment_collection_candidate_query_uses_exact_group_tokens(tmp_path: Path) -> None:
    db_path = tmp_path / "payment_collection_exact_tokens.db"
    sync_url = f"sqlite:///{db_path}"
    async_url = f"sqlite+aiosqlite:///{db_path}"
    engine = sa.create_engine(sync_url)
    with engine.begin() as connection:
        connection.execute(sa.text("""
            CREATE TABLE payment_collections (
                cost_center TEXT NOT NULL,
                order_number TEXT,
                amount NUMERIC(12, 2) NOT NULL,
                source_type TEXT NOT NULL
            )
        """))
        connection.execute(sa.text("""
            INSERT INTO payment_collections (cost_center, order_number, amount, source_type) VALUES
                ('CC1', 'ORD10', 100, 'google_sheet'),
                ('CC1', 'ORD1/ORD2', 200, 'google_sheet'),
                ('CC1', 'ORD1, ORD3', 300, 'google_sheet'),
                ('CC1', 'XORD1', 400, 'google_sheet'),
                ('CC2', 'ORD1', 500, 'google_sheet')
        """))
    engine.dispose()

    payment_collections = sa.table(
        "payment_collections",
        sa.column("cost_center"),
        sa.column("order_number"),
        sa.column("amount"),
        sa.column("source_type"),
    )

    async with session_scope(async_url) as session:
        rows = await _fetch_payment_rows_for_orders(
            session=session,
            payment_collections=payment_collections,
            order_rows=[{"cost_center": "CC1", "order_number": "ORD1"}],
        )

    assert [row["order_number"] for row in rows] == ["ORD1/ORD2", "ORD1, ORD3"]


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
                ('CC1', 'SALES-PROOF-MATCH-SHORT', '2026-05-01T09:00:00', 'Alice', '9001', 100, 'NONE'),
                ('CC1', 'PROOF-ONLY-SHORT', '2026-05-01T10:00:00', 'Bob', '9002', 100, 'NONE'),
                ('CC1', 'SALES-PROOF-MISMATCH-SHORT', '2026-05-01T11:00:00', 'Cara', '9003', 100, 'NONE'),
                ('CC1', 'PROOF-MISSING', '2026-05-01T11:30:00', 'Mina', '9009', 100, 'NONE'),
                ('CC1', 'NULL-STATUS-MISSING', '2026-05-01T11:35:00', 'Nina', '9011', 100, NULL),
                ('CC1', 'CUSTOM-STATUS-MISSING', '2026-05-01T11:40:00', 'Cora', '9012', 100, 'CUSTOM_STATUS'),
                ('CC1', 'TO-BE-RECOVERED-MISSING', '2026-05-01T11:45:00', 'Tara', '9013', 100, 'TO_BE_RECOVERED'),
                ('CC1', 'TO-BE-COMPENSATED-MISSING', '2026-05-01T11:50:00', 'Tom', '9014', 100, 'TO_BE_COMPENSATED'),
                ('CC1', 'RECOVERED-MISSING', '2026-05-01T11:55:00', 'Ria', '9015', 100, 'RECOVERED'),
                ('CC1', 'COMPENSATED-MISSING', '2026-05-01T11:56:00', 'Cam', '9016', 100, 'COMPENSATED'),
                ('CC1', 'WRITE-OFF-MISSING', '2026-05-01T11:57:00', 'Wes', '9017', 100, 'WRITE_OFF'),
                ('CC1', 'PROOF-SHORT-BY-MORE-THAN-TOLERANCE', '2026-05-01T12:00:00', 'Dan', '9004', 200, 'NONE'),
                ('CC1', 'PAID-IN-FULL', '2026-05-01T13:00:00', 'Eve', '9005', 100, 'NONE'),
                ('CC1', 'NULL-STATUS-SHORT', '2026-05-01T13:15:00', 'Nia', '9007', 100, NULL),
                ('CC1', 'CUSTOM-STATUS-SHORT', '2026-05-01T13:30:00', 'Cal', '9008', 100, 'CUSTOM_STATUS'),
                ('CC1', 'TO-BE-RECOVERED-SHORT', '2026-05-01T13:40:00', 'Tia', '9018', 100, 'TO_BE_RECOVERED'),
                ('CC1', 'TO-BE-COMPENSATED-SHORT', '2026-05-01T13:45:00', 'Tim', '9019', 100, 'TO_BE_COMPENSATED'),
                ('CC1', 'RECOVERED-SHORT', '2026-05-01T13:50:00', 'Ray', '9020', 100, 'RECOVERED'),
                ('CC1', 'COMPENSATED-SHORT', '2026-05-01T13:55:00', 'Cat', '9021', 100, 'COMPENSATED'),
                ('CC1', 'WRITE-OFF-SHORT', '2026-05-01T14:00:00', 'Fran', '9006', 100, 'WRITE_OFF')
        """))
        connection.execute(sa.text("""
            INSERT INTO sales (cost_center, order_number, payment_received) VALUES
                ('CC1', 'SALES-PROOF-MATCH-SHORT', 80),
                ('CC1', 'SALES-PROOF-MISMATCH-SHORT', 90),
                ('CC1', 'PROOF-MISSING', 80),
                ('CC1', 'NULL-STATUS-MISSING', 80),
                ('CC1', 'CUSTOM-STATUS-MISSING', 80),
                ('CC1', 'TO-BE-RECOVERED-MISSING', 80),
                ('CC1', 'TO-BE-COMPENSATED-MISSING', 80),
                ('CC1', 'RECOVERED-MISSING', 80),
                ('CC1', 'COMPENSATED-MISSING', 80),
                ('CC1', 'WRITE-OFF-MISSING', 80),
                ('CC1', 'PROOF-SHORT-BY-MORE-THAN-TOLERANCE', 150),
                ('CC1', 'PAID-IN-FULL', 100),
                ('CC1', 'NULL-STATUS-SHORT', 80),
                ('CC1', 'CUSTOM-STATUS-SHORT', 80),
                ('CC1', 'TO-BE-RECOVERED-SHORT', 80),
                ('CC1', 'TO-BE-COMPENSATED-SHORT', 80),
                ('CC1', 'RECOVERED-SHORT', 80),
                ('CC1', 'COMPENSATED-SHORT', 80),
                ('CC1', 'WRITE-OFF-SHORT', 80)
        """))
        connection.execute(sa.text("""
            INSERT INTO payment_collections (cost_center, order_number, amount, source_type) VALUES
                ('CC1', 'SALES-PROOF-MATCH-SHORT', 80, 'google_sheet'),
                ('CC1', 'PROOF-ONLY-SHORT', 80, 'google_sheet'),
                ('CC1', 'SALES-PROOF-MISMATCH-SHORT', 80, 'google_sheet'),
                ('CC1', 'PROOF-SHORT-BY-MORE-THAN-TOLERANCE', 150, 'google_sheet'),
                ('CC1', 'PAID-IN-FULL', 100, 'google_sheet'),
                ('CC1', 'NULL-STATUS-SHORT', 80, 'google_sheet'),
                ('CC1', 'CUSTOM-STATUS-SHORT', 80, 'google_sheet'),
                ('CC1', 'TO-BE-RECOVERED-SHORT', 80, 'google_sheet'),
                ('CC1', 'TO-BE-COMPENSATED-SHORT', 80, 'google_sheet'),
                ('CC1', 'RECOVERED-SHORT', 80, 'google_sheet'),
                ('CC1', 'COMPENSATED-SHORT', 80, 'google_sheet'),
                ('CC1', 'WRITE-OFF-SHORT', 80, 'google_sheet')
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
        missing_rows = await fetch_missing_payment_rows_without_proof(
            session=session,
            orders=orders,
            payment_collections=payment_collections,
            sales=sales,
            start_datetime=datetime(2026, 5, 1),
            end_datetime=datetime(2026, 5, 2),
            row_factory=MissingPaymentReportRow,
        )

    assert [(row.order_number, row.paid_amount, row.shortage_amount) for row in rows] == [
        ("SALES-PROOF-MATCH-SHORT", Decimal("80.00"), Decimal("20.00")),
        (
            "PROOF-SHORT-BY-MORE-THAN-TOLERANCE",
            Decimal("150.00"),
            Decimal("50.00"),
        ),
    ]
    assert [row.order_number for row in missing_rows] == ["PROOF-MISSING"]
    excluded_short_orders = {
        "PROOF-ONLY-SHORT",
        "SALES-PROOF-MISMATCH-SHORT",
        "PROOF-MISSING",
        "NULL-STATUS-SHORT",
        "CUSTOM-STATUS-SHORT",
        "TO-BE-RECOVERED-SHORT",
        "TO-BE-COMPENSATED-SHORT",
        "RECOVERED-SHORT",
        "COMPENSATED-SHORT",
        "WRITE-OFF-SHORT",
    }
    excluded_missing_orders = {
        "NULL-STATUS-MISSING",
        "CUSTOM-STATUS-MISSING",
        "TO-BE-RECOVERED-MISSING",
        "TO-BE-COMPENSATED-MISSING",
        "RECOVERED-MISSING",
        "COMPENSATED-MISSING",
        "WRITE-OFF-MISSING",
    }
    assert {row.order_number for row in rows}.isdisjoint(excluded_short_orders)
    assert {row.order_number for row in missing_rows}.isdisjoint(
        excluded_missing_orders
    )


@pytest.mark.asyncio
async def test_fetch_short_payment_rows_requires_recovery_status_column(tmp_path: Path) -> None:
    db_path = tmp_path / "short_payment_requires_recovery_status.db"
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
                order_amount NUMERIC(12, 2) NOT NULL
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
                order_amount
            ) VALUES
                ('CC1', 'WOULD-BE-SHORT', '2026-05-01T09:00:00', 'Alice', '9001', 100)
        """))
        connection.execute(sa.text("""
            INSERT INTO sales (cost_center, order_number, payment_received) VALUES
                ('CC1', 'WOULD-BE-SHORT', 80)
        """))
        connection.execute(sa.text("""
            INSERT INTO payment_collections (cost_center, order_number, amount, source_type) VALUES
                ('CC1', 'WOULD-BE-SHORT', 80, 'google_sheet')
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
        with pytest.raises(
            RuntimeError,
            match=r"vw_orders\.recovery_status is required for payment reports",
        ):
            await fetch_short_payment_rows(
                session=session,
                orders=orders,
                payment_collections=payment_collections,
                sales=sales,
                start_datetime=datetime(2026, 5, 1),
                end_datetime=datetime(2026, 5, 2),
            )


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
