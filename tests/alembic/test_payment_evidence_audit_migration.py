from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Callable

import pytest
import sqlalchemy as sa

from alembic.migration import MigrationContext
from alembic.operations import Operations


def _load_migration_module():
    project_root = Path(__file__).resolve().parents[2]
    module_path = project_root / "alembic" / "versions" / "0109_payment_evidence_audit.py"
    spec = importlib.util.spec_from_file_location("v0109_payment_evidence_audit", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load migration module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


migration = _load_migration_module()


def _run_migration(connection: sa.Connection, fn: Callable[[], None], monkeypatch: pytest.MonkeyPatch) -> None:
    context = MigrationContext.configure(connection)
    operations = Operations(context)
    original_op = migration.op
    monkeypatch.setattr(migration, "op", operations)
    try:
        fn()
    finally:
        monkeypatch.setattr(migration, "op", original_op)


def test_payment_evidence_audit_view_reports_reconciliation_results(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                CREATE TABLE vw_orders (
                    cost_center TEXT NOT NULL,
                    order_number TEXT NOT NULL,
                    order_date TEXT,
                    order_amount NUMERIC(12, 2) NOT NULL
                )
                """
            )
        )
        connection.execute(
            sa.text(
                """
                CREATE TABLE sales (
                    cost_center TEXT NOT NULL,
                    order_number TEXT NOT NULL,
                    payment_received NUMERIC(12, 2)
                )
                """
            )
        )
        connection.execute(
            sa.text(
                """
                CREATE TABLE payment_collections (
                    payment_id INTEGER PRIMARY KEY,
                    source_type TEXT NOT NULL,
                    source_sheet_row INTEGER NOT NULL,
                    cost_center TEXT NOT NULL,
                    payment_date TEXT,
                    payment_timestamp TEXT,
                    order_number TEXT,
                    amount NUMERIC(12, 2) NOT NULL,
                    bank_row_id TEXT
                )
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO vw_orders (cost_center, order_number, order_date, order_amount)
                VALUES
                    ('CC1', 'ORD-PAID', '2026-05-01', 100),
                    ('CC1', 'ORD-SHORT', '2026-05-01', 100),
                    ('CC1', 'ORD-NOSALES', '2026-05-01', 100),
                    ('CC1', 'GRP-A', '2026-05-01', 100),
                    ('CC1', 'GRP-B', '2026-05-01', 100),
                    ('CC1', 'GRP-C', '2026-05-01', 100),
                    ('CC1', 'GRP-D', '2026-05-01', 100)
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO sales (cost_center, order_number, payment_received)
                VALUES
                    ('CC1', 'ORD-PAID', 100),
                    ('CC1', 'ORD-SHORT', 100),
                    ('CC1', 'GRP-A', 100),
                    ('CC1', 'GRP-B', 100),
                    ('CC1', 'GRP-C', 100),
                    ('CC1', 'GRP-D', 100)
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO payment_collections (
                    payment_id, source_type, source_sheet_row, cost_center, payment_date,
                    payment_timestamp, order_number, amount, bank_row_id
                )
                VALUES
                    (1, 'google_sheet', 101, 'CC1', '2026-05-02', '2026-05-02 10:00:00', ' ord-paid ', 99, 'BANK-1'),
                    (2, 'google_sheet', 102, 'CC1', '2026-05-02', '2026-05-02 10:00:00', 'ORD-SHORT', 50, NULL),
                    (3, 'google_sheet', 103, 'CC1', '2026-05-02', '2026-05-02 10:00:00', 'ORD-NOSALES', 100, NULL),
                    (4, 'google_sheet', 104, 'CC1', '2026-05-02', '2026-05-02 10:00:00', 'NO-ORDER', 100, NULL),
                    (5, 'legacy_sales', 105, 'CC1', '2026-05-02', '2026-05-02 10:00:00', 'GRP-A / grp-b', 200, NULL),
                    (6, 'google_sheet', 106, 'CC1', '2026-05-02', '2026-05-02 10:00:00', 'GRP-C, GRP-D', 150, NULL),
                    (7, 'google_sheet', 107, 'CC1', '2026-05-02', '2026-05-02 10:00:00', '', 10, NULL)
                """
            )
        )

        _run_migration(connection, migration.upgrade, monkeypatch)

        rows = connection.execute(
            sa.text(
                """
                SELECT
                    payment_id,
                    source_type,
                    source_sheet_row,
                    cost_center,
                    order_number,
                    normalized_order_tokens_csv,
                    amount,
                    order_amount,
                    payment_received,
                    reconciliation_result,
                    is_grouped,
                    bank_row_id
                FROM vw_payment_evidence_reconciliation
                ORDER BY payment_id
                """
            )
        ).mappings().all()

    assert [dict(row) for row in rows] == [
        {
            "payment_id": 1,
            "source_type": "google_sheet",
            "source_sheet_row": 101,
            "cost_center": "CC1",
            "order_number": " ord-paid ",
            "normalized_order_tokens_csv": "ORD-PAID",
            "amount": 99,
            "order_amount": 100,
            "payment_received": 100,
            "reconciliation_result": "paid",
            "is_grouped": False,
            "bank_row_id": "BANK-1",
        },
        {
            "payment_id": 2,
            "source_type": "google_sheet",
            "source_sheet_row": 102,
            "cost_center": "CC1",
            "order_number": "ORD-SHORT",
            "normalized_order_tokens_csv": "ORD-SHORT",
            "amount": 50,
            "order_amount": 100,
            "payment_received": 100,
            "reconciliation_result": "short",
            "is_grouped": False,
            "bank_row_id": None,
        },
        {
            "payment_id": 3,
            "source_type": "google_sheet",
            "source_sheet_row": 103,
            "cost_center": "CC1",
            "order_number": "ORD-NOSALES",
            "normalized_order_tokens_csv": "ORD-NOSALES",
            "amount": 100,
            "order_amount": 100,
            "payment_received": 0,
            "reconciliation_result": "missing sales",
            "is_grouped": False,
            "bank_row_id": None,
        },
        {
            "payment_id": 4,
            "source_type": "google_sheet",
            "source_sheet_row": 104,
            "cost_center": "CC1",
            "order_number": "NO-ORDER",
            "normalized_order_tokens_csv": "NO-ORDER",
            "amount": 100,
            "order_amount": 0,
            "payment_received": 0,
            "reconciliation_result": "missing order token",
            "is_grouped": False,
            "bank_row_id": None,
        },
        {
            "payment_id": 5,
            "source_type": "legacy_sales",
            "source_sheet_row": 105,
            "cost_center": "CC1",
            "order_number": "GRP-A / grp-b",
            "normalized_order_tokens_csv": "GRP-A,GRP-B",
            "amount": 200,
            "order_amount": 200,
            "payment_received": 200,
            "reconciliation_result": "grouped paid",
            "is_grouped": True,
            "bank_row_id": None,
        },
        {
            "payment_id": 6,
            "source_type": "google_sheet",
            "source_sheet_row": 106,
            "cost_center": "CC1",
            "order_number": "GRP-C, GRP-D",
            "normalized_order_tokens_csv": "GRP-C,GRP-D",
            "amount": 150,
            "order_amount": 200,
            "payment_received": 200,
            "reconciliation_result": "grouped short",
            "is_grouped": True,
            "bank_row_id": None,
        },
        {
            "payment_id": 7,
            "source_type": "google_sheet",
            "source_sheet_row": 107,
            "cost_center": "CC1",
            "order_number": "",
            "normalized_order_tokens_csv": "",
            "amount": 10,
            "order_amount": 0,
            "payment_received": 0,
            "reconciliation_result": "missing order token",
            "is_grouped": False,
            "bank_row_id": None,
        },
    ]


def test_postgres_view_sql_documents_audit_contract() -> None:
    assert migration.revision == "0109_payment_evidence_audit"
    assert migration.down_revision == "0108_missing_pay_reconcile"

    normalized_sql = " ".join(migration.POSTGRES_VIEW_SQL.split()).lower()

    assert "create or replace view public.vw_payment_evidence_reconciliation" in normalized_sql
    assert "pc.payment_id" in normalized_sql
    assert "pc.source_type" in normalized_sql
    assert "pc.source_sheet_row" in normalized_sql
    assert "pc.cost_center" in normalized_sql
    assert "pc.order_number" in normalized_sql
    assert "normalized_order_tokens" in normalized_sql
    assert "pb.amount::numeric(12, 2) as amount" in normalized_sql
    assert "join public.vw_orders as o" in normalized_sql
    assert "s.payment_received" in normalized_sql
    assert "pc.bank_row_id" in normalized_sql
    assert "missing order token" in normalized_sql
    assert "missing sales" in normalized_sql
    assert "grouped paid" in normalized_sql
    assert "grouped short" in normalized_sql
