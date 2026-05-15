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
    module_path = (
        project_root / "alembic" / "versions" / "0110_sales_evidence_mismatch.py"
    )
    spec = importlib.util.spec_from_file_location(
        "v0110_sales_evidence_mismatch", module_path
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load migration module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


migration = _load_migration_module()


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


def test_sales_evidence_mismatch_view_columns(monkeypatch: pytest.MonkeyPatch) -> None:
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
                    ('CC1', 'ORD-MATCH', '2026-05-01', 100),
                    ('CC1', 'ORD-SALES-HIGH', '2026-05-01', 100),
                    ('CC1', 'ORD-EVID-HIGH', '2026-05-01', 100),
                    ('CC1', 'ORD-BOTH-SHORT', '2026-05-01', 100)
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO sales (cost_center, order_number, payment_received)
                VALUES
                    ('CC1', 'ORD-MATCH', 100),
                    ('CC1', 'ORD-SALES-HIGH', 120),
                    ('CC1', 'ORD-EVID-HIGH', 80),
                    ('CC1', 'ORD-BOTH-SHORT', 50)
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
                    (1, 'google_sheet', 101, 'CC1', '2026-05-02', '2026-05-02 10:00:00', 'ORD-MATCH', 100, NULL),
                    (2, 'google_sheet', 102, 'CC1', '2026-05-02', '2026-05-02 10:00:00', 'ORD-SALES-HIGH', 100, NULL),
                    (3, 'google_sheet', 103, 'CC1', '2026-05-02', '2026-05-02 10:00:00', 'ORD-EVID-HIGH', 100, NULL),
                    (4, 'google_sheet', 104, 'CC1', '2026-05-02', '2026-05-02 10:00:00', 'ORD-BOTH-SHORT', 50, NULL)
                """
            )
        )

        _run_migration(connection, migration.upgrade, monkeypatch)

        rows = connection.execute(
            sa.text(
                """
                SELECT
                    order_number,
                    payment_received,
                    grouped_amount,
                    sales_evidence_difference,
                    sales_evidence_mismatch,
                    reconciliation_result
                FROM vw_payment_evidence_reconciliation
                ORDER BY payment_id
                """
            )
        ).mappings().all()

    assert [dict(row) for row in rows] == [
        {
            "order_number": "ORD-MATCH",
            "payment_received": 100,
            "grouped_amount": 100,
            "sales_evidence_difference": 0,
            "sales_evidence_mismatch": False,
            "reconciliation_result": "paid",
        },
        {
            "order_number": "ORD-SALES-HIGH",
            "payment_received": 120,
            "grouped_amount": 100,
            "sales_evidence_difference": 20,
            "sales_evidence_mismatch": True,
            "reconciliation_result": "paid",
        },
        {
            "order_number": "ORD-EVID-HIGH",
            "payment_received": 80,
            "grouped_amount": 100,
            "sales_evidence_difference": -20,
            "sales_evidence_mismatch": True,
            "reconciliation_result": "paid",
        },
        {
            "order_number": "ORD-BOTH-SHORT",
            "payment_received": 50,
            "grouped_amount": 50,
            "sales_evidence_difference": 0,
            "sales_evidence_mismatch": False,
            "reconciliation_result": "short",
        },
    ]


def test_postgres_view_sql_documents_sales_evidence_mismatch_contract() -> None:
    assert migration.revision == "0110_sales_evidence_mismatch"
    assert migration.down_revision == "0109_payment_evidence_audit"

    normalized_sql = " ".join(migration.POSTGRES_VIEW_SQL.split()).lower()

    assert "sales_evidence_difference" in normalized_sql
    assert "sales_evidence_mismatch" in normalized_sql
    assert "coalesce(st.payment_received, 0) - coalesce(pg.evidence_amount, 0)" in normalized_sql
    assert "abs(coalesce(st.payment_received, 0) - coalesce(pg.evidence_amount, 0)) > 1" in normalized_sql
