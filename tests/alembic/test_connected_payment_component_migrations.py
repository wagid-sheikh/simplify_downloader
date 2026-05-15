from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any, Callable

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations


def _load_migration_module(filename: str) -> Any:
    project_root = Path(__file__).resolve().parents[2]
    module_path = project_root / "alembic" / "versions" / filename
    spec = importlib.util.spec_from_file_location(filename.replace(".", "_"), module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load migration module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


missing_migration = _load_migration_module("0112_connected_payment_missing.py")
audit_migration = _load_migration_module("0113_connected_payment_audit.py")


def _run_migration(
    connection: sa.Connection,
    migration: Any,
    fn: Callable[[], None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = MigrationContext.configure(connection)
    operations = Operations(context)
    original_op = migration.op
    monkeypatch.setattr(migration, "op", operations)
    try:
        fn()
    finally:
        monkeypatch.setattr(migration, "op", original_op)


def _create_component_fixture(connection: sa.Connection) -> None:
    connection.execute(sa.text("""
        CREATE TABLE vw_orders (
            cost_center TEXT NOT NULL,
            order_number TEXT NOT NULL,
            order_date TEXT,
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
            source_type TEXT NOT NULL,
            source_sheet_row INTEGER,
            cost_center TEXT NOT NULL,
            payment_date TEXT,
            payment_timestamp TEXT,
            order_number TEXT,
            amount NUMERIC(12, 2) NOT NULL,
            bank_row_id TEXT
        )
    """))
    connection.execute(sa.text("""
        INSERT INTO vw_orders (
            cost_center, order_number, order_date, customer_name, mobile_number,
            order_amount, recovery_status
        ) VALUES
            ('CC1', 'ORD1', '2026-05-01', 'One', '9001', 100, NULL),
            ('CC1', 'ORD2', '2026-05-01', 'Two', '9002', 100, NULL),
            ('CC1', 'ORD3', '2026-05-01', 'Three', '9003', 100, NULL),
            ('CC1', 'MISSING', '2026-05-01', 'Missing', '9004', 100, NULL)
    """))
    connection.execute(sa.text("""
        INSERT INTO sales (cost_center, order_number, payment_received) VALUES
            ('CC1', 'ORD1', 100),
            ('CC1', 'ORD2', 100),
            ('CC1', 'ORD3', 100),
            ('CC1', 'MISSING', 100)
    """))
    connection.execute(sa.text("""
        INSERT INTO payment_collections (
            payment_id, source_type, source_sheet_row, cost_center, payment_date,
            payment_timestamp, order_number, amount, bank_row_id
        ) VALUES
            (1, 'google_sheet', 101, 'CC1', '2026-05-02', '2026-05-02 10:00:00', 'ORD1,ORD2', 200, NULL),
            (2, 'google_sheet', 102, 'CC1', '2026-05-02', '2026-05-02 10:05:00', 'ORD2,ORD3', 100, NULL)
    """))


def test_audit_view_uses_connected_components_for_overlapping_groups(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        _create_component_fixture(connection)
        _run_migration(connection, audit_migration, audit_migration.upgrade, monkeypatch)
        rows = connection.execute(sa.text("""
            SELECT
                payment_id,
                normalized_order_tokens_csv,
                reconciliation_result,
                group_key,
                grouped_amount,
                grouped_order_amount,
                grouped_payment_received
            FROM vw_payment_evidence_reconciliation
            ORDER BY payment_id
        """)).mappings().all()

    assert [dict(row) for row in rows] == [
        {
            "payment_id": 1,
            "normalized_order_tokens_csv": "ORD1,ORD2,ORD3",
            "reconciliation_result": "grouped paid",
            "group_key": "ORD1|ORD2|ORD3",
            "grouped_amount": 300,
            "grouped_order_amount": 300,
            "grouped_payment_received": 300,
        },
        {
            "payment_id": 2,
            "normalized_order_tokens_csv": "ORD1,ORD2,ORD3",
            "reconciliation_result": "grouped paid",
            "group_key": "ORD1|ORD2|ORD3",
            "grouped_amount": 300,
            "grouped_order_amount": 300,
            "grouped_payment_received": 300,
        },
    ]


def test_missing_payment_view_uses_component_level_tolerance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        _create_component_fixture(connection)
        _run_migration(connection, missing_migration, missing_migration.upgrade, monkeypatch)
        rows = connection.execute(sa.text("""
            SELECT order_number, net_amount
            FROM vw_orders_missing_in_payment_collections
            ORDER BY order_number
        """)).mappings().all()

    assert [dict(row) for row in rows] == [
        {"order_number": "MISSING", "net_amount": 100},
    ]


def test_postgres_view_sql_documents_connected_component_contract() -> None:
    assert missing_migration.revision == "0112_conn_pay_missing"
    assert missing_migration.down_revision == "0111_missing_view_py_logic"
    assert audit_migration.revision == "0113_conn_pay_audit"
    assert audit_migration.down_revision == "0112_conn_pay_missing"

    missing_sql = " ".join(missing_migration.POSTGRES_VIEW_SQL.split()).lower()
    audit_sql = " ".join(audit_migration.POSTGRES_VIEW_SQL.split()).lower()

    for sql in (missing_sql, audit_sql):
        assert "with recursive" in sql
        assert "payment_edges" in sql
        assert "payment_component_walk" in sql
        assert "component_roots" in sql
        assert "component_tokens" in sql
        assert "payment_components" in sql
        assert "+ 1 >=" in sql
        assert "bank_row_id" not in missing_sql
