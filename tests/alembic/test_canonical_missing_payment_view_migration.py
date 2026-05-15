from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any, Callable

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations


def _load_migration_module() -> Any:
    project_root = Path(__file__).resolve().parents[2]
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


def _create_schema(connection: sa.Connection) -> None:
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
        CREATE VIEW vw_orders_missing_in_payment_collections AS
        SELECT cost_center, order_number, date(order_date) AS order_date,
               customer_name, mobile_number, order_amount AS net_amount
        FROM vw_orders
    """))


def _insert_regression_fixture(connection: sa.Connection) -> None:
    connection.execute(sa.text("""
        INSERT INTO vw_orders (
            cost_center, order_number, order_date, customer_name, mobile_number,
            order_amount, recovery_status
        ) VALUES
            ('CC1', 'ORD1', '2026-05-01', 'One', '9001', 100, NULL),
            ('CC1', 'ORD2', '2026-05-01', 'Two', '9002', 100, NULL),
            ('CC1', 'ORD3', '2026-05-01', 'Three', '9003', 100, NULL),
            ('CC1', 'SALES-PAID-NO-PROOF', '2026-05-01', 'Missing', '9004', 120, NULL),
            ('CC1', 'NO-SALES-NO-PROOF', '2026-05-01', 'No Sales', '9005', 130, NULL),
            ('CC1', 'RECOVERY-EXCLUDED', '2026-05-01', 'Recovery', '9006', 140, 'WRITE_OFF'),
            ('CC1', 'UNSUPPORTED-SOURCE', '2026-05-01', 'Unsupported', '9007', 150, NULL)
    """))
    connection.execute(sa.text("""
        INSERT INTO sales (cost_center, order_number, payment_received) VALUES
            ('CC1', 'ORD1', 100),
            ('CC1', 'ORD2', 100),
            ('CC1', 'ORD3', 100),
            ('CC1', 'SALES-PAID-NO-PROOF', 120),
            ('CC1', 'RECOVERY-EXCLUDED', 140),
            ('CC1', 'UNSUPPORTED-SOURCE', 150)
    """))
    connection.execute(sa.text("""
        INSERT INTO payment_collections (
            payment_id, source_type, source_sheet_row, cost_center, payment_date,
            payment_timestamp, order_number, amount, bank_row_id
        ) VALUES
            (1, 'google_sheet', 101, 'CC1', '2026-05-02', '2026-05-02 10:00:00', 'ORD1,ORD2', 200, 'IGNORED-1'),
            (2, 'legacy_sales', 102, 'CC1', '2026-05-02', '2026-05-02 10:05:00', 'ORD2,ORD3', 100, 'IGNORED-2'),
            (3, 'google_sheet', 103, 'CC1', '2026-05-02', '2026-05-02 10:10:00', 'ORD1,TYPO', 100, 'IGNORED-3'),
            (4, 'bank_statement', 104, 'CC1', '2026-05-02', '2026-05-02 10:15:00', 'UNSUPPORTED-SOURCE', 150, 'IGNORED-4')
    """))


def test_final_missing_payment_view_combines_component_and_quarantine_rules(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        _create_schema(connection)
        _insert_regression_fixture(connection)
        _run_migration(connection, migration.upgrade, monkeypatch)
        rows = connection.execute(sa.text("""
            SELECT order_number, net_amount
            FROM vw_orders_missing_in_payment_collections
            ORDER BY order_number
        """)).mappings().all()

    assert [dict(row) for row in rows] == [
        {"order_number": "SALES-PAID-NO-PROOF", "net_amount": 120},
        {"order_number": "UNSUPPORTED-SOURCE", "net_amount": 150},
    ]


def test_post_0114_canonical_missing_view_sql_contract() -> None:
    assert migration.revision == "0115_canon_missing_view"
    assert migration.down_revision == "0114_payment_audit_canon"

    normalized_sql = " ".join(migration.POSTGRES_VIEW_SQL.split()).lower()

    assert (
        "create or replace view public.vw_orders_missing_in_payment_collections"
        in normalized_sql
    )
    assert "with recursive" in normalized_sql
    assert "payment_edges" in normalized_sql
    assert "payment_component_walk" in normalized_sql
    assert "component_roots" in normalized_sql
    assert "payment_components" in normalized_sql
    assert "has_data_quality_exception" in normalized_sql
    assert "lower(pc.source_type) in ('google_sheet', 'legacy_sales')" in normalized_sql
    assert "sales_payment_received > 0" in normalized_sql
    assert "not has_payment_proof" in normalized_sql
    assert "not has_data_quality_exception" in normalized_sql
    assert "+ 1 >=" in normalized_sql
    assert "to_be_recovered" in normalized_sql
    assert "to_be_compensated" in normalized_sql
    assert "recovered" in normalized_sql
    assert "compensated" in normalized_sql
    assert "write_off" in normalized_sql
    assert "bank_row_id" not in normalized_sql
    assert "payment_status" not in normalized_sql
    assert "payment_amount" not in normalized_sql
