from __future__ import annotations

from decimal import Decimal
import importlib.util
from pathlib import Path
from typing import Any, Callable

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations

from app.reports.shared.payment_reconciliation import build_payment_evidence_audit_rows


def _load_migration_module() -> Any:
    project_root = Path(__file__).resolve().parents[2]
    module_path = project_root / "alembic" / "versions" / "0114_payment_audit_canon.py"
    spec = importlib.util.spec_from_file_location(
        "v0114_payment_audit_canon", module_path
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
    connection.execute(
        sa.text("""
        CREATE TABLE vw_orders (
            cost_center TEXT NOT NULL,
            order_number TEXT NOT NULL,
            order_date TEXT,
            customer_name TEXT,
            mobile_number TEXT,
            order_amount NUMERIC(12, 2) NOT NULL,
            recovery_status TEXT,
            recovery_category TEXT
        )
    """)
    )
    connection.execute(
        sa.text("""
        CREATE TABLE sales (
            cost_center TEXT NOT NULL,
            order_number TEXT NOT NULL,
            payment_received NUMERIC(12, 2)
        )
    """)
    )
    connection.execute(
        sa.text("""
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
    """)
    )


def _insert_representative_rows(connection: sa.Connection) -> None:
    connection.execute(
        sa.text("""
        INSERT INTO vw_orders (
            cost_center, order_number, order_date, customer_name, mobile_number,
            order_amount, recovery_status, recovery_category
        ) VALUES
            ('CC1', 'ORD1', '2026-05-01', 'One', '9001', 100, 'TO_BE_RECOVERED', 'OTHER'),
            ('CC1', 'ORD2', '2026-05-02', 'Two', '9002', 100, 'RECOVERED', 'WRITE_OFF_BALANCE'),
            ('CC1', 'ORD3', '2026-05-03', 'Three', '9003', 100, NULL, NULL),
            ('CC1', 'SHORT1', '2026-05-04', 'Short', '9004', 100, NULL, NULL),
            ('CC1', 'NOSALES1', '2026-05-05', 'No Sales', '9005', 100, NULL, NULL)
    """)
    )
    connection.execute(
        sa.text("""
        INSERT INTO sales (cost_center, order_number, payment_received) VALUES
            ('CC1', 'ORD1', 100),
            ('CC1', 'ORD2', 100),
            ('CC1', 'ORD3', 100),
            ('CC1', 'SHORT1', 100)
    """)
    )
    connection.execute(
        sa.text("""
        INSERT INTO payment_collections (
            payment_id, source_type, source_sheet_row, cost_center, payment_date,
            payment_timestamp, order_number, amount, bank_row_id
        ) VALUES
            (1, 'google_sheet', 101, 'CC1', '2026-05-06', '2026-05-06 10:00:00', 'ORD1,ORD2', 150, 'B1'),
            (2, 'google_sheet', 102, 'CC1', '2026-05-06', '2026-05-06 10:05:00', 'ORD2,ORD3', 150, 'B2'),
            (3, 'google_sheet', 103, 'CC1', '2026-05-06', '2026-05-06 10:10:00', 'SHORT1', 80, 'B3'),
            (4, 'google_sheet', 104, 'CC1', '2026-05-06', '2026-05-06 10:15:00', 'NOSALES1', 100, 'B4'),
            (5, 'google_sheet', 105, 'CC1', '2026-05-06', '2026-05-06 10:20:00', 'ORD1,TYPO', 100, 'B5')
    """)
    )


def _source_rows(connection: sa.Connection, table_name: str) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in connection.execute(sa.text(f"SELECT * FROM {table_name}")).mappings()
    ]


def test_audit_view_matches_shared_helper_for_representative_classifications(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        _create_schema(connection)
        _insert_representative_rows(connection)
        expected_by_payment_id = {
            row.payment_id: row.as_dict()
            for row in build_payment_evidence_audit_rows(
                order_rows=_source_rows(connection, "vw_orders"),
                sales_rows=_source_rows(connection, "sales"),
                payment_evidence_rows=_source_rows(connection, "payment_collections"),
            )
        }
        _run_migration(connection, migration.upgrade, monkeypatch)
        rows = (
            connection.execute(
                sa.text("""
            SELECT
                payment_id,
                normalized_order_tokens_csv,
                reconciliation_result,
                group_key,
                component_id,
                grouped_amount,
                grouped_order_amount,
                grouped_payment_received,
                sales_evidence_difference,
                sales_evidence_mismatch,
                sales_evidence_classification,
                recovery_statuses_csv,
                recovery_categories_csv,
                token_count,
                matched_order_count
            FROM vw_payment_evidence_reconciliation
            ORDER BY payment_id
        """)
            )
            .mappings()
            .all()
        )

    for row in rows:
        expected = expected_by_payment_id[row["payment_id"]]
        assert (
            row["normalized_order_tokens_csv"]
            == expected["normalized_order_tokens_csv"]
        )
        assert row["reconciliation_result"] == expected["reconciliation_result"]
        assert row["group_key"] == expected["group_key"]
        assert row["component_id"] == expected["component_id"]
        assert Decimal(str(row["grouped_amount"])) == expected["grouped_amount"]
        assert (
            Decimal(str(row["grouped_order_amount"]))
            == expected["grouped_order_amount"]
        )
        assert (
            Decimal(str(row["grouped_payment_received"]))
            == expected["grouped_payment_received"]
        )
        assert (
            Decimal(str(row["sales_evidence_difference"]))
            == expected["sales_evidence_difference"]
        )
        assert (
            bool(row["sales_evidence_mismatch"]) is expected["sales_evidence_mismatch"]
        )
        assert (
            row["sales_evidence_classification"]
            == expected["sales_evidence_classification"]
        )
        assert row["recovery_statuses_csv"] == expected["recovery_statuses_csv"]
        assert row["recovery_categories_csv"] == expected["recovery_categories_csv"]

    assert [dict(row) for row in rows] == [
        {
            "payment_id": 1,
            "normalized_order_tokens_csv": "ORD1,ORD2,ORD3,TYPO",
            "reconciliation_result": "unmatched order token",
            "group_key": "ORD1|ORD2|ORD3|TYPO",
            "component_id": "CC1|ORD1|ORD2|ORD3|TYPO",
            "grouped_amount": 400,
            "grouped_order_amount": 300,
            "grouped_payment_received": 300,
            "sales_evidence_difference": -100,
            "sales_evidence_mismatch": True,
            "sales_evidence_classification": "evidence higher",
            "recovery_statuses_csv": "RECOVERED,TO_BE_RECOVERED",
            "recovery_categories_csv": "OTHER,WRITE_OFF_BALANCE",
            "token_count": 4,
            "matched_order_count": 3,
        },
        {
            "payment_id": 2,
            "normalized_order_tokens_csv": "ORD1,ORD2,ORD3,TYPO",
            "reconciliation_result": "unmatched order token",
            "group_key": "ORD1|ORD2|ORD3|TYPO",
            "component_id": "CC1|ORD1|ORD2|ORD3|TYPO",
            "grouped_amount": 400,
            "grouped_order_amount": 300,
            "grouped_payment_received": 300,
            "sales_evidence_difference": -100,
            "sales_evidence_mismatch": True,
            "sales_evidence_classification": "evidence higher",
            "recovery_statuses_csv": "RECOVERED,TO_BE_RECOVERED",
            "recovery_categories_csv": "OTHER,WRITE_OFF_BALANCE",
            "token_count": 4,
            "matched_order_count": 3,
        },
        {
            "payment_id": 3,
            "normalized_order_tokens_csv": "SHORT1",
            "reconciliation_result": "short",
            "group_key": "SHORT1",
            "component_id": "CC1|SHORT1",
            "grouped_amount": 80,
            "grouped_order_amount": 100,
            "grouped_payment_received": 100,
            "sales_evidence_difference": 20,
            "sales_evidence_mismatch": True,
            "sales_evidence_classification": "sales higher",
            "recovery_statuses_csv": "",
            "recovery_categories_csv": "",
            "token_count": 1,
            "matched_order_count": 1,
        },
        {
            "payment_id": 4,
            "normalized_order_tokens_csv": "NOSALES1",
            "reconciliation_result": "missing sales",
            "group_key": "NOSALES1",
            "component_id": "CC1|NOSALES1",
            "grouped_amount": 100,
            "grouped_order_amount": 100,
            "grouped_payment_received": 0,
            "sales_evidence_difference": -100,
            "sales_evidence_mismatch": True,
            "sales_evidence_classification": "evidence higher",
            "recovery_statuses_csv": "",
            "recovery_categories_csv": "",
            "token_count": 1,
            "matched_order_count": 1,
        },
        {
            "payment_id": 5,
            "normalized_order_tokens_csv": "ORD1,ORD2,ORD3,TYPO",
            "reconciliation_result": "unmatched order token",
            "group_key": "ORD1|ORD2|ORD3|TYPO",
            "component_id": "CC1|ORD1|ORD2|ORD3|TYPO",
            "grouped_amount": 400,
            "grouped_order_amount": 300,
            "grouped_payment_received": 300,
            "sales_evidence_difference": -100,
            "sales_evidence_mismatch": True,
            "sales_evidence_classification": "evidence higher",
            "recovery_statuses_csv": "RECOVERED,TO_BE_RECOVERED",
            "recovery_categories_csv": "OTHER,WRITE_OFF_BALANCE",
            "token_count": 4,
            "matched_order_count": 3,
        },
    ]


def test_postgres_view_sql_documents_canonical_audit_contract() -> None:
    assert migration.revision == "0114_payment_audit_canon"
    assert migration.down_revision == (
        "0113_audit_unmatched_tokens",
        "0113_conn_pay_audit",
    )

    normalized_sql = " ".join(migration.POSTGRES_VIEW_SQL.split()).lower()

    assert "payment_component_walk" in normalized_sql
    assert "unmatched order token" in normalized_sql
    assert "sales_evidence_classification" in normalized_sql
    assert "recovery_status" in normalized_sql
    assert "recovery_category" in normalized_sql
    assert "component_id" in normalized_sql
