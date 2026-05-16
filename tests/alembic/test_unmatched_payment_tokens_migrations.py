from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any, Callable

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations


def _load_migration_module(filename: str, module_name: str) -> Any:
    project_root = Path(__file__).resolve().parents[2]
    module_path = project_root / "alembic" / "versions" / filename
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load migration module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


missing_migration = _load_migration_module(
    "0112_missing_unmatched_tokens.py", "v0112_missing_unmatched_tokens"
)
audit_migration = _load_migration_module(
    "0113_audit_unmatched_tokens.py", "v0113_audit_unmatched_tokens"
)


def _run_migration(
    migration: Any,
    connection: sa.Connection,
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


def _create_common_schema(connection: sa.Connection) -> None:
    connection.execute(
        sa.text(
            """
            CREATE TABLE vw_orders (
                cost_center TEXT NOT NULL,
                order_number TEXT NOT NULL,
                order_date TEXT,
                customer_name TEXT,
                mobile_number TEXT,
                order_amount NUMERIC(12, 2) NOT NULL,
                recovery_status TEXT
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
                source_sheet_row INTEGER,
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


def test_audit_view_flags_partially_unmatched_payment_components(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        _create_common_schema(connection)
        connection.execute(
            sa.text(
                """
                INSERT INTO vw_orders (
                    cost_center, order_number, order_date, customer_name,
                    mobile_number, order_amount, recovery_status
                ) VALUES
                    ('CC1', 'ORD1', '2026-05-01', 'One', '9001', 100, NULL),
                    ('CC1', 'ORD2', '2026-05-01', 'Two', '9002', 100, NULL)
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO sales (cost_center, order_number, payment_received)
                VALUES
                    ('CC1', 'ORD1', 100),
                    ('CC1', 'ORD2', 100)
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO payment_collections (
                    payment_id, source_type, source_sheet_row, cost_center,
                    payment_date, payment_timestamp, order_number, amount, bank_row_id
                ) VALUES
                    (1, 'google_sheet', 101, 'CC1', '2026-05-02', '2026-05-02 10:00:00', 'ORD1,TYPO', 100, 'B1'),
                    (2, 'google_sheet', 102, 'CC1', '2026-05-02', '2026-05-02 10:00:00', 'ORD1,ORD2,TYPO', 200, 'B2')
                """
            )
        )

        _run_migration(
            audit_migration, connection, audit_migration.upgrade, monkeypatch
        )
        rows = (
            connection.execute(
                sa.text(
                    """
                SELECT
                    payment_id,
                    normalized_order_tokens_csv,
                    token_count,
                    matched_order_count,
                    reconciliation_result
                FROM vw_payment_evidence_reconciliation
                ORDER BY payment_id
                """
                )
            )
            .mappings()
            .all()
        )

    assert [dict(row) for row in rows] == [
        {
            "payment_id": 1,
            "normalized_order_tokens_csv": "ORD1,TYPO",
            "token_count": 2,
            "matched_order_count": 1,
            "reconciliation_result": "unmatched order token",
        },
        {
            "payment_id": 2,
            "normalized_order_tokens_csv": "ORD1,ORD2,TYPO",
            "token_count": 3,
            "matched_order_count": 2,
            "reconciliation_result": "unmatched order token",
        },
    ]


def test_audit_upgrade_drops_postgres_view_before_recreate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executed: list[str] = []

    class _FakeOp:
        def get_bind(self) -> Any:
            return type(
                "Bind", (), {"dialect": type("Dialect", (), {"name": "postgresql"})()}
            )()

        def execute(self, sql: str) -> None:
            executed.append(sql)

    original_op = audit_migration.op
    monkeypatch.setattr(audit_migration, "op", _FakeOp())
    try:
        audit_migration.upgrade()
    finally:
        monkeypatch.setattr(audit_migration, "op", original_op)

    assert executed[:2] == [
        "DROP VIEW IF EXISTS public.vw_payment_evidence_reconciliation;",
        audit_migration.POSTGRES_VIEW_SQL,
    ]


def test_migration_sql_contracts_document_unmatched_token_bucket() -> None:
    assert missing_migration.revision == "0112_missing_unmatched_tokens"
    assert missing_migration.down_revision == "0111_missing_view_py_logic"
    assert audit_migration.revision == "0113_audit_unmatched_tokens"
    assert audit_migration.down_revision == "0112_missing_unmatched_tokens"

    missing_sql = " ".join(missing_migration.POSTGRES_VIEW_SQL.split()).lower()
    audit_sql = " ".join(audit_migration.POSTGRES_VIEW_SQL.split()).lower()

    assert "payment_token_quality" in missing_sql
    assert "matched_order_count" in missing_sql
    assert "has_data_quality_exception" in missing_sql
    assert "unmatched order token" in audit_sql
    assert "token_count" in audit_sql
    assert "matched_order_count" in audit_sql
