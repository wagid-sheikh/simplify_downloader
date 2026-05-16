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
    module_path = project_root / "alembic" / "versions" / "0116_audit_action_status.py"
    spec = importlib.util.spec_from_file_location(
        "v0116_audit_action_status", module_path
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
                order_amount NUMERIC(12, 2) NOT NULL,
                recovery_status TEXT,
                recovery_category TEXT
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


def test_audit_view_marks_write_off_short_as_non_actionable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")
    with engine.begin() as connection:
        _create_schema(connection)
        connection.execute(sa.text("""
                INSERT INTO vw_orders (
                    cost_center, order_number, order_date, order_amount,
                    recovery_status, recovery_category
                ) VALUES
                    ('CC1', 'WRITE-OFF', '2026-05-01', 100, 'WRITE_OFF', 'write off'),
                    ('CC1', 'ACTIONABLE', '2026-05-01', 100, NULL, NULL)
                """))
        connection.execute(sa.text("""
                INSERT INTO sales (cost_center, order_number, payment_received) VALUES
                    ('CC1', 'WRITE-OFF', 50),
                    ('CC1', 'ACTIONABLE', 50)
                """))
        connection.execute(sa.text("""
                INSERT INTO payment_collections (
                    payment_id, source_type, source_sheet_row, cost_center,
                    payment_date, payment_timestamp, order_number, amount, bank_row_id
                ) VALUES
                    (1, 'google_sheet', 101, 'CC1', '2026-05-02', '2026-05-02 10:00:00', 'WRITE-OFF', 50, NULL),
                    (2, 'google_sheet', 102, 'CC1', '2026-05-02', '2026-05-02 10:01:00', 'ACTIONABLE', 50, NULL)
                """))
        _run_migration(connection, migration.upgrade, monkeypatch)
        rows = {row["payment_id"]: dict(row) for row in connection.execute(sa.text("""
                    SELECT
                        payment_id,
                        reconciliation_result,
                        recovery_statuses_csv,
                        operator_actionable_payment_status
                    FROM vw_payment_evidence_reconciliation
                    ORDER BY payment_id
                    """)).mappings()}

    assert rows[1]["reconciliation_result"] == "short"
    assert rows[1]["recovery_statuses_csv"] == "WRITE_OFF"
    assert (
        rows[1]["operator_actionable_payment_status"]
        == "non_actionable_recovery_status"
    )
    assert rows[2]["reconciliation_result"] == "short"
    assert rows[2]["operator_actionable_payment_status"] == "actionable_short_payment"
