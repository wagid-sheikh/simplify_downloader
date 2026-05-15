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
    module_path = project_root / "alembic" / "versions" / "0108_missing_payment_reconcile.py"
    spec = importlib.util.spec_from_file_location("v0108_missing_payment_reconcile", module_path)
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


def test_missing_payment_reconciliation_handles_single_grouped_and_recovery_cases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")

    recovery_statuses = [
        "TO_BE_RECOVERED",
        "TO_BE_COMPENSATED",
        "RECOVERED",
        "COMPENSATED",
        "WRITE_OFF",
    ]

    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                CREATE TABLE vw_orders (
                    cost_center TEXT NOT NULL,
                    order_number TEXT NOT NULL,
                    order_date TEXT NOT NULL,
                    customer_name TEXT,
                    mobile_number TEXT,
                    order_amount NUMERIC(12, 2) NOT NULL,
                    payment_status TEXT,
                    payment_amount NUMERIC(12, 2),
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
                    order_number TEXT NOT NULL
                )
                """
            )
        )
        connection.execute(
            sa.text(
                """
                CREATE TABLE payment_collections (
                    payment_id INTEGER PRIMARY KEY,
                    cost_center TEXT NOT NULL,
                    order_number TEXT,
                    amount NUMERIC(12, 2) NOT NULL,
                    source_type TEXT NOT NULL,
                    bank_row_id TEXT
                )
                """
            )
        )
        connection.execute(
            sa.text(
                """
                CREATE VIEW vw_orders_missing_in_payment_collections AS
                SELECT cost_center, order_number, order_date, customer_name, mobile_number, order_amount AS net_amount
                FROM vw_orders
                """
            )
        )

        order_rows = [
            ("CC1", "Single-Paid", "2026-05-01", "Single Paid", "9001", 100, "Pending", None, "NONE"),
            ("CC1", "Single-Missing", "2026-05-01", "Single Missing", "9002", 100, "Paid", 100, "NONE"),
            ("CC1", "Group-A", "2026-05-01", "Group A", "9003", 100, "Pending", None, "NONE"),
            ("CC1", "Group-B", "2026-05-01", "Group B", "9004", 100, "Pending", None, "NONE"),
            ("CC1", "Group-Short-A", "2026-05-01", "Group Short A", "9005", 100, "Pending", None, "NONE"),
            ("CC1", "Group-Short-B", "2026-05-01", "Group Short B", "9006", 100, "Pending", None, "NONE"),
            ("CC1", "Unsupported-Source", "2026-05-01", "Unsupported Source", "9007", 100, "Pending", None, "NONE"),
            *[
                (
                    "CC1",
                    f"Recovery-{status}",
                    "2026-05-01",
                    f"Recovery {status}",
                    f"91{index:02d}",
                    100,
                    "Pending",
                    None,
                    status,
                )
                for index, status in enumerate(recovery_statuses, start=1)
            ],
        ]
        connection.execute(
            sa.text(
                """
                INSERT INTO vw_orders (
                    cost_center, order_number, order_date, customer_name, mobile_number,
                    order_amount, payment_status, payment_amount, recovery_status
                ) VALUES (
                    :cost_center, :order_number, :order_date, :customer_name, :mobile_number,
                    :order_amount, :payment_status, :payment_amount, :recovery_status
                )
                """
            ),
            [
                {
                    "cost_center": row[0],
                    "order_number": row[1],
                    "order_date": row[2],
                    "customer_name": row[3],
                    "mobile_number": row[4],
                    "order_amount": row[5],
                    "payment_status": row[6],
                    "payment_amount": row[7],
                    "recovery_status": row[8],
                }
                for row in order_rows
            ],
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO sales (cost_center, order_number)
                VALUES (:cost_center, :order_number)
                """
            ),
            [{"cost_center": row[0], "order_number": row[1]} for row in order_rows],
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO payment_collections (
                    payment_id, cost_center, order_number, amount, source_type, bank_row_id
                ) VALUES (
                    :payment_id, :cost_center, :order_number, :amount, :source_type, :bank_row_id
                )
                """
            ),
            [
                {
                    "payment_id": 1,
                    "cost_center": "CC1",
                    "order_number": " single-paid ",
                    "amount": 100,
                    "source_type": "legacy_sales",
                    "bank_row_id": "BANK-IGNORED-1",
                },
                {
                    "payment_id": 2,
                    "cost_center": "CC1",
                    "order_number": " group-a / GROUP-B ",
                    "amount": 200,
                    "source_type": "google_sheet",
                    "bank_row_id": None,
                },
                {
                    "payment_id": 3,
                    "cost_center": "CC1",
                    "order_number": " group-short-a, GROUP-SHORT-B ",
                    "amount": 150,
                    "source_type": "google_sheet",
                    "bank_row_id": None,
                },
                {
                    "payment_id": 4,
                    "cost_center": "CC1",
                    "order_number": "Unsupported-Source",
                    "amount": 100,
                    "source_type": "bank_statement",
                    "bank_row_id": "BANK-IGNORED-2",
                },
            ],
        )

        _run_migration(connection, migration.upgrade, monkeypatch)

        rows = connection.execute(
            sa.text(
                """
                SELECT order_number, net_amount
                FROM vw_orders_missing_in_payment_collections
                ORDER BY order_number
                """
            )
        ).mappings().all()

    assert [dict(row) for row in rows] == [
        {"order_number": "Group-Short-A", "net_amount": 100},
        {"order_number": "Group-Short-B", "net_amount": 100},
        {"order_number": "Single-Missing", "net_amount": 100},
        {"order_number": "Unsupported-Source", "net_amount": 100},
    ]


def test_postgres_view_sql_documents_reconciliation_contract() -> None:
    assert migration.revision == "0108_missing_pay_reconcile"
    assert migration.down_revision == "0107_payment_coll_sources"

    normalized_sql = " ".join(migration.POSTGRES_VIEW_SQL.split()).lower()

    assert "from public.vw_orders as o" in normalized_sql
    assert "pc.source_type in ('google_sheet', 'legacy_sales')" in normalized_sql
    assert "regexp_split_to_array" in normalized_sql
    assert "'[/,]+'" in normalized_sql
    assert "upper(" in normalized_sql
    assert "single_payment_tokens" in normalized_sql
    assert "multi_payment_groups" in normalized_sql
    assert "group_paid_tokens" in normalized_sql
    assert "not group_paid" in normalized_sql
    assert "order_amount::numeric(12, 2) as net_amount" in normalized_sql
    assert "not (single_paid_amount + 1 >= order_amount)" in normalized_sql
    assert "to_be_recovered" in normalized_sql
    assert "to_be_compensated" in normalized_sql
    assert "recovered" in normalized_sql
    assert "compensated" in normalized_sql
    assert "write_off" in normalized_sql
    assert "bank_row_id" not in normalized_sql
    assert "payment_status" not in normalized_sql
    assert "payment_amount" not in normalized_sql
