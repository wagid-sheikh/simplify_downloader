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
    module_path = project_root / "alembic" / "versions" / "0097_payment_collections.py"
    spec = importlib.util.spec_from_file_location("v0097_payment_collections", module_path)
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


def test_payment_collections_uses_cost_center_for_missing_payments_views(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        _run_migration(connection, migration.upgrade, monkeypatch)

        inspector = sa.inspect(connection)
        columns = {column["name"] for column in inspector.get_columns("payment_collections")}
        assert "cost_center" in columns
        assert "store_code" not in columns

        indexes = {
            index["name"]: tuple(index["column_names"])
            for index in inspector.get_indexes("payment_collections")
        }
        assert indexes["idx_payment_collections_store_date"] == (
            "cost_center",
            "payment_date",
        )

        _run_migration(connection, migration.downgrade, monkeypatch)
        assert not inspector.has_table("payment_collections")


def _load_source_tracking_migration_module():
    project_root = Path(__file__).resolve().parents[2]
    module_path = project_root / "alembic" / "versions" / "0107_payment_collections_sources.py"
    spec = importlib.util.spec_from_file_location("v0107_payment_collections_sources", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load migration module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


source_tracking_migration = _load_source_tracking_migration_module()


def _run_source_tracking_migration(
    connection: sa.Connection, fn: Callable[[], None], monkeypatch: pytest.MonkeyPatch
) -> None:
    context = MigrationContext.configure(connection)
    operations = Operations(context)
    original_op = source_tracking_migration.op
    monkeypatch.setattr(source_tracking_migration, "op", operations)
    try:
        fn()
    finally:
        monkeypatch.setattr(source_tracking_migration, "op", original_op)


def test_payment_collections_source_tracking_migration_adds_backend_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        _run_migration(connection, migration.upgrade, monkeypatch)
        connection.execute(
            sa.text(
                """
                INSERT INTO payment_collections (
                    payment_id, source_sheet_row, payment_timestamp, payment_mode, cost_center,
                    payment_date, order_number, amount, created_at, updated_at
                ) VALUES (
                    1, 1, '2026-05-15 00:00:00', 'cash', 'CC1', '2026-05-15', 'ORD1', 100,
                    '2026-05-15 00:00:00', '2026-05-15 00:00:00'
                )
                """
            )
        )

        _run_source_tracking_migration(connection, source_tracking_migration.upgrade, monkeypatch)

        inspector = sa.inspect(connection)
        columns = {column["name"]: column for column in inspector.get_columns("payment_collections")}
        indexes = {index["name"]: tuple(index["column_names"]) for index in inspector.get_indexes("payment_collections")}
        source_type = connection.execute(sa.text("SELECT source_type FROM payment_collections")).scalar_one()

    assert "bank_row_id" in columns
    assert "source_type" in columns
    assert source_type == "google_sheet"
    assert indexes["idx_payment_collections_bank_row_id"] == ("bank_row_id",)


def test_payment_collections_source_tracking_migration_is_idempotent_for_already_altered_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                CREATE TABLE payment_collections (
                    payment_id INTEGER PRIMARY KEY,
                    source_sheet_row INTEGER NOT NULL,
                    bank_row_id TEXT,
                    source_type TEXT NOT NULL DEFAULT 'google_sheet'
                )
                """
            )
        )
        connection.execute(
            sa.text(
                """
                CREATE INDEX idx_payment_collections_bank_row_id
                ON payment_collections (bank_row_id)
                """
            )
        )

        _run_source_tracking_migration(connection, source_tracking_migration.upgrade, monkeypatch)

        inspector = sa.inspect(connection)
        columns = {column["name"] for column in inspector.get_columns("payment_collections")}
        bank_indexes = [
            index["name"]
            for index in inspector.get_indexes("payment_collections")
            if index["name"] == "idx_payment_collections_bank_row_id"
        ]

    assert {"bank_row_id", "source_type"}.issubset(columns)
    assert bank_indexes == ["idx_payment_collections_bank_row_id"]


def test_payment_collections_source_tracking_migration_handles_postgres_manual_schema() -> None:
    migration_path = Path(__file__).resolve().parents[2] / "alembic" / "versions" / "0107_payment_collections_sources.py"
    migration_text = migration_path.read_text()

    assert "ADD COLUMN IF NOT EXISTS bank_row_id text" in migration_text
    assert "ADD COLUMN IF NOT EXISTS source_type text" in migration_text
    assert "ALTER COLUMN source_type SET NOT NULL" in migration_text
    assert "chk_payment_collections_source_type" in migration_text
    assert "uq_payment_collections_source_type_row" in migration_text
    assert "idx_payment_collections_bank_row_id" in migration_text
    assert "DROP CONSTRAINT %I" in migration_text
