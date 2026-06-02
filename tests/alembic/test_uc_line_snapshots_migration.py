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
    module_path = project_root / "alembic" / "versions" / "0122_uc_line_snapshots.py"
    spec = importlib.util.spec_from_file_location("v0122_uc_line_snapshots", module_path)
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


def _create_base_tables(connection: sa.Connection) -> None:
    connection.execute(sa.text("""
        CREATE TABLE order_line_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER,
            order_number TEXT NOT NULL
        )
    """))
    connection.execute(sa.text("""
        CREATE TABLE stg_uc_archive_order_details (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            store_code TEXT,
            order_code TEXT,
            line_hash TEXT
        )
    """))
    connection.execute(sa.text("""
        CREATE UNIQUE INDEX uq_stg_uc_archive_order_details_store_order_line
        ON stg_uc_archive_order_details (store_code, order_code, line_hash)
    """))


def test_uc_line_snapshot_upgrade_is_forward_only_and_preserves_duplicate_capacity(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = sa.create_engine("sqlite://")
    with engine.begin() as connection:
        _create_base_tables(connection)
        _run_migration(connection, migration.upgrade, monkeypatch)

        inspector = sa.inspect(connection)
        order_columns = {column["name"] for column in inspector.get_columns("order_line_items")}
        stg_columns = {column["name"] for column in inspector.get_columns("stg_uc_archive_order_details")}
        stg_indexes = {index["name"] for index in inspector.get_indexes("stg_uc_archive_order_details")}

        assert "line_sequence" in order_columns
        assert "ingest_row_seq" in stg_columns
        assert "stg_uc_order_detail_snapshots" in inspector.get_table_names()
        assert "uq_stg_uc_archive_order_details_store_order_line" not in stg_indexes

        connection.execute(sa.text("""
            INSERT INTO stg_uc_archive_order_details (run_id, store_code, order_code, line_hash, ingest_row_seq)
            VALUES
                ('run-1', 'UC567', 'ORD-1', 'same-hash', 1),
                ('run-1', 'UC567', 'ORD-1', 'same-hash', 2)
        """))
        duplicate_count = connection.execute(
            sa.text("SELECT COUNT(*) FROM stg_uc_archive_order_details WHERE line_hash='same-hash'")
        ).scalar_one()
        assert duplicate_count == 2

        connection.execute(sa.text("""
            INSERT INTO stg_uc_order_detail_snapshots
            (run_id, store_code, order_code, normalized_order_number, snapshot_outcome, detail_row_count)
            VALUES ('run-1', 'UC567', 'ORD-1', 'ORD-1', 'complete_empty', 0)
        """))

        _run_migration(connection, migration.downgrade, monkeypatch)
        inspector_after = sa.inspect(connection)
        assert "line_sequence" in {column["name"] for column in inspector_after.get_columns("order_line_items")}
        assert "stg_uc_order_detail_snapshots" in inspector_after.get_table_names()
