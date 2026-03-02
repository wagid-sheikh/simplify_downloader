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
        project_root / "alembic" / "versions" / "0074_relax_td_garment_line_item_uid_uniqueness.py"
    )
    spec = importlib.util.spec_from_file_location("v0074_relax_td_garment_line_item_uid_uniqueness", module_path)
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
    connection.execute(
        sa.text(
            """
            CREATE TABLE stg_td_garments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                store_code TEXT NOT NULL,
                line_item_uid TEXT NOT NULL
            )
            """
        )
    )
    connection.execute(
        sa.text(
            "CREATE UNIQUE INDEX uq_stg_td_garments_store_line_item_uid "
            "ON stg_td_garments (store_code, line_item_uid)"
        )
    )
    connection.execute(
        sa.text(
            """
            CREATE TABLE order_line_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cost_center TEXT NOT NULL,
                line_item_uid TEXT NOT NULL
            )
            """
        )
    )
    connection.execute(
        sa.text(
            "CREATE UNIQUE INDEX uq_order_line_items_cost_center_line_item_uid "
            "ON order_line_items (cost_center, line_item_uid)"
        )
    )


def test_td_garment_uid_relaxation_upgrade_and_downgrade(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        _create_base_tables(connection)
        connection.execute(
            sa.text(
                "INSERT INTO stg_td_garments (store_code, line_item_uid) VALUES "
                "('S001', 'uid-1'), ('S001', 'uid-2')"
            )
        )
        connection.execute(
            sa.text(
                "INSERT INTO order_line_items (cost_center, line_item_uid) VALUES "
                "('A001', 'uid-1'), ('A001', 'uid-2')"
            )
        )
        _run_migration(connection, migration.upgrade, monkeypatch)

    with engine.connect() as connection:
        inspector = sa.inspect(connection)
        stg_columns = {column["name"] for column in inspector.get_columns("stg_td_garments")}
        order_columns = {column["name"] for column in inspector.get_columns("order_line_items")}
        assert "ingest_row_seq" in stg_columns
        assert "ingest_row_seq" in order_columns

        stg_indexes = {index["name"] for index in inspector.get_indexes("stg_td_garments")}
        order_indexes = {index["name"] for index in inspector.get_indexes("order_line_items")}
        assert "uq_stg_td_garments_store_line_item_uid" not in stg_indexes
        assert "uq_order_line_items_cost_center_line_item_uid" not in order_indexes

        stg_seq_nulls = connection.execute(
            sa.text("SELECT COUNT(*) FROM stg_td_garments WHERE ingest_row_seq IS NULL")
        ).scalar_one()
        order_seq_nulls = connection.execute(
            sa.text("SELECT COUNT(*) FROM order_line_items WHERE ingest_row_seq IS NULL")
        ).scalar_one()
        assert stg_seq_nulls == 0
        assert order_seq_nulls == 0

    with engine.begin() as connection:
        _run_migration(connection, migration.downgrade, monkeypatch)

    with engine.connect() as connection:
        inspector = sa.inspect(connection)
        stg_columns_after = {column["name"] for column in inspector.get_columns("stg_td_garments")}
        order_columns_after = {column["name"] for column in inspector.get_columns("order_line_items")}
        assert "ingest_row_seq" not in stg_columns_after
        assert "ingest_row_seq" not in order_columns_after

        stg_indexes_after = {index["name"] for index in inspector.get_indexes("stg_td_garments")}
        order_indexes_after = {index["name"] for index in inspector.get_indexes("order_line_items")}
        assert "uq_stg_td_garments_store_line_item_uid" in stg_indexes_after
        assert "uq_order_line_items_cost_center_line_item_uid" in order_indexes_after
