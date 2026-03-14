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
    module_path = project_root / "alembic" / "versions" / "0067_add_uc_archive_staging_tables.py"
    spec = importlib.util.spec_from_file_location("v0067_add_uc_archive_staging_tables", module_path)
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


def test_uc_archive_staging_tables_upgrade_and_downgrade(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        _run_migration(connection, migration.upgrade, monkeypatch)

    with engine.connect() as connection:
        inspector = sa.inspect(connection)
        tables = set(inspector.get_table_names())
        assert "stg_uc_archive_orders_base" in tables
        assert "stg_uc_archive_order_details" in tables
        assert "stg_uc_archive_payment_details" in tables

        orders_base_columns = {column["name"] for column in inspector.get_columns("stg_uc_archive_orders_base")}
        assert {"cost_center", "store_code", "ingest_remarks", "order_code", "pickup_raw", "source_file"}.issubset(
            orders_base_columns
        )

        details_columns = {column["name"] for column in inspector.get_columns("stg_uc_archive_order_details")}
        assert {"order_code", "order_datetime_raw", "pickup_datetime_raw", "line_hash", "source_file"}.issubset(
            details_columns
        )

        payment_columns = {column["name"] for column in inspector.get_columns("stg_uc_archive_payment_details")}
        assert {"order_code", "payment_mode", "payment_date_raw", "transaction_id", "source_file"}.issubset(
            payment_columns
        )

        index_names = {index["name"] for index in inspector.get_indexes("stg_uc_archive_orders_base")}
        assert "uq_stg_uc_archive_orders_base_store_order" in index_names

        index_names = {index["name"] for index in inspector.get_indexes("stg_uc_archive_order_details")}
        assert "uq_stg_uc_archive_order_details_store_order_line" in index_names

        # Expression index may not be fully introspected in SQLite via get_indexes,
        # so verify directly from sqlite_master.
        sql = connection.execute(
            sa.text(
                """
                SELECT sql
                FROM sqlite_master
                WHERE type = 'index'
                  AND name = 'uq_stg_uc_archive_payment_details_idempotency'
                """
            )
        ).scalar_one()
        assert "COALESCE(transaction_id, '')" in sql

    with engine.begin() as connection:
        _run_migration(connection, migration.downgrade, monkeypatch)

    with engine.connect() as connection:
        inspector = sa.inspect(connection)
        tables_after = set(inspector.get_table_names())
        assert "stg_uc_archive_orders_base" not in tables_after
        assert "stg_uc_archive_order_details" not in tables_after
        assert "stg_uc_archive_payment_details" not in tables_after
