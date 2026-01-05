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
    module_path = project_root / "alembic" / "versions" / "0031_ingest_remarks_td_sales.py"
    spec = importlib.util.spec_from_file_location("v0031_ingest_remarks_td_sales", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load migration module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


migration = _load_migration_module()


def _table_columns(engine: sa.Engine, table_name: str) -> set[str]:
    with engine.connect() as connection:
        inspector = sa.inspect(connection)
        return {column["name"] for column in inspector.get_columns(table_name)}


def _run_migration(connection: sa.Connection, fn: Callable[[], None], monkeypatch: pytest.MonkeyPatch) -> None:
    context = MigrationContext.configure(connection)
    operations = Operations(context)
    original_op = migration.op
    monkeypatch.setattr(migration, "op", operations)
    try:
        fn()
    finally:
        monkeypatch.setattr(migration, "op", original_op)


def test_td_sales_ingest_remarks_migration(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = sa.create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                CREATE TABLE stg_td_sales (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    store_code TEXT,
                    order_number TEXT,
                    ingest_remark TEXT
                )
                """
            )
        )
        connection.execute(
            sa.text(
                """
                CREATE TABLE td_sales (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    store_code TEXT,
                    order_number TEXT,
                    ingest_remarks TEXT,
                    ingest_remark TEXT
                )
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO stg_td_sales (store_code, order_number, ingest_remark)
                VALUES (:store_code, :order_number, :ingest_remark)
                """
            ),
            {"store_code": "A001", "order_number": "S-1", "ingest_remark": "stg remark"},
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO td_sales (store_code, order_number, ingest_remarks, ingest_remark)
                VALUES (:store_code, :order_number, :ingest_remarks, :ingest_remark)
                """
            ),
            {
                "store_code": "A001",
                "order_number": "S-1",
                "ingest_remarks": None,
                "ingest_remark": "final remark",
            },
        )

    assert "ingest_remark" in _table_columns(engine, "stg_td_sales")
    assert "ingest_remark" in _table_columns(engine, "td_sales")

    with engine.begin() as connection:
        _run_migration(connection, migration.upgrade, monkeypatch)

    stg_columns = _table_columns(engine, "stg_td_sales")
    final_columns = _table_columns(engine, "td_sales")
    assert "ingest_remarks" in stg_columns
    assert "ingest_remark" not in stg_columns
    assert "ingest_remarks" in final_columns
    assert "ingest_remark" not in final_columns

    with engine.connect() as connection:
        stg_remark = connection.execute(sa.text("SELECT ingest_remarks FROM stg_td_sales")).scalar_one()
        final_remark = connection.execute(sa.text("SELECT ingest_remarks FROM td_sales")).scalar_one()
    assert stg_remark == "stg remark"
    assert final_remark == "final remark"

    with engine.begin() as connection:
        _run_migration(connection, migration.downgrade, monkeypatch)

    stg_columns_after = _table_columns(engine, "stg_td_sales")
    final_columns_after = _table_columns(engine, "td_sales")
    assert "ingest_remarks" in stg_columns_after
    assert "ingest_remark" not in stg_columns_after
    assert "ingest_remarks" in final_columns_after
    assert "ingest_remark" not in final_columns_after

    with engine.connect() as connection:
        stg_remark_after = connection.execute(sa.text("SELECT ingest_remarks FROM stg_td_sales")).scalar_one()
        final_remark_after = connection.execute(sa.text("SELECT ingest_remarks FROM td_sales")).scalar_one()
    assert stg_remark_after == "stg remark"
    assert final_remark_after == "final remark"
