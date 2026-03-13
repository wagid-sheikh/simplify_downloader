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
    module_path = project_root / "alembic" / "versions" / "0073_add_td_compare_threshold.py"
    spec = importlib.util.spec_from_file_location("v0073_add_td_compare_threshold", module_path)
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


def test_td_compare_threshold_verdict_upgrade_and_downgrade(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = sa.create_engine("sqlite://")

    metadata = sa.MetaData()
    sa.Table(
        "td_sync_compare_log",
        metadata,
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("run_env", sa.String(length=32), nullable=False),
        sa.Column("store_code", sa.String(length=8), nullable=False),
        sa.Column("from_date", sa.Date(), nullable=False),
        sa.Column("to_date", sa.Date(), nullable=False),
        sa.Column("source_mode", sa.String(length=16), nullable=False),
    )

    with engine.begin() as connection:
        metadata.create_all(connection)
        _run_migration(connection, migration.upgrade, monkeypatch)

    with engine.connect() as connection:
        inspector = sa.inspect(connection)
        columns = {column["name"] for column in inspector.get_columns("td_sync_compare_log")}
        assert {
            "thresholds_json",
            "threshold_verdict_json",
            "consecutive_pass_windows",
            "api_ready",
        }.issubset(columns)

    with engine.begin() as connection:
        _run_migration(connection, migration.downgrade, monkeypatch)

    with engine.connect() as connection:
        inspector = sa.inspect(connection)
        columns = {column["name"] for column in inspector.get_columns("td_sync_compare_log")}
        assert "thresholds_json" not in columns
        assert "threshold_verdict_json" not in columns
        assert "consecutive_pass_windows" not in columns
        assert "api_ready" not in columns
