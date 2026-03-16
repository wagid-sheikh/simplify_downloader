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
    module_path = project_root / "alembic" / "versions" / "0078_store_scope_status_subject_template.py"
    spec = importlib.util.spec_from_file_location("v0078_store_scope_status_subject_template", module_path)
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


def _subject_by_template_id(connection: sa.Connection) -> dict[int, str]:
    rows = connection.execute(sa.text("SELECT id, subject_template FROM email_templates ORDER BY id")).fetchall()
    return {int(row[0]): str(row[1]) for row in rows}


def test_store_scope_status_subject_template_upgrade_and_downgrade(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        connection.execute(sa.text("CREATE TABLE pipelines (id INTEGER PRIMARY KEY, code TEXT)"))
        connection.execute(
            sa.text(
                "CREATE TABLE notification_profiles (id INTEGER PRIMARY KEY, pipeline_id INTEGER, scope TEXT, is_active BOOLEAN)"
            )
        )
        connection.execute(
            sa.text(
                "CREATE TABLE email_templates (id INTEGER PRIMARY KEY, profile_id INTEGER, subject_template TEXT, is_active BOOLEAN)"
            )
        )

        connection.execute(
            sa.text(
                """
                INSERT INTO pipelines (id, code) VALUES
                  (1, 'td_orders_sync'),
                  (2, 'uc_orders_sync'),
                  (3, 'bank_sync')
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO notification_profiles (id, pipeline_id, scope, is_active) VALUES
                  (11, 1, 'store', 1),
                  (12, 2, 'store', 1),
                  (13, 1, 'run', 1),
                  (14, 2, 'store', 0),
                  (15, 3, 'store', 1)
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO email_templates (id, profile_id, subject_template, is_active) VALUES
                  (101, 11, 'TD Orders Sync – {{ store_code }}', 1),
                  (102, 12, 'UC Orders Sync – {{ store_code }}', 1),
                  (103, 13, 'td run subject', 1),
                  (104, 14, 'inactive profile subject', 1),
                  (105, 15, 'bank subject', 1),
                  (106, 12, 'inactive template subject', 0)
                """
            )
        )

    with engine.begin() as connection:
        _run_migration(connection, migration.upgrade, monkeypatch)
        upgraded = _subject_by_template_id(connection)

    assert upgraded[101] == migration.NEW_SUBJECT_TEMPLATE
    assert upgraded[102] == migration.NEW_SUBJECT_TEMPLATE
    assert upgraded[103] == "td run subject"
    assert upgraded[104] == "inactive profile subject"
    assert upgraded[105] == "bank subject"
    assert upgraded[106] == "inactive template subject"

    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "UPDATE email_templates SET subject_template = 'manually changed after upgrade' WHERE id = 101"
            )
        )
        _run_migration(connection, migration.downgrade, monkeypatch)
        downgraded = _subject_by_template_id(connection)

    assert downgraded[101] == "manually changed after upgrade"
    assert downgraded[102] == "UC Orders Sync – {{ store_code }}"
    assert downgraded[103] == "td run subject"
