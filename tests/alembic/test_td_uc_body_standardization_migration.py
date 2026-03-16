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
    module_path = project_root / "alembic" / "versions" / "0077_standardize_td_uc_bodytempl.py"
    spec = importlib.util.spec_from_file_location("v0077_standardize_td_uc_bodytempl", module_path)
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


def _body_by_template_id(connection: sa.Connection) -> dict[int, str]:
    rows = connection.execute(sa.text("SELECT id, body_template FROM email_templates ORDER BY id")).fetchall()
    return {int(row[0]): str(row[1]) for row in rows}


def test_td_uc_body_standardization_upgrade_and_downgrade(monkeypatch: pytest.MonkeyPatch) -> None:
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
                "CREATE TABLE email_templates (id INTEGER PRIMARY KEY, profile_id INTEGER, body_template TEXT, is_active BOOLEAN)"
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
                INSERT INTO email_templates (id, profile_id, body_template, is_active) VALUES
                  (101, 11, '{{ (summary_text or td_summary_text) }}', 1),
                  (102, 12, '{{ (summary_text or uc_summary_text) }}', 1),
                  (103, 13, 'td run body', 1),
                  (104, 14, 'inactive profile body', 1),
                  (105, 15, 'bank body', 1),
                  (106, 12, 'inactive template body', 0)
                """
            )
        )

    with engine.begin() as connection:
        _run_migration(connection, migration.upgrade, monkeypatch)
        upgraded = _body_by_template_id(connection)

    assert upgraded[101] == migration.SUMMARY_TEXT_TEMPLATE
    assert upgraded[102] == migration.SUMMARY_TEXT_TEMPLATE
    assert upgraded[103] == "td run body"
    assert upgraded[104] == "inactive profile body"
    assert upgraded[105] == "bank body"
    assert upgraded[106] == "inactive template body"

    with engine.begin() as connection:
        connection.execute(
            sa.text("UPDATE email_templates SET body_template = 'manually changed after upgrade' WHERE id = 101")
        )
        _run_migration(connection, migration.downgrade, monkeypatch)
        downgraded = _body_by_template_id(connection)

    assert downgraded[101] == "manually changed after upgrade"
    assert downgraded[102] == "{{ (summary_text or uc_summary_text) }}"
    assert downgraded[103] == "td run body"
    assert downgraded[104] == "inactive profile body"
    assert downgraded[105] == "bank body"
    assert downgraded[106] == "inactive template body"
