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
    module_path = project_root / "alembic" / "versions" / "0080_td_uc_run_email_std.py"
    spec = importlib.util.spec_from_file_location("v0080_td_uc_run_email_std", module_path)
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


def _templates_by_id(connection: sa.Connection) -> dict[int, tuple[str, str]]:
    rows = connection.execute(
        sa.text("SELECT id, subject_template, body_template FROM email_templates ORDER BY id")
    ).fetchall()
    return {int(row[0]): (str(row[1]), str(row[2])) for row in rows}


def test_td_uc_run_email_std_upgrade_and_downgrade(monkeypatch: pytest.MonkeyPatch) -> None:
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
                "CREATE TABLE email_templates (id INTEGER PRIMARY KEY, profile_id INTEGER, subject_template TEXT, body_template TEXT, is_active BOOLEAN)"
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
                  (11, 1, 'run', 1),
                  (12, 2, 'run', 1),
                  (13, 1, 'store', 1),
                  (14, 2, 'store', 1),
                  (15, 1, 'run', 0),
                  (16, 3, 'run', 1)
                """
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO email_templates (id, profile_id, subject_template, body_template, is_active) VALUES
                  (101, 11, 'TD Orders Sync – {{ overall_status }}', :td_run_body, 1),
                  (102, 12, 'UC Orders Sync – {{ overall_status }}', :uc_run_body, 1),
                  (103, 13, :std_subject, :std_body, 1),
                  (104, 14, :std_subject, :std_body, 1),
                  (105, 15, 'inactive run subject', 'inactive run body', 1),
                  (106, 16, 'bank subject', 'bank body', 1),
                  (107, 12, 'inactive template subject', 'inactive template body', 0)
                """
            ),
            {
                "td_run_body": migration.PREVIOUS_BODY_TEMPLATES["run"]["td_orders_sync"],
                "uc_run_body": migration.PREVIOUS_BODY_TEMPLATES["run"]["uc_orders_sync"],
                "std_subject": migration.STANDARD_SUBJECT_TEMPLATE,
                "std_body": migration.STANDARD_BODY_TEMPLATE,
            },
        )

    with engine.begin() as connection:
        _run_migration(connection, migration.upgrade, monkeypatch)
        upgraded = _templates_by_id(connection)

    assert upgraded[101] == (migration.STANDARD_SUBJECT_TEMPLATE, migration.STANDARD_BODY_TEMPLATE)
    assert upgraded[102] == (migration.STANDARD_SUBJECT_TEMPLATE, migration.STANDARD_BODY_TEMPLATE)
    assert upgraded[103] == (migration.STANDARD_SUBJECT_TEMPLATE, migration.STANDARD_BODY_TEMPLATE)
    assert upgraded[104] == (migration.STANDARD_SUBJECT_TEMPLATE, migration.STANDARD_BODY_TEMPLATE)
    assert upgraded[105] == ("inactive run subject", "inactive run body")
    assert upgraded[106] == ("bank subject", "bank body")
    assert upgraded[107] == ("inactive template subject", "inactive template body")

    with engine.begin() as connection:
        connection.execute(
            sa.text("UPDATE email_templates SET body_template = 'manually changed after upgrade' WHERE id = 101")
        )
        _run_migration(connection, migration.downgrade, monkeypatch)
        downgraded = _templates_by_id(connection)

    assert downgraded[101] == (migration.STANDARD_SUBJECT_TEMPLATE, "manually changed after upgrade")
    assert downgraded[102] == (
        migration.PREVIOUS_SUBJECT_TEMPLATES["run"]["uc_orders_sync"],
        migration.PREVIOUS_BODY_TEMPLATES["run"]["uc_orders_sync"],
    )
    assert downgraded[103] == (
        migration.PREVIOUS_SUBJECT_TEMPLATES["store"]["td_orders_sync"],
        migration.PREVIOUS_BODY_TEMPLATES["store"]["td_orders_sync"],
    )
    assert downgraded[104] == (
        migration.PREVIOUS_SUBJECT_TEMPLATES["store"]["uc_orders_sync"],
        migration.PREVIOUS_BODY_TEMPLATES["store"]["uc_orders_sync"],
    )
    assert downgraded[105] == ("inactive run subject", "inactive run body")
    assert downgraded[106] == ("bank subject", "bank body")
    assert downgraded[107] == ("inactive template subject", "inactive template body")
