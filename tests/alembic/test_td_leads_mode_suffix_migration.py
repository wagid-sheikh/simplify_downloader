from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
import sqlalchemy as sa

from alembic.migration import MigrationContext
from alembic.operations import Operations


def _load_migration_module():
    project_root = Path(__file__).resolve().parents[2]
    module_path = project_root / "alembic" / "versions" / "0098_td_leads_mode_suffix.py"
    spec = importlib.util.spec_from_file_location("v0098_td_leads_mode_suffix", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load migration module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


migration = _load_migration_module()


def _run_migration(connection: sa.Connection, fn, monkeypatch: pytest.MonkeyPatch) -> None:
    context = MigrationContext.configure(connection)
    operations = Operations(context)
    original_op = migration.op
    monkeypatch.setattr(migration, "op", operations)
    try:
        fn()
    finally:
        monkeypatch.setattr(migration, "op", original_op)


def _subject_by_id(connection: sa.Connection) -> dict[int, str]:
    rows = connection.execute(sa.text("SELECT id, subject_template FROM email_templates ORDER BY id")).fetchall()
    return {int(row.id): str(row.subject_template) for row in rows}


def test_td_leads_mode_suffix_upgrade_and_downgrade(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = sa.create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(sa.text("CREATE TABLE pipelines (id INTEGER PRIMARY KEY, code TEXT)"))
        connection.execute(
            sa.text(
                "CREATE TABLE notification_profiles (id INTEGER PRIMARY KEY, pipeline_id INTEGER, code TEXT, scope TEXT, is_active BOOLEAN)"
            )
        )
        connection.execute(
            sa.text(
                "CREATE TABLE email_templates (id INTEGER PRIMARY KEY, profile_id INTEGER, name TEXT, subject_template TEXT, is_active BOOLEAN)"
            )
        )
        connection.execute(sa.text("INSERT INTO pipelines (id, code) VALUES (1, 'td_crm_leads_sync')"))
        connection.execute(
            sa.text(
                "INSERT INTO notification_profiles (id, pipeline_id, code, scope, is_active) VALUES (10, 1, 'run_summary', 'run', 1)"
            )
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO email_templates (id, profile_id, name, subject_template, is_active) VALUES
                    (100, 10, 'run_summary', '{{ subject_prefix }}TD CRM Leads {{ run_id }}', 1),
                    (101, 10, 'default', 'unchanged', 1)
                """
            )
        )

        _run_migration(connection, migration.upgrade, monkeypatch)
        upgraded = _subject_by_id(connection)
        assert upgraded[100] == "{{ subject_prefix }}TD CRM Leads {{ run_id }}{{ reporting_mode_suffix }}"
        assert upgraded[101] == "unchanged"

        _run_migration(connection, migration.downgrade, monkeypatch)
        downgraded = _subject_by_id(connection)
        assert downgraded[100] == "{{ subject_prefix }}TD CRM Leads {{ run_id }}"
        assert downgraded[101] == "unchanged"
