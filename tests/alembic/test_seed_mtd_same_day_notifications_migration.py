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
    module_path = project_root / "alembic" / "versions" / "0096_seed_mtd_same_day_notif.py"
    spec = importlib.util.spec_from_file_location("v0096_seed_mtd_same_day_notif", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load migration module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


migration = _load_migration_module()
ALLOWED_ENVS = {"dev", "prod", "local", "any"}


def _run_migration(connection: sa.Connection, fn: Callable[[], None], monkeypatch: pytest.MonkeyPatch) -> None:
    context = MigrationContext.configure(connection)
    operations = Operations(context)
    original_op = migration.op
    monkeypatch.setattr(migration, "op", operations)
    try:
        fn()
    finally:
        monkeypatch.setattr(migration, "op", original_op)


def _create_base_notification_tables(connection: sa.Connection) -> None:
    connection.execute(
        sa.text(
            """
            CREATE TABLE pipelines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE,
                description TEXT
            )
            """
        )
    )
    connection.execute(
        sa.text(
            """
            CREATE TABLE notification_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline_id INTEGER,
                code TEXT UNIQUE,
                description TEXT,
                env TEXT,
                scope TEXT,
                attach_mode TEXT,
                is_active BOOLEAN,
                UNIQUE(pipeline_id, code, env)
            )
            """
        )
    )
    connection.execute(
        sa.text(
            """
            CREATE TABLE email_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_id INTEGER,
                name TEXT,
                subject_template TEXT,
                body_template TEXT,
                is_active BOOLEAN,
                UNIQUE(profile_id, name)
            )
            """
        )
    )
    connection.execute(
        sa.text(
            """
            CREATE TABLE notification_recipients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_id INTEGER,
                store_code TEXT,
                env TEXT,
                email_address TEXT,
                display_name TEXT,
                send_as TEXT,
                is_active BOOLEAN,
                created_at DATETIME
            )
            """
        )
    )


def test_seed_mtd_same_day_notif_upgrade_and_downgrade(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        _create_base_notification_tables(connection)
        _run_migration(connection, migration.upgrade, monkeypatch)

        pipeline_id = connection.execute(
            sa.text("SELECT id FROM pipelines WHERE code = 'reports.mtd_same_day_fulfillment'")
        ).scalar_one()
        assert pipeline_id is not None
        pipeline_description = connection.execute(
            sa.text("SELECT description FROM pipelines WHERE id = :pipeline_id"),
            {"pipeline_id": pipeline_id},
        ).scalar_one()
        assert pipeline_description == "Reports Pipeline, MTD Same-Day Fulfillment"

        profile_row = connection.execute(
            sa.text(
                """
                SELECT id, code, scope, env, attach_mode
                FROM notification_profiles
                WHERE pipeline_id = :pipeline_id
                """
            ),
            {"pipeline_id": pipeline_id},
        ).one()
        assert profile_row.code == "default"
        assert profile_row.attach_mode == "all_docs_for_run"
        assert profile_row.scope == "run"
        assert profile_row.env == "any"
        assert profile_row.env in ALLOWED_ENVS

        template_row = connection.execute(
            sa.text(
                """
                SELECT name, subject_template
                     , body_template
                FROM email_templates
                WHERE profile_id = :profile_id
                """
            ),
            {"profile_id": profile_row.id},
        ).one()
        assert template_row.name == "default"
        assert "MTD Same-Day Fulfillment Report" in template_row.subject_template
        assert "Run ID: {{ run_id }} | Env: {{ run_env }}" in template_row.body_template

        recipient_rows = connection.execute(
            sa.text(
                """
                SELECT env, email_address, send_as
                FROM notification_recipients
                WHERE profile_id = :profile_id
                ORDER BY env
                """
            ),
            {"profile_id": profile_row.id},
        ).fetchall()
        assert {row.env for row in recipient_rows} == ALLOWED_ENVS
        assert all(row.email_address == "wagid.sheikh@gmail.com" for row in recipient_rows)
        assert all(row.send_as == "to" for row in recipient_rows)

    with engine.begin() as connection:
        _run_migration(connection, migration.downgrade, monkeypatch)
        pipeline_count = connection.execute(
            sa.text("SELECT COUNT(*) FROM pipelines WHERE code = 'reports.mtd_same_day_fulfillment'")
        ).scalar_one()
        profile_count = connection.execute(
            sa.text(
                """
                SELECT COUNT(*)
                FROM notification_profiles np
                JOIN pipelines p ON np.pipeline_id = p.id
                WHERE p.code = 'reports.mtd_same_day_fulfillment'
                """
            )
        ).scalar_one()
        assert pipeline_count == 0
        assert profile_count == 0
