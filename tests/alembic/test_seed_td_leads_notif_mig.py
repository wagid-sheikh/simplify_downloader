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
    module_path = project_root / "alembic" / "versions" / "0084_seed_td_leads_notif.py"
    spec = importlib.util.spec_from_file_location("v0084_seed_td_leads_notif", module_path)
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
                code TEXT,
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
                code TEXT,
                description TEXT,
                env TEXT,
                scope TEXT,
                attach_mode TEXT,
                is_active BOOLEAN
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
                is_active BOOLEAN
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


def test_seed_td_leads_notif_upgrade_and_downgrade(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        _create_base_notification_tables(connection)
        _run_migration(connection, migration.upgrade, monkeypatch)

        pipeline_id = connection.execute(
            sa.text("SELECT id FROM pipelines WHERE code = 'td_crm_leads_sync'")
        ).scalar_one()
        assert pipeline_id is not None

        profile_id = connection.execute(
            sa.text(
                "SELECT id FROM notification_profiles WHERE pipeline_id = :pipeline_id AND scope = 'run'"
            ),
            {"pipeline_id": pipeline_id},
        ).scalar_one()
        assert profile_id is not None

        profile_env = connection.execute(
            sa.text("SELECT env FROM notification_profiles WHERE id = :profile_id"),
            {"profile_id": profile_id},
        ).scalar_one()
        assert profile_env == "any"
        assert profile_env in ALLOWED_ENVS

        recipient_row = connection.execute(
            sa.text(
                """
                SELECT email_address, env
                FROM notification_recipients
                WHERE profile_id = :profile_id AND send_as = 'to'
                """
            ),
            {"profile_id": profile_id},
        ).one()
        assert recipient_row.email_address == "wagid.sheikh@gmail.com"
        assert recipient_row.env == "any"
        assert recipient_row.env in ALLOWED_ENVS

    with engine.begin() as connection:
        _run_migration(connection, migration.downgrade, monkeypatch)
        pipeline_count = connection.execute(
            sa.text("SELECT COUNT(*) FROM pipelines WHERE code = 'td_crm_leads_sync'")
        ).scalar_one()
        assert pipeline_count == 0
