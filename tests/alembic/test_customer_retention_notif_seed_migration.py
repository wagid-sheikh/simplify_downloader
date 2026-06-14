from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Callable

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations


def _load_migration_module(filename: str, module_name: str):
    project_root = Path(__file__).resolve().parents[2]
    module_path = project_root / "alembic" / "versions" / filename
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load migration module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


migration = _load_migration_module("0133_cfl_notif_seed.py", "v0133_cfl_notif_seed")
cleanup_migration = _load_migration_module(
    "0134_cfl_notif_no_recip.py", "v0134_cfl_notif_no_recip"
)
owner_recipient_migration = _load_migration_module(
    "0135_cfl_owner_recipient.py", "v0135_cfl_owner_recipient"
)


def _run_migration(
    connection: sa.Connection,
    module,
    fn: Callable[[], None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = MigrationContext.configure(connection)
    operations = Operations(context)
    original_op = module.op
    monkeypatch.setattr(module, "op", operations)
    try:
        fn()
    finally:
        monkeypatch.setattr(module, "op", original_op)


def _create_base_notification_tables(connection: sa.Connection) -> None:
    connection.execute(sa.text("""
            CREATE TABLE pipelines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE,
                description TEXT
            )
            """))
    connection.execute(sa.text("""
            CREATE TABLE notification_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline_id INTEGER,
                code TEXT,
                description TEXT,
                env TEXT,
                scope TEXT,
                attach_mode TEXT,
                is_active BOOLEAN,
                UNIQUE(pipeline_id, code, env)
            )
            """))
    connection.execute(sa.text("""
            CREATE TABLE email_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_id INTEGER,
                name TEXT,
                subject_template TEXT,
                body_template TEXT,
                is_active BOOLEAN,
                UNIQUE(profile_id, name)
            )
            """))
    connection.execute(sa.text("""
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
            """))


def test_customer_retention_notif_seed_upgrade_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        _create_base_notification_tables(connection)
        _run_migration(connection, migration, migration.upgrade, monkeypatch)
        _run_migration(connection, migration, migration.upgrade, monkeypatch)

        pipeline = connection.execute(
            sa.text(
                "SELECT id, description FROM pipelines WHERE code = 'customer_retention_pipeline'"
            )
        ).one()
        assert pipeline.description == "Customer Retention Pipeline"
        assert (
            connection.execute(
                sa.text(
                    "SELECT COUNT(*) FROM pipelines WHERE code = 'customer_retention_pipeline'"
                )
            ).scalar_one()
            == 1
        )

        profile = connection.execute(
            sa.text("""
                SELECT id, code, description, env, scope, attach_mode, is_active
                FROM notification_profiles
                WHERE pipeline_id = :pipeline_id AND code = 'owner_summary'
                """),
            {"pipeline_id": pipeline.id},
        ).one()
        assert profile.description == "Customer retention owner run summary"
        assert profile.env == "any"
        assert profile.scope == "run"
        assert profile.attach_mode == "none"
        assert profile.is_active == 1

        template = connection.execute(
            sa.text("""
                SELECT name, subject_template, body_template, is_active
                FROM email_templates
                WHERE profile_id = :profile_id
                """),
            {"profile_id": profile.id},
        ).one()
        assert template.name == "summary"
        assert "Customer Retention Summary" in template.subject_template
        assert "run_summary" in template.body_template
        assert "Store Summary" in template.body_template
        assert "Aging Actionable Workload" in template.body_template
        assert "Staff Productivity" in template.body_template
        assert "Source-Wise Summary" in template.body_template
        assert "Warning/Error Summary" in template.body_template
        assert template.is_active == 1

        hardcoded_recipients = connection.execute(
            sa.text("""
                SELECT env, email_address, display_name, send_as, is_active
                FROM notification_recipients
                WHERE profile_id = :profile_id
                """),
            {"profile_id": profile.id},
        ).fetchall()
        assert hardcoded_recipients == []


def test_customer_retention_recipient_cleanup_only_removes_legacy_seed_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        _create_base_notification_tables(connection)
        _run_migration(connection, migration, migration.upgrade, monkeypatch)

        pipeline_id = connection.execute(
            sa.text(
                "SELECT id FROM pipelines WHERE code = 'customer_retention_pipeline'"
            )
        ).scalar_one()
        profile_id = connection.execute(
            sa.text("""
                SELECT id FROM notification_profiles
                WHERE pipeline_id = :pipeline_id AND code = 'owner_summary' AND env = 'any'
                """),
            {"pipeline_id": pipeline_id},
        ).scalar_one()

        connection.execute(
            sa.text("""
                INSERT INTO notification_recipients
                    (profile_id, store_code, env, email_address, display_name, send_as, is_active)
                VALUES
                    (:profile_id, NULL, 'dev', 'wagid.sheikh@gmail.com', 'Wagid Sheikh', 'to', 1),
                    (:profile_id, NULL, 'prod', 'ops@example.com', 'Ops Managed', 'to', 1),
                    (:profile_id, 'CC01', 'dev', 'wagid.sheikh@gmail.com', 'Wagid Sheikh', 'to', 1),
                    (:profile_id, NULL, 'dev', 'wagid.sheikh@gmail.com', 'Renamed Operator', 'to', 1)
                """),
            {"profile_id": profile_id},
        )

        _run_migration(
            connection, cleanup_migration, cleanup_migration.upgrade, monkeypatch
        )

        recipients = connection.execute(
            sa.text("""
                SELECT store_code, env, email_address, display_name, send_as, is_active
                FROM notification_recipients
                WHERE profile_id = :profile_id
                ORDER BY email_address, display_name, store_code
                """),
            {"profile_id": profile_id},
        ).fetchall()
        assert [
            (row.store_code, row.env, row.email_address, row.display_name)
            for row in recipients
        ] == [
            (None, "prod", "ops@example.com", "Ops Managed"),
            (None, "dev", "wagid.sheikh@gmail.com", "Renamed Operator"),
            ("CC01", "dev", "wagid.sheikh@gmail.com", "Wagid Sheikh"),
        ]


def test_customer_retention_owner_recipient_seed_is_idempotent_and_preserves_unrelated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite://")

    with engine.begin() as connection:
        _create_base_notification_tables(connection)
        _run_migration(connection, migration, migration.upgrade, monkeypatch)

        pipeline_id = connection.execute(
            sa.text(
                "SELECT id FROM pipelines WHERE code = 'customer_retention_pipeline'"
            )
        ).scalar_one()
        profile_id = connection.execute(
            sa.text("""
                SELECT id FROM notification_profiles
                WHERE pipeline_id = :pipeline_id
                  AND code = 'owner_summary'
                  AND scope = 'run'
                """),
            {"pipeline_id": pipeline_id},
        ).scalar_one()

        connection.execute(
            sa.text("""
                INSERT INTO notification_recipients
                    (profile_id, store_code, env, email_address, display_name, send_as, is_active)
                VALUES
                    (:profile_id, NULL, 'dev', 'wagid.sheikh@gmail.com', 'Old Name', 'cc', 0),
                    (:profile_id, NULL, 'prod', 'ops@example.com', 'Ops Managed', 'to', 1),
                    (:profile_id, 'CC01', 'dev', 'wagid.sheikh@gmail.com', 'Store Managed', 'cc', 0)
                """),
            {"profile_id": profile_id},
        )

        _run_migration(
            connection,
            owner_recipient_migration,
            owner_recipient_migration.upgrade,
            monkeypatch,
        )
        _run_migration(
            connection,
            owner_recipient_migration,
            owner_recipient_migration.upgrade,
            monkeypatch,
        )

        owner_rows = connection.execute(
            sa.text("""
                SELECT env, email_address, display_name, send_as, store_code, is_active
                FROM notification_recipients
                WHERE profile_id = :profile_id
                  AND email_address = 'wagid.sheikh@gmail.com'
                  AND store_code IS NULL
                ORDER BY env
                """),
            {"profile_id": profile_id},
        ).fetchall()
        assert {row.env for row in owner_rows} == {"any", "dev", "local", "prod"}
        assert len(owner_rows) == 4
        assert all(row.display_name == "Wagid Sheikh" for row in owner_rows)
        assert all(row.send_as == "to" for row in owner_rows)
        assert all(row.is_active == 1 for row in owner_rows)

        assert (
            connection.execute(
                sa.text("""
                    SELECT COUNT(*)
                    FROM notification_recipients
                    WHERE profile_id = :profile_id
                      AND email_address = 'wagid.sheikh@gmail.com'
                      AND store_code IS NULL
                    """),
                {"profile_id": profile_id},
            ).scalar_one()
            == 4
        )

        unrelated_rows = connection.execute(
            sa.text("""
                SELECT store_code, env, email_address, display_name, send_as, is_active
                FROM notification_recipients
                WHERE profile_id = :profile_id
                  AND (email_address = 'ops@example.com' OR store_code = 'CC01')
                ORDER BY email_address, store_code
                """),
            {"profile_id": profile_id},
        ).fetchall()
        assert [
            (
                row.store_code,
                row.env,
                row.email_address,
                row.display_name,
                row.send_as,
                row.is_active,
            )
            for row in unrelated_rows
        ] == [
            (None, "prod", "ops@example.com", "Ops Managed", "to", 1),
            ("CC01", "dev", "wagid.sheikh@gmail.com", "Store Managed", "cc", 0),
        ]
