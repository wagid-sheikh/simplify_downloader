"""Rename simplify_dashboard pipelines to dashboard_* codes."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0010_update_dashboard_pipeline_codes"
down_revision = "0009_seed_system_config"
branch_labels = None
depends_on = None


def _fetch_one(connection, query: sa.sql.Select) -> dict | None:
    result = connection.execute(query)
    row = result.mappings().first()
    return dict(row) if row else None


def _get_pipeline_row(connection, code: str) -> dict | None:
    pipelines = sa.table("pipelines", sa.column("id"), sa.column("code"))
    return _fetch_one(
        connection,
        sa.select(pipelines.c.id, pipelines.c.code).where(pipelines.c.code == code),
    )


def _get_conflicting_profile_id(connection, *, pipeline_id: int, code: str, env: str) -> int | None:
    notification_profiles = sa.table(
        "notification_profiles", sa.column("id"), sa.column("pipeline_id"), sa.column("code"), sa.column("env")
    )
    row = _fetch_one(
        connection,
        sa.select(notification_profiles.c.id)
        .where(notification_profiles.c.pipeline_id == pipeline_id)
        .where(notification_profiles.c.code == code)
        .where(notification_profiles.c.env == env),
    )
    return row["id"] if row else None


def _move_profiles_to_pipeline(connection, *, old_pipeline_id: int, new_pipeline_id: int) -> None:
    notification_profiles = sa.table(
        "notification_profiles",
        sa.column("id"),
        sa.column("pipeline_id"),
        sa.column("code"),
        sa.column("env"),
    )

    profiles = connection.execute(
        sa.select(notification_profiles.c.id, notification_profiles.c.code, notification_profiles.c.env).where(
            notification_profiles.c.pipeline_id == old_pipeline_id
        )
    ).mappings()

    for profile in profiles:
        conflict_id = _get_conflicting_profile_id(
            connection, pipeline_id=new_pipeline_id, code=profile["code"], env=profile["env"]
        )
        if conflict_id:
            connection.execute(
                notification_profiles.delete().where(notification_profiles.c.id == profile["id"])
            )
        else:
            connection.execute(
                notification_profiles.update()
                .where(notification_profiles.c.id == profile["id"])
                .values(pipeline_id=new_pipeline_id)
            )


PIPELINE_CODE_MAPPING: dict[str, str] = {
    "simplify_dashboard_daily": "dashboard_daily",
    "simplify_dashboard_weekly": "dashboard_weekly",
    "simplify_dashboard_monthly": "dashboard_monthly",
}


def upgrade() -> None:
    connection = op.get_bind()

    pipelines = sa.table("pipelines", sa.column("id"), sa.column("code"))

    for old_code, new_code in PIPELINE_CODE_MAPPING.items():
        old_row = _get_pipeline_row(connection, old_code)
        new_row = _get_pipeline_row(connection, new_code)

        if old_row and new_row:
            _move_profiles_to_pipeline(
                connection, old_pipeline_id=old_row["id"], new_pipeline_id=new_row["id"]
            )
            connection.execute(pipelines.delete().where(pipelines.c.id == old_row["id"]))
        elif old_row:
            connection.execute(
                pipelines.update().where(pipelines.c.id == old_row["id"]).values(code=new_code)
            )

    connection.execute(
        sa.text(
            """
            UPDATE pipeline_run_summaries
            SET pipeline_name = regexp_replace(pipeline_name, '^simplify_dashboard_', 'dashboard_')
            WHERE pipeline_name LIKE 'simplify_dashboard_%'
            """
        )
    )


def downgrade() -> None:
    connection = op.get_bind()

    pipelines = sa.table("pipelines", sa.column("id"), sa.column("code"))

    for old_code, new_code in PIPELINE_CODE_MAPPING.items():
        current_row = _get_pipeline_row(connection, new_code)
        legacy_row = _get_pipeline_row(connection, old_code)

        if current_row and not legacy_row:
            connection.execute(
                pipelines.update().where(pipelines.c.id == current_row["id"]).values(code=old_code)
            )

    connection.execute(
        sa.text(
            """
            UPDATE pipeline_run_summaries
            SET pipeline_name = regexp_replace(pipeline_name, '^dashboard_', 'simplify_dashboard_')
            WHERE pipeline_name LIKE 'dashboard_%'
            """
        )
    )
