"""Upsert leads_assignment pipeline and notification profile."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0018_leads_notification_profile"
down_revision = "0017_lead_assignment_tables"
branch_labels = None
depends_on = None


PIPELINE_CODE = "leads_assignment"
PIPELINE_DESCRIPTION = "Leads Assignment"

PROFILE_CODE = "leads_assignment"
PROFILE_DESCRIPTION = "Assign leads as and when needed"
PROFILE_ENV = "any"
PROFILE_SCOPE = "store"
PROFILE_ATTACH_MODE = "per_store_pdf"


def _get_pipeline_table() -> sa.Table:
    return sa.table(
        "pipelines",
        sa.column("id"),
        sa.column("code"),
        sa.column("description"),
    )


def _get_notification_profiles_table() -> sa.Table:
    return sa.table(
        "notification_profiles",
        sa.column("id"),
        sa.column("pipeline_id"),
        sa.column("code"),
        sa.column("description"),
        sa.column("env"),
        sa.column("scope"),
        sa.column("attach_mode"),
        sa.column("is_active"),
    )


def _upsert_pipeline(connection) -> int:
    pipelines = _get_pipeline_table()

    pipeline_id = connection.execute(
        sa.select(pipelines.c.id).where(pipelines.c.code == PIPELINE_CODE)
    ).scalar()

    if pipeline_id:
        connection.execute(
            pipelines.update()
            .where(pipelines.c.id == pipeline_id)
            .values(description=PIPELINE_DESCRIPTION)
        )
        return pipeline_id

    result = connection.execute(
        pipelines.insert()
        .values(code=PIPELINE_CODE, description=PIPELINE_DESCRIPTION)
        .returning(pipelines.c.id)
    )
    return result.scalar_one()


def _upsert_notification_profile(connection, *, pipeline_id: int) -> None:
    notification_profiles = _get_notification_profiles_table()

    profile_id = connection.execute(
        sa.select(notification_profiles.c.id)
        .where(notification_profiles.c.pipeline_id == pipeline_id)
        .where(notification_profiles.c.code == PROFILE_CODE)
        .where(notification_profiles.c.env == PROFILE_ENV)
    ).scalar()

    payload = {
        "pipeline_id": pipeline_id,
        "code": PROFILE_CODE,
        "description": PROFILE_DESCRIPTION,
        "env": PROFILE_ENV,
        "scope": PROFILE_SCOPE,
        "attach_mode": PROFILE_ATTACH_MODE,
        "is_active": True,
    }

    if profile_id:
        connection.execute(
            notification_profiles.update()
            .where(notification_profiles.c.id == profile_id)
            .values(payload)
        )
    else:
        connection.execute(notification_profiles.insert().values(payload))


def upgrade() -> None:
    connection = op.get_bind()

    pipeline_id = _upsert_pipeline(connection)
    _upsert_notification_profile(connection, pipeline_id=pipeline_id)


def downgrade() -> None:
    connection = op.get_bind()

    pipelines = _get_pipeline_table()
    notification_profiles = _get_notification_profiles_table()

    pipeline_id = connection.execute(
        sa.select(pipelines.c.id).where(pipelines.c.code == PIPELINE_CODE)
    ).scalar()

    if pipeline_id:
        connection.execute(
            notification_profiles.delete()
            .where(notification_profiles.c.pipeline_id == pipeline_id)
            .where(notification_profiles.c.code == PROFILE_CODE)
            .where(notification_profiles.c.env == PROFILE_ENV)
        )

        remaining_profiles = connection.execute(
            sa.select(sa.func.count()).select_from(notification_profiles).where(
                notification_profiles.c.pipeline_id == pipeline_id
            )
        ).scalar()

        if remaining_profiles == 0:
            connection.execute(pipelines.delete().where(pipelines.c.id == pipeline_id))
