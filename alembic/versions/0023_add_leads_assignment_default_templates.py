"""Add leads_assignment default templates and recipients for dev/prod."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0023_leads_assignment_devprod_templates"
down_revision = "0022_add_timestamps_to_missed_leads"
branch_labels = None
depends_on = None


PIPELINE_CODE = "leads_assignment"
PIPELINE_DESCRIPTION = "Leads Assignment"
PROFILE_CODE = "leads_assignment"
PROFILE_DESCRIPTION = "Assign leads as and when needed"
PROFILE_SCOPE = "store"
PROFILE_ATTACH_MODE = "per_store_pdf"
PROFILE_ENVS = ("dev", "prod")

TEMPLATE_NAME = "default"
SUBJECT_TEMPLATE = "[Leads Assignment] Notification for {{ store_code }}"
BODY_TEMPLATE = (
    "Default leads assignment notification for store {{ store_code }} in {{ run_env }}.\n"
    "Review the assigned leads and follow up as needed."
)

STORE_CODES = ("A012", "T105", "A696", "TS62")
RECIPIENT_EMAIL = "wagid.sheikh@gmail.com"
RECIPIENT_SEND_AS = "to"


def _pipelines_table() -> sa.Table:
    return sa.table(
        "pipelines",
        sa.column("id"),
        sa.column("code"),
        sa.column("description"),
    )


def _notification_profiles_table() -> sa.Table:
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


def _email_templates_table() -> sa.Table:
    return sa.table(
        "email_templates",
        sa.column("id"),
        sa.column("profile_id"),
        sa.column("name"),
        sa.column("subject_template"),
        sa.column("body_template"),
        sa.column("is_active"),
    )


def _notification_recipients_table() -> sa.Table:
    return sa.table(
        "notification_recipients",
        sa.column("id"),
        sa.column("profile_id"),
        sa.column("store_code"),
        sa.column("env"),
        sa.column("email_address"),
        sa.column("display_name"),
        sa.column("send_as"),
        sa.column("is_active"),
    )


def _upsert_pipeline(connection) -> int:
    pipelines = _pipelines_table()

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


def _upsert_profile(connection, *, pipeline_id: int, env: str) -> int:
    profiles = _notification_profiles_table()

    profile_id = connection.execute(
        sa.select(profiles.c.id)
        .where(profiles.c.pipeline_id == pipeline_id)
        .where(profiles.c.code == PROFILE_CODE)
        .where(profiles.c.env == env)
    ).scalar()

    payload = {
        "pipeline_id": pipeline_id,
        "code": PROFILE_CODE,
        "description": PROFILE_DESCRIPTION,
        "env": env,
        "scope": PROFILE_SCOPE,
        "attach_mode": PROFILE_ATTACH_MODE,
        "is_active": True,
    }

    if profile_id:
        connection.execute(
            profiles.update().where(profiles.c.id == profile_id).values(payload)
        )
        return profile_id

    result = connection.execute(profiles.insert().values(payload).returning(profiles.c.id))
    return result.scalar_one()


def _upsert_default_template(connection, *, profile_id: int) -> None:
    templates = _email_templates_table()

    template_id = connection.execute(
        sa.select(templates.c.id)
        .where(templates.c.profile_id == profile_id)
        .where(templates.c.name == TEMPLATE_NAME)
    ).scalar()

    payload = {
        "profile_id": profile_id,
        "name": TEMPLATE_NAME,
        "subject_template": SUBJECT_TEMPLATE,
        "body_template": BODY_TEMPLATE,
        "is_active": True,
    }

    if template_id:
        connection.execute(
            templates.update().where(templates.c.id == template_id).values(payload)
        )
    else:
        connection.execute(templates.insert().values(payload))


def _upsert_recipient(connection, *, profile_id: int, store_code: str, env: str) -> None:
    recipients = _notification_recipients_table()

    recipient_id = connection.execute(
        sa.select(recipients.c.id)
        .where(recipients.c.profile_id == profile_id)
        .where(recipients.c.store_code == store_code)
        .where(recipients.c.env == env)
        .where(recipients.c.email_address == RECIPIENT_EMAIL)
        .where(recipients.c.send_as == RECIPIENT_SEND_AS)
    ).scalar()

    payload = {
        "profile_id": profile_id,
        "store_code": store_code,
        "env": env,
        "email_address": RECIPIENT_EMAIL,
        "display_name": None,
        "send_as": RECIPIENT_SEND_AS,
        "is_active": True,
    }

    if recipient_id:
        connection.execute(
            recipients.update().where(recipients.c.id == recipient_id).values(payload)
        )
    else:
        connection.execute(recipients.insert().values(payload))


def upgrade() -> None:
    connection = op.get_bind()

    pipeline_id = _upsert_pipeline(connection)

    for env in PROFILE_ENVS:
        profile_id = _upsert_profile(connection, pipeline_id=pipeline_id, env=env)
        _upsert_default_template(connection, profile_id=profile_id)

        for store_code in STORE_CODES:
            _upsert_recipient(
                connection, profile_id=profile_id, store_code=store_code, env=env
            )


def downgrade() -> None:
    connection = op.get_bind()

    profiles = _notification_profiles_table()
    templates = _email_templates_table()
    recipients = _notification_recipients_table()

    profile_ids = [
        row["id"]
        for row in connection.execute(
            sa.select(profiles.c.id)
            .where(profiles.c.code == PROFILE_CODE)
            .where(profiles.c.env.in_(PROFILE_ENVS))
        ).mappings()
    ]

    if not profile_ids:
        return

    connection.execute(
        recipients.delete()
        .where(recipients.c.profile_id.in_(profile_ids))
        .where(recipients.c.env.in_(PROFILE_ENVS))
        .where(recipients.c.email_address == RECIPIENT_EMAIL)
        .where(recipients.c.send_as == RECIPIENT_SEND_AS)
        .where(recipients.c.store_code.in_(STORE_CODES))
    )

    connection.execute(
        templates.delete()
        .where(templates.c.profile_id.in_(profile_ids))
        .where(templates.c.name == TEMPLATE_NAME)
        .where(templates.c.subject_template == SUBJECT_TEMPLATE)
        .where(templates.c.body_template == BODY_TEMPLATE)
    )
