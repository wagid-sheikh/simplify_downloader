"""Seed leads_assignment summary notification defaults."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0033_leads_assignment_summary_seed"
down_revision = "0032_td_orders_pipe_notitempl"
branch_labels = None
depends_on = None


PIPELINE_CODE = "leads_assignment"
PIPELINE_DESCRIPTION = "Leads Assignment"

PROFILE_CODE = "leads_assignment"
PROFILE_DESCRIPTION = "Assign leads as and when needed"
PROFILE_ENV = "any"
PROFILE_SCOPE = "store"
PROFILE_ATTACH_MODE = "per_store_pdf"

DEFAULT_TEMPLATE_NAME = "default"
SUMMARY_TEMPLATE_NAME = "summary"

DEFAULT_SUBJECT_TEMPLATE = "[Leads Assignment] Notification for {{ store_code }}"
DEFAULT_BODY_TEMPLATE = (
    "Default leads assignment notification for store {{ store_code }} in {{ run_env }}.\n"
    "Review the assigned leads and follow up as needed."
)

SUMMARY_SUBJECT_TEMPLATE = "[Leads Assignment] Run summary | Batch {{ batch_id }} ({{ run_env }})"
SUMMARY_BODY_TEMPLATE = """
Leads assignment run summary

Run ID: {{ run_id }}
Environment: {{ run_env }}
Batch ID: {{ batch_id }}

Assignments created: {{ assignments }}
Documents generated: {{ documents_generated }}
Emails planned: {{ emails_planned }}
Emails sent: {{ emails_sent }}
""".strip()

RECIPIENT_EMAIL = "leads.assignment.summary@example.com"
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


def _upsert_profile(connection, *, pipeline_id: int) -> int:
    profiles = _notification_profiles_table()

    profile_id = connection.execute(
        sa.select(profiles.c.id)
        .where(profiles.c.pipeline_id == pipeline_id)
        .where(profiles.c.code == PROFILE_CODE)
        .where(profiles.c.env == PROFILE_ENV)
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
        connection.execute(profiles.update().where(profiles.c.id == profile_id).values(payload))
        return profile_id

    result = connection.execute(profiles.insert().values(payload).returning(profiles.c.id))
    return result.scalar_one()


def _upsert_template(
    connection,
    *,
    profile_id: int,
    name: str,
    subject_template: str,
    body_template: str,
) -> None:
    templates = _email_templates_table()

    template_id = connection.execute(
        sa.select(templates.c.id)
        .where(templates.c.profile_id == profile_id)
        .where(templates.c.name == name)
    ).scalar()

    payload = {
        "profile_id": profile_id,
        "name": name,
        "subject_template": subject_template,
        "body_template": body_template,
        "is_active": True,
    }

    if template_id:
        connection.execute(templates.update().where(templates.c.id == template_id).values(payload))
    else:
        connection.execute(templates.insert().values(payload))


def _upsert_recipient(connection, *, profile_id: int) -> None:
    recipients = _notification_recipients_table()

    recipient_id = connection.execute(
        sa.select(recipients.c.id)
        .where(recipients.c.profile_id == profile_id)
        .where(recipients.c.store_code.is_(None))
        .where(recipients.c.env == PROFILE_ENV)
        .where(recipients.c.email_address == RECIPIENT_EMAIL)
        .where(recipients.c.send_as == RECIPIENT_SEND_AS)
    ).scalar()

    payload = {
        "profile_id": profile_id,
        "store_code": None,
        "env": PROFILE_ENV,
        "email_address": RECIPIENT_EMAIL,
        "display_name": "Leads Assignment Summary",
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
    profile_id = _upsert_profile(connection, pipeline_id=pipeline_id)
    _upsert_template(
        connection,
        profile_id=profile_id,
        name=DEFAULT_TEMPLATE_NAME,
        subject_template=DEFAULT_SUBJECT_TEMPLATE,
        body_template=DEFAULT_BODY_TEMPLATE,
    )
    _upsert_template(
        connection,
        profile_id=profile_id,
        name=SUMMARY_TEMPLATE_NAME,
        subject_template=SUMMARY_SUBJECT_TEMPLATE,
        body_template=SUMMARY_BODY_TEMPLATE,
    )
    _upsert_recipient(connection, profile_id=profile_id)


def downgrade() -> None:
    connection = op.get_bind()

    pipelines = _pipelines_table()
    profiles = _notification_profiles_table()
    templates = _email_templates_table()
    recipients = _notification_recipients_table()

    pipeline_id = connection.execute(
        sa.select(pipelines.c.id).where(pipelines.c.code == PIPELINE_CODE)
    ).scalar()

    if not pipeline_id:
        return

    profile_id = connection.execute(
        sa.select(profiles.c.id)
        .where(profiles.c.pipeline_id == pipeline_id)
        .where(profiles.c.code == PROFILE_CODE)
        .where(profiles.c.env == PROFILE_ENV)
    ).scalar()

    if not profile_id:
        return

    connection.execute(
        recipients.delete()
        .where(recipients.c.profile_id == profile_id)
        .where(recipients.c.env == PROFILE_ENV)
        .where(recipients.c.email_address == RECIPIENT_EMAIL)
        .where(recipients.c.send_as == RECIPIENT_SEND_AS)
        .where(recipients.c.store_code.is_(None))
    )

    connection.execute(
        templates.delete()
        .where(templates.c.profile_id == profile_id)
        .where(templates.c.name == DEFAULT_TEMPLATE_NAME)
        .where(templates.c.subject_template == DEFAULT_SUBJECT_TEMPLATE)
        .where(templates.c.body_template == DEFAULT_BODY_TEMPLATE)
    )

    connection.execute(
        templates.delete()
        .where(templates.c.profile_id == profile_id)
        .where(templates.c.name == SUMMARY_TEMPLATE_NAME)
        .where(templates.c.subject_template == SUMMARY_SUBJECT_TEMPLATE)
        .where(templates.c.body_template == SUMMARY_BODY_TEMPLATE)
    )

    remaining_templates = connection.execute(
        sa.select(sa.func.count()).select_from(templates).where(templates.c.profile_id == profile_id)
    ).scalar()
    remaining_recipients = connection.execute(
        sa.select(sa.func.count())
        .select_from(recipients)
        .where(recipients.c.profile_id == profile_id)
    ).scalar()

    if remaining_templates == 0 and remaining_recipients == 0:
        connection.execute(
            profiles.delete()
            .where(profiles.c.id == profile_id)
            .where(profiles.c.description == PROFILE_DESCRIPTION)
            .where(profiles.c.scope == PROFILE_SCOPE)
            .where(profiles.c.attach_mode == PROFILE_ATTACH_MODE)
        )
