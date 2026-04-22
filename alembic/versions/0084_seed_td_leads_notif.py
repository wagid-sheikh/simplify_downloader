"""Seed td_crm_leads_sync notification metadata."""

from __future__ import annotations

from datetime import datetime, timezone

from alembic import op
import sqlalchemy as sa


revision = "0084_seed_td_leads_notif"
down_revision = "0083_add_crm_leads_table"
branch_labels = None
depends_on = None

PIPELINE_CODE = "td_crm_leads_sync"
PROFILE_CODE = "run_summary"
RECIPIENT_EMAIL = "wagid.sheikh@gmail.com"

SUBJECT_TEMPLATE = "TD CRM Leads Sync - {{ overall_status|upper }} - {{ report_date }}"
BODY_TEMPLATE = """TD CRM Leads Sync
Run ID: {{ run_id }}
Env: {{ run_env }}
Report Date: {{ report_date }}
Overall Status: {{ overall_status }}

{{ summary_text }}
"""


def _now() -> datetime:
    return datetime.now(timezone.utc)


def upgrade() -> None:
    bind = op.get_bind()

    pipelines = sa.table(
        "pipelines",
        sa.column("id", sa.BigInteger()),
        sa.column("code", sa.Text()),
        sa.column("description", sa.Text()),
    )
    notification_profiles = sa.table(
        "notification_profiles",
        sa.column("id", sa.BigInteger()),
        sa.column("pipeline_id", sa.BigInteger()),
        sa.column("code", sa.Text()),
        sa.column("description", sa.Text()),
        sa.column("env", sa.Text()),
        sa.column("scope", sa.Text()),
        sa.column("attach_mode", sa.Text()),
        sa.column("is_active", sa.Boolean()),
    )
    email_templates = sa.table(
        "email_templates",
        sa.column("id", sa.BigInteger()),
        sa.column("profile_id", sa.BigInteger()),
        sa.column("name", sa.Text()),
        sa.column("subject_template", sa.Text()),
        sa.column("body_template", sa.Text()),
        sa.column("is_active", sa.Boolean()),
    )
    notification_recipients = sa.table(
        "notification_recipients",
        sa.column("id", sa.BigInteger()),
        sa.column("profile_id", sa.BigInteger()),
        sa.column("store_code", sa.Text()),
        sa.column("env", sa.Text()),
        sa.column("email_address", sa.Text()),
        sa.column("display_name", sa.Text()),
        sa.column("send_as", sa.Text()),
        sa.column("is_active", sa.Boolean()),
        sa.column("created_at", sa.DateTime(timezone=True)),
    )

    pipeline_id = bind.execute(sa.select(pipelines.c.id).where(pipelines.c.code == PIPELINE_CODE)).scalar_one_or_none()
    if pipeline_id is None:
        bind.execute(pipelines.insert().values(code=PIPELINE_CODE, description="TD CRM Leads Sync"))
        pipeline_id = bind.execute(sa.select(pipelines.c.id).where(pipelines.c.code == PIPELINE_CODE)).scalar_one()

    profile_id = bind.execute(
        sa.select(notification_profiles.c.id)
        .where(notification_profiles.c.pipeline_id == pipeline_id)
        .where(notification_profiles.c.code == PROFILE_CODE)
        .where(notification_profiles.c.scope == "run")
    ).scalar_one_or_none()
    if profile_id is None:
        bind.execute(
            notification_profiles.insert().values(
                pipeline_id=pipeline_id,
                code=PROFILE_CODE,
                description="TD CRM leads run summary",
                env="all",
                scope="run",
                attach_mode="none",
                is_active=True,
            )
        )
        profile_id = bind.execute(
            sa.select(notification_profiles.c.id)
            .where(notification_profiles.c.pipeline_id == pipeline_id)
            .where(notification_profiles.c.code == PROFILE_CODE)
            .where(notification_profiles.c.scope == "run")
        ).scalar_one()
    else:
        bind.execute(
            notification_profiles.update()
            .where(notification_profiles.c.id == profile_id)
            .values(is_active=True, attach_mode="none", env="all")
        )

    template_id = bind.execute(
        sa.select(email_templates.c.id)
        .where(email_templates.c.profile_id == profile_id)
        .where(email_templates.c.name == "run_summary")
    ).scalar_one_or_none()
    if template_id is None:
        bind.execute(
            email_templates.insert().values(
                profile_id=profile_id,
                name="run_summary",
                subject_template=SUBJECT_TEMPLATE,
                body_template=BODY_TEMPLATE,
                is_active=True,
            )
        )
    else:
        bind.execute(
            email_templates.update()
            .where(email_templates.c.id == template_id)
            .values(subject_template=SUBJECT_TEMPLATE, body_template=BODY_TEMPLATE, is_active=True)
        )

    recipient_id = bind.execute(
        sa.select(notification_recipients.c.id)
        .where(notification_recipients.c.profile_id == profile_id)
        .where(notification_recipients.c.store_code == "ALL")
        .where(notification_recipients.c.env == "all")
        .where(notification_recipients.c.email_address == RECIPIENT_EMAIL)
        .where(notification_recipients.c.send_as == "to")
    ).scalar_one_or_none()
    if recipient_id is None:
        bind.execute(
            notification_recipients.insert().values(
                profile_id=profile_id,
                store_code="ALL",
                env="all",
                email_address=RECIPIENT_EMAIL,
                display_name="Wagid Sheikh",
                send_as="to",
                is_active=True,
                created_at=_now(),
            )
        )
    else:
        bind.execute(
            notification_recipients.update()
            .where(notification_recipients.c.id == recipient_id)
            .values(is_active=True, display_name="Wagid Sheikh")
        )


def downgrade() -> None:
    bind = op.get_bind()

    pipelines = sa.table("pipelines", sa.column("id", sa.BigInteger()), sa.column("code", sa.Text()))
    notification_profiles = sa.table(
        "notification_profiles",
        sa.column("id", sa.BigInteger()),
        sa.column("pipeline_id", sa.BigInteger()),
        sa.column("code", sa.Text()),
    )
    email_templates = sa.table(
        "email_templates",
        sa.column("id", sa.BigInteger()),
        sa.column("profile_id", sa.BigInteger()),
        sa.column("name", sa.Text()),
    )
    notification_recipients = sa.table(
        "notification_recipients",
        sa.column("id", sa.BigInteger()),
        sa.column("profile_id", sa.BigInteger()),
        sa.column("email_address", sa.Text()),
    )

    pipeline_id = bind.execute(sa.select(pipelines.c.id).where(pipelines.c.code == PIPELINE_CODE)).scalar_one_or_none()
    if pipeline_id is None:
        return

    profile_ids = [
        row[0]
        for row in bind.execute(
            sa.select(notification_profiles.c.id)
            .where(notification_profiles.c.pipeline_id == pipeline_id)
            .where(notification_profiles.c.code == PROFILE_CODE)
        ).all()
    ]
    if profile_ids:
        bind.execute(notification_recipients.delete().where(notification_recipients.c.profile_id.in_(profile_ids)).where(notification_recipients.c.email_address == RECIPIENT_EMAIL))
        bind.execute(email_templates.delete().where(email_templates.c.profile_id.in_(profile_ids)).where(email_templates.c.name == "run_summary"))
        bind.execute(notification_profiles.delete().where(notification_profiles.c.id.in_(profile_ids)))

    bind.execute(pipelines.delete().where(pipelines.c.id == pipeline_id))
