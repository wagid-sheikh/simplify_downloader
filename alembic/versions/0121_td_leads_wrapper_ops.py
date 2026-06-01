"""Seed TD-leads wrapper operational notifications."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0121_td_leads_wrapper_ops"
down_revision = "0120_td_leads_timeouts"
branch_labels = None
depends_on = None

PIPELINE_CODE = "td_leads_wrapper_ops"
PROFILE_CODE = "td_leads_wrapper_ops_run"
RECIPIENT_EMAIL = "wagid.sheikh@gmail.com"
SUBJECT_TEMPLATE = (
    "[{{ env_upper }}] TD leads wrapper: {{ metrics_json.resulting_status }}"
)
BODY_TEMPLATE = """TD leads wrapper operational event

Wrapper timestamp: {{ metrics_json.wrapper_timestamp }}
Hostname: {{ metrics_json.hostname }}
Local lock path: {{ metrics_json.local_lock_path }}
Owner PID: {{ metrics_json.owner_pid if metrics_json.owner_pid is not none else 'unknown' }}
Owner PGID: {{ metrics_json.owner_pgid if metrics_json.owner_pgid is not none else 'unknown' }}
Owner age seconds: {{ metrics_json.owner_age_seconds if metrics_json.owner_age_seconds is not none else 'unknown' }}
Recovery action: {{ metrics_json.recovery_action }}
Resulting status: {{ metrics_json.resulting_status }}
Run ID: {{ run_id }}
"""


def upgrade() -> None:
    bind = op.get_bind()
    meta = sa.MetaData()
    meta.reflect(
        bind=bind,
        only=[
            "pipelines",
            "notification_profiles",
            "email_templates",
            "notification_recipients",
        ],
    )
    pipelines = meta.tables["pipelines"]
    profiles = meta.tables["notification_profiles"]
    templates = meta.tables["email_templates"]
    recipients = meta.tables["notification_recipients"]

    pipeline_id = bind.execute(
        sa.select(pipelines.c.id).where(pipelines.c.code == PIPELINE_CODE)
    ).scalar_one_or_none()
    if pipeline_id is None:
        bind.execute(
            pipelines.insert().values(
                code=PIPELINE_CODE, description="TD leads wrapper operational events"
            )
        )
        pipeline_id = bind.execute(
            sa.select(pipelines.c.id).where(pipelines.c.code == PIPELINE_CODE)
        ).scalar_one()

    profile_id = bind.execute(
        sa.select(profiles.c.id)
        .where(profiles.c.pipeline_id == pipeline_id)
        .where(profiles.c.code == PROFILE_CODE)
    ).scalar_one_or_none()
    if profile_id is None:
        bind.execute(
            profiles.insert().values(
                pipeline_id=pipeline_id,
                code=PROFILE_CODE,
                description="TD leads wrapper operational alerts and recoveries",
                env="any",
                scope="run",
                attach_mode="none",
                is_active=True,
            )
        )
        profile_id = bind.execute(
            sa.select(profiles.c.id)
            .where(profiles.c.pipeline_id == pipeline_id)
            .where(profiles.c.code == PROFILE_CODE)
        ).scalar_one()
    else:
        bind.execute(
            profiles.update()
            .where(profiles.c.id == profile_id)
            .values(env="any", scope="run", attach_mode="none", is_active=True)
        )

    template_id = bind.execute(
        sa.select(templates.c.id)
        .where(templates.c.profile_id == profile_id)
        .where(templates.c.name == "run_summary")
    ).scalar_one_or_none()
    template_values = {
        "subject_template": SUBJECT_TEMPLATE,
        "body_template": BODY_TEMPLATE,
        "is_active": True,
    }
    if template_id is None:
        bind.execute(
            templates.insert().values(
                profile_id=profile_id, name="run_summary", **template_values
            )
        )
    else:
        bind.execute(
            templates.update()
            .where(templates.c.id == template_id)
            .values(**template_values)
        )

    recipient_id = bind.execute(
        sa.select(recipients.c.id)
        .where(recipients.c.profile_id == profile_id)
        .where(recipients.c.store_code == "ALL")
        .where(recipients.c.env == "any")
        .where(recipients.c.email_address == RECIPIENT_EMAIL)
        .where(recipients.c.send_as == "to")
    ).scalar_one_or_none()
    if recipient_id is None:
        bind.execute(
            recipients.insert().values(
                profile_id=profile_id,
                store_code="ALL",
                env="any",
                email_address=RECIPIENT_EMAIL,
                display_name="Wagid Sheikh",
                send_as="to",
                is_active=True,
            )
        )
    else:
        bind.execute(
            recipients.update()
            .where(recipients.c.id == recipient_id)
            .values(is_active=True)
        )


def downgrade() -> None:
    bind = op.get_bind()
    meta = sa.MetaData()
    meta.reflect(
        bind=bind,
        only=[
            "pipelines",
            "notification_profiles",
            "email_templates",
            "notification_recipients",
        ],
    )
    pipelines = meta.tables["pipelines"]
    profiles = meta.tables["notification_profiles"]
    templates = meta.tables["email_templates"]
    recipients = meta.tables["notification_recipients"]
    pipeline_id = bind.execute(
        sa.select(pipelines.c.id).where(pipelines.c.code == PIPELINE_CODE)
    ).scalar_one_or_none()
    if pipeline_id is None:
        return
    profile_ids = [
        row[0]
        for row in bind.execute(
            sa.select(profiles.c.id)
            .where(profiles.c.pipeline_id == pipeline_id)
            .where(profiles.c.code == PROFILE_CODE)
        ).all()
    ]
    if profile_ids:
        bind.execute(
            recipients.delete().where(recipients.c.profile_id.in_(profile_ids))
        )
        bind.execute(templates.delete().where(templates.c.profile_id.in_(profile_ids)))
        bind.execute(profiles.delete().where(profiles.c.id.in_(profile_ids)))
    bind.execute(pipelines.delete().where(pipelines.c.id == pipeline_id))
