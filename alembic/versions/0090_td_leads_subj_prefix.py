"""Prefix TD leads subjects when run has newly created leads."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0090_td_leads_subj_prefix"
down_revision = "0089_td_leads_state_events"
branch_labels = None
depends_on = None

PIPELINE_CODE = "td_crm_leads_sync"
PROFILE_CODE = "run_summary"
TEMPLATE_NAME = "run_summary"
NEW_SUBJECT_TEMPLATE = "{{ subject_prefix }}TD CRM Leads {{ run_id }}"
OLD_SUBJECT_TEMPLATE = "TD CRM Leads {{ run_id }}"


def _resolve_template_id(bind) -> int | None:
    pipelines = sa.table("pipelines", sa.column("id", sa.BigInteger()), sa.column("code", sa.Text()))
    notification_profiles = sa.table(
        "notification_profiles",
        sa.column("id", sa.BigInteger()),
        sa.column("pipeline_id", sa.BigInteger()),
        sa.column("code", sa.Text()),
        sa.column("scope", sa.Text()),
        sa.column("is_active", sa.Boolean()),
    )
    email_templates = sa.table(
        "email_templates",
        sa.column("id", sa.BigInteger()),
        sa.column("profile_id", sa.BigInteger()),
        sa.column("name", sa.Text()),
        sa.column("is_active", sa.Boolean()),
    )

    pipeline_id = bind.execute(sa.select(pipelines.c.id).where(pipelines.c.code == PIPELINE_CODE)).scalar_one_or_none()
    if pipeline_id is None:
        return None

    profile_id = bind.execute(
        sa.select(notification_profiles.c.id)
        .where(notification_profiles.c.pipeline_id == pipeline_id)
        .where(notification_profiles.c.code == PROFILE_CODE)
        .where(notification_profiles.c.scope == "run")
        .where(notification_profiles.c.is_active.is_(True))
    ).scalar_one_or_none()
    if profile_id is None:
        return None

    return bind.execute(
        sa.select(email_templates.c.id)
        .where(email_templates.c.profile_id == profile_id)
        .where(email_templates.c.name == TEMPLATE_NAME)
        .where(email_templates.c.is_active.is_(True))
    ).scalar_one_or_none()


def _set_subject_template(bind, subject_template: str) -> None:
    template_id = _resolve_template_id(bind)
    if template_id is None:
        return

    email_templates = sa.table(
        "email_templates",
        sa.column("id", sa.BigInteger()),
        sa.column("subject_template", sa.Text()),
    )
    bind.execute(
        email_templates.update().where(email_templates.c.id == template_id).values(subject_template=subject_template)
    )


def upgrade() -> None:
    bind = op.get_bind()
    _set_subject_template(bind, NEW_SUBJECT_TEMPLATE)


def downgrade() -> None:
    bind = op.get_bind()
    _set_subject_template(bind, OLD_SUBJECT_TEMPLATE)
