"""Seed pipelines and notifications for pending deliveries report."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0063_seed_pending_deliveries_report_notifications"
down_revision = "0062_skip_uc_pending_delivery"
branch_labels = None
depends_on = None

PIPELINE_CODE = "reports.pending_deliveries"
PIPELINE_DESCRIPTION = "Reports Pipeline, Pending Deliveries"

PROFILE_CODE = "default"
PROFILE_ENV = "any"
PROFILE_SCOPE = "run"
PROFILE_ATTACH_MODE = "all_docs_for_run"

RECIPIENT_EMAIL = "wagid.sheikh@gmail.com"
RECIPIENT_ENVS = ["dev", "prod", "local", "any"]

TEMPLATE_SUBJECT = "Pending Deliveries Report - {{ report_date }} ({{ overall_status }})"
TEMPLATE_BODY = """
{{ summary_text }}
{% if not summary_text %}
Pending Deliveries Report
Run ID: {{ run_id }} | Env: {{ run_env }}
Report Date: {{ report_date }}
Started: {{ started_at }} | Finished: {{ finished_at }}
{% if total_time_taken %}Total Duration: {{ total_time_taken }}{% endif %}
Overall Status: {{ overall_status }}
{% endif %}
"""


def _pipelines_table() -> sa.Table:
    return sa.table(
        "pipelines",
        sa.column("id"),
        sa.column("code"),
        sa.column("description"),
    )


def _profiles_table() -> sa.Table:
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


def _templates_table() -> sa.Table:
    return sa.table(
        "email_templates",
        sa.column("id"),
        sa.column("profile_id"),
        sa.column("name"),
        sa.column("subject_template"),
        sa.column("body_template"),
        sa.column("is_active"),
    )


def _recipients_table() -> sa.Table:
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


def _upsert_pipeline(connection: sa.Connection) -> int:
    pipelines = _pipelines_table()
    stmt = (
        postgresql.insert(pipelines)
        .values(code=PIPELINE_CODE, description=PIPELINE_DESCRIPTION)
        .on_conflict_do_update(
            index_elements=[pipelines.c.code], set_={"description": PIPELINE_DESCRIPTION}
        )
        .returning(pipelines.c.id)
    )
    return connection.execute(stmt).scalar_one()


def _upsert_profile(connection: sa.Connection, *, pipeline_id: int) -> int:
    profiles = _profiles_table()
    stmt = (
        postgresql.insert(profiles)
        .values(
            pipeline_id=pipeline_id,
            code=PROFILE_CODE,
            description="Pending deliveries report notifications",
            env=PROFILE_ENV,
            scope=PROFILE_SCOPE,
            attach_mode=PROFILE_ATTACH_MODE,
            is_active=True,
        )
        .on_conflict_do_update(
            index_elements=[profiles.c.pipeline_id, profiles.c.code, profiles.c.env],
            set_={
                "description": "Pending deliveries report notifications",
                "scope": PROFILE_SCOPE,
                "attach_mode": PROFILE_ATTACH_MODE,
                "is_active": True,
            },
        )
        .returning(profiles.c.id)
    )
    return connection.execute(stmt).scalar_one()


def _upsert_template(connection: sa.Connection, *, profile_id: int) -> None:
    templates = _templates_table()
    stmt = (
        postgresql.insert(templates)
        .values(
            profile_id=profile_id,
            name="default",
            subject_template=TEMPLATE_SUBJECT,
            body_template=TEMPLATE_BODY,
            is_active=True,
        )
        .on_conflict_do_update(
            index_elements=[templates.c.profile_id, templates.c.name],
            set_={
                "subject_template": TEMPLATE_SUBJECT,
                "body_template": TEMPLATE_BODY,
                "is_active": True,
            },
        )
    )
    connection.execute(stmt)


def _ensure_recipients(connection: sa.Connection, *, profile_id: int) -> None:
    recipients = _recipients_table()
    for env in RECIPIENT_ENVS:
        existing_id = connection.execute(
            sa.select(recipients.c.id)
            .where(recipients.c.profile_id == profile_id)
            .where(recipients.c.env == env)
            .where(recipients.c.email_address == RECIPIENT_EMAIL)
            .where(recipients.c.send_as == "to")
        ).scalar()
        payload = {
            "profile_id": profile_id,
            "store_code": None,
            "env": env,
            "email_address": RECIPIENT_EMAIL,
            "display_name": None,
            "send_as": "to",
            "is_active": True,
        }
        if existing_id:
            connection.execute(
                recipients.update().where(recipients.c.id == existing_id).values(payload)
            )
        else:
            connection.execute(recipients.insert().values(payload))


def upgrade() -> None:
    connection = op.get_bind()
    pipeline_id = _upsert_pipeline(connection)
    profile_id = _upsert_profile(connection, pipeline_id=pipeline_id)
    _upsert_template(connection, profile_id=profile_id)
    _ensure_recipients(connection, profile_id=profile_id)


def downgrade() -> None:
    connection = op.get_bind()
    pipelines = _pipelines_table()
    profiles = _profiles_table()
    templates = _templates_table()
    recipients = _recipients_table()

    pipeline_id = connection.execute(
        sa.select(pipelines.c.id).where(pipelines.c.code == PIPELINE_CODE)
    ).scalar()
    if not pipeline_id:
        return

    profile_ids = connection.execute(
        sa.select(profiles.c.id).where(profiles.c.pipeline_id == pipeline_id)
    ).scalars().all()

    if profile_ids:
        connection.execute(templates.delete().where(templates.c.profile_id.in_(profile_ids)))
        connection.execute(recipients.delete().where(recipients.c.profile_id.in_(profile_ids)))
        connection.execute(profiles.delete().where(profiles.c.id.in_(profile_ids)))

    connection.execute(pipelines.delete().where(pipelines.c.id == pipeline_id))
