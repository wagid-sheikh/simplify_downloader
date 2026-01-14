"""Seed notification metadata for orders sync profiler emails."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0045_orders_sync_profiler_notif"
down_revision = "0044_add_orders_sync_log_tracks"
branch_labels = None
depends_on = None

PIPELINE_CODE = "orders_sync_run_profiler"
PIPELINE_DESCRIPTION = "Orders Sync Profiler Pipeline"

PROFILE_CODE = "default"
PROFILE_ENV = "any"
PROFILE_SCOPE = "run"
PROFILE_ATTACH_MODE = "none"

RECIPIENT_EMAIL = "wagid.sheikh@gmail.com"
RECIPIENT_ENVS = ["dev", "prod", "local", "any"]

TEMPLATE_SUBJECT = "Orders Sync Profiler – {{ overall_status }}"
TEMPLATE_BODY = """
{{ (summary_text or profiler_summary_text) }}
{% if not (summary_text or profiler_summary_text) %}
Orders Sync Profiler Run Summary
Run ID: {{ run_id }} | Env: {{ run_env }}
Report Date: {{ report_date }}
Started: {{ started_at }} | Finished: {{ finished_at }}
{% if total_time_taken %}Total Duration: {{ total_time_taken }}{% endif %}
Overall Status: {{ overall_status }}

Windows Completed: {{ completed_windows or 0 }} / {{ expected_windows or 0 }}
Missing Windows: {{ missing_windows or 0 }}
{% if missing_window_stores %}Missing Window Stores: {{ missing_window_stores | join(', ') }}{% endif %}

**Per Store Summary:**
{% for store in stores %}
- {{ store.store_code or 'UNKNOWN' }} ({{ store.pipeline_name or store.pipeline_group or 'unknown' }}) — {{
  store.status or 'unknown' }}
  window_count: {{ store.window_count or 0 }}
  primary_metrics: rows_downloaded={{ store.primary_metrics.rows_downloaded or 0 }}, rows_ingested={{
    store.primary_metrics.rows_ingested or 0 }}, staging_rows={{ store.primary_metrics.staging_rows or 0 }},
    staging_inserted={{ store.primary_metrics.staging_inserted or 0 }}, staging_updated={{
    store.primary_metrics.staging_updated or 0 }}, final_inserted={{
    store.primary_metrics.final_inserted or 0 }}, final_updated={{
    store.primary_metrics.final_updated or 0 }}
  secondary_metrics: rows_downloaded={{ store.secondary_metrics.rows_downloaded or 0 }}, rows_ingested={{
    store.secondary_metrics.rows_ingested or 0 }}, staging_rows={{ store.secondary_metrics.staging_rows or 0 }},
    staging_inserted={{ store.secondary_metrics.staging_inserted or 0 }}, staging_updated={{
    store.secondary_metrics.staging_updated or 0 }}, final_inserted={{
    store.secondary_metrics.final_inserted or 0 }}, final_updated={{
    store.secondary_metrics.final_updated or 0 }}{% if store.secondary_metrics.label %} ({{
    store.secondary_metrics.label }}){% endif %}
  {% if store.status_conflict_count %}warning: {{ store.status_conflict_count }} window(s) skipped but rows present{% endif %}
{% endfor %}

Warnings:
{% if warnings %}
{% for warning in warnings %}
- {{ warning }}
{% endfor %}
{% else %}
- None.
{% endif %}
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


def _upsert_pipeline(connection) -> int:
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


def _upsert_profile(connection, *, pipeline_id: int) -> int:
    profiles = _profiles_table()
    stmt = (
        postgresql.insert(profiles)
        .values(
            pipeline_id=pipeline_id,
            code=PROFILE_CODE,
            description="Orders sync profiler run summary",
            env=PROFILE_ENV,
            scope=PROFILE_SCOPE,
            attach_mode=PROFILE_ATTACH_MODE,
            is_active=True,
        )
        .on_conflict_do_update(
            index_elements=[profiles.c.pipeline_id, profiles.c.code, profiles.c.env],
            set_={
                "description": "Orders sync profiler run summary",
                "scope": PROFILE_SCOPE,
                "attach_mode": PROFILE_ATTACH_MODE,
                "is_active": True,
            },
        )
        .returning(profiles.c.id)
    )
    return connection.execute(stmt).scalar_one()


def _upsert_template(connection, *, profile_id: int) -> None:
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


def _upsert_recipients(connection, *, profile_id: int) -> None:
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
            connection.execute(recipients.update().where(recipients.c.id == existing_id).values(payload))
        else:
            connection.execute(recipients.insert().values(payload))


def upgrade() -> None:
    connection = op.get_bind()
    pipeline_id = _upsert_pipeline(connection)
    profile_id = _upsert_profile(connection, pipeline_id=pipeline_id)
    _upsert_template(connection, profile_id=profile_id)
    _upsert_recipients(connection, profile_id=profile_id)


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

    profile_ids = [
        row["id"]
        for row in connection.execute(
            sa.select(profiles.c.id)
            .where(profiles.c.pipeline_id == pipeline_id)
            .where(profiles.c.code == PROFILE_CODE)
            .where(profiles.c.env == PROFILE_ENV)
        ).mappings()
    ]
    if profile_ids:
        connection.execute(
            recipients.delete()
            .where(recipients.c.profile_id.in_(profile_ids))
            .where(recipients.c.email_address == RECIPIENT_EMAIL)
        )
        connection.execute(
            templates.delete()
            .where(templates.c.profile_id.in_(profile_ids))
            .where(templates.c.name == "default")
        )
        connection.execute(
            profiles.delete()
            .where(profiles.c.id.in_(profile_ids))
            .where(profiles.c.code == PROFILE_CODE)
            .where(profiles.c.env == PROFILE_ENV)
        )
    connection.execute(pipelines.delete().where(pipelines.c.id == pipeline_id))
