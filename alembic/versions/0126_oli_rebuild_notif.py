"""Seed notification metadata for order line items rebuild emails."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0126_oli_rebuild_notif"
down_revision = "0125_drop_launch_date"
branch_labels = None
depends_on = None

PIPELINE_CODE = "order_line_items_rebuild"
PIPELINE_DESCRIPTION = "Order Line Items Rebuild Pipeline"

PROFILE_CODE = "default"
PROFILE_ENV = "any"
PROFILE_SCOPE = "run"
PROFILE_ATTACH_MODE = "none"

RECIPIENT_EMAIL = "wagid.sheikh@gmail.com"
RECIPIENT_ENVS = ["dev", "prod", "local", "any"]

TEMPLATE_SUBJECT = "Order Line Items Rebuild – {{ overall_status }}"
TEMPLATE_BODY = """
{% set payload = metrics_json.get('notification_payload', {}) %}
{% set zero_counts = payload.get('zero_snapshot_counts', {}) %}
Order Line Items Rebuild Run Summary
Run ID: {{ run_id }}
Environment: {{ run_env }}
Pipeline: {{ pipeline_name }}
Overall Status: {{ overall_status }}

Timing:
- Started: {{ payload.get('started_at') or started_at }}
- Finished: {{ payload.get('finished_at') or finished_at }}
- Duration: {{ payload.get('total_time_taken') or total_time_taken }}

Selection:
- Source Selection: {{ payload.get('source_selection') or 'unknown' }}
- Sources: {{ payload.get('sources', []) | join(', ') if payload.get('sources') else 'unknown' }}
- Stores: {{ payload.get('selected_stores', []) | join(', ') if payload.get('selected_stores') else 'all selected stores' }}
- Dry Run: {{ payload.get('dry_run') }}
- Resume: {{ payload.get('resume') }}
- Resume Run ID: {{ payload.get('resume_run_id') or 'none' }}

Windows:
- Expected: {{ payload.get('expected_window_count', 0) }}
- Completed: {{ payload.get('completed_window_count', 0) }}
- Missing: {{ payload.get('missing_window_count', 0) }}
- Skipped/Resumable: {{ payload.get('skipped_window_count', 0) }}

Zero Snapshot Warnings:
- Total Zero Snapshots: {{ zero_counts.get('zero_snapshot_count', 0) }}
- Suspicious: {{ zero_counts.get('suspicious_zero_snapshot_count', 0) }}
- Ambiguous: {{ zero_counts.get('ambiguous_zero_snapshot_count', 0) }}
- Source Fetch Failures: {{ zero_counts.get('source_fetch_failure_zero_snapshot_count', 0) }}
- Confirmed Source Empty: {{ zero_counts.get('confirmed_empty_snapshot_count', 0) }}

{% if payload.get('missing_windows') %}
Missing Windows:
{% for window in payload.get('missing_windows', []) %}
- {{ window.source }}:{{ window.store_code }} {{ window.window_start }}..{{ window.window_end }}
{% endfor %}
{% endif %}
{% if payload.get('skipped_windows') %}
Skipped/Resumable Windows:
{% for window in payload.get('skipped_windows', []) %}
- {{ window.source }}:{{ window.store_code }} {{ window.window_start }}..{{ window.window_end }}
{% endfor %}
{% endif %}

Outcome:
{% if overall_status == 'success' %}
- Success: all expected rebuild windows completed without warnings.
{% elif overall_status == 'failed' %}
- Failure: rebuild failed before all expected windows completed. Review warnings and run logs.
{% else %}
- Warning: rebuild completed with missing, skipped, or suspicious zero-snapshot windows.
{% endif %}

Warnings:
{% if payload.get('warnings') %}
{% for warning in payload.get('warnings', []) %}
- {{ warning }}
{% endfor %}
{% else %}
- None.
{% endif %}

Summary Text:
{{ summary_text or 'No additional summary text recorded.' }}
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
            description="Order line items rebuild run summary",
            env=PROFILE_ENV,
            scope=PROFILE_SCOPE,
            attach_mode=PROFILE_ATTACH_MODE,
            is_active=True,
        )
        .on_conflict_do_update(
            index_elements=[profiles.c.pipeline_id, profiles.c.code, profiles.c.env],
            set_={
                "description": "Order line items rebuild run summary",
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
