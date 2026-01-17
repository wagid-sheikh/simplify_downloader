"""Simplify orders sync profiler email template."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0055_simplify_profiler_email_tem"
down_revision = "0054_uc_store_fact_sections_temp"
branch_labels = None
depends_on = None


pipelines = sa.table(
    "pipelines",
    sa.column("id"),
    sa.column("code"),
)

notification_profiles = sa.table(
    "notification_profiles",
    sa.column("id"),
    sa.column("pipeline_id"),
    sa.column("code"),
    sa.column("env"),
    sa.column("scope"),
    sa.column("is_active"),
)

email_templates = sa.table(
    "email_templates",
    sa.column("id"),
    sa.column("profile_id"),
    sa.column("name"),
    sa.column("subject_template"),
    sa.column("body_template"),
)

PROFILER_BODY_TEMPLATE = """
Orders Sync Profiler Run Summary

Overall Run Summary
Run ID: {{ run_id }} | Env: {{ run_env }}
Report Date: {{ report_date }}
Started: {{ started_at }}
Finished: {{ finished_at }}
{% if total_time_taken %}Total Duration: {{ total_time_taken }}{% endif %}
Run Status: {{ overall_status_label or overall_status }}

Windows Completed: {{ completed_windows or 0 }} / {{ expected_windows or 0 }}
Missing Windows: {{ missing_windows or 0 }}
{% if missing_window_stores %}Missing Window Stores: {{ missing_window_stores | join(', ') }}{% endif %}

**Store Window Run Summary**
| Store | Pipeline | Status | Windows | Primary Inserted | Primary Updated | Secondary Inserted | Secondary Updated | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
{% for store in stores %}
| {{ store.store_code or 'UNKNOWN' }} | {{ store.pipeline_name or store.pipeline_group or 'unknown' }} | {{ store.status or 'unknown' }} | {{ store.window_count or 0 }} | {{ store.primary_metrics.final_inserted or store.primary_metrics.staging_inserted or 0 }} | {{ store.primary_metrics.final_updated or store.primary_metrics.staging_updated or 0 }} | {{ store.secondary_metrics.final_inserted or store.secondary_metrics.staging_inserted or 0 }} | {{ store.secondary_metrics.final_updated or store.secondary_metrics.staging_updated or 0 }} | {% if store.status_conflict_count %}{{ store.status_conflict_count }} window(s) skipped but rows present{% elif store.secondary_metrics.label %}{{ store.secondary_metrics.label }}{% else %}—{% endif %} |
{% endfor %}

{% if warnings %}
Warnings:
{% for warning in warnings %}
- {{ warning }}
{% endfor %}
{% endif %}

{% if fact_sections_text %}
Row-level facts:
{{ fact_sections_text }}
{% elif summary_text %}
{{ summary_text }}
{% endif %}
"""

PROFILER_PREVIOUS_BODY_TEMPLATE = """
Orders Sync Profiler Run Summary
Run ID: {{ run_id }} | Env: {{ run_env }}
Report Date: {{ report_date }}
Started: {{ started_at }} | Finished: {{ finished_at }}
{% if total_time_taken %}Total Duration: {{ total_time_taken }}{% endif %}
Overall Status: {{ overall_status_label or overall_status }}

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

{% if fact_sections_text %}
Row-level facts:
{{ fact_sections_text }}
{% elif summary_text %}
{{ summary_text }}
{% endif %}
"""


def _fetch_profiler_profile_ids(connection) -> list[int]:
    pipeline_id = connection.execute(
        sa.select(pipelines.c.id).where(pipelines.c.code == "orders_sync_run_profiler")
    ).scalar()
    if not pipeline_id:
        return []
    return (
        connection.execute(
            sa.select(notification_profiles.c.id)
            .where(notification_profiles.c.pipeline_id == pipeline_id)
            .where(notification_profiles.c.code == "default")
            .where(notification_profiles.c.env == "any")
            .where(notification_profiles.c.scope == "run")
            .where(notification_profiles.c.is_active.is_(True))
        )
        .scalars()
        .all()
    )


def _update_templates(connection, *, profile_ids: list[int], body_template: str) -> None:
    if not profile_ids:
        return
    connection.execute(
        email_templates.update()
        .where(email_templates.c.profile_id.in_(profile_ids))
        .where(email_templates.c.name == "default")
        .values(body_template=body_template)
    )


def upgrade() -> None:
    connection = op.get_bind()
    _update_templates(
        connection,
        profile_ids=_fetch_profiler_profile_ids(connection),
        body_template=PROFILER_BODY_TEMPLATE,
    )


def downgrade() -> None:
    connection = op.get_bind()
    _update_templates(
        connection,
        profile_ids=_fetch_profiler_profile_ids(connection),
        body_template=PROFILER_PREVIOUS_BODY_TEMPLATE,
    )
