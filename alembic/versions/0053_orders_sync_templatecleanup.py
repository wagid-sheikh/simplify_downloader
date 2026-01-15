"""Update orders sync templates to use fact sections and drop legacy row lists."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0053_orders_sync_templatecleanup"
down_revision = "0052_orders_sync_fact_sections"
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

TD_BODY_TEMPLATE = """
TD Orders & Sales Run Summary
Run ID: {{ run_id }} | Env: {{ run_env }}
Report Date: {{ report_date }}
Started (Asia/Kolkata): {{ started_at_formatted }}
Finished (Asia/Kolkata): {{ finished_at_formatted }}
{% if total_time_taken %}Total Duration: {{ total_time_taken }}{% endif %}
Overall Status: {{ td_overall_status or overall_status }} (Orders: {{ orders_status }}, Sales: {{ sales_status }})
{% if expected_windows or completed_windows %}
Windows Completed: {{ completed_windows or 0 }} / {{ expected_windows or 0 }}
{% endif %}
{% if missing_windows is not none %}Missing Windows: {{ missing_windows or 0 }}{% endif %}
{% if missing_window_stores %}Missing Window Stores: {{ missing_window_stores | join(', ') }}{% endif %}

**Per Store Orders Metrics:**
{% for store in stores %}
- {{ store.store_code or 'UNKNOWN' }} — {{ (store.orders_status or 'unknown')|upper }}
  rows_downloaded: {{ store.orders_rows_downloaded or 0 }}
  rows_ingested: {{ store.orders_rows_ingested or store.orders_final_rows or store.orders_staging_rows or 0 }}
  inserted: {{ store.primary_final_inserted or store.primary_staging_inserted or 0 }}
  updated: {{ store.primary_final_updated or store.primary_staging_updated or 0 }}
  {% if store.missing_window_lines %}missing_windows: {{ store.missing_window_lines | join(', ') }}{% endif %}
{% endfor %}

**Per Store Sales Metrics:**
{% for store in stores %}
- {{ store.store_code or 'UNKNOWN' }} — {{ (store.sales_status or 'unknown')|upper }}
  rows_downloaded: {{ store.sales_rows_downloaded or 0 }}
  rows_ingested: {{ store.sales_rows_ingested or store.sales_final_rows or store.sales_staging_rows or 0 }}
  inserted: {{ store.secondary_final_inserted or store.secondary_staging_inserted or 0 }}
  updated: {{ store.secondary_final_updated or store.secondary_staging_updated or 0 }}
{% endfor %}
{% if td_all_stores_failed %}
All TD stores failed for Orders and Sales.
{% endif %}

{% if fact_sections_text %}
Row-level facts:
{{ fact_sections_text }}
{% elif summary_text %}
{{ summary_text }}
{% endif %}
"""

UC_BODY_TEMPLATE = """
UC Orders Sync Run Summary
Run ID: {{ run_id }} | Env: {{ run_env }}
Report Date: {{ report_date }}
Started: {{ started_at }} | Finished: {{ finished_at }}
{% if total_time_taken %}Total Duration: {{ total_time_taken }}{% endif %}
Overall Status: {{ overall_status_label or overall_status }}{% if overall_status_explanation %} ({{ overall_status_explanation }}){% endif %}

**Per Store UC Metrics:**
{% for store in stores %}
- {{ store.store_code or 'UNKNOWN' }} — {{ (store.status or 'unknown')|upper }}
  filename: {{ store.filename or 'n/a' }}
  rows_downloaded: {{ store.rows_downloaded or 0 }}
  rows_ingested: {{ store.final_rows or store.staging_rows or 0 }}
  inserted: {{ store.final_inserted or store.staging_inserted or 0 }}
  updated: {{ store.final_updated or store.staging_updated or 0 }}
  warning_count: {{ store.warning_count or 0 }}
  dropped_count: {{ store.rows_skipped_invalid or 0 }}
  {% if store.missing_window_lines %}missing_windows:{% for window in store.missing_window_lines %}
    - {{ window }}{% endfor %}
  {% endif %}
  {% if store.error_message %}error: {{ store.error_message }}{% endif %}
  {% if store.fact_sections_text %}
  row_facts:
{{ store.fact_sections_text | indent(4, true) }}
  {% endif %}
{% endfor %}

{% if overall_status == 'success' %}
All UC stores completed successfully.
{% elif overall_status == 'success_with_warnings' %}
Completed with warnings. See warnings below.
{% elif overall_status in ['partial', 'failed'] %}
{% if uc_all_stores_failed %}
All UC stores failed. Review the errors above before reattempting the sync.
{% else %}
Review error stores above, fix issues, and rerun.
{% endif %}
{% else %}
Review error stores above, fix issues, and rerun.
{% endif %}

Warnings:
{% if warnings %}
{% for warning in warnings %}
- {{ warning }}
{% endfor %}
{% else %}
- None.
{% endif %}

Deterministic filenames:
{% for store in stores %}
- {{ store.store_code or 'UNKNOWN' }}: {{ store.filename or 'n/a' }}
{% endfor %}
Filename format: {STORE_CODE}_uc_gst_YYYYMMDD_YYYYMMDD.xlsx

{% if fact_sections_text %}
Row-level facts:
{{ fact_sections_text }}
{% elif summary_text %}
{{ summary_text }}
{% endif %}
"""

PROFILER_BODY_TEMPLATE = """
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

TD_PREVIOUS_BODY_TEMPLATE = """
TD Orders & Sales Run Summary
Run ID: {{ run_id }} | Env: {{ run_env }}
Report Date: {{ report_date }}
Started (Asia/Kolkata): {{ started_at_formatted }}
Finished (Asia/Kolkata): {{ finished_at_formatted }}
{% if total_time_taken %}Total Duration: {{ total_time_taken }}{% endif %}
Overall Status: {{ td_overall_status or overall_status }} (Orders: {{ orders_status }}, Sales: {{ sales_status }})
{% if expected_windows or completed_windows %}
Windows Completed: {{ completed_windows or 0 }} / {{ expected_windows or 0 }}
{% endif %}
{% if missing_windows is not none %}Missing Windows: {{ missing_windows or 0 }}{% endif %}
{% if missing_window_stores %}Missing Window Stores: {{ missing_window_stores | join(', ') }}{% endif %}

**Per Store Orders Metrics:**
{% for store in stores %}
- {{ store.store_code or 'UNKNOWN' }} — {{ (store.orders_status or 'unknown')|upper }}
  rows_downloaded: {{ store.orders_rows_downloaded or 0 }}
  rows_ingested: {{ store.orders_rows_ingested or store.orders_final_rows or store.orders_staging_rows or 0 }}
  inserted: {{ store.primary_final_inserted or store.primary_staging_inserted or 0 }}
  updated: {{ store.primary_final_updated or store.primary_staging_updated or 0 }}
  warning_count: {{ store.orders_warning_count or 0 }}
  dropped_count: {{ store.orders_dropped_rows_count or 0 }}
  {% if store.missing_window_lines %}missing_windows: {{ store.missing_window_lines | join(', ') }}{% endif %}
{% endfor %}

**Per Store Sales Metrics:**
{% for store in stores %}
- {{ store.store_code or 'UNKNOWN' }} — {{ (store.sales_status or 'unknown')|upper }}
  rows_downloaded: {{ store.sales_rows_downloaded or 0 }}
  rows_ingested: {{ store.sales_rows_ingested or store.sales_final_rows or store.sales_staging_rows or 0 }}
  inserted: {{ store.secondary_final_inserted or store.secondary_staging_inserted or 0 }}
  updated: {{ store.secondary_final_updated or store.secondary_staging_updated or 0 }}
  warning_count: {{ store.sales_warning_count or 0 }}
  dropped_count: {{ store.sales_dropped_rows_count or 0 }}
  edited_count: {{ store.sales_rows_edited or 0 }}
  duplicate_count: {{ store.sales_rows_duplicate or 0 }}
{% endfor %}
{% if td_all_stores_failed %}
All TD stores failed for Orders and Sales.
{% endif %}

{% if fact_sections_text %}
Row-level facts:
{{ fact_sections_text }}
{% elif summary_text %}
{{ summary_text }}
{% endif %}
"""

UC_PREVIOUS_BODY_TEMPLATE = """
UC Orders Sync Run Summary
Run ID: {{ run_id }} | Env: {{ run_env }}
Report Date: {{ report_date }}
Started: {{ started_at }} | Finished: {{ finished_at }}
{% if total_time_taken %}Total Duration: {{ total_time_taken }}{% endif %}
Overall Status: {{ overall_status_label or overall_status }}{% if overall_status_explanation %} ({{ overall_status_explanation }}){% endif %}

**Per Store UC Metrics:**
{% for store in stores %}
- {{ store.store_code or 'UNKNOWN' }} — {{ (store.status or 'unknown')|upper }}
  filename: {{ store.filename or 'n/a' }}
  rows_downloaded: {{ store.rows_downloaded or 0 }}
  rows_ingested: {{ store.final_rows or store.staging_rows or 0 }}
  inserted: {{ store.final_inserted or store.staging_inserted or 0 }}
  updated: {{ store.final_updated or store.staging_updated or 0 }}
  warning_count: {{ store.warning_count or 0 }}
  dropped_count: {{ store.rows_skipped_invalid or 0 }}
  {% if store.missing_window_lines %}missing_windows:{% for window in store.missing_window_lines %}
    - {{ window }}{% endfor %}
  {% endif %}
  {% if store.error_message %}error: {{ store.error_message }}{% endif %}
  {% if store.fact_sections_text %}
  row_facts:
{{ store.fact_sections_text | indent(4, true) }}
  {% endif %}
{% endfor %}

{% if overall_status == 'success' %}
All UC stores completed successfully.
{% elif overall_status == 'success_with_warnings' %}
Completed with warnings. See warnings below.
{% elif overall_status in ['partial', 'failed'] %}
{% if uc_all_stores_failed %}
All UC stores failed. Review the errors above before reattempting the sync.
{% else %}
Review error stores above, fix issues, and rerun.
{% endif %}
{% else %}
Review error stores above, fix issues, and rerun.
{% endif %}

Warnings:
{% if warnings %}
{% for warning in warnings %}
- {{ warning }}
{% endfor %}
{% else %}
- None.
{% endif %}

Deterministic filenames:
{% for store in stores %}
- {{ store.store_code or 'UNKNOWN' }}: {{ store.filename or 'n/a' }}
{% endfor %}
Filename format: {STORE_CODE}_uc_gst_YYYYMMDD_YYYYMMDD.xlsx
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


def _fetch_profile_ids(connection, *, pipeline_code: str) -> list[int]:
    pipeline_ids = (
        connection.execute(sa.select(pipelines.c.id).where(pipelines.c.code == pipeline_code)).scalars().all()
    )
    if not pipeline_ids:
        return []
    return (
        connection.execute(
            sa.select(notification_profiles.c.id)
            .where(notification_profiles.c.pipeline_id.in_(pipeline_ids))
            .where(notification_profiles.c.is_active.is_(True))
        )
        .scalars()
        .all()
    )


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
        profile_ids=_fetch_profile_ids(connection, pipeline_code="td_orders_sync"),
        body_template=TD_BODY_TEMPLATE,
    )
    _update_templates(
        connection,
        profile_ids=_fetch_profile_ids(connection, pipeline_code="uc_orders_sync"),
        body_template=UC_BODY_TEMPLATE,
    )
    _update_templates(
        connection,
        profile_ids=_fetch_profiler_profile_ids(connection),
        body_template=PROFILER_BODY_TEMPLATE,
    )


def downgrade() -> None:
    connection = op.get_bind()
    _update_templates(
        connection,
        profile_ids=_fetch_profile_ids(connection, pipeline_code="td_orders_sync"),
        body_template=TD_PREVIOUS_BODY_TEMPLATE,
    )
    _update_templates(
        connection,
        profile_ids=_fetch_profile_ids(connection, pipeline_code="uc_orders_sync"),
        body_template=UC_PREVIOUS_BODY_TEMPLATE,
    )
    _update_templates(
        connection,
        profile_ids=_fetch_profiler_profile_ids(connection),
        body_template=PROFILER_PREVIOUS_BODY_TEMPLATE,
    )
