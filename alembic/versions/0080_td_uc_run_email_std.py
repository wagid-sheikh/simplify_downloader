"""Standardize TD/UC run email subject/body templates; include store scope compatibility."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0080_td_uc_run_email_std"
down_revision = "0079_td_uc_store_body_sections"
branch_labels = None
depends_on = None


pipelines = sa.table(
    "pipelines",
    sa.column("id", sa.BigInteger()),
    sa.column("code", sa.Text()),
)

notification_profiles = sa.table(
    "notification_profiles",
    sa.column("id", sa.BigInteger()),
    sa.column("pipeline_id", sa.BigInteger()),
    sa.column("scope", sa.Text()),
    sa.column("is_active", sa.Boolean()),
)

email_templates = sa.table(
    "email_templates",
    sa.column("id", sa.BigInteger()),
    sa.column("profile_id", sa.BigInteger()),
    sa.column("subject_template", sa.Text()),
    sa.column("body_template", sa.Text()),
    sa.column("is_active", sa.Boolean()),
)

PIPELINE_CODES = ("td_orders_sync", "uc_orders_sync")
TARGET_SCOPES = ("run", "store")

STANDARD_SUBJECT_TEMPLATE = (
    "ETL - [{{ env_upper }}][{{ overall_status_upper }}][{{ store_code }}] "
    "{{ pipeline_display_name }} – {{ run_date_display }}"
)

STANDARD_BODY_TEMPLATE = """PIPELINE RUN SUMMARY
- pipeline: {{ pipeline_display_name or pipeline_code or 'n/a' }}
- run_id: {{ run_id or 'n/a' }}
- env: {{ run_env or env or 'n/a' }}
- report_date: {{ report_date or run_date_display or 'n/a' }}
- started_at: {{ started_at_formatted or started_at or 'n/a' }}
- finished_at: {{ finished_at_formatted or finished_at or 'n/a' }}
- duration: {{ total_time_taken or 'n/a' }}
- overall_status: {{ overall_status_label or td_overall_status or overall_status or 'unknown' }}

WINDOW STATUS
- windows_completed: {{ completed_windows or 0 }} / {{ expected_windows or 0 }}
- missing_windows: {{ missing_windows or 0 }}
{% if missing_window_stores %}- missing_window_stores: {{ missing_window_stores | join(', ') }}
{% endif %}

STORE PROCESSING SUMMARY
{% if stores %}{% for store in stores %}- {{ store.store_code or 'UNKNOWN' }} — {{ store.status or store.orders_status or store.sales_status or 'unknown' }}
{% endfor %}{% else %}- (none)
{% endif %}

FILES PROCESSED
{% if stores %}{% for store in stores %}- {{ store.store_code or 'UNKNOWN' }}: {{ store.filename or store.file_name or 'n/a' }}
{% endfor %}{% else %}- (none)
{% endif %}

WARNINGS
{% if warnings %}{% for warning in warnings %}- {{ warning }}
{% endfor %}{% else %}- (none)
{% endif %}{% if notes %}

NOTES
{{ notes }}
{% endif %}"""

PREVIOUS_SUBJECT_TEMPLATES = {
    "run": {
        "td_orders_sync": "TD Orders Sync – {{ overall_status }}",
        "uc_orders_sync": "UC Orders Sync – {{ overall_status }}",
    },
    "store": {
        "td_orders_sync": STANDARD_SUBJECT_TEMPLATE,
        "uc_orders_sync": STANDARD_SUBJECT_TEMPLATE,
    },
}

PREVIOUS_BODY_TEMPLATES = {
    "run": {
        "td_orders_sync": """
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
""",
        "uc_orders_sync": """
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
""",
    },
    "store": {
        "td_orders_sync": STANDARD_BODY_TEMPLATE,
        "uc_orders_sync": STANDARD_BODY_TEMPLATE,
    },
}


def _target_templates(bind: sa.Connection) -> list[tuple[int, str, str]]:
    rows = bind.execute(
        sa.select(email_templates.c.id, pipelines.c.code, notification_profiles.c.scope)
        .select_from(
            email_templates.join(
                notification_profiles,
                email_templates.c.profile_id == notification_profiles.c.id,
            ).join(pipelines, notification_profiles.c.pipeline_id == pipelines.c.id)
        )
        .where(pipelines.c.code.in_(PIPELINE_CODES))
        .where(notification_profiles.c.scope.in_(TARGET_SCOPES))
        .where(notification_profiles.c.is_active.is_(True))
        .where(email_templates.c.is_active.is_(True))
    ).all()
    return [(int(row.id), str(row.code), str(row.scope)) for row in rows]


def upgrade() -> None:
    bind = op.get_bind()
    template_ids = [template_id for template_id, _, _ in _target_templates(bind)]
    if not template_ids:
        return

    bind.execute(
        email_templates.update()
        .where(email_templates.c.id.in_(template_ids))
        .values(
            subject_template=STANDARD_SUBJECT_TEMPLATE,
            body_template=STANDARD_BODY_TEMPLATE,
        )
    )


def downgrade() -> None:
    bind = op.get_bind()
    template_rows = _target_templates(bind)

    for template_id, pipeline_code, scope in template_rows:
        bind.execute(
            email_templates.update()
            .where(email_templates.c.id == template_id)
            .where(email_templates.c.subject_template == STANDARD_SUBJECT_TEMPLATE)
            .where(email_templates.c.body_template == STANDARD_BODY_TEMPLATE)
            .values(
                subject_template=PREVIOUS_SUBJECT_TEMPLATES[scope][pipeline_code],
                body_template=PREVIOUS_BODY_TEMPLATES[scope][pipeline_code],
            )
        )
