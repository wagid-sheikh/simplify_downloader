"""Add missing window lines to TD/UC order summary templates."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0046_add_missing_window_lines"
down_revision = "0045_orders_sync_profiler_notif"
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
)

email_templates = sa.table(
    "email_templates",
    sa.column("id"),
    sa.column("profile_id"),
    sa.column("name"),
    sa.column("subject_template"),
    sa.column("body_template"),
)

TD_BODY_TEMPLATE_WITH_MISSING = """
{{ (summary_text or td_summary_text) }}
{% if not (summary_text or td_summary_text) %}
TD Orders & Sales Run Summary
Run ID: {{ run_id }} | Env: {{ run_env }}
Report Date: {{ report_date }}
Started (Asia/Kolkata): {{ started_at_formatted }}
Finished (Asia/Kolkata): {{ finished_at_formatted }}
{% if total_time_taken %}Total Duration: {{ total_time_taken }}{% endif %}
Overall Status: {{ td_overall_status or overall_status }} (Orders: {{ orders_status }}, Sales: {{ sales_status }})

**Per Store Orders Metrics:**
{% for store in stores %}
- {{ store.store_code or 'UNKNOWN' }} — {{ (store.orders_status or 'unknown')|upper }}
  rows_downloaded: {{ store.orders_rows_downloaded or 0 }}
  rows_ingested: {{ store.orders_rows_ingested or store.orders_final_rows or store.orders_staging_rows or 0 }}
  warning_count: {{ store.orders_warning_count or 0 }}
  dropped_count: {{ store.orders_dropped_rows_count or 0 }}
  {% if store.missing_window_lines %}missing_windows: {{ store.missing_window_lines | join(', ') }}{% endif %}
{% endfor %}

**Per Store Sales Metrics:**
{% for store in stores %}
- {{ store.store_code or 'UNKNOWN' }} — {{ (store.sales_status or 'unknown')|upper }}
  rows_downloaded: {{ store.sales_rows_downloaded or 0 }}
  rows_ingested: {{ store.sales_rows_ingested or store.sales_final_rows or store.sales_staging_rows or 0 }}
  warning_count: {{ store.sales_warning_count or 0 }}
  dropped_count: {{ store.sales_dropped_rows_count or 0 }}
  edited_count: {{ store.sales_rows_edited or 0 }}
  duplicate_count: {{ store.sales_rows_duplicate or 0 }}
{% endfor %}
{% if td_all_stores_failed %}
All TD stores failed for Orders and Sales.
{% endif %}
{% endif %}
"""

TD_BODY_TEMPLATE_WITHOUT_MISSING = """
{{ (summary_text or td_summary_text) }}
{% if not (summary_text or td_summary_text) %}
TD Orders & Sales Run Summary
Run ID: {{ run_id }} | Env: {{ run_env }}
Report Date: {{ report_date }}
Started (Asia/Kolkata): {{ started_at_formatted }}
Finished (Asia/Kolkata): {{ finished_at_formatted }}
{% if total_time_taken %}Total Duration: {{ total_time_taken }}{% endif %}
Overall Status: {{ td_overall_status or overall_status }} (Orders: {{ orders_status }}, Sales: {{ sales_status }})

**Per Store Orders Metrics:**
{% for store in stores %}
- {{ store.store_code or 'UNKNOWN' }} — {{ (store.orders_status or 'unknown')|upper }}
  rows_downloaded: {{ store.orders_rows_downloaded or 0 }}
  rows_ingested: {{ store.orders_rows_ingested or store.orders_final_rows or store.orders_staging_rows or 0 }}
  warning_count: {{ store.orders_warning_count or 0 }}
  dropped_count: {{ store.orders_dropped_rows_count or 0 }}
{% endfor %}

**Per Store Sales Metrics:**
{% for store in stores %}
- {{ store.store_code or 'UNKNOWN' }} — {{ (store.sales_status or 'unknown')|upper }}
  rows_downloaded: {{ store.sales_rows_downloaded or 0 }}
  rows_ingested: {{ store.sales_rows_ingested or store.sales_final_rows or store.sales_staging_rows or 0 }}
  warning_count: {{ store.sales_warning_count or 0 }}
  dropped_count: {{ store.sales_dropped_rows_count or 0 }}
  edited_count: {{ store.sales_rows_edited or 0 }}
  duplicate_count: {{ store.sales_rows_duplicate or 0 }}
{% endfor %}
{% if td_all_stores_failed %}
All TD stores failed for Orders and Sales.
{% endif %}
{% endif %}
"""

UC_BODY_TEMPLATE_WITH_MISSING = """
UC Orders Sync Run Summary
Run ID: {{ run_id }} | Env: {{ run_env }}
Report Date: {{ report_date }}
Started: {{ started_at }} | Finished: {{ finished_at }}
{% if total_time_taken %}Total Duration: {{ total_time_taken }}{% endif %}
Overall Status: {{ overall_status }}

**Per Store UC Metrics:**
{% for store in stores %}
- {{ store.store_code or 'UNKNOWN' }} — {{ (store.status or 'unknown')|upper }}
  filename: {{ store.filename or 'n/a' }}
  staging_inserted: {{ store.staging_inserted or 0 }}
  staging_updated: {{ store.staging_updated or 0 }}
  final_inserted: {{ store.final_inserted or 0 }}
  final_updated: {{ store.final_updated or 0 }}
  warning_count: {{ store.warning_count or 0 }}
  {% if store.missing_window_lines %}missing_windows: {{ store.missing_window_lines | join(', ') }}{% endif %}
  {% if store.error_message %}error: {{ store.error_message }}{% endif %}
{% endfor %}

{% if overall_status in ['ok', 'success'] %}
All UC stores completed successfully. Upsert using (cost_center, order_number, invoice_date) to keep reruns idempotent.
{% elif overall_status in ['warning', 'partial', 'skipped'] %}
{% if uc_all_stores_failed %}
All UC stores failed. Review the errors above before reattempting the sync.
{% else %}
Mixed UC outcomes: review warning/error stores above, fix issues, and rerun; unique constraints prevent duplicate rows on retry.
{% endif %}
{% else %}
{% if uc_all_stores_failed %}
All UC stores failed. Review the errors above before reattempting the sync.
{% else %}
UC sync failed after mixed store outcomes. Review failures above and retry once resolved.
{% endif %}
{% endif %}
"""

UC_BODY_TEMPLATE_WITHOUT_MISSING = """
UC Orders Sync Run Summary
Run ID: {{ run_id }} | Env: {{ run_env }}
Report Date: {{ report_date }}
Started: {{ started_at }} | Finished: {{ finished_at }}
{% if total_time_taken %}Total Duration: {{ total_time_taken }}{% endif %}
Overall Status: {{ overall_status }}

**Per Store UC Metrics:**
{% for store in stores %}
- {{ store.store_code or 'UNKNOWN' }} — {{ (store.status or 'unknown')|upper }}
  filename: {{ store.filename or 'n/a' }}
  staging_inserted: {{ store.staging_inserted or 0 }}
  staging_updated: {{ store.staging_updated or 0 }}
  final_inserted: {{ store.final_inserted or 0 }}
  final_updated: {{ store.final_updated or 0 }}
  warning_count: {{ store.warning_count or 0 }}
  {% if store.error_message %}error: {{ store.error_message }}{% endif %}
{% endfor %}

{% if overall_status in ['ok', 'success'] %}
All UC stores completed successfully. Upsert using (cost_center, order_number, invoice_date) to keep reruns idempotent.
{% elif overall_status in ['warning', 'partial', 'skipped'] %}
{% if uc_all_stores_failed %}
All UC stores failed. Review the errors above before reattempting the sync.
{% else %}
Mixed UC outcomes: review warning/error stores above, fix issues, and rerun; unique constraints prevent duplicate rows on retry.
{% endif %}
{% else %}
{% if uc_all_stores_failed %}
All UC stores failed. Review the errors above before reattempting the sync.
{% else %}
UC sync failed after mixed store outcomes. Review failures above and retry once resolved.
{% endif %}
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
            sa.select(notification_profiles.c.id).where(notification_profiles.c.pipeline_id.in_(pipeline_ids))
        )
        .scalars()
        .all()
    )


def _update_templates(connection, *, pipeline_code: str, body_template: str) -> None:
    profile_ids = _fetch_profile_ids(connection, pipeline_code=pipeline_code)
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
    _update_templates(connection, pipeline_code="td_orders_sync", body_template=TD_BODY_TEMPLATE_WITH_MISSING)
    _update_templates(connection, pipeline_code="uc_orders_sync", body_template=UC_BODY_TEMPLATE_WITH_MISSING)


def downgrade() -> None:
    connection = op.get_bind()
    _update_templates(connection, pipeline_code="td_orders_sync", body_template=TD_BODY_TEMPLATE_WITHOUT_MISSING)
    _update_templates(connection, pipeline_code="uc_orders_sync", body_template=UC_BODY_TEMPLATE_WITHOUT_MISSING)
