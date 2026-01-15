"""Update UC orders sync email template with deterministic footer."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0050_uc_orders_determinisfootr"
down_revision = "0049_widen_orders_sync_logstatus"
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

NEW_UC_BODY_TEMPLATE = """
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
  {% if store.missing_window_lines %}missing_windows:{% for window in store.missing_window_lines %}
    - {{ window }}{% endfor %}
  {% endif %}
  {% if store.error_message %}error: {{ store.error_message }}{% endif %}
{% endfor %}

{% if overall_status == 'success' %}
All UC stores completed successfully.
{% elif overall_status == 'success_with_warnings' %}
Completed with warnings. See warnings below.
{% elif overall_status in ['partial', 'failed'] %}
{% if uc_all_stores_failed %}
All UC stores failed. Review the errors above before reattempting the sync.
{% else %}
Review error stores above, fix issues, and rerun; unique constraints prevent duplicate rows on retry.
{% endif %}
{% else %}
Review error stores above, fix issues, and rerun; unique constraints prevent duplicate rows on retry.
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

OLD_UC_BODY_TEMPLATE = """
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
  {% if store.missing_window_lines %}missing_windows:{% for window in store.missing_window_lines %}
    - {{ window }}{% endfor %}
  {% endif %}
  {% if store.error_message %}error: {{ store.error_message }}{% endif %}
{% endfor %}

{% if overall_status == 'success' %}
All UC stores completed successfully.
{% elif overall_status == 'success_with_warnings' %}
Completed with warnings. See warnings below.
{% elif overall_status in ['partial', 'failed'] %}
{% if uc_all_stores_failed %}
All UC stores failed. Review the errors above before reattempting the sync.
{% else %}
Review error stores above, fix issues, and rerun; unique constraints prevent duplicate rows on retry.
{% endif %}
{% else %}
Review error stores above, fix issues, and rerun; unique constraints prevent duplicate rows on retry.
{% endif %}

{% if warnings %}
Warnings:
{% for warning in warnings %}
- {{ warning }}
{% endfor %}
{% endif %}
"""


def _fetch_uc_profile_ids(connection) -> list[int]:
    pipeline_ids = (
        connection.execute(sa.select(pipelines.c.id).where(pipelines.c.code == "uc_orders_sync")).scalars().all()
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


def upgrade() -> None:
    connection = op.get_bind()
    profile_ids = _fetch_uc_profile_ids(connection)
    if not profile_ids:
        return
    connection.execute(
        email_templates.update()
        .where(email_templates.c.profile_id.in_(profile_ids))
        .where(email_templates.c.name == "default")
        .values(body_template=NEW_UC_BODY_TEMPLATE)
    )


def downgrade() -> None:
    connection = op.get_bind()
    profile_ids = _fetch_uc_profile_ids(connection)
    if not profile_ids:
        return
    connection.execute(
        email_templates.update()
        .where(email_templates.c.profile_id.in_(profile_ids))
        .where(email_templates.c.name == "default")
        .values(body_template=OLD_UC_BODY_TEMPLATE)
    )
