"""Align UC Orders notifications with structured summary payload."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0039_uc_orders_pipe_notitempl"
down_revision = "0038_stg_uc_orders_mobile_null"
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

NEW_BODY_TEMPLATE = """
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
{% elif overall_status in ['warning', 'partial', 'skipped', 'success_with_warnings'] %}
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

OLD_BODY_TEMPLATE = """
Run ID: {{ run_id }} | Overall Status: {{ overall_status }}
Started: {{ started_at }} | Finished: {{ finished_at }}

Per-store UC order status:
{% for store in stores %}
- {{ store.store_code }}: {{ store.status }} | orders: {{ store.order_count }} | files: {{ store.filenames | join(', ') if store.filenames }}{% if store.error_message %} | error: {{ store.error_message }}{% endif %}
{% endfor %}

{% if overall_status == 'ok' %}
All UC stores completed successfully. Upsert using (cost_center, order_number, invoice_date) to keep reruns idempotent.
{% elif overall_status in ['warning', 'partial', 'skipped', 'success_with_warnings'] %}
Mixed UC outcomes: some stores failed. Fix issues and rerun; unique constraints prevent duplicate rows on retry.
{% else %}
All UC stores failed. Review the errors above before reattempting the sync.
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
        .values(body_template=NEW_BODY_TEMPLATE)
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
        .values(body_template=OLD_BODY_TEMPLATE)
    )
