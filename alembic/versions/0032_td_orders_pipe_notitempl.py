"""Align TD Orders notifications with structured summary text."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0032_td_orders_pipe_notitempl"
down_revision = "0031_ingest_remarks_td_sales"
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

OLD_BODY_TEMPLATE = """
Run ID: {{ run_id }} | Overall Status: {{ overall_status }}
Started: {{ started_at }} | Finished: {{ finished_at }}

{{ summary_text }}

{% if overall_status == 'ok' %}
All TD stores completed successfully. Proceed with merge to production using (cost_center, order_number, order_date).
{% elif overall_status == 'warning' %}
Mixed TD outcomes: review failed stores above, re-run after fixing source data, and rely on the unique business key to avoid duplicates.
{% else %}
All TD stores failed. Check error summaries and retry; no production rows were updated due to the enforced unique constraint.
{% endif %}
"""


def _fetch_td_profile_ids(connection) -> list[int]:
    pipeline_ids = (
        connection.execute(sa.select(pipelines.c.id).where(pipelines.c.code == "td_orders_sync")).scalars().all()
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
    profile_ids = _fetch_td_profile_ids(connection)
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
    profile_ids = _fetch_td_profile_ids(connection)
    if not profile_ids:
        return
    connection.execute(
        email_templates.update()
        .where(email_templates.c.profile_id.in_(profile_ids))
        .where(email_templates.c.name == "default")
        .values(body_template=OLD_BODY_TEMPLATE)
    )
