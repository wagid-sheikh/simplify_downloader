"""Update UC store notification templates with row-level facts."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0054_uc_store_fact_sections_templates"
down_revision = "0053_orders_sync_templatecleanup"
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

UC_STORE_BODY_TEMPLATE = """
UC Orders Sync Store Summary
Store: {{ store_name or store_code }}{% if store_code %} ({{ store_code }}){% endif %}
Run ID: {{ run_id }} | Env: {{ run_env }}
Report Date: {{ report_date }}
Started: {{ started_at }} | Finished: {{ finished_at }}
{% if total_time_taken %}Total Duration: {{ total_time_taken }}{% endif %}
Overall Status: {{ overall_status_label or overall_status }}{% if overall_status_explanation %} ({{ overall_status_explanation }}){% endif %}

{% if missing_window_lines %}Missing Windows: {{ missing_window_lines | join(', ') }}{% endif %}

{% if fact_sections_text %}
Row-level facts:
{{ fact_sections_text }}
{% elif summary_text %}
{{ summary_text }}
{% endif %}
"""

UC_STORE_PREVIOUS_BODY_TEMPLATE = """
UC Orders Sync Store Summary
Store: {{ store_name or store_code }}
Run ID: {{ run_id }} | Env: {{ run_env }}
Report Date: {{ report_date }}
Started: {{ started_at }} | Finished: {{ finished_at }}
{% if total_time_taken %}Total Duration: {{ total_time_taken }}{% endif %}
Overall Status: {{ overall_status }}
"""


def _fetch_uc_store_profile_ids(connection) -> list[int]:
    pipeline_ids = (
        connection.execute(sa.select(pipelines.c.id).where(pipelines.c.code == "uc_orders_sync"))
        .scalars()
        .all()
    )
    if not pipeline_ids:
        return []
    return (
        connection.execute(
            sa.select(notification_profiles.c.id)
            .where(notification_profiles.c.pipeline_id.in_(pipeline_ids))
            .where(notification_profiles.c.scope == "store")
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
        profile_ids=_fetch_uc_store_profile_ids(connection),
        body_template=UC_STORE_BODY_TEMPLATE,
    )


def downgrade() -> None:
    connection = op.get_bind()
    _update_templates(
        connection,
        profile_ids=_fetch_uc_store_profile_ids(connection),
        body_template=UC_STORE_PREVIOUS_BODY_TEMPLATE,
    )
