"""Render TD/UC store notification body templates as ordered plain-text sections."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0079_td_uc_store_body_sections"
down_revision = "0078_store_status_subject_templ"
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
    sa.column("body_template", sa.Text()),
    sa.column("is_active", sa.Boolean()),
)

PIPELINE_CODES = ("td_orders_sync", "uc_orders_sync")
PREVIOUS_BODY_TEMPLATE = "{{ summary_text }}"
NEW_BODY_TEMPLATE = """PIPELINE RUN SUMMARY
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


def _target_template_ids(bind: sa.Connection) -> tuple[int, ...]:
    rows = bind.execute(
        sa.select(email_templates.c.id)
        .select_from(
            email_templates.join(
                notification_profiles,
                email_templates.c.profile_id == notification_profiles.c.id,
            ).join(pipelines, notification_profiles.c.pipeline_id == pipelines.c.id)
        )
        .where(pipelines.c.code.in_(PIPELINE_CODES))
        .where(notification_profiles.c.scope == "store")
        .where(notification_profiles.c.is_active.is_(True))
        .where(email_templates.c.is_active.is_(True))
    ).scalars().all()
    return tuple(rows)


def upgrade() -> None:
    bind = op.get_bind()
    template_ids = _target_template_ids(bind)
    if template_ids:
        bind.execute(
            email_templates.update()
            .where(email_templates.c.id.in_(template_ids))
            .values(body_template=NEW_BODY_TEMPLATE)
        )


def downgrade() -> None:
    bind = op.get_bind()
    template_ids = _target_template_ids(bind)
    if template_ids:
        bind.execute(
            email_templates.update()
            .where(email_templates.c.id.in_(template_ids))
            .where(email_templates.c.body_template == NEW_BODY_TEMPLATE)
            .values(body_template=PREVIOUS_BODY_TEMPLATE)
        )
