"""Finalize TD/UC notification template contract blocks and ordering."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0081_td_uc_template_cont_cleanup"
down_revision = "0080_td_uc_run_email_std"
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

FINAL_SUBJECT_TEMPLATE = (
    "ETL - [{{ env_upper }}][{{ overall_status_upper }}][{{ store_code }}] "
    "{{ pipeline_display_name }} – {{ run_date_display }}"
)

FINAL_BODY_TEMPLATE = """PIPELINE RUN SUMMARY
- pipeline: {{ pipeline_display_name or pipeline_code or 'n/a' }}
- run_id: {{ run_id or 'n/a' }}
- env: {{ env_upper or run_env or env or 'n/a' }}
- report_date: {{ run_date_display or report_date or 'n/a' }}
- started_at: {{ started_at_ist or started_at_formatted or started_at or 'n/a' }}
- finished_at: {{ finished_at_ist or finished_at_formatted or finished_at or 'n/a' }}
- duration: {{ total_time_taken or 'n/a' }}
- overall_status: {{ overall_status_upper or overall_status_label or overall_status or 'unknown' }}

WINDOW STATUS
- windows_completed: {{ completed_windows or 0 }} / {{ expected_windows or 0 }}
- missing_windows: {{ missing_windows or 0 }}
{% if missing_window_stores %}- missing_window_stores: {{ missing_window_stores | join(', ') }}
{% endif %}

STORE PROCESSING SUMMARY
{{ store_processing_summary_block or '- (none)' }}

FILES PROCESSED
{{ files_processed_block or '- (none)' }}

WARNINGS
{{ warnings_block or '- (none)' }}{% if optional_notes_block %}

NOTES
{{ optional_notes_block }}{% endif %}
"""


def _target_template_ids(bind: sa.Connection) -> list[int]:
    rows = bind.execute(
        sa.select(email_templates.c.id)
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
    return [int(row.id) for row in rows]


def _update_templates(bind: sa.Connection, template_ids: list[int], *, subject: str, body: str) -> None:
    if not template_ids:
        return
    bind.execute(
        email_templates.update()
        .where(email_templates.c.id.in_(template_ids))
        .values(subject_template=subject, body_template=body)
    )


def upgrade() -> None:
    bind = op.get_bind()
    template_ids = _target_template_ids(bind)
    _update_templates(bind, template_ids, subject=FINAL_SUBJECT_TEMPLATE, body=FINAL_BODY_TEMPLATE)


def downgrade() -> None:
    bind = op.get_bind()
    template_ids = _target_template_ids(bind)
    _update_templates(bind, template_ids, subject=FINAL_SUBJECT_TEMPLATE, body=FINAL_BODY_TEMPLATE)
