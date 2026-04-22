"""Update TD leads notification template for HTML summary tables."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0086_td_leads_html_template"
down_revision = "0085_td_leads_duration"
branch_labels = None
depends_on = None

PIPELINE_CODE = "td_crm_leads_sync"
PROFILE_CODE = "run_summary"
TEMPLATE_NAME = "run_summary"

NEW_BODY_TEMPLATE = """<div style=\"font-family:Arial, sans-serif;\">
  <h2 style=\"margin:0 0 10px 0;\">TD CRM Leads Sync</h2>
  <p style=\"margin:0 0 6px 0;\"><strong>Run ID:</strong> {{ run_id }}</p>
  <p style=\"margin:0 0 6px 0;\"><strong>Env:</strong> {{ run_env }}</p>
  <p style=\"margin:0 0 6px 0;\"><strong>Report Date:</strong> {{ report_date }}</p>
  <p style=\"margin:0 0 12px 0;\"><strong>Overall Status:</strong> {{ overall_status }}</p>
  {{ (summary_html or summary_text) }}
</div>
"""

OLD_BODY_TEMPLATE = """TD CRM Leads Sync
Run ID: {{ run_id }}
Env: {{ run_env }}
Report Date: {{ report_date }}
Overall Status: {{ overall_status }}

{{ summary_text }}
"""


def _resolve_template_id(bind) -> int | None:
    pipelines = sa.table("pipelines", sa.column("id", sa.BigInteger()), sa.column("code", sa.Text()))
    notification_profiles = sa.table(
        "notification_profiles",
        sa.column("id", sa.BigInteger()),
        sa.column("pipeline_id", sa.BigInteger()),
        sa.column("code", sa.Text()),
        sa.column("scope", sa.Text()),
    )
    email_templates = sa.table(
        "email_templates",
        sa.column("id", sa.BigInteger()),
        sa.column("profile_id", sa.BigInteger()),
        sa.column("name", sa.Text()),
    )

    pipeline_id = bind.execute(sa.select(pipelines.c.id).where(pipelines.c.code == PIPELINE_CODE)).scalar_one_or_none()
    if pipeline_id is None:
        return None

    profile_id = bind.execute(
        sa.select(notification_profiles.c.id)
        .where(notification_profiles.c.pipeline_id == pipeline_id)
        .where(notification_profiles.c.code == PROFILE_CODE)
        .where(notification_profiles.c.scope == "run")
    ).scalar_one_or_none()
    if profile_id is None:
        return None

    return bind.execute(
        sa.select(email_templates.c.id)
        .where(email_templates.c.profile_id == profile_id)
        .where(email_templates.c.name == TEMPLATE_NAME)
    ).scalar_one_or_none()


def _set_template_body(bind, body_template: str) -> None:
    template_id = _resolve_template_id(bind)
    if template_id is None:
        return

    email_templates = sa.table(
        "email_templates",
        sa.column("id", sa.BigInteger()),
        sa.column("body_template", sa.Text()),
    )
    bind.execute(
        email_templates.update().where(email_templates.c.id == template_id).values(body_template=body_template)
    )


def upgrade() -> None:
    bind = op.get_bind()
    _set_template_body(bind, NEW_BODY_TEMPLATE)


def downgrade() -> None:
    bind = op.get_bind()
    _set_template_body(bind, OLD_BODY_TEMPLATE)
