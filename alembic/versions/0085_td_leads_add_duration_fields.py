"""Add duration fields to td leads run-summary notification template."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0085_td_leads_add_duration_fields"
down_revision = "0084_seed_td_leads_notif"
branch_labels = None
depends_on = None

PIPELINE_CODE = "td_crm_leads_sync"
PROFILE_CODE = "run_summary"
TEMPLATE_NAME = "run_summary"

SUBJECT_TEMPLATE = "TD CRM Leads Sync - {{ overall_status|upper }} - {{ report_date }}"
BODY_TEMPLATE = """TD CRM Leads Sync
Run ID: {{ run_id }}
Env: {{ run_env }}
Report Date: {{ report_date }}
Overall Status: {{ overall_status }}
Start Time: {{ started_at or 'n/a' }}
End Time: {{ finished_at or 'n/a' }}
Total Duration: {{ duration_human or total_time_taken or 'n/a' }}

{{ summary_text }}
"""


def _template_filters():
    return [
        sa.text(
            "profile_id IN ("
            "SELECT np.id FROM notification_profiles np "
            "JOIN pipelines p ON p.id = np.pipeline_id "
            "WHERE p.code = :pipeline_code AND np.code = :profile_code"
            ")"
        ),
        sa.text("name = :template_name"),
    ]


def upgrade() -> None:
    bind = op.get_bind()
    email_templates = sa.table(
        "email_templates",
        sa.column("id", sa.BigInteger()),
        sa.column("subject_template", sa.Text()),
        sa.column("body_template", sa.Text()),
        sa.column("is_active", sa.Boolean()),
    )

    bind.execute(
        email_templates.update()
        .where(sa.and_(*_template_filters()))
        .values(subject_template=SUBJECT_TEMPLATE, body_template=BODY_TEMPLATE, is_active=True),
        {"pipeline_code": PIPELINE_CODE, "profile_code": PROFILE_CODE, "template_name": TEMPLATE_NAME},
    )


def downgrade() -> None:
    bind = op.get_bind()
    email_templates = sa.table(
        "email_templates",
        sa.column("id", sa.BigInteger()),
        sa.column("subject_template", sa.Text()),
        sa.column("body_template", sa.Text()),
    )

    bind.execute(
        email_templates.update()
        .where(sa.and_(*_template_filters()))
        .values(
            subject_template=SUBJECT_TEMPLATE,
            body_template="""TD CRM Leads Sync
Run ID: {{ run_id }}
Env: {{ run_env }}
Report Date: {{ report_date }}
Overall Status: {{ overall_status }}

{{ summary_text }}
""",
        ),
        {"pipeline_code": PIPELINE_CODE, "profile_code": PROFILE_CODE, "template_name": TEMPLATE_NAME},
    )
