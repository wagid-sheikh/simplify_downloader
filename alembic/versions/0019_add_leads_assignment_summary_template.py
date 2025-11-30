"""Add summary email template for leads_assignment notification profile."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0019_leads_assignment_summary_template"
down_revision = "0018_leads_notification_profile"
branch_labels = None
depends_on = None


PROFILE_CODE = "leads_assignment"
TEMPLATE_NAME = "summary"

SUBJECT_TEMPLATE = "[Leads Assignment] Run summary | Batch {{ batch_id }} ({{ run_env }})"
BODY_TEMPLATE = """
Leads assignment run summary

Run ID: {{ run_id }}
Environment: {{ run_env }}
Batch ID: {{ batch_id }}

Assignments created: {{ assignments }}
Documents generated: {{ documents_generated }}
Emails planned: {{ emails_planned }}
Emails sent: {{ emails_sent }}
""".strip()


def _notification_profiles_table() -> sa.Table:
    return sa.table(
        "notification_profiles",
        sa.column("id"),
        sa.column("code"),
    )


def _email_templates_table() -> sa.Table:
    return sa.table(
        "email_templates",
        sa.column("id"),
        sa.column("profile_id"),
        sa.column("name"),
        sa.column("subject_template"),
        sa.column("body_template"),
        sa.column("is_active"),
    )


def _get_leads_profile_ids(connection) -> list[int]:
    profiles = _notification_profiles_table()

    return [
        row["id"]
        for row in connection.execute(
            sa.select(profiles.c.id).where(profiles.c.code == PROFILE_CODE)
        ).mappings()
    ]


def _upsert_summary_template(connection, profile_id: int) -> None:
    templates = _email_templates_table()

    existing_template_id = (
        connection.execute(
            sa.select(templates.c.id)
            .where(templates.c.profile_id == profile_id)
            .where(templates.c.name == TEMPLATE_NAME)
        )
        .scalar()
    )

    payload = {
        "profile_id": profile_id,
        "name": TEMPLATE_NAME,
        "subject_template": SUBJECT_TEMPLATE,
        "body_template": BODY_TEMPLATE,
        "is_active": True,
    }

    if existing_template_id:
        connection.execute(
            templates.update()
            .where(templates.c.id == existing_template_id)
            .values(payload)
        )
    else:
        connection.execute(templates.insert().values(payload))


def upgrade() -> None:
    connection = op.get_bind()

    profile_ids = _get_leads_profile_ids(connection)
    for profile_id in profile_ids:
        _upsert_summary_template(connection, profile_id)


def downgrade() -> None:
    connection = op.get_bind()

    profile_ids = _get_leads_profile_ids(connection)
    if not profile_ids:
        return

    templates = _email_templates_table()

    connection.execute(
        templates.delete()
        .where(templates.c.profile_id.in_(profile_ids))
        .where(templates.c.name == TEMPLATE_NAME)
    )
