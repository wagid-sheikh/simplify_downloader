"""Ensure leads assignment summary template and recipient exist."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0043_leadsassignmentsummarytempl"
down_revision = "0042_create_orders_sync_log"
branch_labels = None
depends_on = None


PROFILE_CODE = "leads_assignment"

SUMMARY_TEMPLATE_NAME = "summary"
SUMMARY_SUBJECT_TEMPLATE = "[Leads Assignment] Run summary | Batch {{ batch_id }} ({{ run_env }})"
SUMMARY_BODY_TEMPLATE = """
Leads assignment run summary

Run ID: {{ run_id }}
Environment: {{ run_env }}
Batch ID: {{ batch_id }}

Assignments created: {{ assignments }}
Documents generated: {{ documents_generated }}
Emails planned: {{ emails_planned }}
Emails sent: {{ emails_sent }}

Per-store diagnostics:
{% if store_diagnostics %}
{% for store in store_diagnostics %}
- {{ store.store_code }}: eligible {{ store.eligible_leads_count }}, assigned {{ store.assigned_leads_count }} ({{ store.reasons | join('; ') }})
{% endfor %}
{% else %}
- No eligible stores found.
{% endif %}
""".strip()

RECIPIENT_EMAIL = "wagid.sheikh@gmail.com"
RECIPIENT_SEND_AS = "to"
RECIPIENT_ENV = "any"
RECIPIENT_DISPLAY_NAME = "Leads Assignment Summary"


def _notification_profiles_table() -> sa.Table:
    return sa.table(
        "notification_profiles",
        sa.column("id"),
        sa.column("code"),
        sa.column("is_active"),
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


def _notification_recipients_table() -> sa.Table:
    return sa.table(
        "notification_recipients",
        sa.column("id"),
        sa.column("profile_id"),
        sa.column("store_code"),
        sa.column("env"),
        sa.column("email_address"),
        sa.column("display_name"),
        sa.column("send_as"),
        sa.column("is_active"),
    )


def upgrade() -> None:
    connection = op.get_bind()

    profiles = _notification_profiles_table()
    templates = _email_templates_table()
    recipients = _notification_recipients_table()

    profile_rows = connection.execute(
        sa.select(profiles.c.id)
        .where(profiles.c.code == PROFILE_CODE)
        .where(profiles.c.is_active.is_(True))
    ).mappings()

    for profile_row in profile_rows:
        profile_id = int(profile_row["id"])
        template_id = connection.execute(
            sa.select(templates.c.id)
            .where(templates.c.profile_id == profile_id)
            .where(templates.c.name == SUMMARY_TEMPLATE_NAME)
        ).scalar()

        template_payload = {
            "profile_id": profile_id,
            "name": SUMMARY_TEMPLATE_NAME,
            "subject_template": SUMMARY_SUBJECT_TEMPLATE,
            "body_template": SUMMARY_BODY_TEMPLATE,
            "is_active": True,
        }

        if template_id:
            connection.execute(
                templates.update().where(templates.c.id == template_id).values(template_payload)
            )
        else:
            connection.execute(templates.insert().values(template_payload))

        recipient_id = connection.execute(
            sa.select(recipients.c.id)
            .where(recipients.c.profile_id == profile_id)
            .where(recipients.c.store_code.is_(None))
            .where(recipients.c.env == RECIPIENT_ENV)
            .where(recipients.c.email_address == RECIPIENT_EMAIL)
            .where(recipients.c.send_as == RECIPIENT_SEND_AS)
        ).scalar()

        recipient_payload = {
            "profile_id": profile_id,
            "store_code": None,
            "env": RECIPIENT_ENV,
            "email_address": RECIPIENT_EMAIL,
            "display_name": RECIPIENT_DISPLAY_NAME,
            "send_as": RECIPIENT_SEND_AS,
            "is_active": True,
        }

        if recipient_id:
            connection.execute(
                recipients.update().where(recipients.c.id == recipient_id).values(recipient_payload)
            )
        else:
            connection.execute(recipients.insert().values(recipient_payload))


def downgrade() -> None:
    connection = op.get_bind()

    profiles = _notification_profiles_table()
    templates = _email_templates_table()
    recipients = _notification_recipients_table()

    profile_rows = connection.execute(
        sa.select(profiles.c.id).where(profiles.c.code == PROFILE_CODE)
    ).mappings()

    for profile_row in profile_rows:
        profile_id = int(profile_row["id"])

        connection.execute(
            recipients.delete()
            .where(recipients.c.profile_id == profile_id)
            .where(recipients.c.store_code.is_(None))
            .where(recipients.c.env == RECIPIENT_ENV)
            .where(recipients.c.email_address == RECIPIENT_EMAIL)
            .where(recipients.c.send_as == RECIPIENT_SEND_AS)
        )

        connection.execute(
            templates.delete()
            .where(templates.c.profile_id == profile_id)
            .where(templates.c.name == SUMMARY_TEMPLATE_NAME)
            .where(templates.c.subject_template == SUMMARY_SUBJECT_TEMPLATE)
            .where(templates.c.body_template == SUMMARY_BODY_TEMPLATE)
        )
