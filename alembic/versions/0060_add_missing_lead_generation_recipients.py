"""Add missing lead generation notification recipients."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0060_add_missing_lead_generation_recipients"
down_revision = "0059_rename_sakes_to_sales"
branch_labels = None
depends_on = None


PROFILE_CODE = "leads_assignment"
RECIPIENT_EMAIL = "wagid.sheikh@gmail.com"
RECIPIENT_SEND_AS = "to"
STORE_CODES = ("A276", "A526", "B002", "T997", "TS36", "TS74", "TS81")


def _notification_profiles_table() -> sa.Table:
    return sa.table(
        "notification_profiles",
        sa.column("id"),
        sa.column("code"),
        sa.column("env"),
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
    recipients = _notification_recipients_table()

    profile_rows = connection.execute(
        sa.select(profiles.c.id, profiles.c.env)
        .where(profiles.c.code == PROFILE_CODE)
        .where(profiles.c.is_active.is_(True))
    ).mappings()

    for profile_row in profile_rows:
        profile_id = int(profile_row["id"])
        env = profile_row["env"]

        for store_code in STORE_CODES:
            recipient_id = connection.execute(
                sa.select(recipients.c.id)
                .where(recipients.c.profile_id == profile_id)
                .where(recipients.c.store_code == store_code)
                .where(recipients.c.env == env)
                .where(recipients.c.email_address == RECIPIENT_EMAIL)
                .where(recipients.c.send_as == RECIPIENT_SEND_AS)
            ).scalar()

            payload = {
                "profile_id": profile_id,
                "store_code": store_code,
                "env": env,
                "email_address": RECIPIENT_EMAIL,
                "display_name": None,
                "send_as": RECIPIENT_SEND_AS,
                "is_active": True,
            }

            if recipient_id:
                connection.execute(
                    recipients.update()
                    .where(recipients.c.id == recipient_id)
                    .values(payload)
                )
            else:
                connection.execute(recipients.insert().values(payload))


def downgrade() -> None:
    connection = op.get_bind()

    profiles = _notification_profiles_table()
    recipients = _notification_recipients_table()

    profile_ids = [
        row["id"]
        for row in connection.execute(
            sa.select(profiles.c.id)
            .where(profiles.c.code == PROFILE_CODE)
            .where(profiles.c.is_active.is_(True))
        ).mappings()
    ]

    if not profile_ids:
        return

    connection.execute(
        recipients.delete()
        .where(recipients.c.profile_id.in_(profile_ids))
        .where(recipients.c.email_address == RECIPIENT_EMAIL)
        .where(recipients.c.send_as == RECIPIENT_SEND_AS)
        .where(recipients.c.store_code.in_(STORE_CODES))
    )
