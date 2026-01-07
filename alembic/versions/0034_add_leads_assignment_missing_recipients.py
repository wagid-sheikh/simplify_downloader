"""Insert missing leads_assignment recipients for active stores."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0034_add_leads_assignment_missing_recipients"
down_revision = "0033_leads_assignment_summary_seed"
branch_labels = None
depends_on = None


PROFILE_CODE = "leads_assignment"
RECIPIENT_EMAIL = "wagid.sheikh@gmail.com"
RECIPIENT_SEND_AS = "to"


def _store_master_table() -> sa.Table:
    return sa.table(
        "store_master",
        sa.column("store_code"),
        sa.column("etl_flag"),
        sa.column("assign_leads"),
    )


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


def _missing_store_codes(
    *,
    stores: sa.Table,
    recipients: sa.Table,
    profile_id: int,
    env: str,
) -> sa.Select:
    recipient_exists = (
        sa.select(sa.literal(1))
        .select_from(recipients)
        .where(recipients.c.profile_id == profile_id)
        .where(recipients.c.is_active.is_(True))
        .where(recipients.c.env.in_(["any", env]))
        .where(recipients.c.send_as.in_(["to", "cc"]))
        .where(
            sa.or_(
                recipients.c.store_code.is_(None),
                sa.func.upper(recipients.c.store_code) == sa.func.upper(stores.c.store_code),
            )
        )
    )

    return (
        sa.select(sa.func.upper(stores.c.store_code).label("store_code"))
        .where(stores.c.etl_flag.is_(True))
        .where(stores.c.assign_leads.is_(True))
        .where(~sa.exists(recipient_exists))
    )


def upgrade() -> None:
    connection = op.get_bind()

    profiles = _notification_profiles_table()
    recipients = _notification_recipients_table()
    stores = _store_master_table()

    profile_rows = connection.execute(
        sa.select(profiles.c.id, profiles.c.env)
        .where(profiles.c.code == PROFILE_CODE)
        .where(profiles.c.is_active.is_(True))
    ).mappings()

    for profile_row in profile_rows:
        profile_id = int(profile_row["id"])
        env = profile_row["env"]

        missing_codes = connection.execute(
            _missing_store_codes(
                stores=stores,
                recipients=recipients,
                profile_id=profile_id,
                env=env,
            )
        ).scalars()

        payload = [
            {
                "profile_id": profile_id,
                "store_code": store_code,
                "env": env,
                "email_address": RECIPIENT_EMAIL,
                "display_name": None,
                "send_as": RECIPIENT_SEND_AS,
                "is_active": True,
            }
            for store_code in missing_codes
        ]

        if payload:
            connection.execute(recipients.insert(), payload)


def downgrade() -> None:
    connection = op.get_bind()

    profiles = _notification_profiles_table()
    recipients = _notification_recipients_table()
    stores = _store_master_table()

    profile_rows = connection.execute(
        sa.select(profiles.c.id, profiles.c.env).where(profiles.c.code == PROFILE_CODE)
    ).mappings()

    for profile_row in profile_rows:
        profile_id = int(profile_row["id"])
        env = profile_row["env"]

        connection.execute(
            recipients.delete()
            .where(recipients.c.profile_id == profile_id)
            .where(recipients.c.env == env)
            .where(recipients.c.email_address == RECIPIENT_EMAIL)
            .where(recipients.c.send_as == RECIPIENT_SEND_AS)
            .where(
                recipients.c.store_code.in_(
                    sa.select(stores.c.store_code)
                    .where(stores.c.etl_flag.is_(True))
                    .where(stores.c.assign_leads.is_(True))
                )
            )
        )
