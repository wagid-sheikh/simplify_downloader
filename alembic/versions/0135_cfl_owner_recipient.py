"""Seed customer retention owner recipient."""

from __future__ import annotations

from datetime import datetime, timezone

from alembic import op
import sqlalchemy as sa

revision = "0135_cfl_owner_recipient"
down_revision = "0134_cfl_notif_no_recip"
branch_labels = None
depends_on = None

PIPELINE_CODE = "customer_retention_pipeline"
PROFILE_CODE = "owner_summary"
PROFILE_SCOPE = "run"
RECIPIENT_EMAIL = "wagid.sheikh@gmail.com"
RECIPIENT_DISPLAY_NAME = "Wagid Sheikh"
RECIPIENT_SEND_AS = "to"
RECIPIENT_ENVS = ("dev", "prod", "local", "any")


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def upgrade() -> None:
    bind = op.get_bind()
    pipelines = sa.table("pipelines", sa.column("id"), sa.column("code"))
    profiles = sa.table(
        "notification_profiles",
        sa.column("id"),
        sa.column("pipeline_id"),
        sa.column("code"),
        sa.column("scope"),
    )
    recipients = sa.table(
        "notification_recipients",
        sa.column("id"),
        sa.column("profile_id"),
        sa.column("store_code"),
        sa.column("env"),
        sa.column("email_address"),
        sa.column("display_name"),
        sa.column("send_as"),
        sa.column("is_active"),
        sa.column("created_at"),
    )

    pipeline_id = bind.execute(
        sa.select(pipelines.c.id).where(pipelines.c.code == PIPELINE_CODE)
    ).scalar_one_or_none()
    if pipeline_id is None:
        return

    profile_id = bind.execute(
        sa.select(profiles.c.id)
        .where(profiles.c.pipeline_id == pipeline_id)
        .where(profiles.c.code == PROFILE_CODE)
        .where(profiles.c.scope == PROFILE_SCOPE)
    ).scalar_one_or_none()
    if profile_id is None:
        return

    for env in RECIPIENT_ENVS:
        existing_ids = bind.execute(
            sa.select(recipients.c.id)
            .where(recipients.c.profile_id == profile_id)
            .where(recipients.c.store_code.is_(None))
            .where(recipients.c.env == env)
            .where(recipients.c.email_address == RECIPIENT_EMAIL)
        ).scalars().all()

        values = {
            "display_name": RECIPIENT_DISPLAY_NAME,
            "send_as": RECIPIENT_SEND_AS,
            "is_active": True,
        }
        if existing_ids:
            bind.execute(
                recipients.update()
                .where(recipients.c.id.in_(existing_ids))
                .values(**values)
            )
        else:
            bind.execute(
                recipients.insert().values(
                    profile_id=profile_id,
                    store_code=None,
                    env=env,
                    email_address=RECIPIENT_EMAIL,
                    created_at=_now(),
                    **values,
                )
            )


def downgrade() -> None:
    # Forward-only seed migration. Deliberately keep recipient state in place.
    return None
