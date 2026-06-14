"""Remove hardcoded customer retention recipients."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0134_cfl_notif_no_recip"
down_revision = "0133_cfl_notif_seed"
branch_labels = None
depends_on = None

PIPELINE_CODE = "customer_retention_pipeline"
PROFILE_CODE = "owner_summary"
PROFILE_ENV = "any"
OLD_RECIPIENT_EMAIL = "wagid.sheikh@gmail.com"
OLD_RECIPIENT_DISPLAY_NAME = "Wagid Sheikh"
OLD_RECIPIENT_ENVS = ("dev", "prod", "local", "any")


def upgrade() -> None:
    bind = op.get_bind()
    pipelines = sa.table("pipelines", sa.column("id"), sa.column("code"))
    profiles = sa.table(
        "notification_profiles",
        sa.column("id"),
        sa.column("pipeline_id"),
        sa.column("code"),
        sa.column("env"),
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
        .where(profiles.c.env == PROFILE_ENV)
    ).scalar_one_or_none()
    if profile_id is None:
        return

    bind.execute(
        recipients.delete()
        .where(recipients.c.profile_id == profile_id)
        .where(recipients.c.store_code.is_(None))
        .where(recipients.c.env.in_(OLD_RECIPIENT_ENVS))
        .where(recipients.c.email_address == OLD_RECIPIENT_EMAIL)
        .where(recipients.c.display_name == OLD_RECIPIENT_DISPLAY_NAME)
        .where(recipients.c.send_as == "to")
    )


def downgrade() -> None:
    # Forward-only cleanup; do not recreate hardcoded recipient addresses.
    return None
