"""Standardize TD/UC store notification body templates to summary text."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0077_standardize_td_uc_store_body_template"
down_revision = ("0075_store_scope_subj_codes", "0076_add_stg_td_garments_weight")
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
SUMMARY_TEXT_TEMPLATE = "{{ summary_text }}"


def upgrade() -> None:
    bind = op.get_bind()
    template_ids = bind.execute(
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

    if template_ids:
        bind.execute(
            email_templates.update()
            .where(email_templates.c.id.in_(template_ids))
            .values(body_template=SUMMARY_TEXT_TEMPLATE)
        )


def downgrade() -> None:
    pass
