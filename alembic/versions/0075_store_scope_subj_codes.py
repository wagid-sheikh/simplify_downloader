"""Ensure store-scoped orders sync subjects include store code."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0075_store_scope_subj_codes"
down_revision = "0074_relax_td_garmentlineitemuid"
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
    sa.column("is_active", sa.Boolean()),
)

PIPELINE_SUBJECTS = {
    "td_orders_sync": "TD Orders Sync – {{ store_code }}",
    "uc_orders_sync": "UC Orders Sync – {{ store_code }}",
}

DEFAULT_SUBJECTS = {
    "td_orders_sync": "TD Orders Sync – {{ overall_status }}",
    "uc_orders_sync": "UC Orders Sync – {{ overall_status }}",
}


def _target_template_ids(bind: sa.Connection) -> dict[int, str]:
    rows = bind.execute(
        sa.select(email_templates.c.id, pipelines.c.code)
        .select_from(
            email_templates.join(
                notification_profiles,
                email_templates.c.profile_id == notification_profiles.c.id,
            ).join(pipelines, notification_profiles.c.pipeline_id == pipelines.c.id)
        )
        .where(pipelines.c.code.in_(tuple(PIPELINE_SUBJECTS)))
        .where(notification_profiles.c.scope == "store")
        .where(notification_profiles.c.is_active.is_(True))
        .where(email_templates.c.is_active.is_(True))
    ).all()
    return {row.id: row.code for row in rows}


def upgrade() -> None:
    bind = op.get_bind()
    template_map = _target_template_ids(bind)
    for template_id, pipeline_code in template_map.items():
        bind.execute(
            email_templates.update()
            .where(email_templates.c.id == template_id)
            .values(subject_template=PIPELINE_SUBJECTS[pipeline_code])
        )


def downgrade() -> None:
    bind = op.get_bind()
    template_map = _target_template_ids(bind)
    for template_id, pipeline_code in template_map.items():
        bind.execute(
            email_templates.update()
            .where(email_templates.c.id == template_id)
            .where(email_templates.c.subject_template == PIPELINE_SUBJECTS[pipeline_code])
            .values(subject_template=DEFAULT_SUBJECTS[pipeline_code])
        )
