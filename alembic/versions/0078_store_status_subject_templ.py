"""Update TD/UC store profile subject templates to include status taxonomy."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0078_store_status_subject_templ"
down_revision = "0077_standardize_td_uc_bodytempl"
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

PIPELINE_CODES = ("td_orders_sync", "uc_orders_sync")
NEW_SUBJECT_TEMPLATE = (
    "ETL - [{{ env_upper }}][{{ overall_status_upper }}][{{ store_code }}] "
    "{{ pipeline_display_name }} – {{ run_date_display }}"
)

PREVIOUS_SUBJECTS = {
    "td_orders_sync": "TD Orders Sync – {{ store_code }}",
    "uc_orders_sync": "UC Orders Sync – {{ store_code }}",
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
        .where(pipelines.c.code.in_(PIPELINE_CODES))
        .where(notification_profiles.c.scope == "store")
        .where(notification_profiles.c.is_active.is_(True))
        .where(email_templates.c.is_active.is_(True))
    ).all()
    return {row.id: row.code for row in rows}


def upgrade() -> None:
    bind = op.get_bind()
    template_ids = tuple(_target_template_ids(bind))
    if template_ids:
        bind.execute(
            email_templates.update()
            .where(email_templates.c.id.in_(template_ids))
            .values(subject_template=NEW_SUBJECT_TEMPLATE)
        )


def downgrade() -> None:
    bind = op.get_bind()
    template_map = _target_template_ids(bind)
    for template_id, pipeline_code in template_map.items():
        bind.execute(
            email_templates.update()
            .where(email_templates.c.id == template_id)
            .where(email_templates.c.subject_template == NEW_SUBJECT_TEMPLATE)
            .values(subject_template=PREVIOUS_SUBJECTS[pipeline_code])
        )
