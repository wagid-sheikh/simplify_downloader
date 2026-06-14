"""Seed customer retention notification contract."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0133_cfl_notif_seed"
down_revision = "0132_cfl_backlog_threshold"
branch_labels = None
depends_on = None

PIPELINE_CODE = "customer_retention_pipeline"
PIPELINE_DESCRIPTION = "Customer Retention Pipeline"
PROFILE_CODE = "owner_summary"
PROFILE_DESCRIPTION = "Customer retention owner run summary"
PROFILE_ENV = "any"
PROFILE_SCOPE = "run"
PROFILE_ATTACH_MODE = "none"
TEMPLATE_NAME = "summary"
SUBJECT_TEMPLATE = "Customer Retention Summary {{ run_summary.run_date }} ({{ run_summary.success_failure_status }})"
BODY_TEMPLATE = """
Customer Retention Pipeline Run {{ run_summary.pipeline_run_id }}
Status: {{ run_summary.success_failure_status }}
Duration: {{ run_summary.duration_seconds }} seconds

Run Summary:
- Run Date: {{ run_summary.run_date }}
- Started: {{ run_summary.started_at or 'not recorded' }}
- Finished: {{ run_summary.finished_at or 'not recorded' }}
- Status: {{ run_summary.success_failure_status }}

Store Summary:
{% for store in store_summary -%}
- {{ store.cost_center }}: workbook={{ store.workbook_generated_path or 'not generated' }}, due={{ store.due_followups_included }}, carry_forward={{ store.pending_carry_forward_included }}, fresh_retention={{ store.fresh_retention_leads_generated }}, TD={{ store.td_leads_included }}, EXTERNAL={{ store.external_leads_included }}, recovered={{ store.recovered_customers }}, recovered_revenue={{ store.recovered_revenue_value }}, warnings={{ store.rows_with_warnings }}, frozen={{ store.fresh_retention_frozen }}
{% endfor %}

Aging Actionable Workload:
{% for row in aging_actionable_workload -%}
- {{ row.cost_center }}: pending={{ row.pending_carry_forward }}, rolling_14_day={{ row.rolling_14_day_backlog_count }}, >3d={{ row.unworked_gt_3_days }}, >7d={{ row.unworked_gt_7_days }}, threshold={{ row.backlog_threshold }}, frozen={{ row.fresh_retention_frozen }}
{% endfor %}

Staff Productivity:
{% for row in staff_productivity -%}
- {{ row.cost_center }} / {{ row.handled_by }}: assigned={{ row.total_leads_assigned }}, worked={{ row.worked }}, dead_ends={{ row.dead_ends_logged }}, recovered={{ row.recovered }}, warning={{ row.operational_warning }}
{% endfor %}

Source-Wise Summary:
{% for row in source_wise_summary -%}
- {{ row.source }}: included={{ row.included }}, worked={{ row.worked }}, pending={{ row.pending }}, closed={{ row.closed }}, recovered={{ row.recovered }}, recovered_revenue={{ row.recovered_revenue_value }}
{% endfor %}

Warning/Error Summary:
{{ warning_error_summary }}
"""


def _tables() -> tuple[sa.Table, sa.Table, sa.Table]:
    pipelines = sa.table(
        "pipelines", sa.column("id"), sa.column("code"), sa.column("description")
    )
    profiles = sa.table(
        "notification_profiles",
        sa.column("id"),
        sa.column("pipeline_id"),
        sa.column("code"),
        sa.column("description"),
        sa.column("env"),
        sa.column("scope"),
        sa.column("attach_mode"),
        sa.column("is_active"),
    )
    templates = sa.table(
        "email_templates",
        sa.column("id"),
        sa.column("profile_id"),
        sa.column("name"),
        sa.column("subject_template"),
        sa.column("body_template"),
        sa.column("is_active"),
    )
    return pipelines, profiles, templates


def upgrade() -> None:
    bind = op.get_bind()
    pipelines, profiles, templates = _tables()

    pipeline_id = bind.execute(
        sa.select(pipelines.c.id).where(pipelines.c.code == PIPELINE_CODE)
    ).scalar_one_or_none()
    if pipeline_id is None:
        bind.execute(
            pipelines.insert().values(
                code=PIPELINE_CODE, description=PIPELINE_DESCRIPTION
            )
        )
        pipeline_id = bind.execute(
            sa.select(pipelines.c.id).where(pipelines.c.code == PIPELINE_CODE)
        ).scalar_one()
    else:
        bind.execute(
            pipelines.update()
            .where(pipelines.c.id == pipeline_id)
            .values(description=PIPELINE_DESCRIPTION)
        )

    profile_id = bind.execute(
        sa.select(profiles.c.id)
        .where(profiles.c.pipeline_id == pipeline_id)
        .where(profiles.c.code == PROFILE_CODE)
        .where(profiles.c.env == PROFILE_ENV)
    ).scalar_one_or_none()
    profile_values = {
        "pipeline_id": pipeline_id,
        "code": PROFILE_CODE,
        "description": PROFILE_DESCRIPTION,
        "env": PROFILE_ENV,
        "scope": PROFILE_SCOPE,
        "attach_mode": PROFILE_ATTACH_MODE,
        "is_active": True,
    }
    if profile_id is None:
        bind.execute(profiles.insert().values(**profile_values))
        profile_id = bind.execute(
            sa.select(profiles.c.id)
            .where(profiles.c.pipeline_id == pipeline_id)
            .where(profiles.c.code == PROFILE_CODE)
            .where(profiles.c.env == PROFILE_ENV)
        ).scalar_one()
    else:
        bind.execute(
            profiles.update()
            .where(profiles.c.id == profile_id)
            .values(**profile_values)
        )

    template_id = bind.execute(
        sa.select(templates.c.id)
        .where(templates.c.profile_id == profile_id)
        .where(templates.c.name == TEMPLATE_NAME)
    ).scalar_one_or_none()
    template_values = {
        "profile_id": profile_id,
        "name": TEMPLATE_NAME,
        "subject_template": SUBJECT_TEMPLATE,
        "body_template": BODY_TEMPLATE,
        "is_active": True,
    }
    if template_id is None:
        bind.execute(templates.insert().values(**template_values))
    else:
        bind.execute(
            templates.update()
            .where(templates.c.id == template_id)
            .values(**template_values)
        )


def downgrade() -> None:
    # Forward-only seed migration. Deliberately keep seeded notification metadata in place.
    return None
