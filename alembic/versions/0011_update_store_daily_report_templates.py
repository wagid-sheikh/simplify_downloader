"""Update store report email templates to use store name and report date."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0011_update_store_daily_report_templates"
down_revision = "0010_update_dashboard_pipeline_codes"
branch_labels = None
depends_on = None


notification_profiles = sa.table(
    "notification_profiles",
    sa.column("id"),
    sa.column("code"),
)

email_templates = sa.table(
    "email_templates",
    sa.column("id"),
    sa.column("profile_id"),
    sa.column("name"),
    sa.column("subject_template"),
    sa.column("body_template"),
)


def _fetch_profile_ids(connection, codes: list[str]) -> dict[str, list[int]]:
    rows = (
        connection.execute(
            sa.select(notification_profiles.c.id, notification_profiles.c.code).where(
                notification_profiles.c.code.in_(codes)
            )
        )
        .mappings()
        .all()
    )

    profile_ids: dict[str, list[int]] = {code: [] for code in codes}
    for row in rows:
        profile_ids[row["code"]].append(row["id"])
    return profile_ids


def upgrade() -> None:
    connection = op.get_bind()
    profile_ids_by_code = _fetch_profile_ids(
        connection,
        [
            "store_daily_reports",
            "store_weekly_reports",
            "store_monthly_reports",
        ],
    )

    updates = {
        "store_daily_reports": {
            "subject_template": "{{ store_name }} Daily Performance Report {{ report_date }}",
            "body_template": "Attached: daily performance report for store {{ store_name }} on {{ report_date }}.",
        },
        "store_weekly_reports": {
            "subject_template": "[TSV Store Performance - Weekly] {{ store_name }} | Week ending {{ report_date }}",
            "body_template": "Attached: weekly performance report for store {{ store_name }} (week ending {{ report_date }}).",
        },
        "store_monthly_reports": {
            "subject_template": "[TSV Store Performance - Monthly] {{ store_name }} | {{ report_date }}",
            "body_template": "Attached: monthly performance report for store {{ store_name }} (month ending {{ report_date }}).",
        },
    }

    for code, profile_ids in profile_ids_by_code.items():
        if not profile_ids:
            continue

        connection.execute(
            email_templates.update()
            .where(email_templates.c.profile_id.in_(profile_ids))
            .where(email_templates.c.name == "default")
            .values(**updates[code])
        )


def downgrade() -> None:
    connection = op.get_bind()
    profile_ids_by_code = _fetch_profile_ids(
        connection,
        [
            "store_daily_reports",
            "store_weekly_reports",
            "store_monthly_reports",
        ],
    )

    rollbacks = {
        "store_daily_reports": {
            "subject_template": "{{ store_name }} Daily Performance Report {{ run_date }}",
            "body_template": "Attached: daily performance report for store {{ store_name }} on {{ run_date }}.",
        },
        "store_weekly_reports": {
            "subject_template": "[TSV Store Performance - Weekly] {{ store_code }} | Week ending {{ report_date }}",
            "body_template": "Attached: weekly performance report for store {{ store_code }} (week ending {{ report_date }}).",
        },
        "store_monthly_reports": {
            "subject_template": "[TSV Store Performance - Monthly] {{ store_code }} | {{ report_date }}",
            "body_template": "Attached: monthly performance report for store {{ store_code }} (month ending {{ report_date }}).",
        },
    }

    for code, profile_ids in profile_ids_by_code.items():
        if not profile_ids:
            continue

        connection.execute(
            email_templates.update()
            .where(email_templates.c.profile_id.in_(profile_ids))
            .where(email_templates.c.name == "default")
            .values(**rollbacks[code])
        )
