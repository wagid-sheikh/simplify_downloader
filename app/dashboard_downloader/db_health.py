"""Database health checks for CLI tooling."""
from __future__ import annotations

from typing import List

import sqlalchemy as sa

from app.dashboard_downloader.db_tables import (
    documents,
    email_templates,
    notification_profiles,
    notification_recipients,
    pipeline_run_summaries,
    pipelines,
)
from app.common.db import session_scope

REQUIRED_TABLES = [
    pipeline_run_summaries,
    documents,
    pipelines,
    notification_profiles,
    email_templates,
    notification_recipients,
]

REQUIRED_PIPELINE_PROFILES: dict[str, list[str]] = {
    "simplify_dashboard_daily": ["run_summary", "store_daily_reports"],
    "simplify_dashboard_weekly": ["run_summary", "store_weekly_reports"],
    "simplify_dashboard_monthly": ["run_summary", "store_monthly_reports"],
}


async def check_database_health(database_url: str) -> List[str]:
    """Return a list of detected issues. Empty list == healthy."""

    errors: List[str] = []
    async with session_scope(database_url) as session:
        for table in REQUIRED_TABLES:
            try:
                await session.execute(sa.select(sa.literal(1)).select_from(table).limit(1))
            except Exception as exc:  # pragma: no cover - defensive
                errors.append(f"table {table.name} not accessible: {exc}")

        pipeline_rows = (
            await session.execute(sa.select(pipelines.c.id, pipelines.c.code))
        ).mappings().all()
        code_to_id = {row["code"]: row["id"] for row in pipeline_rows}

        for pipeline_code, profile_codes in REQUIRED_PIPELINE_PROFILES.items():
            pipeline_id = code_to_id.get(pipeline_code)
            if not pipeline_id:
                errors.append(f"missing pipeline metadata for {pipeline_code}")
                continue

            profile_rows = (
                await session.execute(
                    sa.select(notification_profiles)
                    .where(notification_profiles.c.pipeline_id == pipeline_id)
                    .where(notification_profiles.c.is_active.is_(True))
                )
            ).mappings().all()
            profiles_by_code = {row["code"]: row for row in profile_rows}

            for profile_code in profile_codes:
                profile_row = profiles_by_code.get(profile_code)
                if not profile_row:
                    errors.append(f"missing active profile {profile_code} for pipeline {pipeline_code}")
                    continue

                template_row = (
                    await session.execute(
                        sa.select(email_templates)
                        .where(email_templates.c.profile_id == profile_row["id"])
                        .where(email_templates.c.is_active.is_(True))
                        .where(email_templates.c.name == "default")
                    )
                ).mappings().first()
                if not template_row:
                    errors.append(f"missing template for profile {profile_code} ({pipeline_code})")

                recipients_count = (
                    await session.execute(
                        sa.select(sa.func.count())
                        .select_from(notification_recipients)
                        .where(notification_recipients.c.profile_id == profile_row["id"])
                        .where(notification_recipients.c.is_active.is_(True))
                    )
                ).scalar_one()
                if recipients_count == 0:
                    errors.append(f"profile {profile_code} ({pipeline_code}) has no active recipients")

    return errors
