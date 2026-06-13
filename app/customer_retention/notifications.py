"""DB-backed owner notifications for customer retention management summaries."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import sqlalchemy as sa
from jinja2 import Template
from sqlalchemy.ext.asyncio import AsyncSession

from app.dashboard_downloader.db_tables import (
    email_templates,
    notification_profiles,
    notification_recipients,
    pipelines,
)
from app.dashboard_downloader.json_logger import JsonLogger, log_event
from app.dashboard_downloader.notifications import (
    EmailPlan,
    _collect_recipient_lists,
    _load_smtp_config,
    _send_email,
)

PIPELINE_CODE = "customer_retention_pipeline"
PROFILE_CODE = "owner_summary"

DEFAULT_SUBJECT_TEMPLATE = "Customer Retention Summary {{ run_summary.run_date }} ({{ run_summary.success_failure_status }})"
DEFAULT_BODY_TEMPLATE = """
Customer Retention Pipeline Run {{ run_summary.pipeline_run_id }}
Status: {{ run_summary.success_failure_status }}
Duration: {{ run_summary.duration_seconds }} seconds

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
- {{ row.cost_center }} / {{ row.handled_by }}: assigned={{ row.total_leads_assigned }}, worked={{ row.worked }}, dead_ends={{ row.dead_ends_logged }}
{% endfor %}

Source-Wise Summary:
{% for row in source_wise_summary -%}
- {{ row.source }}: included={{ row.included }}, worked={{ row.worked }}, pending={{ row.pending }}, closed={{ row.closed }}, recovered={{ row.recovered }}, recovered_revenue={{ row.recovered_revenue_value }}
{% endfor %}

Warning/Error Summary:
{{ warning_error_summary }}
"""


@dataclass(frozen=True)
class NotificationResult:
    planned: int
    sent: int
    skipped: bool = False
    reason: str | None = None
    subject: str | None = None
    body: str | None = None


async def send_owner_summary(
    session: AsyncSession,
    *,
    payload: dict[str, Any],
    env: str | None,
    skip_email: bool = False,
    logger: JsonLogger | None = None,
) -> NotificationResult:
    profile, template, recipients = await _load_notification_contract(session, env=env)
    subject = _render(
        template.get("subject_template") or DEFAULT_SUBJECT_TEMPLATE, payload
    )
    body = _render(template.get("body_template") or DEFAULT_BODY_TEMPLATE, payload)
    if skip_email:
        _log(
            logger,
            "info",
            "customer_retention_owner_summary_email_skipped",
            reason="skip_email",
        )
        return NotificationResult(
            planned=1,
            sent=0,
            skipped=True,
            reason="skip_email",
            subject=subject,
            body=body,
        )
    to, cc, bcc = _collect_recipient_lists(recipients, store_code=None)
    if not to and cc:
        to, cc = cc, []
    if not to and not cc:
        _log(logger, "warning", "customer_retention_owner_summary_no_recipients")
        return NotificationResult(
            planned=0,
            sent=0,
            skipped=True,
            reason="no_recipients",
            subject=subject,
            body=body,
        )
    plan = EmailPlan(
        profile_code=str(profile.get("code") or PROFILE_CODE),
        scope="run",
        store_code=None,
        subject=subject,
        body=body,
        to=to,
        cc=cc,
        bcc=bcc,
        attachments=[],
    )
    result = _send_email(_load_smtp_config(), plan)
    sent = 1 if getattr(result, "sent", False) else 0
    _log(
        logger,
        "ok" if sent else "warning",
        "customer_retention_owner_summary_email_dispatched",
        emails_sent=sent,
    )
    return NotificationResult(
        planned=1, sent=sent, skipped=False, subject=subject, body=body
    )


async def _load_notification_contract(
    session: AsyncSession, *, env: str | None
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    def _has_notification_tables(sync_session: Any) -> bool:
        inspector = sa.inspect(sync_session.connection())
        required = {
            "pipelines",
            "notification_profiles",
            "email_templates",
            "notification_recipients",
        }
        return required.issubset(set(inspector.get_table_names()))

    if not await session.run_sync(_has_notification_tables):
        return (
            {"id": None, "code": PROFILE_CODE},
            {
                "subject_template": DEFAULT_SUBJECT_TEMPLATE,
                "body_template": DEFAULT_BODY_TEMPLATE,
            },
            [],
        )
    pipeline_id = (
        await session.execute(
            sa.select(pipelines.c.id).where(pipelines.c.code == PIPELINE_CODE)
        )
    ).scalar_one_or_none()
    profile_row = None
    if pipeline_id is not None:
        profile_row = (
            (
                await session.execute(
                    sa.select(notification_profiles)
                    .where(
                        notification_profiles.c.pipeline_id == pipeline_id,
                        notification_profiles.c.scope == "run",
                        notification_profiles.c.is_active.is_(True),
                        sa.or_(
                            notification_profiles.c.env == env,
                            notification_profiles.c.env == "any",
                            notification_profiles.c.env.is_(None),
                        ),
                    )
                    .order_by(
                        notification_profiles.c.env.desc().nullslast(),
                        notification_profiles.c.id.asc(),
                    )
                    .limit(1)
                )
            )
            .mappings()
            .first()
        )
    profile = dict(profile_row or {"id": None, "code": PROFILE_CODE})
    template_row = None
    if profile.get("id") is not None:
        template_row = (
            (
                await session.execute(
                    sa.select(email_templates)
                    .where(
                        email_templates.c.profile_id == profile["id"],
                        email_templates.c.is_active.is_(True),
                    )
                    .order_by(email_templates.c.id.asc())
                    .limit(1)
                )
            )
            .mappings()
            .first()
        )
    template = dict(
        template_row
        or {
            "subject_template": DEFAULT_SUBJECT_TEMPLATE,
            "body_template": DEFAULT_BODY_TEMPLATE,
        }
    )
    recipients: list[dict[str, Any]] = []
    if profile.get("id") is not None:
        rows = (
            (
                await session.execute(
                    sa.select(notification_recipients).where(
                        notification_recipients.c.profile_id == profile["id"],
                        notification_recipients.c.is_active.is_(True),
                        sa.or_(
                            notification_recipients.c.env == env,
                            notification_recipients.c.env == "any",
                            notification_recipients.c.env.is_(None),
                        ),
                    )
                )
            )
            .mappings()
            .all()
        )
        recipients = [dict(row) for row in rows]
    return profile, template, recipients


def _render(raw: str, context: dict[str, Any]) -> str:
    return Template(raw).render(**context)


def _log(logger: JsonLogger | None, status: str, message: str, **extras: Any) -> None:
    if logger:
        log_event(
            logger=logger, phase="email", status=status, message=message, extras=extras
        )
