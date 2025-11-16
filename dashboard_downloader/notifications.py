from __future__ import annotations

import logging
import os
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Iterable

import sqlalchemy as sa
from jinja2 import Template

from common.db import session_scope
from dashboard_downloader.db_tables import (
    documents,
    email_templates,
    notification_profiles,
    notification_recipients,
    pipeline_run_summaries,
    pipelines,
)

logger = logging.getLogger(__name__)

STORE_PROFILE_DOC_TYPES: dict[tuple[str, str], str] = {
    ("simplify_dashboard_daily", "store_daily_reports"): "store_daily_pdf",
    ("simplify_dashboard_weekly", "store_weekly_reports"): "store_weekly_pdf",
    ("simplify_dashboard_monthly", "store_monthly_reports"): "store_monthly_pdf",
}


@dataclass
class SmtpConfig:
    host: str
    port: int
    sender: str
    username: str | None
    password: str | None
    use_tls: bool


@dataclass
class DocumentRecord:
    doc_type: str
    store_code: str | None
    path: Path


@dataclass
class EmailPlan:
    profile_code: str
    scope: str
    store_code: str | None
    subject: str
    body: str
    to: list[str]
    cc: list[str]
    bcc: list[str]
    attachments: list[Path]


def _load_smtp_config() -> SmtpConfig | None:
    host = os.getenv("REPORT_EMAIL_SMTP_HOST")
    port_raw = os.getenv("REPORT_EMAIL_SMTP_PORT")
    if not host or not port_raw:
        return None
    sender = os.getenv("REPORT_EMAIL_FROM", "reports@tsv.com")
    username = os.getenv("REPORT_EMAIL_SMTP_USERNAME")
    password = os.getenv("REPORT_EMAIL_SMTP_PASSWORD")
    use_tls = os.getenv("REPORT_EMAIL_USE_TLS", "true").lower() == "true"
    return SmtpConfig(
        host=host,
        port=int(port_raw),
        sender=sender,
        username=username,
        password=password,
        use_tls=use_tls,
    )


def _render_template(raw: str, context: dict[str, Any]) -> str:
    try:
        return Template(raw).render(**context)
    except Exception:
        logger.exception("failed to render notification template")
        return raw


def _format_address(email: str, display_name: str | None) -> str:
    if display_name:
        return f"{display_name} <{email}>"
    return email


def _unique(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def _normalize_store_code(value: str | None) -> str | None:
    if not value:
        return None
    return value.upper()


def _recipient_matches(row_store: str | None, store_code: str | None) -> bool:
    if store_code is None:
        return row_store is None
    if row_store is None:
        return True
    return row_store.upper() == store_code


def _collect_recipient_lists(
    rows: list[dict[str, Any]],
    *,
    store_code: str | None,
) -> tuple[list[str], list[str], list[str]]:
    to: list[str] = []
    cc: list[str] = []
    bcc: list[str] = []
    for row in rows:
        target_store = _normalize_store_code(row.get("store_code"))
        if not _recipient_matches(target_store, store_code):
            continue
        address = _format_address(row["email_address"], row.get("display_name"))
        send_as = (row.get("send_as") or "to").lower()
        if send_as == "cc":
            cc.append(address)
        elif send_as == "bcc":
            bcc.append(address)
        else:
            to.append(address)
    return _unique(to), _unique(cc), _unique(bcc)


def _paths_for_documents(records: Iterable[DocumentRecord]) -> list[Path]:
    attachments: list[Path] = []
    for record in records:
        if record.path.exists():
            attachments.append(record.path)
        else:
            logger.warning("attachment missing on disk: %s", record.path)
    return attachments


def _group_documents(rows: list[dict[str, Any]]) -> list[DocumentRecord]:
    records: list[DocumentRecord] = []
    for row in rows:
        storage_backend = row.get("storage_backend") or "fs"
        if storage_backend != "fs":
            continue
        raw_path = row.get("file_path")
        if not raw_path:
            continue
        doc_type = row.get("doc_type")
        if not doc_type:
            continue
        store_code = _normalize_store_code(row.get("reference_id_3"))
        records.append(DocumentRecord(doc_type=doc_type, store_code=store_code, path=Path(raw_path)))
    return records


def _build_run_plan(
    profile: dict[str, Any],
    template: dict[str, Any] | None,
    recipients: list[dict[str, Any]],
    docs: list[DocumentRecord],
    context: dict[str, Any],
) -> EmailPlan | None:
    if not template:
        return None
    to, cc, bcc = _collect_recipient_lists(recipients, store_code=None)
    if not to:
        return None
    attachments: list[Path] = []
    attach_mode = profile.get("attach_mode")
    if attach_mode == "all_docs_for_run":
        attachments = _paths_for_documents(docs)
    elif attach_mode == "all_store_pdfs":
        attachments = _paths_for_documents([rec for rec in docs if rec.store_code])
    subject = _render_template(template["subject_template"], context)
    body = _render_template(template["body_template"], context)
    return EmailPlan(
        profile_code=profile["code"],
        scope=profile["scope"],
        store_code=None,
        subject=subject,
        body=body,
        to=to,
        cc=cc,
        bcc=bcc,
        attachments=attachments,
    )


def _build_store_plans(
    pipeline_code: str,
    profile: dict[str, Any],
    template: dict[str, Any] | None,
    recipients: list[dict[str, Any]],
    docs: list[DocumentRecord],
    context: dict[str, Any],
) -> list[EmailPlan]:
    if not template:
        return []
    doc_type = STORE_PROFILE_DOC_TYPES.get((pipeline_code, profile["code"]))
    if not doc_type:
        return []
    plans: list[EmailPlan] = []
    grouped: dict[str, list[DocumentRecord]] = {}
    for record in docs:
        if record.doc_type != doc_type or not record.store_code or record.store_code == "ALL":
            continue
        grouped.setdefault(record.store_code, []).append(record)
    for store_code in sorted(grouped):
        store_records = grouped[store_code]
        to, cc, bcc = _collect_recipient_lists(recipients, store_code=store_code)
        if not to:
            continue
        attachments = _paths_for_documents(store_records)
        if profile.get("attach_mode") == "per_store_pdf" and not attachments:
            continue
        store_context = dict(context)
        store_context["store_code"] = store_code
        subject = _render_template(template["subject_template"], store_context)
        body = _render_template(template["body_template"], store_context)
        plans.append(
            EmailPlan(
                profile_code=profile["code"],
                scope=profile["scope"],
                store_code=store_code,
                subject=subject,
                body=body,
                to=to,
                cc=cc,
                bcc=bcc,
                attachments=attachments,
            )
        )
    return plans


def _send_email(config: SmtpConfig, plan: EmailPlan) -> bool:
    message = EmailMessage()
    message["Subject"] = plan.subject
    message["From"] = config.sender
    message["To"] = ", ".join(plan.to)
    if plan.cc:
        message["Cc"] = ", ".join(plan.cc)
    message.set_content(plan.body)
    for attachment in plan.attachments:
        try:
            data = attachment.read_bytes()
        except FileNotFoundError:
            logger.warning("attachment missing during send", extra={"path": str(attachment)})
            continue
        message.add_attachment(
            data,
            maintype="application",
            subtype="pdf",
            filename=attachment.name,
        )
    recipients = _unique(plan.to + plan.cc + plan.bcc)
    if not recipients:
        return False
    try:
        if config.use_tls:
            with smtplib.SMTP(config.host, config.port) as client:
                client.starttls()
                if config.username and config.password:
                    client.login(config.username, config.password)
                client.send_message(message, to_addrs=recipients)
        else:
            with smtplib.SMTP(config.host, config.port) as client:
                if config.username and config.password:
                    client.login(config.username, config.password)
                client.send_message(message, to_addrs=recipients)
        return True
    except Exception:
        logger.exception(
            "failed to send notification email",
            extra={"profile": plan.profile_code, "store_code": plan.store_code},
        )
        return False


def _build_email_plans(
    *,
    pipeline_code: str,
    profiles: list[dict[str, Any]],
    templates: dict[int, dict[str, Any]],
    recipients: dict[int, list[dict[str, Any]]],
    docs: list[DocumentRecord],
    context: dict[str, Any],
) -> list[EmailPlan]:
    plans: list[EmailPlan] = []
    for profile in profiles:
        profile_recipients = recipients.get(profile["id"], [])
        if not profile_recipients:
            continue
        template = templates.get(profile["id"])
        scope = profile.get("scope")
        if scope == "run":
            plan = _build_run_plan(profile, template, profile_recipients, docs, context)
            if plan:
                plans.append(plan)
        elif scope == "store":
            plans.extend(
                _build_store_plans(
                    pipeline_code,
                    profile,
                    template,
                    profile_recipients,
                    docs,
                    context,
                )
            )
        else:
            logger.debug("notification scope not implemented", extra={"scope": scope})
    return plans


async def _load_notification_resources(
    pipeline_name: str, run_id: str
) -> tuple[dict[str, Any] | None, list[str]]:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return None, ["DATABASE_URL is not configured"]

    async with session_scope(database_url) as session:
        pipeline_row = (
            await session.execute(sa.select(pipelines).where(pipelines.c.code == pipeline_name))
        ).mappings().first()
        if not pipeline_row:
            return None, [f"pipeline {pipeline_name} is not registered in notification metadata"]

        run_row = (
            await session.execute(
                sa.select(pipeline_run_summaries).where(pipeline_run_summaries.c.run_id == run_id)
            )
        ).mappings().first()
        if not run_row:
            return None, [f"run summary {run_id} not found"]

        docs_rows = (
            await session.execute(
                sa.select(documents)
                .where(documents.c.reference_name_1 == "pipeline")
                .where(documents.c.reference_id_1 == pipeline_name)
                .where(documents.c.reference_name_2 == "run_id")
                .where(documents.c.reference_id_2 == run_id)
                .where(documents.c.status == "ok")
            )
        ).mappings().all()

        profiles_rows = (
            await session.execute(
                sa.select(notification_profiles)
                .where(notification_profiles.c.pipeline_id == pipeline_row["id"])
                .where(notification_profiles.c.is_active.is_(True))
                .where(notification_profiles.c.env.in_(["any", run_row["run_env"]]))
            )
        ).mappings().all()
        if not profiles_rows:
            return None, [f"no active notification profiles found for pipeline {pipeline_name}"]

        profile_ids = [row["id"] for row in profiles_rows]
        templates_rows = (
            await session.execute(
                sa.select(email_templates)
                .where(email_templates.c.profile_id.in_(profile_ids))
                .where(email_templates.c.is_active.is_(True))
                .where(email_templates.c.name == "default")
            )
        ).mappings().all()
        templates_by_profile = {row["profile_id"]: dict(row) for row in templates_rows}

        recipients_rows = (
            await session.execute(
                sa.select(notification_recipients)
                .where(notification_recipients.c.profile_id.in_(profile_ids))
                .where(notification_recipients.c.is_active.is_(True))
                .where(notification_recipients.c.env.in_(["any", run_row["run_env"]]))
            )
        ).mappings().all()

    recipients_by_profile: dict[int, list[dict[str, Any]]] = {}
    for row in recipients_rows:
        data = dict(row)
        recipients_by_profile.setdefault(data["profile_id"], []).append(data)

    resources = {
        "pipeline": dict(pipeline_row),
        "run": dict(run_row),
        "docs": _group_documents([dict(row) for row in docs_rows]),
        "profiles": [dict(row) for row in profiles_rows],
        "templates": templates_by_profile,
        "recipients": recipients_by_profile,
    }
    return resources, []


async def send_notifications_for_run(pipeline_name: str, run_id: str) -> None:
    resources, errors = await _load_notification_resources(pipeline_name, run_id)
    if errors:
        for message in errors:
            logger.warning(message, extra={"pipeline": pipeline_name, "run_id": run_id})
        return

    pipeline_data = resources["pipeline"]
    run_data = resources["run"]
    documents_list = resources["docs"]
    profiles_data = resources["profiles"]
    templates_map = resources["templates"]
    recipients_by_profile = resources["recipients"]

    context = {
        "pipeline_name": pipeline_name,
        "pipeline_description": pipeline_data["description"],
        "run_id": run_id,
        "run_env": run_data["run_env"],
        "report_date": run_data.get("report_date").isoformat() if run_data.get("report_date") else "",
        "overall_status": run_data.get("overall_status"),
        "summary_text": run_data.get("summary_text", ""),
    }

    plans = _build_email_plans(
        pipeline_code=pipeline_name,
        profiles=profiles_data,
        templates=templates_map,
        recipients=recipients_by_profile,
        docs=documents_list,
        context=context,
    )

    if not plans:
        logger.info("No notification emails scheduled", extra={"pipeline": pipeline_name, "run_id": run_id})
        return

    smtp_config = _load_smtp_config()
    if not smtp_config:
        logger.warning("SMTP configuration missing; skipping notifications")
        return

    sent = 0
    for plan in plans:
        if _send_email(smtp_config, plan):
            sent += 1
    logger.info(
        "Notification dispatch complete",
        extra={"pipeline": pipeline_name, "run_id": run_id, "emails_sent": sent, "emails_planned": len(plans)},
    )


async def diagnose_notification_run(pipeline_name: str, run_id: str) -> list[str]:
    """Return any findings for the notifications CLI diagnostic command."""

    findings: list[str] = []
    resources, errors = await _load_notification_resources(pipeline_name, run_id)
    findings.extend(errors)

    if not _load_smtp_config():
        findings.append("SMTP configuration is incomplete (REPORT_EMAIL_SMTP_* and REPORT_EMAIL_FROM)")

    if not resources:
        return findings

    pipeline_data = resources["pipeline"]
    run_data = resources["run"]
    documents_list = resources["docs"]
    profiles_data = resources["profiles"]
    templates_map = resources["templates"]
    recipients_by_profile = resources["recipients"]

    context = {
        "pipeline_name": pipeline_name,
        "pipeline_description": pipeline_data["description"],
        "run_id": run_id,
        "run_env": run_data["run_env"],
        "report_date": run_data.get("report_date").isoformat() if run_data.get("report_date") else "",
        "overall_status": run_data.get("overall_status"),
        "summary_text": run_data.get("summary_text", ""),
    }

    plans = _build_email_plans(
        pipeline_code=pipeline_name,
        profiles=profiles_data,
        templates=templates_map,
        recipients=recipients_by_profile,
        docs=documents_list,
        context=context,
    )
    if not plans:
        findings.append("No notification emails would be scheduled for this run. Check recipients/templates/docs.")
    else:
        planned_profiles = {plan.profile_code for plan in plans}
        for profile in profiles_data:
            if profile["code"] not in planned_profiles:
                findings.append(
                    f"Profile {profile['code']} is active but has no matching recipients or documents for run {run_id}."
                )

    expected_doc_types = {
        doc_type
        for (pipeline_code, _), doc_type in STORE_PROFILE_DOC_TYPES.items()
        if pipeline_code == pipeline_name
    }
    for doc_type in expected_doc_types:
        if not any(record.doc_type == doc_type for record in documents_list):
            findings.append(f"No documents of type {doc_type} recorded for run {run_id}.")

    return findings
