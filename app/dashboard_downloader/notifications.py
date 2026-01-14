from __future__ import annotations

import logging
import smtplib
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Iterable, Mapping

import sqlalchemy as sa
from jinja2 import Template

from app.common.date_utils import get_timezone
from app.common.db import session_scope
from app.dashboard_downloader.db_tables import (
    documents,
    email_templates,
    notification_profiles,
    notification_recipients,
    pipeline_run_summaries,
    pipelines,
)
from app.common.dashboard_store import store_master
from app.config import config

logger = logging.getLogger(__name__)

STORE_PROFILE_DOC_TYPES: dict[tuple[str, str], str] = {
    ("dashboard_daily", "store_daily_reports"): "store_daily_pdf",
    ("dashboard_weekly", "store_weekly_reports"): "store_weekly_pdf",
    ("dashboard_monthly", "store_monthly_reports"): "store_monthly_pdf",
}
INGEST_REMARKS_MAX_ROWS = 50
INGEST_REMARKS_MAX_CHARS = 200
STATUS_EXPLANATIONS = {
    "ok": "run completed with no issues recorded",
    "warning": "run completed but row-level issues were recorded",
    "error": "run failed or data could not be ingested",
}
UNIFIED_METRIC_FIELDS = (
    "rows_downloaded",
    "rows_ingested",
    "staging_rows",
    "staging_inserted",
    "staging_updated",
    "final_inserted",
    "final_updated",
)


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


def _load_smtp_config() -> SmtpConfig:
    return SmtpConfig(
        host=config.report_email_smtp_host,
        port=config.report_email_smtp_port,
        sender=config.report_email_from,
        username=config.report_email_smtp_username or None,
        password=config.report_email_smtp_password or None,
        use_tls=config.report_email_use_tls,
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


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    try:
        return int(str(value))
    except Exception:
        return None


def _coalesce_int(payload: Mapping[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = _coerce_int(payload.get(key))
        if value is not None:
            return value
    return None


def _build_unified_metrics(report: Mapping[str, Any] | None) -> dict[str, int | None]:
    if not report:
        return {field: None for field in UNIFIED_METRIC_FIELDS}
    return {
        "rows_downloaded": _coerce_int(report.get("rows_downloaded")),
        "rows_ingested": _coalesce_int(report, "rows_ingested", "final_rows", "staging_rows"),
        "staging_rows": _coerce_int(report.get("staging_rows")),
        "staging_inserted": _coerce_int(report.get("staging_inserted")),
        "staging_updated": _coerce_int(report.get("staging_updated")),
        "final_inserted": _coalesce_int(report, "final_inserted", "rows_inserted"),
        "final_updated": _coalesce_int(report, "final_updated", "rows_updated"),
    }


def _not_applicable_metrics() -> dict[str, Any]:
    payload = {field: None for field in UNIFIED_METRIC_FIELDS}
    payload["label"] = "not applicable"
    return payload


def _sum_unified_metrics(totals: dict[str, int], metrics: Mapping[str, Any]) -> None:
    for field in UNIFIED_METRIC_FIELDS:
        value = _coerce_int(metrics.get(field))
        if value is not None:
            totals[field] = totals.get(field, 0) + value


def _prefix_unified_metrics(prefix: str, metrics: Mapping[str, Any]) -> dict[str, Any]:
    return {f"{prefix}{field}": metrics.get(field) for field in UNIFIED_METRIC_FIELDS}


def _status_explanation(status: str | None) -> str:
    return STATUS_EXPLANATIONS.get(str(status or "").lower(), "run completed with mixed results")


def _status_counts(stores_payload: list[Mapping[str, Any]]) -> dict[str, int]:
    counts = {"ok": 0, "warning": 0, "error": 0}
    for store in stores_payload:
        status = str(store.get("status") or "").lower()
        if status in counts:
            counts[status] += 1
    return counts


def _summarize_ingest_remarks_by_store(
    rows: list[Mapping[str, Any]], *, max_entries: int = 3, max_chars: int = 160
) -> dict[str, dict[str, Any]]:
    per_store: dict[str, list[str]] = {}
    for entry in rows:
        store_code = _normalize_store_code(entry.get("store_code"))
        if not store_code:
            continue
        remark_text = str(entry.get("ingest_remarks") or "").strip()
        if not remark_text:
            continue
        order_number = str(entry.get("order_number") or "").strip()
        prefix = f"{order_number}: " if order_number else ""
        per_store.setdefault(store_code, []).append(f"{prefix}{remark_text}")
    summarized: dict[str, dict[str, Any]] = {}
    for store_code, remarks in per_store.items():
        entries = remarks[:max_entries]
        summary = "; ".join(entries)
        if len(remarks) > max_entries:
            summary = f"{summary}; …"
        if len(summary) > max_chars:
            summary = summary[: max_chars - 1] + "…"
        summarized[store_code] = {"count": len(remarks), "summary": summary}
    return summarized


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


def _prepare_ingest_remarks(
    rows: list[dict[str, Any]], *, max_rows: int = INGEST_REMARKS_MAX_ROWS, max_chars: int = INGEST_REMARKS_MAX_CHARS
) -> tuple[list[dict[str, str]], bool, bool, str]:
    if not rows:
        return [], False, False, ""

    truncated_rows = len(rows) > max_rows
    truncated_length = False
    cleaned_rows: list[dict[str, str]] = []
    for entry in rows[:max_rows]:
        remark = str(entry.get("ingest_remarks") or "")
        if len(remark) > max_chars:
            remark = remark[: max_chars - 1] + "…"
            truncated_length = True
        cleaned_rows.append(
            {
                "store_code": (_normalize_store_code(entry.get("store_code")) or ""),
                "order_number": str(entry.get("order_number") or ""),
                "ingest_remarks": remark,
            }
        )

    lines = [f"- {row['store_code']} {row['order_number']}: {row['ingest_remarks']}" for row in cleaned_rows]
    if truncated_rows:
        hidden = len(rows) - max_rows
        if hidden > 0:
            lines.append(f"... additional {hidden} remarks truncated")
    elif truncated_length:
        lines.append("... some remarks truncated for length")
    ingest_text = "\n".join(lines)
    return cleaned_rows, truncated_rows, truncated_length, ingest_text


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
    store_names: dict[str, str],
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
        if not to and cc:
            logger.info(
                "store recipients fallback to CC",
                extra={
                    "pipeline_code": pipeline_code,
                    "profile_code": profile.get("code"),
                    "store_code": store_code,
                },
            )
            to = cc
            cc = []
        if not to and not cc:
            logger.warning(
                "no active recipients for store report; email skipped",
                extra={
                    "pipeline_code": pipeline_code,
                    "profile_code": profile.get("code"),
                    "store_code": store_code,
                },
            )
            continue
        attachments = _paths_for_documents(store_records)
        if profile.get("attach_mode") == "per_store_pdf" and not attachments:
            continue
        store_context = dict(context)
        store_context["store_code"] = store_code
        store_context["store_name"] = store_names.get(store_code, store_code)
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
    store_names: dict[str, str],
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
                    store_names,
                )
            )
        else:
            logger.debug("notification scope not implemented", extra={"scope": scope})
    return plans


async def _load_notification_resources(
    pipeline_name: str, run_id: str
) -> tuple[dict[str, Any] | None, list[str]]:
    database_url = config.database_url

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

        store_codes: set[str] = set()
        for row in docs_rows:
            code = _normalize_store_code(row.get("reference_id_3"))
            if code and code != "ALL":
                store_codes.add(code)

        store_names: dict[str, str] = {}
        if store_codes:
            store_rows = (
                await session.execute(
                    sa.select(
                        sa.func.upper(store_master.c.store_code).label("store_code"),
                        store_master.c.store_name,
                    ).where(sa.func.upper(store_master.c.store_code).in_(store_codes))
                )
            ).mappings().all()
            store_names = {row["store_code"]: row["store_name"] for row in store_rows}

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
        "store_names": store_names,
    }
    return resources, []


def _normalize_datetime(value: Any) -> str:
    try:
        return value.isoformat()  # type: ignore[attr-defined]
    except Exception:
        return str(value) if value is not None else ""


def _build_td_orders_context(run_data: dict[str, Any]) -> dict[str, Any]:
    metrics = run_data.get("metrics_json") or {}
    payload = metrics.get("notification_payload") or {}
    stores_payload = payload.get("stores") or []

    def _rows_ingested(report: Mapping[str, Any]) -> int:
        for candidate in (report.get("rows_ingested"), report.get("final_rows"), report.get("staging_rows")):
            if candidate is not None:
                return _coerce_int(candidate) or 0
        return 0

    def _count_from_fields(report: Mapping[str, Any], *keys: str) -> int:
        for key in keys:
            value = report.get(key)
            if isinstance(value, int):
                return _coerce_int(value) or 0
            if isinstance(value, list):
                return len(value)
        return 0

    def _with_store_metadata(
        rows: Iterable[Mapping[str, Any]] | None,
        *,
        store_code: str,
        include_order_number: bool = False,
        include_remarks: bool = False,
    ) -> list[dict[str, Any]]:
        prepared: list[dict[str, Any]] = []
        for row in rows or []:
            data = dict(row)
            data.setdefault("store_code", store_code)
            if include_order_number:
                order_number = data.get("order_number")
                if not order_number:
                    values = data.get("values") or {}
                    for key in ("order_number", "Order Number", "Order No."):
                        if values.get(key):
                            order_number = values.get(key)
                            break
                data["order_number"] = "" if order_number in (None, "") else str(order_number)
            if include_remarks:
                remarks = data.get("ingest_remarks") or data.get("remarks")
                if remarks is not None:
                    data["ingest_remarks"] = str(remarks)
            prepared.append(data)
        return prepared

    stores: list[dict[str, Any]] = []
    primary_totals: dict[str, int] = {field: 0 for field in UNIFIED_METRIC_FIELDS}
    secondary_totals: dict[str, int] = {field: 0 for field in UNIFIED_METRIC_FIELDS}
    for store in stores_payload:
        orders = store.get("orders") or {}
        sales = store.get("sales") or {}
        orders_warning_rows = _with_store_metadata(
            orders.get("warning_rows"), store_code=store.get("store_code"), include_order_number=True, include_remarks=True
        )
        orders_dropped_rows = _with_store_metadata(
            orders.get("dropped_rows"), store_code=store.get("store_code"), include_order_number=True, include_remarks=True
        )
        sales_warning_rows = _with_store_metadata(
            sales.get("warning_rows"), store_code=store.get("store_code"), include_order_number=True, include_remarks=True
        )
        sales_dropped_rows = _with_store_metadata(
            sales.get("dropped_rows"), store_code=store.get("store_code"), include_order_number=True, include_remarks=True
        )
        sales_edited_rows = _with_store_metadata(
            sales.get("edited_rows"), store_code=store.get("store_code"), include_order_number=True
        )
        sales_duplicate_rows = _with_store_metadata(
            sales.get("duplicate_rows"), store_code=store.get("store_code"), include_order_number=True
        )
        orders = {**orders, "warning_rows": orders_warning_rows, "dropped_rows": orders_dropped_rows}
        sales = {
            **sales,
            "warning_rows": sales_warning_rows,
            "dropped_rows": sales_dropped_rows,
            "edited_rows": sales_edited_rows,
            "duplicate_rows": sales_duplicate_rows,
        }
        primary_metrics = _build_unified_metrics(orders)
        secondary_metrics = _build_unified_metrics(sales)
        _sum_unified_metrics(primary_totals, primary_metrics)
        _sum_unified_metrics(secondary_totals, secondary_metrics)
        stores.append(
            {
                "store_code": store.get("store_code"),
                "status": store.get("status"),
                "message": store.get("message"),
                "orders_status": orders.get("status"),
                "orders_filenames": orders.get("filenames") or [],
                "orders_staging_rows": orders.get("staging_rows"),
                "orders_final_rows": orders.get("final_rows"),
                "orders_rows_downloaded": _coerce_int(orders.get("rows_downloaded")),
                "orders_rows_ingested": _rows_ingested(orders),
                "orders_warning_count": _count_from_fields(orders, "warning_count", "warning_rows", "warnings"),
                "orders_dropped_rows_count": _count_from_fields(orders, "dropped_rows_count", "dropped_rows"),
                "orders_dropped_rows": orders.get("dropped_rows") or [],
                "orders_warning_rows": orders.get("warning_rows") or [],
                "orders_warnings": orders.get("warnings") or [],
                "orders_error": orders.get("error_message"),
                "sales_status": sales.get("status"),
                "sales_filenames": sales.get("filenames") or [],
                "sales_staging_rows": sales.get("staging_rows"),
                "sales_final_rows": sales.get("final_rows"),
                "sales_rows_downloaded": _coerce_int(sales.get("rows_downloaded")),
                "sales_rows_ingested": _rows_ingested(sales),
                "sales_warning_count": _count_from_fields(sales, "warning_count", "warning_rows", "warnings"),
                "sales_dropped_rows_count": _count_from_fields(sales, "dropped_rows_count", "dropped_rows"),
                "sales_rows_edited": _count_from_fields(sales, "edited_rows_count", "edited_rows"),
                "sales_rows_duplicate": _count_from_fields(sales, "duplicate_rows_count", "duplicate_rows"),
                "sales_dropped_rows": sales.get("dropped_rows") or [],
                "sales_warning_rows": sales.get("warning_rows") or [],
                "sales_edited_rows": sales.get("edited_rows") or [],
                "sales_duplicate_rows": sales.get("duplicate_rows") or [],
                "sales_warnings": sales.get("warnings") or [],
                "sales_error": sales.get("error_message"),
                "primary_metrics": primary_metrics,
                "secondary_metrics": secondary_metrics,
                **_prefix_unified_metrics("primary_", primary_metrics),
                **_prefix_unified_metrics("secondary_", secondary_metrics),
            }
        )

    summary_text = _td_summary_text_from_payload(run_data) or run_data.get("summary_text") or ""
    started_at_formatted = _format_td_timestamp(payload.get("started_at") or run_data.get("started_at"))
    finished_at_formatted = _format_td_timestamp(payload.get("finished_at") or run_data.get("finished_at"))
    window_summary = metrics.get("window_summary") or {}

    return {
        "summary_text": summary_text,
        "td_summary_text": summary_text,
        "started_at": _normalize_datetime(payload.get("started_at") or run_data.get("started_at")),
        "finished_at": _normalize_datetime(payload.get("finished_at") or run_data.get("finished_at")),
        "started_at_formatted": started_at_formatted,
        "finished_at_formatted": finished_at_formatted,
        "total_time_taken": payload.get("total_time_taken") or run_data.get("total_time_taken"),
        "overall_status": payload.get("overall_status") or run_data.get("overall_status"),
        "td_overall_status": payload.get("overall_status") or run_data.get("overall_status"),
        "orders_status": payload.get("orders_status") or (metrics.get("orders") or {}).get("overall_status"),
        "sales_status": payload.get("sales_status") or (metrics.get("sales") or {}).get("overall_status"),
        "stores": stores,
        "td_all_stores_failed": _td_all_stores_failed(stores_payload),
        "notification_payload": payload,
        "primary_totals": primary_totals,
        "secondary_totals": secondary_totals,
        "window_summary": window_summary,
        "expected_windows": window_summary.get("expected_windows"),
        "completed_windows": window_summary.get("completed_windows"),
        "missing_windows": window_summary.get("missing_windows"),
        "missing_window_stores": window_summary.get("missing_store_codes") or [],
    }


def _build_uc_orders_context(run_data: dict[str, Any]) -> dict[str, Any]:
    metrics = run_data.get("metrics_json") or {}
    payload = metrics.get("notification_payload") or {}
    stores_payload = payload.get("stores") or []
    ingest_rows = (metrics.get("ingest_remarks") or {}).get("rows") or []
    warning_summaries = _summarize_ingest_remarks_by_store(ingest_rows)
    summary_stores = (metrics.get("stores_summary") or {}).get("stores") or {}
    summary_warning_counts = {
        _normalize_store_code(store_code) or store_code: _coerce_int(store_data.get("warning_count"))
        for store_code, store_data in summary_stores.items()
        if store_code
    }
    window_warning_counts: dict[str, int] = {}
    for entry in metrics.get("window_audit") or []:
        status = str(entry.get("status") or "").lower()
        if status not in {"warning", "warn"}:
            continue
        store_code = _normalize_store_code(entry.get("store_code")) or entry.get("store_code")
        if not store_code:
            continue
        window_warning_counts[store_code] = window_warning_counts.get(store_code, 0) + 1
    status_counts = _status_counts(stores_payload)

    stores: list[dict[str, Any]] = []
    primary_totals: dict[str, int] = {field: 0 for field in UNIFIED_METRIC_FIELDS}
    secondary_metrics = _not_applicable_metrics()
    for store in stores_payload:
        store_code = _normalize_store_code(store.get("store_code")) or store.get("store_code")
        status = str(store.get("status") or "").lower()
        warning_data = warning_summaries.get(_normalize_store_code(store.get("store_code")) or "", {})
        warning_summary = warning_data.get("summary")
        warning_count = _coerce_int(store.get("warning_count"))
        if warning_count is None:
            warning_count = summary_warning_counts.get(_normalize_store_code(store_code) or "")
        if warning_count is None and warning_data:
            warning_count = _coerce_int(warning_data.get("count"))
        window_warning_count = window_warning_counts.get(_normalize_store_code(store_code) or "")
        if warning_count is None and window_warning_count is None:
            resolved_warning_count = None
        else:
            resolved_warning_count = 0
            if warning_count is not None:
                resolved_warning_count += warning_count
            if window_warning_count:
                resolved_warning_count += window_warning_count
        primary_metrics = _build_unified_metrics(store)
        _sum_unified_metrics(primary_totals, primary_metrics)
        stores.append(
            {
                "store_code": store_code,
                "status": store.get("status"),
                "message": store.get("message"),
                "error_message": store.get("error_message") if status == "error" else None,
                "info_message": store.get("info_message")
                or (store.get("message") if status != "error" else None),
                "warning_count": resolved_warning_count,
                "warnings_summary": warning_summary,
                "filename": store.get("filename"),
                "staging_rows": store.get("staging_rows"),
                "final_rows": store.get("final_rows"),
                "staging_inserted": store.get("staging_inserted"),
                "staging_updated": store.get("staging_updated"),
                "final_inserted": store.get("final_inserted"),
                "final_updated": store.get("final_updated"),
                "primary_metrics": primary_metrics,
                "secondary_metrics": secondary_metrics,
                **_prefix_unified_metrics("primary_", primary_metrics),
                **_prefix_unified_metrics("secondary_", secondary_metrics),
            }
        )

    started_at = payload.get("started_at") or run_data.get("started_at")
    finished_at = payload.get("finished_at") or run_data.get("finished_at")
    window_summary = metrics.get("window_summary") or {}
    summary_text = _uc_summary_text_from_payload(run_data) or run_data.get("summary_text") or ""

    return {
        "summary_text": summary_text,
        "started_at": _normalize_datetime(started_at),
        "finished_at": _normalize_datetime(finished_at),
        "total_time_taken": payload.get("total_time_taken") or run_data.get("total_time_taken"),
        "overall_status": payload.get("overall_status") or run_data.get("overall_status"),
        "overall_status_explanation": _status_explanation(payload.get("overall_status") or run_data.get("overall_status")),
        "store_status_counts": status_counts,
        "stores_succeeded": status_counts.get("ok", 0),
        "stores_warned": status_counts.get("warning", 0),
        "stores_failed": status_counts.get("error", 0),
        "stores": stores,
        "uc_all_stores_failed": _uc_all_stores_failed(stores_payload),
        "notification_payload": payload,
        "primary_totals": primary_totals,
        "secondary_totals": secondary_metrics,
        "secondary_metrics_label": secondary_metrics.get("label"),
        "window_summary": window_summary,
        "expected_windows": window_summary.get("expected_windows"),
        "completed_windows": window_summary.get("completed_windows"),
        "missing_windows": window_summary.get("missing_windows"),
        "missing_window_stores": window_summary.get("missing_store_codes") or [],
    }


def _td_all_stores_failed(stores_payload: list[Mapping[str, Any]]) -> bool:
    if not stores_payload:
        return True

    def _failed(report: Mapping[str, Any]) -> bool:
        status = str(report.get("status") or "").lower()
        if status in {"ok", "warning", "skipped"}:
            return False
        return True

    for store in stores_payload:
        orders_failed = _failed(store.get("orders") or {})
        sales_failed = _failed(store.get("sales") or {})
        if not (orders_failed and sales_failed):
            return False
    return True


def _uc_all_stores_failed(stores_payload: list[Mapping[str, Any]]) -> bool:
    if not stores_payload:
        return True

    def _failed(report: Mapping[str, Any]) -> bool:
        status = str(report.get("status") or "").lower()
        if status in {"ok", "warning", "skipped"}:
            return False
        return True

    for store in stores_payload:
        if not _failed(store):
            return False
    return True


def _format_td_timestamp(value: Any) -> str:
    if not value:
        return ""
    tz = get_timezone()
    try:
        if isinstance(value, str):
            dt = datetime.fromisoformat(value)
        elif isinstance(value, datetime):
            dt = value
        else:
            return _normalize_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(tz).strftime("%d-%m-%Y %H:%M:%S")
    except Exception:
        return _normalize_datetime(value)


def _td_summary_text_from_payload(run_data: Mapping[str, Any]) -> str:
    metrics = run_data.get("metrics_json") or {}
    payload = metrics.get("notification_payload") or {}
    if not payload:
        return ""

    stores_payload = payload.get("stores") or []

    def _rows_ingested(report: Mapping[str, Any]) -> int:
        for candidate in (report.get("rows_ingested"), report.get("final_rows"), report.get("staging_rows")):
            if candidate is not None:
                try:
                    return int(candidate)
                except Exception:
                    return 0
        return 0

    def _count(report: Mapping[str, Any], *keys: str) -> int:
        for key in keys:
            value = report.get(key)
            if isinstance(value, int):
                return value
            if isinstance(value, list):
                return len(value)
        return 0

    def _format_store_section(*, sales: bool = False) -> list[str]:
        lines: list[str] = []
        for store in stores_payload:
            report = (store.get("sales") if sales else store.get("orders")) or {}
            status = str(report.get("status") or "unknown").upper()
            rows_downloaded = report.get("rows_downloaded") or 0
            rows_ingested = _rows_ingested(report)
            warning_count = _count(report, "warning_count", "warning_rows", "warnings")
            dropped_count = _count(report, "dropped_rows_count", "dropped_rows")
            base = [
                f"- {store.get('store_code') or 'UNKNOWN'} — {status}",
                f"  rows_downloaded: {rows_downloaded}",
                f"  rows_ingested: {rows_ingested}",
                f"  warning_count: {warning_count}",
                f"  dropped_count: {dropped_count}",
            ]
            if sales:
                edited = _count(report, "edited_rows_count", "edited_rows")
                duplicate = _count(report, "duplicate_rows_count", "duplicate_rows")
                base.extend(
                    [
                        f"  edited_count: {edited}",
                        f"  duplicate_count: {duplicate}",
                    ]
                )
            lines.extend(base)
        if not lines:
            lines.append("- (none)")
        return lines

    started = _format_td_timestamp(payload.get("started_at") or run_data.get("started_at"))
    finished = _format_td_timestamp(payload.get("finished_at") or run_data.get("finished_at"))
    duration = payload.get("total_time_taken") or run_data.get("total_time_taken") or ""
    orders_status = payload.get("orders_status") or (metrics.get("orders") or {}).get("overall_status")
    sales_status = payload.get("sales_status") or (metrics.get("sales") or {}).get("overall_status")
    window_summary = metrics.get("window_summary") or {}
    status_line = f"Overall Status: {payload.get('overall_status') or run_data.get('overall_status')} (Orders: {orders_status}, Sales: {sales_status})"
    window_lines = []
    if window_summary:
        window_lines = [
            f"Windows Completed: {window_summary.get('completed_windows', 0)} / {window_summary.get('expected_windows', 0)}",
            f"Missing Windows: {window_summary.get('missing_windows', 0)}",
        ]
        missing_stores = window_summary.get("missing_store_codes") or []
        if missing_stores:
            window_lines.append(f"Missing Window Stores: {', '.join(missing_stores)}")

    lines = [
        "TD Orders & Sales Run Summary",
        f"Run ID: {run_data.get('run_id')} | Env: {run_data.get('run_env')}",
        f"Report Date: {(run_data.get('report_date') or '')}",
        f"Started (Asia/Kolkata): {started}",
        f"Finished (Asia/Kolkata): {finished}",
    ]
    if duration:
        lines.append(f"Total Duration: {duration}")
    lines.extend([status_line, *window_lines])
    lines.extend(
        [
            "",
            "**Per Store Orders Metrics:**",
            *_format_store_section(sales=False),
            "",
            "**Per Store Sales Metrics:**",
            *_format_store_section(sales=True),
        ]
    )
    if _td_all_stores_failed(stores_payload):
        lines.append("All TD stores failed for Orders and Sales.")
    return "\n".join(lines)


def _uc_summary_text_from_payload(run_data: Mapping[str, Any]) -> str:
    metrics = run_data.get("metrics_json") or {}
    payload = metrics.get("notification_payload") or {}
    stores_payload = payload.get("stores") or []
    if not payload:
        return ""
    status_counts = _status_counts(stores_payload)
    total = sum(status_counts.values())
    overall_status = payload.get("overall_status") or run_data.get("overall_status")
    status_explanation = _status_explanation(overall_status)
    window_summary = metrics.get("window_summary") or {}
    missing_windows = window_summary.get("missing_windows")
    missing_stores = window_summary.get("missing_store_codes") or []
    missing_line = f"Missing Windows: {missing_windows if missing_windows is not None else 0}"
    if missing_stores:
        missing_line += f" ({', '.join(missing_stores)})"
    return (
        "UC GST Run Summary\n"
        f"Overall Status: {overall_status} ({status_explanation})\n"
        f"Stores: {status_counts.get('ok', 0)} ok, {status_counts.get('warning', 0)} warnings, "
        f"{status_counts.get('error', 0)} errors across {total} stores\n"
        f"Windows Completed: {window_summary.get('completed_windows', 0)} / {window_summary.get('expected_windows', 0)}\n"
        f"{missing_line}"
    )


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
    store_names = resources["store_names"]

    context: dict[str, Any] = {
        "pipeline_name": pipeline_name,
        "pipeline_description": pipeline_data["description"],
        "run_id": run_id,
        "run_env": run_data["run_env"],
        "report_date": run_data.get("report_date").isoformat() if run_data.get("report_date") else "",
        "overall_status": run_data.get("overall_status"),
        "total_time_taken": run_data.get("total_time_taken"),
        "summary_text": run_data.get("summary_text", ""),
        "metrics_json": run_data.get("metrics_json") or {},
    }
    if pipeline_name == "td_orders_sync":
        context.update(_build_td_orders_context(run_data))
    elif pipeline_name == "uc_orders_sync":
        context.update(_build_uc_orders_context(run_data))
    else:
        context["started_at"] = _normalize_datetime(run_data.get("started_at"))
        context["finished_at"] = _normalize_datetime(run_data.get("finished_at"))
    ingest_rows = (run_data.get("metrics_json") or {}).get("ingest_remarks", {}).get("rows") or []
    prepared_rows, truncated_rows, truncated_length, ingest_text = _prepare_ingest_remarks(ingest_rows)
    context["ingest_remarks"] = prepared_rows
    context["ingest_remarks_truncated"] = truncated_rows or truncated_length
    context["ingest_remarks_truncated_rows"] = truncated_rows
    context["ingest_remarks_truncated_length"] = truncated_length
    context["ingest_remarks_text"] = ingest_text

    plans = _build_email_plans(
        pipeline_code=pipeline_name,
        profiles=profiles_data,
        templates=templates_map,
        recipients=recipients_by_profile,
        docs=documents_list,
        context=context,
        store_names=store_names,
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
    store_names = resources["store_names"]

    context = {
        "pipeline_name": pipeline_name,
        "pipeline_description": pipeline_data["description"],
        "run_id": run_id,
        "run_env": run_data["run_env"],
        "report_date": run_data.get("report_date").isoformat() if run_data.get("report_date") else "",
        "overall_status": run_data.get("overall_status"),
        "total_time_taken": run_data.get("total_time_taken"),
        "summary_text": run_data.get("summary_text", ""),
        "metrics_json": run_data.get("metrics_json") or {},
    }
    if pipeline_name == "td_orders_sync":
        context.update(_build_td_orders_context(run_data))
    elif pipeline_name == "uc_orders_sync":
        context.update(_build_uc_orders_context(run_data))
    else:
        context["started_at"] = _normalize_datetime(run_data.get("started_at"))
        context["finished_at"] = _normalize_datetime(run_data.get("finished_at"))

    plans = _build_email_plans(
        pipeline_code=pipeline_name,
        profiles=profiles_data,
        templates=templates_map,
        recipients=recipients_by_profile,
        docs=documents_list,
        context=context,
        store_names=store_names,
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
