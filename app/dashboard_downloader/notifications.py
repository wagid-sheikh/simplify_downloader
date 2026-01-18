from __future__ import annotations

import json
import logging
import smtplib
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

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
STATUS_EXPLANATIONS = {
    "ok": "run completed with no issues recorded",
    "success": "run completed with no issues recorded",
    "success_with_warnings": "run completed with row-level warnings",
    "warning": "run completed but row-level issues were recorded",
    "partial": "run completed but row-level issues were recorded",
    "error": "run failed or data could not be ingested",
    "failed": "run failed or data could not be ingested",
    "skipped": "run was skipped or produced no data",
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
FACT_ROW_COLUMNS = ("store_code", "order_number", "order_date", "customer_name", "mobile_number")
FACT_ROW_COLUMNS_WITH_REMARKS = (*FACT_ROW_COLUMNS, "ingestion_remarks")
MISSING_ORDER_NUMBER_PLACEHOLDER = "<missing_order_number>"
TD_SALES_ROW_SAMPLE_LIMIT: int | None = None
TD_SALES_EDITED_LIMIT: int | None = None
FACT_SECTION_ROW_LIMIT: int | None = None
PROFILER_FACT_ROW_LIMIT_PER_STORE: int = 200
UC_GSTIN_MISSING_REMARK = "Customer GSTIN missing"
PROFILER_HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
  <body style="margin:0; padding:0; background-color:#f5f5f5;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="width:100%; background-color:#f5f5f5;">
      <tr>
        <td align="center" style="padding:24px 12px;">
          <table role="presentation" width="640" cellpadding="0" cellspacing="0" style="width:640px; max-width:640px; background-color:#ffffff; border:1px solid #e1e1e1;">
            <tr>
              <td style="padding:20px 24px 12px 24px; font-family:Arial, sans-serif; color:#111111;">
                <div style="font-size:20px; font-weight:bold; margin:0 0 4px 0;">
                  Orders Sync Profiler Run Summary
                </div>
                <div style="font-size:13px; color:#666666; margin:0;">
                  Report Date: {{ report_date }}
                </div>
              </td>
            </tr>
            <tr>
              <td style="padding:0 24px 16px 24px;">
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="width:100%; border-collapse:collapse; font-family:Arial, sans-serif; font-size:13px; color:#111111;">
                  <tr>
                    <td style="padding:6px 8px; background-color:#f7f7f7; border:1px solid #e1e1e1; width:25%;">Run ID</td>
                    <td style="padding:6px 8px; border:1px solid #e1e1e1;">{{ run_id }}</td>
                    <td style="padding:6px 8px; background-color:#f7f7f7; border:1px solid #e1e1e1; width:25%;">Env</td>
                    <td style="padding:6px 8px; border:1px solid #e1e1e1;">{{ run_env }}</td>
                  </tr>
                  <tr>
                    <td style="padding:6px 8px; background-color:#f7f7f7; border:1px solid #e1e1e1;">Status</td>
                    <td style="padding:6px 8px; border:1px solid #e1e1e1;">{{ overall_status_label or overall_status }}</td>
                    <td style="padding:6px 8px; background-color:#f7f7f7; border:1px solid #e1e1e1;">Duration</td>
                    <td style="padding:6px 8px; border:1px solid #e1e1e1;">{{ total_time_taken }}</td>
                  </tr>
                  <tr>
                    <td style="padding:6px 8px; background-color:#f7f7f7; border:1px solid #e1e1e1;">Started</td>
                    <td style="padding:6px 8px; border:1px solid #e1e1e1;">{{ started_at }}</td>
                    <td style="padding:6px 8px; background-color:#f7f7f7; border:1px solid #e1e1e1;">Finished</td>
                    <td style="padding:6px 8px; border:1px solid #e1e1e1;">{{ finished_at }}</td>
                  </tr>
                </table>
              </td>
            </tr>
            <tr>
              <td style="padding:0 24px 16px 24px;">
                <div style="font-family:Arial, sans-serif; font-size:14px; font-weight:bold; margin-bottom:8px;">KPI Summary</div>
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="width:100%; border-collapse:collapse; font-family:Arial, sans-serif; font-size:13px; color:#111111;">
                  <tr>
                    <td style="padding:8px; border:1px solid #e1e1e1; background-color:#f7f7f7;">Windows Completed</td>
                    <td style="padding:8px; border:1px solid #e1e1e1;">{{ completed_windows or 0 }} / {{ expected_windows or 0 }}</td>
                    <td style="padding:8px; border:1px solid #e1e1e1; background-color:#f7f7f7;">Missing Windows</td>
                    <td style="padding:8px; border:1px solid #e1e1e1;">{{ missing_windows or 0 }}</td>
                  </tr>
                  <tr>
                    <td style="padding:8px; border:1px solid #e1e1e1; background-color:#f7f7f7;">Missing Window Stores</td>
                    <td style="padding:8px; border:1px solid #e1e1e1;" colspan="3">
                      {% if missing_window_stores %}{{ missing_window_stores | join(', ') }}{% else %}—{% endif %}
                    </td>
                  </tr>
                </table>
              </td>
            </tr>
            <tr>
              <td style="padding:0 24px 16px 24px;">
                <div style="font-family:Arial, sans-serif; font-size:14px; font-weight:bold; margin-bottom:8px;">Store Window Run Summary</div>
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="width:100%; border-collapse:collapse; font-family:Arial, sans-serif; font-size:12px; color:#111111;">
                  <tr>
                    <th align="left" style="padding:6px 8px; border:1px solid #e1e1e1; background-color:#f0f0f0;">Store</th>
                    <th align="left" style="padding:6px 8px; border:1px solid #e1e1e1; background-color:#f0f0f0;">Pipeline</th>
                    <th align="left" style="padding:6px 8px; border:1px solid #e1e1e1; background-color:#f0f0f0;">Status</th>
                    <th align="right" style="padding:6px 8px; border:1px solid #e1e1e1; background-color:#f0f0f0;">Windows</th>
                    <th align="right" style="padding:6px 8px; border:1px solid #e1e1e1; background-color:#f0f0f0;">Primary Inserted</th>
                    <th align="right" style="padding:6px 8px; border:1px solid #e1e1e1; background-color:#f0f0f0;">Primary Updated</th>
                    <th align="right" style="padding:6px 8px; border:1px solid #e1e1e1; background-color:#f0f0f0;">Secondary Inserted</th>
                    <th align="right" style="padding:6px 8px; border:1px solid #e1e1e1; background-color:#f0f0f0;">Secondary Updated</th>
                    <th align="left" style="padding:6px 8px; border:1px solid #e1e1e1; background-color:#f0f0f0;">Notes</th>
                  </tr>
                  {% for store in stores %}
                  <tr>
                    <td style="padding:6px 8px; border:1px solid #e1e1e1;">{{ store.store_code or 'UNKNOWN' }}</td>
                    <td style="padding:6px 8px; border:1px solid #e1e1e1;">{{ store.pipeline_name or 'unknown' }}</td>
                    <td style="padding:6px 8px; border:1px solid #e1e1e1;">{{ store.status or 'unknown' }}</td>
                    <td align="right" style="padding:6px 8px; border:1px solid #e1e1e1;">{{ store.window_count or 0 }}</td>
                    <td align="right" style="padding:6px 8px; border:1px solid #e1e1e1;">
                      {{ store.primary_metrics.final_inserted or store.primary_metrics.staging_inserted or 0 }}
                    </td>
                    <td align="right" style="padding:6px 8px; border:1px solid #e1e1e1;">
                      {{ store.primary_metrics.final_updated or store.primary_metrics.staging_updated or 0 }}
                    </td>
                    <td align="right" style="padding:6px 8px; border:1px solid #e1e1e1;">
                      {{ store.secondary_metrics.final_inserted or store.secondary_metrics.staging_inserted or 0 }}
                    </td>
                    <td align="right" style="padding:6px 8px; border:1px solid #e1e1e1;">
                      {{ store.secondary_metrics.final_updated or store.secondary_metrics.staging_updated or 0 }}
                    </td>
                    <td style="padding:6px 8px; border:1px solid #e1e1e1;">
                      {% if store.status_conflict_count %}
                        {{ store.status_conflict_count }} window(s) skipped but rows present
                      {% else %}
                        —
                      {% endif %}
                    </td>
                  </tr>
                  {% endfor %}
                </table>
              </td>
            </tr>
            {% if warnings %}
            <tr>
              <td style="padding:0 24px 16px 24px;">
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="width:100%; border-collapse:collapse; font-family:Arial, sans-serif;">
                  <tr>
                    <td style="padding:10px 12px; border:1px solid #f2c2c2; background-color:#fff2f2; color:#8a1f1f; font-size:13px;">
                      <div style="font-weight:bold; margin-bottom:6px;">Warnings</div>
                      <ul style="margin:0; padding-left:18px;">
                        {% for warning in warnings %}
                        <li style="margin:0 0 4px 0;">{{ warning }}</li>
                        {% endfor %}
                      </ul>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>
            {% endif %}
            <tr>
              <td style="padding:0 24px 24px 24px;">
                {% if fact_sections_text %}
                <div style="font-family:Arial, sans-serif; font-size:14px; font-weight:bold; margin-bottom:8px;">Row-level facts</div>
                <div style="font-family:Menlo, Consolas, 'Courier New', monospace; font-size:12px; white-space:pre-wrap; background-color:#f7f7f7; border:1px solid #e1e1e1; padding:10px;">{{- fact_sections_text -}}</div>
                {% elif summary_text %}
                <div style="font-family:Arial, sans-serif; font-size:14px; font-weight:bold; margin-bottom:8px;">Summary</div>
                <div style="font-family:Menlo, Consolas, 'Courier New', monospace; font-size:12px; white-space:pre-wrap; background-color:#f7f7f7; border:1px solid #e1e1e1; padding:10px;">{{- summary_text -}}</div>
                {% endif %}
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>
"""


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
    body_html: str | None
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


def _strip_uc_gstin_warning(raw: Any) -> tuple[str | None, bool]:
    if raw is None:
        return None, False
    text = str(raw).strip()
    if not text:
        return None, False
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        parsed = None
    if isinstance(parsed, Mapping):
        warnings = [str(entry) for entry in (parsed.get("warnings") or [])]
        failures = [str(entry) for entry in (parsed.get("failures") or [])]
        filtered_warnings = [warning for warning in warnings if warning != UC_GSTIN_MISSING_REMARK]
        removed = len(filtered_warnings) != len(warnings)
        if not filtered_warnings and not failures:
            return None, removed
        if removed:
            cleaned_payload = {"warnings": filtered_warnings, "failures": failures}
            return json.dumps(cleaned_payload, ensure_ascii=False), True
        return text, False
    if UC_GSTIN_MISSING_REMARK not in text:
        return text, False
    parts = [part.strip() for part in text.split(";") if part.strip()]
    filtered_parts = [part for part in parts if part != UC_GSTIN_MISSING_REMARK]
    cleaned = "; ".join(filtered_parts).strip()
    if not cleaned:
        return None, True
    return cleaned, True


def _clean_uc_rows_for_reporting(
    rows: Iterable[Mapping[str, Any]] | None,
    *,
    drop_empty: bool,
) -> list[dict[str, Any]]:
    cleaned_rows: list[dict[str, Any]] = []
    for row in rows or []:
        remark_value = _extract_row_value(row, "ingest_remarks", "ingestion_remarks", "remarks")
        cleaned_remark, removed = _strip_uc_gstin_warning(remark_value)
        if cleaned_remark is None and removed and drop_empty:
            continue
        data = dict(row)
        if cleaned_remark is not None:
            data["ingest_remarks"] = cleaned_remark
            data["ingestion_remarks"] = cleaned_remark
            data["remarks"] = cleaned_remark
        elif removed:
            data["ingest_remarks"] = ""
            data["ingestion_remarks"] = ""
            data["remarks"] = ""
        cleaned_rows.append(data)
    return cleaned_rows


def _coerce_mapping(raw: Any) -> Mapping[str, Any]:
    if isinstance(raw, Mapping):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, Mapping) else {}
    return {}


def _normalize_missing_windows(
    payload: Mapping[str, Any] | None,
) -> dict[str, list[dict[str, str]]]:
    if not payload:
        return {}
    normalized: dict[str, list[dict[str, str]]] = {}
    for store_code, entries in payload.items():
        if not store_code:
            continue
        normalized_code = _normalize_store_code(str(store_code)) or str(store_code)
        if not isinstance(entries, list):
            continue
        prepared: list[dict[str, str]] = []
        for entry in entries:
            if not isinstance(entry, Mapping):
                continue
            from_date = entry.get("from_date") or entry.get("from")
            to_date = entry.get("to_date") or entry.get("to")
            if not from_date and not to_date:
                continue
            prepared.append({"from_date": str(from_date), "to_date": str(to_date)})
        if prepared:
            normalized[normalized_code] = prepared
    return normalized


def _format_missing_window_lines(entries: Iterable[Mapping[str, Any]] | None) -> list[str]:
    lines: list[str] = []
    for entry in entries or []:
        if not isinstance(entry, Mapping):
            continue
        from_date = entry.get("from_date") or entry.get("from") or "unknown"
        to_date = entry.get("to_date") or entry.get("to") or "unknown"
        lines.append(f"{from_date}→{to_date}")
    return lines


def _merge_missing_windows(*entries: Iterable[Mapping[str, Any]] | None) -> list[dict[str, str]]:
    seen: set[tuple[str, str]] = set()
    merged: list[dict[str, str]] = []
    for chunk in entries:
        for entry in chunk or []:
            if not isinstance(entry, Mapping):
                continue
            from_date = entry.get("from_date") or entry.get("from")
            to_date = entry.get("to_date") or entry.get("to")
            if not from_date and not to_date:
                continue
            key = (str(from_date), str(to_date))
            if key in seen:
                continue
            seen.add(key)
            merged.append({"from_date": str(from_date), "to_date": str(to_date)})
    return merged


def _missing_windows_from_audit(window_audit: Iterable[Mapping[str, Any]] | None) -> dict[str, list[dict[str, str]]]:
    missing_by_store: dict[str, list[dict[str, str]]] = {}
    for entry in window_audit or []:
        if not isinstance(entry, Mapping):
            continue
        status = str(entry.get("status") or "").lower()
        if status != "skipped":
            continue
        raw_store_code = entry.get("store_code")
        store_code = _normalize_store_code(str(raw_store_code)) if raw_store_code is not None else None
        if not store_code and raw_store_code is not None:
            store_code = str(raw_store_code)
        if not store_code:
            continue
        from_date = entry.get("from_date") or entry.get("from")
        to_date = entry.get("to_date") or entry.get("to")
        if not from_date and not to_date:
            continue
        missing_by_store.setdefault(store_code, []).append(
            {"from_date": str(from_date), "to_date": str(to_date)}
        )
    return missing_by_store


async def _load_profiler_missing_windows(run_id: str) -> dict[str, list[dict[str, str]]]:
    if not config.database_url or not run_id:
        return {}
    async with session_scope(config.database_url) as session:
        row = (
            await session.execute(
                sa.select(pipeline_run_summaries.c.metrics_json).where(
                    pipeline_run_summaries.c.pipeline_name == "orders_sync_run_profiler",
                    pipeline_run_summaries.c.run_id == run_id,
                )
            )
        ).mappings().first()
    if not row:
        return {}
    metrics = _coerce_mapping(row.get("metrics_json"))
    missing_windows = metrics.get("missing_windows")
    if isinstance(missing_windows, str):
        missing_windows = _coerce_mapping(missing_windows)
    if not isinstance(missing_windows, Mapping):
        return {}
    return _normalize_missing_windows(missing_windows)


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


def _has_positive_unified_metrics(metrics: Mapping[str, Any]) -> bool:
    for field in UNIFIED_METRIC_FIELDS:
        value = _coerce_int(metrics.get(field))
        if value is not None and value > 0:
            return True
    return False


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


def _normalize_output_status(status: str | None) -> str:
    normalized = str(status or "").lower()
    mapping = {
        "ok": "success",
        "success": "success",
        "warning": "success_with_warnings",
        "warn": "success_with_warnings",
        "success_with_warnings": "success_with_warnings",
        "partial": "partial",
        "skipped": "partial",
        "error": "failed",
        "failed": "failed",
    }
    return mapping.get(normalized, normalized or "unknown")


def _status_explanation(status: str | None) -> str:
    return STATUS_EXPLANATIONS.get(str(status or "").lower(), "run completed with mixed results")


def _format_status_label(status: str | None) -> str:
    raw = str(status or "").strip()
    if not raw:
        return "unknown"
    return raw.replace("_", " ")


def _build_outcome_summary(overall_status: str | None, *, store_count: int) -> dict[str, str]:
    normalized = _normalize_uc_status(overall_status)
    label = "Outcome" if store_count == 1 else "Outcomes"
    if normalized == "success":
        summary = "Completed successfully."
    elif normalized == "success_with_warnings":
        summary = "Completed with warnings."
    elif normalized in {"partial", "failed"}:
        summary = "Failed."
    else:
        summary = _status_explanation(normalized)
    return {"outcome_label": label, "outcome_summary": summary}


def _normalize_uc_status(status: str | None) -> str:
    normalized = str(status or "").lower()
    mapping = {
        "ok": "success",
        "warning": "success_with_warnings",
        "warn": "success_with_warnings",
        "error": "failed",
        "failed": "failed",
        "success": "success",
        "partial": "partial",
        "success_with_warnings": "success_with_warnings",
        "skipped": "partial",
    }
    return mapping.get(normalized, normalized or "unknown")


def _normalize_warning_entries(entries: Iterable[Any] | None) -> list[str]:
    warnings: list[str] = []
    for entry in entries or []:
        text = ""
        if isinstance(entry, str):
            text = entry.strip()
        elif isinstance(entry, Mapping):
            code = entry.get("code") or entry.get("warning_code") or entry.get("type")
            message = entry.get("message") or entry.get("detail") or entry.get("text")
            if code and message:
                text = f"{code}: {message}"
            elif code:
                text = str(code)
            elif message:
                text = str(message)
            else:
                text = str(entry)
        else:
            text = str(entry)
        if text and text not in warnings:
            warnings.append(text)
    return warnings


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
                for key in ("order_number", "Order Number", "Order No.", "Booking ID"):
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


def _extract_row_value(row: Mapping[str, Any], key: str, *fallback_keys: str) -> Any:
    values = row.get("values") or {}
    for candidate in (key, *fallback_keys):
        if candidate in row and row.get(candidate) not in (None, ""):
            return row.get(candidate)
        if isinstance(values, Mapping) and values.get(candidate) not in (None, ""):
            return values.get(candidate)
    return None


def _format_identifier(row: Mapping[str, Any], *, include_extras: bool = False) -> str:
    store_code = _normalize_store_code(str(_extract_row_value(row, "store_code") or "").strip()) or ""
    order_number = str(_extract_row_value(row, "order_number", "Order Number", "Order No.") or "").strip()
    base = " ".join(part for part in (store_code, order_number) if part)
    if not include_extras:
        return base
    extras: list[str] = []
    order_date = _extract_row_value(row, "order_date", "Order Date", "invoice_date", "Invoice Date")
    if order_date:
        extras.append(f"order_date={order_date}")
    if extras:
        return f"{base} ({', '.join(str(value) for value in extras)})" if base else ", ".join(extras)
    return base


def _unique_rows(rows: Iterable[Mapping[str, Any]], formatter: Callable[[Mapping[str, Any]], str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for row in rows:
        formatted = formatter(row).strip()
        if not formatted or formatted in seen:
            continue
        seen.add(formatted)
        ordered.append(formatted)
    return ordered


def _format_row_block(
    rows: Iterable[Mapping[str, Any]],
    *,
    formatter: Callable[[Mapping[str, Any]], str],
    limit: int | None,
) -> tuple[list[str], bool]:
    entries = _unique_rows(rows, formatter)
    if limit is None:
        samples = entries
        truncated = False
    else:
        truncated = len(entries) > limit
        samples = entries[:limit]
    if not samples:
        return ["- (none)"], False
    lines = [f"- {entry}" for entry in samples]
    if truncated:
        lines.append(f"... additional {len(entries) - limit} row(s) truncated")
    return lines, truncated


def _normalize_fact_value(value: Any) -> str:
    if value in (None, ""):
        return ""
    return str(value).strip()


def _normalize_fact_row(
    row: Mapping[str, Any],
    *,
    include_remarks: bool,
    store_code_fallback: str | None = None,
) -> dict[str, str]:
    store_code = _normalize_store_code(_normalize_fact_value(_extract_row_value(row, "store_code") or "")) or ""
    if not store_code and store_code_fallback:
        store_code = _normalize_store_code(store_code_fallback) or store_code_fallback
    order_number = _normalize_fact_value(
        _extract_row_value(row, "order_number", "Order Number", "Order No.", "Booking ID")
    )
    if not order_number:
        order_number = MISSING_ORDER_NUMBER_PLACEHOLDER
    order_date = _normalize_fact_value(
        _extract_row_value(row, "order_date", "Order Date", "invoice_date", "Invoice Date")
    )
    customer_name = _normalize_fact_value(
        _extract_row_value(row, "customer_name", "Customer Name", "Customer")
    )
    mobile_number = _normalize_fact_value(
        _extract_row_value(row, "mobile_number", "Mobile Number", "Phone", "Phone Number")
    )
    normalized = {
        "store_code": store_code,
        "order_number": order_number,
        "order_date": order_date,
        "customer_name": customer_name,
        "mobile_number": mobile_number,
    }
    if include_remarks:
        remarks = _normalize_fact_value(
            _extract_row_value(row, "ingest_remarks", "ingestion_remarks", "remarks")
        )
        normalized["ingestion_remarks"] = remarks
    return normalized


def _build_fact_rows(
    rows: Iterable[Mapping[str, Any]] | None,
    *,
    include_remarks: bool,
    store_code_fallback: str | None = None,
) -> list[dict[str, str]]:
    normalized_rows = [
        _normalize_fact_row(row, include_remarks=include_remarks, store_code_fallback=store_code_fallback)
        for row in rows or []
    ]
    return sorted(
        normalized_rows,
        key=lambda row: (
            row.get("store_code") or "",
            row.get("order_number") or "",
        ),
    )


def _limit_fact_rows(
    rows: list[dict[str, str]], *, limit: int | None = FACT_SECTION_ROW_LIMIT
) -> tuple[list[dict[str, str]], int]:
    if limit is None or len(rows) <= limit:
        return rows, 0
    return rows[:limit], len(rows) - limit


def _format_fact_section(
    title: str,
    rows: list[dict[str, str]],
    *,
    include_remarks: bool,
) -> list[str]:
    if not rows:
        return []
    display_rows, truncated_count = _limit_fact_rows(rows)
    columns = FACT_ROW_COLUMNS_WITH_REMARKS if include_remarks else FACT_ROW_COLUMNS
    header = " | ".join(columns)
    lines = [f"{title} ({len(rows)}):", header]
    for row in display_rows:
        lines.append(" | ".join(row.get(column, "") for column in columns))
    if truncated_count:
        lines.append(f"...truncated {truncated_count} more")
    return lines


def _format_fact_section_by_store(
    title: str,
    rows: list[dict[str, str]],
    *,
    include_remarks: bool,
    per_store_limit: int,
) -> list[str]:
    if not rows:
        return []
    columns = FACT_ROW_COLUMNS_WITH_REMARKS if include_remarks else FACT_ROW_COLUMNS
    header = " | ".join(columns)
    lines = [f"{title} ({len(rows)}):", header]
    current_store: str | None = None
    current_rows: list[dict[str, str]] = []

    def _flush_store_rows() -> None:
        if not current_rows:
            return
        display_rows = current_rows[:per_store_limit]
        for row in display_rows:
            lines.append(" | ".join(row.get(column, "") for column in columns))
        truncated_count = len(current_rows) - len(display_rows)
        if truncated_count:
            lines.append(f"...truncated {truncated_count} more")

    for row in rows:
        store_code = row.get("store_code") or ""
        if current_store is None:
            current_store = store_code
        if store_code != current_store:
            _flush_store_rows()
            current_rows = []
            current_store = store_code
        current_rows.append(row)
    _flush_store_rows()
    return lines


def _format_fact_sections_text(
    *,
    warning_rows: list[dict[str, str]] | None = None,
    dropped_rows: list[dict[str, str]] | None = None,
    edited_rows: list[dict[str, str]] | None = None,
    error_rows: list[dict[str, str]] | None = None,
) -> str:
    sections: list[str] = []
    sections.extend(_format_fact_section("Warning rows", warning_rows or [], include_remarks=True))
    sections.extend(_format_fact_section("Dropped rows", dropped_rows or [], include_remarks=True))
    sections.extend(_format_fact_section("Edited rows", edited_rows or [], include_remarks=False))
    sections.extend(_format_fact_section("Error rows", error_rows or [], include_remarks=True))
    return "\n".join(sections)


def _format_fact_sections_text_by_store(
    *,
    warning_rows: list[dict[str, str]] | None = None,
    dropped_rows: list[dict[str, str]] | None = None,
    edited_rows: list[dict[str, str]] | None = None,
    error_rows: list[dict[str, str]] | None = None,
    per_store_limit: int = PROFILER_FACT_ROW_LIMIT_PER_STORE,
) -> str:
    sections: list[str] = []
    sections.extend(
        _format_fact_section_by_store(
            "Warning rows",
            warning_rows or [],
            include_remarks=True,
            per_store_limit=per_store_limit,
        )
    )
    sections.extend(
        _format_fact_section_by_store(
            "Dropped rows",
            dropped_rows or [],
            include_remarks=True,
            per_store_limit=per_store_limit,
        )
    )
    sections.extend(
        _format_fact_section_by_store(
            "Edited rows",
            edited_rows or [],
            include_remarks=False,
            per_store_limit=per_store_limit,
        )
    )
    sections.extend(
        _format_fact_section_by_store(
            "Error rows",
            error_rows or [],
            include_remarks=True,
            per_store_limit=per_store_limit,
        )
    )
    return "\n".join(sections)


def _build_profiler_row_fact_warnings(
    *,
    warning_rows: list[dict[str, str]],
    dropped_rows: list[dict[str, str]],
    edited_rows: list[dict[str, str]],
    error_rows: list[dict[str, str]],
) -> list[str]:
    warnings: list[str] = []
    if warning_rows:
        warnings.append(f"ROW_WARNINGS: {len(warning_rows)} row(s) with warnings")
    if dropped_rows:
        warnings.append(f"ROW_DROPPED: {len(dropped_rows)} row(s) dropped")
    if edited_rows:
        warnings.append(f"ROW_EDITED: {len(edited_rows)} row(s) edited")
    if error_rows:
        warnings.append(f"ROW_ERRORS: {len(error_rows)} row(s) errored")
    return warnings


def _replace_profiler_warnings_section(summary_text: str, warnings: Sequence[str]) -> str:
    if not summary_text or not warnings:
        return summary_text
    lines = summary_text.splitlines()
    try:
        warnings_index = lines.index("Warnings:")
    except ValueError:
        return summary_text
    updated = lines[: warnings_index + 1]
    updated.extend(f"- {warning}" for warning in warnings)
    return "\n".join(updated)


def _append_fact_sections(summary_text: str, fact_text: str) -> str:
    if not fact_text:
        return summary_text
    header = "Row-level facts:"
    if summary_text:
        return f"{summary_text.rstrip()}\n\n{header}\n{fact_text}"
    return f"{header}\n{fact_text}"


def _uc_warning_entries(
    *,
    stores_payload: Iterable[Mapping[str, Any]],
    payload_warnings: Iterable[Any] | None,
    warning_counts_by_store: Mapping[str, int] | None = None,
) -> list[str]:
    warnings = [
        warning
        for warning in _normalize_warning_entries(payload_warnings)
        if warning != UC_GSTIN_MISSING_REMARK
    ]
    if warnings:
        return warnings
    for store in stores_payload:
        store_code = store.get("store_code") or "UNKNOWN"
        normalized_store = _normalize_store_code(store_code) or store_code
        warning_count = None
        if warning_counts_by_store is not None:
            warning_count = warning_counts_by_store.get(normalized_store)
        if warning_count is None:
            warning_count = _coerce_int(store.get("warning_count"))
        if warning_count is None or warning_count <= 0:
            continue
        warnings.append(
            f"UC_STORE_WARNINGS: {store_code} reported {warning_count} row-level warning(s)"
        )
    return warnings


def _uc_window_status_by_store(window_audit: Iterable[Mapping[str, Any]]) -> dict[str, str]:
    status_by_store: dict[str, str] = {}
    for entry in window_audit:
        store_code = _normalize_store_code(entry.get("store_code")) or entry.get("store_code")
        if not store_code:
            continue
        status_by_store[store_code] = _normalize_uc_status(entry.get("status"))
    return status_by_store


def _uc_store_status(
    store: Mapping[str, Any], status_by_store: Mapping[str, str]
) -> str:
    store_code = _normalize_store_code(store.get("store_code")) or store.get("store_code")
    if store_code and store_code in status_by_store:
        return status_by_store[store_code]
    return _normalize_uc_status(store.get("status"))


def _uc_status_counts(
    stores_payload: list[Mapping[str, Any]], status_by_store: Mapping[str, str]
) -> dict[str, int]:
    counts = {"success": 0, "success_with_warnings": 0, "partial": 0, "failed": 0, "skipped": 0}
    for store in stores_payload:
        status = _uc_store_status(store, status_by_store)
        if status in counts:
            counts[status] += 1
    return counts


def _uc_overall_status(
    stores_payload: list[Mapping[str, Any]],
    status_by_store: Mapping[str, str],
    *,
    fallback_status: str | None = None,
) -> str:
    statuses = [_uc_store_status(store, status_by_store) for store in stores_payload]
    if not statuses and status_by_store:
        statuses = list(status_by_store.values())
    if any(status == "failed" for status in statuses):
        return "failed"
    if any(status == "partial" for status in statuses):
        return "partial"
    if any(status == "success_with_warnings" for status in statuses):
        return "success_with_warnings"
    if any(status == "success" for status in statuses):
        return "success"
    if any(status == "skipped" for status in statuses):
        return "skipped"
    return _normalize_uc_status(fallback_status)


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


def _prepare_ingest_remarks(rows: list[dict[str, Any]]) -> tuple[list[dict[str, str]], bool, bool, str]:
    if not rows:
        return [], False, False, ""

    cleaned_rows: list[dict[str, str]] = []
    for entry in rows:
        cleaned_rows.append(
            {
                "store_code": (_normalize_store_code(entry.get("store_code")) or ""),
                "order_number": str(entry.get("order_number") or ""),
                "ingest_remarks": str(entry.get("ingest_remarks") or ""),
            }
        )

    lines = [f"- {row['store_code']} {row['order_number']}: {row['ingest_remarks']}" for row in cleaned_rows]
    ingest_text = "\n".join(lines)
    return cleaned_rows, False, False, ingest_text


def _build_run_plan(
    pipeline_code: str,
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
    body_html = None
    if pipeline_code == "orders_sync_run_profiler":
        html_context = dict(context)
        html_context["report_date"] = _format_profiler_html_timestamp(context.get("report_date"))
        html_context["started_at"] = _format_profiler_html_timestamp(context.get("started_at"))
        html_context["finished_at"] = _format_profiler_html_timestamp(context.get("finished_at"))
        body_html = _render_template(PROFILER_HTML_TEMPLATE, html_context)
    return EmailPlan(
        profile_code=profile["code"],
        scope=profile["scope"],
        store_code=None,
        subject=subject,
        body=body,
        body_html=body_html,
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
        missing_windows_by_store = store_context.get("missing_windows_by_store") or {}
        resolved_missing = _normalize_missing_windows(missing_windows_by_store).get(store_code) or []
        store_context["missing_windows"] = resolved_missing
        store_context["missing_window_lines"] = _format_missing_window_lines(resolved_missing)
        store_context["missing_window_count"] = len(store_context["missing_window_lines"])
        store_payloads = store_context.get("stores") or []
        store_payload = next(
            (
                payload
                for payload in store_payloads
                if _normalize_store_code(payload.get("store_code")) == _normalize_store_code(store_code)
            ),
            None,
        )
        fact_sections_text = ""
        if store_payload:
            fact_sections_text = str(store_payload.get("fact_sections_text") or "")
            store_context["warning_fact_rows"] = store_payload.get("warning_fact_rows") or []
            store_context["dropped_fact_rows"] = store_payload.get("dropped_fact_rows") or []
            store_context["edited_fact_rows"] = store_payload.get("edited_fact_rows") or []
            store_context["error_fact_rows"] = store_payload.get("error_fact_rows") or []
        store_context["fact_sections_text"] = fact_sections_text
        store_context["summary_text"] = _append_fact_sections(
            str(store_context.get("summary_text") or ""), fact_sections_text
        )
        subject = _render_template(template["subject_template"], store_context)
        body = _render_template(template["body_template"], store_context)
        plans.append(
            EmailPlan(
                profile_code=profile["code"],
                scope=profile["scope"],
                store_code=store_code,
                subject=subject,
                body=body,
                body_html=None,
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
    if plan.body_html:
        message.add_alternative(plan.body_html, subtype="html")
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
            plan = _build_run_plan(pipeline_code, profile, template, profile_recipients, docs, context)
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

    profiler_missing_windows = await _load_profiler_missing_windows(run_id)
    resources = {
        "pipeline": dict(pipeline_row),
        "run": dict(run_row),
        "docs": _group_documents([dict(row) for row in docs_rows]),
        "profiles": [dict(row) for row in profiles_rows],
        "templates": templates_by_profile,
        "recipients": recipients_by_profile,
        "store_names": store_names,
        "profiler_missing_windows": profiler_missing_windows,
    }
    return resources, []


def _normalize_datetime(value: Any) -> str:
    try:
        return value.isoformat()  # type: ignore[attr-defined]
    except Exception:
        return str(value) if value is not None else ""


def _build_td_orders_context(
    run_data: dict[str, Any],
    *,
    missing_windows_by_store: dict[str, list[dict[str, str]]] | None = None,
) -> dict[str, Any]:
    metrics = run_data.get("metrics_json") or {}
    payload = metrics.get("notification_payload") or {}
    stores_payload = payload.get("stores") or []
    ingest_rows = (metrics.get("ingest_remarks") or {}).get("rows") or []

    def _rows_ingested(report: Mapping[str, Any]) -> int:
        for candidate in (report.get("rows_ingested"), report.get("final_rows"), report.get("staging_rows")):
            if candidate is not None:
                return _coerce_int(candidate) or 0
        return 0

    stores: list[dict[str, Any]] = []
    resolved_missing_windows = _normalize_missing_windows(missing_windows_by_store)
    primary_totals: dict[str, int] = {field: 0 for field in UNIFIED_METRIC_FIELDS}
    secondary_totals: dict[str, int] = {field: 0 for field in UNIFIED_METRIC_FIELDS}
    for store in stores_payload:
        store_code = _normalize_store_code(store.get("store_code")) or store.get("store_code")
        missing_windows = resolved_missing_windows.get(store_code or "") or []
        missing_window_lines = _format_missing_window_lines(missing_windows)
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
        orders_warning_fact_rows = _build_fact_rows(
            orders_warning_rows, include_remarks=True, store_code_fallback=store_code
        )
        orders_dropped_fact_rows = _build_fact_rows(
            orders_dropped_rows, include_remarks=True, store_code_fallback=store_code
        )
        sales_warning_fact_rows = _build_fact_rows(
            sales_warning_rows, include_remarks=True, store_code_fallback=store_code
        )
        sales_dropped_fact_rows = _build_fact_rows(
            sales_dropped_rows, include_remarks=True, store_code_fallback=store_code
        )
        sales_edited_fact_rows = _build_fact_rows(
            sales_edited_rows, include_remarks=False, store_code_fallback=store_code
        )
        orders_warning_display_rows, _ = _limit_fact_rows(orders_warning_fact_rows)
        orders_dropped_display_rows, _ = _limit_fact_rows(orders_dropped_fact_rows)
        sales_warning_display_rows, _ = _limit_fact_rows(sales_warning_fact_rows)
        sales_dropped_display_rows, _ = _limit_fact_rows(sales_dropped_fact_rows)
        sales_edited_display_rows, _ = _limit_fact_rows(sales_edited_fact_rows)
        orders = {
            **orders,
            "warning_rows": orders_warning_display_rows,
            "dropped_rows": orders_dropped_display_rows,
        }
        sales = {
            **sales,
            "warning_rows": sales_warning_display_rows,
            "dropped_rows": sales_dropped_display_rows,
            "edited_rows": sales_edited_display_rows,
            "duplicate_rows": [],
        }
        primary_metrics = _build_unified_metrics(orders)
        secondary_metrics = _build_unified_metrics(sales)
        _sum_unified_metrics(primary_totals, primary_metrics)
        _sum_unified_metrics(secondary_totals, secondary_metrics)
        orders_status_raw = str(orders.get("status") or "").lower()
        sales_status_raw = str(sales.get("status") or "").lower()
        orders_status = _normalize_output_status(orders_status_raw)
        sales_status = _normalize_output_status(sales_status_raw)
        orders_status_conflict = orders_status_raw == "skipped" and _has_positive_unified_metrics(primary_metrics)
        sales_status_conflict = sales_status_raw == "skipped" and _has_positive_unified_metrics(secondary_metrics)
        if orders_status_conflict:
            logger.warning(
                "orders report status skipped but rows present",
                extra={"store_code": store.get("store_code"), "report": "orders"},
            )
        if sales_status_conflict:
            logger.warning(
                "sales report status skipped but rows present",
                extra={"store_code": store.get("store_code"), "report": "sales"},
            )
        warning_fact_rows = _build_fact_rows(
            orders_warning_rows + sales_warning_rows,
            include_remarks=True,
            store_code_fallback=store_code,
        )
        dropped_fact_rows = _build_fact_rows(
            orders_dropped_rows + sales_dropped_rows,
            include_remarks=True,
            store_code_fallback=store_code,
        )
        edited_fact_rows = _build_fact_rows(
            sales_edited_rows,
            include_remarks=False,
            store_code_fallback=store_code,
        )
        error_fact_rows = _build_fact_rows(
            (orders.get("error_rows") or []) + (sales.get("error_rows") or []),
            include_remarks=True,
            store_code_fallback=store_code,
        )
        fact_sections_text = _format_fact_sections_text(
            warning_rows=warning_fact_rows,
            dropped_rows=dropped_fact_rows,
            edited_rows=edited_fact_rows,
            error_rows=error_fact_rows,
        )
        stores.append(
            {
                "store_code": store_code,
                "status": _normalize_output_status(store.get("status")),
                "message": store.get("message"),
                "orders_status": orders_status,
                "orders_status_conflict": orders_status_conflict,
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
                "sales_status": sales_status,
                "sales_status_conflict": sales_status_conflict,
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
                "warning_fact_rows": warning_fact_rows,
                "dropped_fact_rows": dropped_fact_rows,
                "edited_fact_rows": edited_fact_rows,
                "error_fact_rows": error_fact_rows,
                "fact_sections_text": fact_sections_text,
                "missing_windows": missing_windows,
                "missing_window_lines": missing_window_lines,
                "missing_window_count": len(missing_window_lines),
                "primary_metrics": primary_metrics,
                "secondary_metrics": secondary_metrics,
                **_prefix_unified_metrics("primary_", primary_metrics),
                **_prefix_unified_metrics("secondary_", secondary_metrics),
            }
        )

    sales_warning_rows = [row for store in stores for row in store.get("sales_warning_rows") or []]
    sales_dropped_rows = [row for store in stores for row in store.get("sales_dropped_rows") or []]
    sales_edited_rows = [row for store in stores for row in store.get("sales_edited_rows") or []]
    sales_warning_lines, sales_warning_truncated = _format_row_block(
        sales_warning_rows,
        formatter=lambda row: " — ".join(
            part
            for part in (
                _format_identifier(row, include_extras=True),
                str(_extract_row_value(row, "ingest_remarks", "ingestion_remarks", "remarks") or "").strip()
                or "",
            )
            if part
        ),
        limit=TD_SALES_ROW_SAMPLE_LIMIT,
    )
    sales_edited_lines, sales_edited_truncated = _format_row_block(
        sales_edited_rows,
        formatter=lambda row: _format_identifier(row),
        limit=TD_SALES_EDITED_LIMIT,
    )
    dropped_identifiers = _unique_rows(sales_dropped_rows, lambda row: _format_identifier(row))
    dropped_samples = dropped_identifiers
    dropped_truncated = False
    if not dropped_samples:
        dropped_samples = ["(none)"]
    dropped_lines = [
        f"Total dropped rows: {len(sales_dropped_rows)}",
        f"Identifiers: {', '.join(dropped_samples)}",
    ]
    warning_fact_rows = [row for store in stores for row in store.get("warning_fact_rows") or []]
    dropped_fact_rows = [row for store in stores for row in store.get("dropped_fact_rows") or []]
    edited_fact_rows = [row for store in stores for row in store.get("edited_fact_rows") or []]
    error_fact_rows = [row for store in stores for row in store.get("error_fact_rows") or []]
    fact_sections_text = _format_fact_sections_text_by_store(
        warning_rows=warning_fact_rows,
        dropped_rows=dropped_fact_rows,
        edited_rows=edited_fact_rows,
        error_rows=error_fact_rows,
    )
    filtered_ingest_rows = [
        row
        for row in ingest_rows
        if (_normalize_store_code(row.get("store_code")) and str(row.get("order_number") or "").strip())
    ]
    _, _, _, td_ingest_text = _prepare_ingest_remarks(filtered_ingest_rows)

    summary_text = run_data.get("summary_text") or _td_summary_text_from_payload(run_data) or ""
    summary_text = _append_fact_sections(summary_text, fact_sections_text)
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
        "overall_status": _normalize_output_status(payload.get("overall_status") or run_data.get("overall_status")),
        "td_overall_status": _normalize_output_status(payload.get("overall_status") or run_data.get("overall_status")),
        "orders_status": _normalize_output_status(
            payload.get("orders_status") or (metrics.get("orders") or {}).get("overall_status")
        ),
        "sales_status": _normalize_output_status(
            payload.get("sales_status") or (metrics.get("sales") or {}).get("overall_status")
        ),
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
        "missing_windows_by_store": resolved_missing_windows,
        "fact_sections_text": fact_sections_text,
        "td_sales_warning_rows_text": "\n".join(sales_warning_lines),
        "td_sales_warning_rows_truncated": sales_warning_truncated,
        "td_sales_edited_rows_text": "\n".join(sales_edited_lines),
        "td_sales_edited_rows_truncated": sales_edited_truncated,
        "td_sales_dropped_rows_text": "\n".join(dropped_lines),
        "td_sales_dropped_rows_count": len(sales_dropped_rows),
        "td_sales_dropped_rows_samples": dropped_samples,
        "td_sales_ingest_remarks_text": td_ingest_text,
    }


def _build_uc_orders_context(
    run_data: dict[str, Any],
    *,
    missing_windows_by_store: dict[str, list[dict[str, str]]] | None = None,
) -> dict[str, Any]:
    metrics = run_data.get("metrics_json") or {}
    payload = metrics.get("notification_payload") or {}
    stores_payload = payload.get("stores") or []
    raw_ingest_rows = (metrics.get("ingest_remarks") or {}).get("rows") or []
    ingest_rows = _clean_uc_rows_for_reporting(raw_ingest_rows, drop_empty=True)
    warning_summaries = _summarize_ingest_remarks_by_store(ingest_rows)
    store_outcomes = (metrics.get("stores") or {}).get("outcomes") or {}
    ingest_warning_counts = {
        _normalize_store_code(store_code) or store_code: _coerce_int(outcome.get("warning_count"))
        for store_code, outcome in store_outcomes.items()
        if store_code
    }
    window_audit = metrics.get("window_audit") or []
    for entry in window_audit:
        status = str(entry.get("status") or "").lower()
        final_rows = _coerce_int(entry.get("final_rows"))
        if status == "skipped" and final_rows is not None and final_rows > 0:
            logger.warning(
                "UC window status skipped but rows present",
                extra={
                    "store_code": entry.get("store_code"),
                    "from_date": entry.get("from_date"),
                    "to_date": entry.get("to_date"),
                    "final_rows": final_rows,
                },
            )
    uc_status_by_store = _uc_window_status_by_store(window_audit)
    uc_status_counts = _uc_status_counts(stores_payload, uc_status_by_store)

    stores: list[dict[str, Any]] = []
    resolved_missing_windows = _normalize_missing_windows(missing_windows_by_store)
    audit_missing_windows = _missing_windows_from_audit(window_audit)
    merged_missing_windows: dict[str, list[dict[str, str]]] = {}
    for store_code in set(resolved_missing_windows) | set(audit_missing_windows):
        merged_missing_windows[store_code] = _merge_missing_windows(
            audit_missing_windows.get(store_code),
            resolved_missing_windows.get(store_code),
        )
    primary_totals: dict[str, int] = {field: 0 for field in UNIFIED_METRIC_FIELDS}
    secondary_metrics = _not_applicable_metrics()
    store_warning_counts: dict[str, int] = {}
    for store in stores_payload:
        store_code = _normalize_store_code(store.get("store_code")) or store.get("store_code")
        missing_windows = merged_missing_windows.get(store_code or "") or []
        missing_window_lines = _format_missing_window_lines(missing_windows)
        status = str(store.get("status") or "").lower()
        warning_data = warning_summaries.get(_normalize_store_code(store.get("store_code")) or "", {})
        warning_summary = warning_data.get("summary")
        store_warning_count = _coerce_int(store.get("warning_count"))
        ingest_warning_count = ingest_warning_counts.get(_normalize_store_code(store_code) or "")
        if ingest_warning_count is None:
            ingest_warning_count = store_warning_count
        if (
            ingest_warning_count is not None
            and store_warning_count is not None
            and ingest_warning_count != store_warning_count
        ):
            logger.warning(
                "UC email warning count mismatch with ingest warning count",
                extra={
                    "store_code": store_code,
                    "email_warning_count": store_warning_count,
                    "ingest_warning_count": ingest_warning_count,
                },
            )
        primary_metrics = _build_unified_metrics(store)
        _sum_unified_metrics(primary_totals, primary_metrics)
        store_status = _uc_store_status(store, uc_status_by_store)
        show_error_message = store_status in {"failed", "partial"}
        warning_rows_payload = _clean_uc_rows_for_reporting(store.get("warning_rows"), drop_empty=True)
        if not warning_rows_payload:
            warning_rows_payload = [
                row for row in ingest_rows if _normalize_store_code(row.get("store_code")) == store_code
            ]
        warning_rows = _with_store_metadata(
            warning_rows_payload,
            store_code=store.get("store_code"),
            include_order_number=True,
            include_remarks=True,
        )
        resolved_warning_count = len(warning_rows)
        store_warning_counts[_normalize_store_code(store_code) or store_code or ""] = resolved_warning_count
        dropped_rows_payload = _clean_uc_rows_for_reporting(store.get("dropped_rows"), drop_empty=False)
        dropped_rows = _with_store_metadata(
            dropped_rows_payload,
            store_code=store.get("store_code"),
            include_order_number=True,
            include_remarks=True,
        )
        warning_fact_rows = _build_fact_rows(
            warning_rows, include_remarks=True, store_code_fallback=store_code
        )
        dropped_fact_rows = _build_fact_rows(
            dropped_rows, include_remarks=True, store_code_fallback=store_code
        )
        warning_display_rows, _ = _limit_fact_rows(warning_fact_rows)
        dropped_display_rows, _ = _limit_fact_rows(dropped_fact_rows)
        fact_sections_text = _format_fact_sections_text(
            warning_rows=warning_fact_rows, dropped_rows=dropped_fact_rows
        )
        stores.append(
            {
                "store_code": store_code,
                "status": store_status,
                "message": store.get("message"),
                "error_message": store.get("error_message") if show_error_message else None,
                "info_message": store.get("info_message")
                or (store.get("message") if not show_error_message else None),
                "warning_count": resolved_warning_count,
                "warnings_summary": warning_summary,
                "warning_rows": warning_display_rows,
                "dropped_rows": dropped_display_rows,
                "warning_fact_rows": warning_fact_rows,
                "dropped_fact_rows": dropped_fact_rows,
                "fact_sections_text": fact_sections_text,
                "filename": store.get("filename"),
                "staging_rows": store.get("staging_rows"),
                "final_rows": store.get("final_rows"),
                "staging_inserted": store.get("staging_inserted"),
                "staging_updated": store.get("staging_updated"),
                "final_inserted": store.get("final_inserted"),
                "final_updated": store.get("final_updated"),
                "rows_downloaded": store.get("rows_downloaded"),
                "rows_skipped_invalid": store.get("rows_skipped_invalid"),
                "rows_skipped_invalid_reasons": store.get("rows_skipped_invalid_reasons"),
                "missing_windows": missing_windows,
                "missing_window_lines": missing_window_lines,
                "missing_window_count": len(missing_window_lines),
                "primary_metrics": primary_metrics,
                "secondary_metrics": secondary_metrics,
                **_prefix_unified_metrics("primary_", primary_metrics),
                **_prefix_unified_metrics("secondary_", secondary_metrics),
            }
        )

    started_at = payload.get("started_at") or run_data.get("started_at")
    finished_at = payload.get("finished_at") or run_data.get("finished_at")
    window_summary = metrics.get("window_summary") or {}
    window_audit = metrics.get("window_audit") or []
    summary_text = (
        _uc_summary_text_from_payload(run_data, missing_windows_by_store=merged_missing_windows)
        or run_data.get("summary_text")
        or ""
    )
    overall_status = _uc_overall_status(
        stores_payload,
        uc_status_by_store,
        fallback_status=payload.get("overall_status") or run_data.get("overall_status"),
    )
    warnings = _uc_warning_entries(
        stores_payload=stores_payload,
        payload_warnings=payload.get("warnings") or metrics.get("warnings") or [],
        warning_counts_by_store=store_warning_counts,
    )
    store_count = len(stores_payload)
    outcome_summary = _build_outcome_summary(overall_status, store_count=store_count)
    status_counts = _uc_status_counts(stores_payload, uc_status_by_store)

    return {
        "summary_text": summary_text,
        "started_at": _normalize_datetime(started_at),
        "finished_at": _normalize_datetime(finished_at),
        "total_time_taken": payload.get("total_time_taken") or run_data.get("total_time_taken"),
        "overall_status": overall_status,
        "overall_status_explanation": _status_explanation(overall_status),
        "overall_status_label": _format_status_label(overall_status),
        "store_count": store_count,
        **outcome_summary,
        "store_status_counts": status_counts,
        "stores_succeeded": status_counts.get("success", 0),
        "stores_warned": status_counts.get("success_with_warnings", 0),
        "stores_failed": status_counts.get("failed", 0),
        "stores_partial": status_counts.get("partial", 0),
        "uc_store_status_counts": uc_status_counts,
        "stores": stores,
        "uc_all_stores_failed": _uc_all_stores_failed(stores_payload),
        "warnings": warnings,
        "warnings_text": "\n".join(warnings) if warnings else "",
        "warnings_count": len(warnings),
        "has_warnings": bool(warnings),
        "notification_payload": payload,
        "primary_totals": primary_totals,
        "secondary_totals": secondary_metrics,
        "secondary_metrics_label": secondary_metrics.get("label"),
        "window_summary": window_summary,
        "window_audit": window_audit,
        "expected_windows": window_summary.get("expected_windows"),
        "completed_windows": window_summary.get("completed_windows"),
        "missing_windows": window_summary.get("missing_windows"),
        "missing_window_stores": window_summary.get("missing_store_codes") or [],
        "missing_windows_by_store": merged_missing_windows,
    }


def _build_profiler_context(run_data: dict[str, Any]) -> dict[str, Any]:
    metrics = run_data.get("metrics_json") or {}
    payload = metrics.get("notification_payload") or {}
    stores_payload = payload.get("stores") or []
    window_summary = payload.get("window_summary") or metrics.get("window_summary") or {}
    stores: list[dict[str, Any]] = []
    primary_totals: dict[str, int] = {field: 0 for field in UNIFIED_METRIC_FIELDS}
    secondary_totals: dict[str, int] = {field: 0 for field in UNIFIED_METRIC_FIELDS}
    for store in stores_payload:
        store_code = _normalize_store_code(store.get("store_code")) or store.get("store_code")
        primary_metrics = _build_unified_metrics(store.get("primary_metrics") or {})
        secondary_metrics = _build_unified_metrics(store.get("secondary_metrics") or {})
        _sum_unified_metrics(primary_totals, primary_metrics)
        _sum_unified_metrics(secondary_totals, secondary_metrics)
        stores.append(
            {
                "store_code": store_code,
                "pipeline_group": store.get("pipeline_group"),
                "pipeline_name": store.get("pipeline_name"),
                "status": _normalize_uc_status(store.get("status")),
                "window_count": store.get("window_count"),
                "status_conflict_count": store.get("status_conflict_count"),
                "primary_metrics": primary_metrics,
                "secondary_metrics": secondary_metrics,
            }
        )

    row_facts = metrics.get("row_facts") or {}
    warning_rows = _clean_uc_rows_for_reporting(row_facts.get("warning_rows"), drop_empty=True)
    dropped_rows = _clean_uc_rows_for_reporting(row_facts.get("dropped_rows"), drop_empty=False)
    edited_rows = row_facts.get("edited_rows") or []
    error_rows = _clean_uc_rows_for_reporting(row_facts.get("error_rows"), drop_empty=False)
    warning_fact_rows = _build_fact_rows(warning_rows, include_remarks=True)
    dropped_fact_rows = _build_fact_rows(dropped_rows, include_remarks=True)
    edited_fact_rows = _build_fact_rows(edited_rows, include_remarks=False)
    error_fact_rows = _build_fact_rows(error_rows, include_remarks=True)
    warnings = _normalize_warning_entries(payload.get("warnings") or [])
    if not warnings:
        warnings = _build_profiler_row_fact_warnings(
            warning_rows=warning_fact_rows,
            dropped_rows=dropped_fact_rows,
            edited_rows=edited_fact_rows,
            error_rows=error_fact_rows,
        )
    fact_sections_text = _format_fact_sections_text_by_store(
        warning_rows=warning_fact_rows,
        dropped_rows=dropped_fact_rows,
        edited_rows=edited_fact_rows,
        error_rows=error_fact_rows,
    )
    summary_text = run_data.get("summary_text") or ""
    summary_text = _replace_profiler_warnings_section(summary_text, warnings)
    summary_text = _append_fact_sections(summary_text, fact_sections_text)
    overall_status = payload.get("overall_status") or run_data.get("overall_status")
    store_count = len(stores_payload)
    outcome_summary = _build_outcome_summary(overall_status, store_count=store_count)
    return {
        "summary_text": summary_text,
        "profiler_summary_text": summary_text,
        "started_at": _normalize_datetime(payload.get("started_at") or run_data.get("started_at")),
        "finished_at": _normalize_datetime(payload.get("finished_at") or run_data.get("finished_at")),
        "total_time_taken": payload.get("total_time_taken") or run_data.get("total_time_taken"),
        "overall_status": overall_status,
        "overall_status_label": _format_status_label(overall_status),
        "store_count": store_count,
        **outcome_summary,
        "stores": stores,
        "primary_totals": primary_totals,
        "secondary_totals": secondary_totals,
        "window_summary": window_summary,
        "expected_windows": window_summary.get("expected_windows"),
        "completed_windows": window_summary.get("completed_windows"),
        "missing_windows": window_summary.get("missing_windows"),
        "missing_window_stores": window_summary.get("missing_store_codes") or [],
        "warnings": warnings,
        "warnings_text": "\n".join(str(entry) for entry in warnings) if warnings else "",
        "warnings_count": len(warnings),
        "has_warnings": bool(warnings),
        "notification_payload": payload,
        "fact_sections_text": fact_sections_text,
        "warning_fact_rows": warning_fact_rows,
        "dropped_fact_rows": dropped_fact_rows,
        "edited_fact_rows": edited_fact_rows,
        "error_fact_rows": error_fact_rows,
    }


def _td_all_stores_failed(stores_payload: list[Mapping[str, Any]]) -> bool:
    if not stores_payload:
        return True

    def _failed(report: Mapping[str, Any]) -> bool:
        status = _normalize_output_status(report.get("status"))
        if status in {"success", "success_with_warnings", "partial"}:
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
        status = _normalize_output_status(report.get("status"))
        if status in {"success", "success_with_warnings", "partial"}:
            return False
        return True

    for store in stores_payload:
        if not _failed(store):
            return False
    return True


def _format_profiler_html_timestamp(value: Any) -> str:
    if not value:
        return ""
    tz = get_timezone()
    try:
        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, str):
            try:
                dt = datetime.fromisoformat(value)
            except ValueError:
                try:
                    dt = datetime.strptime(value, "%Y-%m-%d")
                except ValueError:
                    return _normalize_datetime(value)
        else:
            return _normalize_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(tz).strftime("%d-%b-%Y %H:%M:%S")
    except Exception:
        return _normalize_datetime(value)


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

    def _inserted_updated(report: Mapping[str, Any]) -> tuple[int, int]:
        inserted = _coalesce_int(report, "final_inserted", "staging_inserted") or 0
        updated = _coalesce_int(report, "final_updated", "staging_updated") or 0
        return inserted, updated

    def _format_store_section(*, sales: bool = False) -> list[str]:
        lines: list[str] = []
        for store in stores_payload:
            report = (store.get("sales") if sales else store.get("orders")) or {}
            status = _normalize_output_status(report.get("status"))
            status_label = _format_status_label(status).upper()
            rows_downloaded = report.get("rows_downloaded") or 0
            rows_ingested = _rows_ingested(report)
            inserted, updated = _inserted_updated(report)
            warning_count = _count(report, "warning_count", "warning_rows", "warnings")
            dropped_count = _count(report, "dropped_rows_count", "dropped_rows")
            base = [
                f"- {store.get('store_code') or 'UNKNOWN'} — {status_label}",
                f"  rows_downloaded: {rows_downloaded}",
                f"  rows_ingested: {rows_ingested}",
                f"  inserted: {inserted}",
                f"  updated: {updated}",
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
    status_line = (
        "Overall Status: "
        f"{_format_status_label(_normalize_output_status(payload.get('overall_status') or run_data.get('overall_status')))}"
        " (Orders: "
        f"{_format_status_label(_normalize_output_status(orders_status))}, Sales: "
        f"{_format_status_label(_normalize_output_status(sales_status))})"
    )
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


def _uc_summary_text_from_payload(
    run_data: Mapping[str, Any],
    *,
    missing_windows_by_store: dict[str, list[dict[str, str]]] | None = None,
) -> str:
    metrics = run_data.get("metrics_json") or {}
    payload = metrics.get("notification_payload") or {}
    stores_payload = payload.get("stores") or []
    if not payload:
        return ""
    window_audit = metrics.get("window_audit") or []
    uc_status_by_store = _uc_window_status_by_store(window_audit)
    uc_status_counts = _uc_status_counts(stores_payload, uc_status_by_store)
    total = sum(uc_status_counts.values())
    overall_status = _uc_overall_status(
        stores_payload,
        uc_status_by_store,
        fallback_status=payload.get("overall_status") or run_data.get("overall_status"),
    )
    skipped = uc_status_counts.get("skipped", 0)
    skipped_suffix = f", {skipped} skipped" if skipped else ""
    status_explanation = _status_explanation(overall_status)
    window_summary = metrics.get("window_summary") or {}
    missing_windows = window_summary.get("missing_windows")
    missing_stores = window_summary.get("missing_store_codes") or []
    missing_line = f"Missing Windows: {missing_windows if missing_windows is not None else 0}"
    if missing_stores:
        missing_line += f" ({', '.join(missing_stores)})"
    resolved_missing_windows = _normalize_missing_windows(missing_windows_by_store)
    audit_missing_windows = _missing_windows_from_audit(window_audit)
    merged_missing_windows: dict[str, list[dict[str, str]]] = {}
    for store_code in set(resolved_missing_windows) | set(audit_missing_windows):
        merged_missing_windows[store_code] = _merge_missing_windows(
            audit_missing_windows.get(store_code),
            resolved_missing_windows.get(store_code),
        )
    missing_window_lines: list[str] = []
    for store_code in sorted(merged_missing_windows):
        window_lines = _format_missing_window_lines(merged_missing_windows.get(store_code))
        if not window_lines:
            continue
        missing_window_lines.append(f"- {store_code}: {', '.join(window_lines)}")
    missing_windows_detail = "\n".join(missing_window_lines) if missing_window_lines else ""
    lines = [
        "UC GST Run Summary",
        f"Overall Status: {_format_status_label(overall_status)} ({status_explanation})",
        (
            f"Stores: {uc_status_counts.get('success', 0)} success, "
            f"{uc_status_counts.get('success_with_warnings', 0)} success with warnings, "
            f"{uc_status_counts.get('partial', 0)} partial, {uc_status_counts.get('failed', 0)} failed"
            f"{skipped_suffix} across {total} stores"
        ),
        f"Windows Completed: {window_summary.get('completed_windows', 0)} / {window_summary.get('expected_windows', 0)}",
        missing_line,
    ]
    if missing_windows_detail:
        lines.append("Missing Window Ranges:")
        lines.append(missing_windows_detail)
    warning_counts_by_store: dict[str, int] = {}
    for store in stores_payload:
        store_code = store.get("store_code") or "UNKNOWN"
        normalized_store = _normalize_store_code(store_code) or store_code
        warning_rows_payload = _clean_uc_rows_for_reporting(store.get("warning_rows"), drop_empty=True)
        if warning_rows_payload:
            warning_counts_by_store[normalized_store] = len(warning_rows_payload)
            continue
        warning_count = _coerce_int(store.get("warning_count"))
        if warning_count is not None:
            warning_counts_by_store[normalized_store] = warning_count
    warnings = _uc_warning_entries(
        stores_payload=stores_payload,
        payload_warnings=payload.get("warnings") or metrics.get("warnings") or [],
        warning_counts_by_store=warning_counts_by_store,
    )
    if warnings:
        lines.append("Warnings:")
        lines.extend(f"- {warning}" for warning in warnings)
    store_lines: list[str] = []
    for store in stores_payload:
        store_code = store.get("store_code") or "UNKNOWN"
        rows_downloaded = _coerce_int(store.get("rows_downloaded")) or 0
        rows_ingested = _coalesce_int(store, "rows_ingested", "final_rows", "staging_rows") or 0
        inserted = _coalesce_int(store, "final_inserted", "staging_inserted") or 0
        updated = _coalesce_int(store, "final_updated", "staging_updated") or 0
        dropped_count = _coerce_int(store.get("rows_skipped_invalid")) or 0
        normalized_store = _normalize_store_code(store_code) or store_code
        warning_count = warning_counts_by_store.get(normalized_store, _coerce_int(store.get("warning_count")) or 0)
        reconciliation = (
            f"{rows_downloaded} == {inserted} + {updated} + {dropped_count}"
        )
        reason_counts = store.get("rows_skipped_invalid_reasons") or {}
        reason_parts = [f"{key}={value}" for key, value in reason_counts.items()] or ["(none)"]
        store_lines.extend(
            [
                f"- {store_code}",
                f"  rows_downloaded: {rows_downloaded}",
                f"  rows_ingested: {rows_ingested}",
                f"  inserted: {inserted}",
                f"  updated: {updated}",
                f"  warning_count: {warning_count}",
                f"  dropped_count: {dropped_count} ({', '.join(reason_parts)})",
                f"  reconciliation: rows_downloaded == inserted + updated + dropped_count ({reconciliation})",
            ]
        )
    if store_lines:
        lines.append("Window Reconciliation:")
        lines.extend(store_lines)
    return "\n".join(lines)


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
    profiler_missing_windows = resources.get("profiler_missing_windows") or {}

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
        "missing_windows_by_store": profiler_missing_windows,
    }
    if pipeline_name == "td_orders_sync":
        context.update(_build_td_orders_context(run_data, missing_windows_by_store=profiler_missing_windows))
    elif pipeline_name == "uc_orders_sync":
        context.update(_build_uc_orders_context(run_data, missing_windows_by_store=profiler_missing_windows))
    elif pipeline_name == "orders_sync_run_profiler":
        context.update(_build_profiler_context(run_data))
    else:
        context["started_at"] = _normalize_datetime(run_data.get("started_at"))
        context["finished_at"] = _normalize_datetime(run_data.get("finished_at"))
    ingest_rows = (run_data.get("metrics_json") or {}).get("ingest_remarks", {}).get("rows") or []
    if pipeline_name == "uc_orders_sync":
        ingest_rows = _clean_uc_rows_for_reporting(ingest_rows, drop_empty=True)
    prepared_rows, _, _, ingest_text = _prepare_ingest_remarks(ingest_rows)
    context["ingest_remarks"] = prepared_rows
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
    elif pipeline_name == "orders_sync_run_profiler":
        context.update(_build_profiler_context(run_data))
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
