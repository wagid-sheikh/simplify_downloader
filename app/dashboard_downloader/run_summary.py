from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Sequence

import sqlalchemy as sa

from app.common.db import session_scope

from .data_quality import (
    DASHBOARD_DATA_QUALITY_WARNING_THRESHOLDS,
    INVALID_CSV_DOWNLOADS,
    NAVIGATION_FAILURES,
    ROW_COERCION_FAILURES,
    SKIPPED_REQUIRED_ROWS,
    format_threshold_breach,
    threshold_for,
)
from .db_tables import documents, pipeline_run_summaries


PIPELINE_NAME = "dashboard_daily"
REQUIRED_RUN_SUMMARY_COLUMNS = (
    "pipeline_name",
    "run_id",
    "run_env",
    "started_at",
    "finished_at",
    "total_time_taken",
    "overall_status",
    "summary_text",
    "created_at",
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _format_ts(value: datetime | None) -> str:
    if not value:
        return ""
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _format_duration(seconds: int) -> str:
    seconds = max(0, seconds)
    hh = seconds // 3600
    mm = (seconds % 3600) // 60
    ss = seconds % 60
    return f"{hh:02d}:{mm:02d}:{ss:02d}"


def _normalize_status(raw: str | None) -> str:
    normalized = (raw or "ok").lower()
    if normalized in {"warning"}:
        return "warning"
    if normalized == "error":
        return "error"
    return "ok"


def _normalize_json_for_db(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value) if value.is_finite() else str(value)
    if isinstance(value, Mapping):
        return {key: _normalize_json_for_db(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_json_for_db(item) for item in value]
    if isinstance(value, tuple):
        return [_normalize_json_for_db(item) for item in value]
    return value


def missing_required_run_summary_columns(record: Mapping[str, Any]) -> List[str]:
    missing = [column for column in REQUIRED_RUN_SUMMARY_COLUMNS if column not in record]
    return missing


def _phase_overall(counters: Mapping[str, int]) -> str:
    if counters.get("error"):
        return "error"
    if counters.get("warning"):
        return "warning"
    return "ok"


@dataclass
class RunAggregator:
    run_id: str
    run_env: str
    store_codes: Sequence[str]
    pipeline_name: str = PIPELINE_NAME
    started_at: datetime = field(default_factory=_utc_now)
    report_date: date | None = None
    phase_counters: MutableMapping[str, Dict[str, int]] = field(
        default_factory=lambda: defaultdict(lambda: {"ok": 0, "warning": 0, "error": 0})
    )
    bucket_metrics: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    stores_processed: set[str] = field(default_factory=set)
    report_success: Dict[str, str] = field(default_factory=dict)
    report_failures: Dict[str, str] = field(default_factory=dict)
    pdf_records: List[Dict[str, Any]] = field(default_factory=list)
    email_metrics: Dict[str, Any] = field(
        default_factory=lambda: {"status": "pending", "message": "pending", "to": [], "attachment_count": 0}
    )
    issues: deque[str] = field(default_factory=lambda: deque(maxlen=5))
    data_quality_warnings: Dict[str, Any] = field(
        default_factory=lambda: {
            "thresholds": dict(DASHBOARD_DATA_QUALITY_WARNING_THRESHOLDS),
            "counts": {},
            "details": {},
            "breaches": [],
        }
    )
    bootstrap_retry_telemetry: Dict[str, Any] = field(
        default_factory=lambda: {"incidents": [], "recovered_count": 0}
    )

    def _add_issue(self, detail: str) -> None:
        if detail not in self.issues:
            self.issues.append(detail)

    def _record_data_quality_warning(
        self, code: str, *, count: int = 1, detail: Mapping[str, Any] | None = None
    ) -> None:
        if count <= 0:
            return
        counts = self.data_quality_warnings.setdefault("counts", {})
        counts[code] = int(counts.get(code, 0) or 0) + count
        if detail:
            details = self.data_quality_warnings.setdefault("details", {}).setdefault(
                code, []
            )
            if len(details) < 20:
                details.append(dict(detail))
        current_count = int(counts[code])
        threshold = threshold_for(code)
        breaches = self.data_quality_warnings.setdefault("breaches", [])
        if current_count >= threshold and not any(
            item.get("code") == code for item in breaches
        ):
            breach = {"code": code, "count": current_count, "threshold": threshold}
            breaches.append(breach)
            self._add_issue(
                "Data quality threshold breached: " + format_threshold_breach(breach)
            )

    def _record_data_quality_from_event(self, payload: Mapping[str, Any]) -> None:
        message = str(payload.get("message") or "")
        if message == "navigation attempt failed":
            self._record_data_quality_warning(
                NAVIGATION_FAILURES,
                detail={
                    "store_code": payload.get("store_code"),
                    "target_url": (payload.get("extras") or {}).get("target_url"),
                    "attempt": (payload.get("extras") or {}).get("attempt"),
                },
            )
        if message.startswith("discarding invalid CSV download"):
            self._record_data_quality_warning(
                INVALID_CSV_DOWNLOADS,
                detail={
                    "store_code": payload.get("store_code"),
                    "bucket": payload.get("bucket")
                    or (payload.get("extras") or {}).get("bucket"),
                    "reason": (payload.get("extras") or {}).get("reason")
                    or payload.get("reason"),
                },
            )
        if message == "failed to coerce csv row":
            details = self.data_quality_warnings.setdefault("details", {}).setdefault(
                ROW_COERCION_FAILURES, []
            )
            if len(details) < 20:
                details.append(
                    {
                        "bucket": payload.get("bucket"),
                        "merged_file": payload.get("merged_file"),
                        "row_index": payload.get("row_index"),
                        "error": payload.get("error"),
                    }
                )
        counts_payload = payload.get("counts")
        if message == "csv ingest summary" and isinstance(counts_payload, Mapping):
            self._record_data_quality_warning(
                ROW_COERCION_FAILURES,
                count=int(counts_payload.get("failed_rows") or 0),
                detail={
                    "bucket": payload.get("bucket"),
                    "merged_file": payload.get("merged_file"),
                    "failed_rows": counts_payload.get("failed_rows"),
                    "total_rows": counts_payload.get("total_rows"),
                },
            )
        skipped_payload = payload.get("skipped_required_rows") or payload.get(
            "skipped_missing_mobile"
        )
        if skipped_payload:
            if isinstance(skipped_payload, Mapping):
                total = int(skipped_payload.get("total") or 0)
                details = skipped_payload.get("details") or []
            else:
                details = (
                    list(skipped_payload)
                    if isinstance(skipped_payload, Iterable)
                    and not isinstance(skipped_payload, (str, bytes))
                    else []
                )
                total = sum(
                    int(item.get("count") or 0)
                    for item in details
                    if isinstance(item, Mapping)
                )
            if isinstance(details, list) and details:
                # Repeat-customer identity exclusions are informational only. Keep
                # them out of warning thresholds even if an older caller emits the
                # legacy generic skipped-required payload.
                details = [
                    item
                    for item in details
                    if not (
                        isinstance(item, Mapping)
                        and item.get("bucket") == "repeat_customers"
                        and item.get("column") == "mobile_no"
                    )
                ]
                total = sum(
                    int(item.get("count") or 0)
                    for item in details
                    if isinstance(item, Mapping)
                )
            self._record_data_quality_warning(
                SKIPPED_REQUIRED_ROWS,
                count=total,
                detail={"bucket": payload.get("bucket"), "details": details[:10]},
            )

    def record_log_event(self, payload: Mapping[str, Any]) -> None:
        phase = payload.get("phase")
        if not phase:
            return
        status = _normalize_status(payload.get("status"))
        self.phase_counters[phase][status] += 1
        if status in {"warning", "error"}:
            message = payload.get("message") or phase
            store_code = payload.get("store_code")
            bucket = payload.get("bucket")
            detail = message
            if store_code:
                detail = f"{detail} (store {store_code})"
            if bucket:
                detail = f"{detail} (bucket {bucket})"
            self._add_issue(detail)

        self._record_data_quality_from_event(payload)
        self._record_bootstrap_retry_telemetry(payload, status=status)

        if phase == "download" and payload.get("message") == "store download completed":
            store_code = payload.get("store_code")
            if store_code:
                self.stores_processed.add(store_code)

        if phase == "report":
            store_code = payload.get("store_code")
            if store_code and status in {"warning", "error"}:
                reason = payload.get("message", "report issue")
                self.report_failures[store_code] = reason

        if phase == "report_email":
            self.email_metrics["last_message"] = payload.get("message")
            self.email_metrics["status"] = status

    def _record_bootstrap_retry_telemetry(
        self, payload: Mapping[str, Any], *, status: str
    ) -> None:
        """Reclassify only terminal bootstrap-probe errors that later recover."""

        if payload.get("phase") != "download":
            return

        message = str(payload.get("message") or "")
        extras = payload.get("extras") or {}
        if not isinstance(extras, Mapping):
            return

        retry_context = extras.get("retry_context")
        if (
            message == "navigation attempt failed"
            and status == "error"
            and retry_context == "bootstrap_session_probe"
        ):
            self.bootstrap_retry_telemetry.setdefault("incidents", []).append(
                {
                    "retry_context": retry_context,
                    "status": "error",
                    "recovered": False,
                    "retry": dict(extras),
                }
            )
            return

        recovered_context = extras.get("recovered_retry_context")
        if (
            message != "bootstrap connectivity recovered successfully"
            or status != "warning"
            or recovered_context != "bootstrap_session_probe"
        ):
            return

        for incident in reversed(self.bootstrap_retry_telemetry.setdefault("incidents", [])):
            if incident.get("retry_context") != recovered_context or incident.get("recovered"):
                continue
            incident.update(
                {
                    "status": "warning",
                    "recovered": True,
                    "recovery_strategy": extras.get("recovery_strategy"),
                }
            )
            self.bootstrap_retry_telemetry["recovered_count"] = int(
                self.bootstrap_retry_telemetry.get("recovered_count", 0) or 0
            ) + 1
            # The recovery event already added one warning. Remove only the
            # matching terminal probe error; unrelated completion failures stay fatal.
            counters = self.phase_counters["download"]
            counters["error"] = max(0, int(counters.get("error", 0) or 0) - 1)
            break

    def set_report_date(self, report_date: date) -> None:
        self.report_date = report_date

    def record_download_summary(self, summary: Mapping[str, Dict[str, Dict[str, Any]]]) -> None:
        for bucket, info in summary.items():
            entry = self.bucket_metrics.setdefault(bucket, {"stores": {}, "counts": {}})
            stores = entry.setdefault("stores", {})
            counts = entry.setdefault("counts", {})
            total = 0
            for store_code, details in info.items():
                if store_code == "__merged__":
                    counts["merged_rows"] = int(details.get("rows", 0) or 0)
                    entry["merged_file"] = details.get("path")
                    continue
                rows = int(details.get("rows", 0) or 0)
                stores[store_code] = rows
                total += rows
                if store_code:
                    self.stores_processed.add(store_code)
            counts["download_total"] = max(counts.get("download_total", 0), total)

    def record_bucket_counts(self, bucket: str, counts: Mapping[str, Any]) -> None:
        entry = self.bucket_metrics.setdefault(bucket, {"stores": {}, "counts": {}})
        existing = entry.setdefault("counts", {})
        for key in ("download_total", "merged_rows", "ingested_rows"):
            value = counts.get(key)
            if value is not None:
                existing[key] = int(value)
        ingested_by_store = counts.get("ingested_by_store")
        if ingested_by_store:
            entry["ingested_by_store"] = {
                store_code: int(store_count)
                for store_code, store_count in ingested_by_store.items()
            }

    def register_pdf_success(self, store_code: str, output_path: str, *, record_file: bool = True) -> None:
        self.report_success[store_code] = output_path
        if record_file:
            self.pdf_records.append({"store_code": store_code, "file_path": output_path})

    def record_pdf_file(self, store_code: str, output_path: str) -> None:
        self.pdf_records.append({"store_code": store_code, "file_path": output_path})

    def register_pdf_failure(self, store_code: str, message: str) -> None:
        self.report_failures[store_code] = message
        self.report_success.pop(store_code, None)

    def plan_email(self, *, recipients: Sequence[str], attachment_count: int, message: str) -> None:
        self.email_metrics.update(
            {
                "status": "pending",
                "message": message,
                "to": list(recipients),
                "attachment_count": attachment_count,
            }
        )

    def finalize_email(self, *, status: str, message: str, recipients: Sequence[str], attachment_count: int) -> None:
        self.email_metrics.update(
            {
                "status": status,
                "message": message,
                "to": list(recipients),
                "attachment_count": attachment_count,
            }
        )

    def _phases_json(self) -> Dict[str, Dict[str, int]]:
        return {phase: dict(counts) for phase, counts in self.phase_counters.items()}

    def _metrics_json(self) -> Dict[str, Any]:
        return {
            "stores": {
                "configured": sorted(set(self.store_codes)),
                "processed": sorted(self.stores_processed),
                "report_success": sorted(self.report_success.keys()),
                "report_failures": self.report_failures,
            },
            "buckets": self.bucket_metrics,
            "pdf": {
                "generated": len(self.pdf_records),
                "failed": len(self.report_failures),
                "records": list(self.pdf_records),
            },
            "email": dict(self.email_metrics),
            "issues": list(self.issues),
            "data_quality_warnings": self.data_quality_warnings,
            "bootstrap_retry_telemetry": self.bootstrap_retry_telemetry,
        }

    def _phase_status(self, phase: str) -> str:
        return _phase_overall(self.phase_counters.get(phase, {}))

    def _phase_line(self, phase: str) -> str:
        status = self._phase_status(phase).upper()
        if phase == "download":
            total = len(self.store_codes)
            processed = len(self.stores_processed)
            return f"- {phase}: {status} ({processed}/{total} stores)"
        if phase == "merge":
            buckets = len(self.bucket_metrics)
            return f"- {phase}: {status} ({buckets} buckets)"
        if phase == "report":
            return f"- {phase}: {status} ({len(self.pdf_records)} PDFs)"
        if phase == "report_email":
            email_status = self.email_metrics.get("status", "pending")
            return f"- {phase}: {status} ({email_status})"
        return f"- {phase}: {status}"

    def _data_section(self) -> List[str]:
        lines = []
        processed = ", ".join(sorted(self.stores_processed)) or "None"
        lines.append(f"- Stores processed: {processed}")
        lines.append("- Buckets:")
        if not self.bucket_metrics:
            lines.append("  - None")
        else:
            for bucket in sorted(self.bucket_metrics):
                counts = self.bucket_metrics[bucket].get("counts", {})
                row_count = counts.get("ingested_rows") or counts.get("merged_rows") or counts.get("download_total") or 0
                lines.append(f"  - {bucket}: {row_count} rows")
        missed_leads = self.bucket_metrics.get("missed_leads", {})
        missed_downloads = missed_leads.get("stores", {}) or {}
        missed_ingested = missed_leads.get("ingested_by_store", {}) or {}
        recovered_bootstrap_count = int(
            self.bootstrap_retry_telemetry.get("recovered_count", 0) or 0
        )
        if recovered_bootstrap_count:
            lines.append(
                "- Bootstrap connectivity recovered successfully "
                f"after terminal retry exhaustion: {recovered_bootstrap_count} incident(s)"
            )
        breaches = self.data_quality_warnings.get("breaches") or []
        if breaches:
            lines.append("- Data quality warning thresholds:")
            for breach in breaches:
                lines.append(f"  - {format_threshold_breach(breach)}")
        if missed_downloads or missed_ingested:
            lines.append("- Missed leads by store:")
            for store_code in sorted(set(missed_downloads) | set(missed_ingested)):
                downloaded = missed_downloads.get(store_code, 0)
                ingested = missed_ingested.get(store_code, 0)
                lines.append(
                    f"  - {store_code}: downloaded {downloaded}, ingested {ingested}"
                )
        return lines

    def build_summary_text(self, *, finished_at: datetime) -> str:
        started = _format_ts(self.started_at)
        finished = _format_ts(finished_at)
        duration = _format_duration(int((finished_at - self.started_at).total_seconds()))
        report_date = self.report_date.isoformat() if self.report_date else "unknown"
        status = self.overall_status().lower()
        lines = [
            f"Pipeline: {self.pipeline_name}",
            f"Run ID: {self.run_id}",
            f"Env: {self.run_env}",
            f"Report Date: {report_date}",
            f"Started: {started}  Finished: {finished}  Duration: {duration}",
            f"Status: {status}",
            "",
            "Phases:",
        ]
        for phase in ("db", "download", "merge", "ingest", "audit", "cleanup", "report", "report_email"):
            if phase in self.phase_counters:
                lines.append(self._phase_line(phase))
        lines.append("")
        lines.append("Data:")
        lines.extend(self._data_section())
        lines.append("")
        lines.append("Issues:")
        if self.issues:
            for issue in self.issues:
                lines.append(f"- {issue}")
        else:
            lines.append("- None.")
        return "\n".join(lines)

    def overall_status(self) -> str:
        all_counts = self._phases_json().values()
        if any(counts.get("error") for counts in all_counts):
            return "error"
        if any(counts.get("warning") for counts in all_counts):
            return "warning"
        return "ok"

    def build_record(self, *, finished_at: datetime) -> Dict[str, Any]:
        summary_text = self.build_summary_text(finished_at=finished_at)
        total_time = _format_duration(int((finished_at - self.started_at).total_seconds()))
        record = {
            "pipeline_name": self.pipeline_name,
            "run_id": self.run_id,
            "run_env": self.run_env,
            "started_at": self.started_at,
            "finished_at": finished_at,
            "total_time_taken": total_time,
            "report_date": self.report_date,
            "overall_status": self.overall_status(),
            "summary_text": summary_text,
            "phases_json": self._phases_json(),
            "metrics_json": self._metrics_json(),
        }
        return record


async def insert_run_summary(database_url: str, record: Mapping[str, Any]) -> None:
    normalized_record = dict(record)
    normalized_record["phases_json"] = _normalize_json_for_db(record.get("phases_json") or {})
    normalized_record["metrics_json"] = _normalize_json_for_db(record.get("metrics_json") or {})
    async with session_scope(database_url) as session:
        await session.execute(sa.insert(pipeline_run_summaries).values(**normalized_record))
        await session.commit()


async def update_run_summary(database_url: str, run_id: str, record: Mapping[str, Any]) -> None:
    normalized_record = dict(record)
    normalized_record["phases_json"] = _normalize_json_for_db(record.get("phases_json") or {})
    normalized_record["metrics_json"] = _normalize_json_for_db(record.get("metrics_json") or {})
    async with session_scope(database_url) as session:
        await session.execute(
            sa.update(pipeline_run_summaries)
            .where(pipeline_run_summaries.c.run_id == run_id)
            .values(**normalized_record)
        )
        await session.commit()


async def fetch_summary_for_run(database_url: str, run_id: str) -> Mapping[str, Any] | None:
    async with session_scope(database_url) as session:
        result = await session.execute(
            sa.select(pipeline_run_summaries).where(pipeline_run_summaries.c.run_id == run_id).limit(1)
        )
        return result.mappings().first()
