from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Sequence

import sqlalchemy as sa

from common.db import session_scope

from .db_tables import documents, pipeline_run_summaries


PIPELINE_NAME = "simplify_dashboard_daily"


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
    if normalized in {"warn", "warning"}:
        return "warning"
    if normalized == "error":
        return "error"
    return "ok"


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
            if detail not in self.issues:
                self.issues.append(detail)

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
    async with session_scope(database_url) as session:
        await session.execute(sa.insert(pipeline_run_summaries).values(**record))
        await session.commit()


async def update_run_summary(database_url: str, run_id: str, record: Mapping[str, Any]) -> None:
    async with session_scope(database_url) as session:
        await session.execute(
            sa.update(pipeline_run_summaries).where(pipeline_run_summaries.c.run_id == run_id).values(**record)
        )
        await session.commit()


async def fetch_summary_for_run(database_url: str, run_id: str) -> Mapping[str, Any] | None:
    async with session_scope(database_url) as session:
        result = await session.execute(
            sa.select(pipeline_run_summaries).where(pipeline_run_summaries.c.run_id == run_id).limit(1)
        )
        return result.mappings().first()



