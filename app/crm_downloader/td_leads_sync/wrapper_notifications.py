"""Persist and deliver sanitized operational notifications for the TD-leads cron wrapper.

Inputs are explicit wrapper metadata CLI flags: timestamp, host, local-lock path,
owner PID/PGID/age, recovery action, and resulting status. The module writes a
sanitized ``pipeline_run_summaries`` telemetry row for every invocation and
returns a JSON delivery result. It sends DB-driven email only for an initial
incident edge or a recovery edge; repeated active-owner suppressions are stored
but deduplicated.

Example::

    poetry run python -m app.crm_downloader.td_leads_sync.wrapper_notifications \
      --wrapper-timestamp 2026-06-01T00:00:00Z --hostname td-host \
      --local-lock-path /srv/app/tmp/cron_run_td_leads_sync.lock \
      --owner-pid 123 --owner-pgid 123 --owner-age-seconds 301 \
      --recovery-action terminated_stale_owner_process_group \
      --status stale_owner_terminated
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

import sqlalchemy as sa

from app.common.db import session_scope
from app.config import config
from app.dashboard_downloader.db_tables import pipeline_run_summaries
from app.dashboard_downloader.notifications import send_notifications_for_run

PIPELINE_CODE = "td_leads_wrapper_ops"
RECOVERY_STATUS = "fresh_run_succeeded_after_stale_owner_terminated"
ALERT_STATUSES = frozenset(
    {
        "watchdog_timeout",
        "stale_owner_terminated",
        "skipped_due_to_active_same_pipeline_owner",
        "lock_metadata_ambiguous",
    }
)
ALLOWED_STATUSES = ALERT_STATUSES | {RECOVERY_STATUS}
_REDACTED = "<redacted>"


@dataclass(frozen=True)
class WrapperEvent:
    wrapper_timestamp: datetime
    hostname: str
    local_lock_path: str
    owner_pid: int | None
    owner_pgid: int | None
    owner_age_seconds: int | None
    recovery_action: str
    status: str

    @property
    def incident_fingerprint(self) -> str:
        return "|".join(
            (
                self.hostname,
                self.local_lock_path,
                str(self.owner_pid or "unknown"),
                str(self.owner_pgid or "unknown"),
            )
        )

    def as_metrics(self) -> dict[str, Any]:
        return {
            "wrapper_timestamp": self.wrapper_timestamp.isoformat(),
            "hostname": self.hostname,
            "local_lock_path": self.local_lock_path,
            "owner_pid": self.owner_pid,
            "owner_pgid": self.owner_pgid,
            "owner_age_seconds": self.owner_age_seconds,
            "recovery_action": self.recovery_action,
            "resulting_status": self.status,
            "incident_fingerprint": self.incident_fingerprint,
        }


def _sanitize_text(value: str, *, max_length: int = 500) -> str:
    """Keep wrapper telemetry operational-only even if a caller passes unsafe text."""

    text = " ".join(str(value or "").split())
    text = re.sub(
        r"(?i)\b(password|passwd|token|secret|credential|authorization)\s*[:=]\s*\S+",
        rf"\1={_REDACTED}",
        text,
    )
    text = re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", _REDACTED, text)
    text = re.sub(r"(?<!\d)(?:\+?\d[\d\s().-]{7,}\d)(?!\d)", _REDACTED, text)
    return text[:max_length]


def _parse_optional_non_negative_int(
    value: str | int | None, *, field_name: str
) -> int | None:
    if value in (None, "", "unknown"):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        # Ambiguous lock metadata is itself reportable. Preserve the event while
        # refusing to echo malformed free-form PID text into telemetry or email.
        return None
    return parsed if parsed >= 0 else None


def build_wrapper_event(
    *,
    wrapper_timestamp: str,
    hostname: str,
    local_lock_path: str,
    owner_pid: str | int | None,
    owner_pgid: str | int | None,
    owner_age_seconds: str | int | None,
    recovery_action: str,
    status: str,
) -> WrapperEvent:
    if status not in ALLOWED_STATUSES:
        raise ValueError(f"unsupported TD-leads wrapper status: {status}")
    parsed_timestamp = datetime.fromisoformat(wrapper_timestamp.replace("Z", "+00:00"))
    if parsed_timestamp.tzinfo is None:
        parsed_timestamp = parsed_timestamp.replace(tzinfo=timezone.utc)
    return WrapperEvent(
        wrapper_timestamp=parsed_timestamp.astimezone(timezone.utc),
        hostname=_sanitize_text(hostname, max_length=255),
        local_lock_path=_sanitize_text(local_lock_path),
        owner_pid=_parse_optional_non_negative_int(owner_pid, field_name="owner_pid"),
        owner_pgid=_parse_optional_non_negative_int(
            owner_pgid, field_name="owner_pgid"
        ),
        owner_age_seconds=_parse_optional_non_negative_int(
            owner_age_seconds, field_name="owner_age_seconds"
        ),
        recovery_action=_sanitize_text(recovery_action),
        status=status,
    )


def _event_summary(event: WrapperEvent) -> str:
    metrics = event.as_metrics()
    return "\n".join(
        [
            "TD leads wrapper operational event",
            f"Wrapper timestamp: {metrics['wrapper_timestamp']}",
            f"Hostname: {metrics['hostname']}",
            f"Local lock path: {metrics['local_lock_path']}",
            f"Owner PID: {metrics['owner_pid'] if metrics['owner_pid'] is not None else 'unknown'}",
            f"Owner PGID: {metrics['owner_pgid'] if metrics['owner_pgid'] is not None else 'unknown'}",
            f"Owner age seconds: {metrics['owner_age_seconds'] if metrics['owner_age_seconds'] is not None else 'unknown'}",
            f"Recovery action: {metrics['recovery_action']}",
            f"Resulting status: {metrics['resulting_status']}",
        ]
    )


def _same_lock_scope(metrics: Mapping[str, Any], event: WrapperEvent) -> bool:
    return (
        metrics.get("hostname") == event.hostname
        and metrics.get("local_lock_path") == event.local_lock_path
    )


def _should_notify(
    event: WrapperEvent, previous_rows: Sequence[Mapping[str, Any]]
) -> tuple[bool, str]:
    if event.status == RECOVERY_STATUS:
        # A fresh owner has a different PID. Match recovery by host + lock path and
        # stop at the newest scope event so a healthy run cannot re-close old incidents.
        for row in previous_rows:
            metrics = row.get("metrics_json") or {}
            if not _same_lock_scope(metrics, event):
                continue
            if metrics.get("resulting_status") in ALERT_STATUSES:
                return True, "recovery"
            return False, "no_active_incident"
        return False, "no_active_incident"
    for row in previous_rows:
        metrics = row.get("metrics_json") or {}
        if metrics.get("incident_fingerprint") != event.incident_fingerprint:
            continue
        previous_status = metrics.get("resulting_status")
        if previous_status == RECOVERY_STATUS:
            return True, "initial_after_recovery"
        if previous_status in ALERT_STATUSES:
            return False, "deduplicated_active_incident"
    return True, "initial"


async def _load_previous_events(database_url: str) -> list[Mapping[str, Any]]:
    async with session_scope(database_url) as session:
        rows = await session.execute(
            sa.select(pipeline_run_summaries.c.metrics_json)
            .where(pipeline_run_summaries.c.pipeline_name == PIPELINE_CODE)
            .order_by(
                pipeline_run_summaries.c.created_at.desc(),
                pipeline_run_summaries.c.id.desc(),
            )
            .limit(500)
        )
        return list(rows.mappings().all())


async def _persist_event(
    database_url: str, event: WrapperEvent, *, disposition: str
) -> str:
    run_id = f"tdlw-{uuid.uuid4().hex[:24]}"
    metrics = event.as_metrics()
    metrics["notification_disposition"] = disposition
    record = {
        "pipeline_name": PIPELINE_CODE,
        "run_id": run_id,
        "run_env": config.run_env,
        "started_at": event.wrapper_timestamp,
        "finished_at": event.wrapper_timestamp,
        "total_time_taken": "00:00:00",
        "report_date": event.wrapper_timestamp.date(),
        "overall_status": "ok" if event.status == RECOVERY_STATUS else "warning",
        "summary_text": _event_summary(event),
        "phases_json": {
            "wrapper": {
                "warning": 0 if event.status == RECOVERY_STATUS else 1,
                "ok": 1 if event.status == RECOVERY_STATUS else 0,
            }
        },
        "metrics_json": metrics,
    }
    async with session_scope(database_url) as session:
        await session.execute(sa.insert(pipeline_run_summaries).values(**record))
        await session.commit()
    return run_id


async def process_wrapper_event(event: WrapperEvent) -> dict[str, Any]:
    """Persist every sanitized event; email only an incident edge or recovery edge."""

    previous_rows = await _load_previous_events(config.database_url)
    should_notify, disposition = _should_notify(event, previous_rows)
    if event.status == RECOVERY_STATUS and not should_notify:
        return {
            "run_id": None,
            "status": event.status,
            "notification_disposition": disposition,
            "notification_requested": False,
            "emails_sent": 0,
            "errors": [],
        }
    run_id = await _persist_event(config.database_url, event, disposition=disposition)
    result: dict[str, Any] = {
        "run_id": run_id,
        "status": event.status,
        "notification_disposition": disposition,
        "notification_requested": should_notify,
        "emails_sent": 0,
        "errors": [],
    }
    if not should_notify:
        return result
    delivery = await send_notifications_for_run(PIPELINE_CODE, run_id)
    result["emails_sent"] = delivery.get("emails_sent", 0)
    result["emails_planned"] = delivery.get("emails_planned", 0)
    result["errors"] = delivery.get("errors", [])
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Persist and notify a sanitized TD-leads wrapper operational event."
    )
    parser.add_argument("--wrapper-timestamp", required=True)
    parser.add_argument("--hostname", required=True)
    parser.add_argument("--local-lock-path", required=True)
    parser.add_argument("--owner-pid", default="unknown")
    parser.add_argument("--owner-pgid", default="unknown")
    parser.add_argument("--owner-age-seconds", default="unknown")
    parser.add_argument("--recovery-action", required=True)
    parser.add_argument("--status", required=True, choices=sorted(ALLOWED_STATUSES))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    event = build_wrapper_event(
        wrapper_timestamp=args.wrapper_timestamp,
        hostname=args.hostname,
        local_lock_path=args.local_lock_path,
        owner_pid=args.owner_pid,
        owner_pgid=args.owner_pgid,
        owner_age_seconds=args.owner_age_seconds,
        recovery_action=args.recovery_action,
        status=args.status,
    )
    result = asyncio.run(process_wrapper_event(event))
    print(json.dumps(result, sort_keys=True, default=str))
    return 1 if result["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
