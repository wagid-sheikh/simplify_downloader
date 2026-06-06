"""Network preflight checks for the TD CRM leads cron wrapper.

The cron wrapper runs this module before launching Playwright so predictable
DNS/TCP outages are surfaced as short, operator-readable run summaries instead
of long browser timeouts.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import socket
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from app.common.date_utils import aware_now, get_timezone
from app.config import config
from app.dashboard_downloader.notifications import probe_smtp_tcp_connectivity
from app.dashboard_downloader.run_summary import (
    fetch_summary_for_run,
    insert_run_summary,
    update_run_summary,
)

PIPELINE_NAME = "td_crm_leads_sync"
CRM_HOST = "subs.quickdrycleaning.com"
CRM_PORT = 443
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
EXIT_OK = 0
EXIT_CRM_UNREACHABLE = 20
EXIT_SMTP_DEGRADED = 30


@dataclass(frozen=True)
class EndpointProbe:
    name: str
    host: str
    port: int
    ok: bool
    dns_ok: bool
    tcp_ok: bool
    failure_class: str | None
    elapsed_ms: float
    error: str | None = None
    exception_type: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "host": self.host,
            "port": self.port,
            "ok": self.ok,
            "dns_ok": self.dns_ok,
            "tcp_ok": self.tcp_ok,
            "failure_class": self.failure_class,
            "elapsed_ms": self.elapsed_ms,
            "error": self.error,
            "exception_type": self.exception_type,
        }


def _elapsed_ms(started: float) -> float:
    return round((time.monotonic() - started) * 1000, 2)


def _clean(value: object) -> str:
    return str(value).replace("\n", " ").strip()


def probe_dns_tcp_endpoint(
    *, name: str, host: str, port: int, timeout_seconds: float
) -> EndpointProbe:
    """Resolve an endpoint and open one TCP socket without application login."""

    started = time.monotonic()
    try:
        addresses = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        return EndpointProbe(
            name=name,
            host=host,
            port=port,
            ok=False,
            dns_ok=False,
            tcp_ok=False,
            failure_class="dns_resolution_failure",
            elapsed_ms=_elapsed_ms(started),
            error=_clean(exc),
            exception_type=type(exc).__name__,
        )

    last_error = ""
    saw_timeout = False
    for family, socktype, proto, _canonname, sockaddr in addresses:
        try:
            with socket.socket(family, socktype, proto) as sock:
                sock.settimeout(timeout_seconds)
                sock.connect(sockaddr)
                return EndpointProbe(
                    name=name,
                    host=host,
                    port=port,
                    ok=True,
                    dns_ok=True,
                    tcp_ok=True,
                    failure_class=None,
                    elapsed_ms=_elapsed_ms(started),
                )
        except OSError as exc:
            last_error = _clean(exc)
            saw_timeout = saw_timeout or isinstance(exc, socket.timeout)

    return EndpointProbe(
        name=name,
        host=host,
        port=port,
        ok=False,
        dns_ok=True,
        tcp_ok=False,
        failure_class=(
            "tcp_connection_timeout" if saw_timeout else "tcp_connection_failure"
        ),
        elapsed_ms=_elapsed_ms(started),
        error=last_error or "no_addresses_attempted",
        exception_type="TimeoutError" if saw_timeout else "OSError",
    )


def probe_smtp_endpoint(*, timeout_seconds: float) -> EndpointProbe:
    """Probe the notification SMTP endpoint through the shared SMTP diagnostic."""

    result = probe_smtp_tcp_connectivity(
        timeout_seconds=timeout_seconds,
        host=SMTP_HOST,
        port=SMTP_PORT,
    )
    ok = bool(result.get("ok"))
    exception_type = str(result.get("exception_type") or "") or None
    if ok:
        failure_class = None
        dns_ok = True
        tcp_ok = True
    elif exception_type == "gaierror":
        failure_class = "dns_resolution_failure"
        dns_ok = False
        tcp_ok = False
    elif exception_type in {"TimeoutError", "timeout"}:
        failure_class = "tcp_connection_timeout"
        dns_ok = True
        tcp_ok = False
    else:
        failure_class = "tcp_connection_failure"
        dns_ok = True
        tcp_ok = False
    return EndpointProbe(
        name="smtp_notifications",
        host=str(result.get("host") or SMTP_HOST),
        port=int(result.get("port") or SMTP_PORT),
        ok=ok,
        dns_ok=dns_ok,
        tcp_ok=tcp_ok,
        failure_class=failure_class,
        elapsed_ms=float(result.get("elapsed_ms") or 0),
        error=str(result.get("exception_summary") or "") or None,
        exception_type=exception_type,
    )


def run_preflight(*, timeout_seconds: float) -> tuple[int, dict[str, Any]]:
    crm_probe = probe_dns_tcp_endpoint(
        name="td_crm", host=CRM_HOST, port=CRM_PORT, timeout_seconds=timeout_seconds
    )
    smtp_probe = probe_smtp_endpoint(timeout_seconds=timeout_seconds)
    if not crm_probe.ok:
        classification = "crm_connectivity_failed"
        exit_code = EXIT_CRM_UNREACHABLE
    elif not smtp_probe.ok:
        classification = "smtp_notification_degraded"
        exit_code = EXIT_SMTP_DEGRADED
    else:
        classification = "all_endpoints_ok"
        exit_code = EXIT_OK
    payload = {
        "classification": classification,
        "crm": crm_probe.as_dict(),
        "smtp": smtp_probe.as_dict(),
        "notification_delivery_degraded": not smtp_probe.ok,
        "browser_launch_allowed": crm_probe.ok,
    }
    return exit_code, payload


def _duration_human(started_at: datetime, finished_at: datetime) -> str:
    elapsed_seconds = max(0, int((finished_at - started_at).total_seconds()))
    hh, mm, ss = (
        elapsed_seconds // 3600,
        (elapsed_seconds % 3600) // 60,
        elapsed_seconds % 60,
    )
    return f"{hh:02d}:{mm:02d}:{ss:02d}"


def build_crm_failure_summary_record(
    *,
    run_id: str,
    run_env: str,
    started_at: datetime,
    finished_at: datetime,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    crm = payload.get("crm") if isinstance(payload.get("crm"), Mapping) else {}
    smtp = payload.get("smtp") if isinstance(payload.get("smtp"), Mapping) else {}
    failure_class = crm.get("failure_class") or "unknown_connectivity_failure"
    summary_lines = [
        "TD CRM Leads Sync Preflight Summary",
        f"Run ID: {run_id}",
        f"Env: {run_env}",
        f"Overall Status: skipped_preflight_failed",
        "Browser launch skipped because TD CRM DNS/TCP preflight failed.",
        f"CRM Endpoint: {CRM_HOST}:{CRM_PORT}",
        f"CRM Failure Class: {failure_class}",
        f"SMTP Endpoint: {SMTP_HOST}:{SMTP_PORT}",
        f"SMTP Status: {'ok' if smtp.get('ok') else 'degraded'}",
    ]
    return {
        "pipeline_name": PIPELINE_NAME,
        "run_id": run_id,
        "run_env": run_env,
        "started_at": started_at,
        "finished_at": finished_at,
        "total_time_taken": _duration_human(started_at, finished_at),
        "report_date": aware_now(get_timezone()).date(),
        "overall_status": "skipped_preflight_failed",
        "summary_text": "\n".join(summary_lines),
        "phases_json": {"preflight": {"error": 1}},
        "metrics_json": {
            "preflight": payload,
            "crm_preflight_failed": True,
            "browser_launch_skipped": True,
            "notification_delivery_degraded": bool(
                payload.get("notification_delivery_degraded")
            ),
        },
        "created_at": started_at,
    }


async def persist_crm_failure_summary(
    *, payload: Mapping[str, Any], run_id: str | None = None
) -> str | None:
    if not config.database_url:
        return None
    started_at = datetime.now(timezone.utc)
    finished_at = datetime.now(timezone.utc)
    resolved_run_id = run_id or f"td-leads-preflight-{uuid.uuid4().hex[:12]}"
    record = build_crm_failure_summary_record(
        run_id=resolved_run_id,
        run_env=config.run_env,
        started_at=started_at,
        finished_at=finished_at,
        payload=payload,
    )
    existing = await fetch_summary_for_run(config.database_url, resolved_run_id)
    if existing:
        await update_run_summary(config.database_url, resolved_run_id, record)
    else:
        await insert_run_summary(config.database_url, record)
    return resolved_run_id


def _emit(payload: Mapping[str, Any]) -> None:
    print(
        "td_leads_network_preflight_summary "
        f"classification={payload.get('classification')} "
        f"browser_launch_allowed={payload.get('browser_launch_allowed')} "
        f"notification_delivery_degraded={payload.get('notification_delivery_degraded')} "
        f"payload_json={json.dumps(payload, sort_keys=True)}"
    )


async def _async_entrypoint(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run TD leads DNS/TCP network preflight"
    )
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args(list(argv) if argv is not None else None)

    exit_code, payload = run_preflight(timeout_seconds=args.timeout_seconds)
    persisted_run_id = None
    if exit_code == EXIT_CRM_UNREACHABLE:
        persisted_run_id = await persist_crm_failure_summary(
            payload=payload, run_id=args.run_id
        )
        payload = {**payload, "persisted_run_id": persisted_run_id}
    _emit(payload)
    return exit_code


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(_async_entrypoint(argv))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
