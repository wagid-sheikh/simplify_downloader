from __future__ import annotations

from dataclasses import dataclass

DEGRADED_ORDERS_SYNC_MESSAGE = (
    "Orders sync was not verified as successful before this report; "
    "data freshness or completeness could not be verified."
)
HEALTHY_ORDERS_SYNC_STATUSES = frozenset({"success"})
DEGRADED_ORDERS_SYNC_STATUSES = frozenset(
    {
        "failed",
        "error",
        "success_with_warnings",
        "warning",
        "partial",
        "skipped",
        "unknown",
    }
)


@dataclass(frozen=True)
class OrdersSyncUpstreamContext:
    status: str | None = None
    run_id: str | None = None

    @property
    def is_degraded(self) -> bool:
        # Reports must fail closed: cron runs without a verified successful
        # profiler status cannot claim that upstream data is fresh or complete.
        return self.status not in HEALTHY_ORDERS_SYNC_STATUSES

    @property
    def warning_text(self) -> str:
        return DEGRADED_ORDERS_SYNC_MESSAGE if self.is_degraded else ""

    def as_metrics(self) -> dict[str, str | bool | None]:
        return {
            "status": self.status,
            "run_id": self.run_id,
            "is_degraded": self.is_degraded,
            "warning_text": self.warning_text,
        }


def build_orders_sync_upstream_context(
    *, status: str | None = None, run_id: str | None = None
) -> OrdersSyncUpstreamContext:
    normalized_status = (status or "").strip().lower() or None
    normalized_run_id = (run_id or "").strip() or None
    return OrdersSyncUpstreamContext(status=normalized_status, run_id=normalized_run_id)
