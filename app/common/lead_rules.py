from __future__ import annotations

from typing import Any, Literal

CancelledLeadAttribution = Literal["customer", "store"]


def cancelled_flag_from_reason(reason: Any) -> CancelledLeadAttribution:
    """Canonical attribution rule for cancelled leads.

    Blank/whitespace/null reason means customer cancelled; otherwise store cancelled.
    """

    normalized_reason = str(reason or "").strip()
    return "customer" if not normalized_reason else "store"


def resolve_cancelled_flag(*, cancelled_flag: Any, reason: Any) -> CancelledLeadAttribution:
    """Resolve cancelled attribution with persisted flag as primary source."""

    normalized_flag = str(cancelled_flag or "").strip().lower()
    if normalized_flag in {"customer", "store"}:
        return normalized_flag  # type: ignore[return-value]
    return cancelled_flag_from_reason(reason)


def is_customer_cancelled(*, cancelled_flag: Any, reason: Any) -> bool:
    return resolve_cancelled_flag(cancelled_flag=cancelled_flag, reason=reason) == "customer"
