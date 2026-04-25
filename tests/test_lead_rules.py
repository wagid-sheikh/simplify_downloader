from __future__ import annotations

from app.common.lead_rules import cancelled_flag_from_reason, is_customer_cancelled, resolve_cancelled_flag


def test_cancelled_flag_from_reason_handles_null_blank_and_whitespace() -> None:
    assert cancelled_flag_from_reason(None) == "customer"
    assert cancelled_flag_from_reason("") == "customer"
    assert cancelled_flag_from_reason("   ") == "customer"
    assert cancelled_flag_from_reason("No inventory") == "store"


def test_resolve_cancelled_flag_prefers_persisted_normalized_flag() -> None:
    assert resolve_cancelled_flag(cancelled_flag=" customer ", reason="No inventory") == "customer"
    assert resolve_cancelled_flag(cancelled_flag="STORE", reason="") == "store"


def test_resolve_cancelled_flag_falls_back_to_reason_rule_for_missing_or_unknown_flag() -> None:
    assert resolve_cancelled_flag(cancelled_flag=None, reason="") == "customer"
    assert resolve_cancelled_flag(cancelled_flag="", reason="  ") == "customer"
    assert resolve_cancelled_flag(cancelled_flag="unknown", reason="No stock") == "store"


def test_is_customer_cancelled_uses_resolved_flag() -> None:
    assert is_customer_cancelled(cancelled_flag="customer", reason="No inventory") is True
    assert is_customer_cancelled(cancelled_flag="store", reason="") is False
