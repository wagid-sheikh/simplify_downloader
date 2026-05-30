from __future__ import annotations

from app.reports.upstream import build_orders_sync_upstream_context


def test_success_with_warnings_orders_sync_degrades_downstream_reports() -> None:
    upstream = build_orders_sync_upstream_context(
        status="success_with_warnings", run_id="orders-run-with-garment-warning"
    )

    assert upstream.status == "success_with_warnings"
    assert upstream.run_id == "orders-run-with-garment-warning"
    assert upstream.is_degraded is True
    assert "stale or incomplete" in upstream.warning_text


def test_success_orders_sync_does_not_degrade_downstream_reports() -> None:
    upstream = build_orders_sync_upstream_context(
        status="success", run_id="orders-run-ok"
    )

    assert upstream.is_degraded is False
    assert upstream.warning_text == ""
