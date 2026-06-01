from __future__ import annotations

import pytest

from app.reports.daily_sales_report import pipeline as daily_sales_pipeline
from app.reports.pending_deliveries import pipeline as pending_deliveries_pipeline
from app.reports.upstream import (
    DEGRADED_ORDERS_SYNC_STATUSES,
    HEALTHY_ORDERS_SYNC_STATUSES,
    build_orders_sync_upstream_context,
)


@pytest.mark.parametrize("status", sorted(DEGRADED_ORDERS_SYNC_STATUSES))
def test_supported_degraded_orders_sync_statuses_degrade_downstream_reports(
    status: str,
) -> None:
    upstream = build_orders_sync_upstream_context(
        status=status, run_id=f"orders-run-{status}"
    )

    assert upstream.status == status
    assert upstream.run_id == f"orders-run-{status}"
    assert upstream.is_degraded is True
    assert (
        upstream.warning_text
        == "Orders sync was not verified as successful before this report; "
        "data freshness or completeness could not be verified."
    )


@pytest.mark.parametrize("status", sorted(HEALTHY_ORDERS_SYNC_STATUSES))
def test_supported_healthy_orders_sync_statuses_do_not_degrade_downstream_reports(
    status: str,
) -> None:
    upstream = build_orders_sync_upstream_context(status=status, run_id="orders-run-ok")

    assert upstream.status == "success"
    assert upstream.is_degraded is False
    assert upstream.warning_text == ""


def test_missing_orders_sync_status_degrades_cron_launched_report() -> None:
    upstream = build_orders_sync_upstream_context(run_id=None)

    assert upstream.status is None
    assert upstream.run_id is None
    assert upstream.is_degraded is True
    assert (
        "data freshness or completeness could not be verified" in upstream.warning_text
    )


def test_unrecognized_orders_sync_status_fails_closed() -> None:
    upstream = build_orders_sync_upstream_context(status="new-profiler-status")

    assert upstream.is_degraded is True


@pytest.mark.parametrize(
    "report_builder",
    [
        daily_sales_pipeline.build_orders_sync_upstream_context,
        pending_deliveries_pipeline.build_orders_sync_upstream_context,
    ],
)
@pytest.mark.parametrize(
    ("status", "is_degraded"),
    [
        ("success", False),
        ("partial", True),
        ("skipped", True),
        ("unknown", True),
        (None, True),
    ],
)
def test_daily_sales_and_pending_deliveries_share_upstream_classification(
    report_builder, status: str | None, is_degraded: bool
) -> None:
    upstream = report_builder(status=status)

    assert upstream.is_degraded is is_degraded
