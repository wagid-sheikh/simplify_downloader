from __future__ import annotations

import runpy
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

import app.__main__ as app_main


def test_report_cli_includes_mtd_same_day_fulfillment_subcommand() -> None:
    parser = app_main._build_parser()
    parsed = parser.parse_args(["report", "mtd-same-day-fulfillment"])

    assert parsed.command == "report"
    assert parsed.report_command == "mtd-same-day-fulfillment"


def test_report_cli_invokes_mtd_same_day_fulfillment_runner_with_common_args(
    monkeypatch,
) -> None:
    captured: list[list[str] | None] = []

    def _fake_runner(argv: list[str] | None = None) -> None:
        captured.append(argv)

    monkeypatch.setattr("app.reports.mtd_same_day_fulfillment.main.main", _fake_runner)

    exit_code = app_main.main(
        [
            "report",
            "mtd-same-day-fulfillment",
            "--report-date",
            "2026-04-29",
            "--env",
            "prod",
            "--force",
        ]
    )

    assert exit_code == 0
    assert captured == [["--report-date", "2026-04-29", "--env", "prod"]]


def test_report_cli_invokes_daily_sales_runner_with_common_args(monkeypatch) -> None:
    captured: list[list[str] | None] = []

    def _fake_runner(argv: list[str] | None = None) -> None:
        captured.append(argv)

    monkeypatch.setattr("app.reports.daily_sales_report.main.main", _fake_runner)

    exit_code = app_main.main(["report", "daily-sales", "--env", "stage"])

    assert exit_code == 0
    assert captured == [["--env", "stage"]]


def test_report_cli_invokes_pending_deliveries_runner_with_upstream_args(
    monkeypatch,
) -> None:
    captured: list[list[str] | None] = []

    def _fake_runner(argv: list[str] | None = None) -> None:
        captured.append(argv)

    monkeypatch.setattr("app.reports.pending_deliveries.main.main", _fake_runner)

    exit_code = app_main.main(
        [
            "report",
            "pending-deliveries",
            "--report-date",
            "2026-04-29",
            "--env",
            "prod",
            "--orders-sync-upstream-status",
            "failed",
            "--orders-sync-upstream-run-id",
            "orders-run-1",
        ]
    )

    assert exit_code == 0
    assert captured == [
        [
            "--report-date",
            "2026-04-29",
            "--env",
            "prod",
            "--orders-sync-upstream-status",
            "failed",
            "--orders-sync-upstream-run-id",
            "orders-run-1",
        ]
    ]


def test_recovery_cli_invokes_runner_with_common_args(monkeypatch) -> None:
    captured: list[list[str] | None] = []

    def _fake_runner(argv: list[str] | None = None) -> None:
        captured.append(argv)

    monkeypatch.setattr("app.recovery.main.main", _fake_runner)

    exit_code = app_main.main(
        [
            "recovery",
            "mark-aged-pending-deliveries",
            "--report-date",
            "2026-04-30",
            "--env",
            "dev",
        ]
    )

    assert exit_code == 0
    assert captured == [["--report-date", "2026-04-30", "--env", "dev"]]


def test_crm_order_line_items_rebuild_returns_nonzero_when_runner_exits_nonzero(
    monkeypatch,
) -> None:
    def _fake_runner(argv: list[str] | None = None) -> None:
        raise SystemExit(1)

    monkeypatch.setattr("app.crm_downloader.order_line_items_rebuild.run", _fake_runner)

    exit_code = app_main.main(["crm", "rebuild-order-line-items", "--source", "td"])

    assert exit_code == 1


def test_crm_order_line_items_rebuild_aliases_forward_to_rebuild_runner(
    monkeypatch,
) -> None:
    captured: list[list[str] | None] = []

    def _fake_runner(argv: list[str] | None = None) -> None:
        captured.append(argv)

    monkeypatch.setattr("app.crm_downloader.order_line_items_rebuild.run", _fake_runner)

    exit_code = app_main.main(
        [
            "crm",
            "order-line-items-rebuild",
            "--source",
            "uc",
            "--from-date",
            "2025-01-01",
            "--to-date",
            "2025-02-15",
            "--window-days",
            "45",
            "--stores",
            "UC001",
            "--dry-run",
            "--resume",
            "--run-id",
            "alias-run",
        ]
    )

    assert exit_code == 0
    assert captured == [
        [
            "--source",
            "uc",
            "--end-date",
            "2025-02-15",
            "--start-date",
            "2025-01-01",
            "--window-size",
            "45",
            "--stores",
            "UC001",
            "--dry-run",
            "--resume",
            "--run-id",
            "alias-run",
        ]
    ]


def test_pending_deliveries_direct_cli_accepts_upstream_args(monkeypatch) -> None:
    import app.reports.pending_deliveries.main as pending_main

    captured: list[dict[str, object]] = []

    def _fake_run_pipeline(**kwargs) -> None:
        captured.append(kwargs)

    monkeypatch.setattr(pending_main, "run_pipeline", _fake_run_pipeline)

    pending_main.main(
        [
            "--report-date",
            "2026-04-29",
            "--env",
            "prod",
            "--orders-sync-upstream-status",
            "FAILED",
            "--orders-sync-upstream-run-id",
            "orders-run-1",
        ]
    )

    assert captured == [
        {
            "report_date": pending_main.date(2026, 4, 29),
            "env": "prod",
            "force": False,
            "orders_sync_upstream_status": "FAILED",
            "orders_sync_upstream_run_id": "orders-run-1",
        }
    ]


def test_pending_deliveries_cli_forwards_degraded_upstream_context_end_to_end(
    monkeypatch,
) -> None:
    import app.reports.pending_deliveries.main as pending_main
    import app.reports.pending_deliveries.pipeline as pending_pipeline

    captured: list[dict[str, object]] = []

    async def _fake_run(
        report_date,
        env,
        force,
        orders_sync_upstream_status=None,
        orders_sync_upstream_run_id=None,
    ) -> None:
        captured.append(
            {
                "report_date": report_date,
                "env": env,
                "force": force,
                "orders_sync_upstream_status": orders_sync_upstream_status,
                "orders_sync_upstream_run_id": orders_sync_upstream_run_id,
            }
        )

    monkeypatch.setattr(pending_main, "get_timezone", lambda: ZoneInfo("UTC"))
    monkeypatch.setattr(
        pending_main,
        "aware_now",
        lambda _timezone: datetime(2026, 5, 30, tzinfo=ZoneInfo("UTC")),
    )
    monkeypatch.setattr(pending_pipeline, "_run", _fake_run)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "python",
            "report",
            "pending-deliveries",
            "--env",
            "prod",
            "--orders-sync-upstream-status",
            "success_with_warnings",
            "--orders-sync-upstream-run-id",
            "test-run",
        ],
    )

    with pytest.warns(RuntimeWarning, match="found in sys.modules"):
        with pytest.raises(SystemExit) as exc_info:
            runpy.run_module("app.__main__", run_name="__main__")

    assert exc_info.value.code == 0
    assert captured == [
        {
            "report_date": pending_main.date(2026, 5, 30),
            "env": "prod",
            "force": False,
            "orders_sync_upstream_status": "success_with_warnings",
            "orders_sync_upstream_run_id": "test-run",
        }
    ]
