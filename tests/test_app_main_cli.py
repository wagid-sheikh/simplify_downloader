from __future__ import annotations

import app.__main__ as app_main


def test_report_cli_includes_mtd_same_day_fulfillment_subcommand() -> None:
    parser = app_main._build_parser()
    parsed = parser.parse_args(["report", "mtd-same-day-fulfillment"])

    assert parsed.command == "report"
    assert parsed.report_command == "mtd-same-day-fulfillment"


def test_report_cli_invokes_mtd_same_day_fulfillment_runner_with_common_args(monkeypatch) -> None:
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


def test_report_cli_invokes_pending_deliveries_runner_with_upstream_args(monkeypatch) -> None:
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
