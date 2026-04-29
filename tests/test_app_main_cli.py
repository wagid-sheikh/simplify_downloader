from __future__ import annotations

import app.__main__ as app_main


def test_report_cli_includes_mtd_same_day_fulfillment_subcommand() -> None:
    parser = app_main._build_parser()
    parsed = parser.parse_args(["report", "mtd-same-day-fulfillment"])

    assert parsed.command == "report"
    assert parsed.report_command == "mtd-same-day-fulfillment"


def test_report_cli_invokes_mtd_same_day_fulfillment_runner_with_common_args(monkeypatch) -> None:
    captured: list[list[str]] = []

    def _fake_runner() -> None:
        captured.append(list(app_main.sys.argv))

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
    assert captured == [
        [
            "mtd_same_day_fulfillment_report",
            "--report-date",
            "2026-04-29",
            "--env",
            "prod",
            "--force",
        ]
    ]
