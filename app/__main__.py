from __future__ import annotations

import argparse
import asyncio
import signal
import sys
from typing import Sequence

from app.dashboard_downloader import cli as pipeline_cli


async def _run_server() -> int:
    """Run in idle server mode until terminated."""

    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            # Some environments (e.g. Windows) do not support custom signal handlers.
            pass

    print("[app] Running in server mode. Waiting for pipeline invocations...", flush=True)
    await stop_event.wait()
    print("[app] Shutdown signal received. Exiting server mode.", flush=True)
    return 0


def _run_pipeline(args: argparse.Namespace) -> int:
    pipeline_args: list[str] = ["run-single-session"]
    if args.dry_run:
        pipeline_args.append("--dry_run")
    if args.run_id:
        pipeline_args.extend(["--run_id", args.run_id])
    if args.run_migrations:
        pipeline_args.append("--run-migrations")

    return pipeline_cli.main(pipeline_args)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="app", description="Application entrypoint")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("server", help="Run in idle server mode (does not start pipelines)")

    pipeline_parser = subparsers.add_parser(
        "pipeline", help="Run the single-session dashboard pipeline once and exit"
    )
    pipeline_parser.add_argument("--dry-run", dest="dry_run", action="store_true", help="Skip DB writes")
    pipeline_parser.add_argument("--run-id", dest="run_id", type=str, default=None, help="Override generated run id")
    pipeline_parser.add_argument(
        "--run-migrations",
        dest="run_migrations",
        action="store_true",
        help="Run Alembic migrations before executing the pipeline",
    )

    report_parser = subparsers.add_parser("report", help="Run report pipelines")
    report_subparsers = report_parser.add_subparsers(dest="report_command", required=True)

    def _add_common_report_args(report_subparser: argparse.ArgumentParser) -> None:
        report_subparser.add_argument(
            "--report-date",
            dest="report_date",
            type=str,
            default=None,
            help="Report date (YYYY-MM-DD)",
        )
        report_subparser.add_argument("--env", dest="env", type=str, default=None, help="Override run environment")
        report_subparser.add_argument(
            "--force",
            dest="force",
            action="store_true",
            help="Re-generate the report even if a successful run already exists",
        )

    _add_common_report_args(report_subparsers.add_parser("daily-sales", help="Run daily sales report"))
    _add_common_report_args(
        report_subparsers.add_parser("pending-deliveries", help="Run pending deliveries report")
    )
    _add_common_report_args(
        report_subparsers.add_parser("mtd-same-day-fulfillment", help="Run MTD same-day fulfillment report")
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = list(argv) if argv is not None else sys.argv[1:]

    # Preserve existing CLI behaviour for legacy commands (e.g. run-weekly, db upgrade).
    if args and args[0] not in {"server", "pipeline", "report"}:
        return pipeline_cli.main(args)

    parser = _build_parser()
    parsed = parser.parse_args(args)

    if parsed.command == "server":
        return asyncio.run(_run_server())

    if parsed.command == "pipeline":
        return _run_pipeline(parsed)

    if parsed.command == "report":
        report_args: list[str] = []
        if parsed.report_date:
            report_args.extend(["--report-date", parsed.report_date])
        if parsed.env:
            report_args.extend(["--env", parsed.env])
        if parsed.force:
            report_args.append("--force")

        if parsed.report_command == "daily-sales":
            from app.reports.daily_sales_report.main import main as daily_sales_report_main

            sys.argv = ["daily_sales_report", *report_args]
            daily_sales_report_main()
            return 0
        if parsed.report_command == "pending-deliveries":
            from app.reports.pending_deliveries.main import main as pending_deliveries_main

            sys.argv = ["pending_deliveries_report", *report_args]
            pending_deliveries_main()
            return 0
        if parsed.report_command == "mtd-same-day-fulfillment":
            from app.reports.mtd_same_day_fulfillment.main import main as mtd_same_day_fulfillment_main

            sys.argv = ["mtd_same_day_fulfillment_report", *report_args]
            mtd_same_day_fulfillment_main()
            return 0

    parser.error("Unknown command")
    return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
