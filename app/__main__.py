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

    def _add_common_report_args(
        report_subparser: argparse.ArgumentParser,
        *,
        include_orders_sync_upstream: bool = False,
    ) -> None:
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
            help="Deprecated no-op; reports always regenerate and append new summaries/documents",
        )
        if include_orders_sync_upstream:
            report_subparser.add_argument(
                "--orders-sync-upstream-status",
                dest="orders_sync_upstream_status",
                type=str,
                default=None,
                help="Status of the upstream orders sync run that preceded this report",
            )
            report_subparser.add_argument(
                "--orders-sync-upstream-run-id",
                dest="orders_sync_upstream_run_id",
                type=str,
                default=None,
                help="Run ID of the upstream orders sync run that preceded this report",
            )

    _add_common_report_args(
        report_subparsers.add_parser("daily-sales", help="Run daily sales report"),
        include_orders_sync_upstream=True,
    )
    _add_common_report_args(
        report_subparsers.add_parser("pending-deliveries", help="Run pending deliveries report"),
        include_orders_sync_upstream=True,
    )
    _add_common_report_args(
        report_subparsers.add_parser("mtd-same-day-fulfillment", help="Run MTD same-day fulfillment report")
    )

    crm_parser = subparsers.add_parser("crm", help="Run CRM maintenance commands")
    crm_subparsers = crm_parser.add_subparsers(dest="crm_command", required=True)
    oli_rebuild = crm_subparsers.add_parser(
        "rebuild-order-line-items",
        help="Rebuild order_line_items from authoritative CRM snapshots",
    )
    oli_rebuild.add_argument("--source", choices=("td", "uc", "both"), required=True)
    oli_rebuild.add_argument("--stores", nargs="*", default=None, help="Optional store codes")
    oli_rebuild.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    oli_rebuild.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    oli_rebuild.add_argument("--window-size", type=int, default=7, help="Window size in days")
    oli_rebuild.add_argument("--dry-run", action="store_true")
    oli_rebuild.add_argument("--run-id", default=None)

    recovery_parser = subparsers.add_parser("recovery", help="Run recovery maintenance commands")
    recovery_subparsers = recovery_parser.add_subparsers(dest="recovery_command", required=True)
    recovery_mark_pending = recovery_subparsers.add_parser(
        "mark-aged-pending-deliveries",
        help="Mark >30 day pending deliveries as TO_BE_RECOVERED",
    )
    recovery_mark_pending.add_argument("--report-date", dest="report_date", type=str, default=None, help="Report date (YYYY-MM-DD)")
    recovery_mark_pending.add_argument("--env", dest="env", type=str, default=None, help="Override run environment")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = list(argv) if argv is not None else sys.argv[1:]

    # Preserve existing CLI behaviour for legacy commands (e.g. run-weekly, db upgrade).
    if args and args[0] not in {"server", "pipeline", "report", "recovery", "crm"}:
        return pipeline_cli.main(args)

    parser = _build_parser()
    parsed = parser.parse_args(args)

    if parsed.command == "server":
        return asyncio.run(_run_server())

    if parsed.command == "pipeline":
        return _run_pipeline(parsed)

    if parsed.command == "crm":
        if parsed.crm_command == "rebuild-order-line-items":
            from app.crm_downloader.order_line_items_rebuild import run as rebuild_run

            rebuild_args = [
                "--source", parsed.source,
                "--start-date", parsed.start_date,
                "--end-date", parsed.end_date,
                "--window-size", str(parsed.window_size),
            ]
            if parsed.stores:
                rebuild_args.append("--stores")
                rebuild_args.extend(parsed.stores)
            if parsed.dry_run:
                rebuild_args.append("--dry-run")
            if parsed.run_id:
                rebuild_args.extend(["--run-id", parsed.run_id])
            rebuild_run(rebuild_args)
            return 0

    if parsed.command == "recovery":
        recovery_args: list[str] = []
        if parsed.report_date:
            recovery_args.extend(["--report-date", parsed.report_date])
        if parsed.env:
            recovery_args.extend(["--env", parsed.env])
        if parsed.recovery_command == "mark-aged-pending-deliveries":
            from app.recovery.main import main as recovery_main

            recovery_main(recovery_args)
            return 0

    if parsed.command == "report":
        report_args: list[str] = []
        if parsed.report_date:
            report_args.extend(["--report-date", parsed.report_date])
        if parsed.env:
            report_args.extend(["--env", parsed.env])
        orders_sync_upstream_status = getattr(parsed, "orders_sync_upstream_status", None)
        orders_sync_upstream_run_id = getattr(parsed, "orders_sync_upstream_run_id", None)
        if orders_sync_upstream_status:
            report_args.extend(["--orders-sync-upstream-status", orders_sync_upstream_status])
        if orders_sync_upstream_run_id:
            report_args.extend(["--orders-sync-upstream-run-id", orders_sync_upstream_run_id])
        # --force is retained at this wrapper for backward compatibility, but
        # operational reports now regenerate on every invocation.

        if parsed.report_command == "daily-sales":
            from app.reports.daily_sales_report.main import main as daily_sales_report_main

            daily_sales_report_main(report_args)
            return 0
        if parsed.report_command == "pending-deliveries":
            from app.reports.pending_deliveries.main import main as pending_deliveries_main

            pending_deliveries_main(report_args)
            return 0
        if parsed.report_command == "mtd-same-day-fulfillment":
            from app.reports.mtd_same_day_fulfillment.main import main as mtd_same_day_fulfillment_main

            mtd_same_day_fulfillment_main(report_args)
            return 0

    parser.error("Unknown command")
    return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
