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

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = list(argv) if argv is not None else sys.argv[1:]

    # Preserve existing CLI behaviour for legacy commands (e.g. run-weekly, db upgrade).
    if args and args[0] not in {"server", "pipeline"}:
        return pipeline_cli.main(args)

    parser = _build_parser()
    parsed = parser.parse_args(args)

    if parsed.command == "server":
        return asyncio.run(_run_server())

    if parsed.command == "pipeline":
        return _run_pipeline(parsed)

    parser.error("Unknown command")
    return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
