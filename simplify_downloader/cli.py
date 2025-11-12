from __future__ import annotations

import argparse
import asyncio
import os
from typing import List, Optional

from downloader.json_logger import JsonLogger, get_logger, new_run_id

from .pipeline import run_pipeline
from .settings import load_settings
from .utils import configure_logging


async def _run_async(args: argparse.Namespace) -> int:
    run_id = args.run_id or new_run_id()
    logger = get_logger(run_id=run_id)
    configure_logging(logger)
    settings = load_settings(
        stores_list=args.stores_list,
        dry_run=args.dry_run,
        run_id=run_id,
    )
    await run_pipeline(settings=settings, logger=logger)
    logger.close()
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="simplify_downloader")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Execute full pipeline")
    run_parser.add_argument("--stores_list", type=str, default=None, help="Comma separated store keys")
    run_parser.add_argument("--dry_run", action="store_true", help="Skip DB writes")
    run_parser.add_argument("--run_id", type=str, default=None, help="Override generated run id")

    db_parser = subparsers.add_parser("db", help="Database operations")
    db_sub = db_parser.add_subparsers(dest="db_command", required=True)
    upgrade_parser = db_sub.add_parser("upgrade", help="Run Alembic upgrade head")
    upgrade_parser.add_argument("--revision", default="head")

    args = parser.parse_args(argv)

    if args.command == "run":
        return asyncio.run(_run_async(args))
    if args.command == "db" and args.db_command == "upgrade":
        revision = args.revision
        os.environ.setdefault("ALEMBIC_CONFIG", "alembic.ini")
        from .db import run_alembic_upgrade

        run_alembic_upgrade(revision)
        return 0
    parser.error("Unknown command")
    return 1
