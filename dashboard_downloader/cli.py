from __future__ import annotations

import argparse
import argparse
import asyncio
import os
from pathlib import Path
from typing import List, Optional

from dashboard_downloader.json_logger import JsonLogger, get_logger, log_event, new_run_id
from dashboard_downloader.run_downloads import LoginBootstrapError
from dashboard_downloader.run_summary import RunAggregator
from dashboard_downloader.settings import PipelineSettings, load_settings

from simplify_downloader.common.db import run_alembic_upgrade


def configure_logging(logger: JsonLogger) -> None:
    """Hook to extend logging configuration if needed."""
    _ = logger


class PrerequisiteValidationError(Exception):
    """Raised when required inputs are missing before a run starts."""


def _validate_prerequisites(*, settings: PipelineSettings, logger: JsonLogger, dry_run: bool) -> None:
    errors: list[str] = []
    if not dry_run and not settings.database_url:
        errors.append("DATABASE_URL must be configured unless --dry_run is supplied")

    for store_code, cfg in settings.stores.items():
        username = (cfg.get("username") or "").strip()
        password = (cfg.get("password") or "").strip()
        if not username or not password:
            errors.append(f"store {store_code}: missing username/password credentials")
        storage_state = cfg.get("storage_state")
        if storage_state:
            storage_path = Path(storage_state)
            if not storage_path.exists():
                errors.append(f"store {store_code}: storage state missing at {storage_path}")
        else:
            errors.append(f"store {store_code}: storage state path not configured")

    if errors:
        for message in errors:
            log_event(logger=logger, phase="prereq", status="error", message=message)
        raise PrerequisiteValidationError("pipeline prerequisites not satisfied")


async def _run_async(args: argparse.Namespace) -> int:
    run_id = args.run_id or new_run_id()
    logger = get_logger(run_id=run_id)
    configure_logging(logger)
    settings = load_settings(
        stores_list=args.stores_list,
        dry_run=args.dry_run,
        run_id=run_id,
    )
    run_env = os.getenv("RUN_ENV") or os.getenv("ENVIRONMENT") or "dev"
    store_codes = list(settings.stores.keys()) if settings.stores else []
    aggregator = RunAggregator(run_id=run_id, run_env=run_env, store_codes=store_codes)
    logger.attach_aggregator(aggregator)

    try:
        _validate_prerequisites(settings=settings, logger=logger, dry_run=args.dry_run)
    except PrerequisiteValidationError:
        return 2

    if getattr(args, "run_migrations", False):
        if not settings.database_url:
            log_event(
                logger=logger,
                phase="db",
                status="warning",
                message="skipping migrations because DATABASE_URL is not configured",
            )
        else:
            log_event(logger=logger, phase="db", message="running migrations")
            await asyncio.to_thread(run_alembic_upgrade, "head")

    from dashboard_downloader.pipeline import run_pipeline

    try:
        await run_pipeline(settings=settings, logger=logger, aggregator=aggregator)
    except LoginBootstrapError as exc:
        log_event(
            logger=logger,
            phase="orchestrator",
            status="error",
            store_code=None,
            bucket=None,
            message="pipeline failed during login bootstrap",
            extras={"error": str(exc), "exc_type": type(exc).__name__},
        )
        return 1
    except Exception as exc:
        log_event(
            logger=logger,
            phase="orchestrator",
            status="error",
            store_code=None,
            bucket=None,
            message="pipeline failed with unexpected error",
            extras={"error": str(exc), "exc_type": type(exc).__name__},
        )
        return 1
    else:
        failure_info = getattr(settings, "single_session_failure", None)
        if failure_info:
            extras = dict(failure_info)
            extras.setdefault("exc_type", "LoginBootstrapError")
            log_event(
                logger=logger,
                phase="orchestrator",
                status="error",
                store_code=None,
                bucket=None,
                message="pipeline failed during login bootstrap",
                extras=extras,
            )
            return 1
        return 0
    finally:
        logger.close()


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="simplify_downloader")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Existing multi-session pipeline
    run_parser = subparsers.add_parser("run", help="Execute full pipeline")
    run_parser.add_argument("--stores_list", type=str, default=None, help="Comma separated store keys")
    run_parser.add_argument("--dry_run", action="store_true", help="Skip DB writes")
    run_parser.add_argument("--run_id", type=str, default=None, help="Override generated run id")
    run_parser.add_argument(
        "--run-migrations",
        action="store_true",
        dest="run_migrations",
        help="Run Alembic migrations before executing the pipeline",
    )

    # NEW: single-session pipeline command
    run_single_parser = subparsers.add_parser(
        "run-single-session",
        help="Execute full pipeline using a single browser session for all stores",
    )
    run_single_parser.add_argument("--stores_list", type=str, default=None, help="Comma separated store keys")
    run_single_parser.add_argument("--dry_run", action="store_true", help="Skip DB writes")
    run_single_parser.add_argument("--run_id", type=str, default=None, help="Override generated run id")
    run_single_parser.add_argument(
        "--run-migrations",
        action="store_true",
        dest="run_migrations",
        help="Run Alembic migrations before executing the pipeline",
    )

    weekly_parser = subparsers.add_parser(
        "run-weekly", help="Execute the weekly reporting pipeline"
    )
    weekly_parser.add_argument("--env", dest="run_env", default=None, help="Override RUN_ENV for summaries")

    monthly_parser = subparsers.add_parser(
        "run-monthly", help="Execute the monthly reporting pipeline"
    )
    monthly_parser.add_argument("--env", dest="run_env", default=None, help="Override RUN_ENV for summaries")

    # DB command as before
    db_parser = subparsers.add_parser("db", help="Database operations")
    db_sub = db_parser.add_subparsers(dest="db_command", required=True)
    upgrade_parser = db_sub.add_parser("upgrade", help="Run Alembic upgrade head")
    upgrade_parser.add_argument("--revision", default="head")
    db_check_parser = db_sub.add_parser("check", help="Validate required tables and notification seeds")

    notifications_parser = subparsers.add_parser(
        "notifications", help="Notification diagnostics"
    )
    notifications_sub = notifications_parser.add_subparsers(
        dest="notifications_command", required=True
    )
    notif_test_parser = notifications_sub.add_parser("test", help="Validate SMTP/profiles/docs for a run")
    notif_test_parser.add_argument("--pipeline", required=True, help="Pipeline code (e.g. simplify_dashboard_daily)")
    notif_test_parser.add_argument("--run-id", required=True, help="Existing run_id to inspect")

    args = parser.parse_args(argv)

    if args.command == "run":
        # existing behaviour — now single-session under the hood
        return asyncio.run(_run_async(args))

    if args.command == "run-single-session":
        return asyncio.run(_run_async(args))

    if args.command == "run-weekly":
        from tsv_dashboard.pipelines import dashboard_weekly

        dashboard_weekly.run_pipeline(env=args.run_env)
        return 0

    if args.command == "run-monthly":
        from tsv_dashboard.pipelines import dashboard_monthly

        dashboard_monthly.run_pipeline(env=args.run_env)
        return 0

    if args.command == "db" and args.db_command == "upgrade":
        revision = args.revision
        os.environ.setdefault("ALEMBIC_CONFIG", "alembic.ini")
        run_alembic_upgrade(revision)
        return 0

    if args.command == "db" and args.db_command == "check":
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            print("DATABASE_URL must be configured for db check")
            return 1
        from dashboard_downloader.db_health import check_database_health

        errors = asyncio.run(check_database_health(database_url))
        if errors:
            for issue in errors:
                print(f"[db-check] {issue}")
            return 1
        print("[db-check] database looks healthy")
        return 0

    if args.command == "notifications" and args.notifications_command == "test":
        from dashboard_downloader.notifications import diagnose_notification_run

        findings = asyncio.run(diagnose_notification_run(args.pipeline, args.run_id))
        if findings:
            for issue in findings:
                print(f"[notifications] {issue}")
            return 1
        print("[notifications] all prerequisites satisfied")
        return 0

    parser.error("Unknown command")
    return 1
