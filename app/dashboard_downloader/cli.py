from __future__ import annotations
from __future__ import annotations

import argparse
import asyncio
from typing import List, Optional, TYPE_CHECKING

from app.dashboard_downloader.json_logger import JsonLogger, get_logger, log_event, new_run_id
from app.common.db import run_alembic_upgrade

if TYPE_CHECKING:  # pragma: no cover - import cycles avoided at runtime
    from app.dashboard_downloader.run_summary import RunAggregator
    from app.dashboard_downloader.settings import PipelineSettings
    from app.config import Config


def configure_logging(logger: JsonLogger) -> None:
    """Hook to extend logging configuration if needed."""

    _ = logger


class PrerequisiteValidationError(Exception):
    """Raised when required inputs are missing before a run starts."""


def _validate_prerequisites(
    *,
    settings: PipelineSettings,
    logger: JsonLogger,
    app_config: Config,
    credential_error: str,
) -> None:
    errors: list[str] = []
    if not settings.stores:
        errors.append("No stores are flagged for ETL")

    if not settings.global_username or not settings.global_password:
        errors.append(credential_error)

    if not app_config.td_base_url:
        errors.append("TD_BASE_URL is required")
    if not app_config.tms_base:
        errors.append("TMS_BASE is required")
    if not app_config.td_login_url:
        errors.append("TD_LOGIN_URL is required")
    if not app_config.td_store_dashboard_path:
        errors.append("TD_STORE_DASHBOARD_PATH is required")

    if errors:
        for message in errors:
            log_event(logger=logger, phase="prereq", status="error", message=message)
        raise PrerequisiteValidationError("pipeline prerequisites not satisfied")


async def _run_async(args: argparse.Namespace) -> int:
    from app.config import config as runtime_config
    from app.dashboard_downloader.run_downloads import LoginBootstrapError
    from app.dashboard_downloader.run_summary import RunAggregator
    from app.dashboard_downloader.settings import GLOBAL_CREDENTIAL_ERROR, load_settings

    run_id = args.run_id or new_run_id()
    logger = get_logger(run_id=run_id)
    configure_logging(logger)
    try:
        settings = await load_settings(dry_run=args.dry_run, run_id=run_id)
    except ValueError as exc:
        log_event(
            logger=logger,
            phase="prereq",
            status="error",
            message=str(exc),
        )
        logger.close()
        return 2

    run_env = runtime_config.run_env or runtime_config.environment
    store_codes = list(settings.stores.keys()) if settings.stores else []
    aggregator: RunAggregator = RunAggregator(run_id=run_id, run_env=run_env, store_codes=store_codes)
    logger.attach_aggregator(aggregator)

    try:
        _validate_prerequisites(
            settings=settings,
            logger=logger,
            app_config=runtime_config,
            credential_error=GLOBAL_CREDENTIAL_ERROR,
        )
    except PrerequisiteValidationError:
        return 2

    if getattr(args, "run_migrations", False):
        log_event(logger=logger, phase="db", message="running migrations")
        await asyncio.to_thread(
            run_alembic_upgrade,
            revision="head",
            database_url=runtime_config.database_url,
            alembic_config_path=runtime_config.alembic_config,
        )

    from app.dashboard_downloader.pipeline import run_pipeline
    from app.lead_assignment.pipeline import run_leads_assignment_pipeline

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
        try:
            log_event(
                logger=logger,
                phase="orchestrator",
                message="starting leads assignment tail step",
                extras={"run_env": run_env},
            )
            await run_leads_assignment_pipeline(env=run_env, run_id=run_id)
            log_event(
                logger=logger,
                phase="orchestrator",
                status="info",
                message="leads assignment pipeline completed",
                extras={"run_env": run_env},
            )
        except Exception as exc:
            log_event(
                logger=logger,
                phase="orchestrator",
                status="error",
                message="leads assignment pipeline failed",
                extras={"error": str(exc)},
            )
        return 0
    finally:
        logger.close()


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="app")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Execute full pipeline")
    run_parser.add_argument("--dry_run", action="store_true", help="Skip DB writes")
    run_parser.add_argument("--run_id", type=str, default=None, help="Override generated run id")
    run_parser.add_argument(
        "--run-migrations",
        action="store_true",
        dest="run_migrations",
        help="Run Alembic migrations before executing the pipeline",
    )

    run_single_parser = subparsers.add_parser(
        "run-single-session",
        help="Execute full pipeline using a single browser session for all stores",
    )
    run_single_parser.add_argument("--dry_run", action="store_true", help="Skip DB writes")
    run_single_parser.add_argument("--run_id", type=str, default=None, help="Override generated run id")
    run_single_parser.add_argument(
        "--run-migrations",
        action="store_true",
        dest="run_migrations",
        help="Run Alembic migrations before executing the pipeline",
    )

    weekly_parser = subparsers.add_parser("run-weekly", help="Execute the weekly reporting pipeline")
    weekly_parser.add_argument("--env", dest="run_env", default=None, help="Override RUN_ENV for summaries")

    monthly_parser = subparsers.add_parser("run-monthly", help="Execute the monthly reporting pipeline")
    monthly_parser.add_argument("--env", dest="run_env", default=None, help="Override RUN_ENV for summaries")

    db_parser = subparsers.add_parser("db", help="Database operations")
    db_sub = db_parser.add_subparsers(dest="db_command", required=True)
    upgrade_parser = db_sub.add_parser("upgrade", help="Run Alembic upgrade head")
    upgrade_parser.add_argument("--revision", default="head")
    db_sub.add_parser("check", help="Validate required tables and notification seeds")

    notifications_parser = subparsers.add_parser("notifications", help="Notification diagnostics")
    notifications_sub = notifications_parser.add_subparsers(dest="notifications_command", required=True)
    notif_test_parser = notifications_sub.add_parser("test", help="Validate SMTP/profiles/docs for a run")
    notif_test_parser.add_argument("--pipeline", required=True, help="Pipeline code (e.g. dashboard_daily)")
    notif_test_parser.add_argument("--run-id", required=True, help="Existing run_id to inspect")

    args = parser.parse_args(argv)

    if args.command in {"run", "run-single-session"}:
        return asyncio.run(_run_async(args))

    if args.command == "run-weekly":
        from app.config import config as runtime_config
        from app.dashboard_downloader.pipelines import dashboard_weekly

        dashboard_weekly.run_pipeline(env=args.run_env or runtime_config.run_env)
        return 0

    if args.command == "run-monthly":
        from app.config import config as runtime_config
        from app.dashboard_downloader.pipelines import dashboard_monthly

        dashboard_monthly.run_pipeline(env=args.run_env or runtime_config.run_env)
        return 0

    if args.command == "db" and args.db_command == "upgrade":
        from app.config import config as runtime_config

        run_alembic_upgrade(
            revision=args.revision,
            database_url=runtime_config.database_url,
            alembic_config_path=runtime_config.alembic_config,
        )
        return 0

    if args.command == "db" and args.db_command == "check":
        from app.config import config as runtime_config
        from app.dashboard_downloader.db_health import check_database_health

        errors = asyncio.run(check_database_health(runtime_config.database_url))
        if errors:
            for issue in errors:
                print(f"[db-check] {issue}")
            return 1
        print("[db-check] database looks healthy")
        return 0

    if args.command == "notifications" and args.notifications_command == "test":
        from app.dashboard_downloader.notifications import diagnose_notification_run

        findings = asyncio.run(diagnose_notification_run(args.pipeline, args.run_id))
        if findings:
            for issue in findings:
                print(f"[notifications] {issue}")
            return 1
        print("[notifications] all prerequisites satisfied")
        return 0

    parser.error("Unknown command")
    return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
