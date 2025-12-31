from __future__ import annotations

from datetime import date

from app.common.date_utils import get_daily_report_date
from app.common.db import session_scope
from app.config import config
from app.dashboard_downloader.json_logger import get_logger, log_event, new_run_id
from app.dashboard_downloader.notifications import send_notifications_for_run

PIPELINE_NAME = "td_orders_sync"


async def main(
    *,
    run_env: str | None = None,
    run_id: str | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> None:
    """Stub orchestrator for the TD orders sync pipeline."""

    resolved_run_id = run_id or new_run_id()
    resolved_env = run_env or config.run_env
    run_start_date = from_date or get_daily_report_date()
    run_end_date = to_date or run_start_date
    logger = get_logger(run_id=resolved_run_id)

    log_event(
        logger=logger,
        phase="init",
        message="Starting TD orders sync stub orchestrator",
        run_env=resolved_env,
        from_date=run_start_date,
        to_date=run_end_date,
    )

    async with session_scope(config.database_url) as db_session:
        log_event(
            logger=logger,
            phase="orchestrate",
            message="TODO: implement TD orders sync orchestration",
            run_env=resolved_env,
            from_date=run_start_date,
            to_date=run_end_date,
            db_session=repr(db_session),
        )

    log_event(
        logger=logger,
        phase="notifications",
        message="TODO: wire notifications once pipeline artifacts are recorded",
        run_env=resolved_env,
    )
    await send_notifications_for_run(PIPELINE_NAME, resolved_run_id)
