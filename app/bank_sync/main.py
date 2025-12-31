from __future__ import annotations

from datetime import date

from app.common.date_utils import get_daily_report_date
from app.common.db import session_scope
from app.config import config
from app.dashboard_downloader.json_logger import get_logger, log_event, new_run_id
from app.dashboard_downloader.notifications import send_notifications_for_run

PIPELINE_NAME = "bank_sync"


async def main(
    *,
    run_env: str | None = None,
    run_id: str | None = None,
    run_date: date | None = None,
) -> None:
    """Stub orchestrator for the bank sync pipeline."""

    resolved_run_id = run_id or new_run_id()
    resolved_env = run_env or config.run_env
    resolved_run_date = run_date or get_daily_report_date()
    logger = get_logger(run_id=resolved_run_id)

    log_event(
        logger=logger,
        phase="init",
        message="Starting bank sync stub orchestrator",
        run_env=resolved_env,
        run_date=resolved_run_date,
    )

    async with session_scope(config.database_url) as db_session:
        log_event(
            logger=logger,
            phase="orchestrate",
            message="TODO: implement bank sync orchestration",
            run_env=resolved_env,
            run_date=resolved_run_date,
            db_session=repr(db_session),
        )

    log_event(
        logger=logger,
        phase="notifications",
        message="TODO: wire notifications once pipeline artifacts are recorded",
        run_env=resolved_env,
        run_date=resolved_run_date,
    )
    await send_notifications_for_run(PIPELINE_NAME, resolved_run_id)
