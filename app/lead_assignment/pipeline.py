from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

from sqlalchemy import bindparam, text

from app.common.db import session_scope
from app.config import config
from app.dashboard_downloader.json_logger import get_logger, log_event, new_run_id
from app.dashboard_downloader.notifications import (
    EmailPlan,
    _collect_recipient_lists,
    _load_smtp_config,
    _render_template,
    _send_email,
)
from app.dashboard_downloader.pipelines.base import resolve_run_env

from .assigner import run_leads_assignment
from .pdf_generator import generate_pdfs_for_batch


async def _count_assignments(db_session, batch_id: int) -> int:
    result = await db_session.execute(
        text(
            """
            SELECT COUNT(*)
            FROM lead_assignments
            WHERE assignment_batch_id = :batch_id
            """
        ),
        {"batch_id": batch_id},
    )
    return int(result.scalar_one() or 0)


async def _load_documents(db_session, document_ids: list[int]) -> dict[str, list[Path]]:
    if not document_ids:
        return {}

    query = text(
        """
        SELECT id, reference_id_2 AS store_code, file_path
        FROM documents
        WHERE id IN :document_ids
        """
    ).bindparams(bindparam("document_ids", expanding=True))
    rows = await db_session.execute(query, {"document_ids": document_ids})

    grouped: dict[str, list[Path]] = {}
    for row in rows:
        if not row.file_path:
            continue
        store_code = (row.store_code or "").upper()
        if not store_code:
            continue
        grouped.setdefault(store_code, []).append(Path(row.file_path))
    return grouped


async def _load_store_names(db_session, store_codes: Iterable[str]) -> dict[str, str]:
    codes = list(store_codes)
    if not codes:
        return {}

    query = text(
        """
        SELECT upper(store_code) AS store_code, store_name
        FROM store_master
        WHERE upper(store_code) IN :store_codes
        """
    ).bindparams(bindparam("store_codes", expanding=True))
    rows = await db_session.execute(query, {"store_codes": codes})
    return {row.store_code: row.store_name for row in rows}


async def _load_notification_resources(db_session, run_env: str):
    profile_row = (
        await db_session.execute(
            text(
                """
                SELECT id, code, attach_mode
                FROM notification_profiles
                WHERE code = :code
                  AND is_active = true
                  AND env IN ('any', :env)
                ORDER BY CASE WHEN env = :env THEN 0 ELSE 1 END
                LIMIT 1
                """
            ),
            {"code": "leads_assignment", "env": run_env},
        )
    ).mappings().first()
    if not profile_row:
        return None, None, []

    template_row = (
        await db_session.execute(
            text(
                """
                SELECT subject_template, body_template
                FROM email_templates
                WHERE profile_id = :profile_id
                  AND is_active = true
                  AND name = 'default'
                LIMIT 1
                """
            ),
            {"profile_id": profile_row["id"]},
        )
    ).mappings().first()

    recipients_rows = (
        await db_session.execute(
            text(
                """
                SELECT store_code, email_address, display_name, send_as
                FROM notification_recipients
                WHERE profile_id = :profile_id
                  AND is_active = true
                  AND env IN ('any', :env)
                """
            ),
            {"profile_id": profile_row["id"], "env": run_env},
        )
    ).mappings().all()

    return profile_row, template_row, [dict(row) for row in recipients_rows]


def _send_store_notifications(
    *,
    profile: dict[str, object],
    template: dict[str, str],
    recipients: list[dict[str, object]],
    store_documents: dict[str, list[Path]],
    store_names: dict[str, str],
    run_env: str,
    batch_id: int,
    logger,
) -> tuple[int, int]:
    smtp_config = _load_smtp_config()
    sent = 0
    planned = 0

    for store_code in sorted(store_documents):
        attachments = [path for path in store_documents[store_code] if path.exists()]
        if not attachments:
            log_event(
                logger=logger,
                phase="notify",
                status="warn",
                message="no attachments found for store",
                extras={"store_code": store_code, "batch_id": batch_id},
            )
            continue

        to, cc, bcc = _collect_recipient_lists(recipients, store_code=store_code)
        if not to and cc:
            to, cc = cc, []
        if not to and not cc:
            log_event(
                logger=logger,
                phase="notify",
                status="warn",
                message="no active recipients for store",
                extras={"store_code": store_code, "batch_id": batch_id},
            )
            continue

        context = {
            "store_code": store_code,
            "store_name": store_names.get(store_code, store_code),
            "run_env": run_env,
            "batch_id": batch_id,
        }
        subject = _render_template(template["subject_template"], context)
        body = _render_template(template["body_template"], context)
        planned += 1

        plan = EmailPlan(
            profile_code=profile["code"],
            scope="store",
            store_code=store_code,
            subject=subject,
            body=body,
            to=to,
            cc=cc,
            bcc=bcc,
            attachments=attachments,
        )

        if _send_email(smtp_config, plan):
            sent += 1
            log_event(
                logger=logger,
                phase="notify",
                status="ok",
                message="notification email sent",
                extras={"store_code": store_code, "batch_id": batch_id},
            )
        else:
            log_event(
                logger=logger,
                phase="notify",
                status="warn",
                message="failed to send notification email",
                extras={"store_code": store_code, "batch_id": batch_id},
            )

    return planned, sent


async def run_leads_assignment_pipeline(env: str | None = None, run_id: str | None = None) -> None:
    logger = get_logger(run_id=run_id or new_run_id())
    run_env = resolve_run_env(env)

    log_event(
        logger=logger,
        phase="orchestrator",
        message="starting leads assignment pipeline",
        extras={"run_env": run_env},
    )

    database_url = config.database_url
    async with session_scope(database_url) as session:
        batch_id = await run_leads_assignment(
            db_session=session, triggered_by="pipeline", run_id=logger.run_id
        )
        assignment_count = await _count_assignments(session, batch_id)
        log_event(
            logger=logger,
            phase="lead_assignment",
            message="assignment batch created",
            extras={"batch_id": batch_id, "assigned_count": assignment_count},
        )

        if assignment_count == 0:
            log_event(
                logger=logger,
                phase="orchestrator",
                status="warn",
                message="no assignments created; skipping pdf generation and notifications",
                extras={"batch_id": batch_id},
            )
            logger.close()
            return

        async with session.begin():
            document_ids = await generate_pdfs_for_batch(session, batch_id)
        log_event(
            logger=logger,
            phase="documents",
            message="generated assignment documents",
            extras={"batch_id": batch_id, "document_count": len(document_ids)},
        )

        store_documents = await _load_documents(session, document_ids)
        store_names = await _load_store_names(session, store_documents.keys())
        profile, template, recipients = await _load_notification_resources(
            session, run_env
        )

    if not document_ids or not store_documents:
        log_event(
            logger=logger,
            phase="notify",
            status="warn",
            message="no documents available for notification",
            extras={"batch_id": batch_id},
        )
        logger.close()
        return

    if not profile or not template:
        log_event(
            logger=logger,
            phase="notify",
            status="warn",
            message="notification profile or template missing",
            extras={"batch_id": batch_id, "run_env": run_env},
        )
        logger.close()
        return

    planned, sent = _send_store_notifications(
        profile=profile,
        template=template,
        recipients=recipients,
        store_documents=store_documents,
        store_names=store_names,
        run_env=run_env,
        batch_id=batch_id,
        logger=logger,
    )
    log_event(
        logger=logger,
        phase="notify",
        message="notification dispatch complete",
        extras={"batch_id": batch_id, "emails_planned": planned, "emails_sent": sent},
    )

    log_event(
        logger=logger,
        phase="orchestrator",
        message="leads assignment pipeline complete",
        extras={"batch_id": batch_id},
    )
    logger.close()


def run_pipeline(env: str | None = None, run_id: str | None = None) -> None:
    import asyncio

    asyncio.run(run_leads_assignment_pipeline(env=env, run_id=run_id))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the leads assignment pipeline")
    parser.add_argument("--env", dest="env", default=None, help="Override RUN_ENV for the pipeline")
    parser.add_argument("--run-id", dest="run_id", default=None, help="Override generated run id")
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    run_pipeline(env=args.env, run_id=args.run_id)


if __name__ == "__main__":
    main()
