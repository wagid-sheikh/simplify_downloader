from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Iterable

from sqlalchemy import bindparam, text

from app.common.db import session_scope
from app.config import config
from app.dashboard_downloader.pipelines.base import resolve_run_env
from app.dashboard_downloader.notifications import _collect_recipient_lists

STORE_CODES = [
    "TS86",
    "TS36",
    "A696",
    "TS81",
    "TS74",
    "TS71",
    "T997",
    "B002",
    "TS27",
    "A526",
    "A276",
]


@dataclass
class StoreDiagnosis:
    store_code: str
    assignments: int
    eligible_leads: int
    documents: int
    missing_files: int
    has_recipients: bool
    reasons: list[str]


async def _load_batch(db_session, run_id: str):
    result = await db_session.execute(
        text(
            """
            SELECT id, batch_date, created_at
            FROM lead_assignment_batches
            WHERE run_id = :run_id
            ORDER BY id DESC
            LIMIT 1
            """
        ),
        {"run_id": run_id},
    )
    return result.mappings().first()


async def _load_assignments(db_session, batch_id: int, store_codes: Iterable[str]) -> dict[str, int]:
    result = await db_session.execute(
        text(
            """
            SELECT store_code, COUNT(*) AS assignment_count
            FROM lead_assignments
            WHERE assignment_batch_id = :batch_id
              AND store_code IN :store_codes
            GROUP BY store_code
            """
        ).bindparams(bindparam("store_codes", expanding=True)),
        {"batch_id": batch_id, "store_codes": list(store_codes)},
    )
    return {row.store_code: int(row.assignment_count) for row in result}


async def _load_eligible_leads(db_session, store_codes: Iterable[str]) -> dict[str, int]:
    result = await db_session.execute(
        text(
            """
            SELECT
                ml.store_code,
                COUNT(*) AS eligible_count
            FROM missed_leads ml
            JOIN store_master sm ON sm.store_code = ml.store_code AND sm.assign_leads = true
            JOIN store_lead_assignment_map slam
              ON slam.store_code = ml.store_code AND slam.is_enabled = true
            JOIN agents_master am ON am.id = slam.agent_id AND am.is_active = true
            WHERE ml.customer_type = 'New'
              AND ml.lead_assigned = false
              AND (ml.is_order_placed = false OR ml.is_order_placed IS NULL)
              AND ml.store_code IN :store_codes
            GROUP BY ml.store_code
            """
        ).bindparams(bindparam("store_codes", expanding=True)),
        {"store_codes": list(store_codes)},
    )
    return {row.store_code: int(row.eligible_count) for row in result}


async def _load_documents(
    db_session,
    *,
    batch_date,
    created_at,
    store_codes: Iterable[str],
) -> dict[str, list[Path]]:
    end_time = created_at + timedelta(minutes=30)
    result = await db_session.execute(
        text(
            """
            SELECT reference_id_2 AS store_code, file_path
            FROM documents
            WHERE doc_type = 'leads_assignment'
              AND doc_subtype = 'per_store_agent_pdf'
              AND created_by = 'leads_assignment_pipeline'
              AND doc_date = :batch_date
              AND created_at BETWEEN :start_time AND :end_time
              AND reference_id_2 IN :store_codes
            """
        ).bindparams(bindparam("store_codes", expanding=True)),
        {
            "batch_date": batch_date,
            "start_time": created_at,
            "end_time": end_time,
            "store_codes": list(store_codes),
        },
    )

    grouped: dict[str, list[Path]] = {}
    for row in result:
        if not row.file_path:
            continue
        grouped.setdefault(row.store_code, []).append(Path(row.file_path))
    return grouped


async def _load_recipients(db_session, run_env: str) -> list[dict[str, object]]:
    profile_row = (
        await db_session.execute(
            text(
                """
                SELECT id
                FROM notification_profiles
                WHERE code = 'leads_assignment'
                  AND is_active = true
                  AND env IN ('any', :env)
                ORDER BY CASE WHEN env = :env THEN 0 ELSE 1 END
                LIMIT 1
                """
            ),
            {"env": run_env},
        )
    ).mappings().first()
    if not profile_row:
        return []

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
    return [dict(row) for row in recipients_rows]


async def _diagnose(run_id: str, run_env: str) -> list[StoreDiagnosis]:
    async with session_scope(config.database_url) as session:
        batch_row = await _load_batch(session, run_id)
        if not batch_row:
            return [
                StoreDiagnosis(
                    store_code=store_code,
                    assignments=0,
                    eligible_leads=0,
                    documents=0,
                    missing_files=0,
                    has_recipients=False,
                    reasons=[f"no lead_assignment batch found for run_id {run_id}"],
                )
                for store_code in STORE_CODES
            ]

        assignments = await _load_assignments(session, batch_row["id"], STORE_CODES)
        eligible = await _load_eligible_leads(session, STORE_CODES)
        documents = await _load_documents(
            session,
            batch_date=batch_row["batch_date"],
            created_at=batch_row["created_at"],
            store_codes=STORE_CODES,
        )
        recipients = await _load_recipients(session, run_env)

    diagnoses: list[StoreDiagnosis] = []
    for store_code in STORE_CODES:
        assignment_count = assignments.get(store_code, 0)
        eligible_count = eligible.get(store_code, 0)
        document_paths = documents.get(store_code, [])
        missing_files = sum(1 for path in document_paths if not path.exists())
        to, cc, _bcc = _collect_recipient_lists(recipients, store_code=store_code)
        has_recipients = bool(to or cc)
        reasons: list[str] = []

        if assignment_count == 0:
            reasons.append("no assignments created for store")
            if eligible_count == 0:
                reasons.append("no eligible leads matched assignment filters")
        if assignment_count > 0 and not document_paths:
            reasons.append("no documents generated for assignment batch window")
        if document_paths and missing_files:
            reasons.append("document file missing on disk")
        if document_paths and not has_recipients:
            reasons.append("no active recipients (to/cc) for store")
        if not reasons:
            reasons.append("assignments and documents exist; email should have been sent (check notify logs)")

        diagnoses.append(
            StoreDiagnosis(
                store_code=store_code,
                assignments=assignment_count,
                eligible_leads=eligible_count,
                documents=len(document_paths),
                missing_files=missing_files,
                has_recipients=has_recipients,
                reasons=reasons,
            )
        )

    return diagnoses


def _render_report(diagnoses: list[StoreDiagnosis]) -> None:
    for diagnosis in diagnoses:
        reasons = "; ".join(diagnosis.reasons)
        print(
            f"{diagnosis.store_code}\t"
            f"assignments={diagnosis.assignments}\t"
            f"eligible_leads={diagnosis.eligible_leads}\t"
            f"documents={diagnosis.documents}\t"
            f"missing_files={diagnosis.missing_files}\t"
            f"has_recipients={diagnosis.has_recipients}\t"
            f"reasons={reasons}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Diagnose why lead assignment emails were not sent for a run."
    )
    parser.add_argument("--run-id", required=True, help="Run id to diagnose.")
    parser.add_argument("--env", dest="run_env", default=None, help="Run env override (dev/prod).")
    args = parser.parse_args()

    run_env = resolve_run_env(args.run_env)
    diagnoses = asyncio.run(_diagnose(args.run_id, run_env))
    _render_report(diagnoses)


if __name__ == "__main__":
    main()
