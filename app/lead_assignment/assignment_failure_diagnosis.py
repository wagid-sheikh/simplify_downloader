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

REPORT_VERSION = "2.0"


@dataclass
class StoreDiagnosis:
    store_code: str
    assignments: int
    total_missed_leads: int
    new_missed_leads: int
    unassigned_new_leads: int
    eligible_leads: int
    assign_leads_enabled: bool | None
    enabled_mapping_count: int
    enabled_active_agent_count: int
    documents: int
    missing_files: int
    document_paths: list[Path]
    has_recipients: bool
    recipients_to: list[str]
    recipients_cc: list[str]
    recipients_bcc: list[str]
    quota_summary: str
    today_counts_summary: str
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


async def _load_lead_filter_counts(
    db_session, store_codes: Iterable[str]
) -> dict[str, dict[str, int]]:
    result = await db_session.execute(
        text(
            """
            SELECT
                store_code,
                COUNT(*) AS total_count,
                SUM(CASE WHEN customer_type = 'New' THEN 1 ELSE 0 END) AS new_count,
                SUM(
                    CASE
                        WHEN customer_type = 'New'
                             AND lead_assigned = false
                             AND (is_order_placed = false OR is_order_placed IS NULL)
                        THEN 1
                        ELSE 0
                    END
                ) AS unassigned_new_count
            FROM missed_leads
            WHERE store_code IN :store_codes
            GROUP BY store_code
            """
        ).bindparams(bindparam("store_codes", expanding=True)),
        {"store_codes": list(store_codes)},
    )
    return {
        row.store_code: {
            "total": int(row.total_count),
            "new": int(row.new_count or 0),
            "unassigned_new": int(row.unassigned_new_count or 0),
        }
        for row in result
    }


async def _load_quota_settings(db_session, store_codes: Iterable[str]) -> dict[str, list[dict[str, object]]]:
    result = await db_session.execute(
        text(
            """
            SELECT
                slam.store_code,
                slam.agent_id,
                am.agent_code,
                slam.max_existing_per_lot,
                slam.max_new_per_lot,
                slam.max_daily_leads
            FROM store_lead_assignment_map slam
            JOIN agents_master am ON am.id = slam.agent_id AND am.is_active = true
            WHERE slam.is_enabled = true
              AND slam.store_code IN :store_codes
            ORDER BY slam.store_code, am.agent_code
            """
        ).bindparams(bindparam("store_codes", expanding=True)),
        {"store_codes": list(store_codes)},
    )
    grouped: dict[str, list[dict[str, object]]] = {}
    for row in result.mappings():
        grouped.setdefault(row["store_code"], []).append(dict(row))
    return grouped


async def _load_store_settings(db_session, store_codes: Iterable[str]) -> dict[str, bool]:
    result = await db_session.execute(
        text(
            """
            SELECT store_code, assign_leads
            FROM store_master
            WHERE store_code IN :store_codes
            """
        ).bindparams(bindparam("store_codes", expanding=True)),
        {"store_codes": list(store_codes)},
    )
    return {row.store_code: bool(row.assign_leads) for row in result}


async def _load_mapping_counts(db_session, store_codes: Iterable[str]) -> dict[str, dict[str, int]]:
    result = await db_session.execute(
        text(
            """
            SELECT
                slam.store_code,
                SUM(CASE WHEN slam.is_enabled = true THEN 1 ELSE 0 END) AS enabled_mappings,
                SUM(
                    CASE
                        WHEN slam.is_enabled = true AND am.is_active = true THEN 1
                        ELSE 0
                    END
                ) AS enabled_active_agents
            FROM store_lead_assignment_map slam
            LEFT JOIN agents_master am ON am.id = slam.agent_id
            WHERE slam.store_code IN :store_codes
            GROUP BY slam.store_code
            """
        ).bindparams(bindparam("store_codes", expanding=True)),
        {"store_codes": list(store_codes)},
    )
    return {
        row.store_code: {
            "enabled_mappings": int(row.enabled_mappings or 0),
            "enabled_active_agents": int(row.enabled_active_agents or 0),
        }
        for row in result
    }


async def _load_today_assignment_counts(
    db_session, store_codes: Iterable[str]
) -> dict[tuple[str, int], dict[str, int]]:
    result = await db_session.execute(
        text(
            """
            SELECT
                store_code,
                agent_id,
                SUM(CASE WHEN lead_type = 'E' THEN 1 ELSE 0 END) AS existing_count,
                SUM(CASE WHEN lead_type = 'N' THEN 1 ELSE 0 END) AS new_count,
                COUNT(*) AS total_count
            FROM lead_assignments
            WHERE (lead_date = CURRENT_DATE OR CAST(assigned_at AS DATE) = CURRENT_DATE)
              AND store_code IN :store_codes
            GROUP BY store_code, agent_id
            """
        ).bindparams(bindparam("store_codes", expanding=True)),
        {"store_codes": list(store_codes)},
    )

    return {
        (row.store_code, row.agent_id): {
            "existing": int(row.existing_count or 0),
            "new": int(row.new_count or 0),
            "total": int(row.total_count or 0),
        }
        for row in result
    }


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


def _format_limit(value: object) -> str:
    if value is None:
        return "unlimited"
    return str(value)


def _build_quota_summary(quota_settings: list[dict[str, object]]) -> str:
    if not quota_settings:
        return "none"
    summaries = []
    for setting in quota_settings:
        summaries.append(
            f"{setting['agent_code']}("
            f"existing={_format_limit(setting['max_existing_per_lot'])},"
            f"new={_format_limit(setting['max_new_per_lot'])},"
            f"daily={_format_limit(setting['max_daily_leads'])})"
        )
    return "; ".join(summaries)


def _build_today_counts_summary(
    store_code: str,
    quota_settings: list[dict[str, object]],
    today_counts: dict[tuple[str, int], dict[str, int]],
) -> str:
    if not quota_settings:
        return "none"
    summaries = []
    for setting in quota_settings:
        key = (store_code, setting["agent_id"])
        counts = today_counts.get(key, {"existing": 0, "new": 0, "total": 0})
        summaries.append(
            f"{setting['agent_code']}"
            f"(existing={counts['existing']},new={counts['new']},total={counts['total']})"
        )
    return "; ".join(summaries)


def _build_quota_reason(
    store_code: str,
    quota_settings: list[dict[str, object]],
    today_counts: dict[tuple[str, int], dict[str, int]],
) -> str:
    if not quota_settings:
        return "quota limits met: no enabled store/agent mappings"
    met = []
    for setting in quota_settings:
        key = (store_code, setting["agent_id"])
        counts = today_counts.get(key, {"existing": 0, "new": 0, "total": 0})
        met_parts = []
        max_existing = setting["max_existing_per_lot"]
        max_new = setting["max_new_per_lot"]
        max_daily = setting["max_daily_leads"]
        if max_existing is not None and counts["existing"] >= max_existing:
            met_parts.append(f"existing {counts['existing']}/{max_existing}")
        if max_new is not None and counts["new"] >= max_new:
            met_parts.append(f"new {counts['new']}/{max_new}")
        if max_daily is not None and counts["total"] >= max_daily:
            met_parts.append(f"daily {counts['total']}/{max_daily}")
        if met_parts:
            met.append(f"{setting['agent_code']}: " + ", ".join(met_parts))
    if met:
        return "quota limits met: " + "; ".join(met)
    return "quota limits met: none"


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
        store_settings = await _load_store_settings(session, STORE_CODES)
        mapping_counts = await _load_mapping_counts(session, STORE_CODES)
        lead_counts = await _load_lead_filter_counts(session, STORE_CODES)
        if not batch_row:
            return [
                StoreDiagnosis(
                    store_code=store_code,
                    assignments=0,
                    total_missed_leads=lead_counts.get(store_code, {}).get("total", 0),
                    new_missed_leads=lead_counts.get(store_code, {}).get("new", 0),
                    unassigned_new_leads=lead_counts.get(store_code, {}).get(
                        "unassigned_new", 0
                    ),
                    eligible_leads=0,
                    assign_leads_enabled=store_settings.get(store_code),
                    enabled_mapping_count=mapping_counts.get(store_code, {}).get(
                        "enabled_mappings", 0
                    ),
                    enabled_active_agent_count=mapping_counts.get(store_code, {}).get(
                        "enabled_active_agents", 0
                    ),
                    documents=0,
                    missing_files=0,
                    document_paths=[],
                    has_recipients=False,
                    recipients_to=[],
                    recipients_cc=[],
                    recipients_bcc=[],
                    quota_summary="none",
                    today_counts_summary="none",
                    reasons=[f"no lead_assignment batch found for run_id {run_id}"],
                )
                for store_code in STORE_CODES
            ]

        assignments = await _load_assignments(session, batch_row["id"], STORE_CODES)
        eligible = await _load_eligible_leads(session, STORE_CODES)
        quota_settings = await _load_quota_settings(session, STORE_CODES)
        today_counts = await _load_today_assignment_counts(session, STORE_CODES)
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
        lead_count_info = lead_counts.get(store_code, {})
        store_setting = store_settings.get(store_code)
        mapping_info = mapping_counts.get(store_code, {})
        document_paths = documents.get(store_code, [])
        store_quota_settings = quota_settings.get(store_code, [])
        quota_summary = _build_quota_summary(store_quota_settings)
        today_counts_summary = _build_today_counts_summary(
            store_code, store_quota_settings, today_counts
        )
        missing_files = sum(1 for path in document_paths if not path.exists())
        to, cc, _bcc = _collect_recipient_lists(recipients, store_code=store_code)
        has_recipients = bool(to or cc)
        bcc = _bcc or []
        reasons: list[str] = []

        if assignment_count == 0:
            reasons.append("no assignments created for store")
            if eligible_count == 0:
                reasons.append("no eligible leads matched assignment filters")
                if store_setting is False:
                    reasons.append("store assign_leads is disabled")
                if mapping_info.get("enabled_mappings", 0) == 0:
                    reasons.append("no enabled agent mappings for store")
                if mapping_info.get("enabled_active_agents", 0) == 0:
                    reasons.append("no enabled mappings with active agents")
            if eligible_count > 0:
                reasons.append(
                    _build_quota_reason(store_code, store_quota_settings, today_counts)
                )
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
                total_missed_leads=lead_count_info.get("total", 0),
                new_missed_leads=lead_count_info.get("new", 0),
                unassigned_new_leads=lead_count_info.get("unassigned_new", 0),
                eligible_leads=eligible_count,
                assign_leads_enabled=store_setting,
                enabled_mapping_count=mapping_info.get("enabled_mappings", 0),
                enabled_active_agent_count=mapping_info.get("enabled_active_agents", 0),
                documents=len(document_paths),
                missing_files=missing_files,
                document_paths=document_paths,
                has_recipients=has_recipients,
                recipients_to=to,
                recipients_cc=cc,
                recipients_bcc=bcc,
                quota_summary=quota_summary,
                today_counts_summary=today_counts_summary,
                reasons=reasons,
            )
        )

    return diagnoses


def _render_report(
    diagnoses: list[StoreDiagnosis],
    *,
    run_id: str,
    run_env: str,
    batch_row: dict[str, object] | None,
    recipients_rows: list[dict[str, object]],
    profile_row: dict[str, object] | None,
) -> None:
    print("Lead assignment failure diagnosis")
    print(f"report_version={REPORT_VERSION}")
    print(f"run_id={run_id} env={run_env}")
    if batch_row:
        print(
            "batch_id={id} batch_date={batch_date} created_at={created_at}".format(
                id=batch_row["id"],
                batch_date=batch_row["batch_date"],
                created_at=batch_row["created_at"],
            )
        )
    else:
        print("batch_id=none batch_date=none created_at=none")
    if profile_row:
        print(f"notification_profile_id={profile_row['id']}")
    else:
        print("notification_profile_id=none")
    print(f"notification_recipients={len(recipients_rows)}")

    header_columns = [
        "store_code",
        "assignments",
        "missed_total",
        "missed_new",
        "missed_unassigned_new",
        "eligible_leads",
        "assign_leads_enabled",
        "enabled_mappings",
        "enabled_active_agents",
        "documents",
        "missing_files",
        "document_paths",
        "has_recipients",
        "to",
        "cc",
        "bcc",
        "quotas",
        "today_counts",
        "reasons",
    ]
    print("columns=" + ",".join(header_columns))

    for diagnosis in diagnoses:
        reasons = "; ".join(diagnosis.reasons)
        doc_paths = ",".join(str(path) for path in diagnosis.document_paths) or "none"
        to_list = ",".join(diagnosis.recipients_to) or "none"
        cc_list = ",".join(diagnosis.recipients_cc) or "none"
        bcc_list = ",".join(diagnosis.recipients_bcc) or "none"
        assign_leads_value = (
            "unknown" if diagnosis.assign_leads_enabled is None else str(diagnosis.assign_leads_enabled)
        )
        print(
            f"{diagnosis.store_code}\t"
            f"assignments={diagnosis.assignments}\t"
            f"missed_total={diagnosis.total_missed_leads}\t"
            f"missed_new={diagnosis.new_missed_leads}\t"
            f"missed_unassigned_new={diagnosis.unassigned_new_leads}\t"
            f"eligible_leads={diagnosis.eligible_leads}\t"
            f"assign_leads_enabled={assign_leads_value}\t"
            f"enabled_mappings={diagnosis.enabled_mapping_count}\t"
            f"enabled_active_agents={diagnosis.enabled_active_agent_count}\t"
            f"documents={diagnosis.documents}\t"
            f"missing_files={diagnosis.missing_files}\t"
            f"document_paths={doc_paths}\t"
            f"has_recipients={diagnosis.has_recipients}\t"
            f"to={to_list}\t"
            f"cc={cc_list}\t"
            f"bcc={bcc_list}\t"
            f"quotas={diagnosis.quota_summary}\t"
            f"today_counts={diagnosis.today_counts_summary}\t"
            f"reasons={reasons}"
        )


async def _run_diagnosis(run_id: str, run_env: str) -> None:
    diagnoses = await _diagnose(run_id, run_env)
    async with session_scope(config.database_url) as session:
        batch_row = await _load_batch(session, run_id)
        profile_row = (
            await session.execute(
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
        recipients_rows = await _load_recipients(session, run_env)

    _render_report(
        diagnoses,
        run_id=run_id,
        run_env=run_env,
        batch_row=batch_row,
        recipients_rows=recipients_rows,
        profile_row=profile_row,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Diagnose why lead assignment emails were not sent for a run."
    )
    parser.add_argument("--run-id", required=True, help="Run id to diagnose.")
    parser.add_argument("--env", dest="run_env", default=None, help="Run env override (dev/prod).")
    args = parser.parse_args()

    run_env = resolve_run_env(args.run_env)
    asyncio.run(_run_diagnosis(args.run_id, run_env))


if __name__ == "__main__":
    main()
