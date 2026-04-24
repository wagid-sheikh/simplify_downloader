from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.common.db import session_scope


@dataclass
class TdLeadsIngestResult:
    rows_received: int
    rows_upserted: int
    bucket_write_counts: dict[str, dict[str, int]]
    status_transitions: list[dict[str, Any]]
    lead_change_details: dict[str, Any]
    task_stub: dict[str, Any]


LEAD_CHANGE_DETAILS_GROUP_CAP = 100


def _stable_lead_identity(values: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "lead_uid": str(values.get("lead_uid") or ""),
        "pickup_no": values.get("pickup_no"),
        "store_code": values.get("store_code"),
    }


def _mobile_for_display(raw_mobile: Any) -> str | None:
    mobile = str(raw_mobile or "").strip()
    if not mobile:
        return None
    return mobile


def _build_lead_change_payload(*, cap_per_group: int, rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    dedupe_keys: set[tuple[str, str, str]] = set()
    grouped: dict[str, list[dict[str, Any]]] = {}
    overflow_counts: dict[str, int] = {}

    def _group_key(row: Mapping[str, Any]) -> str:
        action = str(row.get("action") or "").strip().lower()
        current = str(row.get("current_status_bucket") or "").strip().lower()
        previous = str(row.get("previous_status_bucket") or "").strip().lower()
        if previous and previous != current:
            return f"transition:{previous}->{current}"
        return f"{action}:{current}"

    for row in rows:
        group_key = _group_key(row)
        lead_identity = row.get("lead_identity") if isinstance(row.get("lead_identity"), Mapping) else {}
        stable_id = str(lead_identity.get("lead_uid") or row.get("lead_uid") or "").strip()
        fallback_key = "|".join(
            (
                stable_id,
                str(lead_identity.get("pickup_no") or row.get("pickup_no") or "").strip(),
            )
        )
        identity_for_dedupe = stable_id or fallback_key
        dedupe_key = (group_key, str(row.get("action") or ""), identity_for_dedupe)
        if dedupe_key in dedupe_keys:
            continue
        dedupe_keys.add(dedupe_key)

        grouped.setdefault(group_key, [])
        if len(grouped[group_key]) >= cap_per_group:
            overflow_counts[group_key] = overflow_counts.get(group_key, 0) + 1
            continue
        grouped[group_key].append(dict(row))

    created_by_bucket: list[dict[str, Any]] = []
    updated_by_bucket: list[dict[str, Any]] = []
    transitions: list[dict[str, Any]] = []
    for group_key in sorted(grouped):
        rows_for_group = grouped[group_key]
        overflow = overflow_counts.get(group_key, 0)
        if group_key.startswith("created:"):
            created_by_bucket.append(
                {"status_bucket": group_key.split(":", 1)[1], "rows": rows_for_group, "overflow_count": overflow}
            )
        elif group_key.startswith("updated:"):
            updated_by_bucket.append(
                {"status_bucket": group_key.split(":", 1)[1], "rows": rows_for_group, "overflow_count": overflow}
            )
        elif group_key.startswith("transition:"):
            route = group_key.split(":", 1)[1]
            from_bucket, _, to_bucket = route.partition("->")
            transitions.append(
                {
                    "from_status_bucket": from_bucket,
                    "to_status_bucket": to_bucket,
                    "rows": rows_for_group,
                    "overflow_count": overflow,
                }
            )

    return {
        "cap_per_group": cap_per_group,
        "created_by_bucket": created_by_bucket,
        "updated_by_bucket": updated_by_bucket,
        "transitions": transitions,
    }


def _crm_leads_current_table(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "crm_leads_current",
        metadata,
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), primary_key=True, autoincrement=True),
        sa.Column("lead_uid", sa.String(length=128), nullable=False),
        sa.Column("store_code", sa.String(length=8), nullable=False),
        sa.Column("pickup_no", sa.String(length=64), nullable=False),
        sa.Column("status_bucket", sa.String(length=16), nullable=False),
        sa.Column("customer_name", sa.String(length=256)),
        sa.Column("address", sa.Text()),
        sa.Column("mobile", sa.String(length=32)),
        sa.Column("pickup_date", sa.String(length=64)),
        sa.Column("pickup_created_at", sa.DateTime(timezone=True)),
        sa.Column("pickup_time", sa.String(length=64)),
        sa.Column("special_instruction", sa.Text()),
        sa.Column("reason", sa.String(length=128)),
        sa.Column("source", sa.String(length=128)),
        sa.Column("cancelled_flag", sa.String(length=16)),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("run_env", sa.String(length=32), nullable=False),
        sa.Column("source_file", sa.Text()),
        sa.Column("scraped_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.UniqueConstraint("lead_uid", name="uq_crm_leads_uid"),
    )


def _crm_leads_event_table(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "crm_leads_status_events",
        metadata,
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), primary_key=True, autoincrement=True),
        sa.Column("lead_uid", sa.String(length=128), nullable=False),
        sa.Column("store_code", sa.String(length=8), nullable=False),
        sa.Column("pickup_no", sa.String(length=64), nullable=False),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column("previous_status_bucket", sa.String(length=16)),
        sa.Column("status_bucket", sa.String(length=16), nullable=False),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("run_env", sa.String(length=32), nullable=False),
        sa.Column("source_file", sa.Text()),
        sa.Column("scraped_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
    )


def build_lead_uid(row: Mapping[str, Any]) -> str:
    normalized_store_code = str(row.get("store_code") or "").strip().upper()
    normalized_pickup_no = str(row.get("pickup_no") or "").strip().upper()
    parts = [
        normalized_store_code,
        normalized_pickup_no,
    ]
    materialized = "|".join(parts)
    return sha256(materialized.encode("utf-8")).hexdigest()


def _normalized_pickup_created_text(row: Mapping[str, Any]) -> str:
    created_text = str(row.get("pickup_created_at") or "").strip()
    if created_text:
        return created_text
    created_text = str(row.get("pickup_created_date") or "").strip()
    if created_text:
        return created_text
    return str(row.get("pickup_date") or "").strip()


_IST = ZoneInfo("Asia/Kolkata")
_UTC = ZoneInfo("UTC")


def _coerce_pickup_created_at(row: Mapping[str, Any], *, normalized_created_date: str) -> datetime | None:
    existing = row.get("pickup_created_at")
    if isinstance(existing, datetime):
        if existing.tzinfo is None or existing.utcoffset() is None:
            return existing.replace(tzinfo=_IST).astimezone(_UTC)
        return existing.astimezone(_UTC)
    raw_value = str(existing or normalized_created_date or "").strip()
    if not raw_value:
        return None

    normalized = " ".join(raw_value.split())
    for fmt in ("%d %b %Y %I:%M:%S %p", "%d %b %Y %I:%M %p"):
        try:
            return datetime.strptime(normalized, fmt).replace(tzinfo=_IST).astimezone(_UTC)
        except ValueError:
            continue
    try:
        parsed_date = datetime.strptime(normalized, "%d %b %Y").date()
        return datetime(
            parsed_date.year,
            parsed_date.month,
            parsed_date.day,
            0,
            0,
            0,
            tzinfo=_IST,
        ).astimezone(_UTC)
    except ValueError:
        pass

    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=_IST)
    return parsed.astimezone(_UTC)


async def ingest_td_crm_leads_rows(
    *,
    rows: Sequence[Mapping[str, Any]],
    run_id: str,
    run_env: str,
    source_file: str | None,
    database_url: str,
) -> TdLeadsIngestResult:
    metadata = sa.MetaData()
    current_table = _crm_leads_current_table(metadata)
    event_table = _crm_leads_event_table(metadata)
    use_sqlite = database_url.startswith("sqlite")

    now_utc = datetime.now(timezone.utc)
    upserted = 0
    bucket_write_counts: dict[str, dict[str, int]] = {
        "pending": {"created": 0, "updated": 0},
        "completed": {"created": 0, "updated": 0},
        "cancelled": {"created": 0, "updated": 0},
    }
    status_transitions: list[dict[str, Any]] = []
    lead_change_rows: list[dict[str, Any]] = []

    async with session_scope(database_url) as session:
        connection = await session.connection()
        await connection.run_sync(metadata.create_all)

        prepared_rows: list[dict[str, Any]] = []
        lead_uids: list[str] = []
        for row in rows:
            normalized_created_text = _normalized_pickup_created_text(row)
            normalized_pickup_no = str(row.get("pickup_no") or "").strip().upper()
            if not normalized_pickup_no:
                continue
            normalized_pickup_time = str(row.get("pickup_time") or "").strip()
            pickup_created_at = _coerce_pickup_created_at(row, normalized_created_date=normalized_created_text)
            reason = (str(row.get("reason")).strip() or None) if row.get("reason") is not None else None
            cancelled_flag = "customer" if reason else "store"
            lead_uid = build_lead_uid(row)
            values = {
                "lead_uid": lead_uid,
                "store_code": str(row.get("store_code") or "").upper(),
                "status_bucket": str(row.get("status_bucket") or "").lower(),
                "pickup_no": normalized_pickup_no,
                "customer_name": (str(row.get("customer_name")).strip() or None) if row.get("customer_name") is not None else None,
                "address": (str(row.get("address")).strip() or None) if row.get("address") is not None else None,
                "mobile": (str(row.get("mobile")).strip() or None) if row.get("mobile") is not None else None,
                "pickup_date": (str(row.get("pickup_date")).strip() or None) if row.get("pickup_date") is not None else None,
                "pickup_created_at": pickup_created_at,
                "pickup_time": normalized_pickup_time or None,
                "special_instruction": (str(row.get("special_instruction")).strip() or None) if row.get("special_instruction") is not None else None,
                "reason": reason,
                "source": (str(row.get("source")).strip() or None) if row.get("source") is not None else None,
                "cancelled_flag": cancelled_flag,
                "run_id": run_id,
                "run_env": run_env,
                "source_file": source_file,
                "scraped_at": row.get("scraped_at") or now_utc,
                "updated_at": now_utc,
            }
            prepared_rows.append(values)
            lead_uids.append(lead_uid)

        existing_by_uid: dict[str, dict[str, Any]] = {}
        if lead_uids:
            existing_stmt = sa.select(
                current_table.c.lead_uid,
                current_table.c.status_bucket,
                current_table.c.run_id,
            ).where(current_table.c.lead_uid.in_(lead_uids))
            existing_rows = (await session.execute(existing_stmt)).mappings().all()
            existing_by_uid = {str(row["lead_uid"]): dict(row) for row in existing_rows}

        for values in prepared_rows:
            existing = existing_by_uid.get(str(values["lead_uid"]))
            write_action = "created" if existing is None else "updated"
            status_bucket = str(values.get("status_bucket") or "").strip().lower()
            if status_bucket in bucket_write_counts:
                bucket_write_counts[status_bucket][write_action] += 1
            else:
                bucket_write_counts.setdefault(status_bucket, {"created": 0, "updated": 0})
                bucket_write_counts[status_bucket][write_action] += 1

            if existing is not None:
                from_bucket = str(existing.get("status_bucket") or "").strip().lower()
                to_bucket = status_bucket
                if from_bucket and to_bucket and from_bucket != to_bucket:
                    status_transitions.append(
                        {
                            "lead_uid": values["lead_uid"],
                            "pickup_no": values.get("pickup_no"),
                            "customer_name": values.get("customer_name"),
                            "mobile": values.get("mobile"),
                            "from_status_bucket": from_bucket,
                            "to_status_bucket": to_bucket,
                            "previous_run_id": existing.get("run_id"),
                            "current_run_id": run_id,
                        }
                    )
            lead_change_rows.append(
                {
                    "action": write_action,
                    "current_status_bucket": status_bucket or None,
                    "previous_status_bucket": (
                        str(existing.get("status_bucket") or "").strip().lower() if existing and str(existing.get("status_bucket") or "").strip().lower() != status_bucket else None
                    ),
                    "customer_name": values.get("customer_name"),
                    "mobile": _mobile_for_display(values.get("mobile")),
                    "lead_identity": _stable_lead_identity(values),
                }
            )
            should_insert_event = False
            event_values: dict[str, Any] = {}
            if existing is None:
                should_insert_event = True
                event_values = {
                    "lead_uid": values["lead_uid"],
                    "store_code": values["store_code"],
                    "pickup_no": values["pickup_no"],
                    "event_type": "new_lead",
                    "previous_status_bucket": None,
                    "status_bucket": status_bucket,
                    "run_id": run_id,
                    "run_env": run_env,
                    "source_file": source_file,
                    "scraped_at": values["scraped_at"],
                }
            elif str(existing.get("status_bucket") or "").strip().lower() != status_bucket:
                should_insert_event = True
                event_values = {
                    "lead_uid": values["lead_uid"],
                    "store_code": values["store_code"],
                    "pickup_no": values["pickup_no"],
                    "event_type": "status_transition",
                    "previous_status_bucket": str(existing.get("status_bucket") or "").strip().lower() or None,
                    "status_bucket": status_bucket,
                    "run_id": run_id,
                    "run_env": run_env,
                    "source_file": source_file,
                    "scraped_at": values["scraped_at"],
                }

            insert_stmt = (sqlite_insert(current_table) if use_sqlite else pg_insert(current_table)).values(**values)
            update_values = dict(values)
            update_values.pop("lead_uid", None)
            update_values.pop("created_at", None)
            update_values["pickup_created_at"] = sa.func.coalesce(
                insert_stmt.excluded.pickup_created_at,
                current_table.c.pickup_created_at,
            )
            stmt = insert_stmt.on_conflict_do_update(index_elements=["lead_uid"], set_=update_values)
            await session.execute(stmt)
            if should_insert_event:
                await session.execute(sa.insert(event_table).values(**event_values))
            upserted += 1
        await session.commit()

    task_stub = {
        "task_type": "td_leads_status_bucket_review",
        "run_id": run_id,
        "total_transitions": len(status_transitions),
        "status": "open" if status_transitions else "noop",
    }
    return TdLeadsIngestResult(
        rows_received=len(rows),
        rows_upserted=upserted,
        bucket_write_counts=bucket_write_counts,
        status_transitions=status_transitions,
        lead_change_details=_build_lead_change_payload(cap_per_group=LEAD_CHANGE_DETAILS_GROUP_CAP, rows=lead_change_rows),
        task_stub=task_stub,
    )


__all__ = ["TdLeadsIngestResult", "ingest_td_crm_leads_rows", "build_lead_uid"]
