from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import re
from typing import Any, Mapping, Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.common.db import session_scope


@dataclass
class TdLeadsIngestResult:
    rows_received: int
    rows_upserted: int


def _crm_leads_table(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "crm_leads",
        metadata,
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), primary_key=True, autoincrement=True),
        sa.Column("lead_uid", sa.String(length=128), nullable=False),
        sa.Column("store_code", sa.String(length=8), nullable=False),
        sa.Column("status_bucket", sa.String(length=16), nullable=False),
        sa.Column("pickup_id", sa.String(length=64)),
        sa.Column("pickup_no", sa.String(length=64)),
        sa.Column("customer_name", sa.String(length=256)),
        sa.Column("address", sa.Text()),
        sa.Column("mobile", sa.String(length=32)),
        sa.Column("pickup_created_date", sa.String(length=64)),
        sa.Column("pickup_time", sa.String(length=64)),
        sa.Column("special_instruction", sa.Text()),
        sa.Column("status_text", sa.String(length=64)),
        sa.Column("reason", sa.String(length=128)),
        sa.Column("source", sa.String(length=128)),
        sa.Column("user_name", sa.String(length=128)),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("run_env", sa.String(length=32), nullable=False),
        sa.Column("source_file", sa.Text()),
        sa.Column("scraped_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.UniqueConstraint("lead_uid", name="uq_crm_leads_uid"),
    )


def build_lead_uid(row: Mapping[str, Any]) -> str:
    normalized_created_date = _normalized_pickup_created_date(row)
    normalized_pickup_time = _normalized_pickup_time(row, created_text=normalized_created_date)
    parts = [
        str(row.get("store_code") or "").strip().upper(),
        str(row.get("status_bucket") or "").strip().lower(),
        str(row.get("pickup_id") or "").strip(),
        str(row.get("pickup_no") or "").strip(),
        str(row.get("mobile") or "").strip(),
        normalized_created_date,
        normalized_pickup_time,
    ]
    materialized = "|".join(parts)
    return sha256(materialized.encode("utf-8")).hexdigest()


def _normalized_pickup_created_date(row: Mapping[str, Any]) -> str:
    created_text = str(row.get("pickup_created_date") or "").strip()
    if created_text:
        return created_text
    return str(row.get("pickup_date") or "").strip()


def _normalized_pickup_time(row: Mapping[str, Any], *, created_text: str) -> str:
    pickup_time = str(row.get("pickup_time") or "").strip()
    if pickup_time:
        return pickup_time
    return _extract_time_from_created_text(created_text)


def _extract_time_from_created_text(created_text: str) -> str:
    if not created_text:
        return ""
    normalized = created_text.strip()
    for fmt in ("%d %b %Y %I:%M:%S %p", "%d %b %Y %I:%M %p"):
        try:
            parsed = datetime.strptime(normalized, fmt)
            return parsed.strftime("%I:%M:%S %p").lstrip("0")
        except ValueError:
            continue
    fallback_match = re.search(r"(\d{1,2}:\d{2}(?::\d{2})?\s*[AaPp][Mm])$", normalized)
    if not fallback_match:
        return ""
    raw_time = fallback_match.group(1).upper().replace(" ", "")
    return f"{raw_time[:-2]} {raw_time[-2:]}"


async def ingest_td_crm_leads_rows(
    *,
    rows: Sequence[Mapping[str, Any]],
    run_id: str,
    run_env: str,
    source_file: str | None,
    database_url: str,
) -> TdLeadsIngestResult:
    metadata = sa.MetaData()
    table = _crm_leads_table(metadata)
    use_sqlite = database_url.startswith("sqlite")

    now_utc = datetime.now(timezone.utc)
    upserted = 0

    async with session_scope(database_url) as session:
        connection = await session.connection()
        await connection.run_sync(metadata.create_all)

        for row in rows:
            normalized_created_date = _normalized_pickup_created_date(row)
            normalized_pickup_time = _normalized_pickup_time(row, created_text=normalized_created_date)
            values = {
                "lead_uid": build_lead_uid(row),
                "store_code": str(row.get("store_code") or "").upper(),
                "status_bucket": str(row.get("status_bucket") or "").lower(),
                "pickup_id": (str(row.get("pickup_id")).strip() or None) if row.get("pickup_id") is not None else None,
                "pickup_no": (str(row.get("pickup_no")).strip() or None) if row.get("pickup_no") is not None else None,
                "customer_name": (str(row.get("customer_name")).strip() or None) if row.get("customer_name") is not None else None,
                "address": (str(row.get("address")).strip() or None) if row.get("address") is not None else None,
                "mobile": (str(row.get("mobile")).strip() or None) if row.get("mobile") is not None else None,
                "pickup_created_date": normalized_created_date or None,
                "pickup_time": normalized_pickup_time or None,
                "special_instruction": (str(row.get("special_instruction")).strip() or None) if row.get("special_instruction") is not None else None,
                "status_text": (str(row.get("status_text")).strip() or None) if row.get("status_text") is not None else None,
                "reason": (str(row.get("reason")).strip() or None) if row.get("reason") is not None else None,
                "source": (str(row.get("source")).strip() or None) if row.get("source") is not None else None,
                "user_name": (str(row.get("user") or row.get("user_name") or "").strip() or None),
                "run_id": run_id,
                "run_env": run_env,
                "source_file": source_file,
                "scraped_at": row.get("scraped_at") or now_utc,
                "updated_at": now_utc,
            }
            insert_stmt = (sqlite_insert(table) if use_sqlite else pg_insert(table)).values(**values)
            update_values = dict(values)
            update_values.pop("lead_uid", None)
            update_values.pop("created_at", None)
            stmt = insert_stmt.on_conflict_do_update(index_elements=["lead_uid"], set_=update_values)
            await session.execute(stmt)
            upserted += 1
        await session.commit()

    return TdLeadsIngestResult(rows_received=len(rows), rows_upserted=upserted)


__all__ = ["TdLeadsIngestResult", "ingest_td_crm_leads_rows", "build_lead_uid"]
