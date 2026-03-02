from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from app.common.db import session_scope


@dataclass
class TdGarmentIngestResult:
    staging_rows: int
    staging_inserted: int
    staging_updated: int
    final_rows: int
    final_inserted: int
    final_updated: int
    row_count: int
    duplicate_rows: int
    changed_rows: int
    late_updates: int
    orphan_rows: int
    warnings: list[str] = field(default_factory=list)


def stg_td_garments_table(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "stg_td_garments",
        metadata,
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.Text()),
        sa.Column("run_date", sa.DateTime(timezone=True)),
        sa.Column("window_from_date", sa.Date(), nullable=False),
        sa.Column("window_to_date", sa.Date(), nullable=False),
        sa.Column("cost_center", sa.String(length=8), nullable=False),
        sa.Column("store_code", sa.String(length=8), nullable=False),
        sa.Column("api_order_id", sa.String(length=64)),
        sa.Column("api_line_item_id", sa.String(length=64)),
        sa.Column("api_garment_id", sa.String(length=64)),
        sa.Column("order_number", sa.String(length=32), nullable=False),
        sa.Column("line_item_key", sa.String(length=128), nullable=False),
        sa.Column("line_item_uid", sa.String(length=160), nullable=False),
        sa.Column("garment_name", sa.String(length=128)),
        sa.Column("service_name", sa.String(length=128)),
        sa.Column("quantity", sa.Numeric(12, 2)),
        sa.Column("amount", sa.Numeric(12, 2)),
        sa.Column("order_date", sa.DateTime(timezone=True)),
        sa.Column("updated_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String(length=32)),
        sa.Column("raw_payload", sa.JSON()),
        sa.Column("ingest_row_seq", sa.Integer(), nullable=False),
        sa.Column("ingest_remarks", sa.Text()),
        sqlite_autoincrement=True,
    )


def order_line_items_table(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "order_line_items",
        metadata,
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.Text()),
        sa.Column("run_date", sa.DateTime(timezone=True)),
        sa.Column("cost_center", sa.String(length=8), nullable=False),
        sa.Column("store_code", sa.String(length=8), nullable=False),
        sa.Column("order_id", sa.BigInteger()),
        sa.Column("order_number", sa.String(length=32), nullable=False),
        sa.Column("api_order_id", sa.String(length=64)),
        sa.Column("api_line_item_id", sa.String(length=64)),
        sa.Column("api_garment_id", sa.String(length=64)),
        sa.Column("line_item_key", sa.String(length=128), nullable=False),
        sa.Column("line_item_uid", sa.String(length=160), nullable=False),
        sa.Column("garment_name", sa.String(length=128)),
        sa.Column("service_name", sa.String(length=128)),
        sa.Column("quantity", sa.Numeric(12, 2)),
        sa.Column("amount", sa.Numeric(12, 2)),
        sa.Column("order_date", sa.DateTime(timezone=True)),
        sa.Column("updated_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String(length=32)),
        sa.Column("ingest_row_seq", sa.Integer(), nullable=False),
        sa.Column("is_orphan", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("ingest_remarks", sa.Text()),
        sqlite_autoincrement=True,
    )


def _parse_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value).replace(",", ""))
    except (InvalidOperation, ValueError):
        return None


def _line_item_uid(
    *, cost_center: str, order_number: str, row: Mapping[str, Any], line_item_key: str, row_seq: int
) -> str:
    api_line_item_id = row.get("api_line_item_id") or row.get("apiLineItemId")
    if api_line_item_id:
        return str(api_line_item_id).strip()
    api_garment_id = row.get("api_garment_id") or row.get("apiGarmentId")
    if api_garment_id:
        return str(api_garment_id).strip()
    return f"{cost_center}|{order_number}|{line_item_key}|{row_seq}"


def _line_item_key(row: Mapping[str, Any]) -> str:
    for key in ("line_item_key", "lineItemKey", "itemKey", "line_item_name"):
        value = row.get(key)
        if value and str(value).strip():
            return str(value).strip()
    garment = str(row.get("garment_name") or row.get("sub_garment") or row.get("subGarment") or row.get("garment") or "").strip()
    service = str(row.get("service_name") or row.get("service") or row.get("primary_service") or row.get("primaryService") or "").strip()
    return f"{garment}|{service}".strip("|") or "unknown"


def _make_insert(table: sa.Table, values: dict[str, Any], *, use_sqlite: bool):
    insert = sqlite_insert(table).values(**values) if use_sqlite else pg_insert(table).values(**values)
    unique_cols: list[str] = []
    for constraint in table.constraints:
        if isinstance(constraint, sa.UniqueConstraint):
            unique_cols = [col.name for col in constraint.columns]
            break
    if not unique_cols:
        return insert
    return insert.on_conflict_do_update(
        index_elements=unique_cols,
        set_={key: insert.excluded[key] for key in values},
    )


async def ingest_td_garment_rows(
    *,
    rows: Sequence[Mapping[str, Any]],
    store_code: str,
    cost_center: str,
    run_id: str,
    run_date: datetime,
    window_from_date: date,
    window_to_date: date,
    database_url: str,
) -> TdGarmentIngestResult:
    metadata = sa.MetaData()
    stg_table = stg_td_garments_table(metadata)
    line_items_table = order_line_items_table(metadata)
    use_sqlite = database_url.startswith("sqlite")

    warnings: list[str] = []
    normalized: list[dict[str, Any]] = []

    for row_seq, row in enumerate(rows, start=1):
        order_number = str(
            row.get("order_no")
            or row.get("order_number")
            or row.get("orderNo")
            or row.get("orderNumber")
            or ""
        ).strip()
        if not order_number:
            warnings.append("Skipped garment row without order number")
            continue
        line_item_key = _line_item_key(row)
        uid = _line_item_uid(
            cost_center=cost_center,
            order_number=order_number,
            row=row,
            line_item_key=line_item_key,
            row_seq=row_seq,
        )
        api_order_id = str(row.get("api_order_id") or row.get("apiOrderId") or "").strip() or None
        api_line_item_id = str(row.get("api_line_item_id") or row.get("apiLineItemId") or "").strip() or None
        api_garment_id = str(row.get("api_garment_id") or row.get("apiGarmentId") or "").strip() or None
        normalized.append(
            {
                "order_number": order_number,
                "line_item_key": line_item_key,
                "line_item_uid": uid,
                "api_order_id": api_order_id,
                "api_line_item_id": api_line_item_id,
                "api_garment_id": api_garment_id,
                "garment_name": str(row.get("garment_name") or row.get("sub_garment") or row.get("subGarment") or row.get("garment") or "").strip() or None,
                "service_name": str(row.get("service_name") or row.get("service") or row.get("primary_service") or row.get("primaryService") or "").strip() or None,
                "quantity": _parse_decimal(row.get("quantity")),
                "amount": _parse_decimal(row.get("amount")),
                "order_date": row.get("order_date") if isinstance(row.get("order_date"), datetime) else None,
                "updated_at": row.get("updated_at") if isinstance(row.get("updated_at"), datetime) else None,
                "status": str(row.get("status") or "").strip() or None,
                "raw_payload": dict(row),
                "ingest_row_seq": row_seq,
            }
        )

    duplicate_rows = sum(max(0, c - 1) for c in Counter([r["line_item_uid"] for r in normalized]).values())
    late_updates = sum(
        1
        for r in normalized
        if isinstance(r.get("order_date"), datetime) and r["order_date"].date() < window_from_date
    )

    if not normalized:
        return TdGarmentIngestResult(0, 0, 0, 0, 0, 0, 0, duplicate_rows, 0, late_updates, 0, warnings)

    async with session_scope(database_url) as session:
        bind = session.bind
        if isinstance(bind, AsyncEngine):
            async with bind.begin() as conn:
                await conn.run_sync(metadata.create_all)
        elif isinstance(bind, AsyncConnection):
            await bind.run_sync(metadata.create_all)

        order_lookup_stmt = sa.text(
            "SELECT id, order_number FROM orders WHERE cost_center = :cost_center"
        )
        order_rows = (await session.execute(order_lookup_stmt, {"cost_center": cost_center})).fetchall()
        order_map = {str(row.order_number): int(row.id) for row in order_rows if row.order_number is not None}

        orphan_rows = 0
        for row in normalized:
            order_id = order_map.get(row["order_number"])
            is_orphan = order_id is None
            remarks = ""
            if is_orphan:
                orphan_rows += 1
                remarks = "ORPHAN_ORDER_REFERENCE: missing orders.id for (cost_center, order_number)"

            stg_values = {
                "run_id": run_id,
                "run_date": run_date,
                "window_from_date": window_from_date,
                "window_to_date": window_to_date,
                "cost_center": cost_center,
                "store_code": store_code,
                **row,
                "ingest_remarks": remarks,
            }
            await session.execute(_make_insert(stg_table, stg_values, use_sqlite=use_sqlite))

            final_values = {
                "run_id": run_id,
                "run_date": run_date,
                "cost_center": cost_center,
                "store_code": store_code,
                "order_id": order_id,
                "order_number": row["order_number"],
                "api_order_id": row.get("api_order_id"),
                "api_line_item_id": row.get("api_line_item_id"),
                "api_garment_id": row.get("api_garment_id"),
                "line_item_key": row["line_item_key"],
                "line_item_uid": row["line_item_uid"],
                "garment_name": row.get("garment_name"),
                "service_name": row.get("service_name"),
                "quantity": row.get("quantity"),
                "amount": row.get("amount"),
                "order_date": row.get("order_date"),
                "updated_at": row.get("updated_at"),
                "status": row.get("status"),
                "ingest_row_seq": row["ingest_row_seq"],
                "is_orphan": is_orphan,
                "ingest_remarks": remarks,
            }
            await session.execute(_make_insert(line_items_table, final_values, use_sqlite=use_sqlite))

        await session.commit()

    total = len(normalized)
    stg_inserted = total
    final_inserted = total

    return TdGarmentIngestResult(
        staging_rows=total,
        staging_inserted=stg_inserted,
        staging_updated=total - stg_inserted,
        final_rows=total,
        final_inserted=final_inserted,
        final_updated=total - final_inserted,
        row_count=total,
        duplicate_rows=duplicate_rows,
        changed_rows=0,
        late_updates=late_updates,
        orphan_rows=orphan_rows,
        warnings=warnings,
    )


__all__ = ["TdGarmentIngestResult", "ingest_td_garment_rows", "stg_td_garments_table", "order_line_items_table"]
