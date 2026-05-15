"""Payment evidence review CSV export.

Purpose: expose payment_collections rows with normalized order tokens and
reconciliation outcomes for operator audit.
Inputs: optional source_type, cost_center, payment_date range, grouped-row, and
limit filters.
Outputs: rows from vw_payment_evidence_reconciliation, usually written as CSV.
Example usage: python scripts/payment_evidence_review.py --source-type google_sheet --grouped true
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import sys
from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping, Sequence, TextIO

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.common.db import session_scope
from app.reports.shared.payment_reconciliation import (
    VALID_PAYMENT_SOURCE_TYPES,
    build_payment_evidence_audit_rows,
    split_payment_order_numbers,
)

PAYMENT_EVIDENCE_REVIEW_COLUMNS = (
    "payment_id",
    "source_type",
    "source_sheet_row",
    "cost_center",
    "payment_date",
    "payment_timestamp",
    "order_number",
    "normalized_order_tokens_csv",
    "amount",
    "order_amount",
    "payment_received",
    "reconciliation_result",
    "is_grouped",
    "bank_row_id",
    "group_key",
    "grouped_amount",
    "grouped_order_amount",
    "grouped_payment_received",
    "sales_evidence_difference",
    "sales_evidence_mismatch",
    "sales_evidence_classification",
    "component_id",
    "recovery_statuses_csv",
    "recovery_categories_csv",
)


@dataclass(frozen=True)
class PaymentEvidenceReviewFilters:
    source_type: str | None = None
    cost_center: str | None = None
    start_date: date | str | None = None
    end_date: date | str | None = None
    grouped: bool | None = None
    limit: int | None = None


def build_payment_evidence_review_query(
    filters: PaymentEvidenceReviewFilters,
) -> tuple[sa.TextClause, dict[str, Any]]:
    """Build the operator query for the payment-evidence reconciliation audit view."""

    where_clauses = ["1 = 1"]
    params: dict[str, Any] = {
        "source_type": filters.source_type,
        "cost_center": filters.cost_center,
        "start_date": filters.start_date,
        "end_date": filters.end_date,
        "grouped": filters.grouped,
        "limit": filters.limit,
    }

    if filters.source_type:
        where_clauses.append("source_type = :source_type")
    if filters.cost_center:
        where_clauses.append("cost_center = :cost_center")
    if filters.start_date:
        where_clauses.append("payment_date >= :start_date")
    if filters.end_date:
        where_clauses.append("payment_date <= :end_date")
    if filters.grouped is not None:
        where_clauses.append("is_grouped = :grouped")

    limit_clause = ""
    if filters.limit is not None:
        if filters.limit <= 0:
            raise ValueError("limit must be greater than zero")
        limit_clause = "\nLIMIT :limit"

    column_sql = ",\n        ".join(PAYMENT_EVIDENCE_REVIEW_COLUMNS)
    sql = f"""
    SELECT
        {column_sql}
    FROM vw_payment_evidence_reconciliation
    WHERE {" AND ".join(where_clauses)}
    ORDER BY
        payment_date DESC NULLS LAST,
        payment_id DESC{limit_clause}
    """
    return sa.text(sql), params


async def fetch_payment_evidence_review_rows(
    session: AsyncSession,
    filters: PaymentEvidenceReviewFilters,
) -> list[Mapping[str, Any]]:
    """Fetch audit rows using the canonical payment reconciliation engine.

    The legacy SQL view remains queryable for operators, but the application CSV
    path derives statuses from ``payment_reconciliation.py`` so grouped payments
    and single-order top-ups sharing a token are reconciled as one component.
    """

    seed_payment_rows = await _fetch_payment_collection_rows(session, filters)
    seed_payment_ids = {row.get("payment_id") for row in seed_payment_rows}
    component_payment_rows = await _fetch_connected_component_payment_rows(
        session, seed_payment_rows
    )
    order_tokens_by_cost_center = _payment_order_tokens_by_cost_center(
        component_payment_rows
    )
    order_rows = await _fetch_audit_order_rows(session, order_tokens_by_cost_center)
    sales_rows = await _fetch_audit_sales_rows(session, order_tokens_by_cost_center)
    audit_rows = build_payment_evidence_audit_rows(
        order_rows=order_rows,
        sales_rows=sales_rows,
        payment_evidence_rows=component_payment_rows,
    )
    rows = [row.as_dict() for row in audit_rows if row.payment_id in seed_payment_ids]
    if filters.grouped is not None:
        rows = [row for row in rows if bool(row["is_grouped"]) is filters.grouped]
    rows.sort(
        key=lambda row: (
            str(row.get("payment_date") or ""),
            row.get("payment_id") or 0,
        ),
        reverse=True,
    )
    if filters.limit is not None:
        if filters.limit <= 0:
            raise ValueError("limit must be greater than zero")
        rows = rows[: filters.limit]
    return rows


async def _fetch_payment_collection_rows(
    session: AsyncSession,
    filters: PaymentEvidenceReviewFilters,
) -> list[dict[str, Any]]:
    payment_collections = sa.table(
        "payment_collections",
        sa.column("payment_id"),
        sa.column("source_type"),
        sa.column("source_sheet_row"),
        sa.column("cost_center"),
        sa.column("payment_date"),
        sa.column("payment_timestamp"),
        sa.column("order_number"),
        sa.column("amount"),
        sa.column("bank_row_id"),
    )
    stmt = sa.select(
        payment_collections.c.payment_id,
        payment_collections.c.source_type,
        payment_collections.c.source_sheet_row,
        payment_collections.c.cost_center,
        payment_collections.c.payment_date,
        payment_collections.c.payment_timestamp,
        payment_collections.c.order_number,
        payment_collections.c.amount,
        payment_collections.c.bank_row_id,
    )
    if filters.source_type:
        stmt = stmt.where(payment_collections.c.source_type == filters.source_type)
    if filters.cost_center:
        stmt = stmt.where(payment_collections.c.cost_center == filters.cost_center)
    if filters.start_date:
        stmt = stmt.where(payment_collections.c.payment_date >= filters.start_date)
    if filters.end_date:
        stmt = stmt.where(payment_collections.c.payment_date <= filters.end_date)
    stmt = stmt.order_by(
        payment_collections.c.payment_date.desc().nulls_last(),
        payment_collections.c.payment_id.desc(),
    )
    if filters.limit is not None:
        if filters.limit <= 0:
            raise ValueError("limit must be greater than zero")
        stmt = stmt.limit(filters.limit)
    result = await session.execute(stmt)
    return [dict(row) for row in result.mappings().all()]


async def _fetch_connected_component_payment_rows(
    session: AsyncSession,
    seed_payment_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    if not seed_payment_rows:
        return []

    seed_cost_centers = {str(row.get("cost_center") or "") for row in seed_payment_rows}
    seed_cost_centers.discard("")
    if not seed_cost_centers:
        return list(seed_payment_rows)

    payment_collections = sa.table(
        "payment_collections",
        sa.column("payment_id"),
        sa.column("source_type"),
        sa.column("source_sheet_row"),
        sa.column("cost_center"),
        sa.column("payment_date"),
        sa.column("payment_timestamp"),
        sa.column("order_number"),
        sa.column("amount"),
        sa.column("bank_row_id"),
    )
    result = await session.execute(
        sa.select(
            payment_collections.c.payment_id,
            payment_collections.c.source_type,
            payment_collections.c.source_sheet_row,
            payment_collections.c.cost_center,
            payment_collections.c.payment_date,
            payment_collections.c.payment_timestamp,
            payment_collections.c.order_number,
            payment_collections.c.amount,
            payment_collections.c.bank_row_id,
        )
        .where(payment_collections.c.cost_center.in_(sorted(seed_cost_centers)))
        .where(
            sa.func.lower(payment_collections.c.source_type).in_(
                sorted(VALID_PAYMENT_SOURCE_TYPES)
            )
        )
        .order_by(
            payment_collections.c.payment_date.desc().nulls_last(),
            payment_collections.c.payment_id.desc(),
        )
    )
    candidate_rows = [dict(row) for row in result.mappings().all()]
    return _filter_payment_rows_to_seed_components(candidate_rows, seed_payment_rows)


def _filter_payment_rows_to_seed_components(
    candidate_rows: Sequence[Mapping[str, Any]],
    seed_payment_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    seed_payment_ids = {row.get("payment_id") for row in seed_payment_rows}
    seed_tokens_by_cost_center = _payment_order_tokens_by_cost_center(seed_payment_rows)
    connected_rows: list[dict[str, Any]] = []

    candidate_rows_by_cost_center: dict[str, list[Mapping[str, Any]]] = {}
    for row in candidate_rows:
        candidate_rows_by_cost_center.setdefault(
            str(row.get("cost_center") or ""), []
        ).append(row)

    for cost_center, rows in candidate_rows_by_cost_center.items():
        seed_tokens = seed_tokens_by_cost_center.get(cost_center, set())
        if not seed_tokens:
            connected_rows.extend(
                dict(row) for row in rows if row.get("payment_id") in seed_payment_ids
            )
            continue

        parent: dict[str, str] = {}

        def find(value: str) -> str:
            parent.setdefault(value, value)
            if parent[value] != value:
                parent[value] = find(parent[value])
            return parent[value]

        def union(left: str, right: str) -> None:
            parent[find(right)] = find(left)

        row_nodes: list[tuple[str, Mapping[str, Any]]] = []
        for index, row in enumerate(rows):
            row_node = f"row:{index}:{row.get('payment_id')}"
            find(row_node)
            row_nodes.append((row_node, row))
            for token in split_payment_order_numbers(row.get("order_number")):
                token_node = f"token:{token}"
                find(token_node)
                union(row_node, token_node)

        seed_roots = {
            find(f"token:{token}")
            for token in seed_tokens
            if f"token:{token}" in parent
        }
        seed_roots.update(
            find(row_node)
            for row_node, row in row_nodes
            if row.get("payment_id") in seed_payment_ids
        )
        connected_rows.extend(
            dict(row) for row_node, row in row_nodes if find(row_node) in seed_roots
        )

    return connected_rows


def _payment_order_tokens_by_cost_center(
    payment_rows: Sequence[Mapping[str, Any]],
) -> dict[str, set[str]]:
    tokens_by_cost_center: dict[str, set[str]] = {}
    for row in payment_rows:
        cost_center = str(row.get("cost_center") or "")
        tokens = split_payment_order_numbers(row.get("order_number"))
        if not cost_center or not tokens:
            continue
        tokens_by_cost_center.setdefault(cost_center, set()).update(tokens)
    return tokens_by_cost_center


def _normalized_order_number_expression(column: Any) -> Any:
    normalized = column
    for whitespace in (" ", "\t", "\n", "\r", "\f", "\v"):
        normalized = sa.func.replace(normalized, whitespace, "")
    return sa.func.upper(normalized)


def _matching_order_token_clause(
    *,
    cost_center_column: Any,
    order_number_column: Any,
    tokens_by_cost_center: Mapping[str, set[str]],
) -> Any:
    normalized_order_number = _normalized_order_number_expression(order_number_column)
    clauses = [
        sa.and_(
            cost_center_column == cost_center,
            normalized_order_number.in_(sorted(tokens)),
        )
        for cost_center, tokens in sorted(tokens_by_cost_center.items())
        if tokens
    ]
    if not clauses:
        return sa.false()
    return sa.or_(*clauses)


async def _fetch_audit_order_rows(
    session: AsyncSession, tokens_by_cost_center: Mapping[str, set[str]]
) -> list[dict[str, Any]]:
    if not tokens_by_cost_center:
        return []

    def _vw_orders_columns(sync_session: Any) -> set[str]:
        connection = sync_session.connection()
        return {
            column["name"] for column in sa.inspect(connection).get_columns("vw_orders")
        }

    available_columns = await session.run_sync(_vw_orders_columns)
    orders = sa.table(
        "vw_orders",
        sa.column("cost_center"),
        sa.column("order_number"),
        sa.column("order_date"),
        sa.column("order_amount"),
        sa.column("recovery_status"),
        sa.column("recovery_category"),
    )
    selected_columns = [
        orders.c.cost_center,
        orders.c.order_number,
        orders.c.order_date,
        orders.c.order_amount,
    ]
    if "recovery_status" in available_columns:
        selected_columns.append(orders.c.recovery_status)
    if "recovery_category" in available_columns:
        selected_columns.append(orders.c.recovery_category)
    result = await session.execute(
        sa.select(*selected_columns).where(
            _matching_order_token_clause(
                cost_center_column=orders.c.cost_center,
                order_number_column=orders.c.order_number,
                tokens_by_cost_center=tokens_by_cost_center,
            )
        )
    )
    return [dict(row) for row in result.mappings().all()]


async def _fetch_audit_sales_rows(
    session: AsyncSession, tokens_by_cost_center: Mapping[str, set[str]]
) -> list[dict[str, Any]]:
    if not tokens_by_cost_center:
        return []
    sales = sa.table(
        "sales",
        sa.column("cost_center"),
        sa.column("order_number"),
        sa.column("payment_received"),
    )
    result = await session.execute(
        sa.select(
            sales.c.cost_center,
            sales.c.order_number,
            sales.c.payment_received,
        ).where(
            _matching_order_token_clause(
                cost_center_column=sales.c.cost_center,
                order_number_column=sales.c.order_number,
                tokens_by_cost_center=tokens_by_cost_center,
            )
        )
    )
    return [dict(row) for row in result.mappings().all()]


def write_payment_evidence_review_csv(
    rows: Sequence[Mapping[str, Any]], output: TextIO
) -> None:
    writer = csv.DictWriter(
        output, fieldnames=PAYMENT_EVIDENCE_REVIEW_COLUMNS, extrasaction="ignore"
    )
    writer.writeheader()
    writer.writerows(rows)


async def _run_report(args: argparse.Namespace) -> int:
    filters = PaymentEvidenceReviewFilters(
        source_type=args.source_type,
        cost_center=args.cost_center,
        start_date=args.start_date,
        end_date=args.end_date,
        grouped=_parse_grouped_filter(args.grouped),
        limit=args.limit,
    )
    from app.config import config

    async with session_scope(config.database_url) as session:
        rows = await fetch_payment_evidence_review_rows(session, filters)
    write_payment_evidence_review_csv(rows, sys.stdout)
    return 0


def _parse_grouped_filter(value: str) -> bool | None:
    if value == "all":
        return None
    return value == "true"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="payment_evidence_review",
        description="Export payment evidence rows with order/sales reconciliation results as CSV.",
    )
    parser.add_argument(
        "--source-type", help="Filter by payment_collections.source_type"
    )
    parser.add_argument(
        "--cost-center", help="Filter by payment_collections.cost_center"
    )
    parser.add_argument(
        "--start-date", help="Filter payment_date on or after YYYY-MM-DD"
    )
    parser.add_argument(
        "--end-date", help="Filter payment_date on or before YYYY-MM-DD"
    )
    parser.add_argument(
        "--grouped",
        choices=("all", "true", "false"),
        default="all",
        help="Filter grouped rows; defaults to all rows",
    )
    parser.add_argument("--limit", type=int, help="Maximum rows to export")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    return asyncio.run(_run_report(args))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
