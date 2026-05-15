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
)


@dataclass(frozen=True)
class PaymentEvidenceReviewFilters:
    source_type: str | None = None
    cost_center: str | None = None
    start_date: date | str | None = None
    end_date: date | str | None = None
    grouped: bool | None = None
    limit: int | None = None


def build_payment_evidence_review_query(filters: PaymentEvidenceReviewFilters) -> tuple[sa.TextClause, dict[str, Any]]:
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
    WHERE {' AND '.join(where_clauses)}
    ORDER BY
        payment_date DESC NULLS LAST,
        payment_id DESC{limit_clause}
    """
    return sa.text(sql), params


async def fetch_payment_evidence_review_rows(
    session: AsyncSession,
    filters: PaymentEvidenceReviewFilters,
) -> list[Mapping[str, Any]]:
    query, params = build_payment_evidence_review_query(filters)
    result = await session.execute(query, params)
    return [dict(row) for row in result.mappings().all()]


def write_payment_evidence_review_csv(rows: Sequence[Mapping[str, Any]], output: TextIO) -> None:
    writer = csv.DictWriter(output, fieldnames=PAYMENT_EVIDENCE_REVIEW_COLUMNS, extrasaction="ignore")
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
    parser.add_argument("--source-type", help="Filter by payment_collections.source_type")
    parser.add_argument("--cost-center", help="Filter by payment_collections.cost_center")
    parser.add_argument("--start-date", help="Filter payment_date on or after YYYY-MM-DD")
    parser.add_argument("--end-date", help="Filter payment_date on or before YYYY-MM-DD")
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
