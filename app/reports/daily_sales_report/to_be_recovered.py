from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Iterable

from .data import RecoveryOrderRow

TEMPLATE_NAME = "to_be_recovered_report.html"
DOCUMENT_TYPE = "to_be_recovered_report_pdf"
PIPELINE_OUTPUT_PREFIX = "reports.to_be_recovered"


@dataclass(frozen=True)
class RecoverySummaryRow:
    cost_center: str
    count: int
    total_order_amount: Decimal
    total_recoverable_amount: Decimal


@dataclass(frozen=True)
class RecoveryDetailGroup:
    cost_center: str
    rows: list[RecoveryOrderRow]
    group_total_order_amount: Decimal
    group_total_recoverable_amount: Decimal


def _row_sort_key(row: RecoveryOrderRow) -> tuple[str, date, str]:
    sort_date = row.order_date or date.min
    return (row.cost_center, sort_date, row.order_number)


def _build_grouped_rows(rows: Iterable[RecoveryOrderRow]) -> list[RecoveryDetailGroup]:
    rows_by_cost_center: dict[str, list[RecoveryOrderRow]] = {}
    for row in sorted(rows, key=_row_sort_key):
        rows_by_cost_center.setdefault(row.cost_center, []).append(row)

    return [
        RecoveryDetailGroup(
            cost_center=cost_center,
            rows=cost_center_rows,
            group_total_order_amount=sum(
                (row.order_amount for row in cost_center_rows), Decimal("0")
            ),
            group_total_recoverable_amount=sum(
                (row.order_amount for row in cost_center_rows), Decimal("0")
            ),
        )
        for cost_center, cost_center_rows in rows_by_cost_center.items()
    ]


def _build_summary_rows(
    grouped_rows: Iterable[RecoveryDetailGroup],
) -> list[RecoverySummaryRow]:
    return [
        RecoverySummaryRow(
            cost_center=group.cost_center,
            count=len(group.rows),
            total_order_amount=group.group_total_order_amount,
            total_recoverable_amount=group.group_total_recoverable_amount,
        )
        for group in grouped_rows
    ]


def build_context(
    *,
    rows: Iterable[RecoveryOrderRow],
    report_date: date,
    run_environment: str,
    company_name: str = "The Shaw Ventures",
    auto_cleared_order_numbers_text: str = "",
) -> dict[str, object]:
    row_list = list(rows)
    grouped_rows = _build_grouped_rows(row_list)
    summary_rows = _build_summary_rows(grouped_rows)
    summary_grand_total_order_amount = sum(
        (row.total_order_amount for row in summary_rows), Decimal("0")
    )
    summary_grand_total_recoverable_amount = sum(
        (row.total_recoverable_amount for row in summary_rows), Decimal("0")
    )
    return {
        "company_name": company_name,
        "report_date_display": report_date.strftime("%d-%b-%Y"),
        "report_date_value": report_date,
        "run_environment": run_environment,
        "rows": row_list,
        "grouped_rows": grouped_rows,
        "total_recoverable": summary_grand_total_recoverable_amount,
        "summary_rows": summary_rows,
        "summary_grand_total_order_amount": summary_grand_total_order_amount,
        "summary_grand_total_recoverable_amount": summary_grand_total_recoverable_amount,
        "auto_cleared_order_numbers_text": auto_cleared_order_numbers_text,
    }


__all__ = [
    "DOCUMENT_TYPE",
    "PIPELINE_OUTPUT_PREFIX",
    "RecoveryDetailGroup",
    "RecoverySummaryRow",
    "TEMPLATE_NAME",
    "build_context",
]
