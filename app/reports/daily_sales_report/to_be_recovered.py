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
    total_order_amount: Decimal
    total_recoverable_amount: Decimal


def _build_summary_rows(rows: Iterable[RecoveryOrderRow]) -> list[RecoverySummaryRow]:
    totals_by_cost_center: dict[str, Decimal] = {}
    for row in rows:
        totals_by_cost_center[row.cost_center] = (
            totals_by_cost_center.get(row.cost_center, Decimal("0")) + row.order_amount
        )

    return [
        RecoverySummaryRow(
            cost_center=cost_center,
            total_order_amount=total_order_amount,
            total_recoverable_amount=total_order_amount,
        )
        for cost_center, total_order_amount in totals_by_cost_center.items()
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
    summary_rows = _build_summary_rows(row_list)
    summary_grand_total_order_amount = sum(
        (row.total_order_amount for row in summary_rows), Decimal("0")
    )
    summary_grand_total_recoverable_amount = sum(
        (row.total_recoverable_amount for row in summary_rows), Decimal("0")
    )
    return {
        "company_name": company_name,
        "report_date_display": report_date.strftime("%d-%b-%Y"),
        "run_environment": run_environment,
        "rows": row_list,
        "total_recoverable": summary_grand_total_recoverable_amount,
        "summary_rows": summary_rows,
        "summary_grand_total_order_amount": summary_grand_total_order_amount,
        "summary_grand_total_recoverable_amount": summary_grand_total_recoverable_amount,
        "auto_cleared_order_numbers_text": auto_cleared_order_numbers_text,
    }


__all__ = [
    "DOCUMENT_TYPE",
    "PIPELINE_OUTPUT_PREFIX",
    "RecoverySummaryRow",
    "TEMPLATE_NAME",
    "build_context",
]
