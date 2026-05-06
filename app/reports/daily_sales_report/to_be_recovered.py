from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Iterable

from .data import RecoveryOrderRow

TEMPLATE_NAME = "to_be_recovered_report.html"
DOCUMENT_TYPE = "to_be_recovered_report_pdf"
PIPELINE_OUTPUT_PREFIX = "reports.to_be_recovered"


def build_context(
    *,
    rows: Iterable[RecoveryOrderRow],
    report_date: date,
    run_environment: str,
    company_name: str = "The Shaw Ventures",
) -> dict[str, object]:
    row_list = list(rows)
    total_recoverable = sum((row.order_value for row in row_list), Decimal("0"))
    return {
        "company_name": company_name,
        "report_date_display": report_date.strftime("%d-%b-%Y"),
        "run_environment": run_environment,
        "rows": row_list,
        "total_recoverable": total_recoverable,
    }


__all__ = ["DOCUMENT_TYPE", "PIPELINE_OUTPUT_PREFIX", "TEMPLATE_NAME", "build_context"]
