from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Sequence

from jinja2 import Environment, FileSystemLoader, select_autoescape

from .data import MTDSameDayFulfillmentRow

TEMPLATE_DIR = Path("app") / "reports" / "mtd_same_day_fulfillment" / "templates"


def _format_amount(value: Decimal | int | float | None) -> str:
    if value is None:
        return "0"
    return f"{Decimal(str(value)):.0f}"


def render_html(
    *,
    rows: Sequence[MTDSameDayFulfillmentRow],
    report_date_display: str,
    mtd_start_display: str,
    mtd_end_display: str,
) -> str:
    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), autoescape=select_autoescape(["html", "xml"]))
    env.filters["format_amount"] = _format_amount
    template = env.get_template("report.html")
    return template.render(
        rows=rows,
        report_date_display=report_date_display,
        mtd_start_display=mtd_start_display,
        mtd_end_display=mtd_end_display,
    )
