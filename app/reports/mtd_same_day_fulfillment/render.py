from __future__ import annotations

from pathlib import Path
from typing import Sequence

from jinja2 import Environment, FileSystemLoader, select_autoescape

from app.reports.shared.formatters import format_amount, format_ddmmyyyy

from .data import MTDSameDayFulfillmentRow

TEMPLATE_DIR = Path("app") / "reports" / "mtd_same_day_fulfillment" / "templates"
SHARED_TEMPLATE_DIR = Path("app") / "reports" / "shared" / "templates"


def render_html(
    *,
    rows: Sequence[MTDSameDayFulfillmentRow],
    report_date_display: str,
    mtd_start_display: str,
    mtd_end_display: str,
) -> str:
    env = Environment(
        loader=FileSystemLoader([str(TEMPLATE_DIR), str(SHARED_TEMPLATE_DIR)]),
        autoescape=select_autoescape(["html", "xml"]),
    )
    env.filters["format_amount"] = format_amount
    env.filters["format_ddmmyyyy"] = format_ddmmyyyy
    template = env.get_template("report.html")
    return template.render(
        rows=rows,
        report_date_display=report_date_display,
        mtd_start_display=mtd_start_display,
        mtd_end_display=mtd_end_display,
    )
