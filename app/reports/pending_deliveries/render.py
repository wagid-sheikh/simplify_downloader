from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Mapping

from jinja2 import Environment, FileSystemLoader, select_autoescape

from app.dashboard_downloader.json_logger import JsonLogger
from app.dashboard_downloader.report_generator import render_pdf_with_configured_browser

TEMPLATE_NAME = "report.html"
TEMPLATE_DIR = Path("app") / "reports" / "pending_deliveries" / "templates"


def _format_amount(value: Decimal | int | float | None) -> str:
    if value is None:
        return "0.00"
    try:
        numeric = Decimal(str(value))
    except Exception:  # pragma: no cover - defensive
        return "0.00"
    rounded = numeric.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    sign = "-" if rounded < 0 else ""
    absolute = abs(rounded)
    integer_part, _, decimal_part = format(absolute, "f").partition(".")
    return f"{sign}{_format_indian_number(int(integer_part))}.{decimal_part[:2]}"


def _format_indian_number(value: int) -> str:
    digits = str(value)
    if len(digits) <= 3:
        return digits
    last_three = digits[-3:]
    remaining = digits[:-3]
    chunks: list[str] = []
    while len(remaining) > 2:
        chunks.insert(0, remaining[-2:])
        remaining = remaining[:-2]
    if remaining:
        chunks.insert(0, remaining)
    return ",".join(chunks + [last_three])


def render_html(context: Mapping[str, object]) -> str:
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    env.filters["format_amount"] = _format_amount
    template = env.get_template(TEMPLATE_NAME)
    return template.render(**context)


async def render_pdf(html: str, output_path: Path, *, logger: JsonLogger) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    await render_pdf_with_configured_browser(
        html,
        output_path,
        pdf_options={"format": "A4", "landscape": True},
        logger=logger,
    )
