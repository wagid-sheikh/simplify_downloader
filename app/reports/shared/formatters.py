from __future__ import annotations

from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal


def format_amount(value: Decimal | int | float | None) -> str:
    if value is None:
        return "0"
    try:
        numeric = Decimal(str(value))
    except Exception:  # pragma: no cover - defensive
        return "0"
    rounded = int(numeric.to_integral_value(rounding=ROUND_HALF_UP))
    sign = "-" if rounded < 0 else ""
    return f"{sign}{_format_indian_number(abs(rounded))}"


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


def format_ddmmyyyy(value: object | None) -> str:
    if value is None:
        return "--"
    if isinstance(value, datetime):
        return value.date().strftime("%d-%m-%Y")
    if isinstance(value, date):
        return value.strftime("%d-%m-%Y")
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return "--"
        try:
            return date.fromisoformat(text).strftime("%d-%m-%Y")
        except ValueError:
            return text
    return str(value)


def format_hhmm_ampm(value: object | None) -> str:
    if value is None:
        return "--"
    if isinstance(value, datetime):
        return value.strftime("%I:%M %p")
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return "--"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return "--"
        return parsed.strftime("%I:%M %p")
    return "--"
