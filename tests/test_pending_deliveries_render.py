from decimal import Decimal

from app.reports.pending_deliveries.render import _format_amount


def test_format_amount_keeps_paise_precision_with_indian_grouping() -> None:
    assert _format_amount(Decimal("216.50")) == "216.50"
    assert _format_amount(Decimal("1234567.40")) == "12,34,567.40"


def test_format_amount_handles_none_and_rounding() -> None:
    assert _format_amount(None) == "0.00"
    assert _format_amount(Decimal("216.499")) == "216.50"
    assert _format_amount(Decimal("-1234.5")) == "-1,234.50"
