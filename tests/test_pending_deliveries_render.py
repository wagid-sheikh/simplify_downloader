from datetime import date
from decimal import Decimal

from app.reports.pending_deliveries.data import (
    PendingDeliveriesBucket,
    PendingDeliveriesCostCenterSection,
    PendingDeliveriesSummarySection,
    PendingDeliveryRow,
)
from app.reports.pending_deliveries.render import _format_amount, render_html


def test_format_amount_keeps_paise_precision_with_indian_grouping() -> None:
    assert _format_amount(Decimal("216.50")) == "216.50"
    assert _format_amount(Decimal("1234567.40")) == "12,34,567.40"


def test_format_amount_handles_none_and_rounding() -> None:
    assert _format_amount(None) == "0.00"
    assert _format_amount(Decimal("216.499")) == "216.50"
    assert _format_amount(Decimal("-1234.5")) == "-1,234.50"


def test_render_html_includes_due_date_column_and_value() -> None:
    row = PendingDeliveryRow(
        cost_center="UN3668",
        store_code="A668",
        order_number="ORD-1",
        customer_name="Customer 1",
        order_date=date(2025, 5, 10),
        default_due_date=date(2025, 5, 12),
        age_days=8,
        gross_amount=Decimal("1000.00"),
        paid_amount=Decimal("200.00"),
        pending_amount=Decimal("800.00"),
        adjustments=Decimal("0"),
        is_edited_order=False,
        is_duplicate=False,
        source_system="TumbleDry",
    )
    bucket = PendingDeliveriesBucket(
        label="6-15 days",
        min_days=6,
        max_days=15,
        rows=[row],
        total_count=1,
        total_pending_amount=Decimal("800.00"),
    )
    summary_section = PendingDeliveriesSummarySection(
        cost_center="UN3668",
        buckets=[bucket],
        total_pending_amount=Decimal("800.00"),
        total_count=1,
    )
    cost_center_section = PendingDeliveriesCostCenterSection(
        cost_center="UN3668",
        buckets=[bucket],
        total_pending_amount=Decimal("800.00"),
        total_count=1,
    )

    html = render_html(
        {
            "report_date_display": "20-May-2025",
            "run_id": "run-1",
            "timezone": "Asia/Kolkata",
            "summary_sections": [summary_section],
            "cost_center_sections": [cost_center_section],
            "total_count": 1,
            "total_pending_amount": Decimal("800.00"),
        }
    )

    assert "<th>Due Date</th>" in html
    assert "10-May-2025</td>" in html
    assert "12-May-2025</td>" in html
