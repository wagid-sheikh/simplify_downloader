from datetime import date, timedelta
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
        order_amount=Decimal("1000.00"),
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
            "run_environment": "prod",
            "summary_sections": [summary_section],
            "cost_center_sections": [cost_center_section],
            "total_count": 1,
            "total_pending_amount": Decimal("800.00"),
        }
    )

    assert "<strong>Report Date:</strong> 20-May-2025" in html
    assert "<strong>Run ID:</strong> run-1" in html
    assert "<strong>Timezone:</strong> Asia/Kolkata" in html
    assert "<strong>Run Environment:</strong> prod" in html
    assert "<th>Due Date</th>" in html
    assert '<th class="text-right">Order Amount</th>' in html
    assert "Payment Action Required" not in html
    assert "10-May-2025</td>" in html
    assert "12-May-2025</td>" in html


def test_render_html_uses_16_30_bucket_without_15_plus_bucket() -> None:
    def row_for_age(age_days: int) -> PendingDeliveryRow:
        return PendingDeliveryRow(
            cost_center="UN3668",
            store_code="A668",
            order_number=f"ORD-{age_days}",
            customer_name=f"Customer {age_days}",
            order_date=date(2025, 5, 20) - timedelta(days=age_days),
            default_due_date=date(2025, 5, 20) - timedelta(days=age_days),
            age_days=age_days,
            order_amount=Decimal("100.00"),
            paid_amount=Decimal("0.00"),
            pending_amount=Decimal("100.00"),
            adjustments=Decimal("0"),
            is_edited_order=False,
            is_duplicate=False,
            source_system="TumbleDry",
        )

    rows = [row_for_age(16), row_for_age(30)]
    bucket = PendingDeliveriesBucket(
        label="16-30 days",
        min_days=16,
        max_days=30,
        rows=rows,
        total_count=2,
        total_pending_amount=Decimal("200.00"),
    )
    summary_section = PendingDeliveriesSummarySection(
        cost_center="UN3668",
        buckets=[bucket],
        total_pending_amount=Decimal("200.00"),
        total_count=2,
    )
    cost_center_section = PendingDeliveriesCostCenterSection(
        cost_center="UN3668",
        buckets=[bucket],
        total_pending_amount=Decimal("200.00"),
        total_count=2,
    )

    html = render_html(
        {
            "report_date_display": "20-May-2025",
            "run_id": "run-1",
            "timezone": "Asia/Kolkata",
            "run_environment": "prod",
            "summary_sections": [summary_section],
            "cost_center_sections": [cost_center_section],
            "total_count": 2,
            "total_pending_amount": Decimal("200.00"),
        }
    )

    assert "16-30 days" in html
    assert ">15 days" not in html
    assert "ORD-16" in html
    assert "ORD-30" in html
    assert "ORD-31" not in html
