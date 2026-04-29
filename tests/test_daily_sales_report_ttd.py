from datetime import date
from decimal import Decimal

from app.reports.daily_sales_report.data import (
    DailySalesReportData,
    DailySalesRow,
    RecoveryOrderRow,
    SameDayFulfillmentRow,
    _calculate_ttd,
    _totals_row,
)
from app.reports.daily_sales_report.pipeline import _build_context, _render_html


def test_daily_sales_report_ttd_calculation_and_rendering() -> None:
    report_date = date(2026, 1, 19)
    day_of_month = report_date.day
    days_in_month = 31

    uttam_target = Decimal("270000")
    uttam_achieved = Decimal("165790")
    uttam_ttd = _calculate_ttd(uttam_target, uttam_achieved, day_of_month, days_in_month)

    kirti_target = Decimal("270000")
    kirti_achieved = Decimal("124366")
    kirti_ttd = _calculate_ttd(kirti_target, kirti_achieved, day_of_month, days_in_month)

    rows = [
        DailySalesRow(
            cost_center="CC-1",
            cost_center_name="Uttam Nagar",
            target_type="value",
            sales_ftd=Decimal("0"),
            sales_mtd=uttam_achieved,
            sales_lmtd=Decimal("0"),
            orders_count_ftd=0,
            orders_count_mtd=0,
            orders_count_lmtd=0,
            collections_ftd=Decimal("0"),
            collections_mtd=Decimal("0"),
            collections_lmtd=Decimal("0"),
            collections_count_ftd=0,
            collections_count_mtd=0,
            collections_count_lmtd=0,
            target=uttam_target,
            achieved=uttam_achieved,
            ttd=uttam_ttd,
            delta=uttam_achieved - uttam_target,
            reqd_per_day=Decimal("0"),
            orders_sync_time="09:00",
            pickup_new_conv_pct=None,
            pickup_existing_conv_pct=None,
            pickup_total_count=None,
            pickup_total_conv_pct=None,
            delivery_tat_pct=None,
            kpi_snapshot_label="--",
        ),
        DailySalesRow(
            cost_center="CC-2",
            cost_center_name="Kirti Nagar",
            target_type="value",
            sales_ftd=Decimal("0"),
            sales_mtd=kirti_achieved,
            sales_lmtd=Decimal("0"),
            orders_count_ftd=0,
            orders_count_mtd=0,
            orders_count_lmtd=0,
            collections_ftd=Decimal("0"),
            collections_mtd=Decimal("0"),
            collections_lmtd=Decimal("0"),
            collections_count_ftd=0,
            collections_count_mtd=0,
            collections_count_lmtd=0,
            target=kirti_target,
            achieved=kirti_achieved,
            ttd=kirti_ttd,
            delta=kirti_achieved - kirti_target,
            reqd_per_day=Decimal("0"),
            orders_sync_time="09:00",
            pickup_new_conv_pct=None,
            pickup_existing_conv_pct=None,
            pickup_total_count=None,
            pickup_total_conv_pct=None,
            delivery_tat_pct=None,
            kpi_snapshot_label="--",
        ),
    ]

    totals = _totals_row(rows)
    totals.ttd = _calculate_ttd(totals.target, totals.achieved, day_of_month, days_in_month)

    report_data = DailySalesReportData(
        report_date=report_date,
        rows=rows,
        totals=totals,
        edited_orders=[],
        edited_orders_totals=None,
        edited_orders_summary=None,
        missed_leads=[],
        cancelled_leads=[],
        lead_performance_summary=[],
        td_leads_sync_metrics={},
        td_leads_sync_lead_changes={},
    )

    html = _render_html(
        {
            "company_name": "The Shaw Ventures",
            "report_date_display": report_date.strftime("%d-%b-%Y"),
            "run_environment": "prod",
            "rows": report_data.rows,
            "totals": report_data.totals,
            "edited_orders": report_data.edited_orders,
            "edited_orders_summary": report_data.edited_orders_summary,
            "edited_orders_totals": report_data.edited_orders_totals,
            "missed_leads": report_data.missed_leads,
            "cancelled_leads": report_data.cancelled_leads,
        }
    )

    assert "Run Environment: prod" in html
    assert html.index("Pickup & Delivery KPIs Report for:") < html.index("Run Environment: prod")
    assert uttam_ttd == Decimal("307")
    assert kirti_ttd == Decimal("-41117")
    assert html.count('class="ttd-negative"') == 2
    assert "0 orders" in html
    assert "-41,117" in html
    assert "-40,811" in html


def test_daily_sales_report_missed_leads_micro_layout_rendering() -> None:
    report_date = date(2026, 1, 19)
    rows = [
        DailySalesRow(
            cost_center="CC-1",
            cost_center_name="Uttam Nagar",
            target_type="value",
            sales_ftd=Decimal("0"),
            sales_mtd=Decimal("100"),
            sales_lmtd=Decimal("90"),
            orders_count_ftd=0,
            orders_count_mtd=1,
            orders_count_lmtd=1,
            collections_ftd=Decimal("0"),
            collections_mtd=Decimal("0"),
            collections_lmtd=Decimal("0"),
            collections_count_ftd=0,
            collections_count_mtd=0,
            collections_count_lmtd=0,
            target=Decimal("1000"),
            achieved=Decimal("100"),
            ttd=Decimal("0"),
            delta=Decimal("-900"),
            reqd_per_day=Decimal("0"),
            orders_sync_time="09:00",
            pickup_new_conv_pct=None,
            pickup_existing_conv_pct=None,
            pickup_total_count=None,
            pickup_total_conv_pct=None,
            delivery_tat_pct=None,
            kpi_snapshot_label="--",
        )
    ]
    totals = _totals_row(rows)

    report_data = DailySalesReportData(
        report_date=report_date,
        rows=rows,
        totals=totals,
        edited_orders=[],
        edited_orders_totals=None,
        edited_orders_summary=None,
        missed_leads=[
            {
                "store_name": "Uttam Nagar",
                "customer_type": "New",
                "leads": [
                    {"mobile_number": "9999999999", "customer_name": "Alice"},
                    {"mobile_number": "8888888888", "customer_name": "Bob"},
                ],
            }
        ],
        cancelled_leads=[
            {
                "store_name": "Uttam Nagar",
                "total_cancelled_count": 3,
                "customer_cancelled_count": 2,
                "store_cancelled_rows": [
                    {
                        "customer_name": "Bob",
                        "mobile": "8888888888",
                        "reason": "No stock",
                    },
                ],
            }
        ],
        lead_performance_summary=[
            {
                "store": "UN",
                "store_name": "Uttam Nagar",
                "period_start": "2026-01-01",
                "period_end": "2026-01-19",
                "total_leads": 10,
                "completed_leads": 8,
                "cancelled_leads": 1,
                "pending_leads": 1,
                "conversion_pct": {"value": 80.0, "color": "YELLOW", "status": "HEALTHY"},
                "cancelled_pct": {"value": 10.0, "color": "GREEN", "status": "EXCELLENT"},
                "pending_pct": {"value": 10.0, "color": "RED", "status": "FOLLOW_UP_GAP"},
            }
        ],
        td_leads_sync_metrics={},
        td_leads_sync_lead_changes={
            "stores": [
                {
                    "store_code": "UN",
                    "created_by_bucket": [
                        {
                            "status_bucket": "pending",
                            "rows": [
                                {"customer_name": "Alice", "mobile": "9999999999", "current_status_bucket": "pending", "previous_status_bucket": None}
                            ],
                            "overflow_count": 0,
                        }
                    ],
                    "updated_by_bucket": [],
                    "transitions": [],
                }
            ]
        },
        to_be_recovered=[
            RecoveryOrderRow(
                cost_center="UN",
                order_number="ORD-REC-1",
                order_date=date(2026, 1, 3),
                customer_name="Chris",
                mobile_number="9999999998",
                order_value=Decimal("1250"),
            )
        ],
        to_be_compensated=[
            RecoveryOrderRow(
                cost_center="KN",
                order_number="ORD-COMP-1",
                order_date=date(2026, 1, 4),
                customer_name="Dana",
                mobile_number="9999999997",
                order_value=Decimal("840"),
            )
        ],
        to_be_recovered_total_order_value=Decimal("1250"),
        to_be_compensated_total_order_value=Decimal("840"),
    )

    html = _render_html(
        {
            "company_name": "The Shaw Ventures",
            "report_date_display": report_date.strftime("%d-%b-%Y"),
            "run_environment": "prod",
            "rows": report_data.rows,
            "totals": report_data.totals,
            "edited_orders": report_data.edited_orders,
            "edited_orders_summary": report_data.edited_orders_summary,
            "edited_orders_totals": report_data.edited_orders_totals,
            "missed_leads": report_data.missed_leads,
            "cancelled_leads": report_data.cancelled_leads,
            "lead_performance_summary": report_data.lead_performance_summary,
            "to_be_recovered": report_data.to_be_recovered,
            "to_be_compensated": report_data.to_be_compensated,
            "to_be_recovered_total_order_value": report_data.to_be_recovered_total_order_value,
            "to_be_compensated_total_order_value": report_data.to_be_compensated_total_order_value,
        }
    )

    assert "Run Environment: prod" in html
    assert html.index("Pickup & Delivery KPIs Report for:") < html.index("Run Environment: prod")
    assert "Missed Leads for this month" in html
    assert "Uttam Nagar New" in html
    assert "(9999999999, Alice), (8888888888, Bob)" in html
    assert "Cancelled Leads for this Month" in html
    assert "Store-Cancelled Leads" in html
    assert "Uttam Nagar" in html
    assert ">3<" in html
    assert "(8888888888, Bob - No stock)" in html
    assert ", store," not in html
    assert "(9999999999, Alice - --)" not in html
    assert "Customer Cancelled" not in html
    assert "Lead Performance Summary (MTD)" in html
    assert "HEALTHY" in html
    assert "EXCELLENT" in html
    assert "FOLLOW_UP_GAP" in html
    assert "metric-yellow" in html
    assert "metric-green" in html
    assert "metric-red" in html
    assert "01-01-2026 to 19-01-2026" in html
    assert "03-01-2026" in html
    assert "04-01-2026" in html
    assert "Sync Group" not in html
    assert html.index("Pickup & Delivery KPIs") < html.index("Missed Leads for this month")
    assert "TD Leads Sync Upsert Metrics (Latest Run)" not in html
    assert "TD Leads Sync Lead Changes (Actionable Details)" not in html
    assert "Alice" in html
    assert "Net Amount" in html
    assert "Payment Received" in html


def test_daily_sales_report_cancelled_leads_empty_state_rendering() -> None:
    report_date = date(2026, 1, 19)
    rows = []
    totals = _totals_row(rows)
    report_data = DailySalesReportData(
        report_date=report_date,
        rows=rows,
        totals=totals,
        edited_orders=[],
        edited_orders_totals=None,
        edited_orders_summary=None,
        missed_leads=[],
        cancelled_leads=[],
        lead_performance_summary=[],
        td_leads_sync_metrics={},
        td_leads_sync_lead_changes={},
    )

    html = _render_html(
        {
            "company_name": "The Shaw Ventures",
            "report_date_display": report_date.strftime("%d-%b-%Y"),
            "run_environment": "prod",
            "rows": report_data.rows,
            "totals": report_data.totals,
            "edited_orders": report_data.edited_orders,
            "edited_orders_summary": report_data.edited_orders_summary,
            "edited_orders_totals": report_data.edited_orders_totals,
            "missed_leads": report_data.missed_leads,
            "cancelled_leads": report_data.cancelled_leads,
            "lead_performance_summary": report_data.lead_performance_summary,
        }
    )

    assert "Cancelled Leads for this Month" in html
    assert ">None<" in html


def test_daily_sales_report_same_day_section_uses_shared_table_partial() -> None:
    report = DailySalesReportData(
        report_date=date(2026, 4, 29),
        rows=[],
        totals=_totals_row([]),
        edited_orders=[],
        edited_orders_summary={},
        edited_orders_totals={},
        missed_leads=[],
        cancelled_leads=[],
        lead_performance_summary=[],
        td_leads_sync_metrics={},
        td_leads_sync_lead_changes={},
        to_be_recovered=[],
        to_be_compensated=[],
        to_be_recovered_total_order_value=Decimal("0"),
        to_be_compensated_total_order_value=Decimal("0"),
        same_day_fulfillment_rows=[
            SameDayFulfillmentRow(
                store_code="TD01",
                order_number="ORD-1",
                order_date=date(2026, 4, 29),
                customer_name="Jane",
                mobile_number="9999999999",
                line_items="Shirt x1",
                delivery_or_payment_date=date(2026, 4, 29),
                payment_mode="UPI",
                hours=Decimal("2.5"),
                net_amount=Decimal("500"),
                payment_received=Decimal("500"),
            )
        ],
    )
    html = _render_html(_build_context(report, "prod"))

    assert "Same-Day Fulfillment (Created &amp; Delivered/Paid on Report Date)" in html
    assert "Store: TD01" in html
    assert "ORD-1" in html
    assert "Payment Date" in html
    assert "Delivery/Payment Date" not in html
    assert "Customer</th>" in html
    assert "150 min" in html
    assert "Payment Received" in html
