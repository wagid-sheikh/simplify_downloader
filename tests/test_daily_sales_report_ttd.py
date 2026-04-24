from datetime import date
from decimal import Decimal

from app.reports.daily_sales_report.data import DailySalesReportData, DailySalesRow, _calculate_ttd, _totals_row
from app.reports.daily_sales_report.pipeline import _render_html


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
            "td_leads_sync_metrics": report_data.td_leads_sync_metrics,
            "td_leads_sync_lead_changes": report_data.td_leads_sync_lead_changes,
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
                "customer_cancelled_count": 1,
                "store_cancelled_rows": [
                    {
                        "customer_name": "Alice",
                        "mobile": "9999999999",
                        "flag": "store",
                        "reason": "--",
                    },
                    {
                        "customer_name": "Bob",
                        "mobile": "8888888888",
                        "flag": "store",
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
            "td_leads_sync_metrics": report_data.td_leads_sync_metrics,
            "td_leads_sync_lead_changes": report_data.td_leads_sync_lead_changes,
        }
    )

    assert "Run Environment: prod" in html
    assert html.index("Pickup & Delivery KPIs Report for:") < html.index("Run Environment: prod")
    assert "Missed Leads for this month" in html
    assert "Uttam Nagar New" in html
    assert "(9999999999, Alice), (8888888888, Bob)" in html
    assert "Cancelled Leads for this Month" in html
    assert "Uttam Nagar" in html
    assert ">3<" in html
    assert "Alice (9999999999) [store]" in html
    assert "Bob (8888888888) [store: No stock]" in html
    assert "Lead Performance Summary (MTD)" in html
    assert "HEALTHY" in html
    assert "EXCELLENT" in html
    assert "FOLLOW_UP_GAP" in html
    assert "metric-yellow" in html
    assert "metric-green" in html
    assert "metric-red" in html
    assert "Sync Group" not in html
    assert html.index("Pickup & Delivery KPIs") < html.index("Missed Leads for this month")
    assert "TD Leads Sync Lead Changes (Actionable Details)" in html
    assert "Alice" in html


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
            "td_leads_sync_metrics": report_data.td_leads_sync_metrics,
            "td_leads_sync_lead_changes": report_data.td_leads_sync_lead_changes,
        }
    )

    assert "Cancelled Leads for this Month" in html
    assert ">None<" in html
