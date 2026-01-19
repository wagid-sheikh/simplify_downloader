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
            collections_ftd=Decimal("0"),
            collections_mtd=Decimal("0"),
            collections_lmtd=Decimal("0"),
            target=uttam_target,
            achieved=uttam_achieved,
            ttd=uttam_ttd,
            delta=uttam_achieved - uttam_target,
            reqd_per_day=Decimal("0"),
            orders_sync_time="09:00",
        ),
        DailySalesRow(
            cost_center="CC-2",
            cost_center_name="Kirti Nagar",
            target_type="value",
            sales_ftd=Decimal("0"),
            sales_mtd=kirti_achieved,
            sales_lmtd=Decimal("0"),
            collections_ftd=Decimal("0"),
            collections_mtd=Decimal("0"),
            collections_lmtd=Decimal("0"),
            target=kirti_target,
            achieved=kirti_achieved,
            ttd=kirti_ttd,
            delta=kirti_achieved - kirti_target,
            reqd_per_day=Decimal("0"),
            orders_sync_time="09:00",
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
        missed_leads=[],
    )

    html = _render_html(
        {
            "company_name": "The Shaw Ventures",
            "report_date_display": report_date.strftime("%d-%b-%Y"),
            "rows": report_data.rows,
            "totals": report_data.totals,
            "edited_orders": report_data.edited_orders,
            "edited_orders_totals": report_data.edited_orders_totals,
            "missed_leads": report_data.missed_leads,
        }
    )

    assert uttam_ttd == Decimal("307")
    assert kirti_ttd == Decimal("-41117")
    assert html.count('class="ttd-negative"') == 2
    assert "-41,117" in html
    assert "-40,811" in html
