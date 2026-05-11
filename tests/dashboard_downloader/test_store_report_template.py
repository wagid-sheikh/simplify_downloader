from __future__ import annotations

from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape


def _base_store_context() -> dict[str, object]:
    return {
        "store_name": "Demo Store",
        "report_date": "2026-05-10",
        "generated_at": "2026-05-10T12:00:00+00:00",
        "run_id": "run-1",
        "logo_src": None,
        "overall_health_status_class": "status-good",
        "overall_health_score": 80,
        "overall_health_label": "Stable",
        "overall_health_summary": "Stable operations.",
        "pickup_conversion_total_pct": 90,
        "pickup_conversion_total_status_class": "kpi-status-excellent",
        "pickup_conversion_total_status_label": "Excellent",
        "pickup_conversion_new_pct": 85,
        "pickup_conversion_new_status_class": "kpi-status-good",
        "pickup_conversion_new_status_label": "Stable",
        "pickup_conversion_existing_pct": 95,
        "pickup_conversion_existing_status_class": "kpi-status-excellent",
        "pickup_conversion_existing_status_label": "Excellent",
        "delivery_tat_pct": 92,
        "delivery_tat_status_class": "kpi-status-good",
        "delivery_tat_status_label": "Stable",
        "undelivered_10_plus_count": 1,
        "undelivered_10_plus_status_class": "kpi-status-good",
        "undelivered_10_plus_status_label": "Stable",
        "undelivered_total_count": 2,
        "undelivered_total_status_class": "kpi-status-good",
        "undelivered_total_status_label": "Stable",
        "repeat_customer_pct": 70,
        "repeat_customer_status_class": "kpi-status-good",
        "repeat_customer_status_label": "Stable",
        "ftd_revenue": 1000,
        "high_value_orders_count": 1,
        "undelivered_snapshot_count": 2,
        "leads_yday": 2,
        "leads_today": 3,
        "leads_delta": 1,
        "leads_change_class": "snapshot-positive",
        "leads_note": "Up",
        "pickups_yday": 2,
        "pickups_today": 2,
        "pickups_delta": 0,
        "pickups_change_class": "snapshot-neutral",
        "pickups_note": "Flat",
        "deliveries_yday": 1,
        "deliveries_today": 2,
        "deliveries_delta": 1,
        "deliveries_change_class": "snapshot-positive",
        "deliveries_note": "Up",
        "new_customers_yday": 1,
        "new_customers_today": 1,
        "new_customers_delta": 0,
        "new_customers_change_class": "snapshot-neutral",
        "new_customers_note": "Flat",
        "repeat_customers_yday": 1,
        "repeat_customers_today": 2,
        "repeat_customers_delta": 1,
        "repeat_customers_change_class": "snapshot-positive",
        "repeat_customers_note": "Up",
        "highlights": ["Good execution."],
        "focus_areas": ["Keep focus."],
        "actions_today": ["Follow up."],
        "undelivered_orders_total_amount": 1234.5,
        "missed_leads_rows": [],
    }


def test_store_report_template_labels_undelivered_order_amount() -> None:
    template_dir = Path("app") / "dashboard_downloader" / "templates"
    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    template = env.get_template("store_report.html")
    context = _base_store_context()
    context["undelivered_orders_rows"] = [
        {
            "order_id": "ORD-1",
            "order_date": "2026-05-09",
            "committed_date": "2026-05-10",
            "age_days": 0,
            "order_amount": 1234.5,
        }
    ]

    html = template.render(**context)

    assert '<th class="text-right">Order Amount</th>' in html
    assert "Net Amount" not in html
    assert "1234.50" in html
