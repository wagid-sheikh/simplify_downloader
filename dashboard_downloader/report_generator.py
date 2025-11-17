from __future__ import annotations

import math
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

from playwright.async_api import async_playwright
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from simplify_downloader.common.db import session_scope
from simplify_downloader.common.dashboard_store import (
    store_dashboard_summary,
    store_master,
)
from simplify_downloader.common.ingest.models import MissedLead, RepeatCustomer, UndeliveredOrder

__all__ = [
    "StoreReportDataNotFound",
    "build_store_context",
    "render_store_report_pdf",
]


class StoreReportDataNotFound(RuntimeError):
    """Raised when no dashboard data is available for the requested store/date."""


@dataclass
class MetricStatus:
    value: float | int | None
    label: str
    status_key: str
    css_class: str
    score: int


KPI_RULES: Dict[str, Dict[str, Any]] = {
    "pickup_conversion_total_pct": {
        "direction": "higher",
        "excellent": 82,
        "good": 70,
        "poor": 55,
    },
    "pickup_conversion_new_pct": {
        "direction": "higher",
        "excellent": 75,
        "good": 60,
        "poor": 45,
    },
    "delivery_tat_pct": {
        "direction": "higher",
        "excellent": 92,
        "good": 85,
        "poor": 70,
    },
    "undelivered_10_plus_count": {
        "direction": "lower",
        "excellent": 10,
        "good": 20,
        "poor": 35,
    },
    "undelivered_total_count": {
        "direction": "lower",
        "excellent": 25,
        "good": 45,
        "poor": 65,
    },
    "repeat_customer_pct": {
        "direction": "higher",
        "excellent": 65,
        "good": 50,
        "poor": 35,
    },
}

STATUS_CLASS_MAP = {
    "excellent": "kpi-status-excellent",
    "good": "kpi-status-good",
    "poor": "kpi-status-poor",
    "critical": "kpi-status-critical",
    "missing": "kpi-status-missing",
}

OVERALL_CLASS_MAP = {
    "excellent": "status-excellent",
    "good": "status-good",
    "warning": "status-warning",
    "critical": "status-critical",
}


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, Decimal):
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


async def _fetch_dashboard_summary(
    session: AsyncSession,
    store_code: str,
    report_date: date,
    *,
    comparison: bool = False,
) -> Mapping[str, Any] | None:
    summary_cols = list(store_dashboard_summary.c)
    stmt = (
        sa.select(
            *summary_cols,
            store_master.c.store_name.label("store_name"),
            store_master.c.store_code.label("store_code"),
        )
        .select_from(store_dashboard_summary)
        .join(store_master, store_master.c.id == store_dashboard_summary.c.store_id)
        .where(sa.func.upper(store_master.c.store_code) == store_code.upper())
    )
    if comparison:
        stmt = stmt.where(store_dashboard_summary.c.dashboard_date < report_date)
    else:
        stmt = stmt.where(store_dashboard_summary.c.dashboard_date == report_date)
    stmt = stmt.order_by(
        store_dashboard_summary.c.dashboard_date.desc(),
        store_dashboard_summary.c.run_date_time.desc(),
    ).limit(1)
    result = await session.execute(stmt)
    row = result.mappings().first()
    return row


def _evaluate_metric(metric: str, value: float | int | None) -> MetricStatus:
    rule = KPI_RULES.get(metric)
    if not rule or value is None:
        return MetricStatus(value=value, label="No Data", status_key="missing", css_class=STATUS_CLASS_MAP["missing"], score=0)

    numeric = float(value)
    direction = rule.get("direction", "higher")

    if direction == "higher":
        if numeric >= rule["excellent"]:
            key = "excellent"
        elif numeric >= rule["good"]:
            key = "good"
        elif numeric >= rule["poor"]:
            key = "poor"
        else:
            key = "critical"
    else:
        if numeric <= rule["excellent"]:
            key = "excellent"
        elif numeric <= rule["good"]:
            key = "good"
        elif numeric <= rule["poor"]:
            key = "poor"
        else:
            key = "critical"

    score_map = {"excellent": 100, "good": 80, "poor": 55, "critical": 35}
    label_map = {
        "excellent": "Excellent",
        "good": "Good",
        "poor": "Needs Attention",
        "critical": "Critical",
    }

    return MetricStatus(
        value=numeric,
        label=label_map[key],
        status_key=key,
        css_class=STATUS_CLASS_MAP[key],
        score=score_map[key],
    )


def _overall_health(metric_statuses: Iterable[MetricStatus]) -> Dict[str, Any]:
    valid = [status for status in metric_statuses if status.status_key != "missing"]
    if not valid:
        return {
            "overall_health_score": 0,
            "overall_health_label": "No Data",
            "overall_health_status_class": OVERALL_CLASS_MAP["warning"],
            "overall_health_summary": "Insufficient dashboard data available for this store on the selected date.",
        }
    average_score = sum(status.score for status in valid) / len(valid)
    if average_score >= 90:
        key = "excellent"
        label = "Excellent"
        summary = "Operations delivered above plan with strong conversion and service levels."
    elif average_score >= 75:
        key = "good"
        label = "Stable"
        summary = "Overall performance was stable with minor optimisations available."
    elif average_score >= 60:
        key = "warning"
        label = "At Risk"
        summary = "Mixed performance today – a few KPIs require closer monitoring tomorrow."
    else:
        key = "critical"
        label = "Critical"
        summary = "Multiple KPIs triggered alerts; immediate follow-up is required."

    return {
        "overall_health_score": int(round(average_score)),
        "overall_health_label": label,
        "overall_health_status_class": OVERALL_CLASS_MAP[key],
        "overall_health_summary": summary,
    }


def _change_note(current: float | int | None, previous: float | int | None) -> str:
    if current is None:
        return "No data captured for this metric today."
    if previous is None:
        return "No previous data available for comparison yet."
    delta = float(current) - float(previous)
    if math.isclose(delta, 0, abs_tol=0.01):
        return "Flat versus the previous data point."
    direction = "up" if delta > 0 else "down"
    return f"{direction.capitalize()} by {abs(delta):.1f} compared to the previous report."


def _build_recommendations(statuses: Dict[str, MetricStatus]) -> Dict[str, List[str]]:
    highlights: List[str] = []
    focus_areas: List[str] = []
    actions: List[str] = []

    if statuses["pickup_conversion_total_pct"].status_key in {"excellent", "good"}:
        highlights.append("Lead-to-pickup conversion held strong today, indicating solid front-desk execution.")
    if statuses["delivery_tat_pct"].status_key in {"excellent", "good"}:
        highlights.append("Delivery turnaround time stayed on track with most orders delivered within TAT.")
    if statuses["repeat_customer_pct"].status_key in {"excellent", "good"}:
        highlights.append("Repeat customers contributed meaningfully, showing healthy loyalty.")

    if statuses["pickup_conversion_total_pct"].status_key in {"poor", "critical"}:
        focus_areas.append("Pickup conversion dipped – review lead quality and outbound follow-ups.")
        actions.append("Audit today's leads and coach the team on callbacks to lift conversion.")
    if statuses["undelivered_total_count"].status_key in {"poor", "critical"}:
        focus_areas.append("Undelivered order backlog is elevated.")
        actions.append("Run an escalation huddle with the delivery team; prioritise oldest cases first.")
    if statuses["undelivered_10_plus_count"].status_key in {"poor", "critical"}:
        focus_areas.append("Orders pending beyond 10 days require urgent intervention.")
        actions.append("Call customers with >10 day delays and align revised ETAs by 11am tomorrow.")
    if statuses["repeat_customer_pct"].status_key in {"poor", "critical"}:
        focus_areas.append("Repeat customer contribution softened today.")
        actions.append("Plan a targeted WhatsApp nudge to the last 30 repeat buyers with offers.")

    if not highlights:
        highlights.append("No standout wins logged – maintain steady execution tomorrow.")
    if not focus_areas:
        focus_areas.append("Operations were stable with no major risk signals.")
    if not actions:
        actions.append("Continue standard playbook reviews with the on-duty manager.")

    return {
        "highlights": highlights,
        "focus_areas": focus_areas,
        "actions_tomorrow": actions,
    }


async def build_store_context(
    store_code: str,
    report_date: date,
    run_id: str,
    *,
    database_url: str,
    logo_src: str | None = None,
) -> Dict[str, Any]:
    if not database_url:
        raise ValueError("database_url is required to build store report context")

    normalized_code = store_code.strip().upper()

    t_minus_one = report_date - timedelta(days=1)

    async with session_scope(database_url) as session:
        summary_row = await _fetch_dashboard_summary(session, normalized_code, report_date)
        if not summary_row:
            raise StoreReportDataNotFound(
                f"no dashboard summary for store {normalized_code} on {report_date.isoformat()}"
            )
        comparison_row = await _fetch_dashboard_summary(session, normalized_code, report_date, comparison=True)

        missed_stmt = sa.select(sa.func.count()).select_from(MissedLead).where(
            sa.func.upper(MissedLead.store_code) == normalized_code,
            MissedLead.pickup_date == report_date,
        )
        missed_count = (await session.execute(missed_stmt)).scalar_one()

        prev_leads_stmt = sa.select(sa.func.count()).select_from(MissedLead).where(
            sa.func.upper(MissedLead.store_code) == normalized_code,
            MissedLead.pickup_date == t_minus_one,
        )
        previous_missed_count = (await session.execute(prev_leads_stmt)).scalar_one()

        undelivered_stmt = sa.select(sa.func.count()).select_from(UndeliveredOrder).where(
            sa.func.upper(UndeliveredOrder.store_code) == normalized_code,
            UndeliveredOrder.order_date == report_date,
        )
        undelivered_count_for_snapshot = (await session.execute(undelivered_stmt)).scalar_one()

        repeat_stmt = sa.select(sa.func.count()).select_from(RepeatCustomer).where(
            sa.func.upper(RepeatCustomer.store_code) == normalized_code,
        )
        repeat_base_count = (await session.execute(repeat_stmt)).scalar_one()

    pickup_total_pct = _as_float(summary_row.get("pickup_total_conv_pct"))
    pickup_new_pct = _as_float(summary_row.get("pickup_new_conv_pct"))
    delivery_tat_pct = _as_float(summary_row.get("delivery_tat_pct"))
    undelivered_10_plus = _as_int(summary_row.get("delivery_undel_over_10_days"))
    undelivered_total = _as_int(summary_row.get("delivery_total_undelivered"))
    repeat_pct = _as_float(summary_row.get("repeat_total_base_pct"))
    ftd_revenue = _as_float(summary_row.get("ftd_revenue"))
    high_value_orders = _as_int(summary_row.get("package_non_pkg_over_800"))

    statuses: Dict[str, MetricStatus] = {
        "pickup_conversion_total_pct": _evaluate_metric("pickup_conversion_total_pct", pickup_total_pct),
        "pickup_conversion_new_pct": _evaluate_metric("pickup_conversion_new_pct", pickup_new_pct),
        "delivery_tat_pct": _evaluate_metric("delivery_tat_pct", delivery_tat_pct),
        "undelivered_10_plus_count": _evaluate_metric("undelivered_10_plus_count", undelivered_10_plus),
        "undelivered_total_count": _evaluate_metric("undelivered_total_count", undelivered_total),
        "repeat_customer_pct": _evaluate_metric("repeat_customer_pct", repeat_pct),
    }

    overall = _overall_health(statuses.values())
    recos = _build_recommendations(statuses)

    snapshot = {
        "leads_total": missed_count,
        "leads_note": _change_note(missed_count, previous_missed_count),
        "pickups_total": _as_int(summary_row.get("pickup_total_count")) or 0,
        "pickups_note": _change_note(
            summary_row.get("pickup_total_count"),
            comparison_row.get("pickup_total_count") if comparison_row else None,
        ),
        "deliveries_total": _as_int(summary_row.get("delivery_total_delivered")) or 0,
        "deliveries_note": _change_note(
            summary_row.get("delivery_total_delivered"),
            comparison_row.get("delivery_total_delivered") if comparison_row else None,
        ),
        "new_customers_count": _as_int(summary_row.get("pickup_new_count")) or 0,
        "new_customers_note": _change_note(
            summary_row.get("pickup_new_count"),
            comparison_row.get("pickup_new_count") if comparison_row else None,
        ),
        "repeat_customers_count": _as_int(summary_row.get("repeat_orders")) or repeat_base_count,
        "repeat_customers_note": _change_note(
            summary_row.get("repeat_orders"),
            comparison_row.get("repeat_orders") if comparison_row else None,
        ),
    }

    context: Dict[str, Any] = {
        "store_name": summary_row.get("store_name") or normalized_code,
        "report_date": report_date.isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "logo_src": logo_src,
        **overall,
        "pickup_conversion_total_pct": pickup_total_pct,
        "pickup_conversion_total_status_label": statuses["pickup_conversion_total_pct"].label,
        "pickup_conversion_total_status_class": statuses["pickup_conversion_total_pct"].css_class,
        "pickup_conversion_new_pct": pickup_new_pct,
        "pickup_conversion_new_status_label": statuses["pickup_conversion_new_pct"].label,
        "pickup_conversion_new_status_class": statuses["pickup_conversion_new_pct"].css_class,
        "delivery_tat_pct": delivery_tat_pct,
        "delivery_tat_status_label": statuses["delivery_tat_pct"].label,
        "delivery_tat_status_class": statuses["delivery_tat_pct"].css_class,
        "undelivered_10_plus_count": undelivered_10_plus,
        "undelivered_10_plus_status_label": statuses["undelivered_10_plus_count"].label,
        "undelivered_10_plus_status_class": statuses["undelivered_10_plus_count"].css_class,
        "undelivered_total_count": undelivered_total,
        "undelivered_total_status_label": statuses["undelivered_total_count"].label,
        "undelivered_total_status_class": statuses["undelivered_total_count"].css_class,
        "repeat_customer_pct": repeat_pct,
        "repeat_customer_status_label": statuses["repeat_customer_pct"].label,
        "repeat_customer_status_class": statuses["repeat_customer_pct"].css_class,
        "ftd_revenue": ftd_revenue,
        "high_value_orders_count": high_value_orders,
        **snapshot,
        **recos,
        "undelivered_snapshot_count": undelivered_count_for_snapshot,
    }

    return context


async def render_store_report_pdf(store_context: Dict, template_path: str | Path, output_path: str | Path) -> None:
    from jinja2 import Environment, FileSystemLoader, select_autoescape

    template_dir = Path(template_path)
    template_dir.mkdir(parents=True, exist_ok=True)
    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    template = env.get_template("store_report.html")
    html = template.render(**store_context)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    await render_pdf_with_configured_browser(html, output_path)


async def render_pdf_with_configured_browser(html_content: str, output_path: str | Path) -> None:
    from simplify_downloader.config import config

    backend = config.pdf_render_backend.lower()
    headless = config.pdf_render_headless
    chrome_exec = config.pdf_render_chrome_executable

    async with async_playwright() as p:
        if backend == "local_chrome":
            if not chrome_exec:
                raise RuntimeError(
                    "PDF_RENDER_CHROME_EXECUTABLE must be set when PDF_RENDER_BACKEND=local_chrome"
                )
            browser = await p.chromium.launch(
                executable_path=chrome_exec,
                headless=headless,
            )
        else:
            browser = await p.chromium.launch(headless=headless)

        page = await browser.new_page()
        await page.set_content(html_content, wait_until="networkidle")
        await page.pdf(path=str(output_path), print_background=True, format="A4")
        await browser.close()
