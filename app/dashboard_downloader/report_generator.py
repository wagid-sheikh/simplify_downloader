from __future__ import annotations

import math
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from math import inf
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas
from reportlab.platypus import Paragraph, Table, TableStyle

from playwright.async_api import async_playwright
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.common.db import session_scope
from app.common.dashboard_store import (
    store_dashboard_summary,
    store_master,
)
from app.common.ingest.models import MissedLead, RepeatCustomer, UndeliveredOrder

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
        "excellent": 80,
        "good": 60,
        "poor": 45,
    },
    "pickup_conversion_existing_pct": {
        "direction": "higher",
        "excellent": 95,
        "good": 80,
        "poor": 70,
    },
    "delivery_tat_pct": {
        "direction": "higher",
        "excellent": 95,
        "good": 85,
        "poor": 70,
    },
    "undelivered_10_plus_count": {
        "direction": "lower",
        "excellent": 0,
        "good": 0,
        "poor": 2,
    },
    "undelivered_total_count": {
        "direction": "lower",
        "excellent": 2,
        "good": 5,
        "poor": inf,
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


def _snapshot_delta_class(
    current: float | int | None,
    previous: float | int | None,
    *,
    direction: str,
) -> str:
    if current is None or previous is None:
        return "snapshot-neutral"
    delta = float(current) - float(previous)
    if math.isclose(delta, 0, abs_tol=0.01):
        return "snapshot-neutral"
    if direction == "lower":
        improved = delta < 0
    else:
        improved = delta > 0
    return "snapshot-positive" if improved else "snapshot-negative"


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
        "actions_today": actions,
    }


def _combine_date_time(date_value: date | None, time_value: str | None) -> str:
    if date_value and time_value:
        return f"{date_value.isoformat()} {time_value}"
    if date_value:
        return date_value.isoformat()
    return time_value or ""


async def _fetch_undelivered_order_rows(
    session: AsyncSession,
    store_code: str,
    report_date: date,
) -> tuple[List[Dict[str, Any]], float]:
    stmt = (
        sa.select(
            UndeliveredOrder.order_id,
            UndeliveredOrder.order_date,
            UndeliveredOrder.expected_deliver_on,
            UndeliveredOrder.net_amount,
            UndeliveredOrder.actual_deliver_on,
        )
        .where(sa.func.upper(UndeliveredOrder.store_code) == store_code)
        .where(UndeliveredOrder.actual_deliver_on.is_(None))
    )
    result = await session.execute(stmt)
    rows = []
    total_amount = 0.0
    for row in result:
        committed = row.expected_deliver_on
        if committed is None and row.order_date:
            committed = row.order_date + timedelta(days=3)
        age_days = None
        if committed:
            age_days = (report_date - committed).days
        net_amount = _as_float(row.net_amount)
        if net_amount is not None:
            total_amount += net_amount
        rows.append(
            {
                "order_id": row.order_id,
                "order_date": row.order_date,
                "committed_date": committed,
                "age_days": age_days,
                "net_amount": net_amount,
            }
        )
    rows.sort(key=lambda r: r["age_days"] if r["age_days"] is not None else -1, reverse=True)
    return rows, total_amount


async def _fetch_missed_leads_rows(session: AsyncSession, store_code: str) -> List[Dict[str, Any]]:
    stmt = (
        sa.select(
            MissedLead.mobile_number,
            MissedLead.customer_name,
            MissedLead.pickup_created_date,
            MissedLead.pickup_created_time,
            MissedLead.pickup_date,
            MissedLead.pickup_time,
            MissedLead.source,
            MissedLead.customer_type,
        )
        .where(sa.func.upper(MissedLead.store_code) == store_code)
        .where(MissedLead.is_order_placed.is_(False))
        .order_by(MissedLead.customer_type.asc(), MissedLead.pickup_created_date.asc(), MissedLead.pickup_created_time.asc())
    )
    result = await session.execute(stmt)
    rows = []
    for row in result:
        rows.append(
            {
                "phone": row.mobile_number,
                "customer_name": row.customer_name,
                "pickup_created": _combine_date_time(row.pickup_created_date, row.pickup_created_time),
                "pickup_time": _combine_date_time(row.pickup_date, row.pickup_time),
                "source": row.source,
                "customer_type": row.customer_type,
            }
        )
    return rows


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
        comparison_row = await _fetch_dashboard_summary(
            session, normalized_code, report_date, comparison=True
        )

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

        undelivered_rows, undelivered_total_amount = await _fetch_undelivered_order_rows(
            session,
            normalized_code,
            report_date,
        )
        missed_leads_rows = await _fetch_missed_leads_rows(session, normalized_code)

    pickup_total_pct = _as_float(summary_row.get("pickup_total_conv_pct"))
    pickup_new_pct = _as_float(summary_row.get("pickup_new_conv_pct"))
    pickup_existing_pct = _as_float(summary_row.get("pickup_existing_conv_pct"))
    delivery_tat_pct = _as_float(summary_row.get("delivery_tat_pct"))
    undelivered_10_plus = _as_int(summary_row.get("delivery_undel_over_10_days"))
    undelivered_total = _as_int(summary_row.get("delivery_total_undelivered"))
    repeat_pct = _as_float(summary_row.get("repeat_total_base_pct"))
    ftd_revenue = _as_float(summary_row.get("ftd_revenue"))
    high_value_orders = _as_int(summary_row.get("package_non_pkg_over_800"))

    statuses: Dict[str, MetricStatus] = {
        "pickup_conversion_total_pct": _evaluate_metric("pickup_conversion_total_pct", pickup_total_pct),
        "pickup_conversion_new_pct": _evaluate_metric("pickup_conversion_new_pct", pickup_new_pct),
        "pickup_conversion_existing_pct": _evaluate_metric("pickup_conversion_existing_pct", pickup_existing_pct),
        "delivery_tat_pct": _evaluate_metric("delivery_tat_pct", delivery_tat_pct),
        "undelivered_10_plus_count": _evaluate_metric("undelivered_10_plus_count", undelivered_10_plus),
        "undelivered_total_count": _evaluate_metric("undelivered_total_count", undelivered_total),
        "repeat_customer_pct": _evaluate_metric("repeat_customer_pct", repeat_pct),
    }

    overall = _overall_health(statuses.values())
    recos = _build_recommendations(statuses)

    def snapshot_values(today: float | int | None, yesterday: float | int | None) -> tuple[
        float | int | None, float | int | None, float | None
    ]:
        if today is None:
            return today, yesterday, None
        if yesterday is None:
            return today, yesterday, None
        return today, yesterday, float(today) - float(yesterday)

    leads_today, leads_yday, leads_delta = snapshot_values(missed_count, previous_missed_count)

    pickups_today, pickups_yday, pickups_delta = snapshot_values(
        _as_int(summary_row.get("pickup_total_count")),
        _as_int(comparison_row.get("pickup_total_count")) if comparison_row else None,
    )

    deliveries_today, deliveries_yday, deliveries_delta = snapshot_values(
        _as_int(summary_row.get("delivery_total_delivered")),
        _as_int(comparison_row.get("delivery_total_delivered")) if comparison_row else None,
    )

    new_customers_today, new_customers_yday, new_customers_delta = snapshot_values(
        _as_int(summary_row.get("pickup_new_count")),
        _as_int(comparison_row.get("pickup_new_count")) if comparison_row else None,
    )

    repeat_customers_today, repeat_customers_yday, repeat_customers_delta = snapshot_values(
        _as_int(summary_row.get("repeat_orders")) or repeat_base_count,
        _as_int(comparison_row.get("repeat_orders")) if comparison_row else None,
    )

    snapshot = {
        "leads_today": leads_today,
        "leads_yday": leads_yday,
        "leads_delta": leads_delta,
        "leads_total": leads_today,
        "leads_note": _change_note(leads_today, leads_yday),
        "pickups_today": pickups_today,
        "pickups_yday": pickups_yday,
        "pickups_delta": pickups_delta,
        "pickups_total": pickups_today,
        "pickups_note": _change_note(pickups_today, pickups_yday),
        "deliveries_today": deliveries_today,
        "deliveries_yday": deliveries_yday,
        "deliveries_delta": deliveries_delta,
        "deliveries_total": deliveries_today,
        "deliveries_note": _change_note(deliveries_today, deliveries_yday),
        "new_customers_today": new_customers_today,
        "new_customers_yday": new_customers_yday,
        "new_customers_delta": new_customers_delta,
        "new_customers_count": new_customers_today,
        "new_customers_note": _change_note(new_customers_today, new_customers_yday),
        "repeat_customers_today": repeat_customers_today,
        "repeat_customers_yday": repeat_customers_yday,
        "repeat_customers_delta": repeat_customers_delta,
        "repeat_customers_count": repeat_customers_today,
        "repeat_customers_note": _change_note(repeat_customers_today, repeat_customers_yday),
    }

    snapshot_classes = {
        "leads_change_class": _snapshot_delta_class(missed_count, previous_missed_count, direction="higher"),
        "pickups_change_class": _snapshot_delta_class(
            summary_row.get("pickup_total_count"),
            comparison_row.get("pickup_total_count") if comparison_row else None,
            direction="higher",
        ),
        "deliveries_change_class": _snapshot_delta_class(
            summary_row.get("delivery_total_delivered"),
            comparison_row.get("delivery_total_delivered") if comparison_row else None,
            direction="higher",
        ),
        "new_customers_change_class": _snapshot_delta_class(
            summary_row.get("pickup_new_count"),
            comparison_row.get("pickup_new_count") if comparison_row else None,
            direction="higher",
        ),
        "repeat_customers_change_class": _snapshot_delta_class(
            summary_row.get("repeat_orders"),
            comparison_row.get("repeat_orders") if comparison_row else None,
            direction="higher",
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
        "pickup_conversion_existing_pct": pickup_existing_pct,
        "pickup_conversion_existing_status_label": statuses["pickup_conversion_existing_pct"].label,
        "pickup_conversion_existing_status_class": statuses["pickup_conversion_existing_pct"].css_class,
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
        **snapshot_classes,
        "undelivered_orders_rows": undelivered_rows,
        "undelivered_orders_total_amount": undelivered_total_amount,
        "missed_leads_rows": missed_leads_rows,
    }

    return context


async def render_store_report_pdf(
    store_context: Dict[str, Any], output_path: str | Path, template_path: str | Path | None = None
) -> None:
    """Render a store report PDF using the ReportLab builder."""

    builder = StoreReportPdfBuilder(store_context=store_context, output_path=Path(output_path))
    builder.build()


async def render_pdf_with_configured_browser(html_content: str, output_path: str | Path) -> None:
    from app.config import config

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


class StoreReportPdfBuilder:
    STATUS_COLOR_MAP = {
        "kpi-status-excellent": colors.HexColor("#20bf6b"),
        "kpi-status-good": colors.HexColor("#2d98da"),
        "kpi-status-poor": colors.HexColor("#f39c12"),
        "kpi-status-critical": colors.HexColor("#e74c3c"),
        "kpi-status-missing": colors.HexColor("#7f8c8d"),
    }
    OVERALL_COLOR_MAP = {
        "status-excellent": colors.HexColor("#20bf6b"),
        "status-good": colors.HexColor("#2d98da"),
        "status-warning": colors.HexColor("#f39c12"),
        "status-critical": colors.HexColor("#e74c3c"),
    }
    SNAPSHOT_COLOR_MAP = {
        "snapshot-positive": colors.HexColor("#20bf6b"),
        "snapshot-negative": colors.HexColor("#e74c3c"),
        "snapshot-neutral": colors.HexColor("#1c1c1c"),
    }

    def __init__(self, *, store_context: Dict[str, Any], output_path: Path) -> None:
        self.context = store_context
        self.output_path = Path(output_path)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.canvas = canvas.Canvas(str(self.output_path), pagesize=A4)
        self.width, self.height = A4
        self.margin = 36
        self.body_width = self.width - 2 * self.margin
        self.y = self.height - self.margin
        self.form = self.canvas.acroForm
        self.styles = {
            "normal": ParagraphStyle(
                name="Normal",
                fontName="Helvetica",
                fontSize=10,
                leading=14,
                textColor=colors.HexColor("#1c1c1c"),
            ),
            "list": ParagraphStyle(
                name="List",
                fontName="Helvetica",
                fontSize=10,
                leading=14,
                textColor=colors.HexColor("#1c1c1c"),
            ),
        }

    def build(self) -> None:
        self._draw_header()
        self._draw_overall_health()
        self._draw_primary_kpis()
        self._draw_secondary_indicators()
        self._draw_snapshot()
        self._draw_recommendations()
        self._draw_action_lists()
        self._draw_footer()
        self.canvas.save()

    def build_action_list(self) -> None:
        """Render only the interactive action list tables."""

        self._draw_header()
        self._draw_action_lists()
        self._draw_footer()
        self.canvas.save()

    def _new_page(self) -> None:
        self.canvas.showPage()
        self.form = self.canvas.acroForm
        self.y = self.height - self.margin

    def _ensure_space(self, required_height: float) -> None:
        if self.y - required_height < self.margin:
            self._new_page()

    def _draw_header(self) -> None:
        self._ensure_space(70)
        c = self.canvas
        store_name = self.context.get("store_name", "Store")
        c.setFont("Helvetica-Bold", 20)
        c.setFillColor(colors.HexColor("#1c1c1c"))
        c.drawString(self.margin, self.y, store_name)

        logo_src = self.context.get("logo_src")
        if logo_src:
            try:
                logo = ImageReader(logo_src)
                max_width, max_height = 90, 40
                c.drawImage(
                    logo,
                    self.width - self.margin - max_width,
                    self.y - max_height + 5,
                    width=max_width,
                    height=max_height,
                    preserveAspectRatio=True,
                    mask="auto",
                )
            except Exception:
                pass

        self.y -= 26
        meta_lines = [
            f"Report date: {self.context.get('report_date', '')}",
            f"Generated at: {self._format_generated_at(self.context.get('generated_at'))}",
            f"Run ID: {self.context.get('run_id', '')}",
        ]
        c.setFont("Helvetica", 10)
        for line in meta_lines:
            c.drawString(self.margin, self.y, line)
            self.y -= 14
        self.y -= 10

    def _draw_overall_health(self) -> None:
        block_height = 90
        self._ensure_space(block_height + 20)
        c = self.canvas
        status_class = self.context.get("overall_health_status_class", "status-good")
        accent = self.OVERALL_COLOR_MAP.get(status_class, colors.HexColor("#2d98da"))
        c.setFillColor(colors.HexColor("#f3f7ff"))
        c.roundRect(self.margin, self.y - block_height, self.body_width, block_height, 10, fill=1, stroke=0)
        c.setFillColor(accent)
        c.rect(self.margin, self.y - block_height, 6, block_height, fill=1, stroke=0)

        score = self.context.get("overall_health_score", 0)
        label = self.context.get("overall_health_label", "No Data")
        summary = self.context.get("overall_health_summary", "")

        c.setFont("Helvetica-Bold", 46)
        c.setFillColor(colors.HexColor("#1f3a93"))
        c.drawString(self.margin + 20, self.y - 50, str(score))
        c.setFillColor(colors.HexColor("#1c1c1c"))
        c.setFont("Helvetica-Bold", 16)
        c.drawString(self.margin + 120, self.y - 24, label)

        summary_para = Paragraph(summary, self.styles["normal"])
        summary_width = self.body_width - 140
        wrapped_width, wrapped_height = summary_para.wrap(summary_width, block_height - 36)
        summary_para.drawOn(c, self.margin + 120, self.y - 36 - wrapped_height)

        self.y -= block_height + 20

    def _draw_primary_kpis(self) -> None:
        self._draw_section_title("Primary KPIs")
        rows = [
            (
                "Pickup conversion (total)",
                self._format_percent(self.context.get("pickup_conversion_total_pct")),
                self.context.get("pickup_conversion_total_status_label", ""),
                self.context.get("pickup_conversion_total_status_class"),
            ),
            (
                "Pickup conversion (new)",
                self._format_percent(self.context.get("pickup_conversion_new_pct")),
                self.context.get("pickup_conversion_new_status_label", ""),
                self.context.get("pickup_conversion_new_status_class"),
            ),
            (
                "Pickup conversion (existing)",
                self._format_percent(self.context.get("pickup_conversion_existing_pct")),
                self.context.get("pickup_conversion_existing_status_label", ""),
                self.context.get("pickup_conversion_existing_status_class"),
            ),
            (
                "Delivery within TAT",
                self._format_percent(self.context.get("delivery_tat_pct")),
                self.context.get("delivery_tat_status_label", ""),
                self.context.get("delivery_tat_status_class"),
            ),
            (
                "Undelivered >10 days",
                self._value_or_na(self.context.get("undelivered_10_plus_count")),
                self.context.get("undelivered_10_plus_status_label", ""),
                self.context.get("undelivered_10_plus_status_class"),
            ),
            (
                "Total undelivered orders",
                self._value_or_na(self.context.get("undelivered_total_count")),
                self.context.get("undelivered_total_status_label", ""),
                self.context.get("undelivered_total_status_class"),
            ),
            (
                "Repeat customer contribution",
                self._format_percent(self.context.get("repeat_customer_pct")),
                self.context.get("repeat_customer_status_label", ""),
                self.context.get("repeat_customer_status_class"),
            ),
        ]
        data = [["KPI", "Value", "Status"]]
        data.extend([[name, value, status] for name, value, status, _ in rows])
        table = Table(data, colWidths=[self.body_width * 0.45, self.body_width * 0.2, self.body_width * 0.35])
        style = TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#fafafa")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#1c1c1c")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 10),
                ("ALIGN", (1, 1), (1, -1), "RIGHT"),
                ("ALIGN", (2, 1), (2, -1), "LEFT"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#e1e1e1")),
            ]
        )
        for idx, (_, _, _, status_class) in enumerate(rows, start=1):
            color = self.STATUS_COLOR_MAP.get(status_class, colors.HexColor("#1c1c1c"))
            style.add("TEXTCOLOR", (2, idx), (2, idx), color)
            style.add("FONTNAME", (2, idx), (2, idx), "Helvetica-Bold")
        table.setStyle(style)
        self._draw_table(table)

    def _draw_secondary_indicators(self) -> None:
        self._draw_section_title("Secondary Indicators")
        data = [
            ["Metric", "Value"],
            ["FTD revenue", self._currency(self.context.get("ftd_revenue"))],
            [
                "High value orders (>800)",
                self._value_or_na(self.context.get("high_value_orders_count")),
            ],
            [
                "Undelivered snapshot today",
                self._value_or_na(self.context.get("undelivered_snapshot_count")),
            ],
        ]
        table = Table(data, colWidths=[self.body_width * 0.6, self.body_width * 0.4])
        table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#fafafa")),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, -1), 10),
                    ("ALIGN", (1, 1), (1, -1), "RIGHT"),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#e1e1e1")),
                ]
            )
        )
        self._draw_table(table)

    def _draw_snapshot(self) -> None:
        self._draw_section_title("Today's Operating Snapshot")
        rows = [
            (
                "Leads captured",
                self._value_or_na(self.context.get("leads_today")),
                self._value_or_na(self.context.get("leads_yday")),
                self._format_delta(self.context.get("leads_delta")),
                self.context.get("leads_note", ""),
                self.context.get("leads_change_class", "snapshot-neutral"),
            ),
            (
                "Pickups",
                self._value_or_na(self.context.get("pickups_today")),
                self._value_or_na(self.context.get("pickups_yday")),
                self._format_delta(self.context.get("pickups_delta")),
                self.context.get("pickups_note", ""),
                self.context.get("pickups_change_class", "snapshot-neutral"),
            ),
            (
                "Deliveries",
                self._value_or_na(self.context.get("deliveries_today")),
                self._value_or_na(self.context.get("deliveries_yday")),
                self._format_delta(self.context.get("deliveries_delta")),
                self.context.get("deliveries_note", ""),
                self.context.get("deliveries_change_class", "snapshot-neutral"),
            ),
            (
                "New customers",
                self._value_or_na(self.context.get("new_customers_today")),
                self._value_or_na(self.context.get("new_customers_yday")),
                self._format_delta(self.context.get("new_customers_delta")),
                self.context.get("new_customers_note", ""),
                self.context.get("new_customers_change_class", "snapshot-neutral"),
            ),
            (
                "Repeat customers",
                self._value_or_na(self.context.get("repeat_customers_today")),
                self._value_or_na(self.context.get("repeat_customers_yday")),
                self._format_delta(self.context.get("repeat_customers_delta")),
                self.context.get("repeat_customers_note", ""),
                self.context.get("repeat_customers_change_class", "snapshot-neutral"),
            ),
        ]
        data = [["Metric", "T", "T-1", "Δ", "Note"]]
        data.extend([[metric, today, yday, delta, note] for metric, today, yday, delta, note, _ in rows])
        table = Table(
            data,
            colWidths=[
                self.body_width * 0.3,
                self.body_width * 0.12,
                self.body_width * 0.12,
                self.body_width * 0.12,
                self.body_width * 0.34,
            ],
        )
        style = TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#fafafa")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 10),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#e1e1e1")),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("ALIGN", (1, 1), (3, -1), "RIGHT"),
            ]
        )
        for idx, (_, _, _, _, _, cls) in enumerate(rows, start=1):
            color = self.SNAPSHOT_COLOR_MAP.get(cls, colors.HexColor("#1c1c1c"))
            style.add("TEXTCOLOR", (3, idx), (3, idx), color)
            style.add("TEXTCOLOR", (4, idx), (4, idx), color)
        table.setStyle(style)
        self._draw_table(table)

    def _draw_recommendations(self) -> None:
        self._draw_section_title("Recommendations")
        highlights = self.context.get("highlights", []) or ["No standout wins logged – maintain steady execution tomorrow."]
        focus = self.context.get("focus_areas", []) or ["Operations were stable with no major risk signals."]
        actions = self.context.get("actions_today", []) or [
            "Continue standard playbook reviews with the on-duty manager."
        ]
        columns = [
            self._list_block("Highlights", highlights),
            self._list_block("Focus Areas", focus),
            self._list_block("Actions for Today", actions),
        ]
        table = Table([columns], colWidths=[self.body_width / 3] * 3)
        table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f9f9f9")),
                    ("INNERGRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#ffffff")),
                    ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#f0f0f0")),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ]
            )
        )
        self._draw_table(table)

    def _draw_action_lists(self) -> None:
        self._draw_undelivered_orders()
        self._draw_missed_leads()

    def _draw_footer(self) -> None:
        footer_text = f"TSV Store Performance Report • Automated dashboard insights • {self.context.get('report_date', '')}"
        self._ensure_space(30)
        self.canvas.setFont("Helvetica", 9)
        self.canvas.setFillColor(colors.HexColor("#7f8c8d"))
        self.canvas.drawCentredString(self.width / 2, self.margin / 2, footer_text)

    def _draw_section_title(self, title: str) -> None:
        self._ensure_space(24)
        self.canvas.setFont("Helvetica-Bold", 16)
        self.canvas.setFillColor(colors.HexColor("#1c1c1c"))
        self.canvas.drawString(self.margin, self.y, title)
        self.y -= 20

    def _draw_table(self, table: Table) -> None:
        table_width, table_height = table.wrap(self.body_width, self.y)
        self._ensure_space(table_height + 10)
        table.drawOn(self.canvas, self.margin, self.y - table_height)
        self.y -= table_height + 18

    def _list_block(self, title: str, items: List[str]) -> Paragraph:
        bullet_lines = "<br/>".join(f"• {item}" for item in items)
        html = f"<b>{title}</b><br/>{bullet_lines}"
        return Paragraph(html, self.styles["list"])

    def _format_percent(self, value: float | int | None) -> str:
        if value is None:
            return "N/A"
        return f"{value:.0f}%"

    def _format_delta(self, delta: float | int | None) -> str:
        if delta is None:
            return "—"
        if math.isclose(float(delta), 0, abs_tol=0.01):
            return "0"
        sign = "+" if float(delta) > 0 else ""
        if isinstance(delta, int) or float(delta).is_integer():
            return f"{sign}{int(delta)}"
        return f"{sign}{float(delta):.1f}"

    def _value_or_na(self, value: Any) -> str:
        if value is None:
            return "N/A"
        return str(value)

    def _currency(self, value: float | int | None) -> str:
        if value is None:
            return " 0.00"
        return f" {float(value):.2f}"

    def _value_from_row(self, row: Any, key: str) -> Any:
        if isinstance(row, Mapping):
            return row.get(key)
        return getattr(row, key, None)

    def _draw_undelivered_orders(self) -> None:
        self._draw_section_title("Undelivered Orders (Action List)")
        positions = {
            "sno": self.margin,
            "order_info": self.margin + 30,
            "age": self.margin + 270,
            "amount": self.margin + 310,
            "delivered": self.margin + 380,
            "comments": self.margin + 410,
        }
        comments_width = self.width - self.margin - positions["comments"]
        data_row_height = 14
        action_row_height = 18
        block_spacing = 6

        def draw_headers(y: float) -> None:
            c = self.canvas
            c.setFont("Helvetica-Bold", 9)
            c.drawString(positions["sno"], y, "S. No.")
            c.drawString(positions["order_info"], y, "Order Info")
            c.drawString(positions["age"], y, "Age")
            c.drawString(positions["amount"], y, "Net Amount")
            c.drawString(positions["delivered"], y, "")
            c.drawString(positions["comments"], y, "")

        self._ensure_space(40)
        header_y = self.y
        draw_headers(header_y)
        self.canvas.setLineWidth(0.5)
        self.canvas.setStrokeColor(colors.HexColor("#e1e1e1"))
        self.canvas.line(self.margin, header_y - 4, self.width - self.margin, header_y - 4)
        self.y = header_y - 10

        rows = self.context.get("undelivered_orders_rows", []) or []
        row_count = len(rows)
        if not rows:
            self.canvas.setFont("Helvetica-Oblique", 9)
            self.canvas.setFillColor(colors.HexColor("#7f8c8d"))
            self.canvas.drawString(self.margin, self.y - 4, "No undelivered orders pending as of this report.")
            self.y -= 20
        else:
            self.canvas.setFillColor(colors.HexColor("#1c1c1c"))
            for idx, row in enumerate(rows, start=1):
                required_height = data_row_height + action_row_height + block_spacing
                if self.y - required_height < self.margin + 20:
                    self._new_page()
                    self._draw_section_title("Undelivered Orders (Action List)")
                    header_y = self.y
                    draw_headers(header_y)
                    self.canvas.line(self.margin, header_y - 4, self.width - self.margin, header_y - 4)
                    self.y = header_y - 10
                order_id = self._value_from_row(row, "order_id") or ""
                order_date = self._value_from_row(row, "order_date")
                committed = self._value_from_row(row, "committed_date")
                age_days = self._value_from_row(row, "age_days")
                net_amount = self._value_from_row(row, "net_amount")
                order_info_parts = []
                if order_id:
                    order_info_parts.append(str(order_id))
                if order_date:
                    order_info_parts.append(self._format_date(order_date))
                if committed:
                    order_info_parts.append(f"Del. {self._format_date(committed)}")
                order_info_text = ", ".join(order_info_parts)
                data_y = self.y
                self.canvas.setFont("Helvetica", 9)
                self.canvas.drawString(positions["sno"], data_y, str(idx))
                self.canvas.drawString(positions["order_info"], data_y, order_info_text)
                self.canvas.drawString(positions["age"], data_y, self._value_or_na(age_days))
                if net_amount is not None:
                    self.canvas.drawRightString(positions["delivered"] - 8, data_y, f"{float(net_amount):.2f}")
                self.y -= data_row_height
                action_y = self.y
                self.form.checkbox(
                    name=f"undelivered_delivered_{idx}",
                    tooltip="Delivered?",
                    x=self.margin,
                    y=action_y - 6,
                    size=12,
                    borderColor=colors.HexColor("#1c1c1c"),
                    fillColor=colors.white,
                )
                self.form.textfield(
                    name=f"undelivered_comment_{idx}",
                    tooltip="Comments",
                    x=self.margin + 20,
                    y=action_y - 7,
                    width=self.width - self.margin - (self.margin + 20),
                    height=16,
                    borderWidth=1,
                )
                self.y -= action_row_height
                self.y -= block_spacing

        self.canvas.setFillColor(colors.HexColor("#1c1c1c"))
        total_amount = self.context.get("undelivered_orders_total_amount") or 0
        self.y -= 4
        summary_text = (
            f"Total undelivered orders: {row_count}    |    Total net amount: {float(total_amount):.2f}"
        )
        self.canvas.setFont("Helvetica-Bold", 10)
        self.canvas.drawString(self.margin, self.y, summary_text)
        self.y -= 24

    def _draw_missed_leads(self) -> None:
        self._draw_section_title("Missed Leads – Not Converted")
        positions = {
            "sno": self.margin,
            "customer_details": self.margin + 30,
            "customer_type": self.margin + 350,
            "converted": self.margin + 450,
            "comments": self.margin + 490,
        }
        comments_width = self.width - self.margin - positions["comments"]
        data_row_height = 14
        action_row_height = 18
        block_spacing = 6

        def draw_headers(y: float) -> None:
            c = self.canvas
            c.setFont("Helvetica-Bold", 9)
            c.drawString(positions["sno"], y, "S. No.")
            c.drawString(positions["customer_details"], y, "Customer Details")
            c.drawString(positions["customer_type"], y, "Customer Type")
            c.drawString(positions["converted"], y, "")
            c.drawString(positions["comments"], y, "")

        self._ensure_space(40)
        header_y = self.y
        draw_headers(header_y)
        self.canvas.line(self.margin, header_y - 4, self.width - self.margin, header_y - 4)
        self.y = header_y - 10

        rows = self.context.get("missed_leads_rows", []) or []
        row_count = len(rows)
        if not rows:
            self.canvas.setFont("Helvetica-Oblique", 9)
            self.canvas.setFillColor(colors.HexColor("#7f8c8d"))
            self.canvas.drawString(self.margin, self.y - 4, "No pending missed leads requiring follow-up.")
            self.y -= 20
        else:
            self.canvas.setFillColor(colors.HexColor("#1c1c1c"))
            for idx, row in enumerate(rows, start=1):
                required_height = data_row_height + action_row_height + block_spacing
                if self.y - required_height < self.margin + 20:
                    self._new_page()
                    self._draw_section_title("Missed Leads – Not Converted")
                    header_y = self.y
                    draw_headers(header_y)
                    self.canvas.line(self.margin, header_y - 4, self.width - self.margin, header_y - 4)
                    self.y = header_y - 10
                details_parts = []
                phone = self._value_from_row(row, "phone")
                customer_name = self._value_from_row(row, "customer_name")
                pickup_created = self._value_from_row(row, "pickup_created")
                source = self._value_from_row(row, "source")
                if phone:
                    details_parts.append(str(phone))
                if customer_name:
                    details_parts.append(str(customer_name))
                if pickup_created:
                    details_parts.append(str(pickup_created))
                if source:
                    details_parts.append(str(source))
                details_text = ", ".join(details_parts)
                self.canvas.setFont("Helvetica", 9)
                self.canvas.drawString(positions["sno"], self.y, str(idx))
                self.canvas.drawString(positions["customer_details"], self.y, details_text)
                self.canvas.drawString(
                    positions["customer_type"], self.y, self._value_from_row(row, "customer_type") or ""
                )
                self.y -= data_row_height
                action_y = self.y
                self.form.checkbox(
                    name=f"missed_lead_converted_{idx}",
                    tooltip="Lead converted?",
                    x=self.margin,
                    y=action_y - 6,
                    size=12,
                    borderColor=colors.HexColor("#1c1c1c"),
                    fillColor=colors.white,
                )
                self.form.textfield(
                    name=f"missed_lead_comment_{idx}",
                    tooltip="Comments",
                    x=self.margin + 20,
                    y=action_y - 7,
                    width=self.width - self.margin - (self.margin + 20),
                    height=16,
                    borderWidth=1,
                )
                self.y -= action_row_height
                self.y -= block_spacing

        self.canvas.setFillColor(colors.HexColor("#1c1c1c"))
        self.y -= 4
        self.canvas.setFont("Helvetica-Bold", 10)
        self.canvas.drawString(self.margin, self.y, f"Total missed leads: {row_count}")
        self.y -= 24

    def _format_date(self, value: Any) -> str:
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return value or ""

    def _format_generated_at(self, value: Any) -> str:
        if not value:
            return ""
        if isinstance(value, datetime):
            dt_value = value
        else:
            try:
                dt_value = datetime.fromisoformat(str(value))
            except ValueError:
                return str(value)
        if dt_value.tzinfo is not None:
            dt_value = dt_value.astimezone()
        return dt_value.strftime("%d %b %Y, %I:%M %p")
