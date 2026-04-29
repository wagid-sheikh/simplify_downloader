from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
import sqlalchemy as sa

from app.common.db import session_scope
import app.reports.daily_sales_report.pipeline as pipeline


def _create_tables(database_url: str) -> None:
    engine = sa.create_engine(database_url.replace("+aiosqlite", ""))
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                CREATE TABLE orders (
                    cost_center TEXT,
                    order_number TEXT,
                    order_date TIMESTAMP,
                    net_amount NUMERIC
                )
                """
            )
        )
        conn.execute(
            sa.text(
                """
                CREATE TABLE sales (
                    cost_center TEXT,
                    order_number TEXT,
                    payment_date TIMESTAMP,
                    payment_received NUMERIC
                )
                """
            )
        )
        conn.execute(sa.text("CREATE TABLE store_master (cost_center TEXT, store_code TEXT)"))
        conn.execute(
            sa.text(
                """
                CREATE TABLE documents (
                    doc_type TEXT,
                    doc_subtype TEXT,
                    doc_date DATE,
                    reference_name_1 TEXT,
                    reference_id_1 TEXT,
                    reference_name_2 TEXT,
                    reference_id_2 TEXT,
                    reference_name_3 TEXT,
                    reference_id_3 TEXT,
                    file_name TEXT,
                    mime_type TEXT,
                    file_size_bytes INTEGER,
                    storage_backend TEXT,
                    file_path TEXT,
                    created_at TIMESTAMP,
                    created_by TEXT
                )
                """
            )
        )
    engine.dispose()


@pytest.mark.asyncio
async def test_daily_pipeline_writes_mtd_attachment_window_and_metadata(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "daily_sales_pipeline.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)

    report_date = date(2026, 4, 29)

    async with session_scope(database_url) as session:
        await session.execute(sa.text("INSERT INTO store_master (cost_center, store_code) VALUES ('CC1', 'S1')"))
        await session.execute(
            sa.text(
                """
                INSERT INTO orders (cost_center, order_number, order_date, net_amount)
                VALUES
                    ('CC1', 'RPT-DATE-1', '2026-04-29T09:00:00+05:30', 900),
                    ('CC1', 'IN-MONTH-1', '2026-04-10T09:00:00+05:30', 800),
                    ('CC1', 'OUT-MONTH-1', '2026-03-31T09:00:00+05:30', 700)
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO sales (cost_center, order_number, payment_date, payment_received)
                VALUES
                    ('CC1', 'RPT-DATE-1', '2026-04-29T10:00:00+05:30', 900),
                    ('CC1', 'IN-MONTH-1', '2026-04-10T10:00:00+05:30', 800),
                    ('CC1', 'OUT-MONTH-1', '2026-04-29T10:00:00+05:30', 700)
                """
            )
        )
        await session.commit()

    monkeypatch.setattr(
        pipeline, "config", SimpleNamespace(database_url=database_url, pdf_render_timeout_seconds=30)
    )
    monkeypatch.setattr(pipeline, "get_timezone", lambda: ZoneInfo("Asia/Kolkata"))
    monkeypatch.setattr(pipeline, "resolve_run_env", lambda env: "test")
    monkeypatch.setattr(pipeline, "new_run_id", lambda: "run-mtd-1")

    async def _no_existing(*args, **kwargs):
        return None

    async def _summary_noop(*args, **kwargs):
        return None

    monkeypatch.setattr(pipeline, "check_existing_run", _no_existing)
    monkeypatch.setattr(pipeline, "persist_summary_record", _summary_noop)
    monkeypatch.setattr(pipeline, "update_summary_record", _summary_noop)

    async def _fake_daily_data(*args, **kwargs):
        return SimpleNamespace(
            report_date=report_date,
            rows=[],
            totals=SimpleNamespace(),
            edited_orders=[],
            edited_orders_summary=SimpleNamespace(),
            edited_orders_totals=SimpleNamespace(),
            missed_leads=[],
            cancelled_leads=[],
            lead_performance_summary=SimpleNamespace(),
            to_be_recovered=[],
            to_be_compensated=[],
            to_be_recovered_total_order_value=0,
            to_be_compensated_total_order_value=0,
            same_day_fulfillment_rows=[SimpleNamespace(order_number="RPT-DATE-1")],
        )

    monkeypatch.setattr(pipeline, "fetch_daily_sales_report", _fake_daily_data)

    monkeypatch.setattr(
        pipeline,
        "_render_html",
        lambda context: "DAILY SAME DAY: " + ",".join(row.order_number for row in context["same_day_fulfillment_rows"]),
    )

    rendered: dict[str, str] = {}

    async def _fake_render_pdf(html, output_path: Path, pdf_options=None, logger=None):
        rendered[str(output_path)] = html
        output_path.write_bytes(b"pdf")

    monkeypatch.setattr(pipeline, "render_pdf_with_configured_browser", _fake_render_pdf)

    async def _fake_notify(*args, **kwargs):
        return {"emails_planned": 1, "emails_sent": 1, "errors": []}

    monkeypatch.setattr(pipeline, "send_notifications_for_run", _fake_notify)

    await pipeline._run(report_date=report_date, env="test", force=True)

    daily_path = str(pipeline.OUTPUT_ROOT / f"{pipeline.PIPELINE_NAME}_{report_date.isoformat()}.pdf")
    mtd_path = str(pipeline.OUTPUT_ROOT / f"reports.mtd_same_day_fulfillment_{report_date.isoformat()}.pdf")

    assert "RPT-DATE-1" in rendered[daily_path]

    mtd_html = rendered[mtd_path]
    assert "RPT-DATE-1" in mtd_html
    assert "IN-MONTH-1" in mtd_html
    assert "OUT-MONTH-1" not in mtd_html
    assert "MTD Same-Day Fulfillment (Month Start to Report Date)" in mtd_html
    assert "Window: 01-Apr-2026 to 29-Apr-2026" in mtd_html

    async with session_scope(database_url) as session:
        rows = (
            await session.execute(
                sa.text(
                    """
                    SELECT doc_type, file_name, reference_id_1, reference_id_3
                    FROM documents
                    ORDER BY file_name
                    """
                )
            )
        ).mappings().all()

    assert len(rows) == 2
    assert rows[0]["doc_type"] == "daily_sales_report_pdf"
    assert rows[1]["doc_type"] == "mtd_same_day_fulfillment_pdf"
    assert rows[1]["file_name"] == f"reports.mtd_same_day_fulfillment_{report_date.isoformat()}.pdf"
    assert rows[1]["reference_id_1"] == pipeline.PIPELINE_NAME
    assert rows[1]["reference_id_3"] == report_date.isoformat()
