from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
import sqlalchemy as sa

from app.common.db import session_scope
from app.reports.daily_sales_report.data import RecoveryOrderRow
import app.reports.daily_sales_report.pipeline as pipeline
import app.reports.mtd_same_day_fulfillment.data as mtd_data


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
                    customer_name TEXT,
                    mobile_number TEXT,
                    net_amount NUMERIC
                )
                """
            )
        )
        conn.execute(
            sa.text(
                """
                CREATE TABLE order_line_items (
                    cost_center TEXT,
                    order_number TEXT,
                    service_name TEXT,
                    garment_name TEXT
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
                    payment_mode TEXT,
                    payment_received NUMERIC
                )
                """
            )
        )
        conn.execute(
            sa.text("CREATE TABLE store_master (cost_center TEXT, store_code TEXT)")
        )
        conn.execute(
            sa.text(
                "CREATE TABLE payment_collections (cost_center TEXT, order_number TEXT)"
            )
        )
        conn.execute(
            sa.text(
                """
                CREATE TABLE vw_orders_missing_in_payment_collections (
                    cost_center TEXT,
                    order_number TEXT,
                    order_date TIMESTAMP,
                    customer_name TEXT,
                    mobile_number TEXT,
                    net_amount NUMERIC
                )
                """
            )
        )
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
async def test_daily_pipeline_writes_mtd_attachment_window_and_metadata(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "daily_sales_pipeline.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)

    report_date = date(2026, 4, 29)

    async with session_scope(database_url) as session:
        await session.execute(
            sa.text(
                "INSERT INTO store_master (cost_center, store_code) VALUES ('CC1', 'S1')"
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO orders (cost_center, order_number, order_date, customer_name, mobile_number, net_amount)
                VALUES
                    ('CC1', 'RPT-DATE-1', '2026-04-29T09:00:00+05:30', 'Alice', '9999999999', 900),
                    ('CC1', 'IN-MONTH-1', '2026-04-10T09:00:00+05:30', 'Bob', '8888888888', 800),
                    ('CC1', 'OUT-MONTH-1', '2026-03-31T09:00:00+05:30', 'Cora', '7777777777', 700)
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO order_line_items (cost_center, order_number, service_name, garment_name)
                VALUES
                    ('CC1', 'RPT-DATE-1', 'Wash', 'Shirt'),
                    ('CC1', 'IN-MONTH-1', 'Iron', 'Pant'),
                    ('CC1', 'OUT-MONTH-1', 'Dry', 'Coat')
                """
            )
        )
        await session.execute(
            sa.text(
                """
                INSERT INTO sales (cost_center, order_number, payment_date, payment_mode, payment_received)
                VALUES
                    ('CC1', 'RPT-DATE-1', '2026-04-29T10:00:00+05:30', 'UPI', 900),
                    ('CC1', 'IN-MONTH-1', '2026-04-10T10:00:00+05:30', 'CARD', 800),
                    ('CC1', 'OUT-MONTH-1', '2026-04-29T10:00:00+05:30', 'CASH', 700)
                """
            )
        )
        await session.commit()

    monkeypatch.setattr(
        pipeline,
        "config",
        SimpleNamespace(database_url=database_url, pdf_render_timeout_seconds=30),
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
            to_be_recovered=[
                RecoveryOrderRow(
                    cost_center="CC1",
                    order_number="REC-1",
                    order_date=report_date,
                    customer_name="Rhea",
                    mobile_number="9999999990",
                    order_value=Decimal("125"),
                )
            ],
            to_be_compensated=[],
            to_be_recovered_total_order_value=0,
            to_be_compensated_total_order_value=0,
            same_day_fulfillment_rows=[SimpleNamespace(order_number="RPT-DATE-1")],
            missing_payment_rows=[],
        )

    monkeypatch.setattr(pipeline, "fetch_daily_sales_report", _fake_daily_data)

    monkeypatch.setattr(
        pipeline,
        "_render_html",
        lambda context, *args, **kwargs: (
            "DAILY SAME DAY: "
            + ",".join(row.order_number for row in context["same_day_fulfillment_rows"])
            if "same_day_fulfillment_rows" in context
            else "TO BE RECOVERED: "
            + ",".join(row.order_number for row in context["rows"])
        ),
    )

    rendered: dict[str, str] = {}

    async def _fake_render_pdf(html, output_path: Path, pdf_options=None, logger=None):
        rendered[str(output_path)] = html
        output_path.write_bytes(b"pdf")

    monkeypatch.setattr(
        pipeline, "render_pdf_with_configured_browser", _fake_render_pdf
    )

    async def _fake_notify(*args, **kwargs):
        return {"emails_planned": 1, "emails_sent": 1, "errors": []}

    monkeypatch.setattr(pipeline, "send_notifications_for_run", _fake_notify)

    await pipeline._run(report_date=report_date, env="test", force=True)

    daily_path = str(
        pipeline.OUTPUT_ROOT / f"{pipeline.PIPELINE_NAME}_{report_date.isoformat()}.pdf"
    )
    mtd_path = str(
        pipeline.OUTPUT_ROOT
        / f"reports.mtd_same_day_fulfillment_{report_date.isoformat()}.pdf"
    )
    recovered_path = str(
        pipeline.OUTPUT_ROOT / f"reports.to_be_recovered_{report_date.isoformat()}.pdf"
    )

    assert {daily_path, recovered_path}.issubset(rendered)
    assert "RPT-DATE-1" in rendered[daily_path]
    assert "REC-1" in rendered[recovered_path]

    mtd_html = rendered[mtd_path]
    assert "RPT-DATE-1" in mtd_html
    assert "IN-MONTH-1" in mtd_html
    assert "OUT-MONTH-1" not in mtd_html
    assert "Wash Shirt" in mtd_html
    assert "Iron Pant" in mtd_html
    assert "Same-Day Fulfillment" in mtd_html
    assert "Window: 01-Apr-2026 to 29-Apr-2026" in mtd_html

    async with session_scope(database_url) as session:
        rows = (
            (
                await session.execute(
                    sa.text(
                        """
                    SELECT doc_type, file_name, reference_id_1, reference_id_3
                    FROM documents
                    ORDER BY file_name
                    """
                    )
                )
            )
            .mappings()
            .all()
        )

    assert len(rows) == 3
    assert rows[0]["doc_type"] == "daily_sales_report_pdf"
    assert rows[1]["doc_type"] == "mtd_same_day_fulfillment_pdf"
    assert (
        rows[1]["file_name"]
        == f"reports.mtd_same_day_fulfillment_{report_date.isoformat()}.pdf"
    )
    assert rows[1]["reference_id_1"] == pipeline.PIPELINE_NAME
    assert rows[1]["reference_id_3"] == report_date.isoformat()
    assert rows[2]["doc_type"] == "to_be_recovered_report_pdf"
    assert (
        rows[2]["file_name"] == f"reports.to_be_recovered_{report_date.isoformat()}.pdf"
    )
    assert rows[2]["reference_id_1"] == pipeline.PIPELINE_NAME
    assert rows[2]["reference_id_3"] == report_date.isoformat()


@pytest.mark.asyncio
async def test_daily_pipeline_reaches_render_when_mtd_fetch_invoked_without_reflection(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "daily_sales_pipeline_no_reflection.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)

    report_date = date(2026, 4, 29)
    async with session_scope(database_url) as session:
        await session.execute(
            sa.text(
                "INSERT INTO store_master (cost_center, store_code) VALUES ('CC1', 'S1')"
            )
        )
        await session.execute(
            sa.text(
                "INSERT INTO orders (cost_center, order_number, order_date, customer_name, mobile_number, net_amount) VALUES ('CC1', 'RPT-1', '2026-04-29T09:00:00+05:30', 'Alice', '9999999999', 900)"
            )
        )
        await session.execute(
            sa.text(
                "INSERT INTO order_line_items (cost_center, order_number, service_name, garment_name) VALUES ('CC1', 'RPT-1', 'Wash', 'Shirt')"
            )
        )
        await session.execute(
            sa.text(
                "INSERT INTO sales (cost_center, order_number, payment_date, payment_mode, payment_received) VALUES ('CC1', 'RPT-1', '2026-04-29T10:00:00+05:30', 'UPI', 900)"
            )
        )
        await session.commit()

    monkeypatch.setattr(
        pipeline,
        "config",
        SimpleNamespace(database_url=database_url, pdf_render_timeout_seconds=30),
    )
    monkeypatch.setattr(pipeline, "get_timezone", lambda: ZoneInfo("Asia/Kolkata"))
    monkeypatch.setattr(pipeline, "resolve_run_env", lambda env: "test")
    monkeypatch.setattr(pipeline, "new_run_id", lambda: "run-mtd-2")
    monkeypatch.setattr(
        mtd_data.sa,
        "create_engine",
        lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("unexpected create_engine")
        ),
    )

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
            same_day_fulfillment_rows=[SimpleNamespace(order_number="RPT-1")],
            missing_payment_rows=[],
        )

    monkeypatch.setattr(pipeline, "fetch_daily_sales_report", _fake_daily_data)
    monkeypatch.setattr(pipeline, "_render_html", lambda context, *args, **kwargs: "ok")

    async def _fake_render_pdf(*args, **kwargs):
        return None

    async def _fake_notify(*args, **kwargs):
        return {"emails_planned": 0, "emails_sent": 0, "errors": []}

    monkeypatch.setattr(
        pipeline, "render_pdf_with_configured_browser", _fake_render_pdf
    )
    monkeypatch.setattr(pipeline, "send_notifications_for_run", _fake_notify)

    await pipeline._run(report_date=report_date, env="test", force=True)


@pytest.mark.asyncio
async def test_daily_pipeline_continues_when_mtd_fetch_fails(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "daily_sales_pipeline_mtd_failure.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    _create_tables(database_url)

    report_date = date(2026, 4, 29)
    async with session_scope(database_url) as session:
        await session.execute(
            sa.text(
                "INSERT INTO store_master (cost_center, store_code) VALUES ('CC1', 'S1')"
            )
        )
        await session.execute(
            sa.text(
                "INSERT INTO orders (cost_center, order_number, order_date, customer_name, mobile_number, net_amount) VALUES ('CC1', 'RPT-2', '2026-04-29T09:00:00+05:30', 'Alice', '9999999999', 900)"
            )
        )
        await session.execute(
            sa.text(
                "INSERT INTO order_line_items (cost_center, order_number, service_name, garment_name) VALUES ('CC1', 'RPT-2', 'Wash', 'Shirt')"
            )
        )
        await session.execute(
            sa.text(
                "INSERT INTO sales (cost_center, order_number, payment_date, payment_mode, payment_received) VALUES ('CC1', 'RPT-2', '2026-04-29T10:00:00+05:30', 'UPI', 900)"
            )
        )
        await session.commit()

    monkeypatch.setattr(
        pipeline,
        "config",
        SimpleNamespace(database_url=database_url, pdf_render_timeout_seconds=30),
    )
    monkeypatch.setattr(pipeline, "get_timezone", lambda: ZoneInfo("Asia/Kolkata"))
    monkeypatch.setattr(pipeline, "resolve_run_env", lambda env: "test")
    monkeypatch.setattr(pipeline, "new_run_id", lambda: "run-mtd-failure")

    async def _no_existing(*args, **kwargs):
        return None

    captured_records: list[dict] = []

    async def _capture_persist(_database_url, record):
        captured_records.append(record)

    async def _capture_update(_database_url, _run_id, record):
        captured_records.append(record)

    monkeypatch.setattr(pipeline, "check_existing_run", _no_existing)
    monkeypatch.setattr(pipeline, "persist_summary_record", _capture_persist)
    monkeypatch.setattr(pipeline, "update_summary_record", _capture_update)

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
            same_day_fulfillment_rows=[SimpleNamespace(order_number="RPT-2")],
            missing_payment_rows=[],
        )

    async def _raise_mtd_fetch(*args, **kwargs):
        raise RuntimeError("mtd fetch boom")

    rendered_paths: list[str] = []

    async def _fake_render_pdf(html, output_path: Path, pdf_options=None, logger=None):
        rendered_paths.append(str(output_path))
        output_path.write_bytes(b"pdf")

    async def _fake_notify(*args, **kwargs):
        return {"emails_planned": 1, "emails_sent": 1, "errors": []}

    monkeypatch.setattr(pipeline, "fetch_daily_sales_report", _fake_daily_data)
    monkeypatch.setattr(pipeline, "fetch_mtd_same_day_fulfillment", _raise_mtd_fetch)
    monkeypatch.setattr(
        pipeline, "_render_html", lambda context, *args, **kwargs: "daily-html"
    )
    monkeypatch.setattr(
        pipeline, "render_pdf_with_configured_browser", _fake_render_pdf
    )
    monkeypatch.setattr(pipeline, "send_notifications_for_run", _fake_notify)

    await pipeline._run(report_date=report_date, env="test", force=True)

    daily_path = str(
        pipeline.OUTPUT_ROOT / f"{pipeline.PIPELINE_NAME}_{report_date.isoformat()}.pdf"
    )
    mtd_path = str(
        pipeline.OUTPUT_ROOT
        / f"reports.mtd_same_day_fulfillment_{report_date.isoformat()}.pdf"
    )

    assert daily_path in rendered_paths
    assert mtd_path not in rendered_paths

    async with session_scope(database_url) as session:
        rows = (
            (
                await session.execute(
                    sa.text(
                        """
                    SELECT doc_type, file_name
                    FROM documents
                    ORDER BY file_name
                    """
                    )
                )
            )
            .mappings()
            .all()
        )

    assert len(rows) == 2
    assert rows[0]["doc_type"] == "daily_sales_report_pdf"
    assert rows[1]["doc_type"] == "to_be_recovered_report_pdf"

    assert captured_records
    final_record = captured_records[-1]
    assert final_record["overall_status"] == "warning"
    assert final_record["metrics_json"]["mtd_attachment_generated"] is False
    assert final_record["metrics_json"]["mtd_attachment_error"] == "mtd fetch boom"
    assert (
        "MTD same-day fulfillment attachment was not generated"
        in final_record["summary_text"]
    )
