from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence

import sqlalchemy as sa

from app.dashboard_downloader.config import fetch_store_codes
from app.dashboard_downloader.db_tables import documents
from app.dashboard_downloader.json_logger import JsonLogger
from app.common.dashboard_store import store_dashboard_summary, store_master
from app.common.db import session_scope
from app.config import config

from .base import PipelinePhaseTracker, persist_summary_record

REPORTS_ROOT = Path(config.reports_root).resolve()
TEMPLATE_NAME = "aggregate_report.html"
TEMPLATE_DIR = Path("app") / "dashboard_downloader" / "templates"


def parse_store_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [token.strip().upper() for token in raw.split(",") if token.strip()]


async def get_report_store_codes(database_url: str) -> list[str]:
    stores = await fetch_store_codes(database_url=database_url, report_flag=True)
    if not stores:
        raise RuntimeError(
            "At least one store must be flagged for reporting in store_master before running reporting pipelines"
        )
    return stores


async def fetch_store_period_rows(
    *,
    database_url: str,
    store_codes: Sequence[str],
    period_start: date,
    period_end: date,
) -> list[Mapping[str, Any]]:
    if not store_codes:
        return []
    upper_codes = [code.upper() for code in store_codes]
    async with session_scope(database_url) as session:
        stmt = (
            sa.select(
                sa.func.upper(store_master.c.store_code).label("store_code"),
                sa.func.count().label("row_count"),
                sa.func.avg(store_dashboard_summary.c.delivery_tat_pct).label("avg_delivery_tat"),
                sa.func.avg(store_dashboard_summary.c.repeat_total_base_pct).label("avg_repeat_pct"),
                sa.func.sum(store_dashboard_summary.c.pickup_total_count).label("pickup_total"),
                sa.func.avg(store_dashboard_summary.c.pickup_total_conv_pct).label("avg_conversion"),
            )
            .select_from(store_dashboard_summary.join(store_master, store_master.c.id == store_dashboard_summary.c.store_id))
            .where(sa.func.upper(store_master.c.store_code).in_(upper_codes))
            .where(store_dashboard_summary.c.dashboard_date >= period_start)
            .where(store_dashboard_summary.c.dashboard_date <= period_end)
            .group_by(sa.func.upper(store_master.c.store_code))
        )
        result = await session.execute(stmt)
        return list(result.mappings())


@dataclass
class PdfArtifact:
    store_code: str
    file_path: Path
    period_label: str


async def persist_document(
    *,
    database_url: str,
    pipeline_name: str,
    run_id: str,
    store_code: str,
    report_date: date,
    file_path: Path,
    doc_type: str,
) -> None:
    async with session_scope(database_url) as session:
        await session.execute(
            documents.insert().values(
                doc_type=doc_type,
                doc_subtype="pipeline_report",
                doc_date=report_date,
                reference_name_1="pipeline",
                reference_id_1=pipeline_name,
                reference_name_2="run_id",
                reference_id_2=run_id,
                reference_name_3="store_code",
                reference_id_3=store_code,
                file_name=file_path.name,
                mime_type="application/pdf",
                file_size_bytes=file_path.stat().st_size if file_path.exists() else None,
                storage_backend="fs",
                file_path=str(file_path),
                created_at=datetime.now(timezone.utc),
                created_by="pipeline",
            )
        )
        await session.commit()


async def record_documents(
    *,
    database_url: str,
    pipeline_name: str,
    run_id: str,
    report_date: date,
    artifacts: Sequence[PdfArtifact],
    doc_type: str,
) -> None:
    for artifact in artifacts:
        await persist_document(
            database_url=database_url,
            pipeline_name=pipeline_name,
            run_id=run_id,
            store_code=artifact.store_code,
            report_date=report_date,
            file_path=artifact.file_path,
            doc_type=doc_type,
        )


def render_period_title(prefix: str, period_label: str) -> str:
    return f"{prefix} – {period_label}"


async def render_pdf(
    *,
    template_dir: Path,
    context: Mapping[str, Any],
    output_path: Path,
    logger: JsonLogger,
) -> None:
    from jinja2 import Environment, FileSystemLoader, select_autoescape
    from app.dashboard_downloader.report_generator import render_pdf_with_configured_browser

    template_dir.mkdir(parents=True, exist_ok=True)
    env = Environment(loader=FileSystemLoader(str(template_dir)), autoescape=select_autoescape(["html", "xml"]))
    template = env.get_template(TEMPLATE_NAME)
    html = template.render(**context)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    await render_pdf_with_configured_browser(html, output_path, logger=logger)


async def generate_period_pdfs(
    *,
    pipeline_name: str,
    report_date: date,
    period_label: str,
    store_stats: Mapping[str, Mapping[str, Any]],
    stores_without_data: Sequence[str],
    prefix: str,
    reference_key: str,
    logger: JsonLogger,
) -> list[PdfArtifact]:
    artifacts: list[PdfArtifact] = []
    template_dir = TEMPLATE_DIR
    generated_at = datetime.utcnow().isoformat()

    async def _render_for_store(store_code: str, stats: Mapping[str, Any] | None, missing: bool) -> None:
        context = {
            "title": render_period_title(prefix, period_label),
            "store_code": store_code,
            "period_label": period_label,
            "generated_at": generated_at,
            "stats": stats or {},
            "missing": missing,
            "missing_stores": [],
        }
        output_path = REPORTS_ROOT / f"{report_date.year}" / f"{store_code}_{reference_key}_{period_label}.pdf"
        await render_pdf(
            template_dir=template_dir,
            context=context,
            output_path=output_path,
            logger=logger,
        )
        artifacts.append(PdfArtifact(store_code=store_code, file_path=output_path, period_label=period_label))

    for store_code, stats in store_stats.items():
        await _render_for_store(store_code, stats, False)

    for store_code in stores_without_data:
        await _render_for_store(store_code, None, True)

    return artifacts


async def generate_combined_pdf(
    *,
    pipeline_name: str,
    report_date: date,
    period_label: str,
    combined_stats: Mapping[str, Any],
    missing_stores: Sequence[str],
    prefix: str,
    reference_key: str,
    logger: JsonLogger,
) -> PdfArtifact:
    template_dir = TEMPLATE_DIR
    generated_at = datetime.utcnow().isoformat()
    context = {
        "title": render_period_title(prefix, period_label),
        "store_code": "ALL",
        "period_label": period_label,
        "generated_at": generated_at,
        "stats": combined_stats,
        "missing": False,
        "missing_stores": list(missing_stores),
    }
    output_path = REPORTS_ROOT / f"{report_date.year}" / f"ALL_{reference_key}_{period_label}.pdf"
    await render_pdf(
        template_dir=template_dir,
        context=context,
        output_path=output_path,
        logger=logger,
    )
    return PdfArtifact(store_code="ALL", file_path=output_path, period_label=period_label)


async def write_summary(
    *,
    tracker: PipelinePhaseTracker,
    database_url: str,
) -> None:
    finished_at = datetime.utcnow().replace(tzinfo=timezone.utc)
    record = tracker.build_record(finished_at)
    await persist_summary_record(database_url, record)
