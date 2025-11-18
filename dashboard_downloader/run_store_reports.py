from __future__ import annotations

import argparse
import asyncio
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple

import sqlalchemy as sa

from common.date_utils import get_daily_report_date
from common.db import session_scope

from dashboard_downloader.db_tables import documents
from dashboard_downloader.json_logger import JsonLogger, get_logger, log_event, new_run_id
from dashboard_downloader.report_generator import (
    StoreReportDataNotFound,
    build_store_context,
    render_store_report_pdf,
)
from dashboard_downloader.run_summary import PIPELINE_NAME, RunAggregator
from simplify_downloader.config import config

DEFAULT_TEMPLATE_DIR = Path(__file__).with_name("templates")
STORE_TEMPLATE_FILE_NAME = "store_report.html"
DEFAULT_TEMPLATE_FILE = DEFAULT_TEMPLATE_DIR / STORE_TEMPLATE_FILE_NAME
DEFAULT_REPORTS_ROOT = Path(config.reports_root).resolve()

__all__ = [
    "resolve_report_date",
    "parse_store_list",
    "run_store_reports_for_date",
]


def parse_store_list(raw: str | None) -> List[str]:
    if not raw:
        return []
    return [token.strip().upper() for token in raw.split(",") if token.strip()]


def resolve_report_date(arg_value: str | None = None) -> date:
    if arg_value:
        return date.fromisoformat(arg_value)
    return get_daily_report_date()


def get_configured_store_codes() -> List[str]:
    return [code.upper() for code in config.report_stores_list]


def _log_skip(logger: JsonLogger) -> None:
    log_event(
        logger=logger,
        phase="report",
        status="info",
        message="no REPORT_STORES_LIST configured, skipping report generation",
    )


def _parse_args(argv: Iterable[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate per-store daily PDF reports")
    parser.add_argument("--report-date", dest="report_date", help="Report date (YYYY-MM-DD)")
    parser.add_argument("--run-id", dest="run_id", help="Override run id", default=None)
    parser.add_argument(
        "--template-path",
        dest="template_path",
        default=str(DEFAULT_TEMPLATE_FILE),
        help="Path to store_report.html (or a directory containing it)",
    )
    parser.add_argument(
        "--reports-dir",
        dest="reports_dir",
        default=str(DEFAULT_REPORTS_ROOT),
        help="Directory to write generated PDFs",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


async def _persist_document_record(
    *,
    database_url: str | None,
    report_date: date,
    store_code: str,
    run_id: str,
    file_name: str,
    file_path: Path | None,
    status: str,
    error_message: str | None,
    logger: JsonLogger,
) -> None:
    if not database_url:
        return

    size = None
    path_str = None
    if file_path and file_path.exists():
        size = file_path.stat().st_size
        path_str = str(file_path)

    values = {
        "doc_type": "store_daily_pdf",
        "doc_subtype": "pipeline_report",
        "doc_date": report_date,
        "reference_name_1": "pipeline",
        "reference_id_1": PIPELINE_NAME,
        "reference_name_2": "run_id",
        "reference_id_2": run_id,
        "reference_name_3": "store_code",
        "reference_id_3": store_code,
        "file_name": file_name,
        "mime_type": "application/pdf",
        "file_size_bytes": size,
        "storage_backend": "fs",
        "file_path": path_str,
        "file_blob": None,
        "checksum": None,
        "status": status,
        "error_message": error_message,
        "created_by": "pipeline",
        "created_at": datetime.now(timezone.utc),
    }

    try:
        async with session_scope(database_url) as session:
            await session.execute(sa.insert(documents).values(**values))
            await session.commit()
    except Exception as exc:  # pragma: no cover - defensive
        log_event(
            logger=logger,
            phase="report",
            status="error",
            message="failed to persist document record",
            store_code=store_code,
            extras={"error": str(exc), "file_name": file_name},
        )
        return

    log_event(
        logger=logger,
        phase="report",
        status="ok",
        message="document record persisted",
        store_code=store_code,
        extras={"file_name": file_name, "file_path": path_str},
    )


def _resolve_template_file(template_path: Path) -> Path:
    template_path = Path(template_path)
    if template_path.is_dir():
        template_path = template_path / STORE_TEMPLATE_FILE_NAME
    if not template_path.exists():
        raise FileNotFoundError(f"template not found: {template_path}")
    if not template_path.is_file():
        raise FileNotFoundError(f"template is not a file: {template_path}")
    return template_path


async def _generate_reports(
    store_codes: Sequence[str],
    report_date: date,
    *,
    logger: JsonLogger,
    run_id: str,
    database_url: str,
    template_file: Path,
    reports_root: Path,
    aggregator: RunAggregator | None = None,
) -> List[Tuple[str, Path]]:
    generated: List[Tuple[str, Path]] = []
    for code in store_codes:
        log_event(
            logger=logger,
            phase="report",
            status="info",
            message="report generation start",
            store_code=code,
            extras={"report_date": report_date.isoformat(), "run_id": run_id},
        )
        try:
            context = await build_store_context(
                store_code=code,
                report_date=report_date,
                run_id=run_id,
                database_url=database_url,
            )
        except StoreReportDataNotFound as exc:
            log_event(
                logger=logger,
                phase="report",
                status="warning",
                message="no data available for report date",
                store_code=code,
                extras={"report_date": report_date.isoformat(), "error": str(exc)},
            )
            continue
        except Exception as exc:  # pragma: no cover - safeguard
            log_event(
                logger=logger,
                phase="report",
                status="error",
                message="failed to build report context",
                store_code=code,
                extras={"report_date": report_date.isoformat(), "error": str(exc)},
            )
            if aggregator:
                aggregator.register_pdf_failure(code, "context failure")
            continue

        summary_output_path = reports_root / f"{report_date.year}" / f"{code}_{report_date:%m-%d}.pdf"
        try:
            await render_store_report_pdf(
                store_context=context,
                output_path=summary_output_path,
                template_path=template_file,
            )
        except Exception as exc:  # pragma: no cover - pdf failures
            log_event(
                logger=logger,
                phase="report",
                status="error",
                message="failed to render pdf",
                store_code=code,
                extras={
                    "report_date": report_date.isoformat(),
                    "error": str(exc),
                    "output_path": str(summary_output_path),
                },
            )
            await _persist_document_record(
                database_url=database_url,
                report_date=report_date,
                store_code=code,
                run_id=run_id,
                file_name=summary_output_path.name,
                file_path=None,
                status="error",
                error_message=str(exc),
                logger=logger,
            )
            if aggregator:
                aggregator.register_pdf_failure(code, "render failure")
            continue

        generated.append((code, summary_output_path))
        log_event(
            logger=logger,
            phase="report",
            status="ok",
            message="report pdf generated",
            store_code=code,
            extras={
                "report_date": report_date.isoformat(),
                "output_path": str(summary_output_path),
            },
        )
        if aggregator:
            aggregator.record_pdf_file(code, str(summary_output_path))

        try:
            await _persist_document_record(
                database_url=database_url,
                report_date=report_date,
                store_code=code,
                run_id=run_id,
                file_name=summary_output_path.name,
                file_path=summary_output_path,
                status="ok",
                error_message=None,
                logger=logger,
            )
        except Exception as exc:  # pragma: no cover - defensive
            log_event(
                logger=logger,
                phase="report",
                status="warning",
                message="unexpected error while recording document",
                store_code=code,
                extras={"error": str(exc), "file_name": summary_output_path.name},
            )

        if aggregator:
            aggregator.register_pdf_success(
                code, str(summary_output_path), record_file=False
            )
    return generated


async def run_store_reports_for_date(
    report_date: date,
    *,
    logger: JsonLogger,
    run_id: str,
    database_url: str | None,
    store_codes: Sequence[str] | None = None,
    template_path: str | Path = DEFAULT_TEMPLATE_FILE,
    reports_root: str | Path = DEFAULT_REPORTS_ROOT,
) -> List[Tuple[str, Path]]:
    codes = [code.upper() for code in store_codes] if store_codes else get_configured_store_codes()
    resolved_db_url = database_url or config.database_url
    if not codes:
        _log_skip(logger)
        return []

    if not resolved_db_url:
        log_event(
            logger=logger,
            phase="report",
            status="error",
            message="DATABASE_URL is required for report generation",
        )
        return []

    template_file = _resolve_template_file(Path(template_path))
    reports_dir = Path(reports_root)

    aggregator: RunAggregator | None = getattr(logger, "aggregator", None)

    pdf_records = await _generate_reports(
        codes,
        report_date,
        logger=logger,
        run_id=run_id,
        database_url=resolved_db_url,
        template_file=template_file,
        reports_root=reports_dir,
        aggregator=aggregator,
    )

    return pdf_records


async def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    run_id = args.run_id or new_run_id()
    logger = get_logger(run_id=run_id)

    try:
        report_date = resolve_report_date(args.report_date)
    except ValueError:
        log_event(
            logger=logger,
            phase="report",
            status="error",
            message="invalid --report-date supplied",
            extras={"value": args.report_date},
        )
        logger.close()
        return 1

    await run_store_reports_for_date(
        report_date,
        logger=logger,
        run_id=run_id,
        database_url=config.database_url,
        store_codes=config.report_stores_list,
        template_path=Path(args.template_path),
        reports_root=Path(args.reports_dir),
    )
    logger.close()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(asyncio.run(main()))
