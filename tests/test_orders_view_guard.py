from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

_FAILURE_GUIDANCE = (
    "Reports and decision-making code must use vw_orders.order_amount. "
    "Raw orders.net_amount, orders.gross_amount, and orders.adjustment are "
    "source/ingest fields only."
)

_RAW_ORDERS_SQL = re.compile(r"\b(?:FROM|JOIN)\s+orders\b", re.IGNORECASE)
_SQLALCHEMY_ORDERS_TABLE = re.compile(
    r"\b(?:sa\.)?(?:table|Table)\(\s*[\"']orders[\"']", re.IGNORECASE
)
_RAW_ORDERS_AMOUNT_FIELD = re.compile(
    r"\b(?:orders|o)\.(?:c\.)?(?:net_amount|gross_amount|adjustment)\b",
    re.IGNORECASE,
)
_AMOUNT_DECISION_ASSIGNMENT = re.compile(
    r"\b(?:order_amount|amount|total|value)\b\s*=.*\b(?:net_amount|gross_amount|adjustment)\b",
    re.IGNORECASE,
)

_SOURCE_SUFFIXES = {".py", ".sql", ".html", ".jinja", ".j2"}
_DASHBOARD_REPORT_PATHS = (
    Path("app/dashboard_downloader/report_generator.py"),
    Path("app/dashboard_downloader/run_store_reports.py"),
    Path("app/dashboard_downloader/pipelines/reporting.py"),
    Path("app/dashboard_downloader/templates"),
)


_FORBIDDEN_PATTERNS = (
    ("raw SQL FROM/JOIN orders", _RAW_ORDERS_SQL),
    ("SQLAlchemy table/alias targeting orders", _SQLALCHEMY_ORDERS_TABLE),
    ("raw orders amount field", _RAW_ORDERS_AMOUNT_FIELD),
    ("direct amount decision from raw amount field", _AMOUNT_DECISION_ASSIGNMENT),
)


def _source_files_under(path: Path) -> list[Path]:
    if path.is_file():
        return [path] if path.suffix in _SOURCE_SUFFIXES else []
    return sorted(
        child
        for child in path.rglob("*")
        if child.is_file() and child.suffix in _SOURCE_SUFFIXES
    )


def _guarded_source_files() -> list[Path]:
    guarded: set[Path] = set(_source_files_under(REPO_ROOT / "app/reports"))
    for relative_path in _DASHBOARD_REPORT_PATHS:
        guarded.update(_source_files_under(REPO_ROOT / relative_path))
    return sorted(guarded)


def test_reports_use_vw_orders_for_order_amount_decisions() -> None:
    """Guard report code from reading base orders amounts directly."""

    failures: list[str] = []
    for path in _guarded_source_files():
        relative_path = path.relative_to(REPO_ROOT)
        for line_number, line in enumerate(path.read_text().splitlines(), start=1):
            for label, pattern in _FORBIDDEN_PATTERNS:
                if pattern.search(line):
                    failures.append(
                        f"{relative_path}:{line_number}: {label}: {line.strip()}"
                    )

    assert not failures, _FAILURE_GUIDANCE + "\n" + "\n".join(failures)
