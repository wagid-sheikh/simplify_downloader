from __future__ import annotations

from argparse import ArgumentParser
from datetime import date
from typing import Sequence

from app.common.date_utils import aware_now, get_timezone

from .pipeline import run_pipeline


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid date format: {value}. Use YYYY-MM-DD.") from exc


def main(argv: Sequence[str] | None = None) -> None:
    parser = ArgumentParser(description="Run the daily sales report pipeline.")
    parser.add_argument(
        "--report-date", type=_parse_date, help="Report date (YYYY-MM-DD)."
    )
    parser.add_argument(
        "--env", type=str, default=None, help="Override run environment."
    )
    parser.add_argument(
        "--orders-sync-upstream-status",
        type=str,
        default=None,
        help="Status of the upstream orders sync run that preceded this report.",
    )
    parser.add_argument(
        "--orders-sync-upstream-run-id",
        type=str,
        default=None,
        help="Run ID of the upstream orders sync run that preceded this report.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Deprecated no-op; reports always regenerate and append new summaries/documents.",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    report_date = args.report_date
    if report_date is None:
        report_date = aware_now(get_timezone()).date()

    run_pipeline(
        report_date=report_date,
        env=args.env,
        force=args.force,
        orders_sync_upstream_status=args.orders_sync_upstream_status,
        orders_sync_upstream_run_id=args.orders_sync_upstream_run_id,
    )


if __name__ == "__main__":
    main()
