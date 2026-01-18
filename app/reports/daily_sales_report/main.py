from __future__ import annotations

from argparse import ArgumentParser
from datetime import date

from app.common.date_utils import aware_now, get_timezone

from .pipeline import run_pipeline


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid date format: {value}. Use YYYY-MM-DD.") from exc


def main() -> None:
    parser = ArgumentParser(description="Run the daily sales report pipeline.")
    parser.add_argument("--report-date", type=_parse_date, help="Report date (YYYY-MM-DD).")
    parser.add_argument("--env", type=str, default=None, help="Override run environment.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerate the report even if a successful run exists for the date.",
    )
    args = parser.parse_args()

    report_date = args.report_date
    if report_date is None:
        report_date = aware_now(get_timezone()).date()

    run_pipeline(report_date=report_date, env=args.env, force=args.force)


if __name__ == "__main__":
    main()
