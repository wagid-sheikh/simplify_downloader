from __future__ import annotations

import argparse
import asyncio
from datetime import date
from typing import Any

import sqlalchemy as sa

from app.common.db import session_scope
from app.config import config
from app.dashboard_downloader.json_logger import get_logger, log_event, new_run_id

# TEMP_DEBUG_SHORT_PAYMENTS_WRITE_OFF_LEAK
TEMP_DEBUG_SHORT_PAYMENTS_WRITE_OFF_LEAK_MARKER = "TEMP_DEBUG_SHORT_PAYMENTS_WRITE_OFF_LEAK"
# TEMP_DEBUG_SHORT_PAYMENTS_WRITE_OFF_LEAK
TEMP_DEBUG_RECOVERY_TARGET_COST_CENTER = "UN3668"
# TEMP_DEBUG_SHORT_PAYMENTS_WRITE_OFF_LEAK
TEMP_DEBUG_RECOVERY_TARGET_ORDER_NUMBER = "T2724"
# TEMP_DEBUG_SHORT_PAYMENTS_WRITE_OFF_LEAK
TEMP_DEBUG_RECOVERY_BASE_COLUMNS = (
    "cost_center",
    "order_number",
    "order_date",
    "order_amount",
    "recovery_status",
    "recovery_category",
    "recovery_notes",
)
# TEMP_DEBUG_SHORT_PAYMENTS_WRITE_OFF_LEAK
TEMP_DEBUG_RECOVERY_OPTIONAL_COLUMNS = ("updated_at", "run_id")


def _selectable_vw_orders_columns(available_columns: set[str]) -> list[str]:
    return [
        column
        for column in (
            *TEMP_DEBUG_RECOVERY_BASE_COLUMNS,
            *TEMP_DEBUG_RECOVERY_OPTIONAL_COLUMNS,
        )
        if column in available_columns
    ]


async def log_temp_debug_recovery_status_check(
    *,
    database_url: str,
    logger: Any,
    boundary: str,
    report_date: date | None = None,
) -> None:
    """Log targeted vw_orders recovery state at an orders/report boundary."""

    try:
        async with session_scope(database_url) as session:
            def _vw_orders_columns(sync_session: Any) -> set[str]:
                connection = sync_session.connection()
                return {
                    column["name"]
                    for column in sa.inspect(connection).get_columns("vw_orders")
                }

            available_columns = await session.run_sync(_vw_orders_columns)
            missing_base_columns = [
                column
                for column in TEMP_DEBUG_RECOVERY_BASE_COLUMNS
                if column not in available_columns
            ]
            selected_columns = _selectable_vw_orders_columns(available_columns)
            if missing_base_columns or not selected_columns:
                log_event(
                    logger=logger,
                    phase="temp_debug_recovery_status",
                    status="warning",
                    message=(
                        f"{TEMP_DEBUG_SHORT_PAYMENTS_WRITE_OFF_LEAK_MARKER} "
                        "targeted vw_orders recovery-status check skipped; "
                        "required columns are missing"
                    ),
                    boundary=boundary,
                    report_date=report_date.isoformat() if report_date else None,
                    target_cost_center=TEMP_DEBUG_RECOVERY_TARGET_COST_CENTER,
                    target_order_number=TEMP_DEBUG_RECOVERY_TARGET_ORDER_NUMBER,
                    missing_base_columns=missing_base_columns,
                    available_optional_columns=[
                        column
                        for column in TEMP_DEBUG_RECOVERY_OPTIONAL_COLUMNS
                        if column in available_columns
                    ],
                )
                return

            selected_sql = ", ".join(selected_columns)
            result = await session.execute(
                sa.text(
                    f"""
                    SELECT {selected_sql}
                    FROM vw_orders
                    WHERE cost_center = :cost_center
                      AND order_number = :order_number
                    ORDER BY order_date DESC
                    LIMIT 10
                    """
                ),
                {
                    "cost_center": TEMP_DEBUG_RECOVERY_TARGET_COST_CENTER,
                    "order_number": TEMP_DEBUG_RECOVERY_TARGET_ORDER_NUMBER,
                },
            )
            rows = [dict(row) for row in result.mappings().all()]
            log_event(
                logger=logger,
                phase="temp_debug_recovery_status",
                status="info" if rows else "warning",
                message=(
                    f"{TEMP_DEBUG_SHORT_PAYMENTS_WRITE_OFF_LEAK_MARKER} "
                    "targeted vw_orders recovery-status check"
                ),
                boundary=boundary,
                report_date=report_date.isoformat() if report_date else None,
                target_cost_center=TEMP_DEBUG_RECOVERY_TARGET_COST_CENTER,
                target_order_number=TEMP_DEBUG_RECOVERY_TARGET_ORDER_NUMBER,
                selected_columns=selected_columns,
                row_count=len(rows),
                rows=rows,
            )
    except Exception as exc:
        log_event(
            logger=logger,
            phase="temp_debug_recovery_status",
            status="warning",
            message=(
                f"{TEMP_DEBUG_SHORT_PAYMENTS_WRITE_OFF_LEAK_MARKER} "
                "targeted vw_orders recovery-status check failed"
            ),
            boundary=boundary,
            report_date=report_date.isoformat() if report_date else None,
            target_cost_center=TEMP_DEBUG_RECOVERY_TARGET_COST_CENTER,
            target_order_number=TEMP_DEBUG_RECOVERY_TARGET_ORDER_NUMBER,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )


async def _amain(boundary: str) -> None:
    logger = get_logger(run_id=f"temp_debug_recovery_status_{new_run_id()}")
    database_url = config.database_url
    if not database_url:
        log_event(
            logger=logger,
            phase="temp_debug_recovery_status",
            status="warning",
            message=(
                f"{TEMP_DEBUG_SHORT_PAYMENTS_WRITE_OFF_LEAK_MARKER} "
                "targeted vw_orders recovery-status check skipped; database URL is missing"
            ),
            boundary=boundary,
            target_cost_center=TEMP_DEBUG_RECOVERY_TARGET_COST_CENTER,
            target_order_number=TEMP_DEBUG_RECOVERY_TARGET_ORDER_NUMBER,
        )
        return
    await log_temp_debug_recovery_status_check(
        database_url=database_url,
        logger=logger,
        boundary=boundary,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Temporary targeted vw_orders recovery-status diagnostics."
    )
    parser.add_argument("--boundary", required=True)
    args = parser.parse_args()
    asyncio.run(_amain(args.boundary))


if __name__ == "__main__":
    main()
