"""Helpers for persisting store dashboard summaries."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..dashboard_downloader.json_logger import JsonLogger, log_event
from .db import session_scope

DASHBOARD_SUMMARY_COLUMNS = (
    "prev_month_revenue",
    "target_revenue",
    "lmt_d_revenue",
    "mtd_revenue",
    "ftd_revenue",
    "tgt_vs_ach_pct",
    "growth_pct",
    "extrapolated_pct",
    "pickup_new_count",
    "pickup_new_conv_count",
    "pickup_new_conv_pct",
    "pickup_existing_count",
    "pickup_existing_conv_count",
    "pickup_existing_conv_pct",
    "pickup_total_count",
    "pickup_total_conv_count",
    "pickup_total_conv_pct",
    "delivery_total_orders",
    "delivery_within_tat_count",
    "delivery_tat_pct",
    "delivery_total_delivered",
    "delivery_undel_over_10_days",
    "delivery_total_undelivered",
    "repeat_customer_base_6m",
    "repeat_orders",
    "repeat_total_base_pct",
    "package_target",
    "package_new",
    "package_ftd",
    "package_achievement_pct",
    "package_overall",
    "package_non_pkg_over_800",
    "package_non_pkg_over_800_undelivered",
)

_metadata = sa.MetaData()

store_master = sa.Table(
    "store_master",
    _metadata,
    sa.Column("id", sa.BigInteger, primary_key=True),
    sa.Column("store_code", sa.Text, nullable=False, unique=True),
    sa.Column("store_name", sa.Text),
    sa.Column("gstin", sa.Text),
    sa.Column("launch_date", sa.Date),
    sa.Column("etl_flag", sa.Boolean, nullable=False, server_default=sa.text("false")),
    sa.Column("report_flag", sa.Boolean, nullable=False, server_default=sa.text("false")),
    sa.Column("is_active", sa.Boolean, nullable=False, server_default=sa.text("true")),
    sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
)

store_dashboard_summary = sa.Table(
    "store_dashboard_summary",
    _metadata,
    sa.Column("id", sa.BigInteger, primary_key=True),
    sa.Column("store_id", sa.BigInteger, sa.ForeignKey("store_master.id", ondelete="CASCADE"), nullable=False),
    sa.Column("dashboard_date", sa.Date, nullable=False),
    sa.Column("run_date_time", sa.DateTime(timezone=True), nullable=False),
    sa.Column("prev_month_revenue", sa.Numeric(14, 2), nullable=True),
    sa.Column("target_revenue", sa.Numeric(14, 2), nullable=True),
    sa.Column("lmt_d_revenue", sa.Numeric(14, 2), nullable=True),
    sa.Column("mtd_revenue", sa.Numeric(14, 2), nullable=True),
    sa.Column("ftd_revenue", sa.Numeric(14, 2), nullable=True),
    sa.Column("tgt_vs_ach_pct", sa.Numeric(6, 2), nullable=True),
    sa.Column("growth_pct", sa.Numeric(6, 2), nullable=True),
    sa.Column("extrapolated_pct", sa.Numeric(6, 2), nullable=True),
    sa.Column("pickup_new_count", sa.Integer(), nullable=True),
    sa.Column("pickup_new_conv_count", sa.Integer(), nullable=True),
    sa.Column("pickup_new_conv_pct", sa.Numeric(6, 2), nullable=True),
    sa.Column("pickup_existing_count", sa.Integer(), nullable=True),
    sa.Column("pickup_existing_conv_count", sa.Integer(), nullable=True),
    sa.Column("pickup_existing_conv_pct", sa.Numeric(6, 2), nullable=True),
    sa.Column("pickup_total_count", sa.Integer(), nullable=True),
    sa.Column("pickup_total_conv_count", sa.Integer(), nullable=True),
    sa.Column("pickup_total_conv_pct", sa.Numeric(6, 2), nullable=True),
    sa.Column("delivery_total_orders", sa.Integer(), nullable=True),
    sa.Column("delivery_within_tat_count", sa.Integer(), nullable=True),
    sa.Column("delivery_tat_pct", sa.Numeric(6, 2), nullable=True),
    sa.Column("delivery_total_delivered", sa.Integer(), nullable=True),
    sa.Column("delivery_undel_over_10_days", sa.Integer(), nullable=True),
    sa.Column("delivery_total_undelivered", sa.Integer(), nullable=True),
    sa.Column("repeat_customer_base_6m", sa.Integer(), nullable=True),
    sa.Column("repeat_orders", sa.Integer(), nullable=True),
    sa.Column("repeat_total_base_pct", sa.Numeric(6, 2), nullable=True),
    sa.Column("package_target", sa.Integer(), nullable=True),
    sa.Column("package_new", sa.Integer(), nullable=True),
    sa.Column("package_ftd", sa.Integer(), nullable=True),
    sa.Column("package_achievement_pct", sa.Numeric(6, 2), nullable=True),
    sa.Column("package_overall", sa.Integer(), nullable=True),
    sa.Column("package_non_pkg_over_800", sa.Integer(), nullable=True),
    sa.Column("package_non_pkg_over_800_undelivered", sa.Integer(), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    sa.UniqueConstraint(
        "store_id",
        "dashboard_date",
        name="uq_store_dashboard_summary_store_date",
    ),
)


def _log(
    *,
    logger: JsonLogger,
    store_code: str | None,
    status: str,
    message: str,
    extras: Dict[str, Any] | None = None,
) -> None:
    log_event(
        logger=logger,
        phase="download",
        status=status,
        message=message,
        store_code=store_code,
        bucket=None,
        extras=extras,
    )


async def persist_dashboard_summary(
    dashboard_data: Dict[str, Any],
    *,
    database_url: str,
    logger: JsonLogger,
) -> None:
    if not database_url:
        _log(
            logger=logger,
            store_code=dashboard_data.get("store_code"),
            status="warn",
            message="database url missing; skipping dashboard persistence",
        )
        return

    store_code = dashboard_data.get("store_code")
    if not store_code:
        _log(
            logger=logger,
            store_code=None,
            status="warn",
            message="store_code missing in dashboard data; skipping persist",
        )
        return

    dashboard_date = dashboard_data.get("dashboard_date")
    if not dashboard_date:
        _log(
            logger=logger,
            store_code=store_code,
            status="warn",
            message="dashboard_date missing; skipping persist",
        )
        return

    async with session_scope(database_url) as session:
        async with session.begin():
            _log(
                logger=logger,
                store_code=store_code,
                status="info",
                message="persisting store master",
                extras={"store_code": store_code, "gstin": dashboard_data.get("gstin")},
            )

            store_insert = pg_insert(store_master).values(
                store_code=store_code,
                store_name=dashboard_data.get("store_name"),
                launch_date=dashboard_data.get("launch_date"),
                gstin=dashboard_data.get("gstin"),
                is_active=True,
            )
            update_values = {
                "store_name": sa.func.coalesce(
                    store_insert.excluded.store_name, store_master.c.store_name
                ),
                "launch_date": sa.func.coalesce(
                    store_insert.excluded.launch_date, store_master.c.launch_date
                ),
                "gstin": sa.func.coalesce(
                    store_insert.excluded.gstin, store_master.c.gstin
                ),
                "is_active": sa.true(),
                "updated_at": sa.func.now(),
            }
            store_insert = (
                store_insert.on_conflict_do_update(
                    index_elements=[store_master.c.store_code],
                    set_=update_values,
                ).returning(store_master.c.id)
            )
            store_id = (await session.execute(store_insert)).scalar_one()

            run_time = datetime.now(timezone.utc)
            insert_values = {
                "store_id": store_id,
                "dashboard_date": dashboard_date,
                "run_date_time": run_time,
            }
            for column in DASHBOARD_SUMMARY_COLUMNS:
                insert_values[column] = dashboard_data.get(column)

            summary_insert = pg_insert(store_dashboard_summary).values(**insert_values)
            update_values = {column: dashboard_data.get(column) for column in DASHBOARD_SUMMARY_COLUMNS}
            update_values.update({"run_date_time": run_time, "updated_at": sa.func.now()})

            summary_stmt = summary_insert.on_conflict_do_update(
                index_elements=[
                    store_dashboard_summary.c.store_id,
                    store_dashboard_summary.c.dashboard_date,
                ],
                set_=update_values,
            )
            await session.execute(summary_stmt)

    _log(
        logger=logger,
        store_code=store_code,
        status="info",
        message="dashboard summary persisted",
        extras={"dashboard_date": dashboard_date.isoformat()},
    )
