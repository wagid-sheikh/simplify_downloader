from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from zoneinfo import ZoneInfo
import json
import logging
import os
import re
from decimal import Decimal
from typing import Any

import sqlalchemy as sa
from dateutil import parser

from app.common.db import session_scope
from app.crm_downloader.td_orders_sync.sales_ingest import _sales_table
from app.crm_downloader.uc_orders_sync.archive_ingest import (
    TABLE_ARCHIVE_BASE,
    TABLE_ARCHIVE_ORDER_DETAILS,
    TABLE_ARCHIVE_PAYMENT_DETAILS,
)
from app.crm_downloader.uc_orders_sync.ingest import _stg_uc_orders_table
from app.crm_downloader.uc_orders_sync.ingest import _orders_table

REASON_MISSING_PARENT_ORDER_CONTEXT = "missing_parent_order_context"
REASON_UNPARSEABLE_PAYMENT_DATE = "unparseable_payment_date"
REASON_MISSING_REQUIRED_IDENTIFIERS = "missing_required_identifiers"
REASON_PREFLIGHT_PARENT_COVERAGE_NEAR_ZERO = "preflight_parent_coverage_near_zero"
REASON_PREFLIGHT_PARENT_COVERAGE_LOW = "preflight_parent_coverage_low"

PREFLIGHT_PARENT_COVERAGE_MIN = Decimal("0.80")

LOGGER = logging.getLogger(__name__)


@dataclass
class PublishMetrics:
    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    warnings: int = 0
    reason_codes: dict[str, int] = field(default_factory=dict)
    publish_parent_match_rate: float | None = None
    missing_parent_count: int = 0
    preflight_warning: str | None = None
    preflight_diagnostics: dict[str, Any] | None = None


@dataclass
class ArchivePublishResult:
    orders: PublishMetrics
    sales: PublishMetrics


def _append_reason(counter: Counter[str], reason: str) -> None:
    counter[reason] += 1


def _merge_remarks(*remarks: str | None) -> str | None:
    parts: list[str] = []
    for remark in remarks:
        if not remark:
            continue
        parts.extend([token.strip() for token in remark.split(";") if token.strip()])
    if not parts:
        return None
    return "; ".join(sorted(set(parts)))


def _parse_payment_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = parser.parse(str(value), dayfirst=True)
    except Exception:
        return None
    tz_name = os.getenv("PIPELINE_TIMEZONE", "Asia/Kolkata")
    tz = ZoneInfo(tz_name)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=tz)


def _normalize_store_code(value: Any) -> str | None:
    if value is None:
        return None
    token = re.sub(r"\s+", "", str(value).strip().upper())
    token = re.sub(r"[^A-Z0-9]", "", token)
    if not token:
        return None
    if token.startswith("UC"):
        return token
    if token.isdigit():
        return f"UC{token}"
    return token


def _normalize_order_code(value: Any) -> str | None:
    if value is None:
        return None
    token = re.sub(r"\s+", "", str(value).strip().upper())
    return token or None


def _strip_store_prefix(order_code: str, store_code: str) -> str | None:
    prefixes = [store_code]
    if store_code.startswith("UC"):
        prefixes.append(store_code[2:])
    for prefix in prefixes:
        if not prefix:
            continue
        for separator in ("-", "_", "/"):
            marker = f"{prefix}{separator}"
            if order_code.startswith(marker):
                stripped = order_code[len(marker) :]
                return stripped or None
        if order_code == prefix:
            return None
    return None


def _build_join_key_variants(store_code: Any, order_code: Any) -> list[tuple[str, str]]:
    normalized_store_code = _normalize_store_code(store_code)
    normalized_order_code = _normalize_order_code(order_code)
    if not normalized_store_code or not normalized_order_code:
        return []

    variants = [normalized_order_code]
    stripped = _strip_store_prefix(normalized_order_code, normalized_store_code)
    if stripped:
        variants.append(stripped)
    deduped_variants = list(dict.fromkeys(variants))
    return [(normalized_store_code, variant) for variant in deduped_variants]


async def publish_uc_archive_order_details_to_orders(
    *,
    database_url: str,
    store_code: str | None = None,
    run_id: str | None = None,
) -> PublishMetrics:
    metrics = PublishMetrics()
    metadata = sa.MetaData()
    details = sa.Table(
        TABLE_ARCHIVE_ORDER_DETAILS,
        metadata,
        sa.Column("run_id", sa.Text),
        sa.Column("store_code", sa.String(8)),
        sa.Column("order_code", sa.String(24)),
        sa.Column("quantity", sa.Numeric(12, 2)),
        sa.Column("weight", sa.Numeric(12, 3)),
        sa.Column("service", sa.Text),
        extend_existing=True,
    )
    orders = _orders_table(metadata)

    async with session_scope(database_url) as session:
        normalized_store_scope = _normalize_store_code(store_code)
        detail_filters: list[Any] = []
        if normalized_store_scope:
            detail_filters.append(details.c.store_code == normalized_store_scope)
        if run_id:
            detail_filters.append(details.c.run_id == run_id)

        details_query = sa.select(
            details.c.store_code,
            details.c.order_code,
            details.c.quantity,
            details.c.weight,
            details.c.service,
        )
        if detail_filters:
            details_query = details_query.where(sa.and_(*detail_filters))

        rows = (
            await session.execute(
                details_query
            )
        ).all()

        grouped: dict[tuple[str, str], dict[str, Any]] = defaultdict(
            lambda: {"pieces": Decimal("0"), "weight": Decimal("0"), "services": set()}
        )
        key_variants_by_grouped_key: dict[tuple[str, str], list[tuple[str, str]]] = {}
        for row in rows:
            join_variants = _build_join_key_variants(row.store_code, row.order_code)
            if not join_variants:
                metrics.skipped += 1
                metrics.warnings += 1
                continue
            grouped_key = join_variants[0]
            key_variants_by_grouped_key[grouped_key] = join_variants
            group = grouped[grouped_key]
            if row.quantity is not None:
                group["pieces"] += Decimal(str(row.quantity))
            if row.weight is not None:
                group["weight"] += Decimal(str(row.weight))
            if row.service and str(row.service).strip():
                group["services"].add(str(row.service).strip())

        if not grouped:
            return metrics

        candidate_keys = {variant for variants in key_variants_by_grouped_key.values() for variant in variants}
        existing_orders: dict[tuple[str, str], Any] = {}
        order_rows = (
            await session.execute(
                sa.select(orders.c.id, orders.c.store_code, orders.c.order_number).where(
                    sa.tuple_(orders.c.store_code, orders.c.order_number).in_(list(candidate_keys))
                )
            )
        ).all()
        for row in order_rows:
            normalized_parent_key = _build_join_key_variants(row.store_code, row.order_number)
            if not normalized_parent_key:
                continue
            existing_orders[normalized_parent_key[0]] = row.id

        matched_keys = {
            grouped_key
            for grouped_key, variants in key_variants_by_grouped_key.items()
            if any(variant in existing_orders for variant in variants)
        }
        missing_archive_keys = sorted(set(grouped.keys()) - matched_keys)
        parent_keys = set(existing_orders.keys())
        unmatched_parent_keys = sorted(parent_keys - set(grouped.keys()))
        LOGGER.info(
            "archive_publish_order_details_key_coverage %s",
            json.dumps(
                {
                    "total_candidate_keys": len(grouped),
                    "matched_keys": len(matched_keys),
                    "sample_unmatched_archive_keys": [f"{store}:{order}" for store, order in missing_archive_keys[:5]],
                    "sample_unmatched_parent_keys": [f"{store}:{order}" for store, order in unmatched_parent_keys[:5]],
                },
                sort_keys=True,
            ),
        )

        reasons = Counter[str]()
        for grouped_key, aggregate in grouped.items():
            order_id = None
            for variant in key_variants_by_grouped_key.get(grouped_key, [grouped_key]):
                order_id = existing_orders.get(variant)
                if order_id is not None:
                    break
            if order_id is None:
                metrics.skipped += 1
                metrics.warnings += 1
                _append_reason(reasons, REASON_MISSING_PARENT_ORDER_CONTEXT)
                continue
            values: dict[str, Any] = {}
            if aggregate["pieces"] > 0:
                values["pieces"] = aggregate["pieces"]
            if aggregate["weight"] > 0:
                values["weight"] = aggregate["weight"]
            if aggregate["services"]:
                values["service_type"] = ", ".join(sorted(aggregate["services"]))
            if values:
                await session.execute(sa.update(orders).where(orders.c.id == order_id).values(**values))
                metrics.updated += 1

        await session.commit()
        metrics.reason_codes = dict(reasons)
        return metrics


async def publish_uc_archive_payments_to_sales(
    *,
    database_url: str,
    store_code: str | None = None,
    run_id: str | None = None,
) -> PublishMetrics:
    metrics = PublishMetrics()
    reasons = Counter[str]()
    metadata = sa.MetaData()
    payments = sa.Table(
        TABLE_ARCHIVE_PAYMENT_DETAILS,
        metadata,
        sa.Column("run_id", sa.Text),
        sa.Column("run_date", sa.DateTime(timezone=True)),
        sa.Column("store_code", sa.String(8)),
        sa.Column("order_code", sa.String(24)),
        sa.Column("payment_mode", sa.String(32)),
        sa.Column("amount", sa.Numeric(12, 2)),
        sa.Column("payment_date_raw", sa.Text),
        sa.Column("transaction_id", sa.String(128)),
        sa.Column("ingest_remarks", sa.Text),
    )
    archive_base = sa.Table(
        TABLE_ARCHIVE_BASE,
        metadata,
        sa.Column("store_code", sa.String(8)),
        sa.Column("order_code", sa.String(24)),
        sa.Column("cost_center", sa.String(8)),
        sa.Column("customer_name", sa.String(128)),
        sa.Column("customer_phone", sa.String(24)),
        sa.Column("ingest_remarks", sa.Text),
    )
    stg_orders = _stg_uc_orders_table(metadata)
    orders = _orders_table(metadata)
    sales = _sales_table(metadata)

    async with session_scope(database_url) as session:
        normalized_store_scope = _normalize_store_code(store_code)
        payment_filters: list[Any] = []
        if normalized_store_scope:
            payment_filters.append(payments.c.store_code == normalized_store_scope)
        if run_id:
            payment_filters.append(payments.c.run_id == run_id)

        payments_query = sa.select(
            payments.c.run_id,
            payments.c.run_date,
            payments.c.store_code,
            payments.c.order_code,
            payments.c.payment_mode,
            payments.c.amount,
            payments.c.payment_date_raw,
            payments.c.transaction_id,
            payments.c.ingest_remarks,
        )
        if payment_filters:
            payments_query = payments_query.where(sa.and_(*payment_filters))

        payment_rows = (
            await session.execute(
                payments_query
            )
        ).all()

        if not payment_rows:
            metrics.reason_codes = {}
            return metrics

        order_variants_by_archive_key: dict[tuple[str, str], list[tuple[str, str]]] = {}
        for row in payment_rows:
            join_variants = _build_join_key_variants(row.store_code, row.order_code)
            if join_variants:
                order_variants_by_archive_key[join_variants[0]] = join_variants
        order_keys = set(order_variants_by_archive_key.keys())
        query_keys = {variant for variants in order_variants_by_archive_key.values() for variant in variants}
        order_lookup: dict[tuple[str, str], Any] = {}
        stg_parent_keys: set[tuple[str, str]] = set()
        archive_base_lookup: dict[tuple[str, str], Any] = {}
        if query_keys:
            order_rows = await session.execute(
                sa.select(
                    orders.c.cost_center,
                    orders.c.store_code,
                    orders.c.order_number,
                    orders.c.order_date,
                    orders.c.customer_name,
                    orders.c.mobile_number,
                    orders.c.ingest_remarks,
                ).where(sa.tuple_(orders.c.store_code, orders.c.order_number).in_(list(query_keys)))
            )
            for row in order_rows:
                normalized_parent_key = _build_join_key_variants(row.store_code, row.order_number)
                if not normalized_parent_key:
                    continue
                order_lookup[normalized_parent_key[0]] = row

            stg_rows = await session.execute(
                sa.select(stg_orders.c.store_code, stg_orders.c.order_number).where(
                    sa.tuple_(stg_orders.c.store_code, stg_orders.c.order_number).in_(list(query_keys))
                )
            )
            stg_parent_keys = {
                normalized[0]
                for r in stg_rows
                for normalized in [_build_join_key_variants(r.store_code, r.order_number)]
                if normalized
            }

            archive_base_rows = await session.execute(
                sa.select(
                    archive_base.c.store_code,
                    archive_base.c.order_code,
                    archive_base.c.cost_center,
                    archive_base.c.customer_name,
                    archive_base.c.customer_phone,
                    archive_base.c.ingest_remarks,
                ).where(sa.tuple_(archive_base.c.store_code, archive_base.c.order_code).in_(list(query_keys)))
            )
            for row in archive_base_rows:
                normalized_parent_key = _build_join_key_variants(row.store_code, row.order_code)
                if not normalized_parent_key:
                    continue
                archive_base_lookup[normalized_parent_key[0]] = row

        matched_keys = {
            archive_key
            for archive_key, variants in order_variants_by_archive_key.items()
            if any(variant in order_lookup or variant in stg_parent_keys for variant in variants)
        }
        parent_match_count = len(matched_keys)
        missing_parent_count = len(order_keys) - parent_match_count
        metrics.publish_parent_match_rate = (parent_match_count / len(order_keys)) if order_keys else None
        metrics.missing_parent_count = missing_parent_count

        missing_keys = sorted(order_keys - matched_keys)
        sample_missing_keys = [f"{store_code}:{order_code}" for store_code, order_code in missing_keys[:5]]
        sample_unmatched_parent_keys = [
            f"{store_code}:{order_code}" for store_code, order_code in sorted(set(order_lookup.keys()) - order_keys)[:5]
        ]
        coverage = Decimal(str(metrics.publish_parent_match_rate or 0)) if order_keys else Decimal("1")
        metrics.preflight_diagnostics = {
            "total_archive_payment_keys": len(order_keys),
            "matched_parent_keys": parent_match_count,
            "missing_parent_keys": missing_parent_count,
            "coverage": float(coverage),
            "sample_missing_keys": sample_missing_keys,
            "sample_unmatched_parent_keys": sample_unmatched_parent_keys,
        }
        LOGGER.info(
            "archive_publish_payment_key_coverage %s",
            json.dumps(
                {
                    "total_candidate_keys": len(order_keys),
                    "matched_keys": parent_match_count,
                    "sample_unmatched_archive_keys": sample_missing_keys,
                    "sample_unmatched_parent_keys": sample_unmatched_parent_keys,
                },
                sort_keys=True,
            ),
        )

        if order_keys and parent_match_count == 0:
            metrics.skipped += len(payment_rows)
            metrics.warnings += 1
            _append_reason(reasons, REASON_PREFLIGHT_PARENT_COVERAGE_NEAR_ZERO)
            metrics.preflight_warning = (
                "Skipping archive payment publish: parent order join coverage is near-zero (0%). "
                + (f"Sample missing keys: {', '.join(sample_missing_keys)}" if sample_missing_keys else "No sample keys available.")
            )
            LOGGER.warning(
                "archive_publish_parent_preflight_near_zero %s",
                json.dumps(metrics.preflight_diagnostics, sort_keys=True),
            )
            metrics.reason_codes = dict(reasons)
            return metrics

        if order_keys and coverage < PREFLIGHT_PARENT_COVERAGE_MIN:
            _append_reason(reasons, REASON_PREFLIGHT_PARENT_COVERAGE_LOW)
            metrics.warnings += 1
            metrics.preflight_warning = (
                "Archive payment publish has low parent order join coverage "
                f"({float(coverage) * 100:.1f}%). "
                + (f"Sample missing keys: {', '.join(sample_missing_keys)}" if sample_missing_keys else "No sample keys available.")
            )
            LOGGER.warning(
                "archive_publish_parent_preflight_low %s",
                json.dumps(metrics.preflight_diagnostics, sort_keys=True),
            )

        processed_keys: set[tuple[Any, ...]] = set()
        for row in payment_rows:
            join_variants = _build_join_key_variants(row.store_code, row.order_code)
            matched_join_key: tuple[str, str] | None = None
            for variant in join_variants:
                if variant in order_lookup or variant in stg_parent_keys or variant in archive_base_lookup:
                    matched_join_key = variant
                    break
            selected_join_key = matched_join_key or (join_variants[0] if join_variants else None)
            store_code = selected_join_key[0] if selected_join_key else None
            order_number = selected_join_key[1] if selected_join_key else None
            payment_mode = (row.payment_mode or "").strip()
            amount = row.amount
            txid = (row.transaction_id or "").strip()
            normalized_txid = txid or None
            if not store_code or not order_number or not payment_mode or amount is None:
                metrics.skipped += 1
                metrics.warnings += 1
                _append_reason(reasons, REASON_MISSING_REQUIRED_IDENTIFIERS)
                continue
            payment_date = _parse_payment_datetime(row.payment_date_raw)
            if payment_date is None:
                metrics.skipped += 1
                metrics.warnings += 1
                _append_reason(reasons, REASON_UNPARSEABLE_PAYMENT_DATE)
                continue

            parent = None
            base_parent = None
            has_stg_parent = False
            for variant in join_variants:
                if parent is None:
                    parent = order_lookup.get(variant)
                if base_parent is None:
                    base_parent = archive_base_lookup.get(variant)
                has_stg_parent = has_stg_parent or (variant in stg_parent_keys)
            parent_cost_center = parent.cost_center if parent is not None else None
            if not parent_cost_center and base_parent is not None and (has_stg_parent or parent is not None):
                parent_cost_center = (base_parent.cost_center or "").strip() or None

            if parent is None and (base_parent is None or not has_stg_parent):
                metrics.skipped += 1
                metrics.warnings += 1
                _append_reason(reasons, REASON_MISSING_PARENT_ORDER_CONTEXT)
                continue

            if not parent_cost_center:
                metrics.skipped += 1
                metrics.warnings += 1
                _append_reason(reasons, REASON_MISSING_PARENT_ORDER_CONTEXT)
                continue

            logical_key = (parent_cost_center, order_number, payment_date, payment_mode, Decimal(str(amount)), txid)
            if logical_key in processed_keys:
                continue
            processed_keys.add(logical_key)

            sales_payload = {
                "run_id": row.run_id,
                "run_date": row.run_date,
                "cost_center": parent_cost_center,
                "store_code": store_code,
                "order_date": parent.order_date if parent is not None else row.run_date,
                "payment_date": payment_date,
                "order_number": order_number,
                "customer_code": None,
                "customer_name": (parent.customer_name if parent is not None else None) or (base_parent.customer_name if base_parent is not None else None),
                "customer_address": None,
                "mobile_number": (parent.mobile_number if parent is not None else None) or (base_parent.customer_phone if base_parent is not None else None),
                "payment_received": amount,
                "adjustments": Decimal("0"),
                "balance": Decimal("0"),
                "accepted_by": None,
                "payment_mode": payment_mode,
                "transaction_id": normalized_txid,
                "payment_made_at": None,
                "order_type": "UClean",
                "is_duplicate": False,
                "is_edited_order": False,
                "ingest_remarks": _merge_remarks(
                    parent.ingest_remarks if parent is not None else None,
                    base_parent.ingest_remarks if base_parent is not None else None,
                    row.ingest_remarks,
                ),
            }

            existing = (
                await session.execute(
                    sa.select(sales.c.id).where(
                        sa.and_(
                            sales.c.cost_center == parent_cost_center,
                            sales.c.order_number == order_number,
                            sales.c.payment_date == payment_date,
                            sales.c.payment_mode == payment_mode,
                            sales.c.payment_received == amount,
                            sa.func.coalesce(sales.c.transaction_id, "") == txid,
                        )
                    )
                )
            ).first()

            if existing:
                await session.execute(sa.update(sales).where(sales.c.id == existing.id).values(**sales_payload))
                metrics.updated += 1
                continue

            by_physical = (
                await session.execute(
                    sa.select(sales.c.id).where(
                        sa.and_(
                            sales.c.cost_center == parent_cost_center,
                            sales.c.order_number == order_number,
                            sales.c.payment_date == payment_date,
                        )
                    )
                )
            ).first()
            if by_physical:
                await session.execute(sa.update(sales).where(sales.c.id == by_physical.id).values(**sales_payload))
                metrics.updated += 1
            else:
                await session.execute(sa.insert(sales).values(**sales_payload))
                metrics.inserted += 1

        await session.commit()

    metrics.reason_codes = dict(reasons)
    return metrics


async def publish_uc_archive_stage2_stage3(
    *, database_url: str, store_code: str | None = None, run_id: str | None = None
) -> ArchivePublishResult:
    orders_metrics = await publish_uc_archive_order_details_to_orders(
        database_url=database_url,
        store_code=store_code,
        run_id=run_id,
    )
    sales_metrics = await publish_uc_archive_payments_to_sales(
        database_url=database_url,
        store_code=store_code,
        run_id=run_id,
    )
    return ArchivePublishResult(orders=orders_metrics, sales=sales_metrics)
