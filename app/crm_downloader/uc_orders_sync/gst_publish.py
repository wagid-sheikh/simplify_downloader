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
from app.crm_downloader.td_orders_sync.garment_ingest import order_line_items_table
from app.crm_downloader.td_orders_sync.sales_ingest import _sales_table
from app.crm_downloader.uc_orders_sync.archive_ingest import (
    TABLE_ARCHIVE_BASE,
    TABLE_ARCHIVE_ORDER_DETAILS,
    TABLE_ARCHIVE_PAYMENT_DETAILS,
)
from app.crm_downloader.uc_orders_sync.ingest import _stg_uc_orders_table
from app.crm_downloader.uc_orders_sync.ingest import _orders_table

REASON_GST_LIFECYCLE_PARENT_ORDER_CONTEXT_MISSING = "gst_lifecycle_parent_order_context_missing"
REASON_GST_LIFECYCLE_PARENT_INGEST_FAILURE = "gst_lifecycle_parent_ingest_failure"
REASON_GST_LIFECYCLE_PARENT_SOURCE_ABSENT = "gst_lifecycle_parent_source_absent"
REASON_GST_LIFECYCLE_UNPARSEABLE_PAYMENT_DATE = "gst_lifecycle_unparseable_payment_date"
REASON_GST_LIFECYCLE_MISSING_REQUIRED_IDENTIFIERS = "gst_lifecycle_missing_required_identifiers"
REASON_GST_LIFECYCLE_PARENT_COVERAGE_NEAR_ZERO = "gst_lifecycle_parent_coverage_near_zero"
REASON_GST_LIFECYCLE_PARENT_COVERAGE_LOW = "gst_lifecycle_parent_coverage_low"

PREFLIGHT_PARENT_COVERAGE_MIN = Decimal("0.80")
HISTORICAL_PARENT_LOOKUP_ROW_LIMIT = 5000

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
    post_publish_verification: dict[str, int] | None = None
    line_item_serial_validation: dict[str, Any] | None = None


def _build_line_item_serial_validation_query() -> sa.TextClause:
    return sa.text(
        """
        WITH scoped AS (
            SELECT order_number, order_id
            FROM order_line_items
            WHERE run_id = :run_id
              AND store_code = :store_code
        ),
        per_order AS (
            SELECT
                order_number,
                MIN(order_id) AS min_order_id,
                MAX(order_id) AS max_order_id,
                COUNT(*) AS line_count
            FROM scoped
            GROUP BY order_number
        ),
        duplicates AS (
            SELECT order_number, order_id, COUNT(*) AS duplicate_count
            FROM scoped
            GROUP BY order_number, order_id
            HAVING COUNT(*) > 1
        )
        SELECT
            (SELECT COUNT(*) FROM scoped) AS scoped_rows,
            COALESCE(SUM(CASE WHEN min_order_id <> 1 THEN 1 ELSE 0 END), 0) AS min_order_id_not_1_orders,
            (SELECT COUNT(DISTINCT order_number) FROM duplicates) AS duplicate_order_number_order_id_orders,
            COALESCE(SUM(CASE WHEN line_count = 1 AND min_order_id <> 1 THEN 1 ELSE 0 END), 0) AS single_line_order_id_not_1_orders
        FROM per_order
        """
    )


def _build_line_item_serial_validation_sample_queries(
    sample_limit: int,
) -> dict[str, sa.TextClause]:
    return {
        "min_order_id_not_1": sa.text(
            """
            SELECT order_number, MIN(order_id) AS min_order_id, COUNT(*) AS line_count
            FROM order_line_items
            WHERE run_id = :run_id
              AND store_code = :store_code
            GROUP BY order_number
            HAVING MIN(order_id) <> 1
            ORDER BY min_order_id ASC, line_count DESC, order_number ASC
            LIMIT :sample_limit
            """
        ),
        "duplicate_order_number_order_id": sa.text(
            """
            SELECT order_number, order_id, COUNT(*) AS duplicate_count
            FROM order_line_items
            WHERE run_id = :run_id
              AND store_code = :store_code
            GROUP BY order_number, order_id
            HAVING COUNT(*) > 1
            ORDER BY duplicate_count DESC, order_number ASC, order_id ASC
            LIMIT :sample_limit
            """
        ),
        "max_order_id_outliers": sa.text(
            """
            SELECT order_number, MAX(order_id) AS max_order_id, MIN(order_id) AS min_order_id, COUNT(*) AS line_count
            FROM order_line_items
            WHERE run_id = :run_id
              AND store_code = :store_code
            GROUP BY order_number
            ORDER BY max_order_id DESC, line_count DESC, order_number ASC
            LIMIT :sample_limit
            """
        ),
    }


async def _collect_line_item_serial_validation(
    *,
    session: Any,
    run_id: str,
    store_code: str,
    sample_limit: int = 5,
) -> dict[str, Any]:
    scoped_params = {"run_id": run_id, "store_code": store_code}
    summary = (
        await session.execute(_build_line_item_serial_validation_query(), scoped_params)
    ).mappings().one()

    sample_queries = _build_line_item_serial_validation_sample_queries(sample_limit)
    sample_params = {**scoped_params, "sample_limit": sample_limit}
    samples = {
        key: [dict(row) for row in (await session.execute(query, sample_params)).mappings().all()]
        for key, query in sample_queries.items()
    }

    return {
        "scoped_rows": int(summary["scoped_rows"] or 0),
        "orders_with_min_order_id_not_1": int(summary["min_order_id_not_1_orders"] or 0),
        "orders_with_duplicate_order_number_order_id": int(
            summary["duplicate_order_number_order_id_orders"] or 0
        ),
        "single_line_orders_with_order_id_not_1": int(
            summary["single_line_order_id_not_1_orders"] or 0
        ),
        "sample_limit": sample_limit,
        "samples": samples,
    }


def _build_payment_post_publish_verification_query() -> sa.TextClause:
    return sa.text(
        """
        WITH scoped_sales AS (
            SELECT s.id, s.order_number, s.customer_address, s.order_type
            FROM sales s
            WHERE s.run_id = :run_id
              AND s.store_code = :store_code
        ),
        sales_with_sources AS (
            SELECT
                s.id,
                s.order_type,
                NULLIF(TRIM(s.customer_address), '') AS sales_customer_address,
                (
                    SELECT NULLIF(TRIM(o.customer_address), '')
                    FROM orders o
                    WHERE o.store_code = :store_code
                      AND o.order_number = s.order_number
                    ORDER BY o.id DESC
                    LIMIT 1
                ) AS primary_customer_address,
                (
                    SELECT NULLIF(TRIM(ab.address), '')
                    FROM stg_uc_archive_orders_base ab
                    WHERE ab.run_id = :run_id
                      AND ab.store_code = :store_code
                      AND ab.order_code = s.order_number
                    ORDER BY ab.run_date DESC, ab.id DESC
                    LIMIT 1
                ) AS fallback_customer_address
            FROM scoped_sales s
        )
        SELECT
            COUNT(*) AS touched_rows,
            SUM(CASE WHEN order_type IS NULL THEN 1 ELSE 0 END) AS order_type_null_rows,
            SUM(
                CASE
                    WHEN sales_customer_address IS NOT NULL
                     AND primary_customer_address IS NOT NULL
                     AND sales_customer_address = primary_customer_address
                    THEN 1
                    ELSE 0
                END
            ) AS customer_address_primary_rows,
            SUM(
                CASE
                    WHEN sales_customer_address IS NOT NULL
                     AND fallback_customer_address IS NOT NULL
                     AND sales_customer_address = fallback_customer_address
                     AND (
                        primary_customer_address IS NULL
                        OR sales_customer_address <> primary_customer_address
                     )
                    THEN 1
                    ELSE 0
                END
            ) AS customer_address_fallback_rows,
            SUM(CASE WHEN sales_customer_address IS NULL THEN 1 ELSE 0 END) AS customer_address_null_or_blank_rows
        FROM sales_with_sources
        """
    )


@dataclass
class GstPublishResult:
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


def _build_payment_preflight_diagnostics(
    *,
    archive_payment_keys: set[tuple[str, str]],
    matched_archive_payment_keys: set[tuple[str, str]],
    parent_lookup_keys: set[tuple[str, str]],
) -> dict[str, Any]:
    """Build diagnostics using a single archive-payment keyspace for match coverage.

    Keyspace contract:
    - total_archive_payment_keys / matched_parent_keys / missing_parent_keys / sample_missing_keys
      are all derived from ``archive_payment_keys``.
    - ``sample_parent_lookup_keys_not_in_archive_payment_set`` is intentionally *not* part of
      missing coverage; it samples the opposite set difference for observability.
    """
    total_archive_payment_keys = len(archive_payment_keys)
    unexpected_matched_keys = matched_archive_payment_keys - archive_payment_keys
    if unexpected_matched_keys:
        raise ValueError(
            "Incompatible diagnostics: matched_archive_payment_keys must be a subset of archive_payment_keys"
        )

    matched_parent_keys = len(matched_archive_payment_keys)
    missing_archive_payment_keys = sorted(archive_payment_keys - matched_archive_payment_keys)
    missing_parent_keys = len(missing_archive_payment_keys)

    sample_missing_keys = [f"{store_code}:{order_code}" for store_code, order_code in missing_archive_payment_keys[:5]]
    parent_keys_not_in_archive_payment_set = sorted(parent_lookup_keys - archive_payment_keys)
    sample_parent_lookup_keys_not_in_archive_payment_set = [
        f"{store_code}:{order_code}" for store_code, order_code in parent_keys_not_in_archive_payment_set[:5]
    ]

    if missing_parent_keys == 0 and sample_missing_keys:
        raise ValueError("Incompatible diagnostics: missing_parent_keys is zero but sample_missing_keys is non-empty")
    if matched_parent_keys + missing_parent_keys != total_archive_payment_keys:
        raise ValueError(
            "Incompatible diagnostics: matched_parent_keys + missing_parent_keys must equal total_archive_payment_keys"
        )

    coverage = (matched_parent_keys / total_archive_payment_keys) if total_archive_payment_keys else 1.0
    return {
        "total_archive_payment_keys": total_archive_payment_keys,
        "matched_parent_keys": matched_parent_keys,
        "missing_parent_keys": missing_parent_keys,
        "coverage": coverage,
        "sample_missing_keys": sample_missing_keys,
        "sample_parent_lookup_keys_not_in_archive_payment_set": sample_parent_lookup_keys_not_in_archive_payment_set,
        "parent_lookup_keys_not_in_archive_payment_set_count": len(parent_keys_not_in_archive_payment_set),
        "sample_parent_lookup_keys_not_in_archive_payment_set_derivation": (
            "sorted(parent_lookup_keys - archive_payment_keys)[:5]"
        ),
    }


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


def _non_blank_text(value: Any) -> str | None:
    if value is None:
        return None
    token = str(value).strip()
    return token or None


async def publish_uc_gst_order_details_to_orders(
    *, database_url: str, run_id: str, store_code: str
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
    archive_base = sa.Table(
        TABLE_ARCHIVE_BASE,
        metadata,
        sa.Column("run_id", sa.Text),
        sa.Column("store_code", sa.String(8)),
        sa.Column("order_code", sa.String(24)),
        sa.Column("run_date", sa.DateTime(timezone=True)),
        sa.Column("id", sa.Integer),
        sa.Column("customer_source", sa.String(64)),
        sa.Column("address", sa.Text),
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
                sa.select(
                    details.c.store_code,
                    details.c.order_code,
                    details.c.quantity,
                    details.c.weight,
                    details.c.service,
                ).where(
                    sa.and_(
                        details.c.run_id == run_id,
                        details.c.store_code == store_code,
                    )
                )
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
        archive_customer_lookup: dict[tuple[str, str], dict[str, str | None]] = {}
        order_rows = (
            await session.execute(
                sa.select(orders.c.id, orders.c.store_code, orders.c.order_number).where(
                    sa.and_(
                        orders.c.store_code == store_code,
                        sa.tuple_(orders.c.store_code, orders.c.order_number).in_(list(candidate_keys)),
                    )
                )
            )
        ).all()
        for row in order_rows:
            normalized_parent_key = _build_join_key_variants(row.store_code, row.order_number)
            if not normalized_parent_key:
                continue
            existing_orders[normalized_parent_key[0]] = row.id

        if candidate_keys:
            archive_rows = (
                await session.execute(
                    sa.select(
                        archive_base.c.store_code,
                        archive_base.c.order_code,
                        archive_base.c.run_date,
                        archive_base.c.id,
                        archive_base.c.customer_source,
                        archive_base.c.address,
                    )
                    .where(
                        sa.and_(
                            archive_base.c.run_id == run_id,
                            archive_base.c.store_code == store_code,
                            sa.tuple_(archive_base.c.store_code, archive_base.c.order_code).in_(list(candidate_keys)),
                        )
                    )
                    .order_by(sa.desc(archive_base.c.run_date), sa.desc(archive_base.c.id))
                )
            ).all()
            for row in archive_rows:
                normalized_parent_key = _build_join_key_variants(row.store_code, row.order_code)
                if not normalized_parent_key:
                    continue
                if normalized_parent_key[0] in archive_customer_lookup:
                    continue
                archive_customer_lookup[normalized_parent_key[0]] = {
                    "customer_source": _non_blank_text(row.customer_source),
                    "customer_address": _non_blank_text(row.address),
                }

        matched_keys = {
            grouped_key
            for grouped_key, variants in key_variants_by_grouped_key.items()
            if any(variant in existing_orders for variant in variants)
        }
        missing_archive_keys = sorted(set(grouped.keys()) - matched_keys)
        parent_keys = set(existing_orders.keys())
        unmatched_parent_keys = sorted(parent_keys - set(grouped.keys()))
        LOGGER.info(
            "gst_publish_order_details_key_coverage %s",
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
                _append_reason(reasons, REASON_GST_LIFECYCLE_PARENT_ORDER_CONTEXT_MISSING)
                continue
            values: dict[str, Any] = {}
            customer_values: dict[str, str | None] | None = None
            for variant in key_variants_by_grouped_key.get(grouped_key, [grouped_key]):
                customer_values = archive_customer_lookup.get(variant)
                if customer_values is not None:
                    break
            if aggregate["pieces"] > 0:
                values["pieces"] = aggregate["pieces"]
            if aggregate["weight"] > 0:
                values["weight"] = aggregate["weight"]
            if aggregate["services"]:
                values["service_type"] = ", ".join(sorted(aggregate["services"]))
            if customer_values:
                if customer_values.get("customer_source"):
                    values["customer_source"] = customer_values["customer_source"]
                if customer_values.get("customer_address"):
                    values["customer_address"] = customer_values["customer_address"]
            if values:
                await session.execute(sa.update(orders).where(orders.c.id == order_id).values(**values))
                metrics.updated += 1

        await session.commit()
        metrics.reason_codes = dict(reasons)
        return metrics


async def publish_uc_gst_payments_to_sales(
    *, database_url: str, run_id: str, store_code: str
) -> PublishMetrics:
    metrics = PublishMetrics()
    reasons = Counter[str]()
    unmatched_parent_reasons = Counter[str]()
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
        sa.Column("run_id", sa.Text),
        sa.Column("store_code", sa.String(8)),
        sa.Column("order_code", sa.String(24)),
        sa.Column("cost_center", sa.String(8)),
        sa.Column("customer_name", sa.String(128)),
        sa.Column("customer_phone", sa.String(24)),
        sa.Column("address", sa.Text),
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
                sa.select(
                    payments.c.run_id,
                    payments.c.run_date,
                    payments.c.store_code,
                    payments.c.order_code,
                    payments.c.payment_mode,
                    payments.c.amount,
                    payments.c.payment_date_raw,
                    payments.c.transaction_id,
                    payments.c.ingest_remarks,
                ).where(
                    sa.and_(
                        payments.c.run_id == run_id,
                        payments.c.store_code == store_code,
                    )
                )
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
                    orders.c.customer_address,
                    orders.c.ingest_remarks,
                ).where(
                    sa.and_(
                        orders.c.store_code == store_code,
                        sa.tuple_(orders.c.store_code, orders.c.order_number).in_(list(query_keys)),
                    )
                )
            )
            for row in order_rows:
                normalized_parent_key = _build_join_key_variants(row.store_code, row.order_number)
                if not normalized_parent_key:
                    continue
                order_lookup[normalized_parent_key[0]] = row

            unresolved_query_keys = [key for key in query_keys if key not in order_lookup]
            if unresolved_query_keys:
                historical_order_rows = await session.execute(
                    sa.select(
                        orders.c.cost_center,
                        orders.c.store_code,
                        orders.c.order_number,
                        orders.c.order_date,
                        orders.c.customer_name,
                        orders.c.mobile_number,
                        orders.c.customer_address,
                        orders.c.ingest_remarks,
                    )
                    .where(
                        sa.and_(
                            orders.c.store_code == store_code,
                        )
                    )
                    .order_by(sa.desc(orders.c.created_at), sa.desc(orders.c.id))
                    .limit(HISTORICAL_PARENT_LOOKUP_ROW_LIMIT)
                )
                unresolved_set = set(unresolved_query_keys)
                for row in historical_order_rows:
                    variants = _build_join_key_variants(row.store_code, row.order_number)
                    if not variants:
                        continue
                    for variant in variants:
                        if variant in unresolved_set and variant not in order_lookup:
                            order_lookup[variant] = row

            stg_rows = await session.execute(
                sa.select(stg_orders.c.store_code, stg_orders.c.order_number).where(
                    sa.and_(
                        stg_orders.c.run_id == run_id,
                        stg_orders.c.store_code == store_code,
                        sa.tuple_(stg_orders.c.store_code, stg_orders.c.order_number).in_(list(query_keys)),
                    )
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
                    archive_base.c.address,
                    archive_base.c.ingest_remarks,
                ).where(
                    sa.and_(
                        archive_base.c.run_id == run_id,
                        archive_base.c.store_code == store_code,
                        sa.tuple_(archive_base.c.store_code, archive_base.c.order_code).in_(list(query_keys)),
                    )
                )
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

        metrics.preflight_diagnostics = _build_payment_preflight_diagnostics(
            archive_payment_keys=order_keys,
            matched_archive_payment_keys=matched_keys,
            parent_lookup_keys=set(order_lookup.keys()),
        )
        sample_missing_keys = metrics.preflight_diagnostics["sample_missing_keys"]
        coverage = Decimal(str(metrics.preflight_diagnostics["coverage"])) if order_keys else Decimal("1")
        preflight_log_payload: dict[str, Any] = {
            "total_archive_payment_keys": metrics.preflight_diagnostics["total_archive_payment_keys"],
            "matched_parent_keys": metrics.preflight_diagnostics["matched_parent_keys"],
            "missing_parent_keys": metrics.preflight_diagnostics["missing_parent_keys"],
            "coverage": metrics.preflight_diagnostics["coverage"],
        }
        if coverage == Decimal("1") and missing_parent_count == 0:
            preflight_log_payload["preflight_status"] = "full_coverage"
        else:
            preflight_log_payload.update(
                {
                    "sample_unmatched_archive_keys": sample_missing_keys,
                    "sample_parent_lookup_keys_not_in_archive_payment_set": metrics.preflight_diagnostics[
                        "sample_parent_lookup_keys_not_in_archive_payment_set"
                    ],
                    "sample_parent_lookup_keys_not_in_archive_payment_set_derivation": metrics.preflight_diagnostics[
                        "sample_parent_lookup_keys_not_in_archive_payment_set_derivation"
                    ],
                }
            )
        LOGGER.info(
            "gst_publish_payment_key_coverage %s",
            json.dumps(preflight_log_payload, sort_keys=True),
        )

        if order_keys and parent_match_count == 0:
            metrics.skipped += len(payment_rows)
            metrics.warnings += 1
            _append_reason(reasons, REASON_GST_LIFECYCLE_PARENT_COVERAGE_NEAR_ZERO)
            metrics.preflight_warning = (
                "Skipping GST payment publish: parent order join coverage is near-zero (0%). "
                + (f"Sample missing keys: {', '.join(sample_missing_keys)}" if sample_missing_keys else "No sample keys available.")
            )
            LOGGER.warning(
                "gst_publish_parent_preflight_near_zero %s",
                json.dumps(metrics.preflight_diagnostics, sort_keys=True),
            )
            metrics.reason_codes = dict(reasons)
            return metrics

        if order_keys and coverage < PREFLIGHT_PARENT_COVERAGE_MIN:
            _append_reason(reasons, REASON_GST_LIFECYCLE_PARENT_COVERAGE_LOW)
            metrics.warnings += 1
            metrics.preflight_warning = (
                "GST payment publish has low parent order join coverage "
                f"({float(coverage) * 100:.1f}%). "
                + (f"Sample missing keys: {', '.join(sample_missing_keys)}" if sample_missing_keys else "No sample keys available.")
            )
            LOGGER.warning(
                "gst_publish_parent_preflight_low %s",
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
                _append_reason(reasons, REASON_GST_LIFECYCLE_MISSING_REQUIRED_IDENTIFIERS)
                continue
            payment_date = _parse_payment_datetime(row.payment_date_raw)
            if payment_date is None:
                metrics.skipped += 1
                metrics.warnings += 1
                _append_reason(reasons, REASON_GST_LIFECYCLE_UNPARSEABLE_PAYMENT_DATE)
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

            archive_ingest_remark = (base_parent.ingest_remarks or "") if base_parent is not None else ""
            archive_parent_ingest_failure = bool(archive_ingest_remark.strip())

            if parent is None and (base_parent is None or not has_stg_parent):
                metrics.skipped += 1
                metrics.warnings += 1
                unmatched_reason = (
                    REASON_GST_LIFECYCLE_PARENT_INGEST_FAILURE
                    if archive_parent_ingest_failure
                    else REASON_GST_LIFECYCLE_PARENT_SOURCE_ABSENT
                )
                _append_reason(reasons, unmatched_reason)
                _append_reason(unmatched_parent_reasons, unmatched_reason)
                continue

            if not parent_cost_center:
                metrics.skipped += 1
                metrics.warnings += 1
                unmatched_reason = (
                    REASON_GST_LIFECYCLE_PARENT_INGEST_FAILURE
                    if archive_parent_ingest_failure
                    else REASON_GST_LIFECYCLE_PARENT_ORDER_CONTEXT_MISSING
                )
                _append_reason(reasons, unmatched_reason)
                _append_reason(unmatched_parent_reasons, unmatched_reason)
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
                "customer_address": _non_blank_text(parent.customer_address if parent is not None else None)
                or _non_blank_text(base_parent.address if base_parent is not None else None),
                "mobile_number": (parent.mobile_number if parent is not None else None) or (base_parent.customer_phone if base_parent is not None else None),
                "payment_received": amount,
                "adjustments": Decimal("0"),
                "balance": Decimal("0"),
                "accepted_by": None,
                "payment_mode": payment_mode,
                "transaction_id": normalized_txid,
                "payment_made_at": None,
                "order_type": None,
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
                            sales.c.store_code == store_code,
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
                            sales.c.store_code == store_code,
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

        if unmatched_parent_reasons:
            LOGGER.info(
                "gst_publish_payment_unmatched_parent_reasons %s",
                json.dumps(dict(unmatched_parent_reasons), sort_keys=True),
            )

        verification_row = (
            await session.execute(
                _build_payment_post_publish_verification_query(),
                {"run_id": run_id, "store_code": store_code},
            )
        ).mappings().one()
        metrics.post_publish_verification = {
            "touched_rows": int(verification_row["touched_rows"] or 0),
            "order_type_null_rows": int(verification_row["order_type_null_rows"] or 0),
            "customer_address_primary_rows": int(verification_row["customer_address_primary_rows"] or 0),
            "customer_address_fallback_rows": int(verification_row["customer_address_fallback_rows"] or 0),
            "customer_address_null_or_blank_rows": int(verification_row["customer_address_null_or_blank_rows"] or 0),
        }

    metrics.reason_codes = dict(reasons)
    return metrics


def _normalize_line_item_key(*, line_hash: Any, item_name: Any, service: Any, rate: Any) -> str:
    line_hash_token = _non_blank_text(line_hash)
    if line_hash_token:
        return line_hash_token
    item_token = _non_blank_text(item_name) or "unknown"
    service_token = _non_blank_text(service) or "unknown"
    rate_token = "" if rate is None else str(rate)
    return f"{item_token}|{service_token}|{rate_token}"


async def publish_uc_gst_order_details_to_line_items(
    *, database_url: str, run_id: str, store_code: str
) -> PublishMetrics:
    metrics = PublishMetrics()
    metadata = sa.MetaData()
    details = sa.Table(
        TABLE_ARCHIVE_ORDER_DETAILS,
        metadata,
        sa.Column("id", sa.Integer),
        sa.Column("run_id", sa.Text),
        sa.Column("run_date", sa.DateTime(timezone=True)),
        sa.Column("cost_center", sa.String(8)),
        sa.Column("store_code", sa.String(8)),
        sa.Column("order_code", sa.String(24)),
        sa.Column("service", sa.Text),
        sa.Column("item_name", sa.Text),
        sa.Column("rate", sa.Numeric(12, 2)),
        sa.Column("quantity", sa.Numeric(12, 2)),
        sa.Column("weight", sa.Numeric(12, 3)),
        sa.Column("amount", sa.Numeric(12, 2)),
        sa.Column("order_datetime_raw", sa.Text),
        sa.Column("line_hash", sa.String(64)),
        sa.Column("ingest_remarks", sa.Text),
        extend_existing=True,
    )
    orders = _orders_table(metadata)
    line_items = order_line_items_table(metadata)

    async with session_scope(database_url) as session:
        detail_rows = (
            await session.execute(
                sa.select(
                    details.c.id,
                    details.c.run_id,
                    details.c.run_date,
                    details.c.cost_center,
                    details.c.store_code,
                    details.c.order_code,
                    details.c.service,
                    details.c.item_name,
                    details.c.rate,
                    details.c.quantity,
                    details.c.weight,
                    details.c.amount,
                    details.c.order_datetime_raw,
                    details.c.line_hash,
                    details.c.ingest_remarks,
                ).where(
                    sa.and_(
                        details.c.run_id == run_id,
                        details.c.store_code == store_code,
                    )
                )
            )
        ).all()
        if not detail_rows:
            return metrics

        normalized_orders = {
            normalized[0]: row
            for row in (
                await session.execute(
                    sa.select(
                        orders.c.id,
                        orders.c.cost_center,
                        orders.c.store_code,
                        orders.c.order_number,
                        orders.c.order_date,
                        orders.c.updated_at,
                        orders.c.order_status,
                    ).where(orders.c.store_code == store_code)
                )
            ).all()
            for normalized in [_build_join_key_variants(row.store_code, row.order_number)]
            if normalized
        }

        indexed_rows: list[dict[str, Any]] = []
        for source_idx, row in enumerate(detail_rows, start=1):
            join_variants = _build_join_key_variants(row.store_code, row.order_code)
            if not join_variants:
                metrics.skipped += 1
                metrics.warnings += 1
                continue
            indexed_rows.append(
                {
                    "source_idx": source_idx,
                    "group_key": join_variants[0],
                    "join_variants": join_variants,
                    "row": row,
                }
            )

        grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for record in indexed_rows:
            grouped[record["group_key"]].append(record)

        for group_key in sorted(grouped.keys()):
            group_rows = sorted(
                grouped[group_key],
                key=lambda record: (
                    _non_blank_text(record["row"].line_hash) or "",
                    _non_blank_text(record["row"].item_name) or "",
                    record["source_idx"],
                ),
            )
            for serial, record in enumerate(group_rows, start=1):
                row = record["row"]
                join_variants = record["join_variants"]
                parent_order = next((normalized_orders.get(variant) for variant in join_variants if variant in normalized_orders), None)
                selected_order_number = join_variants[0][1]
                selected_store_code = join_variants[0][0]
                selected_cost_center = _non_blank_text(row.cost_center)
                if parent_order is not None:
                    selected_cost_center = _non_blank_text(parent_order.cost_center) or selected_cost_center
                if not selected_cost_center:
                    metrics.skipped += 1
                    metrics.warnings += 1
                    continue

                line_item_key = _normalize_line_item_key(
                    line_hash=row.line_hash,
                    item_name=row.item_name,
                    service=row.service,
                    rate=row.rate,
                )
                line_item_uid = f"{selected_cost_center}|{selected_order_number}|{line_item_key}|{serial}"
                order_date = _parse_payment_datetime(row.order_datetime_raw)
                source_updated_at = row.run_date or datetime.now(tz=ZoneInfo(os.getenv("PIPELINE_TIMEZONE", "Asia/Kolkata")))
                status = _non_blank_text(parent_order.order_status) if parent_order is not None else None
                ingest_remarks = _merge_remarks(row.ingest_remarks, "parent_order_missing" if parent_order is None else None)

                payload = {
                    "run_id": row.run_id,
                    "run_date": row.run_date,
                    "cost_center": selected_cost_center,
                    "store_code": selected_store_code,
                    "order_id": serial,
                    "order_number": selected_order_number,
                    "line_item_key": line_item_key,
                    "line_item_uid": line_item_uid,
                    "garment_name": _non_blank_text(row.item_name),
                    "service_name": _non_blank_text(row.service),
                    "quantity": row.quantity,
                    "weight": row.weight,
                    "amount": row.amount,
                    "order_date": order_date or (parent_order.order_date if parent_order is not None else None),
                    "updated_at": (parent_order.updated_at if parent_order is not None and parent_order.updated_at else source_updated_at),
                    "status": status,
                    "ingest_row_seq": record["source_idx"],
                    "is_orphan": parent_order is None,
                    "ingest_remarks": ingest_remarks,
                }

                existing = (
                    await session.execute(
                        sa.select(line_items.c.id).where(
                            sa.and_(
                                line_items.c.cost_center == selected_cost_center,
                                line_items.c.order_number == selected_order_number,
                                line_items.c.line_item_uid == line_item_uid,
                            )
                        )
                    )
                ).first()
                if existing:
                    await session.execute(sa.update(line_items).where(line_items.c.id == existing.id).values(**payload))
                    metrics.updated += 1
                else:
                    await session.execute(sa.insert(line_items).values(**payload))
                    metrics.inserted += 1

        metrics.line_item_serial_validation = await _collect_line_item_serial_validation(
            session=session,
            run_id=run_id,
            store_code=store_code,
        )
        LOGGER.info(
            "GST line-item serial validation completed",
            extra={
                "phase": "gst_publish_order_line_items_validation",
                "run_id": run_id,
                "store_code": store_code,
                "validation": metrics.line_item_serial_validation,
            },
        )

        await session.commit()
    return metrics


async def publish_uc_gst_stage2_stage3(
    *, database_url: str, run_id: str, store_code: str
) -> GstPublishResult:
    orders_metrics = await publish_uc_gst_order_details_to_orders(
        database_url=database_url,
        run_id=run_id,
        store_code=store_code,
    )
    await publish_uc_gst_order_details_to_line_items(
        database_url=database_url,
        run_id=run_id,
        store_code=store_code,
    )
    sales_metrics = await publish_uc_gst_payments_to_sales(
        database_url=database_url,
        run_id=run_id,
        store_code=store_code,
    )
    return GstPublishResult(orders=orders_metrics, sales=sales_metrics)
