from __future__ import annotations

import re
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from playwright.async_api import Page, Locator

from .json_logger import JsonLogger, log_event


_LABEL_NORMALIZER = re.compile(r"[^a-z0-9]+").sub


def _clean_number(text: str | None) -> str:
    if text is None:
        return ""

    cleaned = text.replace("₹", "").replace(",", "").replace("%", "")
    cleaned = re.sub(r"\s+", "", cleaned.strip())
    if cleaned.lower() in {"", "-", "--", "n/a", "na"}:
        return ""
    return cleaned


def _to_int(text: str | None) -> int | None:
    cleaned = _clean_number(text)
    if not cleaned:
        return None

    try:
        return int(cleaned)
    except ValueError:
        try:
            return int(float(cleaned))
        except (ValueError, TypeError):
            return None


def _to_decimal(text: str | None) -> Decimal | None:
    cleaned = _clean_number(text)
    if not cleaned:
        return None

    try:
        return Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None


def _parse_ddmmyyyy(text: str | None) -> date | None:
    if not text:
        return None

    cleaned = text.strip()
    if not cleaned or cleaned in {"-", "--"}:
        return None
    cleaned = cleaned.replace("/", "-").replace(".", "-")

    try:
        return datetime.strptime(cleaned, "%d-%m-%Y").date()
    except ValueError:
        return None


def _parse_dashboard_date(text: str | None) -> date | None:
    if not text:
        return None

    cleaned = text.strip()
    if not cleaned:
        return None

    cleaned = re.sub(r"(?i)dashboard\s*date[:\-]*", "", cleaned).strip()
    if not cleaned:
        return None

    for fmt in ("%d %b %Y", "%d %B %Y"):
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    return None


def _normalize_label(text: str | None) -> str:
    if not text:
        return ""
    return _LABEL_NORMALIZER("", text.lower())


def _normalize_space(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.strip().lower())


@dataclass
class SectionConfig:
    keywords: Tuple[str, ...]
    min_matches: int


SECTION_CONFIGS: Dict[str, SectionConfig] = {
    "Revenue": SectionConfig(
        keywords=("revenue", "prev", "previous", "target", "mtd", "ftd", "lmtd"),
        min_matches=3,
    ),
    "Pickup": SectionConfig(
        keywords=("pickup", "new", "existing", "total", "conversion"),
        min_matches=3,
    ),
    "Delivery": SectionConfig(
        keywords=("delivery", "total", "delivered", "undelivered", "tat"),
        min_matches=3,
    ),
    "Repeat Customers": SectionConfig(
        keywords=("repeat", "base", "orders", "customers"),
        min_matches=2,
    ),
    "Package": SectionConfig(
        keywords=("package", "target", "new", "ftd", "achievement", "overall"),
        min_matches=3,
    ),
}


REVENUE_HEADER_TO_METRIC: Dict[str, str] = {
    "prevmonth": "prev_month_revenue",
    "previousmonth": "prev_month_revenue",
    "prevmonthrevenue": "prev_month_revenue",
    "target": "target_revenue",
    "targetrevenue": "target_revenue",
    "lmtd": "lmt_d_revenue",
    "lastmonthtodate": "lmt_d_revenue",
    "lmtdd": "lmt_d_revenue",
    "mtd": "mtd_revenue",
    "monthtodate": "mtd_revenue",
    "ftd": "ftd_revenue",
    "firsttodate": "ftd_revenue",
    "tgtvsach": "tgt_vs_ach_pct",
    "targetvsach": "tgt_vs_ach_pct",
    "tgtvsachpct": "tgt_vs_ach_pct",
    "growth": "growth_pct",
    "growthpct": "growth_pct",
    "extrapolated": "extrapolated_pct",
    "extrapolatedpct": "extrapolated_pct",
}


REVENUE_LABEL_TO_METRIC: Dict[str, str] = {
    "prevmonthrevenue": "prev_month_revenue",
    "previousmonthrevenue": "prev_month_revenue",
    "targetrevenue": "target_revenue",
    "target": "target_revenue",
    "lmtdrevenue": "lmt_d_revenue",
    "lmtd": "lmt_d_revenue",
    "mtdrevenue": "mtd_revenue",
    "mtd": "mtd_revenue",
    "ftdrevenue": "ftd_revenue",
    "ftd": "ftd_revenue",
    "tgtvsach": "tgt_vs_ach_pct",
    "tgtvsachpct": "tgt_vs_ach_pct",
    "targetvsach": "tgt_vs_ach_pct",
    "growth": "growth_pct",
    "growthpct": "growth_pct",
    "extrapolated": "extrapolated_pct",
    "extrapolatedpct": "extrapolated_pct",
}


PICKUP_ROW_METRICS: Dict[str, Dict[str, str]] = {
    "new": {
        "count": "pickup_new_count",
        "conv_count": "pickup_new_conv_count",
        "conv_pct": "pickup_new_conv_pct",
    },
    "existing": {
        "count": "pickup_existing_count",
        "conv_count": "pickup_existing_conv_count",
        "conv_pct": "pickup_existing_conv_pct",
    },
    "total": {
        "count": "pickup_total_count",
        "conv_count": "pickup_total_conv_count",
        "conv_pct": "pickup_total_conv_pct",
    },
}


DELIVERY_LABEL_TO_METRIC: Dict[str, str] = {
    "totalorders": "delivery_total_orders",
    "deliverytotalorders": "delivery_total_orders",
    "withintat": "delivery_within_tat_count",
    "tat": "delivery_within_tat_count",
    "tatpct": "delivery_tat_pct",
    "tatpercentage": "delivery_tat_pct",
    "totaltatpercentage": "delivery_tat_pct",
    "totaldelivered": "delivery_total_delivered",
    "delivered": "delivery_total_delivered",
    "undeliveredover10days": "delivery_undel_over_10_days",
    "undeliveredgt10days": "delivery_undel_over_10_days",
    "undelivered10days": "delivery_undel_over_10_days",
    "totalundelivered": "delivery_total_undelivered",
}


REPEAT_LABEL_TO_METRIC: Dict[str, str] = {
    "repeatcustomerbase6months": "repeat_customer_base_6m",
    "repeatcustomerbase6m": "repeat_customer_base_6m",
    "repeatorders": "repeat_orders",
    "repeatordertotal": "repeat_orders",
    "repeatpercentageoftotalbase": "repeat_total_base_pct",
    "repeattotalbasepct": "repeat_total_base_pct",
}


PACKAGE_LABEL_TO_METRIC: Dict[str, str] = {
    "target": "package_target",
    "packagetarget": "package_target",
    "new": "package_new",
    "packagenew": "package_new",
    "ftd": "package_ftd",
    "packageftd": "package_ftd",
    "achievement": "package_achievement_pct",
    "achievementpct": "package_achievement_pct",
    "packageachievement": "package_achievement_pct",
    "overall": "package_overall",
    "packageoverall": "package_overall",
}


METRIC_PARSERS: Dict[str, Any] = {
    "prev_month_revenue": _to_decimal,
    "target_revenue": _to_decimal,
    "lmt_d_revenue": _to_decimal,
    "mtd_revenue": _to_decimal,
    "ftd_revenue": _to_decimal,
    "tgt_vs_ach_pct": _to_decimal,
    "growth_pct": _to_decimal,
    "extrapolated_pct": _to_decimal,
    "pickup_new_count": _to_int,
    "pickup_new_conv_count": _to_int,
    "pickup_new_conv_pct": _to_decimal,
    "pickup_existing_count": _to_int,
    "pickup_existing_conv_count": _to_int,
    "pickup_existing_conv_pct": _to_decimal,
    "pickup_total_count": _to_int,
    "pickup_total_conv_count": _to_int,
    "pickup_total_conv_pct": _to_decimal,
    "delivery_total_orders": _to_int,
    "delivery_within_tat_count": _to_int,
    "delivery_tat_pct": _to_decimal,
    "delivery_total_delivered": _to_int,
    "delivery_undel_over_10_days": _to_int,
    "delivery_total_undelivered": _to_int,
    "repeat_customer_base_6m": _to_int,
    "repeat_orders": _to_int,
    "repeat_total_base_pct": _to_decimal,
    "package_target": _to_int,
    "package_new": _to_int,
    "package_ftd": _to_int,
    "package_achievement_pct": _to_decimal,
    "package_overall": _to_int,
    "package_non_pkg_over_800": _to_int,
    "package_non_pkg_over_800_undelivered": _to_int,
}


METRIC_SECTIONS: Dict[str, str] = {
    "prev_month_revenue": "Revenue",
    "target_revenue": "Revenue",
    "lmt_d_revenue": "Revenue",
    "mtd_revenue": "Revenue",
    "ftd_revenue": "Revenue",
    "tgt_vs_ach_pct": "Revenue",
    "growth_pct": "Revenue",
    "extrapolated_pct": "Revenue",
    "pickup_new_count": "Pickup",
    "pickup_new_conv_count": "Pickup",
    "pickup_new_conv_pct": "Pickup",
    "pickup_existing_count": "Pickup",
    "pickup_existing_conv_count": "Pickup",
    "pickup_existing_conv_pct": "Pickup",
    "pickup_total_count": "Pickup",
    "pickup_total_conv_count": "Pickup",
    "pickup_total_conv_pct": "Pickup",
    "delivery_total_orders": "Delivery",
    "delivery_within_tat_count": "Delivery",
    "delivery_tat_pct": "Delivery",
    "delivery_total_delivered": "Delivery",
    "delivery_undel_over_10_days": "Delivery",
    "delivery_total_undelivered": "Delivery",
    "repeat_customer_base_6m": "Repeat Customers",
    "repeat_orders": "Repeat Customers",
    "repeat_total_base_pct": "Repeat Customers",
    "package_target": "Package",
    "package_new": "Package",
    "package_ftd": "Package",
    "package_achievement_pct": "Package",
    "package_overall": "Package",
    "package_non_pkg_over_800": "Package",
    "package_non_pkg_over_800_undelivered": "Package",
}


async def extract_dashboard_summary(
    page: Page,
    store_cfg: Dict[str, Any],
    *,
    logger: JsonLogger,
) -> Dict[str, Any]:
    from simplify_downloader.common.dashboard_store import DASHBOARD_SUMMARY_COLUMNS

    store_code = store_cfg.get("store_code")

    def _log(status: str, message: str, *, extras: Optional[Dict] = None) -> None:
        log_event(
            logger=logger,
            phase="download",
            status=status,
            store_code=store_code,
            bucket=None,
            message=message,
            extras=extras,
        )

    dashboard_data: Dict[str, Any] = {key: None for key in DASHBOARD_SUMMARY_COLUMNS}
    dashboard_data.update(
        {
            "store_code": store_code,
            "store_name": None,
            "dashboard_date": None,
            "launch_date": None,
        }
    )

    # Determine GSTIN column name if present
    gstin_column_name = None
    if "gstin" in dashboard_data:
        gstin_column_name = "gstin"
    elif "store_gstin" in dashboard_data:
        gstin_column_name = "store_gstin"

    found_metrics: set[str] = set()
    sections_with_data: set[str] = set()

    def _set_metric(key: str, raw_value: Optional[str]) -> None:
        parser = METRIC_PARSERS.get(key)
        parsed_value = parser(raw_value) if parser else raw_value
        dashboard_data[key] = parsed_value
        found_metrics.add(key)
        if (
            parser
            and parsed_value is None
            and raw_value is not None
            and _clean_number(raw_value)
        ):
            _log(
                "warn",
                "unable to parse dashboard metric",
                extras={"metric": key, "value": raw_value},
            )

    # Title / store name / dashboard date
    title_locator = page.locator("h1.dashboard-title")
    try:
        if await title_locator.count() == 0:
            _log(
                "warn",
                "dashboard title not found",
                extras={"selector": "h1.dashboard-title"},
            )
        else:
            title_text = await title_locator.first.inner_text()
            lines = [line.strip() for line in (title_text or "").splitlines() if line.strip()]

            if lines:
                if len(lines) >= 1:
                    small_locator = title_locator.first.locator("small")
                    date_text = None
                    if await small_locator.count() > 0:
                        date_text = await small_locator.first.inner_text()
                    if not date_text and len(lines) >= 1:
                        date_text = lines[-1]
                    parsed_dashboard_date = _parse_dashboard_date(date_text)
                    if parsed_dashboard_date:
                        dashboard_data["dashboard_date"] = parsed_dashboard_date
                    elif date_text:
                        _log(
                            "warn",
                            "unable to parse dashboard date",
                            extras={"value": date_text},
                        )

                name_lines = lines[1:-1] if len(lines) >= 3 else lines[1:] if len(lines) >= 2 else []
                name_lines = [ln for ln in name_lines if not store_code or store_code not in ln]
                store_name = " ".join(name_lines).strip()
                if store_name:
                    dashboard_data["store_name"] = store_name
                elif store_cfg.get("store_name"):
                    dashboard_data["store_name"] = store_cfg.get("store_name")
                else:
                    _log(
                        "warn",
                        "store name not found in dashboard title",
                        extras={"title_lines": lines},
                    )
            else:
                _log(
                    "warn",
                    "dashboard title empty",
                    extras={"selector": "h1.dashboard-title"},
                )
    except Exception as exc:  # pragma: no cover
        _log(
            "warn",
            "failed to extract dashboard title",
            extras={"error": str(exc)},
        )

    # Extract GSTIN from navbar user bit if possible
    user_bit_text: Optional[str] = None
    if gstin_column_name is not None:
        try:
            user_bit_locator = page.locator("li.user_bit a")
            if await user_bit_locator.count() > 0:
                user_bit_text = await user_bit_locator.first.inner_text()
        except Exception:
            user_bit_text = None

        if user_bit_text:
            m = re.search(r"\b[0-9A-Z]{15}\b", user_bit_text)
            if m:
                dashboard_data[gstin_column_name] = m.group(0)

        if dashboard_data.get(gstin_column_name) is None:
            fallback_gstin = store_cfg.get("gstin") or store_cfg.get("store_gstin")
            if fallback_gstin:
                dashboard_data[gstin_column_name] = fallback_gstin

    # Launch date
    launch_locator = page.locator("h3.section-title:has-text(\"Launch Date\")")
    try:
        if await launch_locator.count() > 0:
            launch_value_locator = launch_locator.first.locator("xpath=following-sibling::p[1]")
            launch_text = None
            if await launch_value_locator.count() == 0:
                launch_value_locator = launch_locator.first.locator("xpath=ancestor::*[1]/following-sibling::p[1]")
            if await launch_value_locator.count() > 0:
                launch_text = await launch_value_locator.first.inner_text()
            if launch_text:
                parsed_launch = _parse_ddmmyyyy(launch_text)
                if parsed_launch:
                    dashboard_data["launch_date"] = parsed_launch
                else:
                    _log(
                        "warn",
                        "unable to parse launch date",
                        extras={"value": launch_text},
                    )
            else:
                _log(
                    "warn",
                    "launch date value missing",
                    extras={"selector": "Launch Date"},
                )
    except Exception as exc:  # pragma: no cover
        _log(
            "warn",
            "failed to extract launch date",
            extras={"error": str(exc)},
        )

    async def _collect_section(section_name: str) -> Tuple[List[List[str]], List[str], bool]:
        config = SECTION_CONFIGS[section_name]
        heading_locator = page.locator(
            "xpath=//h3[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), \"%s\")]"
            % section_name.lower()
        )
        try:
            heading_count = await heading_locator.count()
        except Exception:
            heading_count = 0
        heading_present = heading_count > 0

        candidate_tables: List[Locator] = []
        for idx in range(heading_count):
            heading = heading_locator.nth(idx)
            candidate_tables.extend(await _expand_tables(heading.locator("xpath=following-sibling::table")))
            candidate_tables.extend(
                await _expand_tables(
                    heading.locator(
                        "xpath=ancestor::*[self::section or contains(concat(' ', normalize-space(@class), ' '), ' card ')][1]//table"
                    )
                )
            )
            candidate_tables.extend(await _expand_tables(heading.locator("xpath=parent::*//table")))

        page_tables_locator = page.locator("table")
        candidate_tables.extend(await _expand_tables(page_tables_locator))

        table_locator = await _select_table(candidate_tables, config)
        if table_locator is None:
            if not heading_present:
                _log("warn", "section heading missing", extras={"section": section_name})
            _log("warn", "table not found for section", extras={"section": section_name})
            return [], [], False

        headers = await _get_column_headers(table_locator)
        rows = await _extract_table_rows(table_locator)
        if not rows:
            _log("warn", "section table empty", extras={"section": section_name})
            return [], headers, False
        sections_with_data.add(section_name)
        return rows, headers, True

    # Revenue
    revenue_rows, revenue_headers, revenue_found = await _collect_section("Revenue")
    if revenue_found:
        await _parse_revenue(revenue_rows, revenue_headers, _set_metric)

    # Pickup
    pickup_rows, pickup_headers, pickup_found = await _collect_section("Pickup")
    if pickup_found:
        await _parse_pickup(pickup_rows, pickup_headers, _set_metric)

    # Delivery
    delivery_rows, delivery_headers, delivery_found = await _collect_section("Delivery")
    if delivery_found:
        await _parse_delivery(delivery_rows, delivery_headers, _set_metric)

    # Repeat Customers
    repeat_rows, repeat_headers, repeat_found = await _collect_section("Repeat Customers")
    if repeat_found:
        await _parse_repeat_customers(repeat_rows, repeat_headers, _set_metric)

    # Package
    package_rows, package_headers, package_found = await _collect_section("Package")
    if package_found:
        await _parse_package(package_rows, package_headers, _set_metric)

    for metric in DASHBOARD_SUMMARY_COLUMNS:
        if metric not in found_metrics and dashboard_data.get(metric) is None:
            section = METRIC_SECTIONS.get(metric)
            if section and section not in sections_with_data:
                continue
            _log(
                "warn",
                "dashboard metric missing",
                extras={"metric": metric},
            )

    if gstin_column_name is not None and dashboard_data.get(gstin_column_name) is None:
        _log(
            "warn",
            "gstin not found on dashboard",
            extras={"store_code": store_code, "user_bit_text": user_bit_text},
        )

    return dashboard_data


async def _expand_tables(locator: Locator) -> List[Locator]:
    tables: List[Locator] = []
    try:
        count = await locator.count()
    except Exception:
        return tables
    for idx in range(count):
        tables.append(locator.nth(idx))
    return tables


async def _select_table(candidates: List[Locator], config: SectionConfig) -> Optional[Locator]:
    seen: set[str] = set()
    for locator in candidates:
        try:
            handle = await locator.element_handle()
        except Exception:
            continue
        if handle is None:
            continue
        handle_id = handle._impl_obj._guid if hasattr(handle, "_impl_obj") else repr(handle)
        if handle_id in seen:
            continue
        seen.add(handle_id)
        header_text = await _get_table_header_text(locator)
        if not header_text:
            continue
        matches = sum(1 for keyword in config.keywords if keyword in header_text)
        if matches >= config.min_matches:
            return locator
    return None


async def _get_table_header_text(locator: Locator) -> str:
    try:
        header_locator = locator.locator("thead")
        if await header_locator.count() > 0:
            text = await header_locator.inner_text()
        else:
            first_row = locator.locator("tr").first
            if await first_row.count() == 0:
                return ""
            text = await first_row.inner_text()
    except Exception:
        return ""
    return _normalize_space(text)


async def _get_column_headers(locator: Locator) -> List[str]:
    try:
        header_rows = locator.locator("thead tr")
        if await header_rows.count() > 0:
            header_row = header_rows.nth(await header_rows.count() - 1)
        else:
            header_row = locator.locator("tr").first
        cells = header_row.locator("th,td")
        count = await cells.count()
        headers: List[str] = []
        for idx in range(count):
            try:
                text = await cells.nth(idx).inner_text()
            except Exception:
                text = ""
            headers.append((text or "").strip())
        return headers
    except Exception:
        return []


async def _extract_table_rows(locator: Locator) -> List[List[str]]:
    rows: List[List[str]] = []
    try:
        body_rows = locator.locator("tbody tr")
        count = await body_rows.count()
        if count == 0:
            body_rows = locator.locator("tr")
            count = await body_rows.count()
            skip_header = await locator.locator("thead tr").count() > 0
        else:
            skip_header = False
        for idx in range(count):
            if skip_header and idx == 0:
                continue
            row_locator = body_rows.nth(idx)
            td_count = await row_locator.locator("td").count()
            if td_count == 0:
                continue
            cell_locator = row_locator.locator("th,td")
            cell_count = await cell_locator.count()
            if cell_count == 0:
                continue
            row_values: List[str] = []
            for cell_idx in range(cell_count):
                try:
                    value = await cell_locator.nth(cell_idx).inner_text()
                except Exception:
                    value = ""
                row_values.append((value or "").strip())
            if any(value for value in row_values):
                rows.append(row_values)
    except Exception:
        return rows
    return rows


async def _parse_revenue(rows: List[List[str]], headers: List[str], set_metric) -> None:
    column_map: Dict[int, str] = {}
    for idx in range(len(headers)):
        normalized = _normalize_label(headers[idx])
        metric = REVENUE_HEADER_TO_METRIC.get(normalized)
        if metric:
            column_map[idx] = metric
    revenue_row = _find_revenue_row(rows)
    if revenue_row and column_map:
        for idx, metric in column_map.items():
            if idx >= len(revenue_row):
                continue
            value = revenue_row[idx]
            set_metric(metric, value)
        return
    for row in rows:
        if len(row) < 2:
            continue
        label = _normalize_label(row[0])
        metric = REVENUE_LABEL_TO_METRIC.get(label)
        if not metric:
            continue
        value = _value_from_row(row)
        if value is not None:
            set_metric(metric, value)


def _find_revenue_row(rows: List[List[str]]) -> Optional[List[str]]:
    for row in rows:
        if len(row) < 2:
            continue
        label = _normalize_label(row[0])
        if "revenue" in label or not label:
            return row
    return rows[0] if rows and len(rows[0]) > 1 else None


async def _parse_pickup(rows: List[List[str]], headers: List[str], set_metric) -> None:
    if not rows or not headers:
        return
    data_row = rows[0]
    for idx, header_text in enumerate(headers):
        if idx >= len(data_row):
            continue
        label = _normalize_label(header_text)
        if not label:
            continue
        if "new" in label:
            row_type = "new"
        elif "existing" in label:
            row_type = "existing"
        elif "total" in label:
            row_type = "total"
        else:
            continue
        lower = header_text.lower()
        if "%" in header_text or "pct" in label or "percentage" in lower:
            column_type = "conv_pct"
        elif "conv" in lower:
            column_type = "conv_count"
        else:
            column_type = "count"
        metric_map = PICKUP_ROW_METRICS.get(row_type)
        if not metric_map:
            continue
        metric = metric_map.get(column_type)
        if not metric:
            continue
        set_metric(metric, data_row[idx])


async def _parse_label_section(rows: List[List[str]], mapping: Dict[str, str], set_metric) -> None:
    for row in rows:
        if len(row) < 2:
            continue
        label = _normalize_label(row[0])
        metric = mapping.get(label)
        if not metric:
            continue
        value = _value_from_row(row)
        if value is None:
            continue
        set_metric(metric, value)


async def _parse_delivery(
    rows: List[List[str]],
    headers: List[str],
    set_metric,
) -> None:
    if not rows or not headers:
        return

    data_row = rows[0]
    for idx, header in enumerate(headers):
        if idx >= len(data_row):
            continue
        label = _normalize_label(header)
        metric = DELIVERY_LABEL_TO_METRIC.get(label)
        if not metric:
            continue
        set_metric(metric, data_row[idx])


async def _parse_repeat_customers(
    rows: List[List[str]],
    headers: List[str],
    set_metric,
) -> None:
    if not rows or not headers:
        return

    data_row = rows[0]
    for idx, header in enumerate(headers):
        if idx >= len(data_row):
            continue
        label = _normalize_label(header)
        metric = REPEAT_LABEL_TO_METRIC.get(label)
        if not metric:
            continue
        set_metric(metric, data_row[idx])


async def _parse_package(
    rows: List[List[str]],
    headers: List[str],
    set_metric,
) -> None:
    if not rows or not headers:
        return

    data_row = rows[0]
    for idx, header in enumerate(headers):
        if idx >= len(data_row):
            continue
        label = _normalize_label(header)
        metric = _identify_package_metric(label)
        if not metric:
            continue
        set_metric(metric, data_row[idx])


def _identify_package_metric(label: str) -> Optional[str]:
    direct = PACKAGE_LABEL_TO_METRIC.get(label)
    if direct:
        return direct
    if "nonpackage" in label or "nonpkg" in label:
        if "undelivered" in label:
            return "package_non_pkg_over_800_undelivered"
        return "package_non_pkg_over_800"
    return None


def _value_from_row(row: List[str]) -> Optional[str]:
    for cell in row[1:]:
        if _clean_number(cell):
            return cell
    return row[-1] if len(row) > 1 else None


__all__ = ["extract_dashboard_summary"]
