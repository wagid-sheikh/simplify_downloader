"""Cap resolution for customer retention workbook lead allocation.

Inputs are a DB session plus the source/scope/store/run-date tuple. Output is a
structured decision object that callers can use without consulting environment
variables or generic config. Lead caps are sourced only from
``customer_followup_cap_config``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from .constants import LEAD_SOURCE_TD
from .db_tables import customer_followup_cap_config


@dataclass(frozen=True)
class CapResolutionWarning:
    code: str
    message: str
    cap_config_ids: tuple[int, ...] = ()


@dataclass(frozen=True)
class CapResolutionResult:
    lead_source_type: str
    work_section: str
    cost_center: str | None
    run_date: date
    cap_config_id: int | None
    daily_cap: int | None
    is_uncapped: bool
    missing: bool = False
    valid: bool = True
    warnings: tuple[CapResolutionWarning, ...] = field(default_factory=tuple)

    @property
    def has_cap(self) -> bool:
        return self.valid and not self.missing and not self.is_uncapped and self.daily_cap is not None


def _row_specificity(row: sa.RowMapping | dict[str, Any]) -> int:
    return 1 if row.get("cost_center") else 0


async def resolve_active_cap(
    session: AsyncSession,
    *,
    lead_source_type: str,
    work_section: str,
    cost_center: str | None,
    run_date: date,
) -> CapResolutionResult:
    """Resolve the active cap row for a source/scope/store/date.

    Specificity is deliberately applied before recency: a store-specific row is
    allowed to override a newer global row because global rows are defaults, not
    cross-store policy locks.
    """

    normalized_cost_center = cost_center.strip().upper() if cost_center else None
    rows = (
        await session.execute(
            sa.select(customer_followup_cap_config)
            .where(
                customer_followup_cap_config.c.enabled.is_(True),
                customer_followup_cap_config.c.lead_source_type == lead_source_type,
                customer_followup_cap_config.c.work_section == work_section,
                customer_followup_cap_config.c.effective_from <= run_date,
                sa.or_(
                    customer_followup_cap_config.c.effective_until.is_(None),
                    customer_followup_cap_config.c.effective_until >= run_date,
                ),
                sa.or_(
                    customer_followup_cap_config.c.cost_center.is_(None),
                    customer_followup_cap_config.c.cost_center == normalized_cost_center,
                ),
            )
        )
    ).mappings().all()
    if not rows:
        return CapResolutionResult(lead_source_type, work_section, normalized_cost_center, run_date, None, None, False, missing=True)

    max_specificity = max(_row_specificity(row) for row in rows)
    specific_rows = [row for row in rows if _row_specificity(row) == max_specificity]
    latest_effective = max(row["effective_from"] for row in specific_rows)
    latest_rows = [row for row in specific_rows if row["effective_from"] == latest_effective]

    warnings: list[CapResolutionWarning] = []
    # Multiple latest rows at the winning specificity cannot be ordered
    # deterministically by SRS rules, so surface this as a validation error even
    # if a legacy DB allowed the ambiguous overlap.
    if len(latest_rows) > 1:
        ids = tuple(int(row["cap_config_id"]) for row in latest_rows)
        warnings.append(CapResolutionWarning("ambiguous_active_cap", "Multiple active cap rows have the same specificity and effective_from date", ids))
        return CapResolutionResult(lead_source_type, work_section, normalized_cost_center, run_date, None, None, False, valid=False, warnings=tuple(warnings))

    selected = latest_rows[0]
    cap_id = int(selected["cap_config_id"])
    is_uncapped = bool(selected["is_uncapped"])
    daily_cap = int(selected["daily_cap"]) if selected["daily_cap"] is not None else None

    if lead_source_type == LEAD_SOURCE_TD and not is_uncapped:
        warning = CapResolutionWarning("td_cap_contract_violation", "TD cap rows are valid only when is_uncapped is true", (cap_id,))
        return CapResolutionResult(lead_source_type, work_section, normalized_cost_center, run_date, cap_id, daily_cap, is_uncapped, valid=False, warnings=(warning,))

    return CapResolutionResult(lead_source_type, work_section, normalized_cost_center, run_date, cap_id, daily_cap, is_uncapped)
