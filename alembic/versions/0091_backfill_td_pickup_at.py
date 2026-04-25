"""Backfill crm_leads_current.pickup_created_at from pickup_date when null."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from alembic import op
import sqlalchemy as sa


revision = "0091_backfill_td_pickup_at"
down_revision = "0090_td_leads_subj_prefix"
branch_labels = None
depends_on = None

_IST = ZoneInfo("Asia/Kolkata")
_UTC = ZoneInfo("UTC")


def _parse_pickup_created_at(raw_value: str | None) -> datetime | None:
    if not raw_value:
        return None

    normalized = " ".join(str(raw_value).strip().split())
    if not normalized:
        return None

    for fmt in ("%d %b %Y %I:%M:%S %p", "%d %b %Y %I:%M %p"):
        try:
            return datetime.strptime(normalized, fmt).replace(tzinfo=_IST).astimezone(_UTC)
        except ValueError:
            continue

    try:
        parsed_date = datetime.strptime(normalized, "%d %b %Y").date()
        return datetime(
            parsed_date.year,
            parsed_date.month,
            parsed_date.day,
            0,
            0,
            0,
            tzinfo=_IST,
        ).astimezone(_UTC)
    except ValueError:
        pass

    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return None

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=_IST)
    return parsed.astimezone(_UTC)


def upgrade() -> None:
    bind = op.get_bind()
    crm_leads_current = sa.table(
        "crm_leads_current",
        sa.column("id", sa.BigInteger()),
        sa.column("pickup_date", sa.String(length=64)),
        sa.column("pickup_created_at", sa.DateTime(timezone=True)),
    )

    rows = bind.execute(
        sa.select(crm_leads_current.c.id, crm_leads_current.c.pickup_date)
        .where(crm_leads_current.c.pickup_created_at.is_(None))
        .where(crm_leads_current.c.pickup_date.is_not(None))
    ).mappings().all()

    for row in rows:
        parsed_created_at = _parse_pickup_created_at(row.get("pickup_date"))
        if parsed_created_at is None:
            continue
        bind.execute(
            crm_leads_current.update()
            .where(crm_leads_current.c.id == row["id"])
            .values(pickup_created_at=parsed_created_at)
        )


def downgrade() -> None:
    # Forward-only data migration.
    return None
