"""Add typed pickup_created_at timestamp for CRM leads reporting sort."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from alembic import op
import sqlalchemy as sa


revision = "0087_crm_leads_created_at"
down_revision = "0086_td_leads_html_template"
branch_labels = None
depends_on = None

_CREATED_AT_FORMAT = "%d %b %Y %I:%M:%S %p"
_IST = ZoneInfo("Asia/Kolkata")
_UTC = ZoneInfo("UTC")


def _parse_pickup_created_at(raw_value: str | None) -> datetime | None:
    if not raw_value:
        return None

    normalized = " ".join(str(raw_value).strip().split())
    if not normalized:
        return None

    try:
        parsed = datetime.strptime(normalized, _CREATED_AT_FORMAT)
    except ValueError:
        return None

    return parsed.replace(tzinfo=_IST).astimezone(_UTC)


def upgrade() -> None:
    op.add_column("crm_leads", sa.Column("pickup_created_at", sa.DateTime(timezone=True), nullable=True))

    bind = op.get_bind()
    crm_leads = sa.table(
        "crm_leads",
        sa.column("id", sa.BigInteger()),
        sa.column("pickup_created_date", sa.String(length=64)),
        sa.column("pickup_created_at", sa.DateTime(timezone=True)),
    )

    rows = bind.execute(sa.select(crm_leads.c.id, crm_leads.c.pickup_created_date)).mappings().all()
    for row in rows:
        parsed_created_at = _parse_pickup_created_at(row.get("pickup_created_date"))
        if parsed_created_at is None:
            continue
        bind.execute(
            crm_leads.update().where(crm_leads.c.id == row["id"]).values(pickup_created_at=parsed_created_at)
        )

    op.create_index(
        "ix_crm_leads_store_status_created_at",
        "crm_leads",
        ["store_code", "status_bucket", sa.text("pickup_created_at DESC")],
    )


def downgrade() -> None:
    op.drop_index("ix_crm_leads_store_status_created_at", table_name="crm_leads")
    op.drop_column("crm_leads", "pickup_created_at")
