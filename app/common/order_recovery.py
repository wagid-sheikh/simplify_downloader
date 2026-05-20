from __future__ import annotations

from datetime import date, datetime, time

import sqlalchemy as sa

_ORDERS_RECOVERY_TABLE = sa.table(
    "orders",
    sa.column("cost_center"),
    sa.column("order_number"),
    sa.column("recovery_status"),
    sa.column("recovery_category"),
    sa.column("recovery_notes"),
)


RECOVERY_NOTE_SEPARATOR = "\n"


def format_timestamped_recovery_note(
    *,
    occurred_at: date | datetime,
    message: str,
) -> str:
    """Format a stable timestamp-prefixed recovery note entry."""

    if isinstance(occurred_at, datetime):
        timestamp_source = occurred_at
    else:
        timestamp_source = datetime.combine(occurred_at, time.min)
    return f"[{timestamp_source.isoformat(timespec='seconds')}] {message}"


def append_recovery_note(
    existing_notes: object | None,
    note: str,
) -> str:
    """Append a recovery note without discarding existing notes or duplicating entries."""

    existing_text = "" if existing_notes is None else str(existing_notes).strip()
    note_text = note.strip()
    if not existing_text:
        return note_text
    existing_entries = [
        entry.strip()
        for entry in existing_text.split(RECOVERY_NOTE_SEPARATOR)
        if entry.strip()
    ]
    if note_text in existing_entries:
        return existing_text
    return f"{existing_text}{RECOVERY_NOTE_SEPARATOR}{note_text}"


def _normalized_key(column: sa.ColumnElement[object]) -> sa.ColumnElement[str]:
    return sa.func.upper(sa.func.trim(sa.func.coalesce(column, "")))


async def transition_order_recovery_status(
    *,
    session,
    cost_center: str,
    order_number: str,
    from_status: str,
    to_status: str,
    recovery_category: str | None,
    recovery_note: str,
) -> int:
    """Transition one order recovery status while appending to recovery notes."""

    normalized_cost_center = cost_center.strip().upper()
    normalized_order_number = order_number.strip().upper()
    matching_order = (
        _normalized_key(_ORDERS_RECOVERY_TABLE.c.cost_center) == normalized_cost_center
    ) & (
        _normalized_key(_ORDERS_RECOVERY_TABLE.c.order_number)
        == normalized_order_number
    )

    existing_row = (
        await session.execute(
            sa.select(_ORDERS_RECOVERY_TABLE.c.recovery_notes)
            .where(matching_order)
            .where(_ORDERS_RECOVERY_TABLE.c.recovery_status == from_status)
            .limit(1)
        )
    ).first()
    if existing_row is None:
        return 0

    update_result = await session.execute(
        sa.update(_ORDERS_RECOVERY_TABLE)
        .where(matching_order)
        .where(_ORDERS_RECOVERY_TABLE.c.recovery_status == from_status)
        .values(
            recovery_status=to_status,
            recovery_category=recovery_category,
            recovery_notes=append_recovery_note(existing_row[0], recovery_note),
        )
    )
    return int(update_result.rowcount or 0)


async def clear_to_be_recovered_order(
    *,
    session,
    cost_center: str,
    order_number: str,
    recovery_notes: str,
) -> None:
    """Resolve active recovery statuses into terminal business statuses."""

    await transition_order_recovery_status(
        session=session,
        cost_center=cost_center,
        order_number=order_number,
        from_status="TO_BE_RECOVERED",
        to_status="RECOVERED",
        recovery_category="PAYMENT_PROOF_AUTO_RECOVERED",
        recovery_note=recovery_notes,
    )
    await transition_order_recovery_status(
        session=session,
        cost_center=cost_center,
        order_number=order_number,
        from_status="TO_BE_COMPENSATED",
        to_status="COMPENSATED",
        recovery_category="PAYMENT_PROOF_AUTO_RECOVERED",
        recovery_note=recovery_notes,
    )
