from __future__ import annotations

import sqlalchemy as sa

_ORDERS_RECOVERY_TABLE = sa.table(
    "orders",
    sa.column("cost_center"),
    sa.column("order_number"),
    sa.column("recovery_status"),
    sa.column("recovery_category"),
    sa.column("recovery_notes"),
)


async def clear_to_be_recovered_order(
    *,
    session,
    cost_center: str,
    order_number: str,
    recovery_notes: str,
) -> None:
    """Clear a TO_BE_RECOVERED order after payment evidence is found."""

    await session.execute(
        sa.update(_ORDERS_RECOVERY_TABLE)
        .where(_ORDERS_RECOVERY_TABLE.c.cost_center == cost_center)
        .where(_ORDERS_RECOVERY_TABLE.c.order_number == order_number)
        .where(_ORDERS_RECOVERY_TABLE.c.recovery_status == "TO_BE_RECOVERED")
        .values(
            recovery_status="NONE",
            recovery_category=None,
            recovery_notes=recovery_notes,
        )
    )
