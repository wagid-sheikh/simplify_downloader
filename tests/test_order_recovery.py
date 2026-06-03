from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from app.common.db import session_scope
from app.common.order_recovery import clear_to_be_recovered_order

_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "0124_auto_recovered_category.py"
)
_spec = importlib.util.spec_from_file_location(
    "auto_recovered_category_migration", _MIGRATION_PATH
)
if _spec is None or _spec.loader is None:
    raise RuntimeError("Unable to load auto recovered category migration")
_auto_recovered_category_migration = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_auto_recovered_category_migration)


def test_postgres_constraint_accepts_auto_recovered_category() -> None:
    orders = sa.Table(
        "orders",
        sa.MetaData(),
        sa.Column("recovery_category", sa.String(length=32)),
        sa.CheckConstraint(
            _auto_recovered_category_migration._category_check_sql(),
            name="ck_orders_recovery_category",
        ),
    )

    ddl = str(CreateTable(orders).compile(dialect=postgresql.dialect()))

    assert "ck_orders_recovery_category" in ddl
    assert "PAYMENT_PROOF_AUTO_RECOVERED" in ddl


@pytest.mark.asyncio
async def test_clear_to_be_recovered_order_uses_category_accepted_by_active_schema(
    tmp_path,
) -> None:
    db_path = tmp_path / "order_recovery.db"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    sync_database_url = database_url.replace("+aiosqlite", "")
    category_check_sql = _auto_recovered_category_migration._category_check_sql().text
    create_orders_sql = f"""
    CREATE TABLE orders (
        cost_center TEXT,
        order_number TEXT,
        recovery_status TEXT,
        recovery_category TEXT,
        recovery_notes TEXT,
        CONSTRAINT ck_orders_recovery_category CHECK ({category_check_sql})
    )
    """

    engine = sa.create_engine(sync_database_url)
    with engine.begin() as conn:
        conn.execute(sa.text(create_orders_sql))
        conn.execute(sa.text("""
                INSERT INTO orders (
                    cost_center, order_number, recovery_status, recovery_category, recovery_notes
                )
                VALUES
                    ('cc1', 'ord1', 'TO_BE_RECOVERED', 'OTHER', NULL),
                    ('cc1', 'ord2', 'TO_BE_COMPENSATED', 'OTHER', 'existing note')
                """))

    async with session_scope(database_url) as session:
        await clear_to_be_recovered_order(
            session=session,
            cost_center="CC1",
            order_number="ORD1",
            recovery_notes="auto recovered after matching payment proof",
        )
        await clear_to_be_recovered_order(
            session=session,
            cost_center="CC1",
            order_number="ORD2",
            recovery_notes="auto compensated after matching payment proof",
        )
        await session.commit()

        rows = (await session.execute(sa.text("""
                        SELECT order_number, recovery_status, recovery_category, recovery_notes
                        FROM orders
                        ORDER BY order_number
                        """))).mappings().all()

    by_order_number = {row["order_number"]: row for row in rows}
    assert by_order_number["ord1"]["recovery_status"] == "RECOVERED"
    assert (
        by_order_number["ord1"]["recovery_category"] == "PAYMENT_PROOF_AUTO_RECOVERED"
    )
    assert (
        by_order_number["ord1"]["recovery_notes"]
        == "auto recovered after matching payment proof"
    )
    assert by_order_number["ord2"]["recovery_status"] == "COMPENSATED"
    assert (
        by_order_number["ord2"]["recovery_category"] == "PAYMENT_PROOF_AUTO_RECOVERED"
    )
    assert by_order_number["ord2"]["recovery_notes"] == (
        "existing note\nauto compensated after matching payment proof"
    )
