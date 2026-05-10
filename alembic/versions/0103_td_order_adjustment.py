"""Add TD order adjustment backfill."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0103_td_order_adjustment"
down_revision = "0102_uc_https_config"
branch_labels = None
depends_on = None


POSTGRES_BACKFILL_SQL = """
WITH ranked_adjustments AS (
    SELECT
        store_code,
        order_number,
        order_date,
        adjustment,
        ROW_NUMBER() OVER (
            PARTITION BY store_code, order_number, order_date
            ORDER BY run_date DESC NULLS LAST, id DESC
        ) AS rn
    FROM stg_td_orders
    WHERE COALESCE(adjustment, 0) > 0
)
UPDATE orders AS o
SET adjustment = s.adjustment
FROM ranked_adjustments AS s
WHERE s.rn = 1
  AND o.source_system = 'TumbleDry'
  AND o.store_code = s.store_code
  AND o.order_number = s.order_number
  AND o.order_date = s.order_date
"""


SQLITE_BACKFILL_SQL = """
WITH ranked_adjustments AS (
    SELECT
        store_code,
        order_number,
        order_date,
        adjustment,
        ROW_NUMBER() OVER (
            PARTITION BY store_code, order_number, order_date
            ORDER BY run_date IS NULL ASC, run_date DESC, id DESC
        ) AS rn
    FROM stg_td_orders
    WHERE COALESCE(adjustment, 0) > 0
)
UPDATE orders
SET adjustment = (
    SELECT s.adjustment
    FROM ranked_adjustments AS s
    WHERE s.rn = 1
      AND orders.source_system = 'TumbleDry'
      AND orders.store_code = s.store_code
      AND orders.order_number = s.order_number
      AND orders.order_date = s.order_date
)
WHERE EXISTS (
    SELECT 1
    FROM ranked_adjustments AS s
    WHERE s.rn = 1
      AND orders.source_system = 'TumbleDry'
      AND orders.store_code = s.store_code
      AND orders.order_number = s.order_number
      AND orders.order_date = s.order_date
)
"""


def upgrade() -> None:
    bind = op.get_bind()
    dialect_name = bind.dialect.name

    op.add_column("orders", sa.Column("adjustment", sa.Numeric(12, 2)))
    if dialect_name == "postgresql":
        op.execute(POSTGRES_BACKFILL_SQL)
    else:
        op.execute(SQLITE_BACKFILL_SQL)


def downgrade() -> None:
    with op.batch_alter_table("orders") as batch_op:
        batch_op.drop_column("adjustment")
