"""Add TD order adjustment to orders."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0103_td_order_adjustment"
down_revision = "0102_uc_https_config"
branch_labels = None
depends_on = None


POSTGRES_BACKFILL_SQL = """
WITH latest_stg AS (
    SELECT DISTINCT ON (cost_center, order_number, order_date)
        cost_center,
        order_number,
        order_date,
        adjustment
    FROM stg_td_orders
    WHERE adjustment IS NOT NULL
    ORDER BY
        cost_center,
        order_number,
        order_date,
        run_date DESC NULLS LAST,
        id DESC
)
UPDATE orders AS o
SET adjustment = s.adjustment
FROM latest_stg AS s
WHERE o.source_system = 'TumbleDry'
  AND o.cost_center = s.cost_center
  AND o.order_number = s.order_number
  AND o.order_date = s.order_date
  AND o.adjustment IS DISTINCT FROM s.adjustment;
"""

SQLITE_BACKFILL_SQL = """
WITH latest_stg AS (
    SELECT cost_center, order_number, order_date, adjustment
    FROM (
        SELECT
            cost_center,
            order_number,
            order_date,
            adjustment,
            ROW_NUMBER() OVER (
                PARTITION BY cost_center, order_number, order_date
                ORDER BY run_date DESC NULLS LAST, id DESC
            ) AS row_number
        FROM stg_td_orders
        WHERE adjustment IS NOT NULL
    )
    WHERE row_number = 1
)
UPDATE orders
SET adjustment = (
    SELECT latest_stg.adjustment
    FROM latest_stg
    WHERE orders.cost_center = latest_stg.cost_center
      AND orders.order_number = latest_stg.order_number
      AND orders.order_date = latest_stg.order_date
)
WHERE orders.source_system = 'TumbleDry'
  AND EXISTS (
      SELECT 1
      FROM latest_stg
      WHERE orders.cost_center = latest_stg.cost_center
        AND orders.order_number = latest_stg.order_number
        AND orders.order_date = latest_stg.order_date
        AND orders.adjustment IS NOT latest_stg.adjustment
  );
"""


def upgrade() -> None:
    with op.batch_alter_table("orders") as batch_op:
        batch_op.add_column(sa.Column("adjustment", sa.Numeric(12, 2), nullable=True))

    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute(sa.text(POSTGRES_BACKFILL_SQL))
    else:
        op.execute(sa.text(SQLITE_BACKFILL_SQL))


def downgrade() -> None:
    with op.batch_alter_table("orders") as batch_op:
        batch_op.drop_column("adjustment")
