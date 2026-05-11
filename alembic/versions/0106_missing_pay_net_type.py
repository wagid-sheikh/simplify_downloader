"""Preserve missing-payments net_amount type."""

from __future__ import annotations

from alembic import op


revision = "0106_missing_pay_net_type"
down_revision = "0105_missing_pay_vw_orders"
branch_labels = None
depends_on = None


POSTGRES_VIEW_SQL = """
CREATE OR REPLACE VIEW public.vw_orders_missing_in_payment_collections AS
WITH payment_tokens AS (
    SELECT
        pc.cost_center,
        upper(tok.order_token) AS order_token,
        SUM(COALESCE(pc.amount, 0)) AS paid_amount
    FROM public.payment_collections AS pc
    CROSS JOIN LATERAL regexp_split_to_table(
        regexp_replace(coalesce(pc.order_number, ''), '\\s+', '', 'g'),
        '[/,]+'
    ) AS tok(order_token)
    WHERE tok.order_token <> ''
    GROUP BY
        pc.cost_center,
        upper(tok.order_token)
),
candidate_orders AS (
    SELECT
        o.cost_center,
        o.order_number,
        (o.order_date AT TIME ZONE 'Asia/Kolkata')::date AS order_date,
        o.customer_name,
        o.mobile_number,
        o.order_amount,
        COALESCE(pt.paid_amount, 0) AS paid_amount
    FROM public.vw_orders AS o
    LEFT JOIN payment_tokens AS pt
        ON pt.cost_center = o.cost_center
       AND pt.order_token = upper(regexp_replace(coalesce(o.order_number, ''), '\\s+', '', 'g'))
    WHERE EXISTS (
        SELECT 1
        FROM public.sales AS s
        WHERE s.cost_center = o.cost_center
          AND s.order_number = o.order_number
    )
)
SELECT
    cost_center,
    order_number,
    order_date,
    customer_name,
    mobile_number,
    order_amount::numeric(12, 2) AS net_amount
FROM candidate_orders
WHERE order_amount > 0
  AND NOT (paid_amount + 1 >= order_amount);
"""


SQLITE_VIEW_SQL = """
CREATE VIEW vw_orders_missing_in_payment_collections AS
WITH payment_tokens AS (
    SELECT
        pc.cost_center,
        upper(trim(value)) AS order_token,
        SUM(COALESCE(pc.amount, 0)) AS paid_amount
    FROM (
        SELECT
            cost_center,
            amount,
            replace(replace(coalesce(order_number, ''), ' ', ''), '/', ',') AS order_number_csv
        FROM payment_collections
    ) AS pc
    JOIN json_each('["' || replace(pc.order_number_csv, ',', '","') || '"]') AS tok
    WHERE trim(value) <> ''
    GROUP BY
        pc.cost_center,
        upper(trim(value))
),
candidate_orders AS (
    SELECT
        o.cost_center,
        o.order_number,
        date(o.order_date) AS order_date,
        o.customer_name,
        o.mobile_number,
        o.order_amount,
        COALESCE(pt.paid_amount, 0) AS paid_amount
    FROM vw_orders AS o
    LEFT JOIN payment_tokens AS pt
        ON pt.cost_center = o.cost_center
       AND pt.order_token = upper(replace(coalesce(o.order_number, ''), ' ', ''))
    WHERE EXISTS (
        SELECT 1
        FROM sales AS s
        WHERE s.cost_center = o.cost_center
          AND s.order_number = o.order_number
    )
)
SELECT
    cost_center,
    order_number,
    order_date,
    customer_name,
    mobile_number,
    CAST(order_amount AS NUMERIC(12, 2)) AS net_amount
FROM candidate_orders
WHERE order_amount > 0
  AND NOT (paid_amount + 1 >= order_amount);
"""


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute(POSTGRES_VIEW_SQL)
    else:
        op.execute("DROP VIEW IF EXISTS vw_orders_missing_in_payment_collections;")
        op.execute(SQLITE_VIEW_SQL)


def downgrade() -> None:
    # Forward-only migration: keep the typed vw_orders-backed missing-payments view in place.
    pass
