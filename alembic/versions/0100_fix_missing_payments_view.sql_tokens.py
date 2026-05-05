"""Fix missing-payments view token parsing and add lookup index."""

from __future__ import annotations

from alembic import op


revision = "0100_fix_missing_pay_view"
down_revision = "0099_add_missing_payments_view"
branch_labels = None
depends_on = None


VIEW_SQL = """
CREATE OR REPLACE VIEW vw_orders_missing_in_payment_collections AS
SELECT
    o.cost_center,
    o.order_number,
    (o.order_date AT TIME ZONE 'Asia/Kolkata')::date AS order_date,
    o.customer_name,
    o.mobile_number,
    o.net_amount
FROM public.orders o
JOIN public.sales s
    ON s.cost_center = o.cost_center
   AND s.order_number = o.order_number
WHERE NOT EXISTS (
    SELECT 1
    FROM public.payment_collections pc
    CROSS JOIN LATERAL regexp_split_to_table(
        regexp_replace(coalesce(pc.order_number, ''), '\\s+', '', 'g'),
        '[/,]+'
    ) AS tok(order_token)
    WHERE pc.cost_center = o.cost_center
      AND tok.order_token <> ''
      AND upper(tok.order_token) = upper(regexp_replace(coalesce(o.order_number, ''), '\\s+', '', 'g'))
)
GROUP BY
    o.cost_center,
    o.order_number,
    (o.order_date AT TIME ZONE 'Asia/Kolkata')::date,
    o.customer_name,
    o.mobile_number,
    o.net_amount;
"""


def upgrade() -> None:
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_payment_collections_cost_center
        ON public.payment_collections (cost_center);
        """
    )
    op.execute(VIEW_SQL)


def downgrade() -> None:
    op.execute(
        """
        CREATE OR REPLACE VIEW vw_orders_missing_in_payment_collections AS
        SELECT
            o.cost_center,
            o.order_number,
            (o.order_date AT TIME ZONE 'Asia/Kolkata')::date AS order_date,
            o.customer_name,
            o.mobile_number,
            o.net_amount

        FROM public.orders o
        JOIN public.sales s
            ON s.cost_center = o.cost_center
           AND s.order_number = o.order_number

        WHERE NOT EXISTS (
            SELECT 1
            FROM public.payment_collections pc
            WHERE pc.cost_center = o.cost_center
              AND o.order_number = ANY (
                  regexp_split_to_array(
                      regexp_replace(pc.order_number, '\\s+', '', 'g'),
                      ','
                  )
              )
        )

        GROUP BY
            o.cost_center,
            o.order_number,
            (o.order_date AT TIME ZONE 'Asia/Kolkata')::date,
            o.customer_name,
            o.mobile_number,
            o.net_amount;
        """
    )
    op.execute("DROP INDEX IF EXISTS public.ix_payment_collections_cost_center;")
