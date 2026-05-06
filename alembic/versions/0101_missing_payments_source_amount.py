"""Use source-aware amount in missing-payments view."""

from __future__ import annotations

from alembic import op


revision = "0101_missing_pay_source_amount"
down_revision = "0100_fix_missing_pay_view"
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
    CASE
        WHEN o.source_system = 'TumbleDry' THEN o.net_amount
        ELSE o.gross_amount
    END AS net_amount
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
    CASE
        WHEN o.source_system = 'TumbleDry' THEN o.net_amount
        ELSE o.gross_amount
    END;
"""


def upgrade() -> None:
    op.execute(VIEW_SQL)


def downgrade() -> None:
    # Forward-only migration: keep the source-aware view in place.
    pass
