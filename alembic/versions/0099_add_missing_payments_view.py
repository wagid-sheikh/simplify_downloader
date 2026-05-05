"""Create view for orders missing in payment collections."""

from __future__ import annotations

from alembic import op


revision = "0099_add_missing_payments_view"
down_revision = "0098_td_leads_mode_suffix"
branch_labels = None
depends_on = None


def upgrade() -> None:
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


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS vw_orders_missing_in_payment_collections;")
