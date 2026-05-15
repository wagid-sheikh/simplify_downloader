"""Exclude unmatched-token exceptions from missing-payment view."""

from __future__ import annotations

from alembic import op

revision = "0112_missing_unmatched_tokens"
down_revision = "0111_missing_view_py_logic"
branch_labels = None
depends_on = None

_RECOVERY_EXCLUSIONS_SQL = (
    "'TO_BE_RECOVERED', 'TO_BE_COMPENSATED', 'RECOVERED', 'COMPENSATED', 'WRITE_OFF'"
)

POSTGRES_VIEW_SQL = f"""
CREATE OR REPLACE VIEW public.vw_orders_missing_in_payment_collections AS
WITH sales_totals AS (
    SELECT
        s.cost_center,
        upper(regexp_replace(coalesce(s.order_number, ''), '\\s+', '', 'g')) AS order_token,
        SUM(COALESCE(s.payment_received, 0)) AS payment_received
    FROM public.sales AS s
    GROUP BY
        s.cost_center,
        upper(regexp_replace(coalesce(s.order_number, ''), '\\s+', '', 'g'))
),
payment_tokens AS (
    SELECT
        pc.payment_id,
        pc.cost_center,
        upper(tok.order_token) AS order_token
    FROM public.payment_collections AS pc
    CROSS JOIN LATERAL unnest(
        array_remove(
            regexp_split_to_array(
                regexp_replace(coalesce(pc.order_number, ''), '\\s+', '', 'g'),
                '[/,]+'
            ),
            ''
        )
    ) AS tok(order_token)
    WHERE lower(pc.source_type) IN ('google_sheet', 'legacy_sales')
      AND tok.order_token <> ''
),
payment_token_quality AS (
    SELECT
        pt.payment_id,
        pt.cost_center,
        pt.order_token,
        COUNT(*) AS token_count,
        COUNT(o.order_number) AS matched_order_count
    FROM payment_tokens AS pt
    LEFT JOIN public.vw_orders AS o
      ON o.cost_center = pt.cost_center
     AND upper(regexp_replace(coalesce(o.order_number, ''), '\\s+', '', 'g')) = pt.order_token
    GROUP BY
        pt.payment_id,
        pt.cost_center,
        pt.order_token
),
payment_row_quality AS (
    SELECT
        payment_id,
        cost_center,
        COUNT(DISTINCT order_token) AS token_count,
        COUNT(DISTINCT CASE WHEN matched_order_count > 0 THEN order_token END) AS matched_order_count
    FROM payment_token_quality
    GROUP BY
        payment_id,
        cost_center
),
valid_payment_tokens AS (
    SELECT DISTINCT
        cost_center,
        order_token
    FROM payment_token_quality
    WHERE matched_order_count > 0
),
exception_payment_tokens AS (
    SELECT DISTINCT
        ptq.cost_center,
        ptq.order_token
    FROM payment_token_quality AS ptq
    JOIN payment_row_quality AS prq
      ON prq.payment_id = ptq.payment_id
     AND prq.cost_center = ptq.cost_center
    WHERE ptq.matched_order_count > 0
      AND prq.matched_order_count < prq.token_count
),
candidate_orders AS (
    SELECT
        o.cost_center,
        o.order_number,
        (o.order_date AT TIME ZONE 'Asia/Kolkata')::date AS order_date,
        o.customer_name,
        o.mobile_number,
        o.order_amount,
        COALESCE(st.payment_received, 0) AS sales_payment_received,
        vpt.order_token IS NOT NULL AS has_payment_proof,
        ept.order_token IS NOT NULL AS has_data_quality_exception
    FROM public.vw_orders AS o
    LEFT JOIN sales_totals AS st
      ON st.cost_center = o.cost_center
     AND st.order_token = upper(regexp_replace(coalesce(o.order_number, ''), '\\s+', '', 'g'))
    LEFT JOIN valid_payment_tokens AS vpt
      ON vpt.cost_center = o.cost_center
     AND vpt.order_token = upper(regexp_replace(coalesce(o.order_number, ''), '\\s+', '', 'g'))
    LEFT JOIN exception_payment_tokens AS ept
      ON ept.cost_center = o.cost_center
     AND ept.order_token = upper(regexp_replace(coalesce(o.order_number, ''), '\\s+', '', 'g'))
    WHERE COALESCE(o.recovery_status, 'NONE') NOT IN ({_RECOVERY_EXCLUSIONS_SQL})
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
  AND sales_payment_received > 0
  AND NOT has_payment_proof
  AND NOT has_data_quality_exception;
"""

SQLITE_VIEW_SQL = f"""
CREATE VIEW vw_orders_missing_in_payment_collections AS
WITH sales_totals AS (
    SELECT
        cost_center,
        upper(replace(coalesce(order_number, ''), ' ', '')) AS order_token,
        SUM(COALESCE(payment_received, 0)) AS payment_received
    FROM sales
    GROUP BY
        cost_center,
        upper(replace(coalesce(order_number, ''), ' ', ''))
),
valid_payments AS (
    SELECT
        payment_id,
        cost_center,
        replace(replace(coalesce(order_number, ''), ' ', ''), '/', ',') AS order_number_csv
    FROM payment_collections
    WHERE lower(source_type) IN ('google_sheet', 'legacy_sales')
),
payment_tokens AS (
    SELECT
        vp.payment_id,
        vp.cost_center,
        upper(trim(tok.value)) AS order_token
    FROM valid_payments AS vp
    JOIN json_each('["' || replace(vp.order_number_csv, ',', '","') || '"]') AS tok
    WHERE trim(tok.value) <> ''
),
payment_token_quality AS (
    SELECT
        pt.payment_id,
        pt.cost_center,
        pt.order_token,
        COUNT(*) AS token_count,
        COUNT(o.order_number) AS matched_order_count
    FROM payment_tokens AS pt
    LEFT JOIN vw_orders AS o
      ON o.cost_center = pt.cost_center
     AND upper(replace(coalesce(o.order_number, ''), ' ', '')) = pt.order_token
    GROUP BY
        pt.payment_id,
        pt.cost_center,
        pt.order_token
),
payment_row_quality AS (
    SELECT
        payment_id,
        cost_center,
        COUNT(DISTINCT order_token) AS token_count,
        COUNT(DISTINCT CASE WHEN matched_order_count > 0 THEN order_token END) AS matched_order_count
    FROM payment_token_quality
    GROUP BY
        payment_id,
        cost_center
),
valid_payment_tokens AS (
    SELECT DISTINCT
        cost_center,
        order_token
    FROM payment_token_quality
    WHERE matched_order_count > 0
),
exception_payment_tokens AS (
    SELECT DISTINCT
        ptq.cost_center,
        ptq.order_token
    FROM payment_token_quality AS ptq
    JOIN payment_row_quality AS prq
      ON prq.payment_id = ptq.payment_id
     AND prq.cost_center = ptq.cost_center
    WHERE ptq.matched_order_count > 0
      AND prq.matched_order_count < prq.token_count
),
candidate_orders AS (
    SELECT
        o.cost_center,
        o.order_number,
        date(o.order_date) AS order_date,
        o.customer_name,
        o.mobile_number,
        o.order_amount,
        COALESCE(st.payment_received, 0) AS sales_payment_received,
        vpt.order_token IS NOT NULL AS has_payment_proof,
        ept.order_token IS NOT NULL AS has_data_quality_exception
    FROM vw_orders AS o
    LEFT JOIN sales_totals AS st
      ON st.cost_center = o.cost_center
     AND st.order_token = upper(replace(coalesce(o.order_number, ''), ' ', ''))
    LEFT JOIN valid_payment_tokens AS vpt
      ON vpt.cost_center = o.cost_center
     AND vpt.order_token = upper(replace(coalesce(o.order_number, ''), ' ', ''))
    LEFT JOIN exception_payment_tokens AS ept
      ON ept.cost_center = o.cost_center
     AND ept.order_token = upper(replace(coalesce(o.order_number, ''), ' ', ''))
    WHERE COALESCE(o.recovery_status, 'NONE') NOT IN ({_RECOVERY_EXCLUSIONS_SQL})
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
  AND sales_payment_received > 0
  AND NOT has_payment_proof
  AND NOT has_data_quality_exception;
"""


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute(POSTGRES_VIEW_SQL)
    else:
        op.execute("DROP VIEW IF EXISTS vw_orders_missing_in_payment_collections;")
        op.execute(SQLITE_VIEW_SQL)


def downgrade() -> None:
    return None
