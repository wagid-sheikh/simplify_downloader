"""Revise missing-payment reconciliation logic."""

from __future__ import annotations

from alembic import op


revision = "0108_missing_pay_reconcile"
down_revision = "0107_payment_coll_sources"
branch_labels = None
depends_on = None


_RECOVERY_EXCLUSIONS_SQL = "'TO_BE_RECOVERED', 'TO_BE_COMPENSATED', 'RECOVERED', 'COMPENSATED', 'WRITE_OFF'"


POSTGRES_VIEW_SQL = f"""
CREATE OR REPLACE VIEW public.vw_orders_missing_in_payment_collections AS
WITH valid_payments AS (
    SELECT
        pc.payment_id,
        pc.cost_center,
        COALESCE(pc.amount, 0) AS amount,
        array_remove(
            regexp_split_to_array(
                regexp_replace(coalesce(pc.order_number, ''), '\\s+', '', 'g'),
                '[/,]+'
            ),
            ''
        ) AS order_tokens
    FROM public.payment_collections AS pc
    WHERE pc.source_type IN ('google_sheet', 'legacy_sales')
),
payment_tokens AS (
    SELECT
        vp.payment_id,
        vp.cost_center,
        vp.amount,
        upper(tok.order_token) AS order_token
    FROM valid_payments AS vp
    CROSS JOIN LATERAL unnest(vp.order_tokens) AS tok(order_token)
    WHERE tok.order_token <> ''
),
payment_row_stats AS (
    SELECT
        pt.payment_id,
        pt.cost_center,
        pt.amount,
        COUNT(DISTINCT pt.order_token) AS token_count,
        string_agg(DISTINCT pt.order_token, '|' ORDER BY pt.order_token) AS group_key
    FROM payment_tokens AS pt
    GROUP BY
        pt.payment_id,
        pt.cost_center,
        pt.amount
),
single_payment_tokens AS (
    SELECT
        prs.cost_center,
        prs.group_key AS order_token,
        SUM(prs.amount) AS paid_amount
    FROM payment_row_stats AS prs
    WHERE prs.token_count = 1
    GROUP BY
        prs.cost_center,
        prs.group_key
),
multi_payment_groups AS (
    SELECT
        prs.cost_center,
        prs.group_key,
        SUM(prs.amount) AS paid_amount
    FROM payment_row_stats AS prs
    WHERE prs.token_count > 1
    GROUP BY
        prs.cost_center,
        prs.group_key
),
multi_group_tokens AS (
    SELECT DISTINCT
        prs.cost_center,
        prs.group_key,
        pt.order_token
    FROM payment_row_stats AS prs
    JOIN payment_tokens AS pt
      ON pt.payment_id = prs.payment_id
     AND pt.cost_center = prs.cost_center
    WHERE prs.token_count > 1
),
multi_group_expected AS (
    SELECT
        mgt.cost_center,
        mgt.group_key,
        SUM(o.order_amount) AS expected_amount
    FROM multi_group_tokens AS mgt
    JOIN public.vw_orders AS o
      ON o.cost_center = mgt.cost_center
     AND upper(regexp_replace(coalesce(o.order_number, ''), '\\s+', '', 'g')) = mgt.order_token
    GROUP BY
        mgt.cost_center,
        mgt.group_key
),
group_paid_tokens AS (
    SELECT
        mgt.cost_center,
        mgt.order_token
    FROM multi_group_tokens AS mgt
    JOIN multi_payment_groups AS mpg
      ON mpg.cost_center = mgt.cost_center
     AND mpg.group_key = mgt.group_key
    JOIN multi_group_expected AS mge
      ON mge.cost_center = mgt.cost_center
     AND mge.group_key = mgt.group_key
    WHERE mge.expected_amount > 0
      AND mpg.paid_amount + 1 >= mge.expected_amount
),
candidate_orders AS (
    SELECT
        o.cost_center,
        o.order_number,
        (o.order_date AT TIME ZONE 'Asia/Kolkata')::date AS order_date,
        o.customer_name,
        o.mobile_number,
        o.order_amount,
        COALESCE(spt.paid_amount, 0) AS single_paid_amount,
        gpt.order_token IS NOT NULL AS group_paid
    FROM public.vw_orders AS o
    LEFT JOIN single_payment_tokens AS spt
      ON spt.cost_center = o.cost_center
     AND spt.order_token = upper(regexp_replace(coalesce(o.order_number, ''), '\\s+', '', 'g'))
    LEFT JOIN group_paid_tokens AS gpt
      ON gpt.cost_center = o.cost_center
     AND gpt.order_token = upper(regexp_replace(coalesce(o.order_number, ''), '\\s+', '', 'g'))
    WHERE EXISTS (
        SELECT 1
        FROM public.sales AS s
        WHERE s.cost_center = o.cost_center
          AND s.order_number = o.order_number
    )
      AND COALESCE(o.recovery_status, 'NONE') NOT IN ({_RECOVERY_EXCLUSIONS_SQL})
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
  AND NOT group_paid
  AND NOT (single_paid_amount + 1 >= order_amount);
"""


SQLITE_VIEW_SQL = f"""
CREATE VIEW vw_orders_missing_in_payment_collections AS
WITH valid_payments AS (
    SELECT
        payment_id,
        cost_center,
        COALESCE(amount, 0) AS amount,
        replace(replace(coalesce(order_number, ''), ' ', ''), '/', ',') AS order_number_csv
    FROM payment_collections
    WHERE source_type IN ('google_sheet', 'legacy_sales')
),
payment_tokens AS (
    SELECT
        vp.payment_id,
        vp.cost_center,
        vp.amount,
        upper(trim(tok.value)) AS order_token
    FROM valid_payments AS vp
    JOIN json_each('["' || replace(vp.order_number_csv, ',', '","') || '"]') AS tok
    WHERE trim(tok.value) <> ''
),
payment_row_stats AS (
    SELECT
        pt.payment_id,
        pt.cost_center,
        pt.amount,
        COUNT(DISTINCT pt.order_token) AS token_count,
        (
            SELECT group_concat(ordered_tokens.order_token, '|')
            FROM (
                SELECT DISTINCT pt2.order_token
                FROM payment_tokens AS pt2
                WHERE pt2.payment_id = pt.payment_id
                  AND pt2.cost_center = pt.cost_center
                ORDER BY pt2.order_token
            ) AS ordered_tokens
        ) AS group_key
    FROM payment_tokens AS pt
    GROUP BY
        pt.payment_id,
        pt.cost_center,
        pt.amount
),
single_payment_tokens AS (
    SELECT
        prs.cost_center,
        prs.group_key AS order_token,
        SUM(prs.amount) AS paid_amount
    FROM payment_row_stats AS prs
    WHERE prs.token_count = 1
    GROUP BY
        prs.cost_center,
        prs.group_key
),
multi_payment_groups AS (
    SELECT
        prs.cost_center,
        prs.group_key,
        SUM(prs.amount) AS paid_amount
    FROM payment_row_stats AS prs
    WHERE prs.token_count > 1
    GROUP BY
        prs.cost_center,
        prs.group_key
),
multi_group_tokens AS (
    SELECT DISTINCT
        prs.cost_center,
        prs.group_key,
        pt.order_token
    FROM payment_row_stats AS prs
    JOIN payment_tokens AS pt
      ON pt.payment_id = prs.payment_id
     AND pt.cost_center = prs.cost_center
    WHERE prs.token_count > 1
),
multi_group_expected AS (
    SELECT
        mgt.cost_center,
        mgt.group_key,
        SUM(o.order_amount) AS expected_amount
    FROM multi_group_tokens AS mgt
    JOIN vw_orders AS o
      ON o.cost_center = mgt.cost_center
     AND upper(replace(coalesce(o.order_number, ''), ' ', '')) = mgt.order_token
    GROUP BY
        mgt.cost_center,
        mgt.group_key
),
group_paid_tokens AS (
    SELECT
        mgt.cost_center,
        mgt.order_token
    FROM multi_group_tokens AS mgt
    JOIN multi_payment_groups AS mpg
      ON mpg.cost_center = mgt.cost_center
     AND mpg.group_key = mgt.group_key
    JOIN multi_group_expected AS mge
      ON mge.cost_center = mgt.cost_center
     AND mge.group_key = mgt.group_key
    WHERE mge.expected_amount > 0
      AND mpg.paid_amount + 1 >= mge.expected_amount
),
candidate_orders AS (
    SELECT
        o.cost_center,
        o.order_number,
        date(o.order_date) AS order_date,
        o.customer_name,
        o.mobile_number,
        o.order_amount,
        COALESCE(spt.paid_amount, 0) AS single_paid_amount,
        gpt.order_token IS NOT NULL AS group_paid
    FROM vw_orders AS o
    LEFT JOIN single_payment_tokens AS spt
      ON spt.cost_center = o.cost_center
     AND spt.order_token = upper(replace(coalesce(o.order_number, ''), ' ', ''))
    LEFT JOIN group_paid_tokens AS gpt
      ON gpt.cost_center = o.cost_center
     AND gpt.order_token = upper(replace(coalesce(o.order_number, ''), ' ', ''))
    WHERE EXISTS (
        SELECT 1
        FROM sales AS s
        WHERE s.cost_center = o.cost_center
          AND s.order_number = o.order_number
    )
      AND COALESCE(o.recovery_status, 'NONE') NOT IN ({_RECOVERY_EXCLUSIONS_SQL})
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
  AND NOT group_paid
  AND NOT (single_paid_amount + 1 >= order_amount);
"""


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute(POSTGRES_VIEW_SQL)
    else:
        op.execute("DROP VIEW IF EXISTS vw_orders_missing_in_payment_collections;")
        op.execute(SQLITE_VIEW_SQL)


def downgrade() -> None:
    # Forward-only migration: keep revised missing-payment reconciliation in place.
    return None
