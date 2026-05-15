"""Use connected payment components in missing view."""

from __future__ import annotations

from alembic import op

revision = "0112_conn_pay_missing"
down_revision = "0111_missing_view_py_logic"
branch_labels = None
depends_on = None

_RECOVERY_EXCLUSIONS_SQL = (
    "'TO_BE_RECOVERED', 'TO_BE_COMPENSATED', 'RECOVERED', 'COMPENSATED', 'WRITE_OFF'"
)


POSTGRES_VIEW_SQL = f"""
CREATE OR REPLACE VIEW public.vw_orders_missing_in_payment_collections AS
WITH RECURSIVE sales_totals AS (
    SELECT
        s.cost_center,
        upper(regexp_replace(coalesce(s.order_number, ''), '\\s+', '', 'g')) AS order_token,
        SUM(COALESCE(s.payment_received, 0)) AS payment_received
    FROM public.sales AS s
    GROUP BY
        s.cost_center,
        upper(regexp_replace(coalesce(s.order_number, ''), '\\s+', '', 'g'))
),
valid_payment_rows AS (
    SELECT
        pc.payment_id,
        pc.cost_center,
        COALESCE(pc.amount, 0) AS amount,
        array_remove(
            regexp_split_to_array(
                upper(regexp_replace(coalesce(pc.order_number, ''), '\\s+', '', 'g')),
                '[/,]+'
            ),
            ''
        ) AS order_tokens
    FROM public.payment_collections AS pc
    WHERE lower(pc.source_type) IN ('google_sheet', 'legacy_sales')
),
payment_tokens AS (
    SELECT DISTINCT
        vpr.payment_id,
        vpr.cost_center,
        tok.order_token
    FROM valid_payment_rows AS vpr
    CROSS JOIN LATERAL unnest(vpr.order_tokens) AS tok(order_token)
    WHERE tok.order_token <> ''
),
payment_edges AS (
    SELECT DISTINCT
        left_token.cost_center,
        left_token.order_token AS left_token,
        right_token.order_token AS right_token
    FROM payment_tokens AS left_token
    JOIN payment_tokens AS right_token
      ON right_token.payment_id = left_token.payment_id
     AND right_token.cost_center = left_token.cost_center
),
payment_component_walk(cost_center, order_token, connected_token) AS (
    SELECT
        pe.cost_center,
        pe.left_token,
        pe.right_token
    FROM payment_edges AS pe
    UNION
    SELECT
        pcw.cost_center,
        pcw.order_token,
        pe.right_token
    FROM payment_component_walk AS pcw
    JOIN payment_edges AS pe
      ON pe.cost_center = pcw.cost_center
     AND pe.left_token = pcw.connected_token
),
component_roots AS (
    SELECT
        pcw.cost_center,
        pcw.order_token,
        MIN(pcw.connected_token) AS component_root
    FROM payment_component_walk AS pcw
    GROUP BY
        pcw.cost_center,
        pcw.order_token
),
component_tokens AS (
    SELECT DISTINCT
        cr.cost_center,
        cr.component_root,
        cr.order_token
    FROM component_roots AS cr
),
payment_components AS (
    SELECT
        pt.payment_id,
        pt.cost_center,
        MIN(cr.component_root) AS component_root
    FROM payment_tokens AS pt
    JOIN component_roots AS cr
      ON cr.cost_center = pt.cost_center
     AND cr.order_token = pt.order_token
    GROUP BY
        pt.payment_id,
        pt.cost_center
),
component_evidence_totals AS (
    SELECT
        pc.cost_center,
        pc.component_root,
        SUM(vpr.amount) AS evidence_amount
    FROM payment_components AS pc
    JOIN valid_payment_rows AS vpr
      ON vpr.payment_id = pc.payment_id
     AND vpr.cost_center = pc.cost_center
    GROUP BY
        pc.cost_center,
        pc.component_root
),
component_order_totals AS (
    SELECT
        ct.cost_center,
        ct.component_root,
        SUM(COALESCE(o.order_amount, 0)) AS order_amount
    FROM component_tokens AS ct
    JOIN public.vw_orders AS o
      ON o.cost_center = ct.cost_center
     AND upper(regexp_replace(coalesce(o.order_number, ''), '\\s+', '', 'g')) = ct.order_token
    GROUP BY
        ct.cost_center,
        ct.component_root
),
paid_components AS (
    SELECT
        cet.cost_center,
        cet.component_root
    FROM component_evidence_totals AS cet
    JOIN component_order_totals AS cot
      ON cot.cost_center = cet.cost_center
     AND cot.component_root = cet.component_root
    WHERE cet.evidence_amount + 1 >= cot.order_amount
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
        cr.component_root,
        pc.component_root IS NOT NULL AS component_paid
    FROM public.vw_orders AS o
    LEFT JOIN sales_totals AS st
      ON st.cost_center = o.cost_center
     AND st.order_token = upper(regexp_replace(coalesce(o.order_number, ''), '\\s+', '', 'g'))
    LEFT JOIN component_roots AS cr
      ON cr.cost_center = o.cost_center
     AND cr.order_token = upper(regexp_replace(coalesce(o.order_number, ''), '\\s+', '', 'g'))
    LEFT JOIN paid_components AS pc
      ON pc.cost_center = cr.cost_center
     AND pc.component_root = cr.component_root
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
  AND NOT component_paid;
"""


SQLITE_VIEW_SQL = f"""
CREATE VIEW vw_orders_missing_in_payment_collections AS
WITH RECURSIVE
sales_totals AS (
    SELECT
        cost_center,
        upper(replace(coalesce(order_number, ''), ' ', '')) AS order_token,
        SUM(COALESCE(payment_received, 0)) AS payment_received
    FROM sales
    GROUP BY
        cost_center,
        upper(replace(coalesce(order_number, ''), ' ', ''))
),
valid_payment_rows AS (
    SELECT
        payment_id,
        cost_center,
        COALESCE(amount, 0) AS amount,
        replace(replace(upper(replace(coalesce(order_number, ''), ' ', '')), '/', ','), ',,', ',') AS order_number_csv
    FROM payment_collections
    WHERE lower(source_type) IN ('google_sheet', 'legacy_sales')
),
payment_tokens AS (
    SELECT DISTINCT
        vpr.payment_id,
        vpr.cost_center,
        upper(trim(tok.value)) AS order_token
    FROM valid_payment_rows AS vpr
    JOIN json_each('["' || replace(vpr.order_number_csv, ',', '","') || '"]') AS tok
    WHERE trim(tok.value) <> ''
),
payment_edges AS (
    SELECT DISTINCT
        left_token.cost_center,
        left_token.order_token AS left_token,
        right_token.order_token AS right_token
    FROM payment_tokens AS left_token
    JOIN payment_tokens AS right_token
      ON right_token.payment_id = left_token.payment_id
     AND right_token.cost_center = left_token.cost_center
),
payment_component_walk(cost_center, order_token, connected_token) AS (
    SELECT
        cost_center,
        left_token,
        right_token
    FROM payment_edges
    UNION
    SELECT
        pcw.cost_center,
        pcw.order_token,
        pe.right_token
    FROM payment_component_walk AS pcw
    JOIN payment_edges AS pe
      ON pe.cost_center = pcw.cost_center
     AND pe.left_token = pcw.connected_token
),
component_roots AS (
    SELECT
        cost_center,
        order_token,
        MIN(connected_token) AS component_root
    FROM payment_component_walk
    GROUP BY
        cost_center,
        order_token
),
payment_components AS (
    SELECT
        pt.payment_id,
        pt.cost_center,
        MIN(cr.component_root) AS component_root
    FROM payment_tokens AS pt
    JOIN component_roots AS cr
      ON cr.cost_center = pt.cost_center
     AND cr.order_token = pt.order_token
    GROUP BY
        pt.payment_id,
        pt.cost_center
),
component_tokens AS (
    SELECT DISTINCT
        cost_center,
        component_root,
        order_token
    FROM component_roots
),
component_evidence_totals AS (
    SELECT
        pc.cost_center,
        pc.component_root,
        SUM(vpr.amount) AS evidence_amount
    FROM payment_components AS pc
    JOIN valid_payment_rows AS vpr
      ON vpr.payment_id = pc.payment_id
     AND vpr.cost_center = pc.cost_center
    GROUP BY
        pc.cost_center,
        pc.component_root
),
component_order_totals AS (
    SELECT
        ct.cost_center,
        ct.component_root,
        SUM(COALESCE(o.order_amount, 0)) AS order_amount
    FROM component_tokens AS ct
    JOIN vw_orders AS o
      ON o.cost_center = ct.cost_center
     AND upper(replace(coalesce(o.order_number, ''), ' ', '')) = ct.order_token
    GROUP BY
        ct.cost_center,
        ct.component_root
),
paid_components AS (
    SELECT
        cet.cost_center,
        cet.component_root
    FROM component_evidence_totals AS cet
    JOIN component_order_totals AS cot
      ON cot.cost_center = cet.cost_center
     AND cot.component_root = cet.component_root
    WHERE cet.evidence_amount + 1 >= cot.order_amount
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
        cr.component_root,
        pc.component_root IS NOT NULL AS component_paid
    FROM vw_orders AS o
    LEFT JOIN sales_totals AS st
      ON st.cost_center = o.cost_center
     AND st.order_token = upper(replace(coalesce(o.order_number, ''), ' ', ''))
    LEFT JOIN component_roots AS cr
      ON cr.cost_center = o.cost_center
     AND cr.order_token = upper(replace(coalesce(o.order_number, ''), ' ', ''))
    LEFT JOIN paid_components AS pc
      ON pc.cost_center = cr.cost_center
     AND pc.component_root = cr.component_root
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
  AND NOT component_paid;
"""


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute(POSTGRES_VIEW_SQL)
    else:
        op.execute("DROP VIEW IF EXISTS vw_orders_missing_in_payment_collections;")
        op.execute(SQLITE_VIEW_SQL)


def downgrade() -> None:
    # Forward-only replacement of the compatibility/audit read model.
    return None
