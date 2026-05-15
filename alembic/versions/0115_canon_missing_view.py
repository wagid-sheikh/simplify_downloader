"""Canonicalize missing payment collections view."""

from __future__ import annotations

from alembic import op


revision = "0115_canon_missing_view"
down_revision = "0114_payment_audit_canon"
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
payment_base AS (
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
        pb.payment_id,
        pb.cost_center,
        token.order_token
    FROM payment_base AS pb
    CROSS JOIN LATERAL unnest(pb.order_tokens) AS token(order_token)
    WHERE token.order_token <> ''
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
    SELECT pe.cost_center, pe.left_token, pe.right_token
    FROM payment_edges AS pe
    UNION
    SELECT pcw.cost_center, pcw.order_token, pe.right_token
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
    GROUP BY pcw.cost_center, pcw.order_token
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
    GROUP BY pt.payment_id, pt.cost_center
),
component_tokens AS (
    SELECT DISTINCT
        cr.cost_center,
        cr.component_root,
        cr.order_token,
        o.order_number AS matched_order_number,
        COALESCE(o.order_amount, 0) AS order_amount
    FROM component_roots AS cr
    LEFT JOIN public.vw_orders AS o
      ON o.cost_center = cr.cost_center
     AND upper(regexp_replace(coalesce(o.order_number, ''), '\\s+', '', 'g')) = cr.order_token
),
component_quality AS (
    SELECT
        ct.cost_center,
        ct.component_root,
        COUNT(DISTINCT ct.order_token) AS token_count,
        COUNT(DISTINCT CASE WHEN ct.matched_order_number IS NOT NULL THEN ct.order_token END) AS matched_order_count,
        SUM(CASE WHEN ct.matched_order_number IS NOT NULL THEN COALESCE(ct.order_amount, 0) ELSE 0 END) AS order_amount
    FROM component_tokens AS ct
    GROUP BY ct.cost_center, ct.component_root
),
component_evidence_totals AS (
    SELECT
        pc.cost_center,
        pc.component_root,
        SUM(COALESCE(pb.amount, 0)) AS evidence_amount
    FROM payment_components AS pc
    JOIN payment_base AS pb
      ON pb.payment_id = pc.payment_id
     AND pb.cost_center = pc.cost_center
    GROUP BY pc.cost_center, pc.component_root
),
component_status AS (
    SELECT
        cq.cost_center,
        cq.component_root,
        COALESCE(cq.matched_order_count, 0) > 0 AS has_payment_proof,
        COALESCE(cq.matched_order_count, 0) < COALESCE(cq.token_count, 0) AS has_data_quality_exception,
        COALESCE(cet.evidence_amount, 0) + 1 >= COALESCE(cq.order_amount, 0) AS component_paid
    FROM component_quality AS cq
    LEFT JOIN component_evidence_totals AS cet
      ON cet.cost_center = cq.cost_center
     AND cet.component_root = cq.component_root
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
        COALESCE(cs.has_payment_proof, FALSE) AS has_payment_proof,
        COALESCE(cs.has_data_quality_exception, FALSE) AS has_data_quality_exception,
        COALESCE(cs.component_paid, FALSE) AS component_paid
    FROM public.vw_orders AS o
    LEFT JOIN sales_totals AS st
      ON st.cost_center = o.cost_center
     AND st.order_token = upper(regexp_replace(coalesce(o.order_number, ''), '\\s+', '', 'g'))
    LEFT JOIN component_roots AS cr
      ON cr.cost_center = o.cost_center
     AND cr.order_token = upper(regexp_replace(coalesce(o.order_number, ''), '\\s+', '', 'g'))
    LEFT JOIN component_status AS cs
      ON cs.cost_center = cr.cost_center
     AND cs.component_root = cr.component_root
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
WITH RECURSIVE
sales_totals AS (
    SELECT
        cost_center,
        upper(replace(coalesce(order_number, ''), ' ', '')) AS order_token,
        SUM(COALESCE(payment_received, 0)) AS payment_received
    FROM sales
    GROUP BY cost_center, upper(replace(coalesce(order_number, ''), ' ', ''))
),
payment_base AS (
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
        pb.payment_id,
        pb.cost_center,
        upper(trim(token.value)) AS order_token
    FROM payment_base AS pb
    JOIN json_each('["' || replace(pb.order_number_csv, ',', '","') || '"]') AS token
    WHERE trim(token.value) <> ''
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
    SELECT cost_center, left_token, right_token
    FROM payment_edges
    UNION
    SELECT pcw.cost_center, pcw.order_token, pe.right_token
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
    GROUP BY cost_center, order_token
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
    GROUP BY pt.payment_id, pt.cost_center
),
component_tokens AS (
    SELECT DISTINCT
        cr.cost_center,
        cr.component_root,
        cr.order_token,
        o.order_number AS matched_order_number,
        COALESCE(o.order_amount, 0) AS order_amount
    FROM component_roots AS cr
    LEFT JOIN vw_orders AS o
      ON o.cost_center = cr.cost_center
     AND upper(replace(coalesce(o.order_number, ''), ' ', '')) = cr.order_token
),
component_quality AS (
    SELECT
        ct.cost_center,
        ct.component_root,
        COUNT(DISTINCT ct.order_token) AS token_count,
        COUNT(DISTINCT CASE WHEN ct.matched_order_number IS NOT NULL THEN ct.order_token END) AS matched_order_count,
        SUM(CASE WHEN ct.matched_order_number IS NOT NULL THEN COALESCE(ct.order_amount, 0) ELSE 0 END) AS order_amount
    FROM component_tokens AS ct
    GROUP BY ct.cost_center, ct.component_root
),
component_evidence_totals AS (
    SELECT pc.cost_center, pc.component_root, SUM(COALESCE(pb.amount, 0)) AS evidence_amount
    FROM payment_components AS pc
    JOIN payment_base AS pb
      ON pb.payment_id = pc.payment_id
     AND pb.cost_center = pc.cost_center
    GROUP BY pc.cost_center, pc.component_root
),
component_status AS (
    SELECT
        cq.cost_center,
        cq.component_root,
        COALESCE(cq.matched_order_count, 0) > 0 AS has_payment_proof,
        COALESCE(cq.matched_order_count, 0) < COALESCE(cq.token_count, 0) AS has_data_quality_exception,
        COALESCE(cet.evidence_amount, 0) + 1 >= COALESCE(cq.order_amount, 0) AS component_paid
    FROM component_quality AS cq
    LEFT JOIN component_evidence_totals AS cet
      ON cet.cost_center = cq.cost_center
     AND cet.component_root = cq.component_root
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
        COALESCE(cs.has_payment_proof, 0) AS has_payment_proof,
        COALESCE(cs.has_data_quality_exception, 0) AS has_data_quality_exception,
        COALESCE(cs.component_paid, 0) AS component_paid
    FROM vw_orders AS o
    LEFT JOIN sales_totals AS st
      ON st.cost_center = o.cost_center
     AND st.order_token = upper(replace(coalesce(o.order_number, ''), ' ', ''))
    LEFT JOIN component_roots AS cr
      ON cr.cost_center = o.cost_center
     AND cr.order_token = upper(replace(coalesce(o.order_number, ''), ' ', ''))
    LEFT JOIN component_status AS cs
      ON cs.cost_center = cr.cost_center
     AND cs.component_root = cr.component_root
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
    # Forward-only canonical replacement of the compatibility read model.
    return None
