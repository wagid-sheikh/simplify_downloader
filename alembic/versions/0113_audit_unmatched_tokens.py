"""Flag payment evidence components with unmatched tokens."""

from __future__ import annotations

from alembic import op


revision = "0113_audit_unmatched_tokens"
down_revision = "0112_missing_unmatched_tokens"
branch_labels = None
depends_on = None


POSTGRES_VIEW_SQL = """
CREATE OR REPLACE VIEW public.vw_payment_evidence_reconciliation AS
WITH payment_base AS (
    SELECT
        pc.payment_id,
        pc.source_type,
        pc.source_sheet_row,
        pc.cost_center,
        pc.payment_date,
        pc.payment_timestamp,
        pc.order_number,
        pc.amount,
        pc.bank_row_id,
        array_remove(
            regexp_split_to_array(
                upper(regexp_replace(coalesce(pc.order_number, ''), '\\s+', '', 'g')),
                '[/,]+'
            ),
            ''
        ) AS order_tokens
    FROM public.payment_collections AS pc
),
payment_tokens AS (
    SELECT
        pb.payment_id,
        pb.cost_center,
        token.order_token
    FROM payment_base AS pb
    CROSS JOIN LATERAL unnest(pb.order_tokens) AS token(order_token)
    WHERE token.order_token <> ''
),
payment_row_stats AS (
    SELECT
        pb.payment_id,
        COALESCE(cardinality(pb.order_tokens), 0) AS token_count,
        COALESCE(string_agg(DISTINCT pt.order_token, '|' ORDER BY pt.order_token), '') AS group_key,
        COALESCE(array_agg(DISTINCT pt.order_token ORDER BY pt.order_token) FILTER (WHERE pt.order_token IS NOT NULL), ARRAY[]::text[]) AS normalized_order_tokens
    FROM payment_base AS pb
    LEFT JOIN payment_tokens AS pt
      ON pt.payment_id = pb.payment_id
     AND pt.cost_center = pb.cost_center
    GROUP BY
        pb.payment_id,
        pb.order_tokens
),
payment_groups AS (
    SELECT
        pb.cost_center,
        prs.group_key,
        SUM(COALESCE(pb.amount, 0)) AS evidence_amount,
        COUNT(*) AS evidence_row_count
    FROM payment_base AS pb
    JOIN payment_row_stats AS prs
      ON prs.payment_id = pb.payment_id
    GROUP BY
        pb.cost_center,
        prs.group_key
),
group_tokens AS (
    SELECT DISTINCT
        pb.cost_center,
        prs.group_key,
        pt.order_token
    FROM payment_base AS pb
    JOIN payment_row_stats AS prs
      ON prs.payment_id = pb.payment_id
    JOIN payment_tokens AS pt
      ON pt.payment_id = pb.payment_id
     AND pt.cost_center = pb.cost_center
),
order_totals AS (
    SELECT
        gt.cost_center,
        gt.group_key,
        COUNT(DISTINCT gt.order_token) AS token_count,
        COUNT(DISTINCT CASE WHEN o.order_number IS NOT NULL THEN upper(regexp_replace(o.order_number, '\\s+', '', 'g')) END) AS matched_order_count,
        SUM(COALESCE(o.order_amount, 0)) AS order_amount
    FROM group_tokens AS gt
    LEFT JOIN public.vw_orders AS o
      ON o.cost_center = gt.cost_center
     AND upper(regexp_replace(coalesce(o.order_number, ''), '\\s+', '', 'g')) = gt.order_token
    GROUP BY
        gt.cost_center,
        gt.group_key
),
sales_totals AS (
    SELECT
        gt.cost_center,
        gt.group_key,
        COUNT(s.order_number) AS sales_row_count,
        SUM(COALESCE(s.payment_received, 0)) AS payment_received
    FROM group_tokens AS gt
    LEFT JOIN public.sales AS s
      ON s.cost_center = gt.cost_center
     AND upper(regexp_replace(coalesce(s.order_number, ''), '\\s+', '', 'g')) = gt.order_token
    GROUP BY
        gt.cost_center,
        gt.group_key
)
SELECT
    pb.payment_id,
    pb.source_type,
    pb.source_sheet_row,
    pb.cost_center,
    pb.payment_date,
    pb.payment_timestamp,
    pb.order_number,
    prs.normalized_order_tokens,
    array_to_string(prs.normalized_order_tokens, ',') AS normalized_order_tokens_csv,
    COALESCE(prs.token_count, 0) > 1 AS is_grouped,
    COALESCE(ot.token_count, prs.token_count, 0) AS token_count,
    COALESCE(ot.matched_order_count, 0) AS matched_order_count,
    pb.amount::numeric(12, 2) AS amount,
    COALESCE(ot.order_amount, 0)::numeric(12, 2) AS order_amount,
    COALESCE(st.payment_received, 0)::numeric(12, 2) AS payment_received,
    pb.bank_row_id,
    CASE
        WHEN COALESCE(prs.token_count, 0) = 0 THEN 'missing order token'
        WHEN COALESCE(ot.matched_order_count, 0) < COALESCE(ot.token_count, prs.token_count, 0) THEN 'unmatched order token'
        WHEN COALESCE(st.sales_row_count, 0) = 0 THEN 'missing sales'
        WHEN COALESCE(prs.token_count, 0) > 1 AND COALESCE(pg.evidence_amount, 0) + 1 >= COALESCE(ot.order_amount, 0) THEN 'grouped paid'
        WHEN COALESCE(prs.token_count, 0) > 1 THEN 'grouped short'
        WHEN COALESCE(pg.evidence_amount, 0) + 1 >= COALESCE(ot.order_amount, 0) THEN 'paid'
        ELSE 'short'
    END AS reconciliation_result,
    prs.group_key,
    COALESCE(pg.evidence_amount, 0)::numeric(12, 2) AS grouped_amount,
    COALESCE(ot.order_amount, 0)::numeric(12, 2) AS grouped_order_amount,
    COALESCE(st.payment_received, 0)::numeric(12, 2) AS grouped_payment_received,
    (COALESCE(st.payment_received, 0) - COALESCE(pg.evidence_amount, 0))::numeric(12, 2) AS sales_evidence_difference,
    ABS(COALESCE(st.payment_received, 0) - COALESCE(pg.evidence_amount, 0)) > 1 AS sales_evidence_mismatch
FROM payment_base AS pb
JOIN payment_row_stats AS prs
  ON prs.payment_id = pb.payment_id
LEFT JOIN payment_groups AS pg
  ON pg.cost_center = pb.cost_center
 AND pg.group_key = prs.group_key
LEFT JOIN order_totals AS ot
  ON ot.cost_center = pb.cost_center
 AND ot.group_key = prs.group_key
LEFT JOIN sales_totals AS st
  ON st.cost_center = pb.cost_center
 AND st.group_key = prs.group_key;
"""


SQLITE_VIEW_SQL = """
CREATE VIEW vw_payment_evidence_reconciliation AS
WITH payment_base AS (
    SELECT
        payment_id,
        source_type,
        source_sheet_row,
        cost_center,
        payment_date,
        payment_timestamp,
        order_number,
        amount,
        bank_row_id,
        replace(replace(upper(replace(coalesce(order_number, ''), ' ', '')), '/', ','), ',,', ',') AS order_number_csv
    FROM payment_collections
),
payment_tokens AS (
    SELECT
        pb.payment_id,
        pb.cost_center,
        trim(token.value) AS order_token
    FROM payment_base AS pb
    JOIN json_each('["' || replace(pb.order_number_csv, ',', '","') || '"]') AS token
    WHERE trim(token.value) <> ''
),
payment_row_stats AS (
    SELECT
        pb.payment_id,
        COUNT(DISTINCT pt.order_token) AS token_count,
        COALESCE((
            SELECT group_concat(ordered_tokens.order_token, '|')
            FROM (
                SELECT DISTINCT pt2.order_token
                FROM payment_tokens AS pt2
                WHERE pt2.payment_id = pb.payment_id
                  AND pt2.cost_center = pb.cost_center
                ORDER BY pt2.order_token
            ) AS ordered_tokens
        ), '') AS group_key,
        COALESCE((
            SELECT group_concat(ordered_tokens.order_token, ',')
            FROM (
                SELECT DISTINCT pt2.order_token
                FROM payment_tokens AS pt2
                WHERE pt2.payment_id = pb.payment_id
                  AND pt2.cost_center = pb.cost_center
                ORDER BY pt2.order_token
            ) AS ordered_tokens
        ), '') AS normalized_order_tokens_csv
    FROM payment_base AS pb
    LEFT JOIN payment_tokens AS pt
      ON pt.payment_id = pb.payment_id
     AND pt.cost_center = pb.cost_center
    GROUP BY
        pb.payment_id,
        pb.cost_center
),
payment_groups AS (
    SELECT
        pb.cost_center,
        prs.group_key,
        SUM(COALESCE(pb.amount, 0)) AS evidence_amount,
        COUNT(*) AS evidence_row_count
    FROM payment_base AS pb
    JOIN payment_row_stats AS prs
      ON prs.payment_id = pb.payment_id
    GROUP BY
        pb.cost_center,
        prs.group_key
),
group_tokens AS (
    SELECT DISTINCT
        pb.cost_center,
        prs.group_key,
        pt.order_token
    FROM payment_base AS pb
    JOIN payment_row_stats AS prs
      ON prs.payment_id = pb.payment_id
    JOIN payment_tokens AS pt
      ON pt.payment_id = pb.payment_id
     AND pt.cost_center = pb.cost_center
),
order_totals AS (
    SELECT
        gt.cost_center,
        gt.group_key,
        COUNT(DISTINCT gt.order_token) AS token_count,
        COUNT(DISTINCT CASE WHEN o.order_number IS NOT NULL THEN upper(replace(o.order_number, ' ', '')) END) AS matched_order_count,
        SUM(COALESCE(o.order_amount, 0)) AS order_amount
    FROM group_tokens AS gt
    LEFT JOIN vw_orders AS o
      ON o.cost_center = gt.cost_center
     AND upper(replace(coalesce(o.order_number, ''), ' ', '')) = gt.order_token
    GROUP BY
        gt.cost_center,
        gt.group_key
),
sales_totals AS (
    SELECT
        gt.cost_center,
        gt.group_key,
        COUNT(s.order_number) AS sales_row_count,
        SUM(COALESCE(s.payment_received, 0)) AS payment_received
    FROM group_tokens AS gt
    LEFT JOIN sales AS s
      ON s.cost_center = gt.cost_center
     AND upper(replace(coalesce(s.order_number, ''), ' ', '')) = gt.order_token
    GROUP BY
        gt.cost_center,
        gt.group_key
)
SELECT
    pb.payment_id,
    pb.source_type,
    pb.source_sheet_row,
    pb.cost_center,
    pb.payment_date,
    pb.payment_timestamp,
    pb.order_number,
    prs.normalized_order_tokens_csv AS normalized_order_tokens,
    prs.normalized_order_tokens_csv,
    COALESCE(prs.token_count, 0) > 1 AS is_grouped,
    COALESCE(ot.token_count, prs.token_count, 0) AS token_count,
    COALESCE(ot.matched_order_count, 0) AS matched_order_count,
    CAST(pb.amount AS NUMERIC(12, 2)) AS amount,
    CAST(COALESCE(ot.order_amount, 0) AS NUMERIC(12, 2)) AS order_amount,
    CAST(COALESCE(st.payment_received, 0) AS NUMERIC(12, 2)) AS payment_received,
    pb.bank_row_id,
    CASE
        WHEN COALESCE(prs.token_count, 0) = 0 THEN 'missing order token'
        WHEN COALESCE(ot.matched_order_count, 0) < COALESCE(ot.token_count, prs.token_count, 0) THEN 'unmatched order token'
        WHEN COALESCE(st.sales_row_count, 0) = 0 THEN 'missing sales'
        WHEN COALESCE(prs.token_count, 0) > 1 AND COALESCE(pg.evidence_amount, 0) + 1 >= COALESCE(ot.order_amount, 0) THEN 'grouped paid'
        WHEN COALESCE(prs.token_count, 0) > 1 THEN 'grouped short'
        WHEN COALESCE(pg.evidence_amount, 0) + 1 >= COALESCE(ot.order_amount, 0) THEN 'paid'
        ELSE 'short'
    END AS reconciliation_result,
    prs.group_key,
    CAST(COALESCE(pg.evidence_amount, 0) AS NUMERIC(12, 2)) AS grouped_amount,
    CAST(COALESCE(ot.order_amount, 0) AS NUMERIC(12, 2)) AS grouped_order_amount,
    CAST(COALESCE(st.payment_received, 0) AS NUMERIC(12, 2)) AS grouped_payment_received,
    CAST((COALESCE(st.payment_received, 0) - COALESCE(pg.evidence_amount, 0)) AS NUMERIC(12, 2)) AS sales_evidence_difference,
    ABS(COALESCE(st.payment_received, 0) - COALESCE(pg.evidence_amount, 0)) > 1 AS sales_evidence_mismatch
FROM payment_base AS pb
JOIN payment_row_stats AS prs
  ON prs.payment_id = pb.payment_id
LEFT JOIN payment_groups AS pg
  ON pg.cost_center = pb.cost_center
 AND pg.group_key = prs.group_key
LEFT JOIN order_totals AS ot
  ON ot.cost_center = pb.cost_center
 AND ot.group_key = prs.group_key
LEFT JOIN sales_totals AS st
  ON st.cost_center = pb.cost_center
 AND st.group_key = prs.group_key;
"""


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute(POSTGRES_VIEW_SQL)
    else:
        op.execute("DROP VIEW IF EXISTS vw_payment_evidence_reconciliation;")
        op.execute(SQLITE_VIEW_SQL)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS vw_payment_evidence_reconciliation;")
