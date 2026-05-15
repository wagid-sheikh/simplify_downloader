"""Canonicalize payment evidence audit view."""

from __future__ import annotations

from alembic import op


revision = "0114_payment_audit_canon"
down_revision = ("0113_audit_unmatched_tokens", "0113_conn_pay_audit")
branch_labels = None
depends_on = None


POSTGRES_VIEW_SQL = r"""
CREATE OR REPLACE VIEW public.vw_payment_evidence_reconciliation AS
WITH RECURSIVE payment_base AS (
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
                upper(regexp_replace(coalesce(pc.order_number, ''), '\s+', '', 'g')),
                '[/,]+'
            ),
            ''
        ) AS order_tokens
    FROM public.payment_collections AS pc
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
        o.order_date,
        o.recovery_status,
        o.recovery_category,
        COALESCE(o.order_amount, 0) AS order_amount
    FROM component_roots AS cr
    LEFT JOIN public.vw_orders AS o
      ON o.cost_center = cr.cost_center
     AND upper(regexp_replace(coalesce(o.order_number, ''), '\s+', '', 'g')) = cr.order_token
),
payment_row_stats AS (
    SELECT
        pb.payment_id,
        pb.cost_center,
        COALESCE(cardinality(pb.order_tokens), 0) AS token_count,
        COALESCE(pc.component_root, '') AS component_root,
        COALESCE(
            array_agg(ct.order_token ORDER BY COALESCE(ct.order_date::text, '9999-12-31T23:59:59'), COALESCE(ct.matched_order_number, ct.order_token), ct.order_token)
                FILTER (WHERE ct.order_token IS NOT NULL),
            ARRAY[]::text[]
        ) AS normalized_order_tokens,
        COALESCE(
            string_agg(ct.order_token, '|' ORDER BY COALESCE(ct.order_date::text, '9999-12-31T23:59:59'), COALESCE(ct.matched_order_number, ct.order_token), ct.order_token)
                FILTER (WHERE ct.order_token IS NOT NULL),
            ''
        ) AS group_key
    FROM payment_base AS pb
    LEFT JOIN payment_components AS pc
      ON pc.payment_id = pb.payment_id
     AND pc.cost_center = pb.cost_center
    LEFT JOIN component_tokens AS ct
      ON ct.cost_center = pc.cost_center
     AND ct.component_root = pc.component_root
    GROUP BY pb.payment_id, pb.cost_center, pb.order_tokens, pc.component_root
),
payment_groups AS (
    SELECT
        pc.cost_center,
        pc.component_root,
        SUM(COALESCE(pb.amount, 0)) AS evidence_amount,
        COUNT(*) AS evidence_row_count
    FROM payment_components AS pc
    JOIN payment_base AS pb
      ON pb.payment_id = pc.payment_id
     AND pb.cost_center = pc.cost_center
    GROUP BY pc.cost_center, pc.component_root
),
order_totals AS (
    SELECT
        ct.cost_center,
        ct.component_root,
        COUNT(DISTINCT ct.order_token) AS token_count,
        COUNT(DISTINCT CASE WHEN ct.matched_order_number IS NOT NULL THEN upper(regexp_replace(ct.matched_order_number, '\s+', '', 'g')) END) AS matched_order_count,
        SUM(COALESCE(ct.order_amount, 0)) AS order_amount,
        COALESCE(string_agg(DISTINCT ct.recovery_status, ',') FILTER (WHERE ct.recovery_status IS NOT NULL AND ct.recovery_status <> ''), '') AS recovery_statuses_csv,
        COALESCE(string_agg(DISTINCT ct.recovery_category, ',') FILTER (WHERE ct.recovery_category IS NOT NULL AND ct.recovery_category <> ''), '') AS recovery_categories_csv
    FROM component_tokens AS ct
    GROUP BY ct.cost_center, ct.component_root
),
sales_totals AS (
    SELECT
        ct.cost_center,
        ct.component_root,
        COUNT(s.order_number) AS sales_row_count,
        SUM(COALESCE(s.payment_received, 0)) AS payment_received
    FROM component_tokens AS ct
    LEFT JOIN public.sales AS s
      ON s.cost_center = ct.cost_center
     AND upper(regexp_replace(coalesce(s.order_number, ''), '\s+', '', 'g')) = ct.order_token
    GROUP BY ct.cost_center, ct.component_root
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
    COALESCE(array_length(prs.normalized_order_tokens, 1), 0) > 1 AS is_grouped,
    pb.amount::numeric(12, 2) AS amount,
    COALESCE(ot.order_amount, 0)::numeric(12, 2) AS order_amount,
    COALESCE(st.payment_received, 0)::numeric(12, 2) AS payment_received,
    pb.bank_row_id,
    CASE
        WHEN COALESCE(prs.token_count, 0) = 0 THEN 'missing order token'
        WHEN COALESCE(ot.matched_order_count, 0) = 0 THEN 'missing order token'
        WHEN COALESCE(ot.matched_order_count, 0) < COALESCE(ot.token_count, prs.token_count, 0) THEN 'unmatched order token'
        WHEN COALESCE(st.sales_row_count, 0) = 0 THEN 'missing sales'
        WHEN COALESCE(array_length(prs.normalized_order_tokens, 1), 0) > 1 AND COALESCE(pg.evidence_amount, 0) + 1 >= COALESCE(ot.order_amount, 0) THEN 'grouped paid'
        WHEN COALESCE(array_length(prs.normalized_order_tokens, 1), 0) > 1 THEN 'grouped short'
        WHEN COALESCE(pg.evidence_amount, 0) + 1 >= COALESCE(ot.order_amount, 0) THEN 'paid'
        ELSE 'short'
    END AS reconciliation_result,
    prs.group_key,
    pb.cost_center || '|' || prs.group_key AS component_id,
    COALESCE(pg.evidence_amount, 0)::numeric(12, 2) AS grouped_amount,
    COALESCE(ot.order_amount, 0)::numeric(12, 2) AS grouped_order_amount,
    COALESCE(st.payment_received, 0)::numeric(12, 2) AS grouped_payment_received,
    (COALESCE(st.payment_received, 0) - COALESCE(pg.evidence_amount, 0))::numeric(12, 2) AS sales_evidence_difference,
    ABS(COALESCE(st.payment_received, 0) - COALESCE(pg.evidence_amount, 0)) > 1 AS sales_evidence_mismatch,
    CASE
        WHEN ABS(COALESCE(st.payment_received, 0) - COALESCE(pg.evidence_amount, 0)) <= 1 THEN 'matched'
        WHEN COALESCE(st.payment_received, 0) - COALESCE(pg.evidence_amount, 0) > 0 THEN 'sales higher'
        ELSE 'evidence higher'
    END AS sales_evidence_classification,
    COALESCE(ot.recovery_statuses_csv, '') AS recovery_statuses_csv,
    COALESCE(ot.recovery_categories_csv, '') AS recovery_categories_csv,
    COALESCE(ot.token_count, 0) AS token_count,
    COALESCE(ot.matched_order_count, 0) AS matched_order_count
FROM payment_base AS pb
JOIN payment_row_stats AS prs
  ON prs.payment_id = pb.payment_id
 AND prs.cost_center = pb.cost_center
LEFT JOIN payment_groups AS pg
  ON pg.cost_center = pb.cost_center
 AND pg.component_root = prs.component_root
LEFT JOIN order_totals AS ot
  ON ot.cost_center = pb.cost_center
 AND ot.component_root = prs.component_root
LEFT JOIN sales_totals AS st
  ON st.cost_center = pb.cost_center
 AND st.component_root = prs.component_root;
"""


SQLITE_VIEW_SQL = """
CREATE VIEW vw_payment_evidence_reconciliation AS
WITH RECURSIVE
payment_base AS (
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
    SELECT cost_center, order_token, MIN(connected_token) AS component_root
    FROM payment_component_walk
    GROUP BY cost_center, order_token
),
payment_components AS (
    SELECT pt.payment_id, pt.cost_center, MIN(cr.component_root) AS component_root
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
        o.order_date,
        o.recovery_status,
        o.recovery_category,
        COALESCE(o.order_amount, 0) AS order_amount
    FROM component_roots AS cr
    LEFT JOIN vw_orders AS o
      ON o.cost_center = cr.cost_center
     AND upper(replace(coalesce(o.order_number, ''), ' ', '')) = cr.order_token
),
payment_row_stats AS (
    SELECT
        pb.payment_id,
        pb.cost_center,
        COUNT(DISTINCT own_tokens.order_token) AS token_count,
        COALESCE(pc.component_root, '') AS component_root,
        COALESCE((
            SELECT group_concat(ordered_tokens.order_token, '|')
            FROM (
                SELECT ct.order_token
                FROM component_tokens AS ct
                WHERE ct.cost_center = pb.cost_center
                  AND ct.component_root = pc.component_root
                ORDER BY COALESCE(ct.order_date, '9999-12-31T23:59:59'), COALESCE(ct.matched_order_number, ct.order_token), ct.order_token
            ) AS ordered_tokens
        ), '') AS group_key,
        COALESCE((
            SELECT group_concat(ordered_tokens.order_token, ',')
            FROM (
                SELECT ct.order_token
                FROM component_tokens AS ct
                WHERE ct.cost_center = pb.cost_center
                  AND ct.component_root = pc.component_root
                ORDER BY COALESCE(ct.order_date, '9999-12-31T23:59:59'), COALESCE(ct.matched_order_number, ct.order_token), ct.order_token
            ) AS ordered_tokens
        ), '') AS normalized_order_tokens_csv
    FROM payment_base AS pb
    LEFT JOIN payment_tokens AS own_tokens
      ON own_tokens.payment_id = pb.payment_id
     AND own_tokens.cost_center = pb.cost_center
    LEFT JOIN payment_components AS pc
      ON pc.payment_id = pb.payment_id
     AND pc.cost_center = pb.cost_center
    GROUP BY pb.payment_id, pb.cost_center, pc.component_root
),
payment_groups AS (
    SELECT pc.cost_center, pc.component_root, SUM(COALESCE(pb.amount, 0)) AS evidence_amount, COUNT(*) AS evidence_row_count
    FROM payment_components AS pc
    JOIN payment_base AS pb
      ON pb.payment_id = pc.payment_id
     AND pb.cost_center = pc.cost_center
    GROUP BY pc.cost_center, pc.component_root
),
order_totals AS (
    SELECT
        ct.cost_center,
        ct.component_root,
        COUNT(DISTINCT ct.order_token) AS token_count,
        COUNT(DISTINCT CASE WHEN ct.matched_order_number IS NOT NULL THEN upper(replace(ct.matched_order_number, ' ', '')) END) AS matched_order_count,
        SUM(COALESCE(ct.order_amount, 0)) AS order_amount,
        COALESCE((
            SELECT group_concat(statuses.recovery_status, ',')
            FROM (
                SELECT DISTINCT ct2.recovery_status
                FROM component_tokens AS ct2
                WHERE ct2.cost_center = ct.cost_center
                  AND ct2.component_root = ct.component_root
                  AND ct2.recovery_status IS NOT NULL
                  AND ct2.recovery_status <> ''
                ORDER BY ct2.recovery_status
            ) AS statuses
        ), '') AS recovery_statuses_csv,
        COALESCE((
            SELECT group_concat(categories.recovery_category, ',')
            FROM (
                SELECT DISTINCT ct2.recovery_category
                FROM component_tokens AS ct2
                WHERE ct2.cost_center = ct.cost_center
                  AND ct2.component_root = ct.component_root
                  AND ct2.recovery_category IS NOT NULL
                  AND ct2.recovery_category <> ''
                ORDER BY ct2.recovery_category
            ) AS categories
        ), '') AS recovery_categories_csv
    FROM component_tokens AS ct
    GROUP BY ct.cost_center, ct.component_root
),
sales_totals AS (
    SELECT ct.cost_center, ct.component_root, COUNT(s.order_number) AS sales_row_count, SUM(COALESCE(s.payment_received, 0)) AS payment_received
    FROM component_tokens AS ct
    LEFT JOIN sales AS s
      ON s.cost_center = ct.cost_center
     AND upper(replace(coalesce(s.order_number, ''), ' ', '')) = ct.order_token
    GROUP BY ct.cost_center, ct.component_root
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
    CASE WHEN prs.normalized_order_tokens_csv = '' THEN 0 ELSE instr(prs.normalized_order_tokens_csv, ',') > 0 END AS is_grouped,
    CAST(pb.amount AS NUMERIC(12, 2)) AS amount,
    CAST(COALESCE(ot.order_amount, 0) AS NUMERIC(12, 2)) AS order_amount,
    CAST(COALESCE(st.payment_received, 0) AS NUMERIC(12, 2)) AS payment_received,
    pb.bank_row_id,
    CASE
        WHEN COALESCE(prs.token_count, 0) = 0 THEN 'missing order token'
        WHEN COALESCE(ot.matched_order_count, 0) = 0 THEN 'missing order token'
        WHEN COALESCE(ot.matched_order_count, 0) < COALESCE(ot.token_count, prs.token_count, 0) THEN 'unmatched order token'
        WHEN COALESCE(st.sales_row_count, 0) = 0 THEN 'missing sales'
        WHEN instr(prs.normalized_order_tokens_csv, ',') > 0 AND COALESCE(pg.evidence_amount, 0) + 1 >= COALESCE(ot.order_amount, 0) THEN 'grouped paid'
        WHEN instr(prs.normalized_order_tokens_csv, ',') > 0 THEN 'grouped short'
        WHEN COALESCE(pg.evidence_amount, 0) + 1 >= COALESCE(ot.order_amount, 0) THEN 'paid'
        ELSE 'short'
    END AS reconciliation_result,
    prs.group_key,
    pb.cost_center || '|' || prs.group_key AS component_id,
    CAST(COALESCE(pg.evidence_amount, 0) AS NUMERIC(12, 2)) AS grouped_amount,
    CAST(COALESCE(ot.order_amount, 0) AS NUMERIC(12, 2)) AS grouped_order_amount,
    CAST(COALESCE(st.payment_received, 0) AS NUMERIC(12, 2)) AS grouped_payment_received,
    CAST((COALESCE(st.payment_received, 0) - COALESCE(pg.evidence_amount, 0)) AS NUMERIC(12, 2)) AS sales_evidence_difference,
    ABS(COALESCE(st.payment_received, 0) - COALESCE(pg.evidence_amount, 0)) > 1 AS sales_evidence_mismatch,
    CASE
        WHEN ABS(COALESCE(st.payment_received, 0) - COALESCE(pg.evidence_amount, 0)) <= 1 THEN 'matched'
        WHEN COALESCE(st.payment_received, 0) - COALESCE(pg.evidence_amount, 0) > 0 THEN 'sales higher'
        ELSE 'evidence higher'
    END AS sales_evidence_classification,
    COALESCE(ot.recovery_statuses_csv, '') AS recovery_statuses_csv,
    COALESCE(ot.recovery_categories_csv, '') AS recovery_categories_csv,
    COALESCE(ot.token_count, 0) AS token_count,
    COALESCE(ot.matched_order_count, 0) AS matched_order_count
FROM payment_base AS pb
JOIN payment_row_stats AS prs
  ON prs.payment_id = pb.payment_id
 AND prs.cost_center = pb.cost_center
LEFT JOIN payment_groups AS pg
  ON pg.cost_center = pb.cost_center
 AND pg.component_root = prs.component_root
LEFT JOIN order_totals AS ot
  ON ot.cost_center = pb.cost_center
 AND ot.component_root = prs.component_root
LEFT JOIN sales_totals AS st
  ON st.cost_center = pb.cost_center
 AND st.component_root = prs.component_root;
"""


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute(POSTGRES_VIEW_SQL)
    else:
        op.execute("DROP VIEW IF EXISTS vw_payment_evidence_reconciliation;")
        op.execute(SQLITE_VIEW_SQL)


def downgrade() -> None:
    # Forward-only replacement of the compatibility/audit read model.
    return None
