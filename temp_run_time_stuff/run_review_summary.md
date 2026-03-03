# TD Orders Sync Run Review (run_id: 20260302_203724238635)

## 1) Log failures / concerns

- Overall run status is `success` with 65 `ok`, 4 `info`, and 2 `warn` events.
- The only warnings are session-probe warnings for both stores (`A817`, `A668`) indicating existing storage state redirected to login and a re-login was performed.
- No `error` or `failed` events were observed.

## 2) Count summary (log vs produced JSON/XLSX)

### A817

- Log API fetch counts: orders=767, sales=942, garments=6360.
- JSON counts: orders=769, sales=944, garments=6360.
- XLSX counts: orders=767, sales=942, garments=6360.
- Compare workbook summary: total_rows=767, matched_rows=767, missing_in_api=0, missing_in_ui=0, amount_mismatches=0, status_mismatches=0.

Interpretation:
- Orders and sales JSON files include +2 rows vs compare/XLSX/log counts.
- This lines up with log fields `orders_summary_rows_filtered=2` and `sales_summary_rows_filtered=2`, i.e., 2 summary rows were filtered out for compare/ingest row-count parity.

### A668

- Log API fetch counts: orders=755, sales=831, garments=6704.
- JSON counts: orders=757, sales=833, garments=6704.
- XLSX counts: orders=755, sales=831, garments=6704.
- Compare workbook summary: total_rows=755, matched_rows=755, missing_in_api=0, missing_in_ui=0, amount_mismatches=0, status_mismatches=0.

Interpretation:
- Orders and sales JSON files include +2 rows vs compare/XLSX/log counts.
- This lines up with log fields `orders_summary_rows_filtered=2` and `sales_summary_rows_filtered=2`.

## 3) One comprehensive DB query for count comparison

```sql
WITH params AS (
  SELECT
    '20260302_203724238635'::text AS run_id,
    ARRAY['A817','A668']::text[] AS stores,
    DATE '2025-12-04' AS from_date,
    DATE '2026-03-03' AS to_date
),
store_list AS (
  SELECT unnest((SELECT stores FROM params)) AS store_code
),
run_log_counts AS (
  SELECT
    osl.store_code,
    MAX(osl.primary_rows_downloaded) AS orders_rows_downloaded,
    MAX(osl.secondary_rows_downloaded) AS sales_rows_downloaded,
    MAX(osl.primary_rows_ingested) AS orders_rows_ingested,
    MAX(osl.secondary_rows_ingested) AS sales_rows_ingested,
    MAX(osl.status) AS orders_sync_status
  FROM orders_sync_log osl
  JOIN params p ON osl.run_id = p.run_id
  WHERE osl.store_code = ANY((SELECT stores FROM params))
  GROUP BY osl.store_code
),
orders_counts AS (
  SELECT
    o.store_code,
    COUNT(*) AS orders_table_rows
  FROM orders o
  JOIN params p ON TRUE
  WHERE o.store_code = ANY(p.stores)
    AND o.order_date::date BETWEEN p.from_date AND p.to_date
  GROUP BY o.store_code
),
sales_counts AS (
  SELECT
    s.store_code,
    COUNT(*) AS sales_table_rows
  FROM sales s
  JOIN params p ON TRUE
  WHERE s.store_code = ANY(p.stores)
    AND s.payment_date::date BETWEEN p.from_date AND p.to_date
  GROUP BY s.store_code
),
garment_counts AS (
  SELECT
    g.store_code,
    COUNT(*) AS order_line_items_rows,
    COUNT(*) FILTER (WHERE COALESCE(g.is_orphan, false)) AS orphan_line_items_rows
  FROM order_line_items g
  JOIN params p ON TRUE
  WHERE g.store_code = ANY(p.stores)
    AND g.order_date::date BETWEEN p.from_date AND p.to_date
  GROUP BY g.store_code
),
compare_counts AS (
  SELECT
    t.store_code,
    MAX(t.total_rows) AS compare_total_rows,
    MAX(t.matched_rows) AS compare_matched_rows,
    MAX(t.missing_in_api) AS compare_missing_in_api,
    MAX(t.missing_in_ui) AS compare_missing_in_ui,
    MAX(t.amount_mismatches) AS compare_amount_mismatches,
    MAX(t.status_mismatches) AS compare_status_mismatches,
    BOOL_OR(COALESCE(t.api_ready, false)) AS compare_api_ready
  FROM td_sync_compare_log t
  JOIN params p ON TRUE
  WHERE t.run_id = p.run_id
    AND t.store_code = ANY(p.stores)
    AND t.from_date = p.from_date
    AND t.to_date = p.to_date
  GROUP BY t.store_code
)
SELECT
  sl.store_code,
  p.run_id,
  p.from_date,
  p.to_date,
  r.orders_sync_status,
  r.orders_rows_downloaded,
  r.sales_rows_downloaded,
  r.orders_rows_ingested,
  r.sales_rows_ingested,
  o.orders_table_rows,
  s.sales_table_rows,
  g.order_line_items_rows,
  g.orphan_line_items_rows,
  c.compare_total_rows,
  c.compare_matched_rows,
  c.compare_missing_in_api,
  c.compare_missing_in_ui,
  c.compare_amount_mismatches,
  c.compare_status_mismatches,
  c.compare_api_ready
FROM store_list sl
CROSS JOIN params p
LEFT JOIN run_log_counts r ON r.store_code = sl.store_code
LEFT JOIN orders_counts o ON o.store_code = sl.store_code
LEFT JOIN sales_counts s ON s.store_code = sl.store_code
LEFT JOIN garment_counts g ON g.store_code = sl.store_code
LEFT JOIN compare_counts c ON c.store_code = sl.store_code
ORDER BY sl.store_code;
```
