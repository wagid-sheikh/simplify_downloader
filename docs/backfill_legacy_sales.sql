INSERT INTO public.payment_collections (
    source_sheet_row,
    payment_timestamp,
    email_address,
    payment_mode,
    cost_center,
    payment_date,
    order_number,
    amount,
    remarks,
    source_rowid,
    handed_over,
    source_type
)
SELECT
    s.id,
    s.payment_date,
    'wagid.sheikh@gmail.com',
    'Package',
    s.cost_center,
    s.payment_date::date,
    s.order_number,
    COALESCE(s.payment_received, 0),
    'Backfill Legacy Package Data',
    s.id::text,
    false,
    'legacy_sales'
FROM sales AS s
WHERE LOWER(TRIM(s.payment_mode)) = 'package'
  AND COALESCE(s.payment_received, 0) >= 0
  AND NOT EXISTS (
      SELECT 1
      FROM public.payment_collections pc
      WHERE pc.cost_center = s.cost_center
        AND pc.order_number = s.order_number
        AND LOWER(TRIM(pc.payment_mode)) = 'package'
  )
ON CONFLICT (source_type, source_sheet_row) DO NOTHING;

INSERT INTO public.payment_collections (
    source_type,
    source_sheet_row,
    payment_timestamp,
    email_address,
    payment_mode,
    cost_center,
    payment_date,
    order_number,
    amount,
    remarks,
    source_rowid,
    handed_over
)
SELECT
    'legacy_sales',
    s.id,
    s.payment_date,
    'wagid.sheikh@gmail.com',
    'FranchiseUPI',
    s.cost_center,
    s.payment_date::date,
    s.order_number,
    COALESCE(s.payment_received, 0),
    'Backfill Legacy Data',
    s.id::text,
    false
FROM sales AS s
WHERE s.transaction_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM public.payment_collections pc
      WHERE pc.cost_center = s.cost_center
        AND pc.order_number = s.order_number
  )
ON CONFLICT (source_type, source_sheet_row) DO NOTHING;