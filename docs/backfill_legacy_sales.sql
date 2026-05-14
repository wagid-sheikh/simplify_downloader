ALTER TABLE public.payment_collections
ADD COLUMN source_type text;

UPDATE public.payment_collections
SET source_type = 'google_sheet'
WHERE source_type IS NULL;

ALTER TABLE public.payment_collections
ALTER COLUMN source_type SET NOT NULL;

ALTER TABLE public.payment_collections
ADD CONSTRAINT chk_payment_collections_source_type
CHECK (source_type IN ('google_sheet', 'legacy_sales'));

ALTER TABLE public.payment_collections
DROP CONSTRAINT IF EXISTS payment_collections_source_sheet_row_key;

DROP INDEX IF EXISTS uq_payment_collections_source_rowid;

ALTER TABLE public.payment_collections
ADD CONSTRAINT uq_payment_collections_source_type_row
UNIQUE (source_type, source_sheet_row);

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

UPDATE orders AS s
SET recovery_status = 'TO_BE_RECOVERED'
WHERE COALESCE(s.recovery_status, '') <> 'TO_BE_RECOVERED'
  AND EXISTS (
      SELECT 1
      FROM payment_collections AS pc
      WHERE pc.source_type = 'legacy_sales'
        AND pc.cost_center = s.cost_center
        AND pc.order_number = s.order_number
  );