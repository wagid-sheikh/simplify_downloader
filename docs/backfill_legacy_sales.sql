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
SET recovery_status = 'TO_BE_RECOVERED', recovery_category='OTHER', recovery_notes='Backfill Legacy Data'
WHERE COALESCE(s.recovery_status, '') <> 'TO_BE_RECOVERED'
  AND EXISTS (
      SELECT 1
      FROM payment_collections AS pc
      WHERE pc.source_type = 'legacy_sales'
        AND pc.cost_center = s.cost_center
        AND pc.order_number = s.order_number
  );
-- Mark TumbleDry / BlinkIt as paid
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
WHERE NOT EXISTS (
      SELECT 1
      FROM public.payment_collections pc
      WHERE pc.cost_center = s.cost_center
        AND pc.order_number = s.order_number
  )
AND EXISTS (
  select 1
    from orders o
   where o.cost_center = s.cost_center
     and o.order_number = s.order_number
     and upper(o.customer_name) like '%TUMBLE%'
)
ON CONFLICT (source_type, source_sheet_row) DO NOTHING;

-- Mark all orders as paid where the order exists in orders & sales table (that means store has madked them paid in year 2025)
BEGIN;
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
    CASE
        WHEN TRIM(s.payment_mode) IN ('CASH', 'Cash') THEN 'Cash'

        WHEN TRIM(s.payment_mode) IN (
            'BharatPe',
            'Cheque/Bank',
            'Credit Card/Debit Card',
            'Customer Advance',
            'DEBIT_CREDIT_CARD',
            'Google Pay',
            'Paytm',
            'PhonePe',
            'UPI',
            'WhatsApp pay'
        ) THEN 'UPI'

        WHEN s.payment_mode IS NULL OR TRIM(s.payment_mode) = '' THEN 'Other'

        ELSE 'Other'
    END AS payment_mode,
    s.cost_center,
    s.payment_date::date,
    s.order_number,
    COALESCE(s.payment_received, 0),
    'Backfill Legacy Data 01-Jan-2025 to 31-Dec-2025',
    s.id::text,

    CASE
        WHEN TRIM(s.payment_mode) IN ('CASH', 'Cash') THEN true
        ELSE false
    END AS handed_over

FROM sales AS s
WHERE NOT EXISTS (
      SELECT 1
      FROM public.payment_collections pc
      WHERE pc.cost_center = s.cost_center
        AND pc.order_number = s.order_number
)
AND EXISTS (
      SELECT 1
      FROM orders o
      WHERE o.cost_center = s.cost_center
        AND o.order_number = s.order_number
        AND o.order_date >= DATE '2025-01-01'
        AND o.order_date <  DATE '2026-01-01'
)
ON CONFLICT (source_type, source_sheet_row) DO NOTHING;

SELECT
    payment_mode,
    handed_over,
    COUNT(*) AS inserted_rows,
    SUM(amount) AS inserted_amount
FROM public.payment_collections
WHERE source_type = 'legacy_sales'
  AND remarks = 'Backfill Legacy Data 01-Jan-2025 to 31-Dec-2025'
GROUP BY payment_mode, handed_over
ORDER BY payment_mode, handed_over;

commit;


-- Marked as TO_BE_RECOVERED, but has sales but does not have payment_collections
select * from orders where recovery_status='TO_BE_RECOVERED'
  and exists (
  select 1 from sales
      where sales.cost_center = orders.cost_center
        and sales.order_number = orders.order_number
  )
  and not exists
  (
     select 1 from payment_collections pc
      where pc.cost_center = orders.cost_center
        and pc.order_number = orders.order_number
  )
order by orders.cost_center, orders.order_date,  orders.order_number;
