# Payment Collections — Operations Notes and Business Logic

## Purpose

`payment_collections` stores manually recorded payment transactions shared by stores (for example via WhatsApp delivery/payment confirmations) and transcribed into Excel before SQL insertion.

This table is the verified payment evidence table for current payment reconciliation. It should remain append-first, traceable, and auditable.

## Data handling contract

### 1) Row identity and idempotency

- `source_sheet_row` remains non-null for every row.
- Use `(source_type, source_sheet_row)` as the ingestion idempotency key.
- Re-running inserts for the same source should not create duplicates.
- Prefer `INSERT ... ON CONFLICT (source_type, source_sheet_row) DO UPDATE` for correction workflows.

### 2) Required semantics

- `source_type` identifies the verified evidence source. For APNF (`Actual Payments Not Found`), Short Payment, and payment-proof reconciliation, only currently supported proof source types qualify: `source_type = 'google_sheet'` and `source_type = 'legacy_sales'` are equivalent verified payment evidence.
- `bank_row_id` is reserved for future bank-reconciliation work and is ignored by current business reports and payment reconciliation.
- `payment_timestamp` is the event timestamp captured from operator source.
- `payment_date` should match business-local date derived from `payment_timestamp` unless deliberately corrected.
- `cost_center` and `order_number` must match operational references used in downstream reconciliation.
- `order_number` may contain either one order number or a grouped payment list when one payment evidence row covers multiple orders. Grouped lists are canonical and may use `/` or `,` delimiters, for example `ORD1/ORD2` or `ORD1,ORD2`. Reconciliation splits these delimiters, normalizes each token, and then requires exact token equality; `ORD1` does not match `ORD10` unless `ORD1` appears as its own token.
- `amount` must be non-negative (`CHECK amount >= 0`).

### 3) Workflow fields

- `handed_over = false` means transaction is recorded but not handed to the next accounting/reconciliation stage.
- Set `date_handed` when `handed_over` transitions to `true`.
- `updated_flag = true` indicates a post-insert correction or enrichment happened.
- Set `date_modified` on every manual correction event.

### 4) Audit fields

- `created_at` is insert time (`now()`).
- `updated_at` should be refreshed by update workflows whenever business columns change.
- If no DB trigger exists, SQL correction scripts must set `updated_at = now()` explicitly.

## Current payment/recovery reconciliation contract

- Payment truth ignores CRM/order snapshot fields `orders.payment_status` and `orders.payment_amount`.
- Payment truth uses only:
  - `vw_orders.order_amount` as the canonical order value,
  - `sales.payment_received` as source sales/payment evidence where the report contract already uses sales collections, and
  - `payment_collections.amount` as verified manually captured payment evidence.
- Payment comparisons use tolerance `1` (₹1); overpayments count as paid in full.
- Multi-order `payment_collections.order_number` values are canonical grouped payment evidence. They are split on `/` and `,`, normalized, matched by exact order-token equality, and group-reconciled first. If the group total is paid within tolerance, those rows are excluded from the main missing/short payment outputs.
- Group-short rows are allocated sequentially by `order_date ASC, order_number ASC` before deciding which orders are short.
- `TO_BE_RECOVERED` and `TO_BE_COMPENSATED` are excluded from normal missing-payment rows.
- `RECOVERED`, `COMPENSATED`, and `WRITE_OFF` are excluded from normal pending-delivery buckets.
- `Actual Payments Not Found` is intentionally current/open across all order dates; Daily/MTD report windows do not restrict it. Eligibility requires a `sales` row and no valid qualifying payment proof.
- A separate `Short Payment` sub-report is required for underpaid orders; it must not be merged into `Actual Payments Not Found`.
- `Short Payment` is a current/open action list across all order dates. It behaves like `TO_BE_RECOVERED` by showing current unresolved action rows, and Daily/MTD report date windows do not restrict Short Payment eligibility.
- Short Payment still excludes `TO_BE_RECOVERED`, `TO_BE_COMPENSATED`, `RECOVERED`, `COMPENSATED`, `WRITE_OFF`, and zero-value orders.
- Short Payment requires clean reconciliation: (1) a `sales` row exists; (2) qualifying `payment_collections` proof exists; (3) `sales.payment_received` and proof/evidence amount match within ₹1; and (4) the proof/evidence amount is short against `vw_orders.order_amount` by more than ₹1.
- Show `source_type` in audit/reconciliation reports so analysts can trace evidence provenance. Do not add it to every normal business report by default.

## Daily Sales collections-target exception

- The Daily Sales top summary Target subsection can run with `TARGET_COMPUTE_TYPE = 'COLLECTIONS'`. In that mode, target achievement is computed from allocated `payment_collections.amount` for orders created in the report MTD window, scoped by matched `vw_orders.order_date`.
- This target computation is intentionally different from APNF, Short Payment, and payment-proof reconciliation: it ignores `payment_collections.payment_date` completely and also ignores `payment_collections.source_type`.
- Do not reuse APNF/Short Payment source-type filtering for the collections-target achievement query.
- For collections-target achievement, `source_type` remains audit/provenance data, not an eligibility filter.
- The order-date scope is the same report MTD window used by current sales MTD logic. A payment allocated to a prior-month order is excluded from current-month target achievement even if the payment row was captured in the current month. A payment allocated to a current-month order can count even if `payment_collections.payment_date` is outside the current month, because the metric is actual collections allocated to orders created in the report month.

## Recommended manual upsert pattern

```sql
INSERT INTO payment_collections (
    source_type,
    source_sheet_row,
    bank_row_id,
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
    date_handed,
    date_modified,
    updated_flag
)
VALUES (
    :source_type,
    :source_sheet_row,
    :bank_row_id,
    :payment_timestamp,
    :email_address,
    :payment_mode,
    :cost_center,
    :payment_date,
    :order_number,
    :amount,
    :remarks,
    :source_rowid,
    :handed_over,
    :date_handed,
    :date_modified,
    :updated_flag
)
ON CONFLICT (source_type, source_sheet_row)
DO UPDATE SET
    bank_row_id = EXCLUDED.bank_row_id,
    payment_timestamp = EXCLUDED.payment_timestamp,
    email_address = EXCLUDED.email_address,
    payment_mode = EXCLUDED.payment_mode,
    cost_center = EXCLUDED.cost_center,
    payment_date = EXCLUDED.payment_date,
    order_number = EXCLUDED.order_number,
    amount = EXCLUDED.amount,
    remarks = EXCLUDED.remarks,
    source_rowid = EXCLUDED.source_rowid,
    handed_over = EXCLUDED.handed_over,
    date_handed = EXCLUDED.date_handed,
    date_modified = EXCLUDED.date_modified,
    updated_flag = TRUE,
    updated_at = NOW();
```

## Data quality checks (suggested)

- Null/blank guardrails for `payment_mode`, `cost_center`, `order_number`.
- Duplicate scan for `(cost_center, order_number, amount, payment_date)` to catch accidental repeated rows from sheet edits.
- Weekly audit of rows where:
  - `handed_over = true` and `date_handed IS NULL`
  - `updated_flag = true` and `date_modified IS NULL`
  - `payment_date <> DATE(payment_timestamp)` (timezone/capture mismatch)

## Analysis input expectation

- Current analysis is expected from `docs/payment_collections.csv` export.
- If that file is absent, analysis should be deferred until the CSV is added at that exact path.
