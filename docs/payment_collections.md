# Payment Collections — Operations Notes and Business Logic

## Purpose

`payment_collections` stores manually recorded payment transactions shared by stores (for example via WhatsApp delivery/payment confirmations) and transcribed into Excel before SQL insertion.

This table is the source ledger for manually captured payment events and should remain append-first, traceable, and auditable.

## Data handling contract

### 1) Row identity and idempotency
- Use `source_sheet_row` as the ingestion idempotency key.
- Re-running inserts for the same spreadsheet should not create duplicates.
- Prefer `INSERT ... ON CONFLICT (source_sheet_row) DO UPDATE` for correction workflows.

### 2) Required semantics
- `payment_timestamp` is the event timestamp captured from operator source.
- `payment_date` should match business-local date derived from `payment_timestamp` unless deliberately corrected.
- `store_code` and `order_number` must match operational references used in downstream reconciliation.
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

## Recommended manual upsert pattern

```sql
INSERT INTO payment_collections (
    source_sheet_row,
    payment_timestamp,
    email_address,
    payment_mode,
    store_code,
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
    :source_sheet_row,
    :payment_timestamp,
    :email_address,
    :payment_mode,
    :store_code,
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
ON CONFLICT (source_sheet_row)
DO UPDATE SET
    payment_timestamp = EXCLUDED.payment_timestamp,
    email_address = EXCLUDED.email_address,
    payment_mode = EXCLUDED.payment_mode,
    store_code = EXCLUDED.store_code,
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
- Null/blank guardrails for `payment_mode`, `store_code`, `order_number`.
- Duplicate scan for `(store_code, order_number, amount, payment_date)` to catch accidental repeated rows from sheet edits.
- Weekly audit of rows where:
  - `handed_over = true` and `date_handed IS NULL`
  - `updated_flag = true` and `date_modified IS NULL`
  - `payment_date <> DATE(payment_timestamp)` (timezone/capture mismatch)

## Analysis input expectation
- Current analysis is expected from `docs/payment_collections.csv` export.
- If that file is absent, analysis should be deferred until the CSV is added at that exact path.
