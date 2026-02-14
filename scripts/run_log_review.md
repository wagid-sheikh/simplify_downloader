# Run Log Review (2026-02-14)

## Reviewed sources
- `scripts/run_log.txt`

## Issues identified and fixes applied

1. **UC archive publish failure on `orders.service_type` truncation**
   - Log evidence: `StringDataRightTruncationError` while updating `orders.service_type` with combined services string (`value too long for type character varying(64)`).
   - Fixes:
     - Added Alembic migration `0070_widen_orders_service_type_to_256.py` to widen `orders.service_type` to `VARCHAR(256)`.
     - Updated `orders` table metadata declarations to `sa.String(length=256)` in:
       - `app/crm_downloader/uc_orders_sync/ingest.py`
       - `app/crm_downloader/td_orders_sync/ingest.py`

2. **TD orders discovery was constrained to hard-coded stores**
   - Log evidence: repeated warnings: "Temporarily restricting TD orders discovery to a subset of stores" for `A817`/`A668`.
   - Fix:
     - Replaced hard-coded `TEMP_ENABLED_STORES` set with env-driven opt-in (`TD_ORDERS_TEMP_ENABLED_STORES`).
     - Default behavior now processes all eligible TD stores unless explicitly scoped via env.

## Follow-up verification to run in code mode
- Run the TD + UC orders sync flows and confirm:
  - no `archive_publish_orders` truncation warnings
  - no temporary TD store restriction warning unless env var is set intentionally
- Apply Alembic migrations before the next production run.
