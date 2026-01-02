## Phase 0: Project scaffolding (no business logic yet)

1. **Scaffold directories and entrypoints**
   * Add empty/skeleton modules:
     * `app/crm_downloader/td_orders_sync/__init__.py`
     * `app/crm_downloader/td_orders_sync/main.py` (stub orchestrator)
     * `app/crm_downloader/uc_orders_sync/__init__.py`
     * `app/crm_downloader/uc_orders_sync/main.py` (stub orchestrator)
     * `app/bank_sync/__init__.py`
     * `app/bank_sync/main.py` (stub orchestrator)
   * Add placeholder script entries (no logic):** **`scripts/run_local_td_orders_sync.sh`,** **`scripts/run_prod_td_orders_sync.sh`,** **`scripts/run_local_uc_sales_sync.sh`,** **`scripts/run_prod_uc_sales_sync.sh`,** **`scripts/run_local_bank_sync.sh`,** **`scripts/run_prod_bank_sync.sh`.
2. **Shared helper wiring**
   * In the stubs, import/reuse existing helpers (run_id/run_date generation, DB session context, logging, notification hooks) from the existing codebase without modifying** **`dashboard_downloader`.
   * No Playwright or ETL yet—just ensure imports and orchestration signatures are in place.

---

## Phase 1: TD Orders – session and frame discovery (no download)

3. **TD Orders: login/session reuse + iframe hydration detection**
   * Implement Playwright flow to:
     * Reuse storage_state if present; otherwise login.
     * Verify post-login URL contains store_code.
     * Navigate to Orders report container; enter** **`iframe#ifrmReport` via** **`frameLocator`.
     * Wait for hydration (spinner disappear or key control visible).
   * Add rich logging of observed selectors/texts/spinner cues/date inputs to guide next step.
   * No date selection/download yet.

---

## Phase 2: TD Orders – date range and download

4. **TD Orders: date selection, request polling, download**
   * Using discovered selectors, set from/to dates, submit request, poll Report Requests table for** **`DD Mon YYYY - DD Mon YYYY`, click matching row, capture download.
   * Save as** **`{store_code}_td_orders_{YYYYMMDD-from}_{YYYYMMDD-to}.xlsx`.
   * Add retry/timeouts per DASHBOARD_DOWNLOAD_NAV_TIMEOUT.

---

## Phase 3: TD Orders – ingestion

5. **TD Orders: parse Excel → stg_td_orders → orders**
   * Parse Orders Excel with validation (dates, numerics, phone normalization).
   * Upsert into** **`stg_td_orders` on (store_code, order_number, order_date); then into** **`orders` on (cost_center, order_number, order_date) with mapping rules.
   * Per-store logging + notifications (ok/warning/error).

---

## Phase 4: TD Sales – session/frame discovery

6. **TD Sales: login reuse + iframe hydration detection**
   * Reuse session; navigate to Sales & Delivery report; enter iframe; log selectors/spinner/date controls (no download yet).

---

## Phase 5: TD Sales – date range, download, ingestion

7. **TD Sales: date selection, download, ingest**
   * Select dates, request report, download** **`{store_code}_td_sales_{YYYYMMDD-from}_{YYYYMMDD-to}.xlsx`.
   * Parse/validate, upsert** **`stg_td_sales` on (store_code, order_number, payment_date) →** **`td_sales` on (cost_center, order_number, payment_date).
   * Logging + notifications.

---

## Phase 6: UC GST – session/frame discovery

8. **UC GST: login reuse, navigate GST Report, hydration detection**
   * Session reuse/login; navigate to GST Report; identify “Start Date”/“End Date”, “Apply”, “Export Report”; log selectors/spinner cues (no download yet).

---

## Phase 7: UC GST – date range, download, ingestion

9. **UC GST: date selection, export, ingest**
   * Set dates, apply, wait for data load, export to** **`{store_code}_uc_gst_{YYYYMMDD-from}_{YYYYMMDD-to}.xlsx`.
   * Parse →** **`stg_uc_orders` on (store_code, order_number, invoice_date) →** **`orders` on (cost_center, order_number, order_date=invoice_date).
   * Logging + notifications.

---

## Phase 8: Bank sync

10. **Bank ingest**
    * Detect new** **`*bank.xlsx`, parse with validation, upsert** **`stg_bank` on row_id →** **`bank` on row_id.
    * Move processed files to** **`app/bank_sync/data/ingested` with** **`_processed_{RUN_ID}` suffix.
    * Logging + notifications.

---

## Phase 9: Scripts and orchestration polish

11. **Runner scripts**
    * Implement the shell scripts with** **`--from-date/--to-date` defaults to today (PIPELINE_TIMEZONE), logging, run_id handling, patterned after dashboard_downloader scripts.

---

## Phase 10: Docs update with discovered selectors

12. **Doc runbook**
    * Update** **`docs/crm-sync-pipeline.md` with final selectors, fallback patterns, quirks, and operational notes discovered during Playwright work.

---

### How we’ll execute

* Start with Phase 0 scaffolding PR (tiny, no behavior change).
* Then Phase 1 PR (TD Orders discovery). You run it, share observed selectors/spinner cues; I’ll adapt for Phase 2.
* Proceed sequentially through phases, keeping each PR small and isolated to the new directories (`app/crm_downloader`,** **`app/bank_sync`, scripts), with no changes to existing pipelines.

If this plan looks good, I’ll proceed with the Phase 0 scaffolding PR first.
