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
   * Reuse the same TD Orders session/context (no fresh login); navigate to Sales & Delivery report; enter iframe; log selectors/spinner/date controls (no download yet).
   * Assume the Sales download link follows the same underline-link pattern proven for TD Orders; reuse that locator strategy unless new evidence is found.

---

## Phase 5: TD Sales – date range, download, ingestion

7. **TD Sales: date selection, download, ingest**
   * Select dates, request report, download** **`{store_code}_td_sales_{YYYYMMDD-from}_{YYYYMMDD-to}.xlsx`.
   * Parse/validate, upsert** **`stg_td_sales` on (store_code, order_number, payment_date) →** **`sakes` on (cost_center, order_number, payment_date).
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

## Production readiness blockers (UC windows)

13. **Fix missing UC windows in orders_sync_run_profiler**

    * Reproduce a profiler run with UC567/UC610 and **capture the generated window plan** (from/to ranges) in logs plus a persisted artifact (JSON or CSV) for comparison against expected coverage.
    * Trace where the UC window list is trimmed or skipped (planner, retry loop, exit conditions). Add logging that explicitly states **why** any UC window is skipped or dropped.
    * Validate date-boundary math: inclusive/exclusive edges, overlap behavior, and “end_date” alignment so the **final window always reaches the run’s end date**.
    * Add a regression test (or deterministic dry-run harness) that asserts **no missing windows** for UC567/UC610 across a full-range run.
    * Update the profiler summary to fail loudly (status=failed) if any UC window is missing, so this cannot silently ship.
14. **Eliminate UC partial status (GST export shows row_count=0)**

    * Capture DOM snapshots and logging when GST rows are missing after Apply (selectors, timing, network idle vs spinner waits, visible row count).
    * Compare against a manual run to confirm whether **Apply is required** or if the export button is decoupled from row rendering; ensure row detection matches the DOM structure used by the GST table.
    * Implement a resilient readiness check: wait for a **positive data signal** (row count > 0 or “no data” banner) before exporting, and treat “no data” as a distinct success case with explicit status/notes.
    * If export succeeds but rows are 0, confirm the downloaded file contents (row counts in workbook) and align status with file reality.
    * Add targeted retries for the Apply + table refresh sequence and log a structured “row-detection failure” reason when attempts are exhausted.

### How we’ll execute

* Start with Phase 0 scaffolding PR (tiny, no behavior change).
* Then Phase 1 PR (TD Orders discovery). You run it, share observed selectors/spinner cues; I’ll adapt for Phase 2.
* Proceed sequentially through phases, keeping each PR small and isolated to the new directories (`app/crm_downloader`,** **`app/bank_sync`, scripts), with no changes to existing pipelines.

If this plan looks good, I’ll proceed with the Phase 0 scaffolding PR first.




## All questions I have before implementation

### A) Auth / session / routing

1. Are API calls guaranteed to work using the same browser-authenticated cookies/session after login, with no extra headers (e.g.,** **`Authorization`,** **`x-api-key`, CSRF token)? [I guess yes, we can write a seperate section of code and test this]
2. Do we have any store-specific host variation, or always** **`https://store.ucleanlaundry.com`? [NO]
3. Is** **`franchise=UCLEAN` always constant for all stores you process? [YES]

### B) Pagination contract

4. Can we trust** **`pagination.totalPages` and** **`pagination.total` as authoritative? [yes]
5. Is** **`limit=30` fixed, or can/should we use higher values? [let's keep 30]
6. Any observed duplicates across pages for same date window? [can not say that]
7. Is ordering stable (newest first) and deterministic for a fixed date range? [can not say what ordering is implemented by their backend]
8. Should we stop by** **`totalPages`, by empty** **`data`, or by both safeguards? [yes]

### C) Date filtering semantics

9. Is** **`dateType=delivery` always the intended business filter for archive sync? [yes]
10. Should** **`startDate/endDate` be inclusive in business terms (and is timezone IST on backend)? [yes]
11. Any cases where API ignores** **`dateRange=custom` and returns default page set? [once you reach archive page, then their system automatically pulls 30 records without any date range]

### D) Data mapping (critical)

12. `payment_details` is a JSON string, not object array — should we treat parse failures as warning + keep base row, or hard-fail the order? [payment_details can be converted to our needed object. If an order does not have payment, then this payment_details is null]
13. For multi-payment orders (e.g., UC610-0769), should we emit one payment row per payment entry exactly as-is? [for multi payment we need all rows that will go to our payment details, 1:UPI, 4:Cash, any other pament mode: "Others"]
14. Confirm your mapping:** **`payment_mode=1 => UPI`,** **`payment_mode=4 => Cash`; do we have a full enum (2/3/5/etc.)? [2: Debit/Credit Card, 3: Bank Transfer]
15. If payment mode is unknown code, preferred normalized value:** **`UNKNOWN`? [It should be marked as: UNKNOWN]
16. Should amount rounding follow API value exactly, or follow existing Decimal normalization and two-decimal storage? [Let's pull the raw data we get and save in our excels]
17. For base row status, API gives numeric** **`status: 7`; do you want numeric or mapped text (`DELIVERED`)? [Text: Delivered, 0=Cancel, and any other value: Unknown]

### E) Order details source

18. Is** **`generateInvoice/{id}` always available for every delivered booking? [Yes]
19. Can** **`generateInvoice` return different HTML templates by service/store? [By Service it may have more lines items]
20. Should we parse order details from invoice HTML only, or use list API fields where possible and invoice only for line items? [only possible from HTML]
21. For orders with multiple services + multiline items (like UC610-0759), do you want one row per item line (current behavior) or one row per service block? [one row per item]

### F) Reliability / fallback behavior

22. Do you want UI scraping fallback if API fails, or fail-fast and retry run later? [No fallback to UI method]
23. Retry policy preference for API errors (e.g., 429/5xx): max retries + backoff? [3 max retries]
24. Should partial extraction still be allowed if some invoice/detail calls fail but list pages succeed? [yes]
25. Is it acceptable to proceed with base + payment even if some order_details are unavailable? [yes]

### G) Operational / observability

26. Should logs include API page progress (`page/totalPages`, rows fetched, parse failures) replacing current footer-based telemetry? [yes]
27. Do you want a validation metric:** **`api_total` vs extracted base rows mismatch alerts? [yes]
28. Should we preserve existing output XLSX files exactly (column names/order), or are we free to bypass files and ingest directly (I recommend preserving first for low-risk cutover)? [preserve excel files, data from API will be dumped in Excel and then from there it will be ingested into staging tables & then final tables.]
29. Do you want a feature flag rollout (e.g.,** **`UC_ARCHIVE_USE_API=true`) per store or global?[Global]
30. Do you want side-by-side shadow mode for a few runs (UI + API diff) before full switch? [no]

Essentially regarding API driven data fetch and answers to your queries above. For testing prposes I want you develop a new code file which is totally using API driven data fetch. So in our current flow: We login, go dashboard, go GST report->download & ingest, then we go to Archive report page [& this is the time instead of UI driven data fetch, you go ahead call code from a new file and excute it. Once testing is successfull, then we will fully replce UI path for Archive ORders fetch]

---

## Task stub: UC archive API-path stabilization and warning cleanup

### Goal

Create and execute a focused fix set for UC Archive API ingestion/publish so recent warning/failure patterns are resolved while preserving the current flow (GST UI download/ingest first, then archive via API file path).

### Scope

1. **Increase `orders.service_type` length to 64**

   * Add DB migration to widen `orders.service_type` from `varchar(24)` to `varchar(64)`.
   * Ensure ORM/model metadata reflects the same max length.
   * Add/adjust regression test to prove long values (e.g., "Dry cleaning, Laundry - Wash & Fold") publish without truncation error.

2. **Stop false warnings for `invalid_amount` and `invalid_weight` in `ingest_remarks`**

   * In UC archive ingest rules, treat null/blank amount and weight as acceptable input.
   * Normalize null/blank amount and weight to `0` (preserve existing numeric type handling).
   * Suppress `invalid_amount` / `invalid_weight` ingest remarks for these null/blank cases.
   * Keep warnings/errors only for truly malformed non-null values.

3. **Relax `missing_required_field:item_name` for service-driven rows**

   * Update UC archive detail validation so `item_name` can be null/"-" when service context is laundry-type (e.g., `service_type` like `Laundry%`).
   * Remove false `missing_required_field:item_name` remarks for these accepted cases.
   * Preserve required-field warnings for service types where `item_name` should remain mandatory.

### Acceptance criteria

* Archive order publish no longer fails on `service_type` length for known long service labels.
* Ingest remarks no longer emit false `invalid_amount` / `invalid_weight` for null inputs that are converted to 0.
* Ingest remarks no longer emit false `missing_required_field:item_name` for accepted laundry-service rows with null/"-" item names.
* Existing UC archive output files and API-driven archive flow remain unchanged in structure and sequence.

### Deliverables

* Alembic migration (service_type widen).
* Code changes in UC archive ingest/publish validators and mappings.
* Tests covering all three fixes.
* Run-log evidence snippet from a successful validation run.
