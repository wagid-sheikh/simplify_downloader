# Feature Map

Practical map of where to work for major capabilities.

## 1) Runtime entrypoint and command routing

- **Purpose:** Unified app/server/pipeline CLI entry.
- **Primary paths:**
  - `app/__main__.py`
  - `app/dashboard_downloader/cli.py`
- **Related pieces:** `scripts/*.sh`, `.github/workflows/*.yml`
- **Notes/Risks:** Backward compatibility for existing script invocations matters.

## 2) Configuration and secret materialization

- **Purpose:** Build validated runtime config from env + DB + encrypted values.
- **Primary paths:**
  - `app/config.py`
  - `app/crypto.py`
  - `tests/test_config.py`
- **Related dependencies:** `system_config` table, `SECRET_KEY`.
- **Notes/Risks:** High blast radius; changes can break all pipelines at startup.

## 3) Dashboard CSV download + merge + ingest

- **Purpose:** Single-session per-store download, merged bucket processing, upsert ingestion.
- **Primary paths:**
  - `app/dashboard_downloader/run_downloads.py`
  - `app/dashboard_downloader/pipeline.py`
  - `app/dashboard_downloader/config.py`
  - `app/common/ingest/{schemas.py,models.py,service.py}`
  - `app/common/{audit.py,cleanup.py}`
- **Related tests:** `tests/dashboard_downloader/*`, `tests/crm_downloader/*` (shared contracts).
- **Notes/Risks:** Dedupe and coercion behavior impacts data quality and audit counts. `repeat_customers` rows with missing, blank, malformed, or invalid normalized `mobile_no` values are silently skipped and excluded from repeat-customer reporting until source correction. These exclusions do not raise dashboard warnings or operator-facing notification text; any retained informational telemetry is aggregate-only and must not expose customer-sensitive values.

## 4) Store dashboard summary persistence

- **Purpose:** Persist daily dashboard KPI snapshot per store.
- **Primary paths:**
  - `app/common/dashboard_store.py`
- **Related tables:** `store_master`, `store_dashboard_summary`.
- **Notes/Risks:** `store_code` normalization and upsert semantics are critical. `store_master.start_date` is the canonical store-level start/launch date; dashboard `Launch Date` values are persisted there and existing non-null values must not be casually overwritten because CRM/order-sync uses them as lower bounds.

## 5) TD orders sync

- **Purpose:** Pull TD orders/sales (UI/API modes), ingest, reconcile, summarize.
- **Primary paths:**
  - `app/crm_downloader/td_orders_sync/main.py`
  - `app/crm_downloader/td_orders_sync/{ingest.py,sales_ingest.py,garment_ingest.py,td_api_client.py,td_api_compare.py}`
- **Related tables/logging:** `orders_sync_log`, run summaries, notification payloads.
- **Notes/Risks:** Module is large and stateful; source-mode and compare gating need careful regression testing.

## 6) UC orders sync

- **Purpose:** Extract UC GST/archive orders, ingest staging/final data, publish outputs.
- **Primary paths:**
  - `app/crm_downloader/uc_orders_sync/main.py`
  - `app/crm_downloader/uc_orders_sync/{ingest.py,archive_ingest.py,archive_api_extract.py,gst_api_extract.py,gst_publish.py,extract_comparator.py}`
- **Related docs:** `docs/uc_orders_sync_runbook.md`, `docs/uc_page_htmls/*`.
- **Notes/Risks:** Appears to include fallback/legacy selector paths; validate current production path before refactors.

## 6.1) TD leads sync actionable summaries

- **Purpose:** Scrape TD lead buckets, upsert leads, and emit both aggregate counts and capped actionable lead change rows.
- **Primary paths:**
  - `app/crm_downloader/td_leads_sync/{main.py,ingest.py}`
  - `app/reports/daily_sales_report/{data.py,templates/daily_sales_report.html}`
  - `app/dashboard_downloader/notifications.py`
- **Operational behavior:** Lead-change rows are grouped (created/updated/transitions), deduped by stable lead identity, capped per group, and report `overflow_count` for truncated rows.
- **Section semantics:** Open leads are backlog-style (carry forward until closure), while cancelled/completed cohorts are day-event-style (counted on event day). Completed reconciliation matches on `orders.store_code + orders.mobile_number` and includes `orders.order_number` in the reported output.

## 6.2) Order amount and payment decision contract

- **Purpose:** Keep source synchronization fields separate from business reporting/payment-decision values.
- **Canonical business amount:** Use `vw_orders.order_amount` for reports, payment status decisions, recovery action decisions, and user-facing totals/labels. Labels should say `Order Amount`.
- **Raw source fields:** `orders.net_amount`, `orders.gross_amount`, and `orders.adjustment` are source/ingest fields. TD/UC ingest and sync code may continue to use them when synchronizing CRM data, reconciling source payloads, or auditing raw imported values.
- **Prohibited path:** Direct report reads from `orders` are prohibited unless explicitly approved for a documented exception; report queries should go through `vw_orders` for order value semantics.
- **Payment evidence:** `payment_collections` is the verified payment evidence table. APNF, Short Payment, and payment-proof reconciliation use qualifying proof semantics: only currently supported proof `source_type` values (`google_sheet` and `legacy_sales`) qualify as current payment proof, and they are equivalent for current reconciliation; `bank_row_id` is future bank-reconciliation plumbing and ignored by current reports. Idempotency is `(source_type, source_sheet_row)`, with `source_sheet_row` non-null.
- **Payment proof matching:** `payment_collections.order_number` may be a single token or a comma/slash-delimited group. Reconciliation removes whitespace, uppercases tokens, and matches whole normalized tokens within the same `cost_center`; substring matches do not qualify. Unsupported source types, blank/malformed/unmatched tokens, and different-cost-center rows are audit evidence only and do not satisfy report proof.
- **Payment truth:** Ignore `orders.payment_status` and `orders.payment_amount`; use `vw_orders.order_amount`, `sales.payment_received`, and `payment_collections.amount`.
- **Payment rules:** Compare payments to `vw_orders.order_amount` with tolerance `1` (₹1); overpayments are paid in full. Zero-value orders remain visible in descriptive order reports but are excluded from missing-payment, pending-payment, and recovery action checks. Multi-order `payment_collections.order_number` values are group-reconciled first; group-paid rows are excluded from main missing/short outputs, and group-short rows allocate sequentially by `order_date ASC, order_number ASC`.
- **Daily Sales target mode:** Operators can switch only the top summary table's Target subsection with DB `system_config.TARGET_COMPUTE_TYPE`: missing/blank/invalid values use `SALES`; `sales` renders subsection header `Target`, uses `cost_center_targets.sale_target` as `Target`, uses current-MTD `sum(vw_orders.order_amount)` as `Achieved`, and calculates `TTD`, `Delta`, and `Reqd/Day` from sales achieved. `collection`/`collections` renders subsection header `Target (actual collections)`, uses `cost_center_targets.collection_target` as `Target`, uses allocated verified collections for orders created in the report MTD window as `Achieved`, and calculates `TTD`, `Delta`, and `Reqd/Day` from collections achieved. Child column headers remain unchanged in both modes: `Target`, `Achieved`, `TTD`, `Delta`, `Reqd/Day`. The `COLLECTIONS` target-achievement query intentionally ignores `payment_collections.payment_date` and `payment_collections.source_type`; do not reuse APNF/Short Payment source-type filtering there because `source_type` is audit/provenance data for this target computation, not an eligibility filter. Achievement is scoped by matched `vw_orders.order_date`, using the same report MTD order-date window as current sales MTD logic: prior-month orders are excluded even when their payment rows were captured in the current month, and current-month orders can count even when `payment_collections.payment_date` is outside the current month. Notification subjects/bodies/templates and the visible Collections FTD/MTD/LMTD columns do not change.
- **Report behavior:** `TO_BE_RECOVERED` and `TO_BE_COMPENSATED` are excluded from normal missing-payment rows. `RECOVERED`, `COMPENSATED`, and `WRITE_OFF` are excluded from normal pending-delivery buckets. `Actual Payments Not Found` means no valid qualifying payment proof exists for normalized `(cost_center, order_number)` under the source-type and whole-token rules; it is not a raw physical-row absence check. A valid qualifying proof with an amount mismatch is handled by Short Payment or audit reconciliation instead. `Actual Payments Not Found` is current/open across all order dates, requires a `sales` row, and is not restricted by Daily/MTD report windows. A separate `Short Payment` sub-report is required and distinct from `Actual Payments Not Found`; it is intentionally a current/open action list across all order dates, behaves like `TO_BE_RECOVERED` by showing current unresolved action rows, and is not restricted by Daily/MTD report date windows. `To Be Recovered` is current/open across all order dates for rows currently in recovery workflow statuses (for example `TO_BE_RECOVERED` and `TO_BE_COMPENSATED`). Short Payment still excludes `TO_BE_RECOVERED`, `TO_BE_COMPENSATED`, `RECOVERED`, `COMPENSATED`, `WRITE_OFF`, and zero-value orders. Short Payment requires clean reconciliation: (1) a `sales` row exists; (2) qualifying `payment_collections` proof exists; (3) `sales.payment_received` and proof/evidence amount match within ₹1; and (4) the proof/evidence amount is short against `vw_orders.order_amount` by more than ₹1. Python `app.reports.shared.payment_reconciliation` is canonical for report reconciliation; `vw_orders_missing_in_payment_collections` is a compatibility/audit projection of the Python missing-proof subset. Show `source_type` in audit/reconciliation reports, not every normal business report.
- **Daily Sales attachments:** Daily Sales notification includes the core PDF plus supplemental artifacts recorded for the run (`daily_sales_short_payments_pdf`, `daily_sales_actual_payments_not_found_pdf`, and `daily_sales_actual_payments_not_found_xlsx`, plus `mtd_same_day_fulfillment_pdf` when available). The APNF workbook is grouped by `cost_center` worksheet and sorted by `order_date DESC` within each sheet.
- **Daily Sales integrity:** The core Daily Sales PDF validates each active cost center's descriptive FTD order-number population against count and `vw_orders.order_amount` sum, detects duplicate FTD order numbers, flags suspicious zero-value populations, and validates aggregate totals. Structured findings are stored in run metrics and included concisely in notification summary context. Hard errors deliver a visibly marked `INVALID DATA REPORT`; they do not silently suppress notification delivery.
- **Payment evidence audit surface:** `vw_payment_evidence_reconciliation` and `scripts/payment_evidence_review.py` expose audit-only payment evidence rows with `payment_id`, `source_type`, `source_sheet_row`, original and normalized order tokens, evidence amount, `vw_orders.order_amount`, `sales.payment_received`, grouped classification, `bank_row_id`, `operator_actionable_payment_status`, and filters for source type, cost center, payment date range, and grouped rows. This is not the operator Daily Sales Short Payments PDF; raw `reconciliation_result` classifications can appear with recovery statuses for diagnostics.
- **Primary affected paths:**
  - `app/reports/**`
  - `app/crm_downloader/**` ingest/sync modules when deciding whether usage is source synchronization vs business decision logic
  - SQL/views/migrations that define or consume `vw_orders`

## 6.3) Customer retention input processing

- **Purpose:** Discover returned customer-followup workbooks and external lead imports, ingest them once, and archive processed files.
- **Primary paths:**
  - `app/customer_retention/input_discovery.py`
  - `app/customer_retention/pipeline.py`
  - `tests/customer_retention/test_phase2_ingestion.py`
- **Operational behavior:** Archive semantics are move-and-remove, not copy-and-retain. After a workbook or external import file is processed successfully and archive metadata is written, the source file is moved under `archive/customer_followup/`; repeated pipeline runs should not rediscover the same physical input file.
- **Notes/Risks:** Do not change this to copy semantics unless discovery is also made metadata/digest-aware for already-processed files; otherwise retained inputs will be reprocessed every run.

## 7) Orders sync run profiler (window orchestrator)

- **Purpose:** Run TD/UC sync in date windows, aggregate status, detect missing windows, notify.
- **Primary paths:**
  - `app/crm_downloader/orders_sync_run_profiler/main.py`
  - `app/crm_downloader/orders_sync_window.py`
- **Dependencies:** `orders_sync_log`, `pipeline_run_summaries`, notification profiles.
- **Notes/Risks:** Concurrency + retry + status rollups can produce subtle operational edge cases.


## 7.1) `order_line_items` historical rebuild

- **Purpose:** Operator-triggered, source-authoritative rebuild of `order_line_items` for historical TD/UC windows without SQL-only deduplication.
- **Primary paths:**
  - `app/crm_downloader/order_line_items_rebuild.py`
  - `app/crm_downloader/td_orders_sync/garment_ingest.py`
  - `app/crm_downloader/uc_orders_sync/gst_publish.py`
  - `scripts/run_local_order_line_items_rebuild.sh`
- **CLI:** Small dry-run smoke test: `poetry run python -m app crm rebuild-order-line-items --source both --from-date YYYY-MM-DD --to-date YYYY-MM-DD --window-days 7 --dry-run`. Full first live rebuild: `poetry run python -m app crm rebuild-order-line-items --source both` (or `--fresh`/`--ignore-progress` to make that intent explicit). Interrupted live recovery only: `poetry run python -m app crm rebuild-order-line-items --source both --resume`; stricter recovery can add `--resume-run-id PRIOR_RUN_ID`. Date/window aliases are `--start-date`/`--from-date`, `--end-date`/`--to-date`, and `--window-size`/`--window-days`; store scope can be pinned with `--stores ...` (alias: `order-line-items-rebuild`).
- **Notes/Risks:** Operators run one full-range command for the live rebuild; the rebuild internally expands the range into CRM-safe windows capped at 30 days unless a lower source/store limit applies. If `--start-date` is omitted, the start comes from `store_master.start_date`; if `--end-date` is omitted, the end comes from the current pipeline date; if window size is omitted, the command resolves CRM-safe source windows capped at 30 days. Only `complete_with_rows` and `complete_empty` snapshots replace local rows; `incomplete_or_failed` preserves existing rows. Structured checkpoints are emitted for dry and live runs; `order_line_items_rebuild_progress` is the live-run resume contract only, with current dry runs not writing progress and legacy dry-run progress ignored by resume. `--resume` is source/store/window based, not tied to the current run ID; start logs make that explicit, skipped-window logs include prior run metadata and metric counts, and `--resume-run-id` narrows recovery to one prior run. `--resume` is for interrupted live rebuilds, not the default first live run. The command reports missing windows after the run. TD Over Due Popup windows are operator-visible resumable skips (`orders_overdue_popup_blocked`) rather than fatal missing windows when all other selected windows complete; operators should ask store staff to clear overdue orders and rerun with `--resume` to retry those windows. The command depends on valid CRM auth/session state for live source fetching.

## 8) Daily/weekly/monthly/pending reporting

- **Purpose:** Generate PDFs and persist/send report artifacts.
- **Daily Sales target mode:** `TARGET_COMPUTE_TYPE` is read from `system_config`. Missing or invalid values default to `SALES`; accepted values are case-insensitive: `sales` => `SALES`, and `collection` / `collections` => `COLLECTIONS`. In `SALES` mode, the Target subsection title remains `Target`, targets use `cost_center_targets.sale_target`, achievement uses current-MTD `sum(vw_orders.order_amount)`, and `TTD`, `Delta`, and `Reqd/Day` use sales achieved. In `COLLECTIONS` mode, the subsection title is `Target (actual collections)`, targets use `cost_center_targets.collection_target`, achievement uses allocated verified collections for orders created in the report MTD window, and `TTD`, `Delta`, and `Reqd/Day` use collections achieved. Child column headers remain unchanged in both modes: `Target`, `Achieved`, `TTD`, `Delta`, `Reqd/Day`. This target computation is different from APNF/Short Payment proof checks: it ignores `payment_collections.payment_date` completely and also ignores `payment_collections.source_type`, treats `source_type` as audit/provenance data rather than an eligibility filter, does not change the visible Collections FTD/MTD/LMTD columns, and allocates grouped `payment_collections.order_number` rows to older orders first by `vw_orders.order_date ASC, order_number ASC` before counting only current-MTD order allocations. The current-MTD scope is the same report MTD order-date window used by current sales MTD logic: prior-month orders are excluded even if payment rows were captured in the current month, and current-month orders can count even if `payment_collections.payment_date` is outside the current month.
- **Primary paths:**
  - `app/reports/daily_sales_report/*`
  - `app/reports/mtd_same_day_fulfillment/*`
  - `app/reports/pending_deliveries/*`
  - `app/dashboard_downloader/run_store_reports.py`
  - `app/dashboard_downloader/pipelines/{dashboard_weekly.py,dashboard_monthly.py,reporting.py}`
  - `app/dashboard_downloader/templates/*`
- **Cron orchestration tail order (production wrapper):**
  1. `scripts/run_local_reports_daily_sales.sh`
  2. `scripts/run_local_reports_mtd_same_day_fulfillment.sh`
  3. `scripts/run_local_reports_pending_deliveries.sh`
- **Cron regeneration:** Cron report generation always regenerates Daily Sales, MTD Same-Day Fulfillment, and Pending Deliveries by passing `--force`; retries/rescue passes preserve mandatory regeneration and log `pipeline`, `report_date`, and `regenerate=true`.
- **Dependencies:** `documents` table, report notification templates/profiles.
- **Notes/Risks:** Rendering failures and zero-data scenarios are handled differently per pipeline; keep behavior consistent. For same-day table layout, `app/reports/daily_sales_report/templates/daily_sales_report.html` and `app/reports/shared/templates/same_day_fulfillment_table.html` are the authoritative sources (legacy standalone same-day template removed). Pending deliveries now always includes TD+UC rows where `vw_orders.recovery_status = 'NONE'` and no matching `sales` row; recovery-workflow rows are excluded from normal aging buckets/details. Pending Deliveries notification attachments now include both the existing PDF and an additive XLSX workbook artifact grouped by `cost_center`.

## 9) Lead assignment workflow

- **Purpose:** Assign eligible leads, generate PDFs, ingest outcomes, notify.
- **Primary paths:**
  - `app/lead_assignment/{pipeline.py,assigner.py,pdf_generator.py,outcomes_ingestor.py,assignment_failure_diagnosis.py}`
- **Related docs/tests:** `docs/leads_assignments_pipeline.md`, `tests/lead_assignment/*`.
- **Notes/Risks:** DB template/recipient configuration required for full notification path.

## 10) Notification framework

- **Purpose:** Build pipeline/store/run email plans with attachments from DB metadata.
- **Primary paths:**
  - `app/dashboard_downloader/notifications.py`
  - `app/customer_retention/notifications.py`
  - `app/dashboard_downloader/db_tables.py`
- **Tables:** `pipelines`, `notification_profiles`, `email_templates`, `notification_recipients`, `documents`, `pipeline_run_summaries`. Customer retention owner-summary notifications depend on `pipelines`, `notification_profiles`, `email_templates`, and `notification_recipients`.
- **Customer retention contract:** production must seed/enable pipeline code `customer_retention_pipeline` and run-scoped profile code `owner_summary`, with an active template. Recipient email addresses must be configured operationally in `notification_recipients`, not code. If those contract rows are absent, `app/customer_retention/notifications.py` falls back to built-in default subject/body templates, but missing recipients still skip delivery with a `no_recipients` result and warning telemetry.
- **Notes/Risks:** Mismatched pipeline code/template profile causes silent no-email or summary-only behavior.

## 11) Schema migrations and migration tests

- **Purpose:** Evolve schema + seed metadata safely.
- **Primary paths:**
  - `alembic/env.py`
  - `alembic/versions/*`
  - `tests/alembic/*`
- **Notes/Risks:** Historical chain has many operational seed migrations; never rewrite history.

## 12) Operational scripts and deployment

- **Purpose:** Local/prod wrappers around CLI, cron execution, targeted pipeline runs.
- **Exact TD leads cron invocation:** `bash scripts/cron_run_td_leads_sync.sh` (wrapper), which calls `bash scripts/run_local_td_leads_sync.sh` and ultimately `poetry run python -m app crm td-leads-sync`.
- **Primary paths:**
  - `scripts/*.sh`
  - `.github/workflows/{ci.yml,deploy-prod.yml}`
  - `docker-compose.yml`, `Dockerfile`
- **Notes/Risks:** Script assumptions around env vars and alembic execution must stay aligned with `app/config.py` rules.

- Pipeline lock policy: TD leads and orders/reports acquire only their own `tmp/cron_run_*.lock` directories, preserving per-pipeline ownership metadata, stale-lock recovery, cleanup traps, and watchdogs without cross-pipeline blocking. The TD-leads watchdog defaults to `TD_LEADS_MAX_RUNTIME_SECONDS=300`, runs the local sync in a dedicated session, and terminates/verifies the full child process group before lock cleanup. `scripts/inspect_or_kill_pipeline_stale.sh {td-leads|orders-reports|orders-report|profiler-store-locks}` provides dry-run inspection, explicit no-lock messaging for the selected `tmp/cron_run_*.lock` directory, targeted correction for `Pipeline=...` mistakes, and `--force`/`FORCE=1`/trailing `FORCE=1` process-group termination for either local lock, plus orders profiler per-store lock inspection and an explicit rollout cleanup path for the retired `tmp/cron_heavy_pipelines.lock` directory. `scripts/kill_orders_and_reports_stale.sh` remains a rollout forwarding wrapper for `orders-reports`.
- TD-leads wrapper operational notifications: `scripts/cron_run_td_leads_sync.sh` calls `app.crm_downloader.td_leads_sync.wrapper_notifications` for `watchdog_timeout`, `stale_owner_terminated`, `skipped_due_to_active_same_pipeline_owner`, and `lock_metadata_ambiguous`, plus a successful post-termination recovery edge. The helper persists sanitized `td_leads_wrapper_ops` summaries, uses DB-driven notification metadata, deduplicates repeated same-owner suppression email, and leaves delivery success/failure/timeout in the wrapper log. Notification execution is isolated in a dedicated process group with `TD_LEADS_WRAPPER_NOTIFICATION_TIMEOUT_SECONDS=30`; timeout handling terminates and verifies the helper group before lock-safe cleanup continues. Stale-owner recovery reacquires the lock before best-effort alert delivery. Ordinary successful runs retain a bounded helper probe only because DB-backed incident state may still require a recovery edge.

- Daily and MTD same-day fulfillment outputs now include Order Amount and Payment Received columns (`Order Amount` comes from `vw_orders.order_amount`; `Payment Received` remains collection/payment data, with payment rows summed per order for deterministic multi-payment reporting).

- Query portability: daily same-day line-item/payment-mode concatenation is dialect-aware (`string_agg` on PostgreSQL, `group_concat` on SQLite) while preserving existing same-day grouping and payment sum behavior.
- Failure propagation policy: `scripts/cron_run_orders_and_reports.sh` must exit non-zero when any required report pipeline fails after retries (daily sales, MTD same-day, pending deliveries), and optional rescue attempts cannot mask a failed required daily run. Orders, Daily Sales, and Pending Deliveries run in dedicated child process groups with separate watchdog limits. After timeout, the wrapper verifies complete non-zombie child process-group disappearance before releasing its local lock; an orders timeout remains retryable and downstream reports still execute only when that verification succeeds. Failed termination verification preserves the local lock for explicit operator recovery and aborts downstream execution safely.
- Attachment contract: Daily Sales email has two distinct artifacts—(1) in-report same-day section scoped to report date, and (2) tailed MTD same-day attachment scoped from month start through report date, with separate metadata/doc_type.
