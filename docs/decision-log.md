# Engineering Decision Log

## How to use this file

- Record durable engineering decisions that affect architecture, data contracts, or operational safety.
- Keep entries evidence-based. If inferred from code (not explicitly documented), mark as reconstructed.
- Add new entries at the top.

---

## Initial reconstructed decisions

### DL-011
- **Date:** 2026-04-30
- **Status:** Active
- **Decision:** Formalize manual-ingestion business rules and correction lifecycle for `payment_collections` in a dedicated operator-facing document.
- **Context:** The table is manually fed from Excel transcriptions of store WhatsApp payment confirmations; without a written contract, idempotency, correction handling, and handover semantics can drift.
- **Evidence:** `docs/payment_collections.md` now defines row identity (`source_sheet_row`), update workflow expectations, and recommended upsert SQL with explicit `updated_at` maintenance.
- **Implications:**
  - Operators and engineers have one canonical reference for inserts/updates into this manual ledger.
  - Data reconciliation can rely on consistent meanings for `handed_over`, `date_handed`, `updated_flag`, and `date_modified`.
  - Future tooling can adopt the same contract without reverse-engineering intent from ad-hoc SQL.
- **Follow-up:** Add and periodically refresh `docs/payment_collections.csv` exports to support trend/data-quality analysis snapshots tied to this contract.

### DL-010
- **Date:** 2026-04-30
- **Status:** Active
- **Decision:** Add a dedicated `payment_collections` table to store manually recorded store payment transactions imported from operator-maintained spreadsheets.
- **Context:** Store delivery/payment confirmations are shared in WhatsApp groups, then transcribed by operations into Excel before manual SQL inserts. The service lacked a first-class table to persist this manual payment ledger with lifecycle flags.
- **Evidence:** `alembic/versions/0097_payment_collections.py` creates `payment_collections` with unique source row tracking, payment/order metadata, handover/update flags, and supporting lookup indexes.
- **Implications:**
  - Manual payment ingestion can use stable inserts keyed by `source_sheet_row` to avoid duplicate row ingestion.
  - Operational lookup paths are optimized for `(cost_center, payment_date)`, `order_number`, and `payment_mode`.
  - The table supports later reconciliation workflows through `handed_over`, `date_handed`, `date_modified`, and `updated_flag` fields.
- **Follow-up:** If ingestion tooling is added, enforce idempotent upsert behavior keyed by `source_sheet_row` and maintain `updated_at` on updates.

### DL-009
- **Date:** 2026-04-29
- **Status:** Active
- **Decision:** Run `reports.mtd_same_day_fulfillment` as a separate tailed report in the cron reports block (between Daily Sales and Pending Deliveries), with its own retry envelope and notification metadata contract.
- **Context:** Same-day fulfillment was introduced as a distinct report pipeline and should be production-orchestrated independently instead of relying on Daily Sales attachments only.
- **Evidence:** `scripts/cron_run_orders_and_reports.sh` now executes `run_local_reports_mtd_same_day_fulfillment.sh` as its own report step with dedicated attempt/retry env knobs. `alembic/versions/0096_seed_mtd_same_day_notif.py` seeds `pipelines`, `notification_profiles`, `email_templates`, and `notification_recipients` records for `reports.mtd_same_day_fulfillment`.
- **Implications:**
  - Cron logs and run summaries now show explicit success/failure for MTD same-day fulfillment as an independent report stage.
  - Notification delivery for this report is DB-contract driven and can be managed via profile/template/recipient rows without code changes.
  - Full report-block failure condition should evaluate daily + MTD same-day + pending pipelines together.
- **Follow-up:** Keep migration tests validating seed + cleanup behavior to protect this metadata contract.

### DL-008
- **Date:** 2026-04-29
- **Status:** Proposed
- **Decision:** Add a dedicated same-day fulfillment section in Daily Sales Report for orders created and delivered/paid on the same business day.
- **Context:** Code review confirms the current Daily Sales Report aggregates orders and collections into KPI totals, but does not expose line-level same-day create+deliver rows. Pending Deliveries intentionally filters only `order_status == "Pending"`, so same-day fulfilled orders never appear there either, creating an operator visibility gap for rapid-turnaround orders.
- **Evidence:** `app/reports/daily_sales_report/data.py` aggregates by date windows via `orders.order_date` and `sales.payment_date` but has no extracted detail dataset for same-day fulfillment rows; template sections currently render KPI totals and recovery/lead blocks only. `app/reports/pending_deliveries/data.py` explicitly restricts dataset to pending orders.
- **Implications:**
  - Daily Sales PDF can under-communicate high-velocity operational wins where order creation and completion happen on the same day.
  - Pending Deliveries behavior should remain unchanged because fulfilled orders are out of scope for pending aging buckets.
- **Follow-up (implementation task):**
  1. Extend `DailySalesReportData` with `same_day_fulfillment_rows` and aggregate metadata (count, optional totals).
  2. Add query in `app/reports/daily_sales_report/data.py` joining orders + sales (+ garments if needed) constrained to the report day where `local(order_date) == report_date` and `local(delivery/payment_date) == report_date`.
  3. Build concatenated line-item text in the format `service_name + garment_name` per order (`STRING_AGG`/DB-equivalent with deterministic ordering).
  4. Compute `hours` as elapsed time between order creation timestamp and fulfillment timestamp (delivery timestamp preferred, fallback payment timestamp if delivery timestamp is absent).
  5. Render a new table section in `app/reports/daily_sales_report/templates/daily_sales_report.html` with columns: `store_code, order_number, order_date, customer_name, mobile_number, line_items, delivery/payment_date, hours`.
  6. Add/update tests under `tests/reports/daily_sales_report/` for: inclusion criteria, timezone boundary handling, concatenation formatting, and hours calculation.
  7. Keep `app/reports/pending_deliveries/*` logic unchanged except for optional explanatory note/test asserting fulfilled same-day orders are excluded by design.

### DL-007
- **Date:** 2026-04-27
- **Status:** Active
- **Decision:** Remove configurable UC skip toggle for pending deliveries and always include UC rows unless excluded by core pending filters.
- **Context:** Pending deliveries now relies on recovery-status business rules instead of a source-system toggle, and startup config should not depend on legacy `SKIP_UC_Pending_Delivery`.
- **Evidence:** `app/config.py`, `app/reports/pending_deliveries/data.py`, and pending-deliveries tests.
- **Implications:**
  - `SKIP_UC_Pending_Delivery` is no longer a required runtime config key.
  - Pending deliveries main dataset excludes recovery statuses `TO_BE_RECOVERED`, `TO_BE_COMPENSATED`, `RECOVERED`, `COMPENSATED`, and `WRITE_OFF` before bucket/detail aggregation.
  - UC rows continue to appear in pending deliveries when they satisfy standard pending filters.
- **Follow-up:** Keep migration cleanup in place so legacy `system_config` rows do not imply obsolete behavior.

### DL-006
- **Date:** 2026-04-25
- **Status:** Active
- **Decision:** Standardize manual `orders` recovery updates (SQL/admin UI) using
  explicit status/category transitions and append-only notes.
- **Context:** Store operations for TD/UC need one consistent procedure for
  force-paid unlock recovery, damage-claim compensation, and final closure so
  downstream reporting interprets records uniformly.
- **Evidence:** `alembic/versions/0092_orders_recovery_tracking.py` defines
  allowed recovery statuses/categories; daily sales report tests already use
  `TO_BE_RECOVERED` and `TO_BE_COMPENSATED` buckets.
- **Implications:**
  - Force-paid unlock actions must use:
    - `recovery_status='TO_BE_RECOVERED'`
    - `recovery_category='CRM_FORCED_PAID_90D'`
  - Damage claims must use:
    - `recovery_status='TO_BE_COMPENSATED'`
    - `recovery_category='DAMAGE_CLAIM'`
  - Closures must move to one of `RECOVERED`, `COMPENSATED`, or `WRITE_OFF`.
  - `recovery_notes` is append-only and should include reason + ticket/claim
    reference and actor/timestamp metadata.
- **Follow-up:** Ensure any internal admin UI/input forms enforce these enum
  values and append-style note behavior rather than free-form overwrite.

### DL-001
- **Date:** Date unknown (reconstructed from repository state)
- **Status:** Active
- **Decision:** Standardize all runtime invocation through `python -m app`.
- **Context:** Legacy entrypoints are discouraged/removed, and scripts/workflows target `app` module CLI.
- **Evidence:** `README.md`, `app/__main__.py`, `scripts/*`, Docker `ENTRYPOINT`.
- **Implications:** New automation should not introduce alternate top-level entrypoints.
- **Follow-up:** Verify external schedulers no longer call removed legacy entrypoints.

### DL-002
- **Date:** Date unknown (reconstructed from repository state)
- **Status:** Active
- **Decision:** Enforce a strict configuration SSOT in `app/config.py` with fail-fast validation.
- **Context:** Runtime depends on many environment + DB keys (including encrypted values); drift is high-risk.
- **Evidence:** `app/config.py` module contract comments; `tests/test_config.py` getenv restriction test.
- **Implications:** Direct env/DB config access in feature modules is a policy violation.
- **Follow-up:** Keep tests updated when adding any new config keys.

### DL-003
- **Date:** Date unknown (reconstructed from repository state)
- **Status:** Active
- **Decision:** Use database flags (`store_master`) for run scope instead of ad-hoc hardcoded store lists.
- **Context:** ETL/report/sync inclusion is operationally controlled in DB.
- **Evidence:** `app/dashboard_downloader/settings.py`, `app/dashboard_downloader/config.py`, profiler store queries.
- **Implications:** Operational toggles can be changed without code deploy; devs must avoid bypassing this with inline lists.
- **Follow-up:** Validate admin/operator process around flag lifecycle.

### DL-004
- **Date:** Date unknown (reconstructed from repository state)
- **Status:** Active
- **Decision:** Maintain pipeline observability via structured run summaries and phase logs.
- **Context:** Long-running multi-store jobs require post-run diagnostics and email summaries.
- **Evidence:** `app/dashboard_downloader/json_logger.py`, `run_summary.py`, `pipeline_run_summaries` usage across pipelines.
- **Implications:** New pipelines should emit consistent phase/status telemetry and summary records.
- **Follow-up:** Consider schema-level validation for required summary payload keys per pipeline.

### DL-005
- **Date:** Date unknown (reconstructed from repository state)
- **Status:** Active
- **Decision:** Keep notifications DB-driven (profiles/templates/recipients) rather than hardcoded recipients.
- **Context:** Notification targets vary by pipeline/env/store.
- **Evidence:** `app/dashboard_downloader/notifications.py`, notification table definitions in `app/dashboard_downloader/db_tables.py`.
- **Implications:** Pipeline additions require notification metadata seeding/migration work.
- **Follow-up:** Add stronger automated checks for missing templates/recipients in CI if needed.

---

## Template for future entries

### DL-XXX
- **Date:** YYYY-MM-DD (or “Date unknown (reconstructed from repository state)”)
- **Status:** Proposed | Active | Superseded
- **Decision:** One-sentence decision statement.
- **Context:** What problem/constraints led to this choice?
- **Evidence:** Code paths, migrations, tests, or docs that support this.
- **Implications:** Technical and operational consequences.
- **Follow-up:** Required next actions, validation, or cleanup.

- 2026-04-29: Same-day fulfillment reporting (Daily + MTD) now exposes `net_amount` and aggregated `payment_received` (sum per order within report window) to improve financial visibility for split-payment orders.

### DL-006
- **Date:** 2026-04-29
- **Status:** Active
- **Decision:** Daily same-day SQL now uses dialect-aware string aggregation (`string_agg` for PostgreSQL, `group_concat` for SQLite) to keep one query contract portable across runtime/test databases.
- **Context:** Same-day fulfillment line-item and payment-mode concatenation was vulnerable to backend-specific SQL behavior.
- **Evidence:** `app/reports/daily_sales_report/data.py`, `tests/test_daily_sales_report_data.py`.
- **Implications:** Daily report semantics stay unchanged (same-day filters, row grouping, summed payment_received), while query compilation remains valid for both production and test dialects.
- **Follow-up:** Keep portability tests whenever adding new SQL aggregate concatenations.

### DL-007
- **Date:** 2026-04-29
- **Status:** Active
- **Decision:** Cron report wrapper exits non-zero if any required report step fails after retries.
- **Context:** Partial success previously masked failed report pipelines in final cron status.
- **Evidence:** `scripts/cron_run_orders_and_reports.sh`, `tests/test_cron_run_orders_and_reports.py`.
- **Implications:** Operators and monitors can treat cron exit code as strict health signal for required report generation.
- **Follow-up:** Preserve retry behavior, but never downgrade required-step failures to success (including optional daily rescue attempts).

### DL-008
- **Date:** 2026-04-29
- **Status:** Active
- **Decision:** Daily Sales notification second attachment is explicitly treated as an MTD same-day artifact with month-start→report-date window and distinct document metadata.
- **Context:** Operators must differentiate daily same-day section (report-date window) from tailed MTD attachment (month-to-date window).
- **Evidence:** `app/reports/daily_sales_report/pipeline.py`, `app/reports/mtd_same_day_fulfillment/data.py`, `tests/test_daily_sales_report_pipeline.py`.
- **Implications:** Attachment naming/doc_type/window text remain unambiguous in persisted documents and PDFs.
- **Follow-up:** Keep window labels explicit in templates and pipeline logs.

### DL-009
- **Date:** 2026-04-29
- **Status:** Active
- **Decision:** Same-day fulfillment row selection and line-item aggregation are centralized in `app/reports/shared/same_day_fulfillment.py` and reused by both Daily and MTD reports, with same-day defined as local date equality between order and payment timestamps.
- **Context:** Daily and MTD reports previously duplicated near-identical SQL for same-day filtering, payment aggregation, and line-item concatenation.
- **Evidence:** `app/reports/shared/same_day_fulfillment.py`, `app/reports/daily_sales_report/data.py`, `app/reports/mtd_same_day_fulfillment/data.py`, `tests/test_same_day_fulfillment_shared.py`.
- **Implications:** Canonical behavior stays consistent across report pipelines while allowing each caller to pass its own window boundaries (daily window vs month-to-date window).
- **Follow-up:** Route future same-day rule or aggregation changes through the shared helper and keep report-level tests as integration coverage.
