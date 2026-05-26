# Architecture (current repository state)

## System overview

`simplify_downloader` is an async Python service that combines:
1. headless browser + API extraction from CRM systems,
2. ingestion/upsert into Postgres,
3. PDF report generation,
4. notification dispatch driven by DB templates/recipients,
5. run-summary and per-window observability.

Main runtime entrypoint is `python -m app` (`app/__main__.py`) which delegates to `app/dashboard_downloader/cli.py` and sub-pipelines.

## Major components

### 1) Configuration and secrets
- `app/config.py` is SSOT for runtime config.
- Inputs come from:
  - required env vars (DB pieces, secret key, paths, timezone),
  - `system_config` table for plaintext/encrypted app settings,
  - decryption via `app/crypto.py`.
- Config fails fast on missing/invalid values.
- UC HTTPS certificate handling is controlled by `UC_IGNORE_HTTPS_ERRORS` in
  `system_config`; it defaults to `false` so UC Playwright browser contexts keep
  strict TLS validation. Fix/renew the remote certificate first; setting this to
  `true` is an emergency-only workaround because it suppresses browser-side
  certificate validation for UC sync.

### 2) Shared data access and models
- Async DB session management: `app/common/db.py`.
- Dashboard/store tables and persistence helpers: `app/common/dashboard_store.py`.
- CSV ingestion schema/model pipeline: `app/common/ingest/{schemas.py,models.py,service.py}`.

### 2.1) Order amount and payment-decision contract
- Raw `orders.net_amount`, `orders.gross_amount`, and `orders.adjustment` are source/ingest fields. They are preserved for synchronization fidelity and may be read by ingest/sync code when the purpose is source synchronization, reconciliation, or auditing of raw CRM payloads.
- Business reports, operational decision-making, payment status checks, recovery checks, and user-facing report totals must use `vw_orders.order_amount` as the canonical order value. Direct report reads from `orders` are prohibited unless explicitly approved for a documented exception.
- Payment truth ignores CRM/order snapshot fields `orders.payment_status` and `orders.payment_amount`; use `vw_orders.order_amount`, `sales.payment_received`, and verified `payment_collections.amount` evidence instead.
- `payment_collections` is the verified payment evidence table. For current reconciliation, only `source_type = 'google_sheet'` and `source_type = 'legacy_sales'` are qualifying payment-proof sources; they are equivalent verified evidence. `bank_row_id` is reserved for future bank-reconciliation work and is ignored by current reports.
- `payment_collections.source_sheet_row` remains non-null, and payment evidence idempotency is `(source_type, source_sheet_row)`.
- `payment_collections.order_number` may contain one order token or a grouped list split on comma (`,`) or slash (`/`). Each token is normalized by removing whitespace and uppercasing, then matched as a whole token within the same `cost_center`; substring matches do not qualify (for example, `ORD1` does not match `ORD10`). Blank/malformed/unmatched tokens are not valid proof for the order and belong in payment-evidence audit diagnostics.
- User-facing labels for this business value should say `Order Amount` rather than raw/source column names.
- Payment comparisons use tolerance `1` (₹1) when comparing collected/paid amounts to `vw_orders.order_amount`. Overpayments are treated as paid in full.
- Multi-order `payment_collections.order_number` values are group-reconciled before row-level missing/short classification. Group-paid rows are excluded from main missing/short reports; group-short rows are allocated sequentially by `order_date ASC, order_number ASC`.
- Normal missing-payment rows exclude `TO_BE_RECOVERED` and `TO_BE_COMPENSATED`. Normal pending-delivery aging buckets/details/action buckets exclude all recovery workflow statuses: `TO_BE_RECOVERED`, `TO_BE_COMPENSATED`, `RECOVERED`, `COMPENSATED`, and `WRITE_OFF`.
- `Actual Payments Not Found` means no valid qualifying payment proof exists for the normalized `(cost_center, order_number)` after applying the source-type and whole-token rules above; it is not a raw physical-row absence check. A `payment_collections` row with an unsupported `source_type`, blank/malformed/unmatched token, or different cost center does not satisfy proof. A valid qualifying proof with a payment/order amount mismatch is handled by short-payment or audit reconciliation, not by `Actual Payments Not Found`.
- `Actual Payments Not Found` is intentionally current/open across all order dates (not restricted by Daily/MTD report windows). Eligibility requires a `sales` row and no valid qualifying payment proof.
- A separate `Short Payment` sub-report is required and remains distinct from `Actual Payments Not Found`; `source_type` belongs in audit/reconciliation reports, not every normal business report. Python `app.reports.shared.payment_reconciliation` is canonical for report reconciliation; `vw_orders_missing_in_payment_collections` is a compatibility/audit projection and must mirror the Python missing-proof subset.
- `Short Payment` is intentionally a current/open action list across all order dates, behaving like `TO_BE_RECOVERED` visibility by showing current unresolved action rows rather than rows constrained to the Daily or MTD report date window. Daily/MTD report date windows do not restrict Short Payment eligibility.
- Daily Sales Short Payments PDF rows still exclude orders with `TO_BE_RECOVERED`, `TO_BE_COMPENSATED`, `RECOVERED`, `COMPENSATED`, or `WRITE_OFF` recovery status, and zero-value orders are not eligible.
- `To Be Recovered` is intentionally current/open by recovery workflow status (for example `TO_BE_RECOVERED`, `TO_BE_COMPENSATED`) across all order dates, not by the Daily/MTD report date window.
- Daily Sales Short Payments PDF requires clean reconciliation. A row is eligible only when all of these conditions hold: (1) a `sales` row exists; (2) qualifying `payment_collections` proof exists; (3) `sales.payment_received` and proof/evidence amount match within ₹1; and (4) the proof/evidence amount is short against `vw_orders.order_amount` by more than ₹1.
- Daily Sales also emits an APNF workbook artifact (`daily_sales_actual_payments_not_found_xlsx`) from the same canonical `missing_payment_rows` dataset used by the APNF PDF. Workbook structure is one worksheet per `cost_center` with rows sorted by `order_date` descending; this is additive and does not change payment classification semantics.
- Payment Evidence Review (`vw_payment_evidence_reconciliation` and `scripts/payment_evidence_review.py`) is audit-only. Its `reconciliation_result` column is a raw reconciliation classification and may surface rows with recovery workflow statuses for diagnostics; use `operator_actionable_payment_status` to distinguish rows that are excluded from the operator Daily Sales Short Payments PDF.
- Zero-value orders remain visible as orders in descriptive reporting where order presence matters, but they are excluded from missing-payment, pending-payment, and recovery action checks.

### 3) Dashboard downloader orchestration
- Orchestrator: `app/dashboard_downloader/pipeline.py`.
- CLI/router: `app/dashboard_downloader/cli.py`.
- Responsibilities:
  - run single-session store downloads,
  - merge + ingest bucketed CSVs,
  - audit/cleanup,
  - trigger store report generation tail-step,
  - persist run summary,
  - trigger notifications.

### 4) CRM order-sync pipelines
- UC sync: `app/crm_downloader/uc_orders_sync/main.py`.
- TD sync: `app/crm_downloader/td_orders_sync/main.py`.
- TD leads sync: `app/crm_downloader/td_leads_sync/main.py`.
- Shared window logic: `app/crm_downloader/orders_sync_window.py`.
- Profiler/orchestrator over windows + stores: `app/crm_downloader/orders_sync_run_profiler/main.py`.
- Data source behavior appears to include UI extraction plus TD API compare/source-mode switching.
- TD leads sync run summaries include:
  - aggregate bucket write counts and status transitions, and
  - actionable lead-change payloads grouped by action/bucket + transitions, deduped by lead identity, with per-group truncation and `overflow_count` markers for email/report readability.
- Section semantics used by reports/notifications:
  - Open leads are **backlog-style**: they carry forward across days until a lead closes.
  - Cancelled and completed leads are **day-event-style**: they are counted only when the status event occurs on that day.
  - Completed reconciliation uses `orders.store_code + orders.mobile_number` as the lookup key, and surfaces `orders.order_number` in output for operator traceability.

#### TD leads concurrency controls
- `TD_LEADS_MAX_WORKERS` controls store-worker concurrency for TD leads sync.
  - Default: `2`.
  - Minimum effective value: `1`.
- `TD_LEADS_PARALLEL_ENABLED` toggles parallel store execution for TD leads sync.
  - Default: enabled (`true`).
  - When disabled (`0/false/no/off`), effective concurrency is forced to `1` (sequential mode).
- The pipeline logs resolved startup telemetry with configured/effective concurrency and parallel-mode status.

### 5) Reporting pipelines
- Daily sales: `app/reports/daily_sales_report/`.
- Pending deliveries: `app/reports/pending_deliveries/` (existing PDF output plus additive XLSX artifact attached in the same notification send path).
- Store/week/month reporting helpers: `app/dashboard_downloader/run_store_reports.py` + `app/dashboard_downloader/pipelines/`.
- PDF rendering centralized through report renderer wrappers.
- Pending deliveries canonical contract: normal pending-delivery buckets/details include only rows with `vw_orders.recovery_status = 'NONE'` and no matching `sales` row. Recovery workflow statuses are intentionally excluded from normal pending-delivery queues.

### 6) Lead assignment pipeline
- `app/lead_assignment/pipeline.py` orchestrates:
  - assignment batch creation,
  - PDF generation,
  - notification dispatch,
  - run summary style logging.

### 7) Notifications and operational messaging
- `app/dashboard_downloader/notifications.py` resolves pipeline run context + docs + templates + recipients from DB.
- SMTP config values are loaded from `app.config`.
- Supports diagnostics command (`python -m app notifications test ...`).

## Request / run flow (high level)

1. Operator/cron/script invokes `python -m app ...`.
2. CLI loads `config` (env + DB), resolves run mode.
3. Pipeline executes extraction/ingest/reporting stages.
4. Stage metrics and events are logged via `JsonLogger`.
5. Run summary rows inserted/updated in `pipeline_run_summaries`.
6. Notifications are planned from DB metadata and sent via SMTP.

### Cron wrapper lock hierarchy

For heavy cron wrappers in `scripts/` (including `cron_run_td_leads_sync.sh` and
`cron_run_orders_and_reports.sh`), locking is intentionally layered:

1. Acquire global lock: `tmp/cron_heavy_pipelines.lock`.
2. Acquire per-script lock (for example `tmp/cron_run_td_leads_sync.lock`).
3. Execute wrapper run steps.

Operational logs explicitly label waits/acquisition as `[global lock]` vs
`[local lock]` so operators can quickly identify whether contention is shared
across heavy wrappers or specific to one wrapper.

For order-sync profiler, the run additionally:
- computes date windows per store,
- runs TD/UC sync workers,
- writes/reads `orders_sync_log` and derived summary metrics.

## Data and migration layer

- Alembic is configured in `alembic/env.py` (env-driven URL build).
- Revisions are under `alembic/versions/`.
- Schema includes operational tables such as:
  - `pipeline_run_summaries`,
  - `documents`,
  - `orders_sync_log`,
  - `td_sync_compare_log`,
  - ingest tables (`missed_leads`, `undelivered_orders`, etc.).

### Orders manual recovery workflow (TD + UC operations)

`orders` includes manual recovery-tracking fields introduced by migration
`0092_orders_recovery_tracking`:
- `recovery_status`
- `recovery_category` (expanded by `0106_recovery_categories`)
- `recovery_notes`
- `recovery_marked_at`
- `recovery_marked_by`

Use the following standard lifecycle for both TumbleDry and UClean stores so
reporting and aging buckets remain consistent:

1. **CRM force-paid done for unlock**
   - Set `recovery_status='TO_BE_RECOVERED'`.
   - Set `recovery_category='CRM_FORCED_PAID_90D'`.
   - Set `recovery_notes` with reason and ticket/reference.
2. **Damage case identified**
   - Set `recovery_status='TO_BE_COMPENSATED'`.
   - Set `recovery_category='DAMAGE_CLAIM'`.
   - Set `recovery_notes` with claim details.
3. **Closure**
   - Set `recovery_status` to terminal workflow status (for example `RECOVERED`, `COMPENSATED`, or `WRITE_OFF`).
   - Do **not** revert resolved recovery rows back to `NONE`; `NONE` means no active/terminal recovery workflow has been applied.
   - For write-off/return decisions, use `recovery_category` to distinguish:
     - `WRITE_OFF_FULL` for full write-offs.
     - `WRITE_OFF_BALANCE` for balance-only write-offs.
     - `RETURNED` for returned-order recovery decisions.
   - Keep note history append-only in `recovery_notes` (do not overwrite prior
     context).

#### SQL update template (admin UI should mirror this contract)

Use parameterized updates in admin SQL consoles / backend tooling:

```sql
UPDATE orders
SET
    recovery_status = :recovery_status,
    recovery_category = :recovery_category,
    recovery_notes = CONCAT(
        COALESCE(recovery_notes || E'\n', ''),
        TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS TZ'),
        ' | ',
        :actor,
        ' | ',
        :note
    ),
    recovery_marked_at = NOW(),
    recovery_marked_by = :actor_user_id
WHERE id = :order_id;
```

Operational notes:
- Always append to `recovery_notes`; never replace existing text.
- Include ticket/reference IDs in note lines for force-paid and damage-claim
  actions.
- If an admin UI is used, enforce the same enum values and append-style notes
  behavior to match SQL operations.

## CI/CD and deployment shape

- CI (`.github/workflows/ci.yml`): on push to `main`, install via Poetry, run pytest.
- Deploy (`.github/workflows/deploy-prod.yml`): SSH to host, hard reset to origin/main, docker compose build/up, run alembic upgrade head.
- Runtime container entrypoint: `python -m app`.

## AuthN/AuthZ and sensitive assets

- Primary auth appears to be service credentials for CRM endpoints and SMTP; no user-facing auth layer in this repo.
- Sensitive artifacts include storage-state browser cookies, report PDFs, logs, and encrypted DB config values.

## Integration points

- CRM web UIs and/or APIs (TD/UC flows).
- Postgres via SQLAlchemy.
- SMTP for outbound emails.
- Playwright/browser runtime for scraping/PDF rendering.

## Important boundaries/patterns

- Keep config reads inside `app.config`.
- Keep DB reads/writes via shared async session helpers.
- Keep pipeline telemetry structured (`log_event`, run IDs, phase/status).
- Treat DB notification metadata as runtime contract for email behavior.

## Legacy markdown status (triaged)

From code-audit perspective:

- **Useful + mostly aligned**: `README.md`, `docs/configuration.md`, `docs/uc_orders_sync_runbook.md`, `docs/td_api/*.md`.
- **Useful but likely stale/partial**: `docs/CRM-Downloader-Specs.md`, `docs/crm-sync-pipeline*.md`, `docs/tsv_crm_refactor_master_plan.md`.
- **Historical/process-only**: `docs/StepA-Response.md`, `docs/StepC-Documentation.md`, `docs/CODEX_KICKOFF_SIMPLIFY_DOWNLOADER.md`, `docs/start.md`.
- **Out-of-scope/contradictory for this repo**: `docs/temp_md/*`, `app/charms_wiki/*.md` (different product domain).

Use canonical docs first, then consult legacy docs only for supporting context.

## Known ambiguities needing verification

- Some long CRM sync modules contain mixed legacy and current paths; exact source-of-truth flow per source mode should be validated by focused runtime tests.
- Presence of similarly named migrations (e.g., multiple `0075*`) suggests historical complexity; migration sequence should always be checked from actual revision chain, not filename assumptions.
