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
- Daily Sales reconciles each active cost center's descriptive report-day order-number population against its FTD count and `vw_orders.order_amount` sum, checks duplicate report-day order numbers, and validates aggregate FTD totals. Findings are persisted in run metrics and rendered in the core PDF. Hard reconciliation errors do not suppress delivery: the generated PDF is visibly marked `INVALID DATA REPORT`, and the concise findings remain in notification summary context so operators receive an actionable artifact rather than a silent gap.

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

#### Repeat-customer mobile identity handling
- `repeat_customers.mobile_no` is required for ingest identity. Rows with missing, blank, malformed, or invalid normalized `mobile_no` values remain silently skipped rather than being persisted with an invalid dedupe key.
- Skipped rows are excluded from repeat-customer reporting until corrected at the source and reingested.
- These exclusions are intentionally not dashboard warning-threshold or operator-notification conditions. If aggregate informational telemetry is retained, it contains only counts and store codes; it must not include customer-sensitive row payloads or mobile values.

### 4) CRM order-sync pipelines
- UC sync: `app/crm_downloader/uc_orders_sync/main.py`.
- UC dashboard readiness is DB-driven through `store_master.sync_config.urls.home`; the current dashboard host is `storepanel.ucleanlaundry.com`. The deprecated `store.ucleanlaundry.com/dashboard` value must be migrated rather than hardcoded as an application fallback.
- `store_master.start_date` is the single canonical store-level start/launch date. Dashboard ingestion stores the dashboard's `Launch Date` value there, and CRM/order-sync windows use it as the lower bound when no explicit operator start date is provided.
- TD sync: `app/crm_downloader/td_orders_sync/main.py`.
- TD leads sync: `app/crm_downloader/td_leads_sync/main.py`.
- Shared window logic: `app/crm_downloader/orders_sync_window.py`.
- Profiler/orchestrator over windows + stores: `app/crm_downloader/orders_sync_run_profiler/main.py`.
- Data source behavior appears to include UI extraction plus TD API compare/source-mode switching.
- TD `/garments/details` pagination follows the sanitized `docs/garment-details.har` contract: responses expose rows at `data.rows`, total rows at `data.count`, and the observed safe page shape is `pageSize=100` (`TD_API_GARMENTS_PAGE_SIZE`, default `100`). Completion relies on actual returned rows plus `data.count`, not on an extra empty page after the final partial page.
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
- Supports DB-driven dashboard store-scope diagnostics (`python -m app stores diagnose`)
  for ETL-enabled, report-enabled, and report-eligible store counts and codes.

## Request / run flow (high level)

1. Operator/cron/script invokes `python -m app ...`.
2. CLI loads `config` (env + DB), resolves run mode.
3. Pipeline executes extraction/ingest/reporting stages.
4. Stage metrics and events are logged via `JsonLogger`.
5. Run summary rows inserted/updated in `pipeline_run_summaries`.
6. Notifications are planned from DB metadata and sent via SMTP.

### Cron wrapper pipeline-specific locks

Heavy cron wrappers in `scripts/` acquire only their own pipeline lock:

1. TD leads acquires `tmp/cron_run_td_leads_sync.lock`.
2. Orders/reports acquires `tmp/cron_run_orders_and_reports.lock`.
3. Each wrapper executes its own run steps without blocking the other pipeline.

Each wrapper uses the same pipeline-specific local-lock recovery state machine.
It first attempts `mkdir` for its lock directory and writes fresh ownership
metadata on success. If the directory exists, the wrapper logs the full local
snapshot (`pid`, `pgid`, `command`, `host`, `started_at`, and calculated lock
age) and then takes exactly one action without polling:

1. If both the recorded owner PID and process group are gone, remove the stale
   lock directory, reacquire it, write fresh metadata, and continue.
2. If the owner PID is alive and the calculated lock age is below that
   pipeline's threshold, log
   `status=skipped_due_to_active_same_pipeline_owner`, preserve the lock, and
   exit successfully.
3. If the owner PID is alive and aged beyond the threshold, require numeric
   PID/PGID metadata, an exact live PID-to-PGID match, and both recorded and live
   commands referencing the expected wrapper inside this repository. Send
   `TERM` to the complete process group, wait a bounded grace period, escalate
   to group `KILL` when necessary, and remove the lock only after the group is
   confirmed gone. Reacquire the directory and write fresh metadata before
   continuing.
4. If metadata is malformed, the PID/PGID relationship is inconsistent, the
   dead PID's recorded process group is still alive, or a command points outside
   the expected repository wrapper, fail safely without deleting the lock.

The TD-leads wrapper also invokes
`app.crm_downloader.td_leads_sync.wrapper_notifications` after watchdog timeout,
stale-owner termination, active-owner suppression, and ambiguous-lock fail-safe
outcomes. The helper persists a sanitized `td_leads_wrapper_ops`
`pipeline_run_summaries` event containing wrapper timestamp, host, local-lock
path, owner PID/PGID/age, recovery action, and resulting status. Delivery uses a
dedicated DB-driven profile/template. Repeated suppressions for the same active
owner are persisted but deduplicated after the initial email; a successfully
completed fresh run after stale-owner termination emits a recovery email. The
wrapper log records helper delivery success, failure, or timeout. The helper always runs
in a dedicated child process group bounded by
`TD_LEADS_WRAPPER_NOTIFICATION_TIMEOUT_SECONDS=30`; timeout handling terminates and
verifies the full helper process group, then continues lock-safe recovery or cleanup.
Stale-owner recovery reacquires the fresh lock before attempting best-effort alert
persistence or SMTP. Successful runs retain a bounded recovery probe because only the
DB-backed helper can determine whether an unresolved prior incident needs closure.
Operational event content
must not include credentials, CRM payloads, customer names, mobile numbers, or
scraped rows.

The thresholds are intentionally pipeline-specific:
`TD_LEADS_STALE_OWNER_SECONDS=300` supports the TD-leads 10–20 minute service
objective conservatively, while `ORDERS_REPORTS_STALE_OWNER_SECONDS=7200`
remains separately reviewed because orders/report workloads and retries differ.
`STALE_OWNER_TERM_WAIT_SECONDS` and `STALE_OWNER_KILL_WAIT_SECONDS` bound stale-owner
process-group shutdown. The TD-leads run-step watchdog defaults explicitly to
`TD_LEADS_MAX_RUNTIME_SECONDS=300`; a deprecated per-invocation
`MAX_RUNTIME_SECONDS` value takes precedence only when supplied for compatibility.
The retired
`tmp/cron_heavy_pipelines.lock` directory is not recreated at runtime. During
rollout, `scripts/inspect_or_kill_pipeline_stale.sh` provides an explicit one-time
cleanup path for an obsolete global-lock directory and removes it only after its
recorded process group is gone or has been safely terminated. The legacy
`scripts/kill_orders_and_reports_stale.sh` entrypoint is an orders/reports-only
shortcut that prints explicit helper guidance before forwarding to
`orders-reports` during rollout. The orders/reports
wrapper launches every pipeline step in a dedicated child process group and
enforces step-specific watchdog limits, so a stalled orders browser cleanup
cannot hold its local lock or block required report generation indefinitely.
Both the TD-leads and orders/reports wrappers use the same dedicated-session
watchdog pattern: on timeout each sends group `TERM`, waits a bounded grace
period, escalates group `KILL` while any non-zombie member survives, verifies
complete non-zombie process-group disappearance, and only then allows its cleanup
trap to remove the pipeline-specific local lock. Inside the Python
TD-leads flow, browser launch, storage-state writes, scheduler navigation, ingest,
order-history/reporting enrichment, and browser cleanup are application-bounded.
After an overall worker-gather timeout, cancellation drain is separately bounded by
`TD_LEADS_CANCELLATION_DRAIN_TIMEOUT_SECONDS=10`; a resistant task is logged and
failed-summary persistence continues. The shell watchdog remains the final
process-level safety boundary for any abandoned async work.

If a TD-leads or orders-and-reports run leaves a stale lock behind, inspect the
specific pipeline before any manual termination. The general operator helper
defaults to dry-run inspection:

```bash
./scripts/inspect_or_kill_pipeline_stale.sh td-leads
./scripts/inspect_or_kill_pipeline_stale.sh orders-reports
./scripts/inspect_or_kill_pipeline_stale.sh orders-report  # accepted alias
./scripts/inspect_or_kill_pipeline_stale.sh profiler-store-locks
```

Review the printed process-group snapshots and confirm that they belong to this
repository. Only then run the explicit termination step for the affected
pipeline:

```bash
./scripts/inspect_or_kill_pipeline_stale.sh --force td-leads
FORCE=1 ./scripts/inspect_or_kill_pipeline_stale.sh orders-reports
./scripts/inspect_or_kill_pipeline_stale.sh orders-reports FORCE=1
```

The helper maps those names to `tmp/cron_run_td_leads_sync.lock` and
`tmp/cron_run_orders_and_reports.lock`, accepts the `orders-report` alias,
prints a direct `No active/stale lock found ... at tmp/...` message when the
selected lock directory is absent, and prints a targeted correction for common
assignment-style mistakes such as `Pipeline=orders-reports`. It validates
directory-based lock metadata
(`pid`, `pgid`, `command`, `started_at`, `host`, and `cwd`), refuses malformed or
unrelated ownership, verifies PID-to-PGID membership, prints snapshots before
and after inspection, and removes a lock directory only after its process group
is gone. The same helper also exposes the opt-in/debug order-sync profiler
per-store locks through `profiler-store-locks` and includes those locks when
operators inspect `orders-reports`; each
`app/crm_downloader/data/orders_sync_run_profiler_locks/<store>.lock/` directory
prints `pid`, `pgid`, `started_at`, `started_at_epoch`, `host`, `cwd`, `command`,
`run_id`, `store_code`, and PID/PGID liveness. It also handles the retired
`tmp/cron_heavy_pipelines.lock` as an explicit rollout-cleanup case. The legacy
`scripts/kill_orders_and_reports_stale.sh` command remains an orders/reports-only
forwarding shortcut during rollout. It prints the explicit helper commands for both
`td-leads` and `orders-reports` before forwarding to `orders-reports`; it does not
infer a pipeline from environment variables. Operators should prefer the explicit
helper commands above.

For order-sync profiler, the orders/reports wrapper first runs a bounded connectivity
preflight retry envelope before Playwright starts. Every attempt checks all required
hosts and records host-level DNS, TCP, and optional app-layer HTTP outcomes. DNS
resolution failures, TCP connection failures/timeouts, and temporary app-layer HTTP
failures are retryable; deterministic preflight configuration failures fail fast. The
profiler launches only when all required hosts pass. Exhausted retries preserve degraded
report generation: the profiler is skipped, downstream reports receive
`--orders-sync-upstream-status failed`, and the wrapper exits non-zero after the reports
run. `ORDERS_PREFLIGHT_MAX_ATTEMPTS`, `ORDERS_PREFLIGHT_RETRY_DELAY_SECONDS`,
`ORDERS_PREFLIGHT_RETRY_BACKOFF_MULTIPLIER`, and
`ORDERS_PREFLIGHT_RETRY_MAX_DELAY_SECONDS` keep that envelope bounded.

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
- **Required production remediation:** rotate the exposed dashboard session credentials immediately, invalidate the prior sessions, and remove archived logs containing leaked values or protect them with access controls until secure deletion is complete. JSON logging now redacts sensitive headers, session identifiers, CSRF tokens, and API tokens, but sanitization does not retroactively clean existing archives.

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

### Operator-triggered `order_line_items` historical rebuild

The dedicated operator CLI is `python -m app crm rebuild-order-line-items` (alias: `python -m app crm order-line-items-rebuild`). It replays authoritative CRM line-item snapshots in bounded date windows and is intentionally separate from SQL-only deduplication: repeated line-item rows can be legitimate source data, so correction requires CRM snapshot replay rather than classifying duplicate database rows in isolation.

Typical invocations:

```bash
# Small dry-run smoke test: validate CRM access and replacement planning without writes.
poetry run python -m app crm rebuild-order-line-items --source both --from-date YYYY-MM-DD --to-date YYYY-MM-DD --window-days 7 --dry-run

# Full live rebuild: first live historical run; writes replacements and live resume progress.
poetry run python -m app crm rebuild-order-line-items --source both

# Interrupted live recovery: continue a previously interrupted live rebuild.
poetry run python -m app crm rebuild-order-line-items --source both --resume

# Stricter interrupted recovery: only reuse progress from a specific prior run ID.
poetry run python -m app crm rebuild-order-line-items --source both --resume --resume-run-id PRIOR_RUN_ID

# Explicit fresh/live intent; equivalent to omitting --resume.
poetry run python -m app crm rebuild-order-line-items --source both --fresh

# Optional local wrapper for the same CLI; pass the same flags after the script name.
bash scripts/run_local_order_line_items_rebuild.sh --source both --from-date YYYY-MM-DD --to-date YYYY-MM-DD --window-days 7 --dry-run
```

Operational behavior and limitations:

- Source selection is `td`, `uc`, or `both`; store scope is optional and otherwise uses active `store_master.sync_orders_flag` rows for the selected source group. Operators submit one command for the full historical range; the rebuild splits that range internally into CRM-safe source windows, so operators should not run one command per window.
- CRM source fetch windows are capped at 30 days. Omitted `--window-size`/`--window-days` resolves to CRM-safe source windows capped at 30 days, and a lower store/source config limit is honored. Larger explicit values are capped before source fetch. When `--start-date`/`--from-date` is omitted, each store starts at `store_master.start_date`; when `--end-date`/`--to-date` is omitted, the rebuild ends on the current pipeline date (`aware_now(get_timezone()).date()`). Explicit dates remain supported for smoke tests and dry runs.
- `--dry-run` fetches source snapshots and reports planned replacements without mutating `order_line_items`, staging tables, or live resume progress. Dry-run window results are log-only: a dry run is useful as a small smoke test, but it does not prepare or advance a later live `--resume`. The normal first live full rebuild omits `--resume`; use `--fresh`/`--ignore-progress` when you want that intent to be explicit. Use `--resume` only to recover a live rebuild that was interrupted after it had written live progress rows.
- TD windows use the TD garment snapshot replacement path (`ingest_td_garment_rows`). UC windows stage GST-derived order-detail snapshots and then use the UC final replacement path (`publish_uc_gst_order_details_to_line_items`).
- Only `complete_with_rows` and `complete_empty` outcomes replace local rows. `incomplete_or_failed` outcomes preserve existing rows and are logged as skipped.
- Every window emits a structured checkpoint (`source`, `store_code`, `cost_center`, `window_start`, `window_end`) plus inspected/complete/skipped/deleted/inserted/orphan counts and dry-run state via `JsonLogger`/`log_event`. Resume mode (`--resume`) uses live-run rows in `order_line_items_rebuild_progress` keyed by source, store, window start, and window end to skip successful windows and retry retryable failed windows; it is not tied to the current run ID. Rebuild start logs state that resume scope explicitly, and skipped-window logs include the prior progress row's run ID, `updated_at`, status, and metric counts. Add `--resume-run-id PRIOR_RUN_ID` when recovery must only trust progress from one prior run. Dry-run rows are not written by the current rebuild and any legacy dry-run rows are ignored for resume decisions. The rebuild reports any missing windows at completion.
- The default source fetchers rely on valid CRM browser storage-state/auth context. If CRM auth has expired, refresh normal TD/UC sessions first and rerun the rebuild.

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
