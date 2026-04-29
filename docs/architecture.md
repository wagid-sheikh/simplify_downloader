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

### 2) Shared data access and models
- Async DB session management: `app/common/db.py`.
- Dashboard/store tables and persistence helpers: `app/common/dashboard_store.py`.
- CSV ingestion schema/model pipeline: `app/common/ingest/{schemas.py,models.py,service.py}`.

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
- Pending deliveries: `app/reports/pending_deliveries/`.
- Store/week/month reporting helpers: `app/dashboard_downloader/run_store_reports.py` + `app/dashboard_downloader/pipelines/`.
- PDF rendering centralized through report renderer wrappers.
- Pending deliveries aging buckets/details exclude orders whose `recovery_status` is one of `TO_BE_RECOVERED`, `TO_BE_COMPENSATED`, `RECOVERED`, `COMPENSATED`, or `WRITE_OFF`; `NULL`/other statuses remain eligible.

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
- `recovery_category`
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
   - Set `recovery_status` to one of:
     - `RECOVERED`
     - `COMPENSATED`
     - `WRITE_OFF`
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
