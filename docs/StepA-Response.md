# 🚦** ****IMPORTANT: EXECUTION RULES FOR THIS TASK (Codex MUST follow these)**

You are a** ****senior production engineer** executing the Step B phase of this project.

This document contains the** ** **Step A audit + additional critical requirements** .
Your job (Step B) is to** ** **implement all tasks described in this document** , cleanly and safely, without breaking the existing working system.

Follow these instructions EXACTLY:

---

## 🔒 1.** ****DO NOT rewrite or redesign anything beyond the explicit tasks**

You must** ****only** implement the tasks listed in this document.

You must NOT:

* Introduce new pipelines not specified
* Rewrite the daily pipeline
* Change working logic unless the task explicitly requires it
* “Improve” architecture unless defined inside the task list
* Remove or rename files unless a task directs you to do so

This prevents accidental regressions.

---

## 📌 2.** ****Follow a strict phased workflow**

Your work must proceed in** ** **three phases** , in this exact order:

### **Phase 1 — Understand & Plan (no code changes yet)**

* Read the entire task list.
* Produce a** ** **consolidated, ordered implementation plan** , grouping similar tasks (e.g. pipelines, DB, notifications, env cleanup, scripts cleanup, docs placeholders).
* Wait for my approval.
* Make** ****no code changes** in Phase 1.

### **Phase 2 — Implement all tasks, but in safe incremental commits**

Once I approve the plan:

You will implement the tasks** ** **in the exact order of your plan** , with the following rules:

* One commit per logical block
* Keep changes localized
* Validate each change does** ****not** break:
  * `simplify_dashboard_daily`
  * Notifications
  * PDF generation
* Update tests or create minimal tests as required
* Perform migrations safely

### **Phase 3 — Final consistency review**

After implementing everything:

* Perform a final cross-check:
  * Pipelines behavior
  * Notification integration
  * Env var cleanup
  * Legacy removal
  * DB migrations
  * Logging consistency
* Do not add new features
* Output a final summary of what was done

---

## 🛡️ 3.** ****Critical Safety Guardrails**

Codex must obey the following** ** **strict rules** :

### **(A) Daily pipeline must not break**

* Any change touching daily scraping logic, PDF generation, email routing, or DB ingestion must be strictly limited to what the task list says.
* You must test daily pipeline end-to-end after relevant edits.

### **(B) Notification engine is now the ONLY email system**

* All pipelines must use** **`send_notifications_for_run`.
* Legacy SMTP paths must be removed** ***only* as directed by tasks.

### **(C) DB migrations must be non-destructive**

* No dropping columns or tables unless absolutely confirmed safe.
* All schema alterations must use Alembic migrations.
* Always consider backward compatibility.

### **(D) No secrets in code or logs**

* Any discovered secrets must be redacted.
* Sample env vars must use placeholders.

### **(E) All date logic must use the unified timezone/date-period helper**

* Daily:** **`report_date = T-1`
* Weekly: previous Mon–Sun window
* Monthly: previous full calendar month
* No duplicating date logic across files.

### **(F) Weekly/monthly catch-up must remain idempotent**

* Code must check** **`pipeline_run_summaries` before generating backfill runs.
* No duplicate reports.
* No skipped periods.

---

## 🧩 4.** ****Deliverables**

Codex must produce:

1. **Phase 1 — Implementation plan**
   * Ordered, grouped, clear, safe.
2. **Phase 2 — Code updates**
   * All tasks implemented exactly as required.
   * Clean, readable diffs.
3. **Phase 3 — Final summary**
   * Confirm everything is production-ready.
   * Confirm no regressions.

---

## 🧘 5.** ****Tone & discipline Codex must follow**

When acting on this document, Codex MUST:

* Remain precise
* Ask for clarification ONLY if absolutely necessary
* Avoid assumptions
* Avoid rewriting working, stable code
* Keep the daily pipeline functioning at all times
* Keep commits small and focused

### 0. High-level summary

* The repo implements the** ****daily Simplify dashboard ingestion pipeline** end-to-end (Playwright download → CSV merge → ingest/audit → PDF generation → run summary → DB-backed notifications) via the** **`simplify_downloader` CLI and shared helpers in** **`common/` and** **`dashboard_downloader/`.
* Early-stage** ****weekly and monthly reporting pipelines** exist under** **`tsv_dashboard/pipelines`, reusing PDF/document abstractions but lacking orchestration hardening and prod tooling; they are invoked via separate shell scripts rather than the main CLI.
* A** ** **DB-driven notification layer** (`send_notifications_for_run`) ties** **`pipeline_run_summaries`,** **`documents`,** **`pipelines`,** **`notification_profiles`,** **`email_templates`, and** **`notification_recipients` together, yet it still depends on SMTP env vars and assumes Alembic-seeded data is in place.
* Legacy code remains (e.g., CRM downloader scaffolding, direct email helpers in** **`run_store_reports`, manual scripts) and competes with the new architecture, increasing cognitive load and risk of misconfiguration.
* Biggest production risks today:
  1. **Scheduling & config drift** – weekly/monthly pipelines rely on ad-hoc scripts and shared** **`.env` variables (`REPORT_STORES_LIST`, credentials) that are not validated or namespaced per environment, making multi-env rollout brittle.
  2. **Operational blind spots** – no automated tests or CI coverage for orchestration, notifications, or PDF rendering; JSON logs exist but there’s no alerting on failure phases and partial successes are not surfaced outside DB summaries.
  3. **Deployment gaps** – Docker Compose runs the daily CLI only and does not persist reports, secrets, or Playwright profiles; the GitHub deploy workflow performs DB migrations but never triggers pipelines or notification sanity checks.
  4. **Notification reliance on seeded data** –** **`send_notifications_for_run` silently skips sending when pipeline/profile rows are missing or misaligned with code-defined pipeline names/doc types, without migration or verification tooling.
  5. **Legacy email/env vars** – deprecated** **`REPORT_EMAIL_TO/CC` paths linger in** **`.env`and code, risking dual-send bugs if someone reuses them; the new notification system still depends on the same SMTP env vars without secrets management.

---

### 1. Pipelines & entrypoints audit

#### `simplify_dashboard_daily` (`simplify_downloader run[-single-session]`,** **`scripts/run_dashboard_pipeline_single_context.sh`)

* **Status:** `working`
* **Role:** Daily Playwright download/merge/ingest plus daily PDF & notifications.
* **Findings:**
  * *Strengths:* Single-session downloader orchestrated via** **`dashboard_downloader/pipeline.py`; each phase logs structured events and persists run summaries/metrics before and after email dispatch; ingestion/audit/cleanup hooks are centralized under** **`common/`.
  * *Weaknesses:* The CLI auto-runs Alembic migrations on every invocation (risking long startup); report generation still depends on** **`REPORT_STORES_LIST` and manual Playwright templates; notification sending is gated on having PDF attachments and a DB connection, so runs with zero PDFs never produce alerts even if other failures occur.
* **Recommended tasks:**
  1. `[MUST][MEDIUM]` Introduce pre-flight validation for required env vars (credentials, DATABASE_URL when not dry-run, Playwright storage state) before downloads start to fail mid-run. Touch** **`dashboard_downloader/cli.py`,** **`dashboard_downloader/settings.py`.
  2. `[MUST][HIGH]` Decouple automatic** **`run_alembic_upgrade` from every execution; provide a** **`--migrate` flag or document expectation so production runs don’t block on migration locks. Files:** **`dashboard_downloader/cli.py`.
  3. `[SHOULD][MEDIUM]` Ensure run summaries and notifications fire even when no PDFs were produced (e.g., ingestion-only runs) by emitting explicit “no documents” notifications instead of skipping** **`send_notifications_for_run`. Files:** **`dashboard_downloader/pipeline.py`.
  4. `[SHOULD][MEDIUM]` Remove dead direct-email helpers in** **`run_store_reports` once the new notification profiles cover daily PDFs; otherwise they risk accidental re-use. Files:** **`dashboard_downloader/run_store_reports.py`.
  5. `[NICE][LOW]` Promote weekly/monthly CLI subcommands so daily ops tooling doesn’t need separate shell scripts. Files:** **`dashboard_downloader/cli.py`,** **`scripts/`.

#### `run_store_reports_for_date` (daily PDF generator invoked from daily pipeline)

* **Status:** `working`
* **Role:** Build per-store PDF and persist** **`documents`rows for notifications.
* **Findings:** Generates PDFs only for** **`REPORT_STORES_LIST`, defaults to a fixed list, writes to** **`reports/YYYY`, and records documents via SQLAlchemy; still contains legacy email functions unused by orchestrator.
* **Recommended tasks:**
  1. `[MUST][MEDIUM]` Validate that every generated PDF produces a** **`documents` row with status** **`ok`; add logging when document persistence fails to avoid silent notification skips. Files:** **`dashboard_downloader/run_store_reports.py`.
  2. `[SHOULD][MEDIUM]` Externalize the** **`reports/` output path into config/CLI to avoid hardcoded relative directories conflicting with Docker deployments. Files:** **`dashboard_downloader/run_store_reports.py`.

#### `tsv_dashboard.pipelines.dashboard_weekly`

* **Status:** `WIP`
* **Role:** Aggregates weekly store metrics, renders PDFs, records documents, and sends notifications.
* **Findings:** Uses** **`PipelinePhaseTracker` to write summaries & metrics, checks for prior runs, and records documents/notifications; still depends on** **`REPORT_STORES_LIST` and manual invocation via shell script; no CLI wiring, tests, or config validation.
* **Recommended tasks:**
  1. `[MUST][HIGH]` Add guardrails for** **`DATABASE_URL`/`REPORT_STORES_LIST` and ensure store lists align with daily ingestion output to prevent empty weekly datasets. Files:** **`tsv_dashboard/pipelines/dashboard_weekly.py`,** **`tsv_dashboard/pipelines/reporting.py`.
  2. `[SHOULD][MEDIUM]` Persist pipeline metadata via the main CLI (or scheduler) and document cron expectations; integrate script into CI/CD. Files:** **`scripts/run_dashboard_pipeline_weekly.sh`,** **`dashboard_downloader/cli.py`.
  3. `[NICE][LOW]` Provide observability (structured logs, aggregator style) similar to daily pipeline. Files:** **`tsv_dashboard/pipelines/base.py`.

#### `tsv_dashboard.pipelines.dashboard_monthly`

* **Status:** `WIP`
* **Role:** Similar to weekly but for prior month.
* **Findings:** Shares same tracker/reporting stack; same reliance on** **`REPORT_STORES_LIST`, manual script, and missing orchestration tests.
* **Recommended tasks:** Mirror weekly tasks (env validation, CLI integration, logging/alerting), plus ensure period calculation uses explicit timezone/day overrides for predictable scheduling.

#### `tsv_dashboard.pipelines.reporting` (shared layer)

* **Status:** `working` but untested
* **Role:** Provide store selection, DB queries, PDF rendering, and document persistence for weekly/monthly pipelines.
* **Findings:** Hardcodes default stores (`["A668","A817","A526"]`) when** **`REPORT_STORES_LIST` is unset, risking accidental scope expansion; writes to** **`reports/<year>` with no retention plan; relies on Playwright templates under** **`dashboard_downloader/templates`(shared with daily).
* **Recommended tasks:** Add configuration for output root, ensure Playwright dependencies installed in long-running containers, and align default store list with production requirements.

#### `crm_downloader` scaffolding

* **Status:** `legacy / stub`
* **Role:** Directory scaffolding only, but Alembic seeds a** **`crm_downloader_daily`pipeline/notification profile despite no code executing it.
* **Recommended tasks:** Either remove the unused pipeline row or implement stubs so notification tables aren’t misleading; document status.

---

### 2. Configuration & env vars audit

#### Inventory (selected highlights)

* **Store access:** `TD_UN3668_USERNAME/PASSWORD/STORE_CODE`,** **`TD_KN3817_*`,** **`TD_STORAGE_STATE_FILENAME`,** **`TD_<STORE>_STORAGE_STATE_FILENAME`configure Playwright credentials & storage state paths.
* **Store selection:** `stores_list`/`STORES_LIST`(input for CLI) and** **`REPORT_STORES_LIST`(reporting scope).** **`REPORT_STORES_LIST` also drives weekly/monthly and daily PDF reporting defaults.
* **Pipeline behavior:** `INGEST_BATCH_SIZE`,** **`RUN_ENV/ENVIRONMENT`,** **`JSON_LOG_FILE`,** **`PDF_RENDER_BACKEND`,** **`PDF_RENDER_HEADLESS`,** **`PDF_RENDER_CHROME_EXECUTABLE`.
* **Database:** `DATABASE_URL`,** **`ALEMBIC_CONFIG`. Required for ingest, reporting, and notifications.
* **Notifications:** SMTP env vars** **`REPORT_EMAIL_SMTP_HOST/PORT/USERNAME/PASSWORD`,** **`REPORT_EMAIL_USE_TLS`,** **`REPORT_EMAIL_FROM` are used by the new notification layer; legacy** **`REPORT_EMAIL_TO/CC/SUBJECT_TEMPLATE` remain only in unused helpers.
* **Docker defaults:** `.env.example` includes placeholders for all of the above but mixes current and legacy vars without context.

#### Task list

1. `[MUST][HIGH]` Produce a canonical config reference (`docs/` or** **`.env.example`) splitting** ****required** vs** ****optional** vars, noting which pipelines consume each; remove or mark legacy-only values (`REPORT_EMAIL_TO/CC`). Files:** **`.env.example`, docs.
2. `[MUST][MEDIUM]` Add runtime validation that mutually-exclusive store selectors (`stores_list`,** **`STORES_LIST`,** **`REPORT_STORES_LIST`) are coherent per pipeline (daily vs reporting). Files:** **`dashboard_downloader/settings.py`,** **`tsv_dashboard/pipelines/reporting.py`.
3. `[SHOULD][MEDIUM]` Externalize** **`reports/`output root, Playwright profile dirs, and PDF template paths via env/CLI to align with Docker volume mounts. Files:** **`dashboard_downloader/run_store_reports.py`,** **`tsv_dashboard/pipelines/reporting.py`.
4. `[SHOULD][LOW]` Support secret stores (e.g., GitHub Actions secrets or Docker env files) for SMTP credentials instead of committing them to** **`.env`. Files:** **`.github/workflows/deploy-prod.yml`, docs.

---

### 3. Database & migrations audit

#### Key tables

* **`pipeline_run_summaries`** : Stores run metadata (`pipeline_name`,** **`run_id` unique,** **`phases_json`,** **`metrics_json`,** **`overall_status`, timestamps). Used by daily pipeline aggregator and weekly/monthly trackers to persist run history and feed notifications.
* **`documents`** : Records PDF artifacts with reference names/IDs pointing to pipeline/run/store; notifications filter on** **`status='ok'`. Daily and reporting pipelines insert rows via** **`record_documents` and** **`_persist_document_record`.
* **Notification tables (`pipelines`,** **`notification_profiles`,** **`email_templates`,** **`notification_recipients`)** : Alembic** **`0007`seeds default pipelines/profiles and recipients, including store-scoped profiles for daily/weekly/monthly and a CRM placeholder. These tables drive** **`send_notifications_for_run`.
* **Store fact tables (`store_master`,** **`store_dashboard_summary`)** : Defined in** **`common/dashboard_store.py`, they store ingested dashboard metrics used for reporting queries.

#### Migrations & schema health

* Alembic history (`0001`–`0007`) covers base tables, pipeline summaries, documents, and notification schema; Docker Compose/deploy workflow runs** **`python -m simplify_downloader db upgrade head`, but there is no automated check for pending migrations prior to pipeline execution.
* Constraints exist (e.g., unique run_id, status defaults), but no FK from** **`documents` to** **`pipeline_run_summaries` (consistency relies on naming). Notification tables rely on seeded data; no migrations ensure store_code coverage keeps pace with actual** **`store_master` entries.

#### Task list

1. `[MUST][HIGH]` Provide a lightweight DB health command (e.g.,** **`simplify_downloader db check`) verifying required tables, seed data, and notification profiles exist before pipelines run; integrate with CI/deploy. Files:** **`simplify_downloader.py`,** **`dashboard_downloader/cli.py`,** **`alembic` docs.
2. `[MUST][MEDIUM]` Document and enforce naming alignment between** **`PIPELINE_NAME` constants and** **`pipelines.code` rows, preventing silent notification skips when new pipelines are added. Files:** **`dashboard_downloader/run_summary.py`,** **`tsv_dashboard/pipelines/dashboard_weekly.py`,** **`alembic/versions/0007_notification_tables.py`.
3. `[SHOULD][MEDIUM]` Add FK/backref relationships or runtime validation to ensure** **`documents.reference_id_2` (run_id) exists in** **`pipeline_run_summaries`; include cleanup for orphaned docs. Files:** **`dashboard_downloader/db_tables.py`,** **`dashboard_downloader/run_store_reports.py`,** **`tsv_dashboard/pipelines/reporting.py`.
4. `[SHOULD][LOW]` Revisit Alembic seed data for the unused** **`crm_downloader_daily` pipeline to avoid confusion; either implement pipeline or remove seed. Files:** **`alembic/versions/0007_notification_tables.py`.

---

### 4. Notification & email system audit

#### Current implementation

* `send_notifications_for_run` loads pipeline definitions, run summaries, documents, profiles, templates, and recipients from the DB, builds per-run/store plans, and sends via SMTP configured by** **`REPORT_EMAIL_SMTP_*` env vars.
* Document-to-profile routing uses** **`STORE_PROFILE_DOC_TYPES` mapping to map pipeline+profile to the appropriate** **`doc_type`(`store_daily_pdf`,** **`store_weekly_pdf`,** **`store_monthly_pdf`).
* Daily pipeline invokes it after persisting final run summary; weekly/monthly pipelines call** **`_dispatch_notifications` once documents and summaries exist.

#### Legacy vs new

* Legacy env-driven functions (`load_email_settings`,** **`build_email_message`,** **`send_email`) remain in** **`run_store_reports` but are unused;** **`.env.example` still advertises** **`REPORT_EMAIL_TO/CC`, risking misconfiguration.
* SMTP credentials are shared between legacy and new flows, but the new system requires DB metadata that is only populated via migrations; there’s no runtime fallback when profiles/templates are absent.

#### Task list

1. `[MUST][HIGH]` Implement a diagnostic command (`simplify_downloader notifications test`) that verifies SMTP env vars, profile/template coverage, and document availability for a given run before production reliance. Files:** **`dashboard_downloader/notifications.py`, CLI.
2. `[MUST][MEDIUM]` Remove or clearly quarantine legacy direct-email helpers and their env vars once the new system is validated, to avoid double-sending. Files:** **`dashboard_downloader/run_store_reports.py`,** **`.env.example`.
3. `[SHOULD][MEDIUM]` Extend** **`_build_email_plans` to log when documents are missing for expected store codes, so missing PDF ingestion is surfaced before email send. Files:** **`dashboard_downloader/notifications.py`.
4. `[SHOULD][LOW]` Allow SMTP credentials to be sourced from secrets files or cloud KMS; mask them in logs. Files:** **`dashboard_downloader/notifications.py`, deployment docs.

---

### 5. Logging, error handling, and statuses

#### Current pattern

* Daily pipeline uses** **`log_event` per phase, aggregates counts via** **`RunAggregator`, and records downloads/audit/cleanup metrics.
* Ingestion (`common/ingest/service.py`) emits warnings when CSV parsing fails or looks like HTML; audit/cleanup modules log success/warn states.
* Weekly/monthly trackers log phase counts but rely mainly on print statements when existing runs exist; they don’t integrate with centralized logging or aggregator output beyond DB summaries.

#### Gaps / risks

* Errors in** **`_finalize_summary_and_email` only downgrade status to warning even when notifications fail; there’s no alerting or retries.
* Weekly/monthly code prints to stdout instead of structured logging when duplicates are detected.
* No facility to push aggregator issues to external alerting (e.g., Slack/email) aside from final notifications, which themselves might fail.

#### Task list

1. `[MUST][MEDIUM]` Standardize logging for all pipelines (daily/weekly/monthly) using** **`JsonLogger`, and ensure aggregator-style metrics feed into run summaries; propagate** **`run_id`,** **`pipeline_name`, and** **`phase` for every log event. Files:** **`tsv_dashboard/pipelines/*.py`,** **`dashboard_downloader/json_logger.py`.
2. `[SHOULD][MEDIUM]` Distinguish between** **`warning` vs** **`error` statuses when notifications fail, and set** **`overall_status` accordingly so downstream alerting can react. Files:** **`dashboard_downloader/pipeline.py`,** **`tsv_dashboard/pipelines/base.py`.
3. `[NICE][LOW]` Add optional log shipping (e.g., to CloudWatch) or log rotation for JSON logs referenced by** **`JSON_LOG_FILE`. Files:** **`dashboard_downloader/json_logger.py`.

---

### 6. Tests & CI

#### Tests audit

* Current tests cover CSV schema coercion and login detection heuristics only (`tests/dashboard_downloader/test_ingest_schemas.py`,** **`test_run_downloads_login_detection.py`). No tests exist for ingestion DB writes, reporting pipelines, notifications, or CLI orchestration.
* No fixtures simulate notification profiles or documents; weekly/monthly/reporting modules lack coverage entirely.

#### CI / automation

* GitHub Actions** **`CI` workflow installs dependencies via Poetry and runs** **`pytest`; no linting, typing, or integration tests. The deploy workflow only runs Alembic upgrades over SSH; it never executes pipelines post-deploy.

#### Task list

1. `[MUST][HIGH]` Add integration tests for** **`send_notifications_for_run` using a SQLite or temporary Postgres DB seeded with sample pipelines/documents to catch schema regressions. Files:** **`tests/`,** **`dashboard_downloader/notifications.py`.
2. `[MUST][MEDIUM]` Provide at least one end-to-end smoke test for** **`simplify_downloader run --dry_run` using fixtures/mocks for Playwright and DB to ensure orchestrator wiring remains intact. Files:** **`tests/dashboard_downloader`, CLI.
3. `[SHOULD][MEDIUM]` Extend CI to run lint/type checks and to spin up Postgres via services for DB tests; enforce coverage thresholds before deploy. Files:** **`.github/workflows/ci.yml`.
4. `[SHOULD][LOW]` Document how to run weekly/monthly pipelines in CI or staging (perhaps behind feature flags) to prevent drift.

---

### 7. Deployment & operations

#### Deployment artefacts

* Dockerfile installs Python/Poetry/Playwright, copies source, and runs** **`python -m simplify_downloader` by default; Compose brings up Postgres + the app container running the CLI once at startup with** **`DATABASE_URL`pointed at the Compose DB.
* Deploy workflow pulls latest code on a remote host, rebuilds Docker images, starts DB, runs Alembic upgrades, but does** ****not** execute the pipeline container or schedule jobs.

#### Operational concerns

* No mention of cron/systemd for recurring execution; Compose** **`app` command runs once and exits unless supervised.
* Reports and Playwright artifacts are stored under repo-relative paths (`reports/`,** **`dashboard_downloader/profiles`), but Compose doesn’t mount persistent volumes, so artifacts vanish between container runs.
* Secrets (SMTP creds, store logins) would need to be injected as env vars; there’s no secrets management guidance.

#### Task list

1. `[MUST][HIGH]` Define a production scheduler (cron, Airflow, etc.) or long-running service to invoke daily/weekly/monthly pipelines with retry logic; document expected cadence. Files: docs, deployment scripts.
2. `[MUST][MEDIUM]` Update Docker Compose/Dockerfile to mount persistent volumes for** **`reports/`, Playwright profiles, and logs; parameterize commands so the container can run weekly/monthly as needed. Files:** **`docker-compose.yml`, Docker docs.
3. `[SHOULD][MEDIUM]` Enhance the deploy workflow to run a post-deploy smoke test (e.g.,** **`simplify_downloader run --dry_run --stores_list ...`) and capture logs/artifacts for debugging. Files:** **`.github/workflows/deploy-prod.yml`.
4. `[SHOULD][LOW]` Provide guidance on storing secrets (SMTP, Playwright creds) in GitHub/host secrets and injecting them into containers securely.

---

### 8. Consolidated, prioritised task list

#### Pipelines

1. **Validate pipeline prerequisites before execution** (env vars, storage state, DB availability).** **`[MUST][MEDIUM]` –** **`dashboard_downloader/cli.py`,** **`dashboard_downloader/settings.py`.
2. **Decouple automatic migrations from every run**to avoid locking.** **`[MUST][HIGH]` –** **`dashboard_downloader/cli.py`.
3. **Guarantee notifications/run summaries even without PDFs** and alert on zero-doc runs.** **`[SHOULD][MEDIUM]` –** **`dashboard_downloader/pipeline.py`.
4. **Integrate weekly/monthly pipelines into the main CLI & logging stack** ; validate store lists and DB config.** **`[MUST][HIGH]` –** **`tsv_dashboard/pipelines/*.py`, CLI scripts.
5. **Retire legacy manual scripts/code** (`run_downloads_worked*.py`, direct email functions) once replacements exist.** **`[SHOULD][LOW]` –** **`dashboard_downloader/`, docs.

Dependencies: 1 & 2 should precede scheduling/ops work; 4 depends on config cleanup.

#### Config / env

6. **Publish authoritative config documentation and** **`.env.example` cleanup** , marking legacy vars.** **`[MUST][HIGH]` –** **`.env.example`, docs.
7. **Add runtime checks for conflicting store selectors and configurable output paths** for PDFs/reports.** **`[MUST][MEDIUM]` –** **`dashboard_downloader/run_store_reports.py`,** **`tsv_dashboard/pipelines/reporting.py`.
8. **Adopt secrets management for SMTP/store creds** .** **`[SHOULD][LOW]` – deployment docs/workflows.

Dependencies: 6 should precede pipeline validations (task 1).

#### DB / schema

9. **Implement DB health/check command** verifying migrations and notification seed data.** **`[MUST][HIGH]` – CLI, Alembic integration.
10. **Align pipeline constants with notification metadata and add referential validation** (documents ↔ run summaries).** **`[MUST][MEDIUM]`–** **`dashboard_downloader/run_summary.py`,** **`alembic/versions/0007_notification_tables.py`.
11. **Audit/remove unused notification rows (e.g., CRM)** .** **`[SHOULD][LOW]` – Alembic seeds.

Dependencies: 9 should run before production scheduling; 10 relies on config docs (task 6).

#### Notifications / email

12. **Add a** **`notifications test` command** to validate SMTP/profiles/docs for a run.** **`[MUST][HIGH]` –** **`dashboard_downloader/notifications.py`, CLI.
13. **Remove legacy direct-email code & env vars**once new system verified.** **`[MUST][MEDIUM]` –** **`dashboard_downloader/run_store_reports.py`,** **`.env.example`.
14. **Improve logging for missing docs/recipients per plan** .** **`[SHOULD][MEDIUM]` –** **`dashboard_downloader/notifications.py`.
15. **Support secure secrets injection for SMTP** .** **`[SHOULD][LOW]` – deployment docs.

Dependencies: 12 precedes 13 (need confidence before removal).

#### Logging / error handling

16. **Unify structured logging across all pipelines** and ensure aggregator metrics/overall_status reflect notification failures.** **`[MUST][MEDIUM]` –** **`tsv_dashboard/pipelines/*.py`,** **`dashboard_downloader/pipeline.py`.
17. **Provide optional log shipping/rotation** for long-running services.** **`[NICE][LOW]` – logging config.

Dependencies: 16 supports deployment alerts.

#### Tests / CI

18. **Add integration tests for notifications and dry-run pipeline** using temp DB/mocks.** **`[MUST][HIGH]` –** **`tests/`,** **`dashboard_downloader/notifications.py`.
19. **Extend CI to cover lint/type checks and DB-backed tests** .** **`[MUST][MEDIUM]` –** **`.github/workflows/ci.yml`.
20. **Document CI strategy for weekly/monthly pipelines** (feature flags or separate jobs).** **`[SHOULD][LOW]` – docs.

Dependencies: 18 before CI enforcement (19).

#### Deployment / ops

21. **Define production scheduling mechanism and retries** (cron/systemd/Airflow).** **`[MUST][HIGH]` – ops docs/scripts.
22. **Persist reports/logs/Playwright profiles via Docker volumes & environment-specific paths** .** **`[MUST][MEDIUM]` –** **`docker-compose.yml`,** **`dashboard_downloader/run_store_reports.py`.
23. **Enhance deploy workflow with post-deploy smoke test and log capture** .** **`[SHOULD][MEDIUM]`–** **`.github/workflows/deploy-prod.yml`.
24. **Document secrets injection and env management for deployments** .** **`[SHOULD][LOW]`– docs.

Dependencies: 21 depends on pipeline validation (tasks 1–4); 22 requires config cleanup (task 6) for paths.

---

Here are the **exact add-on points** formatted so you can **copy-paste at the end of your Step A document** and Codex will treat them as **special priority requirements**.
Worded carefully, concise, and labeled in the same `[MUST] / [SHOULD] / [NICE]` format.

---

# 🔒 **Additional Critical Requirements (To Be Appended to Step A Task List)**

Please treat the following as **extra mandatory audit items** that must be incorporated into the consolidated task list and implemented with **high attention and no deviation**:

---

### **1. Weekly & Monthly Catch-Up / Backfill Logic**

**[MUST][MEDIUM][PIPELINES]**

The weekly and monthly pipelines **must** support full **catch-up / backfill** behavior:

* Weekly pipeline must generate reports for any **missed fully completed weeks** (Mon–Sun), based on:

  * The absence of a corresponding `pipeline_run_summaries` entry.
  * Available data already ingested by the daily pipeline.
* Monthly pipeline must generate reports for any **missed fully completed months**, following the same rules.
* Each backfilled period must:

  * Insert a proper `pipeline_run_summaries` row.
  * Generate per-store PDFs (or warnings if no data).
  * Integrate into the notification engine using `send_notifications_for_run`.
* All catch-up logic must be **idempotent**, re-runnable, and safe.

This must be explicitly validated, tested, and aligned with the `report_date` (weekly = period_end Sunday, monthly = last day of month).

---

### **2. Timezone & Date-Boundary Normalization**

**[SHOULD][MEDIUM][CORE-UTILS]**

Introduce a centralized, reusable date/time helper that ensures:

* All pipelines consistently use the **same timezone** (Asia/Kolkata recommended).
* Daily pipeline always uses **T-1** of local time as dashboard `report_date`.
* Weekly periods always resolve to the **most recent fully completed Mon–Sun window**, independent of actual run time.
* Monthly periods resolve to the **previous full calendar month**, independent of run time.
* Protect against:

  * Cross-midnight runs
  * UTC/local mismatches
  * Month-end boundaries
  * Early morning cron drift

Refactor pipelines to use this helper uniformly. Add tests around month-end and week-end boundaries.

---

### **3. Security, PII, & Sensitive Artifact Handling**

**[SHOULD][MEDIUM][OPS/SEC]**

Add a focused security audit of:

* **Playwright storage state** files (cookies):

  * Must be treated as sensitive.
  * Define safe storage path, rotation guidance, and file permissions.
* **Logging hygiene**:

  * Ensure logs never contain PII fields or raw CSV rows (e.g. customer names, phone numbers).
  * Ensure errors do not dump sensitive data.
* **Secrets management**:

  * Verify that SMTP passwords, Postgres passwords, and store login credentials are only set via env vars.
  * No secrets should appear in code, logs, or sample configs.
* **Report retention**:

  * Add a simple guideline or operations note for report/PDF cleanup/rotation in production.

This does **not** require implementing advanced security tech—just ensuring the system is **safe and documented** for production use.

---

These tasks collectively form the roadmap for hardening the repo without rewriting the working daily pipeline or regressing the new notification architecture.
