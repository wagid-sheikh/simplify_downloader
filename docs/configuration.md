# Configuration & Security Reference

This document consolidates every runtime input used by the Simplify pipelines so
operators can bootstrap new environments without guessing which `.env`
variables matter. Use it together with `.env.example`, which now separates
required vs optional settings.

## 1. Required settings

| Category | Variable(s) | Notes |
| --- | --- | --- |
| Database | `DATABASE_URL`, `ALEMBIC_CONFIG` | Needed for ingestion, summaries, document persistence, and notifications. |
| Timezone | `PIPELINE_TIMEZONE` | All helpers in `common/date_utils.py` use this timezone to compute daily/weekly/monthly periods. Default is `Asia/Kolkata`. |
| Scraping credentials | `TD_GLOBAL_USERNAME`, `TD_GLOBAL_PASSWORD` | Single CRM login for every store. `TD_GLOBAL_USERNAME` also doubles as the primary store code. |
| Store selection | `store_master.etl_flag` (daily ingestion) and `store_master.report_flag` (reports/notifications) | Flag stores in the database to control which codes run; there is no CLI override such as `--stores_list`. |
| Base endpoints (CRM) | `TD_BASE_URL`, `TD_LOGIN_URL`, `TD_HOME_URL` | Required for routing the shared session through CRM login. |
| MIS endpoints | `TMS_BASE`, `TD_STORE_DASHBOARD_PATH` | Required for navigating to the TMS dashboards and CSV downloads. |
| Notifications | `REPORT_EMAIL_FROM`, `REPORT_EMAIL_SMTP_HOST`, `REPORT_EMAIL_SMTP_PORT`, `REPORT_EMAIL_SMTP_USERNAME`, `REPORT_EMAIL_SMTP_PASSWORD`, `REPORT_EMAIL_USE_TLS` | SMTP transport only. Recipients/templates live in the database. |

Notification profiles and run summaries now standardise on the
`dashboard_daily`, `dashboard_weekly`, and `dashboard_monthly` pipeline codes
(previously `simplify_dashboard_*`). Use those identifiers when seeding
`notification_profiles` and validating `pipeline_run_summaries` entries.

## 2. Optional but recommended

| Category | Variable(s) | Notes |
| --- | --- | --- |
| Reports & artifacts | `REPORTS_ROOT`, `JSON_LOG_FILE` | Point both at persistent volumes so Docker/Compose deployments keep history. |
| PDF rendering | `PDF_RENDER_BACKEND`, `PDF_RENDER_HEADLESS`, `PDF_RENDER_CHROME_EXECUTABLE` | Tune based on whether Chrome is system-installed or bundled. Cron/non-interactive runs will still force headless mode on as a safety override. |
| Dashboard endpoints | `TD_BASE_URL`, `TD_LOGIN_URL`, `TD_HOME_URL`, `TMS_BASE`, `TD_STORE_DASHBOARD_PATH` | Override only in staging where URLs differ. |
| Batch tuning | `INGEST_BATCH_SIZE` | Adjust ingestion chunking for constrained CPUs. |

## 2.1 Cron environment configuration

The cron runner scripts (`scripts/cron_run_orders_and_reports.sh` and
`scripts/cron_run_td_leads_sync.sh`) load a small environment file before they
execute so that cron does not rely on machine-local paths. Copy `scripts/cron.env.example` to `scripts/cron.env`, set the values for
the cron user, and optionally point cron at a custom `ENV_FILE` path.

Recommended variables for the cron env file:

| Variable | Description |
| --- | --- |
| `CRON_HOME` | Stable home directory for the cron user; the script exports this to `HOME`. |
| `CRON_PATH` | Extra PATH entries needed for Poetry/Python. |
| `ENV_FILE` | (Optional) Override the env file path; defaults to `scripts/cron.env`. |
| `TD_LEADS_MAX_RUNTIME_SECONDS` | TD-leads sync watchdog; defaults explicitly to `300`. A deprecated direct per-invocation `MAX_RUNTIME_SECONDS` compatibility override takes precedence when it is set. |
| `TD_LEADS_BROWSER_OPERATION_TIMEOUT_SECONDS` | DB-backed Python deadline for each ordinary TD-leads Playwright operation, including context/page creation, session checks, login/home waits, and each status-bucket scrape; defaults to `90`. This is intentionally separate from the outer shell watchdog. |
| `TD_LEADS_BROWSER_CLEANUP_TIMEOUT_SECONDS` | DB-backed Python deadline for each TD-leads Playwright context/browser cleanup phase; defaults to `10`. |
| `TD_LEADS_STORE_WORKER_TIMEOUT_SECONDS` | DB-backed Python deadline for one TD-leads store worker; defaults to `240`. |
| `TD_LEADS_GATHER_TIMEOUT_SECONDS` | DB-backed Python deadline for the complete TD-leads worker collection; defaults to `270`. Pending workers are cancelled before failed-run summary persistence and notification delivery; keep this below the shell watchdog configured for the deployment workload. |
| `ORDERS_STEP_TIMEOUT_SECONDS` | Per-attempt watchdog for the orders profiler step; defaults to `5400`. Timed-out attempts are retryable until `ORDERS_MAX_ATTEMPTS` is exhausted. |
| `DAILY_SALES_STEP_TIMEOUT_SECONDS` | Per-attempt watchdog for Daily Sales report generation; defaults to `1800`. |
| `PENDING_DELIVERIES_STEP_TIMEOUT_SECONDS` | Per-attempt watchdog for Pending Deliveries report generation; defaults to `1800`. |
| `ORDERS_SYNC_PROFILER_SHUTDOWN_TIMEOUT_SECONDS` | Bound for each profiler loop-shutdown phase; defaults to `5`. |
| `TD_LEADS_STALE_OWNER_SECONDS` | TD-leads local-lock age threshold before strict stale-owner process-group recovery; defaults to `300` seconds to support the 10–20 minute service objective conservatively. |
| `ORDERS_REPORTS_STALE_OWNER_SECONDS` | Separately reviewed orders/reports local-lock age threshold before strict stale-owner process-group recovery; defaults to `7200` seconds because normal workload and retries differ from TD leads. |
| `STALE_OWNER_TERM_WAIT_SECONDS` | Bounded grace period after sending `TERM` to a validated stale-owner process group; defaults to `5`. |
| `STALE_OWNER_KILL_WAIT_SECONDS` | Bounded verification period after escalating a surviving validated stale-owner process group to `KILL`; defaults to `5`. |

Both cron wrappers use a pipeline-local recovery state machine rather than a
long lock-wait loop. Each wrapper first attempts `mkdir` for its lock. On
contention it logs PID, PGID, command, host, start timestamp, and calculated
age. A fully dead owner is cleaned up and reacquired immediately. A younger
live owner is preserved and causes a successful
`status=skipped_due_to_active_same_pipeline_owner` exit. An aged live owner is
terminated as a complete process group only after its repository-wrapper
command and live PID/PGID relationship validate; `TERM` is followed by bounded
wait and `KILL` escalation when required. Malformed, mismatched, unrelated, or
otherwise ambiguous ownership always fails safely without deleting the lock.
The TD-leads run step also launches in a dedicated session. Its watchdog sends
`TERM` and, when any non-zombie group member survives the bounded grace period,
`KILL` to that complete process group before verifying shutdown and allowing the
cleanup trap to remove the TD-leads lock.

Example crontab entry (runs at 6:00 AM daily and logs via the script):

```bash
0 6 * * * ENV_FILE=/opt/simplify/cron.env /opt/simplify/scripts/cron_run_orders_and_reports.sh
```

When the runtime detects a non-interactive environment (for example, cron with
`CRON_TZ` set, no `SHELL`, `TERM=dumb`, or SSH sessions without a GUI), it
automatically forces both `ETL_HEADLESS` and `PDF_RENDER_HEADLESS` to `true`.
The override is logged once at startup so automated runs avoid hanging on
headful browser prompts.

## 3. Runtime validation guarantees

* `dashboard_downloader.settings.load_settings` resolves stores from
  `store_master.etl_flag` and verifies that all `report_flag` stores are included
  in the scraping scope to avoid generating PDFs for missing data.
* Store selection is database-driven only. Legacy CLI selectors (for example,
  `--stores_list`) are retired; keep flags updated in `store_master` instead of
  passing ad-hoc lists.
* Operators can inspect the active DB-driven dashboard scope with
  `python -m app stores diagnose`. The command prints ETL-enabled, report-enabled,
  and report-eligible (`etl_flag=true` and `report_flag=true`) counts and store codes.
  Zero report-eligible stores is non-fatal: report generation is skipped with an
  actionable warning so intentionally disabled reporting does not stop ingestion.
* TLS verification for TMS traffic is enabled by default. If Playwright hits a
  certificate failure at runtime, `navigate_with_retry` will recreate the
  browser context with HTTPS checks disabled for that retry only—there is no
  environment variable or CLI flag to permanently skip verification.
* Reporting pipelines call `app.dashboard_downloader.pipelines.reporting.get_report_store_codes`
  which pulls stores from `store_master.report_flag`; weekly/monthly runs no
  longer fall back to any system_config entry.
* `REPORTS_ROOT` is configurable everywhere (`dashboard_downloader/run_store_reports.py`
  and `app/dashboard_downloader/pipelines/reporting.py`), so containers can mount a shared
  volume without touching code.
* `common/date_utils.py` centralises daily/weekly/monthly date math, ensuring all
  pipelines use the same timezone-aware T-1 / Mon–Sun / full-month windows.

## 4. Secrets, PII, and artefact hygiene

* **Playwright storage state** (`TD_STORAGE_STATE_FILENAME`) contains authenticated cookies.
  Store it on a secure volume with `chmod 600` semantics, rotate it whenever credentials
  change, and never commit it to git. The default location sits under `dashboard_downloader/profiles`
  so that containers can mount a dedicated secrets volume in production.
* **Logs** produced by `JsonLogger` should be routed to an environment-specific
  location (e.g., `JSON_LOG_FILE=/var/log/simplify/downloader.jsonl`). The
  ingestion and reporting pipelines never log raw CSV rows or customer data;
  review new log sinks to ensure downstream shipping (CloudWatch, Stackdriver,
  etc.) inherits the same sanitisation rules.
* **SMTP + database credentials** must only live in `.env`, docker secrets, or a
  secrets manager (GitHub Actions, AWS SSM, etc.). Never hard-code them inside
  the repository. `.env.example` keeps blanks/placeholder hostnames to make it
  obvious which variables require secure values.
* **Reports & PDFs** under `REPORTS_ROOT` contain business metrics. Mount the
  directory on a persistent, access-controlled volume and define a retention
  rotation (e.g., cron job that removes files older than 90 days) that complies
  with company policies.

Following these guardrails keeps the daily, weekly, and monthly pipelines aligned
and prevents configuration drift between environments.
