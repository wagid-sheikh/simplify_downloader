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
| Store selection | `--stores_list` CLI flag or `STORES_LIST` env **(daily)**, `REPORT_STORES_LIST` **(reporting/PDF/notifications)** | Provide at least one comma-separated store code via CLI or env before a run. |
| Base endpoints | `TD_BASE_URL`, `TD_LOGIN_URL`, `TD_STORE_DASHBOARD_PATH` | Required for routing the shared session through CRM login and the TMS dashboards. |
| Notifications | `REPORT_EMAIL_FROM`, `REPORT_EMAIL_SMTP_HOST`, `REPORT_EMAIL_SMTP_PORT`, `REPORT_EMAIL_SMTP_USERNAME`, `REPORT_EMAIL_SMTP_PASSWORD`, `REPORT_EMAIL_USE_TLS` | SMTP transport only. Recipients/templates live in the database. |

## 2. Optional but recommended

| Category | Variable(s) | Notes |
| --- | --- | --- |
| Reports & artifacts | `REPORTS_ROOT`, `JSON_LOG_FILE` | Point both at persistent volumes so Docker/Compose deployments keep history. |
| PDF rendering | `PDF_RENDER_BACKEND`, `PDF_RENDER_HEADLESS`, `PDF_RENDER_CHROME_EXECUTABLE` | Tune based on whether Chrome is system-installed or bundled. |
| Dashboard endpoints | `TD_BASE_URL`, `TD_LOGIN_URL`, `TD_STORE_DASHBOARD_PATH`, `TD_HOME_URL` | Override only in staging where URLs differ. |
| Batch tuning | `INGEST_BATCH_SIZE` | Adjust ingestion chunking for constrained CPUs. |

## 3. Runtime validation guarantees

* `dashboard_downloader.settings.load_settings` now rejects conflicting
  `--stores_list` / `STORES_LIST` definitions and verifies that every
  `REPORT_STORES_LIST` entry exists in the scraping scope to avoid generating
  PDFs for stores with no data.
* Reporting pipelines call `tsv_dashboard.pipelines.reporting.get_report_store_codes`
  which **requires** `REPORT_STORES_LIST`; this prevents weekly/monthly runs
  from silently defaulting to stale store lists.
* `REPORTS_ROOT` is configurable everywhere (`dashboard_downloader/run_store_reports.py`
  and `tsv_dashboard/pipelines/reporting.py`), so containers can mount a shared
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
