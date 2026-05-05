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
- **Notes/Risks:** Dedupe and coercion behavior impacts data quality and audit counts.

## 4) Store dashboard summary persistence

- **Purpose:** Persist daily dashboard KPI snapshot per store.
- **Primary paths:**
  - `app/common/dashboard_store.py`
- **Related tables:** `store_master`, `store_dashboard_summary`.
- **Notes/Risks:** `store_code` normalization and upsert semantics are critical.

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

## 7) Orders sync run profiler (window orchestrator)

- **Purpose:** Run TD/UC sync in date windows, aggregate status, detect missing windows, notify.
- **Primary paths:**
  - `app/crm_downloader/orders_sync_run_profiler/main.py`
  - `app/crm_downloader/orders_sync_window.py`
- **Dependencies:** `orders_sync_log`, `pipeline_run_summaries`, notification profiles.
- **Notes/Risks:** Concurrency + retry + status rollups can produce subtle operational edge cases.

## 8) Daily/weekly/monthly/pending reporting

- **Purpose:** Generate PDFs and persist/send report artifacts.
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
- **Force toggle:** Report wrappers now support `REPORT_FORCE=true|false` (default `false`) and append `--force` only when enabled; cron retries/rescue passes preserve the same force mode and log `pipeline`, `report_date`, and `force`.
- **Dependencies:** `documents` table, report notification templates/profiles.
- **Notes/Risks:** Rendering failures and zero-data scenarios are handled differently per pipeline; keep behavior consistent. For same-day table layout, `app/reports/daily_sales_report/templates/daily_sales_report.html` and `app/reports/shared/templates/same_day_fulfillment_table.html` are the authoritative sources (legacy standalone same-day template removed). Pending deliveries now always includes TD+UC rows and excludes recovery-status orders (`TO_BE_RECOVERED`, `TO_BE_COMPENSATED`, `RECOVERED`, `COMPENSATED`, `WRITE_OFF`) from main aging buckets/details.

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
  - `app/dashboard_downloader/db_tables.py`
- **Tables:** `pipelines`, `notification_profiles`, `email_templates`, `notification_recipients`, `documents`, `pipeline_run_summaries`.
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

- Daily and MTD same-day fulfillment outputs now include Net Amount and Payment Received columns (payment rows are summed per order for deterministic multi-payment reporting).

- Query portability: daily same-day line-item/payment-mode concatenation is dialect-aware (`string_agg` on PostgreSQL, `group_concat` on SQLite) while preserving existing same-day grouping and payment sum behavior.
- Failure propagation policy: `scripts/cron_run_orders_and_reports.sh` must exit non-zero when any required report pipeline fails after retries (daily sales, MTD same-day, pending deliveries), and optional rescue attempts cannot mask a failed required daily run.
- Attachment contract: Daily Sales email has two distinct artifacts—(1) in-report same-day section scoped to report date, and (2) tailed MTD same-day attachment scoped from month start through report date, with separate metadata/doc_type.
