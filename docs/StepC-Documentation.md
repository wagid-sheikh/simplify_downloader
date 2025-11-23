
## 📚 Prompt for Codex – Full Documentation Pass (Step C)

You are a **senior engineer + technical writer** working on:

> GitHub repo: `wagid-sheikh/simplify_downloader`

Assume:

* The **production-readiness audit (Step A)** has already been done.
* The **implementation/fix tasks (Step B)** have been applied and merged into `main`.
* The code on `main` is now the **source of truth** for how the system actually works.

Your job in this step is **ONLY documentation**:

> You may **create and edit documentation files**, but you must **NOT modify any source code, configs, or tests**.

---

## 🚫 Hard Guardrails

1. **No code changes**

   * Do NOT edit `.py`, `.sh`, migration files, or any code/config.
   * Only create or update documentation files: `README.md`, `AGENT.md`, files under `docs/`, etc.
2. **Docs must reflect current reality**

   * All documentation must be based on the **actual code and scripts in `main`**, not on old assumptions.
   * If existing docs conflict with the code, **the code wins**. Update/replace the docs accordingly.
3. **Ignore legacy documentation content**

   * Treat existing `README.md` / docs as **drafts only**.
   * You may reuse pieces if they’re still correct, but you are allowed (and encouraged) to **rewrite from scratch** for clarity and accuracy.
4. **No secrets / real credentials**

   * If you show `.env` examples, **never** include real passwords, tokens, or personal emails.
   * Use placeholders like `SMTP_USERNAME`, `SMTP_PASSWORD`, `DB_USER`, `DB_PASSWORD`, etc.

---

## 🎯 Documentation Goals

Your goal is to get this repo to a place where:

* A **new engineer** (or future me) can understand:

  * What this system does.
  * How the pipelines work (daily, weekly, monthly, CRM).
  * How notifications/emails flow.
  * How the DB tables relate to the pipelines.
  * How to run it locally and in production.
* A **future AI agent (Codex/GPT)** has a clear **AGENT.md** that explains:

  * Architecture.
  * Constraints.
  * Design decisions.
  * How to safely extend features (without breaking core pipelines).

---

## 📦 Documentation Artefacts to Create/Update

Create or fully update the following files:

1. **`README.md`** (top-level)
2. **`AGENT.md`** (top-level)
3. **`docs/architecture.md`**
4. **`docs/pipelines.md`**
5. **`docs/configuration.md`**
6. **`docs/db-schema.md`**
7. **`docs/operations.md`**
8. **`CONTRIBUTING.md`** (top-level; optional but recommended)
9. If a `CHANGELOG.md` already exists, leave it as-is or add a short entry at the top summarising the “Production readiness & docs overhaul” (no code edits required).

Below is what each file should contain.

---

### 1. `README.md` – Project Overview & Quickstart

Audience: **human developers and operators**.

Include:

1. **Project summary**

   * 1–2 paragraphs describing:

     * That this is a **Tumbledry dashboard/reporting automation**.
     * It uses **Playwright** to log into Tumbledry, downloads dashboard CSVs, ingests into **PostgreSQL**, and generates **PDF reports** plus **notification emails**.
     * It supports:

       * Daily single-session dashboard pipeline (production-ready).
       * Weekly and monthly reporting pipelines (if implemented in Step B).
       * CRM/downloader pipeline (if present).
2. **Key components**

   * Bullet list describing:

     * Playwright scraping layer.
     * ETL / ingestion layer.
     * Reporting layer (PDF generation).
     * Notification & email system (based on DB-driven profiles).
     * DB schema & Alembic migrations.
3. **Getting started**

   * Prerequisites:

     * Python version
     * Poetry / pip (whatever is actually used)
     * Postgres
     * Playwright browser setup if needed.
   * Setup steps:

     * Clone repo.
     * Create virtual env / install dependencies.
     * How to set up `.env` from `.env.example`.
     * How to run migrations.
     * How to run Playwright install if required.
4. **How to run pipelines locally**

   * Show the **canonical commands** for:

     * Daily pipeline (e.g. `./scripts/run_dashboard_pipeline_single_context.sh`).
     * Weekly / monthly pipelines (if scripts exist).
     * Any CRM pipelines if present.
   * For each, give:

     * Short description (what it does).
     * Expected schedule (e.g. daily at 09:00, weekly Monday 09:00).
5. **High-level architecture link**

   * Short note pointing to `docs/architecture.md` and `docs/pipelines.md` for details.
6. **Status & future work**

   * Brief note:

     * What is considered “production-ready”.
     * What areas are still in active development.

---

### 2. `AGENT.md` – Guide for Future AI Agents

Audience: **future Codex / GPT agents** working on this repo.

Include:

1. **Purpose of this file**

   * Explain that this is a guide for AI tools to safely work on this repo.
2. **Core architecture recap** (short and focused)

   * Daily pipeline: what it does, where it lives (e.g. `tsv_dashboard/pipelines/...`).
   * Weekly & monthly pipelines: high-level behaviour.
   * Notification system: where `send_notifications_for_run` lives and how it’s used.
   * DB schema: main tables (not all tables, only pipeline-critical ones).
3. **Absolute constraints / do-not-break rules**

   * Daily pipeline **must remain stable** and production-ready.
   * Do not introduce a second daily scraping pipeline.
   * Always respect the **single-session Playwright model**.
   * Notifications must go through the **DB-driven notification system**, not old direct SMTP hacks.
   * Any changes involving DB schema must:

     * Go through Alembic migrations.
     * Maintain backward compatibility where possible.
4. **Safe extension patterns**

   * How to add a new pipeline (e.g. monthly B2B report) without breaking existing ones:

     * Create new pipeline module under `tsv_dashboard/pipelines`.
     * Use shared helpers for DB, PDF, notifications.
     * Insert entries into `pipelines`, `notification_profiles`, etc.
   * How to safely add new env vars (update `.env.example`, docs, and settings).
5. **What to do BEFORE touching things**

   * Always read:

     * `docs/architecture.md`
     * `docs/pipelines.md`
     * `docs/configuration.md`
   * Always run tests and linting before/after code changes.
6. **When in doubt**

   * Recommendation: prefer adding new functionality behind feature flags / envs.
   * Log clearly and avoid silent failure.

---

### 3. `docs/architecture.md` – System Architecture

Audience: engineers who want to understand the big picture.

Describe:

1. **Overall flow**

   * Tumbledry web → Playwright scraper → CSV files → ETL ingestion → Postgres → Aggregation → PDF reports → Notification / email.
   * A textual diagram (e.g. using bullet points or ASCII-style).
2. **Major components**

   * Scraper / downloader layer (mention key modules).
   * ETL / ingest layer (how CSVs are parsed and upserted).
   * Reporting layer (templates + Playwright/Chromium for PDF).
   * Notification engine (how it looks up profiles/templates/recipients).
   * DB + migrations (Alembic).
3. **Component boundaries**

   * Which modules are allowed to depend on which others (to avoid circular mess later).
4. **Assumptions**

   * E.g. dashboard date = T-1; PDF paths under `reports/YYYY/...`.

---

### 4. `docs/pipelines.md` – Pipelines Details

Audience: devs/ops who run or modify pipelines.

For each pipeline that actually exists now (post-Step B):

* **`simplify_dashboard_daily`**

  * Purpose.
  * Schedule.
  * Input data (yesterday’s dashboard).
  * Steps:

    * Reuse session or login (Playwright).
    * Download CSVs for buckets.
    * Merge/ingest/audit.
    * Generate per-store PDFs.
    * Insert `pipeline_run_summaries` and `documents`.
    * Call `send_notifications_for_run`.
  * Idempotency rules.
  * Failure behaviour (`overall_status` logic).
  * Where logs go and how to inspect them.
* **`simplify_dashboard_weekly`** (if implemented)

  * Period logic (Mon–Sun previous week).
  * Data source (DB only).
  * No scraping.
  * Per-store and ALL-stores PDF.
  * Interaction with notifications.
* **`simplify_dashboard_monthly`** (if implemented)

  * Period logic (previous calendar month).
  * Same structure as weekly.
* **Any CRM/downloader pipelines** (if present)

  * What they do and what they don’t do.
  * How they relate to the rest of the system.

Include short “how to run manually” snippets for each (just the CLI / script names – details stay in README).

---

### 5. `docs/configuration.md` – Env & Settings

Audience: devs, ops, and infra.

Include:

1. **Canonical env var list**

   * Grouped logically:

     * Database (e.g. `DATABASE_URL`).
     * Playwright / browser (e.g. `PDF_RENDER_BACKEND`, `PDF_RENDER_HEADLESS`, etc.).
     * Notifications & SMTP (e.g. SMTP host/port/user/pass, if still relevant).
     * Any store-related configs (store lists, codes).
     * Any environment indicator (e.g. `RUN_ENV`, `APP_ENV`).
2. **For each variable:**

   * Name
   * Required? (yes/no)
   * Default behaviour if unset
   * Short description
   * Example (using safe placeholder values)
3. **Sample `.env.example` alignment**

   * Make sure everything listed here matches the actual `.env.example` file in repo.
   * If `.env.example` is outdated, update it to match this doc (still: no secrets).

---

### 6. `docs/db-schema.md` – Key Tables

Audience: devs who touch DB or analytics.

Document at least these tables (if they exist):

* `pipeline_run_summaries`
* `documents`
* `pipelines`
* `notification_profiles`
* `email_templates`
* `notification_recipients`
* Any main fact tables used by daily/weekly/monthly reporting (e.g. dashboard summary, missed leads, undelivered, repeat customers).

For each table:

* Purpose (1–2 sentences).
* Key columns and their meaning.
* Important constraints / indexes.
* How it relates to pipelines (read/write).

This does not need to be a full DB dump – focus on the tables critical to pipeline logic.

---

### 7. `docs/operations.md` – Running in Production

Audience: ops / SRE / you-in-prod-mode.

Include:

1. **Execution model**

   * How pipelines are scheduled:

     * Daily – cron example
     * Weekly – cron example
     * Monthly – cron example
   * Any relevant arguments/envs to differentiate `dev` vs `prod`.
2. **Deployment model**

   * If Docker is used: high-level description of images, compose, etc.
   * If run on a VM: mention systemd/cron pattern, log file locations.
3. **Monitoring & health**

   * How to check recent runs:

     * Query `pipeline_run_summaries` for last N runs.
     * Interpreting `overall_status`.
   * Where logs are written (stdout, files, external logging system).
4. **Common operational tasks**

   * How to:

     * Re-run a daily pipeline for a specific date (if supported).
     * Backfill weekly/monthly (if supported).
     * Rotate old reports (PDFs) if needed.

---

### 8. `CONTRIBUTING.md` – For Human Contributors

Keep it simple:

* How to set up dev environment.
* How to run tests.
* Coding style expectations (if any).
* How to propose changes:

  * Ensure daily pipeline remains stable.
  * Add tests for new pipelines/features.
  * Update docs when behaviour changes.

---

## ✅ Output Expectations

When you’re done, you should:

1. Show the **full contents** of each newly created or updated doc file in your response:

   * `README.md`
   * `AGENT.md`
   * All `docs/*.md`
   * `CONTRIBUTING.md` (if created)
2. Make sure:

   * They are internally consistent with each other.
   * They reference the actual scripts/modules/functions as they exist on `main`.
   * They use **current** naming conventions (e.g. `simplify_dashboard_daily` vs any older names you find).

Remember:
This is a **documentation-only pass**.
No code changes; just high-quality, accurate, production-grade docs.
