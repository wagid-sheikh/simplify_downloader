# Engineering Decision Log

## How to use this file

- Record durable engineering decisions that affect architecture, data contracts, or operational safety.
- Keep entries evidence-based. If inferred from code (not explicitly documented), mark as reconstructed.
- Add new entries at the top.

---

## Initial reconstructed decisions

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
