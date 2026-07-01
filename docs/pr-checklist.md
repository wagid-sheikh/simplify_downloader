# Pull Request Checklist (repo-specific)

Use this before requesting review.

## 1) Scope and safety

- [ ] Changes are scoped to the intended feature/fix only (no opportunistic refactors).
- [ ] No unrelated files changed.
- [ ] No secrets, credentials, storage-state files, or generated report artifacts were committed.
- [ ] If pipeline behavior changed, I reviewed downstream effects on run summaries and notifications.

## 2) Code and architecture alignment

- [ ] Config access follows `app/config.py` SSOT rules (no new ad-hoc `os.getenv` in feature modules).
- [ ] DB access uses shared async session patterns (`app/common/db.py` helpers).
- [ ] Logging uses structured pipeline events (`JsonLogger` / `log_event`) where applicable.
- [ ] Store scope logic continues to respect `store_master` flags (no hardcoded store lists unless explicitly justified).
- [ ] Store start/launch-date logic uses `store_master.start_date`; non-null values are not overwritten without an intentional CRM/order-sync lower-bound decision.

## 3) Tests and verification

- [ ] I ran: `poetry run pytest` (or documented exactly why not possible).
- [ ] I added/updated tests for changed behavior under `tests/`.
- [ ] Existing migration tests still make sense for touched schema behavior.
- [ ] For CLI changes, I validated command paths and argument compatibility in scripts/docs.
- [ ] For orders sync profiler launches, I checked the connectivity preflight outcome first. If logs show `connectivity_preflight_failed`, treat it as infrastructure/DNS/network failure: do not debug Playwright first; verify host DNS resolution and outbound TCP/443 from the runner to `subs.quickdrycleaning.com`, `store.ucleanlaundry.com`, and `storepanel.ucleanlaundry.com`, then rerun after connectivity is restored.
- [ ] For cron report reruns, I validated mandatory regeneration (`--force` present for every report step) including retry/rescue paths.

## 4) API / contract / data impact

- [ ] I reviewed impacts to pipeline codes, notification profiles, and template expectations.
- [ ] Before enabling customer retention in production, I verified Alembic revision `0133_cfl_notif_seed` has run, no recipient email addresses are hardcoded by the seed, and operators have configured environment-appropriate active `notification_recipients` rows for the seeded `customer_retention_pipeline` / `owner_summary` profile; missing recipients must produce a safe `no_recipients` skip rather than an unintended send.
- [ ] I reviewed impacts to `pipeline_run_summaries`, `orders_sync_log`, or `documents` payload structure if touched.
- [ ] If extraction/ingest semantics changed, I reviewed dedupe/row-count/audit implications.
- [ ] For customer retention input/archive changes, I preserved move-and-remove semantics or added metadata/digest-aware discovery before allowing copy-and-retain behavior.
- [ ] For pending deliveries changes, I validated canonical eligibility: `T = vw_orders.order_date`, `default_due_date = T + 3`, no matching `sales` row, no valid actual payment proof, `vw_orders.recovery_status = 'NONE'`, `age_days <= 3`, zero-value orders included, and no dependency on `order_status == "Pending"`.
- [ ] For pending deliveries artifact changes, I verified both PDF and XLSX are attached in the same existing notification send path without changing recipients/profiles/templates.
- [ ] For reports or payment/recovery decision logic, I used `vw_orders.order_amount` and did not read raw `orders.net_amount`, `orders.gross_amount`, or `orders.adjustment` directly unless the exception was explicitly approved and documented.
- [ ] For payment/recovery report changes, I preserved or explicitly changed the current/open contract: `Actual Payments Not Found` and `Short Payments` are all-date current/open; `To Be Recovered` is all-date current/open by recovery-workflow status; none of these are constrained by Daily/MTD report date windows.
- [ ] For Daily Sales artifact changes, I verified the notification attachment set still includes the main PDF and all additive run artifacts (for example APNF PDF/XLSX, Short Payments PDF, and optional MTD Same-Day PDF) without changing recipient/profile/template routing.
- [ ] For Daily Sales target/config changes, I verified `TARGET_COMPUTE_TYPE` behavior: DB `system_config` lookup, missing/blank/invalid default to `SALES`, case-insensitive accepted values, unchanged Collections FTD/MTD/LMTD columns, and correct grouped-payment allocation for `COLLECTIONS` mode.
- [ ] If Daily Sales Target metrics changed, I verified both `TARGET_COMPUTE_TYPE='SALES'` and `TARGET_COMPUTE_TYPE='COLLECTIONS'`.
- [ ] For Daily Sales Target metrics, I verified the Target subsection header is `Target` for sales mode and `Target (actual collections)` for collections mode.
- [ ] For Daily Sales Target metrics, I verified `sale_target`/`collection_target` source selection.
- [ ] For Daily Sales Target metrics, I verified `sales_mtd` and `collection_mtd` persistence sources.
- [ ] For Daily Sales Target metrics, I verified grouped `payment_collections.order_number` allocation for collections mode.
- [ ] For Daily Sales Target metrics, I verified `payment_collections.payment_date` and `source_type` are ignored for collections-target achievement.
- [ ] For payment comparisons, I applied tolerance `1`, treated overpayments as paid in full, required matching zero-amount proof for zero-value payment proof, preserved zero-value Pending Delivery / To Be Recovered aging behavior, excluded zero-value sales/no-proof rows from APNF/Short Payment/money recovery, and used `Order Amount` as the user-facing label.
- [ ] For ingest/sync changes, any use of raw order amount columns is limited to source synchronization, reconciliation, or raw-payload audit purposes—not business reporting or payment decisions.

## 5) Deployment and ops impact

- [ ] I reviewed `.github/workflows` impact (CI/deploy assumptions still hold).
- [ ] I reviewed Docker/script implications if runtime command or env expectations changed.
- [ ] For heavy cron wrappers, each wrapper acquires only its pipeline-specific lock; no runtime path recreates retired `tmp/cron_heavy_pipelines.lock`, and rollout cleanup removes that obsolete directory only after its recorded process group is gone or safely terminated.
- [ ] I considered rollback behavior and failure modes for this change.

## 6) Documentation

- [ ] Updated canonical docs when behavior/contracts changed:
  - [ ] `/AGENTS.md`
  - [ ] `/docs/architecture.md`
  - [ ] `/docs/decision-log.md`
  - [ ] `/docs/feature-map.md`
- [ ] Updated/annotated any legacy docs touched by this PR to avoid conflicting guidance.

## 7) Migration checklist (mandatory when DB schema/data changes)

- [ ] New forward migration only (no editing old revisions).
- [ ] Did **not** modify any historical Alembic migration file.
- [ ] Verified `down_revision` points to current head/branch target correctly.
- [ ] Verified revision chain integrity before finalizing.
- [ ] Migration descriptive slug is short/safe and within 32 chars.
- [ ] Reviewed production safety (locking/backfill/runtime impact) and rollback approach.

## 8) UI/report artifacts (when applicable)

- [ ] If visual output changed (HTML/PDF/templates), included before/after evidence or sample output notes.
- [ ] If report output format changed, confirmed document persistence metadata remains correct.
