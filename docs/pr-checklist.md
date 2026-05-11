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

## 3) Tests and verification

- [ ] I ran: `poetry run pytest` (or documented exactly why not possible).
- [ ] I added/updated tests for changed behavior under `tests/`.
- [ ] Existing migration tests still make sense for touched schema behavior.
- [ ] For CLI changes, I validated command paths and argument compatibility in scripts/docs.
- [ ] For report reruns, I validated `REPORT_FORCE` behavior (`--force` present only when expected) including cron retry/rescue paths.

## 4) API / contract / data impact

- [ ] I reviewed impacts to pipeline codes, notification profiles, and template expectations.
- [ ] I reviewed impacts to `pipeline_run_summaries`, `orders_sync_log`, or `documents` payload structure if touched.
- [ ] If extraction/ingest semantics changed, I reviewed dedupe/row-count/audit implications.
- [ ] For pending deliveries changes, I validated recovery-status exclusions (`TO_BE_RECOVERED`, `TO_BE_COMPENSATED`, `RECOVERED`, `COMPENSATED`, `WRITE_OFF`) in both summary buckets and detailed rows.
- [ ] For reports or payment/recovery decision logic, I used `vw_orders.order_amount` and did not read raw `orders.net_amount`, `orders.gross_amount`, or `orders.adjustment` directly unless the exception was explicitly approved and documented.
- [ ] For payment comparisons, I applied tolerance `1`, treated overpayments as paid in full, excluded zero-value orders from missing-payment/pending-payment/recovery action checks, and used `Order Amount` as the user-facing label.
- [ ] For ingest/sync changes, any use of raw order amount columns is limited to source synchronization, reconciliation, or raw-payload audit purposes—not business reporting or payment decisions.

## 5) Deployment and ops impact

- [ ] I reviewed `.github/workflows` impact (CI/deploy assumptions still hold).
- [ ] I reviewed Docker/script implications if runtime command or env expectations changed.
- [ ] For heavy cron wrappers, lock hierarchy is preserved: global lock (`tmp/cron_heavy_pipelines.lock`) first, then per-script lock, then run steps; logs clearly distinguish global vs local lock waits.
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
