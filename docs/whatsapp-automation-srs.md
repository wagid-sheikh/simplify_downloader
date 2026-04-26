# WhatsApp Web Automation Sender — Scope Freeze & Software Requirements Specification (SRS)

## 1. Document Control

- **Version:** 1.0 (Phase-1 Scope Freeze)
- **Date:** 2026-04-26
- **Source of truth for requirements:** `docs/whatsapp-automation-requirement-discussion.md`
- **Target platform:** Existing `simplify_downloader` Python 3.12 pipeline service
- **Delivery mode:** Browser automation (Playwright + WhatsApp Web), **no commercial API/gateway**

---

## 2. Scope Freeze

### 2.1 Phase-1 In Scope (Frozen)

1. **Message categories at launch**
   - Order ready
   - Delayed order
   - Payment reminder
   - Pickup reminder
   - Lead follow-up
   - Promotional/offers
   - Feedback request
   - Invoice copy notification
   - Package balance/recharge

2. **Messaging type**
   - Both transactional and promotional messages.

3. **Store-aware behavior**
   - Per-store branding/language differences.

4. **Send time control**
   - Enforce allowed send windows.

5. **Recipient sourcing**
   - Recipient phone from existing orders/leads and/or explicit queue payload.

6. **Phone validation**
   - Normalize to E.164.
   - Reject invalid numbers at enqueue.

7. **Queue model**
   - Dedicated WhatsApp outbox table(s).
   - One queue row = one recipient.
   - Priority values: `instant`, `normal`.
   - Ordering: `instant` first, then `normal` FIFO.
   - Schedule controls: `not_before` + `not_after`.
   - Payload supports text + media metadata.

8. **Reliability policy**
   - Retry max attempts: 3.
   - Permanent failure: invalid number.
   - Other failures: retryable.
   - Dead-letter state after max retries.

9. **Idempotency/dedup**
   - Collapse accidental duplicates.

10. **Playwright session model**
    - Persistent auth/session profile on disk (QR once, reuse).
    - Runtime model keeps one browser/context for many sends.
    - Chromium strategy:
      - local: system Chrome
      - server: bundled Playwright Chromium

11. **Re-auth handling**
    - If WhatsApp QR re-auth is required:
      - send email alert,
      - stop retrying/sending until re-auth completed.

12. **Dispatch model**
    - Periodic wake-up path (15–20 minutes) for normal queue scanning.
    - Continuous instant poller (every few seconds) for `instant` priority.

13. **Template governance**
    - DB-driven templates + context variables.
    - Multilingual support at launch.
    - Persist rendered final message text for audit.

14. **Observability**
    - Integrate run summaries with `pipeline_run_summaries` under a new pipeline code.
    - Mandatory metrics: queued, sent, failed, retrying, avg queue age, instant SLA breach.
    - Alerts via email.

15. **Controls**
    - Human-like typing/rate behavior to reduce platform risk.
    - Opt-out/DND checks at enqueue and send-time.

16. **Operational profile model (sender identity buckets)**
    - Multiple logical profiles configurable, including:
      - Store Manager `{store_code}`
      - Area Manager Delhi
      - Area Manager Gurgaon
      - Feedback
      - Territory Manager
      - Sales Manager
      - Support Manager

### 2.2 Explicitly Out of Scope for Phase-1 (Future)

1. One queue row to multiple recipients.
2. Any commercial WhatsApp API/Gateway integration.
3. Full campaign orchestration UX (advanced segmentation UI, A/B, journey designer).
4. Smart adaptive retry by ML or dynamic traffic learning.
5. Cross-channel fallback orchestration (e.g., SMS/email fallback if WhatsApp fails).
6. Auto-unlock/self-heal for QR re-auth without human scan.

### 2.3 Phase-2 Candidates (Non-committed)

- Multi-recipient batch enqueue in single logical request.
- Rich media lifecycle hardening (upload cache, checksum dedup).
- More granular tenant-level throughput tuning and dynamic throttles.

---

## 3. Objectives

1. Add a production-safe WhatsApp Web sender capability to existing pipeline architecture.
2. Enable both scheduled and near-real-time dispatch (`normal` + `instant`).
3. Preserve auditability, observability, and operator control consistent with current platform conventions.
4. Minimize duplicate sends and provide deterministic queue state transitions.
5. Keep implementation extensible for later multi-recipient and campaign enhancements.

---

## 4. Functional Requirements

### 4.1 Enqueue API/Service

1. System shall provide an enqueue path callable by internal modules/pipelines.
2. Enqueue input shall include:
   - recipient phone (or foreign key resolvable to recipient)
   - template reference or explicit message payload
   - context variables
   - priority (`normal`/`instant`)
   - sender profile key
   - optional `not_before` and `not_after`
   - message category
   - optional media metadata
3. System shall normalize phone number to E.164 and reject invalid numbers.
4. System shall apply opt-out checks before enqueue.
5. System shall enforce idempotency/dedup policy and collapse duplicates.
6. System shall persist queue item with initial `queued` state.

### 4.2 Template Rendering

1. System shall render messages from DB templates with variable interpolation.
2. System shall support multilingual template variants.
3. System shall persist rendered output for audit at send attempt time.

### 4.3 Dispatcher Selection Logic

1. Dispatcher shall prefer `instant` queue first.
2. For same priority level, dispatcher shall process by FIFO (`created_at`, then `id`).
3. Dispatcher shall skip items outside send window or outside `not_before`/`not_after` constraints.
4. Dispatcher shall validate opt-out status again before send execution.

### 4.4 Send Execution via Playwright

1. Worker shall use one persistent browser profile per sender profile key.
2. Worker shall launch one browser/context per cycle and process multiple messages.
3. Worker shall support text and media send workflows.
4. Worker shall detect known send outcomes:
   - sent success
   - retryable technical failure
   - permanent failure (invalid number)
   - auth required/QR required
5. On success, item state becomes `sent`.
6. On retryable failure, state becomes `retry_pending` with computed `next_attempt_at`.
7. On permanent failure, state becomes `failed_permanent`.
8. On max retries reached, state becomes `dead_letter`.

### 4.5 Scheduler & Runtime Modes

1. System shall run periodic scheduler every 15–20 minutes for `normal` and catch-up scanning.
2. System shall run a continuous instant poller every few seconds.
3. Instant poller shall be lightweight and trigger immediate dispatch without waiting for periodic run.
4. Both runtimes shall honor locking/profile-concurrency rules to prevent conflicting session usage.

### 4.6 QR Re-auth Workflow

1. If QR re-auth is required, worker shall:
   - mark profile status `auth_required`
   - pause new sends for that profile
   - generate email alert
2. Queue items blocked by auth shall remain retryable but not actively retried until profile recovered.
3. Operator shall be able to restore profile by scanning once and flipping status back to active.

---

## 5. Non-Functional Requirements

### 5.1 Reliability

- At-least-once processing with idempotency safeguards to avoid duplicate recipient delivery.
- Recover cleanly from worker restarts without losing queue state.

### 5.2 Performance & SLA

- Instant queue pickup target: seconds-level polling.
- Normal queue pickup target: max 20-minute scheduler interval.
- Queue operations should remain index-backed for predictable latency.

### 5.3 Maintainability

- Reuse existing config, DB session, logging, and run-summary patterns from repository conventions.
- Keep module boundaries explicit: enqueue, scheduler, dispatcher, playwright adapter, observability.

### 5.4 Security & Compliance

- No secrets hardcoded in source.
- Session/profile files stored in controlled filesystem path.
- PII minimized in logs (mask phone where practical).

### 5.5 Operability

- Clear runbook-triggered states (`auth_required`, `dead_letter`, stalled queue).
- Email alerts for critical conditions.

---

## 6. Queue Schema & State Machine

### 6.1 Logical Schema (Phase-1)

Recommended table: `whatsapp_outbox_queue`

Core columns:
- `id` (PK)
- `business_key` (nullable, caller-provided dedup handle)
- `recipient_e164` (not null)
- `store_code` (nullable)
- `sender_profile_key` (not null)
- `category` (not null)
- `language_code` (not null)
- `template_code` (nullable if raw text provided)
- `template_context_json` (nullable)
- `rendered_message_text` (nullable until render/attempt)
- `media_payload_json` (nullable)
- `priority` enum: `instant|normal`
- `status` enum: `queued|picked|sending|retry_pending|sent|failed_permanent|dead_letter|expired`
- `attempt_count` (default 0)
- `max_attempts` (default 3)
- `next_attempt_at` (nullable)
- `not_before` (nullable)
- `not_after` (nullable)
- `last_error_code` (nullable)
- `last_error_detail` (nullable)
- `provider_message_ref` (nullable)
- `dedup_hash` (not null)
- `created_at`, `updated_at`

Recommended table: `whatsapp_sender_profiles`
- `profile_key` (PK)
- `display_name`
- `auth_state` enum: `active|auth_required|disabled`
- `session_path`
- `rate_policy_json`
- `created_at`, `updated_at`, `last_auth_at`

### 6.2 State Machine

`queued` → `picked` → `sending` → (`sent` | `retry_pending` | `failed_permanent` | `dead_letter` | `expired`)

Rules:
1. `expired` when current time > `not_after` before successful send.
2. `retry_pending` transitions back to `queued` when `next_attempt_at <= now`.
3. `failed_permanent` terminal.
4. `dead_letter` terminal.

---

## 7. Priority Handling

1. Two priorities only in Phase-1: `instant`, `normal`.
2. Selection order:
   - first: eligible `instant`
   - second: eligible `normal`
3. FIFO inside each priority partition.
4. Starvation guard:
   - configurable ratio window (e.g., process up to N instant consecutively, then at least M normal if pending).

---

## 8. Scheduler/Cron Design

### 8.1 Periodic Path (15–20 min)

- Cron-driven job every 15 or 20 minutes (configurable).
- Responsibilities:
  - normal queue drain
  - retry maturation pickup
  - stale lock recovery
  - aggregate run summary emission

### 8.2 Instant Path (Continuous Poller)

- Long-running loop, polling every few seconds.
- Scope: only `instant` eligible rows.
- On detection, trigger immediate dispatch cycle.
- Backoff when no work to reduce unnecessary load.

### 8.3 Concurrency & Locking

- Profile-level exclusive lock to avoid two workers sending with same profile simultaneously.
- Row-level claim/update with optimistic safeguards (`status` + timestamp + worker id).

---

## 9. Playwright Runtime & Session Management

1. Runtime must support headless mode for production.
2. Use persistent context/session directory per sender profile.
3. QR login executed once during onboarding or recovery.
4. If session invalidated and QR appears:
   - mark profile `auth_required`
   - halt sends for that profile
   - emit email alert
5. Browser strategy:
   - local development: system Chrome
   - server: bundled Playwright Chromium
6. Worker reuses browser/context for multiple sends in one run for efficiency and stability.

---

## 10. Retries, Backoff, Dead-Letter Policy

1. `max_attempts = 3` default.
2. Retryable errors include automation timeout, network instability, transient DOM mismatch, temporary send button unavailability.
3. Permanent failure includes invalid/non-WhatsApp number detection.
4. Backoff strategy: exponential with jitter (e.g., 1m, 3m, 9m + jitter).
5. After attempts exhausted, move to `dead_letter` and emit alert/metric.

---

## 11. Idempotency & Dedup Rules

1. Dedup must collapse accidental duplicate enqueue requests.
2. Recommended dedup key hash inputs:
   - `recipient_e164`
   - `template_code` or normalized message body
   - `business_key` (if provided)
   - sender profile
   - logical send window/date bucket
3. On duplicate detection in open states (`queued|picked|sending|retry_pending`), reject new row or link to existing row.
4. Intentionally repeated sends require distinct business key or dedup override flag (future extension).

---

## 12. Observability, Run Summaries, Logs, Metrics, Alerts

### 12.1 Run Summaries

- Register a dedicated pipeline code for WhatsApp sender.
- Publish periodic and instant run summaries into `pipeline_run_summaries`.
- Include counts by status transition and queue age snapshots.

### 12.2 Logs

Structured logging should include:
- run_id
- worker_mode (`periodic`/`instant`)
- queue_item_id
- profile_key
- priority
- transition
- error_code / error_class
- latency_ms

### 12.3 Metrics

Mandatory metrics:
- total queued
- sent success count
- retry pending count
- permanent failures
- dead-letter count
- avg queue age
- instant SLA breach count

### 12.4 Alerts (Email)

Trigger email alerts for:
- profile auth required (QR needed)
- dead-letter spike
- no successful sends for configurable duration
- retry spike / high failure ratio

---

## 13. Security, Compliance, and Rate Controls

1. Enforce opt-out/DND at both enqueue and pre-send.
2. Restrict sending to configured business hours.
3. Mask sensitive values in logs/audits where possible.
4. Store only required data for operational and audit needs.
5. Implement human-like typing and pacing to reduce anti-automation risk.
6. Use per-profile throughput limits (messages/minute) configurable in DB/config.

---

## 14. Error Taxonomy

Suggested normalized error classes/codes:

1. `VALIDATION.INVALID_E164` (permanent)
2. `POLICY.OPTOUT_BLOCKED` (permanent)
3. `POLICY.OUTSIDE_SEND_WINDOW` (defer/expire)
4. `SCHEDULING.NOT_BEFORE` (defer)
5. `PLAYWRIGHT.SELECTOR_TIMEOUT` (retryable)
6. `PLAYWRIGHT.NAVIGATION_ERROR` (retryable)
7. `WHATSAPP.AUTH_REQUIRED` (pause profile)
8. `WHATSAPP.NUMBER_NOT_REGISTERED` (permanent)
9. `WHATSAPP.SEND_UI_BLOCKED` (retryable)
10. `SYSTEM.LOCK_CONFLICT` (retryable)
11. `SYSTEM.UNKNOWN` (retryable with cap)

---

## 15. Operational Runbooks

### 15.1 QR Re-auth Required

1. Receive email alert.
2. Open operator console/host where session profile exists.
3. Launch assisted login flow and scan QR.
4. Verify profile state switched to `active`.
5. Resume dispatch and monitor first successful sends.

### 15.2 Dead-Letter Build-up

1. Inspect top error codes.
2. Classify into data issue vs automation issue.
3. For data issue (invalid numbers/policy), correct upstream and optionally re-enqueue.
4. For automation issue, patch selectors/runtime and replay eligible dead letters.

### 15.3 Instant SLA Breach

1. Validate instant poller process health.
2. Check DB lock contention and profile availability.
3. Inspect queue age and throughput caps.
4. Scale worker lanes per profile where safe.

---

## 16. Rollout Plan

1. **Stage 0 — Design/Data readiness**
   - Add schema, indexes, profile table, and pipeline code registration.
2. **Stage 1 — Dark launch**
   - Enable enqueue + observability with send disabled (dry run render/validate).
3. **Stage 2 — Controlled pilot**
   - Activate 1–2 profiles/categories, low throughput caps.
4. **Stage 3 — Expand transactional categories**
   - Roll out to all transactional types.
5. **Stage 4 — Add promotional traffic**
   - Enable promotional under stricter windows/throttles.
6. **Stage 5 — Stabilization**
   - Tune retries, rate limits, and alert thresholds.

---

## 17. Testing Strategy

### 17.1 Unit Tests

- Phone normalization and E.164 validation.
- Dedup hash generation and duplicate collapse behavior.
- Retry/backoff schedule calculation.
- Priority/FIFO selector logic.

### 17.2 Integration Tests

- Enqueue to dispatch state transitions.
- `not_before`/`not_after` enforcement.
- Opt-out checks (enqueue + send).
- Run summary write path.

### 17.3 End-to-End/Automation Tests

- Playwright send happy path with controlled test account.
- QR-expired flow and `auth_required` pause behavior.
- Media send path.

### 17.4 Operational Tests

- Cron periodic job execution.
- Instant poller responsiveness.
- Email alert triggers for auth/dead-letter/no-send.

---

## 18. Acceptance Criteria (Phase-1 Exit)

1. Queue supports `instant` + `normal`, FIFO ordering, and schedule gates.
2. Invalid numbers rejected pre-enqueue; dedup collapse works.
3. Playwright worker sends text and media via persistent sessions.
4. QR re-auth scenario pauses profile and sends email alert.
5. Retry policy (3 attempts) and dead-letter behavior verified.
6. Opt-out enforced at enqueue and pre-send.
7. Periodic path (15–20 min) and instant poller both operational.
8. Run summaries and mandatory metrics available.
9. Operational runbooks validated by dry-run drills.
10. No commercial API/gateway dependency introduced.

---

## 19. Open Configuration Knobs (Implementation-Time, Non-Blocking)

1. Exact periodic cron value: 15 min vs 20 min (config default can be 15).
2. Instant poll interval in seconds.
3. Per-profile throughput caps and typing delay ranges.
4. Send window definitions per store/profile/timezone.
5. Dedup time bucket policy for promotional vs transactional categories.

