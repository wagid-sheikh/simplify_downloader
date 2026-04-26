# WhatsApp Automation Requirement Discussion (Working Draft)

## Purpose
This document captures:
1. Your original request/prompt.
2. The discovery questions raised during brainstorming.
3. A place for your answers in the same file.

After you fill this, we will use it as the baseline to finalize scope and then prepare a full SRS.

---

## 1) Original Request / Prompt (Verbatim)

> Deeply review entire repo from the angle of sending WhatsApp messages to the customers. Ask questions, I will answer them. Then you finalise the scope of work. I intend to develop a playwright based WhatsApp message sender and automation. Then you write that requirements/scope of work in a markdown file in extremely structured and orgnanized manner so that WhatsApp message sender module can be developed. I do not intend to integrate with any Commercially available API/Gateway etc. I want to develop a browser based WhatsApp messages wherein playwright works in headless mode and keeps sending message on whatever it finds in the queue. We will create a cron that wakes up WhatsApp message sender on every 15-20 minutes intervals to see if anything in the queue and sends the message, also this another sub-routine of this would developed that keeps looking/polling message queue with "instant" priority and sends them instantly. This is will be brainstorming and Q&A session to freeze scope of work.

---

## 2) Discovery Questions (To Freeze Scope)

> **Instructions:** Please write your answers under each question in the `Answer:` block.

### A) Business + sending policy

#### Q1. What exact message categories do you want first?
Examples: order ready, delayed, payment reminder, pickup reminder, leads follow-up.

**Answer:**

#### Q2. Is this only transactional messaging, or also promotional/broadcast?

**Answer:**

#### Q3. Do we need per-store branding/language differences?

**Answer:**

#### Q4. Should messages be allowed only during certain hours?
Example: 9 AM–8 PM local time.

**Answer:**

---

### B) Recipient identity + phone normalization

#### Q5. Where should recipient phone numbers come from initially?
Options: existing orders/leads tables, or queue payload only.

**Answer:**

#### Q6. Should we normalize phone numbers to E.164 and reject invalid numbers at enqueue time?
Example: `+91XXXXXXXXXX`.

**Answer:**

#### Q7. Should one queue item support multiple recipients, or exactly one recipient per row?

**Answer:**

---

### C) Queue design

#### Q8. Do you want a new dedicated DB table for WhatsApp outbox queue?

**Answer:**

#### Q9. Which priorities are needed exactly?
Current assumption: `normal` and `instant`.

**Answer:**

#### Q10. Processing order preference?
Current assumption: `instant` first, then FIFO by `created_at`.

**Answer:**

#### Q11. Do you want scheduled send time (`not_before`) support from day 1?

**Answer:**

#### Q12. Should queue payload support plain text only initially, or also media/file attachments?

**Answer:**

---

### D) Reliability + retries

#### Q13. Retry policy?
Please define max attempts and backoff style.

**Answer:**

#### Q14. What is permanent failure vs retryable failure?
Examples: invalid number, auth expired, selector timeout, network issue.

**Answer:**

#### Q15. Should repeated failures move to dead-letter status/state?

**Answer:**

---

### E) Idempotency + duplicates

#### Q16. Do you want dedupe rules to avoid accidental duplicate sends?
Example key: `(recipient, message/template, business_key, day)`.

**Answer:**

#### Q17. If same message is enqueued twice intentionally, should both be sent or collapsed?

**Answer:**

---

### F) Playwright runtime model (critical)

#### Q18. Are you okay with a persistent browser profile/session on disk for WhatsApp Web login state?
(Scan QR once, then reuse state)

**Answer:**

#### Q19. Headless-only behavior when re-auth is needed?
If QR is required again, should worker pause queue + alert, or keep retrying?

**Answer:**

#### Q20. Preferred Chromium strategy?
Bundled Playwright Chromium vs system Chrome.

**Answer:**

#### Q21. Worker lifecycle preference?
Keep one browser/context for many messages per run, or restart frequently?

**Answer:**

---

### G) Cron + instant routine behavior

#### Q22. Periodic worker cadence?
Exact preference: every 15 min or every 20 min.

**Answer:**

#### Q23. “Instant” queue routine preference?
- continuously running poller (every few seconds), or
- short-interval cron (for example, every 1 minute).

**Answer:**

#### Q24. Do you want global locking so only one WhatsApp sender instance runs at a time?

**Answer:**

---

### H) Template + content governance

#### Q25. Should message content be DB-template-driven (template + context variables)?

**Answer:**

#### Q26. Need multilingual templates at launch?
Examples: English/Hindi.

**Answer:**

#### Q27. Should we store final rendered message text per attempt for audit?

**Answer:**

---

### I) Observability + operations

#### Q28. Should run summaries integrate with existing `pipeline_run_summaries` via a new pipeline code?

**Answer:**

#### Q29. What alerts do you want when worker is unhealthy?
Examples: auth expired, no sends for X hours, retry spike.

**Answer:**

#### Q30. Which operational metrics are mandatory?
Examples: queued, sent, failed, retrying, avg queue age, instant SLA breach.

**Answer:**

---

### J) Compliance / risk controls

#### Q31. Any explicit throttle requirement?
Example: maximum messages per minute.

**Answer:**

#### Q32. Should opt-out / do-not-contact checks be enforced?
If yes: before enqueue, before send, or both.

**Answer:**

#### Q33. Any legal/compliance constraints on message retention duration and audit logs?

**Answer:**

---

## 3) Notes / Additional Inputs (Optional)
Use this section for any extra details, sample message formats, edge cases, rollout constraints, or known operational realities.

**Your notes:**

---

## 4) Next Step
After you fill answers in this document, we will:
1. Freeze final scope (Phase-1 vs future phases).
2. Define architecture, DB schema, worker behavior, cron strategy, retries, observability, and controls.
3. Draft full SRS for WhatsApp automation in a separate structured markdown.
