# TSV Universal Multi-Tenant SaaS Baseline

## Core Intent

You are building **one universal, enterprise-grade, multi-tenant SaaS baseline** that acts as a **platform factory** for all current and future software products under **The Shaw Ventures (TSV)**.

This baseline is the **single source of truth**.
All products are **instantiations of this baseline**, not architectural forks.

---

## What You Build Once (The Baseline)

### 1. Architecture & Stack (Authoritative)

* **Backend:** Python + FastAPI (strictly versioned APIs)
* **Database:** PostgreSQL (shared schema, mandatory Row Level Security)
* **Vector Layer:** pgvector (required, not optional)
* **Async & Queues:** Redis-backed workers
* **Web UI:** React (mobile-first)
* **Mobile Apps:** React Native (iOS & Android, offline-first mandatory)
* **Messaging (MVP):** WhatsApp via Playwright/Selenium (contained, kill-switch guarded)
* **Future Messaging:** Meta WhatsApp Cloud API (drop-in, no refactor)
* **AI Layer:** Core platform capability (governed, audited, non-autonomous)

---

## 2. Identity, Authentication & Verification (First-Class)

### Authentication

* Email + password (secure hashing)
* OAuth / OIDC (Google, Microsoft, etc.)
* Token-based sessions with rotation & revocation
* API keys for system-to-system access (tenant-scoped)

### Verification

* Mandatory **email verification**
* Mandatory **mobile / WhatsApp OTP verification**
* Rate-limited, TTL-bound, lockout-protected

### Device & Session Control

* Device registry
* Session tracking
* Session and device revocation
* Risk-based lockouts

### Biometrics / Facial Recognition (Mobile)

* Optional **device-level biometric unlock only**
* Uses OS secure enclave (Face ID / Fingerprint)
* Never replaces server-side authentication
* Never treated as identity authority

---

## 3. Authorization, Isolation & Security Model

* **RBAC** with a **platform-owned immutable permission catalog**
* Tenants compose roles from catalog permissions
* Scoped role assignments supported
* Strict separation of:

  * **Platform Plane**
  * **Tenant Plane**
* **Tenant isolation enforced at DB level via RLS (fail-closed)**
* No tenant data access without tenant context
* Background jobs must explicitly set tenant context

---

## 4. Auditing, Security Events & Compliance

* **Immutable, append-only audit logs**
* Audit coverage for:

  * Auth events
  * Privileged actions
  * Messaging
  * Exports
  * AI usage
  * Impersonation
* **Audit-on-read capability supported at baseline**
* Security events recorded for:

  * Rate limit breaches
  * OTP abuse
  * Auth anomalies
* Region-agnostic data storage
* GDPR / UK-GDPR enforced via:

  * Policy
  * Access controls
  * Auditability
  * Retention rules
* Full DSAR lifecycle support (with legal hold)

---

## 5. Mobile & Offline-First (Non-Negotiable)

* Offline data capture is **mandatory**
* Encrypted local storage
* Offline mutation queue with idempotency keys
* Server-authoritative conflict resolution
* Explicit conflict UX
* Sync center visibility
* Offline replay is safe and repeatable

---

## 6. Messaging & Notifications

* Async messaging only (no direct sends)
* Template-based messaging
* Retry, backoff, DLQ
* Rate limits at:

  * IP
  * User
  * Tenant
  * Channel
* Global and tenant-level **kill switches**
* MVP uses Selenium/Playwright WhatsApp
* Architecture explicitly supports migration to Meta Cloud API

---

## 7. AI Layer (Core, Governed, Non-Autonomous)

* AI is **always present** in the platform
* pgvector is required
* AI operates only via **approved tools**
* No direct SQL, no cross-tenant access
* No autonomous actions
* AI capabilities include:

  * Semantic search
  * Summarization
  * Insights
  * Anomaly detection (domain-dependent)
* All AI actions are:

  * Permission-checked
  * Dataset-classified
  * Fully audited
* AI can be disabled or constrained via policy per product

---

## 8. Reporting, Exports & Data Control

* Declarative report definitions
* Async report execution
* Async exports only
* Sensitive exports require:

  * Explicit permission
  * Purpose declaration
  * Watermarking
  * Short retention
* Signed URLs with TTL
* Download tracking

---

## 9. Non-Functional Guarantees

* API timeout limits
* DB statement timeouts
* Horizontal scalability
* Zero-downtime deployment support
* Structured logs, metrics, traces
* Mandatory rate limiting everywhere
* Feature flags at platform & tenant level
* Kill switches for:

  * Messaging
  * AI
  * Exports
  * Sync (incident containment)

---

## 10. Secrets, Environments & Operations

* No secrets in code or repos
* Environment-based secret injection
* Separate DEV / STAGING / PROD
* No prod secrets in non-prod
* Backup & restore supported
* Restore verification expected
* Tenant-safe restore boundaries
* Retention & purge jobs with legal hold support

---

## 11. Requirements Traceability & Enforcement

* Every requirement maps to:

  * API endpoints
  * Database tables
  * RBAC permissions
  * Web & Mobile UX
* No implementation without traceability
* CI/CD enforces RTM references
* Drift is detected and treated as a governance failure

---

## 12. Product Instantiation Model (Critical)

After the baseline is built:

* You create **independent products** (e.g., Foster Care, Laundry) by:

  * Copying the baseline at a tagged version
  * Applying **policy configuration only**

Products:

* Share identical architecture
* Share identical enforcement rules
* Differ only in defaults and policies

No architectural forks.

---

## 13. Baseline Governance & Drift Control

* Baseline is upstream and authoritative
* All architectural changes happen in baseline first
* Product repos may NOT:

  * Alter RLS
  * Weaken auth or verification
  * Bypass audit
  * Bypass AI governance
  * Change offline sync rules
* Deviations require explicit CTO approval
* Security/compliance deviations require CEO approval
* Baseline sync is mandatory and documented

---

## Final One-Sentence Truth

> You are building **one identity-first, AI-enabled, offline-capable, audit-grade SaaS platform factory**, then repeatedly instantiating products from it using configuration and governance—not rewrites.

---
