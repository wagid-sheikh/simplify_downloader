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

## Inputs/Questions asked by Codex Earlier and their answers

### Scope & Stakeholders

1. Primary stakeholders and approvers for the SRS? [I am the only one CEO of The Shaw Ventures]
2. Target audiences (platform engineers, product teams, compliance, QA, ops)? [All & UI/UX]

### Tenancy & Products

3. How many tenants are expected initially and at scale? [Initially 5-6, at scale 100s]
4. Are tenants fully isolated except for shared platform services (logging/metrics)? Any cross-tenant features (e.g., multi-tenant admin dashboards)? [Tenants will remain isolated in general except for shared platform services (logging/metrics)]
5. Product instantiation: are product configurations code-driven (e.g., YAML), UI-driven, or both? Who owns configuration changes? [I will own configuration changed, & for instantiation you will create a guide to follow]

### User & Role Model

6. Core user personas (platform admin, tenant admin, tenant member, support/impersonation roles)? [Yes these roles are non-negotiable and think of other such ciritical roles needed to make such a SaaS system. I as CEO of The Shaw Ventures shall be solely responsible to provide technical support to Tenants]
7. Any default roles per tenant? How are roles provisioned and lifecycle-managed? [Tenant Admin, Tenant Read-only]
8. Impersonation rules and safeguards (audit requirements, approval flows)? [as per industry standards]

### Identity, Auth, Verification

9. Password policy (length, complexity, rotation, reuse, lockout thresholds)? [as per industry standards]
10. OAuth/OIDC providers in scope for v1? [yes - non-negotiable]
11. API key format, rotation policy, and scoping (tenant-only or tenant+environment)? [as per industry standards]
12. OTP delivery specifics: WhatsApp only or also SMS/email? OTP validity duration and retry/lockout thresholds? [WhatsApp, SMS, Email all are valid channels. OTP Validity 30 minutes, retry/lockout 30 minutes]

### Session & Device Management

13. Device registry metadata (device ID, OS, app version, last seen, IP)? [yes]
14. Session durations (idle/absolute), refresh rules, and revocation triggers? [as per industry standards]

### Authorization & Security

15. Permission catalog owner and update process? Versioning strategy? [yes as per industry standards]
16. RLS patterns: per-tenant only, or also per-organization/site within a tenant? [yes]
17. Background jobs: how is tenant context propagated and validated? [as per industry standards]

### Auditing & Compliance

18. Audit log data model requirements (who/what/when/where, reason, correlation IDs)? [Yes]
19. Retention periods and legal hold handling? [as per industry standards, most of the Tenants would be UK & USA so we must stay compliant]
20. DSAR workflow expectations (SLA, export formats, redaction rules)? [as per industry standards]

### AI Layer

21. Approved AI tools/capabilities for v1; any model/provider constraints? [None, AI layer is non-negotiable. AI layer must full data per tenant and must keep learning from ongoing inserts, updates]
22. Dataset classification scheme and policy enforcement requirements? [as per industry standards]
23. AI disablement/killswitch granularity (platform, tenant, feature-level)? [platform, tenant]

### Messaging & Notifications

24. Supported channels beyond WhatsApp MVP (email, push, SMS)? Priority order/fallback rules? [whatsapp, email, push, SMS]
25. Template management (versioning, approvals, localization)? Who owns templates? [Platform wide templates, can be used by Tenants, Tenants can modify and make them specific to their own tenancy]
26. Rate limit defaults per IP/user/tenant/channel? DLQ retention and replay rules? [as per industry standards]

### Mobile & Offline

27. Local storage encryption approach and data categories allowed offline? [as per industry standards]
28. Conflict resolution strategies (last-write-wins, server-authoritative merge) and user-facing conflict UX requirements? [as per industry standards]
29. Offline mutation queue limits, retry/backoff policies? [as per industry stndards]

### Reporting, Exports & Data Control

30. Report definition format (declarative spec) and who authors/approves? [as per industry stndards]
31. Export formats, max size, and redaction/watermarking requirements? Purpose declaration UI/fields? [as per industry stndards]

### Non-Functional & Ops

32. Target SLOs/SLAs (availability, latency) for API, workers, sync, messaging? [as per industry stndards]
33. Deployment model assumptions (Kubernetes? blue/green or canary?) and zero-downtime expectations? []blue/green]
34. Observability stack preferences (logging/metrics/traces tools) and PII handling in logs? [yes]
35. Backup/restore RPO/RTO targets; restore testing cadence; tenant-scoped restore mechanism? [backup & restore to be platform level]

### Environments & Secrets

36. Secrets management platform (Vault, cloud KMS, etc.)? Rotation policies? [Secrets to be injected from GitHub secrets]
37. Environment promotion workflows (DEV→STAGING→PROD) and approval gates? [workflow is fine and no approval gates]

### Traceability & Governance

38. RTM format and enforcement points in CI/CD (e.g., commit tags, PR templates)? [yes]
39. Change management requirements (RFCs, CAB approvals) for baseline and products? [yes]

### Data Model & Integrations

40. Any required integrations (payment, CRM, ticketing, analytics) in baseline? [Payment Gateway in V2, Ticketing yes in V1, Analytics in V1]
41. Multi-region or data residency needs? Region routing rules? [as per industry standards]

### Deliverable Expectations

42. Any mandated SRS template sections (e.g., IEEE 830/29148)? [as per industry standards]
43. Level of detail and priority labeling desired (e.g., Must/Should/Could, release phases)? [yes]

## Additional Inputs


# 1. Non-Functional Targets (SLAs / SLOs)

### Availability (Monthly, excluding scheduled maintenance)

| Plane                       | Availability SLA | Notes                                          |
| --------------------------- | ---------------- | ---------------------------------------------- |
| Public API                  | **99.9%**        | Core tenant-facing APIs                        |
| Background Workers / Queues | **99.9%**        | Retries + DLQ mitigate transient failures      |
| Mobile Sync                 | **99.5%**        | Offline-first reduces strict uptime dependency |
| Messaging Dispatch          | **99.0%**        | External provider dependency acknowledged      |
| Reporting / Exports         | **99.0%**        | Async, non-blocking                            |

---

### Latency Targets

| Plane                     | p95          | p99          |
| ------------------------- | ------------ | ------------ |
| API (internal logic only) | ≤**500 ms**  | ≤**1500 ms** |
| API (with external calls) | ≤**1200 ms** | ≤**3000 ms** |
| Worker job execution      | ≤**2 sec**   | ≤**10 sec**  |
| Mobile sync (per batch)   | ≤**3 sec**   | ≤**8 sec**   |
| Messaging enqueue         | ≤**500 ms**  | ≤**1 sec**   |
| Export generation (async) | ≤**2 min**   | ≤**10 min**  |

---

# 2. Capacity Targets (Initial Release + Growth)

### Initial Baseline (Day-1)

* Sustained API throughput: **50 req/sec**
* Burst capacity: **200 req/sec for 60 seconds**
* Concurrent authenticated sessions: **5,000**
* Concurrent tenants: **≤ 500**
* Daily messages sent: **≤ 100,000**
* Daily exports: **≤ 2,000**

### Growth Assumption (Design For)

* 10× traffic growth without schema redesign
* Horizontal scaling only (no single-node coupling)
* Tenant isolation must not degrade under load (noisy-neighbor protection mandatory)

---

# 3. Password & Session Policy

### Password Policy

* Minimum length: **12 characters**
* Complexity: **At least 3 of 4** (upper, lower, digit, symbol)
* Password reuse: **Last 5 passwords disallowed**
* Rotation: **No forced rotation** (except after compromise)
* Storage: **Argon2id** (or bcrypt if unavailable)

### Account Lockout

* Failed attempts: **5 consecutive**
* Lockout duration: **15 minutes**
* Progressive delay on subsequent failures

---

### Session Policy

| Parameter                 | Value                          |
| ------------------------- | ------------------------------ |
| Idle timeout              | **30 minutes**                 |
| Absolute session lifetime | **12 hours**                   |
| Refresh token TTL         | **7 days**                     |
| Admin session TTL         | **4 hours**                    |
| Impersonation session TTL | **30 minutes (non-renewable)** |

---

# 4. Rate Limits (Default Baselines)

### Authentication & OTP

| Scope          | Limit           |
| -------------- | --------------- |
| Login attempts | 10 / min / IP   |
| OTP send       | 3 / hour / user |
| OTP verify     | 5 / hour / user |
| Password reset | 3 / hour / user |

### API Calls

| Scope      | Limit         |
| ---------- | ------------- |
| Per user   | 60 / min      |
| Per tenant | 300 / min     |
| Burst      | 2× for 60 sec |

### Messaging

| Scope            | Limit                   |
| ---------------- | ----------------------- |
| Per tenant       | 1,000 / day             |
| Per recipient    | 5 / day                 |
| Provider backoff | Exponential (30s → 10m) |

All rate-limit violations return **429 with Retry-After**.

---

# 5. Audit, Security & Observability Retention

| Data Type                     | Retention                            |
| ----------------------------- | ------------------------------------ |
| Audit logs (business actions) | **7 years**                          |
| Security events               | **7 years**                          |
| Access logs                   | **90 days**                          |
| Application logs              | **30 days**                          |
| Metrics / traces              | **14–30 days**                       |
| Legal hold                    | **Indefinite (manual release only)** |

Audit logs MUST be:

* Append-only
* Immutable
* Tenant-scoped
* Exportable only by authorized roles

---

# 6. Data Residency & Regioning

### Supported Regions (v1)

* **US**
* **EU**
* **UK**

### Rules

* Tenant is **pinned to a region at creation**
* Data MUST NOT cross regions (except encrypted backups)
* Read replicas must stay within region
* Platform admin access must respect residency

Optional (future):

* Multi-region active-active (not required for v1)

---

# 7. Export / Reporting Constraints

### Formats Supported

* CSV (default)
* JSON
* PDF
* Parquet (large datasets only)

### Limits

* Max rows per export: **1,000,000**
* Max file size: **500 MB**
* Exports are **async only**

### Mandatory Export Metadata

* Purpose declaration (free text, required)
* Requested by (user ID)
* Timestamp
* Tenant ID

### Watermark (PDF)

* Text:
  `CONFIDENTIAL – Tenant: {{tenant_name}} – Generated {{timestamp}}`
* Footer, every page

---

# 8. Offline Data Scope

### Allowed Offline

* Orders
* Customers (non-sensitive fields)
* Operational metadata
* Cached reference data

### Explicitly Disallowed Offline

* Passwords / secrets
* API keys / tokens
* Payment instruments
* Government IDs
* Biometric data
* Full audit logs

Offline storage MUST be:

* Encrypted
* Expirable
* Wipeable via remote invalidation

---

# 9. AI Data Handling

### Hosting & Residency

* Embeddings and AI processing MUST reside in same tenant region
* No cross-tenant or cross-region embeddings

### Update SLAs

| Type                   | SLA                       |
| ---------------------- | ------------------------- |
| User-triggered updates | ≤**5 seconds**            |
| Background enrichment  | ≤**15 minutes**           |
| Full reindex           | Async, tracked, resumable |

AI outputs MUST be:

* Permission-checked
* Audited
* Attributable to input sources

---

# 10. Template Governance

### Template States

1. Draft
2. Approved
3. Deprecated
4. Archived

### Rules

* Only **Approved** templates can be used
* Tenant overrides require approval
* Deprecated templates are read-only
* Localization required for:

  * UI-facing messages
  * Customer-facing notifications

---

# 11. Backup & Restore Targets

### Targets

| Metric         | Value                           |
| -------------- | ------------------------------- |
| RPO            | **15 minutes**                  |
| RTO            | **4 hours**                     |
| Backup cadence | Continuous WAL + daily snapshot |
| Restore test   | Quarterly (minimum)             |

### Restore Rules

* Tenant-scoped restores preferred
* Max acceptable cross-tenant impact window: **≤ 15 minutes**
* All restores must be audited and approved

---

## Final Instruction to Technical Writer

> Replace all vague phrases with **SHALL statements** using the values above.
> No reinterpretation, no “best practice” language, no unstated defaults.
>
