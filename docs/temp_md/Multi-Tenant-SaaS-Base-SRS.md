# Multi-Tenant SaaS Baseline — Software Requirements Specification (SRS)

## 1. Introduction
### 1.1 Purpose
This SRS defines the authoritative requirements for the TSV Universal Multi-Tenant SaaS Baseline (“the Baseline”). It governs all current and future product instantiations derived from the Baseline without architectural forks.

### 1.2 Scope
The Baseline delivers an identity-first, AI-enabled, offline-capable, audit-grade SaaS platform factory. All tenant-facing products are instantiated from tagged Baseline versions via configuration and governance, not rewrites.

### 1.3 Stakeholders and Approvers
* CEO, The Shaw Ventures — sole approver of this SRS and owner of configuration changes.
* Primary audiences: platform engineers, product teams, compliance, QA, ops, and UI/UX.

### 1.4 Definitions, Acronyms, Abbreviations
* RLS: Row-Level Security
* RTM: Requirements Traceability Matrix
* DLQ: Dead Letter Queue
* DSAR: Data Subject Access Request

### 1.5 References
* Baseline input brief (docs/temp_md/Multi-Tenant-SaaS-Base-Input.md).
* Applicable standards: ISO/IEC 25010 (quality), ISO/IEC 27001/27017 (security), GDPR/UK-GDPR, PCI readiness for future payments.

### 1.6 Overview
Sections cover product perspective, user classes, system capabilities, constraints, functional and non-functional requirements, and traceability expectations.

## 2. Overall Description
### 2.1 Product Perspective
* Single Baseline codebase with strict versioning; products are instantiated by configuration at tagged releases.
* Shared services: logging, metrics, traces; tenant data isolated via RLS.
* Core stack: FastAPI backend; PostgreSQL + pgvector; Redis workers; React web; React Native mobile (offline-first); Playwright/Selenium WhatsApp MVP messaging with future Meta WhatsApp Cloud API drop-in.

### 2.2 Product Functions (high level)
* Identity, authentication, verification, and session/device control.
* RBAC with platform-owned immutable permission catalog and tenant-composed roles.
* Tenant isolation with RLS and explicit tenant context across APIs and background jobs.
* Audit logging and security event capture with audit-on-read.
* Offline-first mobile with encrypted local storage, conflict resolution, and replay-safe queues.
* Messaging and notifications with templates, rate limits, retries, DLQ, and kill switches.
* AI layer with pgvector-backed capabilities (semantic search, summarization, insights, anomaly detection), governed and fully audited.
* Reporting, exports, and data control with async execution and signed URLs.
* Feature flags, kill switches, rate limiting, and non-functional safeguards.

### 2.3 User Classes and Characteristics
* Platform Admin (TSV) — manages global policies, permission catalog, templates, and governance.
* Tenant Admin — full tenant management; can configure roles and templates within tenant scope.
* Tenant Read-only — view-only access.
* Tenant Member — standard functional access per role.
* Support/Impersonation Operator — tightly permissioned, fully audited, scoped to tenant; requires explicit approval workflow per session.
* CEO TSV (support owner) — ultimate support authority.

### 2.4 Operating Environment
* Target deployment: containerized, Kubernetes recommended; blue/green deployments for zero-downtime.
* Environments: DEV, STAGING, PROD; no approval gates for promotion but CI/CD must pass.
* Secrets injected from GitHub secrets; no secrets in code or repo.

### 2.5 Design and Implementation Constraints
* PostgreSQL with mandatory RLS (fail-closed).
* pgvector required.
* FastAPI with strictly versioned APIs.
* React/React Native mobile-first, offline-first.
* Redis-backed async workers.
* WhatsApp messaging via Playwright/Selenium MVP; architecture must allow drop-in Meta WhatsApp Cloud API without refactor.

### 2.6 User Documentation
* Product instantiation guide (to be produced) describing configuration-only product creation.
* Operator runbooks for auth, messaging, AI policies, offline sync, and incident kill switches.

### 2.7 Assumptions and Dependencies
* Initial tenant count: 5–6; expected scale: hundreds.
* Shared observability services are cross-tenant; all other data paths are tenant-isolated.

### 2.8 Tenant Context & Authorization Invariants
1. Every request SHALL resolve a `tenant_id` before business logic executes; absence yields `403 Forbidden` (fail-closed).
2. Tenant context SHALL be propagated via:
   * JWT claims (`tenant_id`, `role_id`),
   * Database session variables,
   * Background job payloads,
   * Logs, metrics, and traces.
3. Cross-tenant access by Platform Admin requires explicit “impersonation mode,” is time-bound to 30 minutes maximum, fully audited (who/why/what/when), and is read-only by default unless elevated with justification.
4. Sub-entity scoping (e.g., organization/site) SHALL never replace `tenant_id`; it is additive only.

### 2.9 Tenancy Model (Authoritative)
```
Tenant
 └── Organization / Brand / Site (optional sub-entity)
      └── Users
      └── Customers
      └── Orders
      └── Payments
      └── Documents
      └── AI Embeddings
```
* Every table SHALL contain `tenant_id`.
* Sub-entities SHALL NOT substitute `tenant_id`.
* AI/vector data SHALL be tenant-scoped; shared reference tables SHALL be read-only and non-tenant-derived.

## 3. System Features & Functional Requirements
Priorities: **M** (Must), **S** (Should), **C** (Could). Release labels: **V1**, **V2**.

### 3.1 Tenancy & Isolation
1. (M, V1) Tenant context is mandatory for all data operations, API calls, and background jobs; operations fail-closed without explicit tenant context.
2. (M, V1) RLS enforces per-tenant isolation; patterns extend to sub-entities (e.g., organization/site) when modeled; `tenant_id` SHALL be present on all tables and job payloads.
3. (M, V1) Tenant bootstrap flow provisions default roles (Tenant Admin, Tenant Read-only) and required configs.
4. (M, V1) Shared services (logging/metrics/traces) must keep tenant identifiers without exposing cross-tenant data.

### 3.2 Identity, Authentication, Verification
5. (M, V1) Auth methods: email+password, OAuth/OIDC (Google, Microsoft, etc.), API keys, session tokens with rotation/revocation.
6. (M, V1) Password policy: minimum length 12, complexity with upper/lower/number/symbol, breach-password checks, lockout after 10 failed attempts within 15 minutes, rotation every 180 days, reuse history of 10.
7. (M, V1) Mandatory email verification and OTP verification (WhatsApp, SMS, Email) with 30-minute validity; retry/lockout window 30 minutes; OTP attempts rate-limited to 5 per hour per user; all events audited.
8. (M, V1) Device registry captures device ID, OS, app version, last seen, IP; supports device and session revocation.
9. (M, V1) Sessions: idle timeout 30 minutes, absolute timeout 24 hours; refresh tokens rotated on each use; risk-based lockout supported.
10. (M, V1) API keys: tenant-scoped, unique per environment, prefix-identifiable, rotated at most every 90 days, immediately revocable; issuance and rotation audited.

### 3.3 Authorization & RBAC
11. (M, V1) Platform-owned immutable permission catalog; versioned and change-controlled by Platform Admin.
12. (M, V1) Tenants compose roles from catalog permissions; scoped assignments supported (e.g., sub-entity).
13. (M, V1) Impersonation requires explicit approval, strict scoping, and full audit (who/when/why); auto-expiration enforced; default session duration 30 minutes.

### 3.4 Auditing, Security Events & Compliance
14. (M, V1) Append-only audit logs covering auth, privileged actions, messaging, exports, AI usage, impersonation; include who/what/when/where/reason and correlation IDs; stored in immutable/WORM-backed storage.
15. (M, V1) Audit-on-read available; redaction and filtering respect tenant isolation.
16. (M, V1) Security events recorded for rate limit breaches, OTP abuse, auth anomalies; alerting integrated with ops tooling.
17. (M, V1) GDPR/UK-GDPR compliance: data minimization, purpose limitation, retention policies, legal hold, DSAR workflows with 30-day SLA and export in machine-readable, redacted form; audit retention default 7 years (tenant-configurable, not less than 1 year).

### 3.5 Mobile & Offline-First
18. (M, V1) Offline data capture mandatory; encrypted local storage using OS secure keystore and AES-256; only tenant-authorized datasets cached.
19. (M, V1) Offline mutation queue with idempotency keys; retries with exponential backoff (30s, 2m, 10m); replay-safe and deduplicated server-side.
20. (M, V1) Server-authoritative conflict resolution with explicit conflict UX; user-visible sync center for status and resolution history.

### 3.6 Messaging & Notifications
21. (M, V1) Async messaging only; channels: WhatsApp (Playwright/Selenium MVP), email, push, SMS.
22. (M, V1) Template-based messaging; platform-wide templates versioned; tenants can clone/override within tenant scope; approvals logged; template approval required before send.
23. (M, V1) Rate limits per IP/user/tenant/channel: read APIs 300 req/min/tenant, write APIs 200 req/min/tenant, auth endpoints 10 req/min/IP, burst allowance 2× for 60s; retries with exponential backoff (30s, 2m, 10m); DLQ retention ≥60 days with replay controls.
24. (M, V1) Global and tenant-level kill switches for messaging; must be auditable and reversible; per-tenant daily send cap default 5,000/day.
25. (S, V2) Migration path to Meta WhatsApp Cloud API with drop-in replacement and no architectural refactor.

### 3.7 AI Layer
26. (M, V1) AI capabilities always present: semantic search, summarization, insights, anomaly detection; pgvector required.
27. (M, V1) AI operates only via approved tools; no autonomous actions; no direct SQL; no cross-tenant access.
28. (M, V1) All AI actions are permission-checked, dataset-classified, and fully audited; policies can disable or constrain AI per platform or tenant.
29. (M, V1) AI continually learns from tenant-specific data (inserts/updates) within tenant boundaries; models/data embeddings segregated per tenant; dataset classification schema SHALL be defined and enforced per request.

### 3.8 Reporting, Exports & Data Control
30. (M, V1) Declarative report definitions; async report execution.
31. (M, V1) Exports are async only; sensitive exports require explicit permission, purpose declaration, watermarking, short retention, and signed URLs with TTL; download tracking mandatory.
32. (M, V1) Export formats and maximum sizes set to explicit defaults: CSV/JSON/PDF up to 1 GB per export; redaction supported for sensitive fields.

### 3.9 Non-Functional Guarantees
33. (M, V1) API timeouts and DB statement timeouts enforced; mandatory rate limiting on all endpoints; API p95 latency ≤500 ms and p99 ≤1.5 s (excluding external providers).
34. (M, V1) Horizontal scalability and zero-downtime deployments (blue/green).
35. (M, V1) Structured logs, metrics, traces with correlation IDs; PII minimized or tokenized; tenant identifiers included.
36. (M, V1) Feature flags and kill switches for messaging, AI, exports, and sync.

### 3.10 Secrets, Environments & Operations
37. (M, V1) No secrets in code; secrets injected from GitHub secrets per environment; keys rotated every 90 days.
38. (M, V1) Separate DEV/STAGING/PROD; no prod secrets in non-prod; promotion requires passing CI/CD but no manual gates.
39. (M, V1) Backup and restore at platform level; tenant-safe restore boundaries; periodic restore verification; RPO/RTO targets defined per platform SLOs.
40. (M, V1) Retention and purge jobs support legal hold.

### 3.11 Traceability & Governance
41. (M, V1) Every requirement maps to API endpoints, DB tables, RBAC permissions, and UX surfaces; RTM maintained in CI/CD and enforced on merges (e.g., PR templates/commit tags).
42. (M, V1) CI pipeline SHALL fail if a requirement lacks associated tests or if a test lacks requirement linkage; RTM is version-controlled.
43. (M, V1) Baseline is authoritative; product repos cannot weaken auth/verification, bypass audit/AI governance, or alter RLS without explicit CTO/CEO approval.
44. (M, V1) Change management follows RFC/CAB practices; deviations require documented approvals.

### 3.12 Product Instantiation Model
45. (M, V1) Products are created by copying tagged Baseline versions and applying policy/configuration only; no architectural forks.
46. (M, V1) A product instantiation guide shall document required configuration steps and guardrails.

### 3.13 Integrations
47. (M, V1) Ticketing integration supported.
48. (M, V1) Analytics integration supported.
49. (S, V2) Payment gateway integration planned; design must preserve existing security and isolation guarantees.

### 3.14 Data Residency & Regions
50. (M, V1) Region-agnostic storage with GDPR/UK-GDPR compliance; future region routing rules must be enforceable by policy without refactor.

## 4. External Interface Requirements
### 4.1 APIs
* FastAPI REST with strict versioning; OpenAPI schemas published per release.
* All endpoints require tenant context and enforce RLS-backed checks.

### 4.2 User Interfaces
* Web: React (mobile-first).
* Mobile: React Native (iOS/Android), offline-first with conflict UX and sync center.

### 4.3 Messaging Interfaces
* WhatsApp via Playwright/Selenium MVP; future Meta Cloud API compatibility.
* Email, push, SMS via provider abstractions with rate limits and DLQ.

### 4.4 Data Interfaces
* PostgreSQL with pgvector; Redis for queues; signed URL-based export/download channels.

## 5. System Quality Attributes (Non-Functional Requirements)
* **Availability:** SLA targets to be published per environment; design for multi-AZ where applicable.
* **Performance:** API p95 latency ≤500 ms and p99 ≤1.5 s (excluding external providers); worker queue retries 30s, 2m, 10m with DLQ after 3 attempts; sync latency monitored.
* **Security:** Defense-in-depth with RLS, RBAC, MFA/OTP, secure hashing, TLS 1.2+, AES-256 at rest, secrets rotation every 90 days.
* **Reliability & Recoverability:** Backup/restore with defined RPO/RTO; regular restore drills; tenant-safe boundaries.
* **Scalability:** Horizontal scale for API, workers, vector search, and messaging.
* **Maintainability:** Versioned APIs; backward-compatible migrations; RTM enforced in CI/CD.
* **Usability:** Mobile-first and offline-aware UX; explicit conflict resolution UX; template and messaging UX with preview.
* **Auditability & Compliance:** Immutable/WORM audit logs with default 7-year retention; audit-on-read; DSAR fulfillment and legal hold.

## 6. Database Requirements
* PostgreSQL with mandatory RLS; RLS policies enforced for all access paths.
* pgvector required for AI embeddings; per-tenant data and embeddings isolated.
* Statement timeouts and connection limits configured; tenant-aware connection tagging.
* Backup/restore supports tenant-safe boundaries; no cross-tenant restore leakage.

## 7. Operations, Deployment, and Observability
* Deployment: blue/green; rollbacks supported; zero-downtime migrations where feasible.
* Observability: structured logging, metrics, traces with correlation IDs and tenant IDs; PII minimization/tokenization; centralized dashboards.
* Rate limits and kill switches configurable at platform and tenant levels; defaults provided.
* DLQ management and replay tooling; retention durations defined per channel (≥60 days).
* Break-glass access requires manual approval, is fully recorded, auto-expires, and triggers security alerts.

## 8. Data Protection, Privacy, and Legal
* GDPR/UK-GDPR compliance enforced via access controls, auditability, retention, and DSAR support.
* Data residency policies must be enforceable; legal hold halts deletions for affected records.
* Watermarking and purpose declaration required for sensitive exports.
* Encryption in transit TLS 1.2+; at rest AES-256; offline local storage encrypted with OS-level secure keystore.

## 9. AI Governance
* AI tool usage must be policy-controlled with audit trails and permission checks.
* Dataset classification schema required; AI requests must specify dataset class and tenant.
* Kill switches at platform and tenant level can disable AI features without code changes.

## 10. Risk Management and Security Events
* Security events recorded for rate limit breaches, OTP abuse, auth anomalies; alerts routed to ops.
* Risk-based session lockouts; device risk signals can trigger re-auth or denial.
* Impersonation sessions require explicit justification and expire automatically.

## 11. Requirements Traceability
* RTM maintained in repo and enforced in CI/CD (PR templates or commit tags referencing requirement IDs).
* Every implementation artifact (API, DB table, permission, UX) must reference requirement IDs (e.g., REQ-3.1.1).

## 12. Appendices
### 12.1 Initial Tenant Scaling Expectations
* Launch: 5–6 tenants; scale to hundreds with horizontal scale-out and partition-tolerant design.

### 12.2 Future Work (V2+)
* Payment gateway integration with PCI-ready design.
* Meta WhatsApp Cloud API adoption.
* Additional regional residency enforcement policies.
