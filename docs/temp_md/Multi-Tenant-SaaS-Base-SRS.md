# Software Requirements Specification (SRS)

## TSV Universal Multi-Tenant SaaS Baseline


## 0. Executive Summary

The TSV Universal Multi-Tenant SaaS Baseline is the **single authoritative platform foundation** for all TSV products.

All products are instantiated from this Baseline **without architectural forks**; product differences are achieved **only through configuration and policy**.

## 0.1 Revision History

| Version | Date | Change Type | Description | Impacted Sections | Approved By |
| ------- | ---- | ----------- | ----------- | ----------------- | ----------- |
| v1.0 | 2025-02-18 | Baseline Freeze | Initial release of TSV Universal Multi-Tenant SaaS Baseline | All | CEO, The Shaw Ventures |

### What This Baseline Guarantees

**Tenant Isolation by Design**

Every request, background job, export, message, and AI operation executes within **exactly one tenant context**.

Row Level Security (RLS) is mandatory and fail-closed. Platform administrators **cannot bypass tenant isolation** except via time-bound, fully audited impersonation.

**Security, Auditability, and Compliance Built-In**

All privileged actions, exports, messaging, AI usage, and impersonation are captured in **immutable audit logs** with defined retention and legal-hold support.

Secrets are never stored in code. GDPR / UK-GDPR alignment is enforced through access controls, auditability, and data-residency rules.

**Explicit Operational Guarantees**

The Baseline defines **numeric, testable SLAs and limits** across availability, latency, capacity, rate-limiting, authentication, and disaster recovery.

There are no “industry standard” assumptions—every guarantee is enforceable via CI/CD and observability.

**Offline-First and Governed AI**

Mobile clients are **offline-first** with encrypted local storage and controlled data scope.

AI capabilities are mandatory but governed: **no autonomous actions, no cross-tenant access, region-pinned processing, and full auditability**.

**Controlled Messaging and Automation**

Messaging is asynchronous, rate-limited, template-driven, and protected by kill switches.

Automation-based messaging (Playwright/Selenium) is explicitly isolated to allow immediate shutdown or replacement without tenant impact.

**Platform Governance**

This Baseline is **upstream and authoritative**. Core security, isolation, audit, AI, and offline rules cannot be altered by product teams.

All changes require formal approval and traceability enforcement.

### Outcome

This Baseline acts as a **platform constitution**: it prevents tenant data leaks, enforces compliance, enables safe product velocity, and supports long-term scale without architectural drift.

---

## 1. Purpose and Scope

1.1 The SRS defines all functional and non-functional requirements for the TSV Universal Multi-Tenant SaaS Baseline (“the Baseline”), which serves as the single, authoritative platform factory for all current and future TSV products.
1.2 All products SHALL be instantiated from the Baseline without architectural forks; differences SHALL be configuration/policy only.
1.3 Stakeholder and approver: CEO, The Shaw Ventures. Target audiences: platform engineers, product teams, compliance, QA, operations, and UI/UX.

## 2. Definitions, Acronyms, Abbreviations

2.1 Tenant: Isolated logical customer environment.
2.2 Platform Plane: Cross-tenant administrative/observability context.
2.3 Tenant Plane: Tenant-specific context.
2.4 RLS: Row Level Security.
2.5 RTM: Requirements Traceability Matrix.
2.6 SLA/SLO: Service Level Agreement/Objectives.
2.7 DLQ: Dead Letter Queue.
2.8 RPO/RTO: Recovery Point / Recovery Time Objective.

## 3. References

3.1 IEEE 29148 (structure inspiration).
3.2 GDPR / UK-GDPR (compliance scope).
3.3 PostgreSQL, FastAPI, React, React Native, Redis, pgvector.

## 4. System Overview

4.1 Architecture: FastAPI backend; PostgreSQL with RLS; pgvector required; Redis-backed workers; React web (mobile-first); React Native mobile (offline-first mandatory).
4.2 Messaging: MVP via WhatsApp (Playwright/Selenium) with kill switch; future drop-in Meta WhatsApp Cloud API.
4.3 AI Layer: Mandatory, governed, permission-checked, audited; pgvector required; no autonomous actions; no cross-tenant access.
4.4 Deployment: Blue/green, zero-downtime support.
4.5 Product Instantiation: Copy Baseline at tagged version; apply policy configuration only; configuration guide SHALL be provided; no architectural deviation.

### 4.6 Repository Strategy & Independent Release Trains

The TSV Universal Multi-Tenant SaaS Baseline SHALL be implemented using multiple independent repositories to reduce blast radius, enable independent deployment cadences, and enforce clear ownership boundaries.

The following repositories are mandatory:

1. Backend Repository

   * FastAPI application
   * Background workers
   * PostgreSQL schema & migrations
   * Tenant isolation, RLS, audit, AI, and messaging logic

2. Frontend Web Repository

   * React (mobile-first)
   * Web-specific UX, RBAC enforcement, and API consumption

3. Mobile Applications Repository

   * Mobile applications for iOS and Android
   * Offline-first implementation
   * Device registry, sync logic, conflict resolution

4. Contracts Repository (Authoritative)

   * OpenAPI specifications (single source of truth)
   * Shared schemas
   * Generated types for frontend and mobile
   * API versioning rules and compatibility guarantees

Each repository SHALL:

* have its own CI/CD pipeline
* be versioned independently
* be deployed independently
* contain its own AGENTS.md file governing AI-agent behavior

Cross-repository coupling SHALL occur only via the Contracts repository.

#### Contracts Repository (Single Source of Truth)

* OpenAPI specifications SHALL be authoritative.
* Backend APIs MUST conform to the published contracts.
* Frontend and Mobile applications MUST consume generated types from contracts.
* Breaking changes REQUIRE:

  * new API version (e.g., `/api/v2`)
  * backward compatibility window for previous version
  * documented migration notes
  * Backend SHALL support N-1 contract minor version for ≥ 90 days or until mobile release adoption threshold is met.
* Backend CI MUST fail if contracts are violated.
* Frontend/Mobile CI MUST fail if generated types are out of sync.

### 4.7 AI Agent Development Governance

Implementation work across all repositories SHALL be performed primarily by AI agents (ChatGPT Codex). Each repository MUST include an AGENTS.md file that governs how AI agents operate within that repository. AGENTS.md is mandatory and enforced. Each repository SHALL maintain requirement coverage mapping to RTM (or a repo-local RTM view) and CI SHALL enforce it.

### 4.8 Clarifications (Locked Assumptions)

* Multi-repo architecture is intentional.
* Independent deployments are mandatory.
* Contracts repository is the single source of truth.
* One AGENTS.md per repository is required.
* AI agents are first-class contributors under governance.

## 5. Overall Constraints and Assumptions

5.1 Shared schema with mandatory RLS (fail-closed).
5.2 No secrets in code; secrets injected from environment (GitHub Secrets).
5.3 Separate DEV/STAGING/PROD; no prod secrets in non-prod.
5.4 Offline-first for mobile is non-negotiable.
5.5 AI capabilities SHALL respect dataset classification and auditing.
5.6 Tenant data SHALL NOT cross regions except encrypted backups; tenants pinned to region at creation (US, EU, UK).

**Database Standard:**
The TSV Universal Multi-Tenant SaaS platform SHALL use **PostgreSQL** as the primary relational database.
All canonical data models, migrations, RLS policies, and audit guarantees are defined with PostgreSQL as the target datastore.

### 5.7 Tenant Context Invariants

* Every request, job, export, message, and AI call SHALL resolve exactly one `tenant_id`.
* Absence of tenant context SHALL result in immediate rejection (403).
* Tenant context SHALL be injected at the API gateway, persisted through DB session variables, and propagated to workers, logs, metrics, and traces.
* Cross-tenant access is forbidden except via audited impersonation.

## 6. Functional Requirements

### 6.1 Identity, Authentication, Verification

* FR-IA-01 (Must): Support email+password auth with Argon2id (bcrypt fallback).
* FR-IA-02 (Must): Support OAuth/OIDC (Google, Microsoft, etc.).
* FR-IA-03 (Must): Support API keys scoped to tenant; rotation and revocation SHALL be available.
* FR-IA-04 (Must): Enforce password policy: min length 12; at least 3 of 4 classes; last 5 disallowed; no forced rotation except compromise.
* FR-IA-05 (Must): Account lockout after 5 consecutive failures for 15 minutes; progressive delay on subsequent failures.
* FR-IA-06 (Must): Mandatory email verification and mobile/WhatsApp/SMS OTP; OTP validity 30 minutes; retry/lockout 30 minutes; rate-limited.
* FR-IA-07 (Must): Sessions with idle timeout 30m; absolute 12h; refresh TTL 7d; admin session TTL 4h; impersonation session TTL 30m non-renewable.
* FR-IA-08 (Must): Device registry capturing device ID, OS, app version, last seen, IP; support session/device revocation and risk-based lockouts.
* FR-IA-09 (Must): Biometrics are device-level unlock only; never identity authority.

### 6.2 Authorization, RBAC, Tenant Isolation

* FR-AU-01 (Must): Platform-owned immutable permission catalog; versioned; tenants compose roles.
* FR-AU-02 (Must): Default tenant roles: Tenant Admin, Tenant Read-only; lifecycle-managed.
* FR-AU-03 (Must): Support scoped role assignments.
* FR-AU-04 (Must): Enforce RLS per tenant (fail-closed); support additional sub-tenant scoping where required.
* FR-AU-05 (Must): Background jobs SHALL set and validate tenant context explicitly.
* FR-AU-06 (Must): Impersonation SHALL be permission-checked, audited, and follow approval safeguards.
* FR-AU-07 (Must): Platform Admin access SHALL NOT bypass RLS; cross-tenant reads SHALL occur only via time-bound, audited impersonation.

### 6.3 Auditing, Security Events, Compliance

* FR-AU-08 (Must): Immutable, append-only audit logs for auth, privileged actions, messaging, exports, AI usage, impersonation.
* FR-AU-09 (Must): Audit-on-read capability at baseline.
* FR-AU-10 (Must): Security events for rate limit breaches, OTP abuse, auth anomalies.
* FR-AU-11 (Must): GDPR/UK-GDPR alignment via policy, access controls, auditability, retention; DSAR lifecycle with legal hold.
* FR-AU-12 (Must): Tenant-scoped, exportable audit logs; access restricted to authorized roles.

### 6.4 AI Layer

* FR-AI-01 (Must): AI always present; operates only via approved tools; permission-checked; audited.
* FR-AI-02 (Must): No direct SQL; no cross-tenant access; embeddings and processing SHALL stay in tenant’s region.
* FR-AI-03 (Must): Capabilities: semantic search, summarization, insights, anomaly detection (domain-dependent).
* FR-AI-04 (Must): AI outputs SHALL be attributable, audited, and respect dataset classification.
* FR-AI-05 (Must): AI disablement/kill switches at platform and tenant levels.
* FR-AI-06 (Must): Vector update SLAs: user-triggered ≤5s; background enrichment ≤15m; full reindex async/resumable.

### 6.5 Messaging & Notifications

* FR-MS-01 (Must): Async-only messaging with retry, backoff, DLQ; template-based.
* FR-MS-02 (Must): Channels: WhatsApp (MVP), email, push, SMS; fallback order configurable.
* FR-MS-03 (Must): Rate limits per IP/user/tenant/channel; 429 with Retry-After on violations.
* FR-MS-04 (Must): Global and tenant-level kill switches; provider backoff exponential 30s→10m.
* FR-MS-05 (Must): Template states: Draft, Approved, Deprecated, Archived; only Approved usable; tenant overrides require approval; localization required for UI/customer-facing messages.
* FR-MS-06 (Must): Automation-based WhatsApp Playwright/Selenium messaging is operationally fragile and SHALL be isolated behind provider abstractions to allow immediate shutdown or replacement without tenant impact.

### 6.6 Mobile & Offline

* FR-MO-01 (Must): Offline data capture mandatory; encrypted local storage; expirable and remote-wipe capable.
* FR-MO-02 (Must): Offline mutation queue with idempotency keys; safe replay; explicit conflict UX; server-authoritative conflict resolution.
* FR-MO-03 (Must): Offline disallowed: passwords/secrets, API keys/tokens, payment instruments, government IDs, biometric data, full audit logs.
* FR-MO-04 (Must): Allowed offline: orders, customers (non-sensitive fields), operational metadata, cached reference data.

### 6.7 Reporting, Exports & Data Control

* FR-RE-01 (Must): Declarative report definitions; async execution.
* FR-RE-02 (Must): Exports async only; formats CSV, JSON, PDF, Parquet (large datasets).
* FR-RE-03 (Must): Limits: ≤1,000,000 rows; ≤500 MB per export.
* FR-RE-04 (Must): Sensitive exports require explicit permission, purpose declaration (free text with optional future controlled enums), watermarking, short retention, signed URLs with TTL, download tracking.
* FR-RE-05 (Must): Watermark text: “CONFIDENTIAL – Tenant: {{tenant_name}} – Generated {{timestamp}}” on every PDF page footer.

### 6.8 Requirements Traceability & Enforcement

* FR-TR-01 (Must): Every requirement SHALL map to API endpoints, DB tables, RBAC permissions, Web & Mobile UX.
* FR-TR-02 (Must): CI/CD SHALL enforce RTM references; no implementation without traceability; drift treated as governance failure.

### 6.9 Product Instantiation & Governance

* FR-PI-01 (Must): Products instantiated by copying Baseline at tagged version; apply policy configuration only.
* FR-PI-02 (Must): No changes to RLS, auth/verification, audit, AI governance, offline rules in product repos.
* FR-PI-03 (Must): Deviations require CTO approval; security/compliance deviations require CEO approval; baseline sync mandatory and documented.

### 6.10 Integrations

* FR-IN-01 (Must): Ticketing (v1) integration supported (system to be selected); SSO/webhooks/data sync as needed.
* FR-IN-02 (Must): Analytics (v1) supported.
* FR-IN-03 (Could): Payment Gateway in v2; maintain PCI scope isolation via tokenization.

### 6.11 Template Governance (expanded)

* FR-TG-01 (Must): Templates versioned; Draft→Approved required for use; Deprecated read-only; Archived not usable.
* FR-TG-02 (Must): Tenant-specific overrides require approval; localization required for UI and customer notifications.

## 7. Non-Functional Requirements

### 7.1 Availability (SLA)

* NFR-AV-01 (Must): Public API 99.9% monthly (excl. maintenance).
* NFR-AV-02 (Must): Background workers/queues 99.9%.
* NFR-AV-03 (Must): Mobile sync 99.5%.
* NFR-AV-04 (Must): Messaging dispatch 99.0%.
* NFR-AV-05 (Must): Reporting/exports 99.0%.

### 7.2 Performance / Latency (p95/p99)

* NFR-PE-01 (Must): API internal ≤500ms/1500ms; with external calls ≤1200ms/3000ms.
* NFR-PE-02 (Must): Worker job exec ≤2s/10s.
* NFR-PE-03 (Must): Mobile sync batch ≤3s/8s.
* NFR-PE-04 (Must): Messaging enqueue ≤500ms/1s.
* NFR-PE-05 (Must): Export generation ≤2m/10m (async).

### 7.3 Capacity (Day-1)

* NFR-CP-01 (Must): Sustained API throughput 50 req/s; burst 200 req/s for 60s.
* NFR-CP-02 (Must): Concurrent authenticated sessions 5,000.
* NFR-CP-03 (Must): Concurrent tenants ≤500.
* NFR-CP-04 (Must): Daily messages ≤100,000; daily exports ≤2,000.
* NFR-CP-05 (Must): Design for 10× growth via horizontal scaling; noisy-neighbor protection.

### 7.4 Security & Privacy

* NFR-SC-01 (Must): Mandatory RLS fail-closed; no tenant access without context.
* NFR-SC-02 (Must): Secrets via environment; no secrets in code/repos.
* NFR-SC-03 (Must): Rate limiting mandatory across APIs, auth, OTP, messaging.
* NFR-SC-04 (Must): PII handling in observability with redaction where required.
* NFR-SC-05 (Must): Region residency enforced; backups encrypted; platform admin access respects residency.

### 7.5 Observability

* NFR-OB-01 (Must): Structured logs, metrics, traces; OpenTelemetry-compatible.
* NFR-OB-02 (Must): Retention: access logs 90d; app logs 30d; metrics/traces 14–30d.
* NFR-OB-03 (Must): Audit/security events retained 7 years; legal hold indefinite until manual release.

### 7.6 Backup & Restore

* NFR-BR-01 (Must): RPO 15 minutes; RTO 4 hours; continuous WAL + daily snapshot; quarterly restore test.
* NFR-BR-02 (Must): Tenant-scoped restores preferred; max cross-tenant impact window ≤15 minutes; all restores audited and approved.

### 7.7 Rate Limits (Defaults)

* Auth/OTP: Login 10/min/IP; OTP send 3/hr/user; OTP verify 5/hr/user; password reset 3/hr/user.
* API: 60/min/user; 300/min/tenant; burst 2× for 60s.
* Messaging: 1,000/day/tenant; 5/day/recipient; exponential backoff 30s→10m; 429 with Retry-After on violations.

### 7.8 Deployment & Operations

* NFR-DO-01 (Must): Blue/green deployments; zero-downtime support.
* NFR-DO-02 (Must): Feature flags at platform and tenant levels; kill switches for messaging, AI, exports, sync.
* NFR-DO-03 (Must): Environment promotion DEV→STAGING→PROD; no approval gates required per input.
* NFR-DO-04 (Must): Backup/restore platform-level service with tenant-safe boundaries.

#### 7.8.1 Redis-Backed Configuration & Secrets Governance

* Canonical hierarchy (mandatory): PostgreSQL is the sole source of truth for configuration via `platform_config` (platform defaults/overrides) and `tenant_config` (tenant overrides). Redis is the **mandatory runtime configuration plane** that serves materialized, versioned snapshots; it is **not** a source of truth. Environment variables exist **only** to bootstrap connections to PostgreSQL and Redis (allowlist: `APP_ENV`, `DATABASE_DSN`, `REDIS_DSN`, `SECRET_KEY`/signing keys, `SERVICE_NAME`, `SERVICE_VERSION`, observability exporters). All secrets are injected through GitHub Secrets; `.env` usage is near-zero and never contains feature flags or business logic.
* Keyspace conventions: Platform configuration snapshots are stored as immutable JSON at `cfg:platform:{platform_config_version}`. Tenant snapshots are stored at `cfg:tenant:{tenant_id}:{tenant_config_version}`. Keys MUST be explicitly scoped and versioned.
* Configuration load flow (standardized for every API request, worker job, or agent execution): (1) resolve exactly one tenant context; (2) fetch tenant snapshot from Redis with PostgreSQL fallback and repopulate on miss; (3) fetch platform snapshot from Redis with PostgreSQL fallback and repopulate on miss; (4) merge platform defaults, platform overrides, and tenant overrides; (5) expose a single immutable config snapshot for the lifetime of the request/job. No other access paths are permitted.
* Update & invalidation: On configuration change, PostgreSQL commits the update, increments the config version, publishes `cfg:invalidate` on Redis, and services refresh lazily on next access. No restarts are required; stale configuration is prohibited.
* Prohibited patterns: Direct `os.getenv()` usage or new environment variables outside bootstrap modules; direct reads from `platform_config` or `tenant_config` outside the configuration subsystem; hardcoded defaults for configurable behavior; per-service or per-worker divergence in configuration handling. Any violation is a governance breach.
* Mandatory access module: A single configuration service/module (e.g., `ConfigService`) is the only component permitted to read from Redis, fall back to configuration tables, merge/validate, and return snapshots via methods such as `get_platform_config()`, `get_tenant_config(tenant_id)`, and `get_config_snapshot(tenant_id)`.
* Enforcement: CI MUST fail on unauthorized `os.getenv()` usage, direct configuration table access, or attempts to introduce new `.env` variables. Staging and production MUST emit runtime warnings or structured security events on unauthorized environment access.
* Outcomes guaranteed: Secrets are injected once and never duplicated; Redis is the single runtime interface; PostgreSQL remains the sole source of truth; configuration drift is eliminated across API, workers, and agents.

### 7.9 Data Residency & Regioning

* NFR-DR-01 (Must): Supported regions v1: US, EU, UK; tenant pinned to region at creation.
* NFR-DR-02 (Must): Data SHALL NOT cross regions except encrypted backups; read replicas remain in-region.

## 8. Data Management

8.1 Offline storage encrypted, expirable, remote-wipe.
8.2 Disallowed offline: passwords, API keys/tokens, payment instruments, government IDs, biometric data, full audit logs.
8.3 Allowed offline: orders, customers (non-sensitive fields), operational metadata, cached reference data.
8.4 AI data: embeddings and processing in-tenant region; no cross-tenant/regional mixing; vector update SLAs per 6.4.
8.5 Retention: Audit/security events 7 years; access logs 90d; app logs 30d; metrics/traces 14–30d; legal hold indefinite.

### Table Naming Standards

#### Platform Tables

Prefix: `platform_`
Examples:

* `platform_tenants`
* `platform_regions`
* `platform_feature_flags`
* `platform_audit_events`

#### Tenant Tables

Prefix: `tenant_`
Examples:

* `tenant_users`
* `tenant_roles`
* `tenant_orders`
* `tenant_documents`

Rules:

* MUST include `tenant_id`
* MUST be protected by RLS
* MUST not contain cross-tenant data

#### Reference Tables

Prefix: `ref_`
Examples:

* `ref_countries`
* `ref_currencies`

Rules:

* Read-only
* No tenant-derived data

General:

* snake_case
* plural nouns
* junction tables: `tenant_user_roles`

## 9. Interfaces

9.1 API: Versioned FastAPI; strict timeouts; rate-limited.
9.2 Web: React mobile-first.
9.3 Mobile: React Native offline-first; device registry; conflict UX.
9.4 Messaging: WhatsApp via Playwright/Selenium MVP; future Meta Cloud API drop-in.
9.5 Integrations: Ticketing (v1), Analytics (v1), Payment Gateway (v2).
9.6 Exports: Signed URLs with TTL; watermarking for PDFs.
9.7 API versioning and naming: Base path `/api/v1/...`; breaking changes REQUIRE new version (e.g., `/api/v2`); tenant context inferred from auth for tenant-plane APIs; `tenant_id` in path only for platform-plane operations; responses MUST include correlation IDs for tracing.

### 9.8 API Pagination and Sorting Standard

* List endpoints SHALL support cursor-based pagination.
* Default ordering SHALL be by `id` (ULID) descending: `sort=-id`.
* Endpoints MAY support user-driven sorting via `sort` query parameter; only documented allowlisted sort keys are permitted.
* When sorting by non-unique fields, pagination MUST be stable and deterministic using ULID as a tie-breaker:
  * `ORDER BY <sort_field> <asc/desc>, id <asc/desc>`
  * cursor MUST encode both `<sort_field>` and `id`.
* If an endpoint does not support stable cursor pagination for a given sort key, it SHALL reject the request with `400` and a clear error message.
* All supported sort keys SHALL be indexed or explicitly documented as “small dataset only.”

### 9.9 API Plane Naming Convention

**Rule A — Mandatory plane prefix**

* All APIs SHALL include a plane prefix immediately after `/api/v{N}`:
  * Platform plane: `/api/v{N}/platform/...`
  * Tenant plane: `/api/v{N}/tenant/...`
* Examples:
  * Platform:
    * `GET /api/v1/platform/tenants`
    * `PATCH /api/v1/platform/tenants/{tenant_id}`
    * `POST /api/v1/platform/tenants/{tenant_id}/impersonation-sessions`
  * Tenant:
    * `GET /api/v1/tenant/users`
    * `POST /api/v1/tenant/documents`
    * `GET /api/v1/tenant/exports/{export_id}`

**Rule B — Tenant context handling (critical)**

* Tenant plane endpoints MUST NOT require `tenant_id` in the path; tenant is resolved strictly from auth context.
* Platform plane endpoints MAY accept `tenant_id` explicitly because the platform operates across tenants.

**Rule C — Resource naming rules**

* Paths use plural nouns with hyphenless, lowercase segments.
* Actions are represented via subresources when possible (e.g., `/exports`, `/message-outbox`); `/actions/{action}` only when a true resource is not appropriate.
* Examples:
  * `POST /api/v1/tenant/exports` (create export job)
  * `POST /api/v1/tenant/exports/{export_id}/actions/cancel` (only if cancel is not modeled as a subresource)

**Rule D — Admin operations within tenant plane**

* Tenant admin privileged operations remain in the tenant plane (e.g., `POST /api/v1/tenant/users/{user_id}/actions/disable`).
* Do NOT move admin actions to `/platform/` unless a platform admin is acting.

**Rule E — Contract enforcement**

* Contracts repository SHALL publish separate OpenAPI tags or grouped sections for `platform_*` and `tenant_*`.
* CI MUST fail any PR that adds endpoints outside this plane naming rule.
* Net effect: the URL alone reveals the plane; tenant isolation rules remain consistent and enforceable.

## 10. Constraints and Design Drivers

10.1 No architectural forks in product instantiation.
10.2 Mandatory pgvector; AI always present (configurable/kill-switchable).
10.3 Zero-downtime expectations; horizontal scalability; mandatory rate limiting.
10.4 Background jobs MUST set tenant context.

## 11. Traceability

11.1 RTM SHALL map each requirement to APIs, DB tables, RBAC permissions, Web/Mobile UX, and tests.
11.2 CI/CD SHALL reject changes lacking RTM references.

## 12. Change Management & Governance

12.1 Baseline is upstream and authoritative; changes land in Baseline first.
12.2 Deviation rules: CTO approval for architecture; CEO approval for security/compliance deviations.
12.3 Baseline sync mandatory and documented for product repos.
12.4 Change management with RFCs/CAB as required.

## 13. Prioritization Key

* Must = Mandatory for Baseline and v1 products.
* Should = Important but deferrable with justification.
* Could = Optional/enhancement.

## 14. Appendices

### 14.1 RTM Template (example columns)

* Requirement ID | Priority | Description | API Endpoint(s) | DB Table/Column | RBAC Permission(s) | Web UI | Mobile UI | Test Case ID | Status | Notes

### 14.2 Role Catalog (baseline)

* Platform Admin (Platform Plane)
* Tenant Admin (Tenant Plane)
* Tenant Read-only (Tenant Plane)
* Support/Impersonation (controlled, audited, approval-based)

### 14.3 Watermark Template

* “CONFIDENTIAL – Tenant: {{tenant_name}} – Generated {{timestamp}}”

### 14.4 Non-Goals

* No per-tenant schema forks.
* No customer-supplied code execution.
* No AI autonomous actions.
* No cross-tenant analytics.

### 14.5 Baseline Data Model (Canonical Definitions)

#### Platform Tables

##### Table: `platform_tenants`

**Purpose**
Represents all tenant records under platform governance, including lifecycle state and residency binding.

**Columns**

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |
| id | IDENTIFIER (ULID) | No | Primary identifier for the tenant. |
| tenant_code | TEXT | No | Human-readable, unique tenant code. |
| name | TEXT | No | Display name for the tenant. |
| status | ENUM (Pending, Active, Suspended, Deactivated) | No | Lifecycle state for provisioning and enforcement. |
| region_id | IDENTIFIER (ULID) | No | References `platform_regions.id`; pins tenant to residency region. |
| billing_tier | TEXT | Yes | Logical tier label for entitlement checks. |
| created_at | TIMESTAMP (UTC) | No | Creation timestamp. |
| updated_at | TIMESTAMP (UTC) | No | Last updated timestamp. |
| suspended_at | TIMESTAMP (UTC) | Yes | When tenant was suspended, if applicable. |

**Constraints**

* Primary Key: `id`
* Foreign Keys: `region_id` → `platform_regions.id`
* Unique constraints: `tenant_code`
* Check constraints: `status` in allowed lifecycle states

**Tenancy & Security**

* Tenant-scoped: No (platform registry)
* RLS Required: No
* Residency-bound: Yes (via `region_id`)

**Audit Requirements**

* Audited: Yes
* Audit Events Triggered: create, update, status transitions

##### Table: `platform_regions`

**Purpose**
Defines platform-supported regions and residency policies for tenant pinning and data governance.

**Columns**

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |
| id | IDENTIFIER (ULID) | No | Primary identifier for the region. |
| code | TEXT | No | Region code (e.g., `US`, `EU`, `UK`). |
| name | TEXT | No | Region display name. |
| status | ENUM (Active, Deprecated) | No | Lifecycle for accepting new tenants. |
| residency_policy | JSONB | Yes | Canonical residency constraints and allowed services. |
| created_at | TIMESTAMP (UTC) | No | Creation timestamp. |
| updated_at | TIMESTAMP (UTC) | No | Last updated timestamp. |

**Constraints**

* Primary Key: `id`
* Unique constraints: `code`
* Check constraints: `status` in allowed lifecycle states

**Tenancy & Security**

* Tenant-scoped: No
* RLS Required: No
* Residency-bound: Yes (policy source)

**Audit Requirements**

* Audited: Yes
* Audit Events Triggered: create, update, status transitions

##### Table: `platform_feature_flags`

**Purpose**
Stores platform-owned feature flags and rollout policies to control platform and tenant feature exposure.

**Columns**

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |
| id | IDENTIFIER (ULID) | No | Primary identifier for the feature flag. |
| key | TEXT | No | Unique feature flag key. |
| description | TEXT | Yes | Canonical description of feature purpose. |
| scope | ENUM (Platform, Tenant) | No | Scope indicating flag applicability. |
| default_value | JSONB | No | Default flag payload or boolean state. |
| rollout_policy | JSONB | Yes | Targeting rules (regions, tenants, cohorts). |
| status | ENUM (Draft, Active, Deprecated, Archived) | No | Lifecycle state. |
| created_at | TIMESTAMP (UTC) | No | Creation timestamp. |
| updated_at | TIMESTAMP (UTC) | No | Last updated timestamp. |

**Constraints**

* Primary Key: `id`
* Unique constraints: `key`
* Check constraints: `status` in allowed lifecycle states; `scope` in allowed values

**Tenancy & Security**

* Tenant-scoped: No (platform-controlled; applied per tenant via policy)
* RLS Required: No
* Residency-bound: No (policy enforcement must respect tenant region downstream)

**Audit Requirements**

* Audited: Yes
* Audit Events Triggered: create, update, status transitions

##### Table: `platform_audit_events`

**Purpose**
Captures immutable platform-managed audit events for privileged actions, exports, messaging, AI usage, and impersonation.

**Columns**

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |
| id | IDENTIFIER (ULID) | No | Primary identifier for the audit event. |
| occurred_at | TIMESTAMP (UTC) | No | Event timestamp. |
| actor_type | ENUM (PlatformUser, TenantUser, ServiceAccount) | No | Actor classification. |
| actor_id | IDENTIFIER (ULID) | No | Identifier of the actor (platform or tenant context). |
| tenant_id | IDENTIFIER (ULID) | Yes | References `platform_tenants.id` when event is tenant-scoped. |
| action | TEXT | No | Canonical action name. |
| resource_type | TEXT | No | Resource category affected. |
| resource_id | IDENTIFIER (ULID) | Yes | Resource identifier when applicable. |
| metadata | JSONB | Yes | Structured event metadata. |
| created_at | TIMESTAMP (UTC) | No | Ingestion timestamp. |

**Constraints**

* Primary Key: `id`
* Foreign Keys: `tenant_id` → `platform_tenants.id` (nullable)
* Check constraints: `actor_type` in allowed values

**Tenancy & Security**

* Tenant-scoped: Partially (optional `tenant_id` for tenant events)
* RLS Required: No (platform-plane storage; tenant access mediated via views/policies)
* Residency-bound: Yes (events stored in tenant region when `tenant_id` present)

**Audit Requirements**

* Audited: Yes
* Audit Events Triggered: append-only create

##### Table: `platform_security_events`

**Purpose**
Records security and anomaly events (rate-limit breaches, OTP abuse, auth anomalies) for centralized monitoring and incident response.

**Columns**

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |
| id | IDENTIFIER (ULID) | No | Primary identifier for the security event. |
| occurred_at | TIMESTAMP (UTC) | No | Event timestamp. |
| tenant_id | IDENTIFIER (ULID) | Yes | References `platform_tenants.id` when tenant-affecting. |
| actor_type | ENUM (PlatformUser, TenantUser, ServiceAccount, Anonymous) | No | Actor classification. |
| actor_id | IDENTIFIER (ULID) | Yes | Actor identifier when available. |
| event_type | TEXT | No | Canonical security event type. |
| severity | ENUM (Info, Low, Medium, High, Critical) | No | Severity classification. |
| source | TEXT | No | Source system or service emitting the event. |
| metadata | JSONB | Yes | Structured context (IP, device, request identifiers). |
| created_at | TIMESTAMP (UTC) | No | Ingestion timestamp. |

**Constraints**

* Primary Key: `id`
* Foreign Keys: `tenant_id` → `platform_tenants.id` (nullable)
* Check constraints: `actor_type` and `severity` in allowed values

**Tenancy & Security**

* Tenant-scoped: Partially (optional `tenant_id` for tenant events)
* RLS Required: No (platform-plane storage; exposure to tenants via governed views)
* Residency-bound: Yes (events stored in tenant region when `tenant_id` present)

**Audit Requirements**

* Audited: Yes
* Audit Events Triggered: append-only create

#### Tenant Tables (RLS-Protected)

##### Table: `tenant_users`

**Purpose**
Stores tenant user profiles under tenant control with lifecycle and residency enforcement.

**Columns**

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |
| id | IDENTIFIER (ULID) | No | Primary identifier for the tenant user. |
| tenant_id | IDENTIFIER (ULID) | No | References `platform_tenants.id`; RLS anchor. |
| email | TEXT | No | Unique per tenant email for login/notification. |
| display_name | TEXT | Yes | Preferred display name. |
| status | ENUM (Pending, Active, Suspended, Disabled) | No | User lifecycle state. |
| created_at | TIMESTAMP (UTC) | No | Creation timestamp. |
| updated_at | TIMESTAMP (UTC) | No | Last updated timestamp. |
| deleted_at | TIMESTAMP (UTC) | Yes | Soft-delete marker when applicable. |

**Constraints**

* Primary Key: `id`
* Foreign Keys: `tenant_id` → `platform_tenants.id`
* Unique constraints: (`tenant_id`, `email`)
* Check constraints: `status` in allowed lifecycle states

**Tenancy & Security**

* Tenant-scoped: Yes
* RLS Required: Yes (enforce tenant_id equality)
* Residency-bound: Yes (via tenant region)

**Audit Requirements**

* Audited: Yes
* Audit Events Triggered: create, update, delete (soft), status transitions

##### Table: `tenant_identities`

**Purpose**
Holds authentication identities and credentials linked to tenant users for password and federated auth flows.

**Columns**

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |
| id | IDENTIFIER (ULID) | No | Primary identifier for the identity record. |
| tenant_id | IDENTIFIER (ULID) | No | References `platform_tenants.id`; RLS anchor. |
| user_id | IDENTIFIER (ULID) | No | References `tenant_users.id`. |
| provider | ENUM (Password, OIDC, SAML, MFA) | No | Identity provider type. |
| provider_subject | TEXT | No | Provider-specific subject/identifier. |
| credential_hash | TEXT | Yes | Argon2id/bcrypt hash for password identities. |
| status | ENUM (Active, Revoked) | No | Identity lifecycle. |
| created_at | TIMESTAMP (UTC) | No | Creation timestamp. |
| updated_at | TIMESTAMP (UTC) | No | Last updated timestamp. |

**Constraints**

* Primary Key: `id`
* Foreign Keys: `tenant_id` → `platform_tenants.id`; `user_id` → `tenant_users.id`
* Unique constraints: (`tenant_id`, `provider`, `provider_subject`)
* Check constraints: `provider` and `status` in allowed values

**Tenancy & Security**

* Tenant-scoped: Yes
* RLS Required: Yes
* Residency-bound: Yes

**Audit Requirements**

* Audited: Yes
* Audit Events Triggered: create, update, revoke

##### Table: `tenant_roles`

**Purpose**
Defines tenant-level role compositions from platform-owned permission catalog.

**Columns**

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |
| id | IDENTIFIER (ULID) | No | Primary identifier for the tenant role. |
| tenant_id | IDENTIFIER (ULID) | No | References `platform_tenants.id`. |
| name | TEXT | No | Role name unique per tenant. |
| description | TEXT | Yes | Role description. |
| permissions | JSONB | No | Canonical list of permission identifiers. |
| status | ENUM (Draft, Active, Deprecated, Archived) | No | Role lifecycle. |
| created_at | TIMESTAMP (UTC) | No | Creation timestamp. |
| updated_at | TIMESTAMP (UTC) | No | Last updated timestamp. |

**Constraints**

* Primary Key: `id`
* Foreign Keys: `tenant_id` → `platform_tenants.id`
* Unique constraints: (`tenant_id`, `name`)
* Check constraints: `status` in allowed lifecycle states

**Tenancy & Security**

* Tenant-scoped: Yes
* RLS Required: Yes
* Residency-bound: Yes

**Audit Requirements**

* Audited: Yes
* Audit Events Triggered: create, update, status transitions

##### Table: `tenant_role_assignments`

**Purpose**
Maps tenant users to roles, enabling scoped permissions and delegated administration.

**Columns**

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |
| id | IDENTIFIER (ULID) | No | Primary identifier for the assignment. |
| tenant_id | IDENTIFIER (ULID) | No | References `platform_tenants.id`. |
| user_id | IDENTIFIER (ULID) | No | References `tenant_users.id`. |
| role_id | IDENTIFIER (ULID) | No | References `tenant_roles.id`. |
| scope | JSONB | Yes | Optional scoped restrictions (e.g., store, project). |
| created_at | TIMESTAMP (UTC) | No | Creation timestamp. |
| updated_at | TIMESTAMP (UTC) | No | Last updated timestamp. |

**Constraints**

* Primary Key: `id`
* Foreign Keys: `tenant_id` → `platform_tenants.id`; `user_id` → `tenant_users.id`; `role_id` → `tenant_roles.id`
* Unique constraints: (`tenant_id`, `user_id`, `role_id`, `scope`)

**Tenancy & Security**

* Tenant-scoped: Yes
* RLS Required: Yes
* Residency-bound: Yes

**Audit Requirements**

* Audited: Yes
* Audit Events Triggered: create, update, delete

##### Table: `tenant_sessions`

**Purpose**
Tracks tenant user sessions for authentication, timeout enforcement, and device/risk assessments.

**Columns**

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |
| id | IDENTIFIER (ULID) | No | Primary identifier for the session. |
| tenant_id | IDENTIFIER (ULID) | No | References `platform_tenants.id`. |
| user_id | IDENTIFIER (ULID) | No | References `tenant_users.id`. |
| device_id | IDENTIFIER (ULID) | Yes | References `tenant_devices.id` when device is registered. |
| status | ENUM (Active, Revoked, Expired) | No | Session lifecycle. |
| expires_at | TIMESTAMP (UTC) | No | Absolute expiry. |
| last_seen_at | TIMESTAMP (UTC) | Yes | Last activity timestamp. |
| created_at | TIMESTAMP (UTC) | No | Creation timestamp. |
| updated_at | TIMESTAMP (UTC) | No | Last updated timestamp. |

**Constraints**

* Primary Key: `id`
* Foreign Keys: `tenant_id` → `platform_tenants.id`; `user_id` → `tenant_users.id`; `device_id` → `tenant_devices.id`
* Check constraints: `status` in allowed lifecycle states

**Tenancy & Security**

* Tenant-scoped: Yes
* RLS Required: Yes
* Residency-bound: Yes

**Audit Requirements**

* Audited: Yes
* Audit Events Triggered: create, update, revoke/expire

##### Table: `tenant_api_keys`

**Purpose**
Stores tenant-scoped API keys with rotation, revocation, and auditing metadata.

**Columns**

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |
| id | IDENTIFIER (ULID) | No | Primary identifier for the API key. |
| tenant_id | IDENTIFIER (ULID) | No | References `platform_tenants.id`. |
| label | TEXT | No | Human-readable label. |
| hashed_key | TEXT | No | Hashed API key material. |
| status | ENUM (Active, Revoked) | No | Lifecycle state. |
| expires_at | TIMESTAMP (UTC) | Yes | Optional expiry. |
| created_by | IDENTIFIER (ULID) | Yes | References `tenant_users.id` (creator). |
| created_at | TIMESTAMP (UTC) | No | Creation timestamp. |
| updated_at | TIMESTAMP (UTC) | No | Last updated timestamp. |
| revoked_at | TIMESTAMP (UTC) | Yes | Revocation timestamp. |

**Constraints**

* Primary Key: `id`
* Foreign Keys: `tenant_id` → `platform_tenants.id`; `created_by` → `tenant_users.id`
* Check constraints: `status` in allowed lifecycle states

**Tenancy & Security**

* Tenant-scoped: Yes
* RLS Required: Yes
* Residency-bound: Yes

**Audit Requirements**

* Audited: Yes
* Audit Events Triggered: create, update, revoke

##### Table: `tenant_impersonation_sessions`

**Purpose**
Records time-bound, fully audited impersonation sessions initiated by authorized platform or tenant administrators.

**Columns**

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |
| id | IDENTIFIER (ULID) | No | Primary identifier for the impersonation session. |
| tenant_id | IDENTIFIER (ULID) | No | References `platform_tenants.id`. |
| impersonator_id | IDENTIFIER (ULID) | No | Identifier of actor initiating impersonation (platform or tenant user). |
| target_user_id | IDENTIFIER (ULID) | No | References `tenant_users.id` being impersonated. |
| reason | TEXT | No | Approved justification for impersonation. |
| expires_at | TIMESTAMP (UTC) | No | Non-renewable expiry. |
| status | ENUM (Active, Expired, Revoked) | No | Session lifecycle. |
| created_at | TIMESTAMP (UTC) | No | Creation timestamp. |
| updated_at | TIMESTAMP (UTC) | No | Last updated timestamp. |
| revoked_at | TIMESTAMP (UTC) | Yes | When revoked early. |

**Constraints**

* Primary Key: `id`
* Foreign Keys: `tenant_id` → `platform_tenants.id`; `target_user_id` → `tenant_users.id`
* Check constraints: `status` in allowed lifecycle states

**Tenancy & Security**

* Tenant-scoped: Yes
* RLS Required: Yes (restricted to tenant and authorized viewers)
* Residency-bound: Yes

**Audit Requirements**

* Audited: Yes
* Audit Events Triggered: create, update, revoke/expire

##### Table: `tenant_templates`

**Purpose**
Manages tenant-specific or inherited messaging/templates with approval workflows and localization.

**Columns**

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |
| id | IDENTIFIER (ULID) | No | Primary identifier for the template. |
| tenant_id | IDENTIFIER (ULID) | No | References `platform_tenants.id`. |
| key | TEXT | No | Template key unique per tenant. |
| name | TEXT | No | Template display name. |
| content | JSONB | No | Structured content and localization payloads. |
| status | ENUM (Draft, Approved, Deprecated, Archived) | No | Template lifecycle. |
| version | INTEGER | No | Incrementing template version per tenant. |
| created_at | TIMESTAMP (UTC) | No | Creation timestamp. |
| updated_at | TIMESTAMP (UTC) | No | Last updated timestamp. |
| approved_at | TIMESTAMP (UTC) | Yes | Approval timestamp when status becomes Approved. |

**Constraints**

* Primary Key: `id`
* Foreign Keys: `tenant_id` → `platform_tenants.id`
* Unique constraints: (`tenant_id`, `key`, `version`)
* Check constraints: `status` in allowed lifecycle states

**Tenancy & Security**

* Tenant-scoped: Yes
* RLS Required: Yes
* Residency-bound: Yes

**Audit Requirements**

* Audited: Yes
* Audit Events Triggered: create, update, approve/deprecate/archive

##### Table: `tenant_message_outbox`

**Purpose**
Stores outbound messages queued for asynchronous delivery with retry and DLQ handling.

**Columns**

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |
| id | IDENTIFIER (ULID) | No | Primary identifier for the message. |
| tenant_id | IDENTIFIER (ULID) | No | References `platform_tenants.id`. |
| template_id | IDENTIFIER (ULID) | Yes | References `tenant_templates.id` when applicable. |
| channel | ENUM (WhatsApp, Email, SMS, Push) | No | Delivery channel. |
| payload | JSONB | No | Rendered content and parameters. |
| status | ENUM (Queued, Sending, Sent, Failed, Dead) | No | Delivery lifecycle. |
| retry_count | INTEGER | No | Number of retries attempted. |
| next_attempt_at | TIMESTAMP (UTC) | Yes | Scheduled next attempt. |
| created_at | TIMESTAMP (UTC) | No | Creation timestamp. |
| updated_at | TIMESTAMP (UTC) | No | Last updated timestamp. |

**Constraints**

* Primary Key: `id`
* Foreign Keys: `tenant_id` → `platform_tenants.id`; `template_id` → `tenant_templates.id`
* Check constraints: `status` and `channel` in allowed values; `retry_count` ≥ 0

**Tenancy & Security**

* Tenant-scoped: Yes
* RLS Required: Yes
* Residency-bound: Yes

**Audit Requirements**

* Audited: Yes
* Audit Events Triggered: create, status transitions, DLQ placement

##### Table: `tenant_message_delivery_log`

**Purpose**
Captures delivery attempts and outcomes for tenant messages to support compliance, retries, and analytics.

**Columns**

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |
| id | IDENTIFIER (ULID) | No | Primary identifier for the delivery log entry. |
| tenant_id | IDENTIFIER (ULID) | No | References `platform_tenants.id`. |
| message_id | IDENTIFIER (ULID) | No | References `tenant_message_outbox.id`. |
| attempt_number | INTEGER | No | Attempt sequence number. |
| status | ENUM (Pending, Delivered, Failed, Retried, Dead) | No | Attempt outcome. |
| provider_response | JSONB | Yes | Provider response payload. |
| occurred_at | TIMESTAMP (UTC) | No | Timestamp of the attempt. |
| created_at | TIMESTAMP (UTC) | No | Creation timestamp. |

**Constraints**

* Primary Key: `id`
* Foreign Keys: `tenant_id` → `platform_tenants.id`; `message_id` → `tenant_message_outbox.id`
* Unique constraints: (`message_id`, `attempt_number`)
* Check constraints: `status` in allowed values; `attempt_number` ≥ 1

**Tenancy & Security**

* Tenant-scoped: Yes
* RLS Required: Yes
* Residency-bound: Yes

**Audit Requirements**

* Audited: Yes
* Audit Events Triggered: append-only create

##### Table: `tenant_exports`

**Purpose**
Represents tenant export jobs with lifecycle, retention, and sensitivity tracking.

**Columns**

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |
| id | IDENTIFIER (ULID) | No | Primary identifier for the export. |
| tenant_id | IDENTIFIER (ULID) | No | References `platform_tenants.id`. |
| requested_by | IDENTIFIER (ULID) | Yes | References `tenant_users.id`. |
| export_type | TEXT | No | Logical export type identifier. |
| status | ENUM (Queued, Running, Succeeded, Failed, Expired) | No | Export lifecycle. |
| sensitivity | ENUM (Standard, Sensitive) | No | Sensitivity classification. |
| retention_ttl | INTERVAL | No | Retention duration before purge. |
| storage_url | TEXT | Yes | Signed URL or storage locator. |
| expires_at | TIMESTAMP (UTC) | Yes | Export availability expiry. |
| created_at | TIMESTAMP (UTC) | No | Creation timestamp. |
| updated_at | TIMESTAMP (UTC) | No | Last updated timestamp. |

**Constraints**

* Primary Key: `id`
* Foreign Keys: `tenant_id` → `platform_tenants.id`; `requested_by` → `tenant_users.id`
* Check constraints: `status` and `sensitivity` in allowed values

**Tenancy & Security**

* Tenant-scoped: Yes
* RLS Required: Yes
* Residency-bound: Yes (storage pinned to tenant region)

**Audit Requirements**

* Audited: Yes
* Audit Events Triggered: create, status transitions, expiry/purge

##### Table: `tenant_export_download_log`

**Purpose**
Tracks access to tenant exports for compliance, watermarks, and download rate enforcement.

**Columns**

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |
| id | IDENTIFIER (ULID) | No | Primary identifier for the download log entry. |
| tenant_id | IDENTIFIER (ULID) | No | References `platform_tenants.id`. |
| export_id | IDENTIFIER (ULID) | No | References `tenant_exports.id`. |
| user_id | IDENTIFIER (ULID) | Yes | References `tenant_users.id` when user-authenticated. |
| ip_address | TEXT | Yes | IP address of downloader. |
| user_agent | TEXT | Yes | User agent string. |
| occurred_at | TIMESTAMP (UTC) | No | Download timestamp. |

**Constraints**

* Primary Key: `id`
* Foreign Keys: `tenant_id` → `platform_tenants.id`; `export_id` → `tenant_exports.id`; `user_id` → `tenant_users.id`

**Tenancy & Security**

* Tenant-scoped: Yes
* RLS Required: Yes
* Residency-bound: Yes

**Audit Requirements**

* Audited: Yes
* Audit Events Triggered: append-only create

##### Table: `tenant_documents`

**Purpose**
Stores tenant-managed documents and metadata with retention and classification controls.

**Columns**

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |
| id | IDENTIFIER (ULID) | No | Primary identifier for the document. |
| tenant_id | IDENTIFIER (ULID) | No | References `platform_tenants.id`. |
| title | TEXT | No | Document title. |
| classification | ENUM (Public, Internal, Confidential, Restricted) | No | Data classification. |
| storage_url | TEXT | No | Storage locator or signed URL. |
| status | ENUM (Active, Archived, Deleted) | No | Lifecycle state. |
| created_by | IDENTIFIER (ULID) | Yes | References `tenant_users.id`. |
| created_at | TIMESTAMP (UTC) | No | Creation timestamp. |
| updated_at | TIMESTAMP (UTC) | No | Last updated timestamp. |
| deleted_at | TIMESTAMP (UTC) | Yes | Soft-delete marker when applicable. |

**Constraints**

* Primary Key: `id`
* Foreign Keys: `tenant_id` → `platform_tenants.id`; `created_by` → `tenant_users.id`
* Check constraints: `classification` and `status` in allowed values

**Tenancy & Security**

* Tenant-scoped: Yes
* RLS Required: Yes
* Residency-bound: Yes

**Audit Requirements**

* Audited: Yes
* Audit Events Triggered: create, update, delete (soft), status transitions

##### Table: `tenant_devices`

**Purpose**
Registers tenant devices for session binding, risk-based access, and remote wipe enforcement.

**Columns**

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |
| id | IDENTIFIER (ULID) | No | Primary identifier for the device. |
| tenant_id | IDENTIFIER (ULID) | No | References `platform_tenants.id`. |
| user_id | IDENTIFIER (ULID) | No | References `tenant_users.id`. |
| device_identifier | TEXT | No | Device fingerprint or platform-specific identifier. |
| platform | ENUM (iOS, Android, Web, Desktop) | No | Device platform. |
| status | ENUM (Active, Suspended, Revoked) | No | Device lifecycle. |
| last_seen_at | TIMESTAMP (UTC) | Yes | Last observed activity. |
| created_at | TIMESTAMP (UTC) | No | Registration timestamp. |
| updated_at | TIMESTAMP (UTC) | No | Last updated timestamp. |
| revoked_at | TIMESTAMP (UTC) | Yes | Revocation timestamp. |

**Constraints**

* Primary Key: `id`
* Foreign Keys: `tenant_id` → `platform_tenants.id`; `user_id` → `tenant_users.id`
* Unique constraints: (`tenant_id`, `device_identifier`)
* Check constraints: `platform` and `status` in allowed values

**Tenancy & Security**

* Tenant-scoped: Yes
* RLS Required: Yes
* Residency-bound: Yes

**Audit Requirements**

* Audited: Yes
* Audit Events Triggered: create, update, revoke

##### Table: `tenant_sync_state`

**Purpose**
Maintains sync cursors and checkpoints for offline-first mobile clients to ensure safe replay and conflict handling.

**Columns**

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |
| id | IDENTIFIER (ULID) | No | Primary identifier for the sync state record. |
| tenant_id | IDENTIFIER (ULID) | No | References `platform_tenants.id`. |
| user_id | IDENTIFIER (ULID) | No | References `tenant_users.id`. |
| device_id | IDENTIFIER (ULID) | Yes | References `tenant_devices.id`. |
| cursor | TEXT | Yes | Logical checkpoint/token for incremental sync. |
| last_synced_at | TIMESTAMP (UTC) | Yes | Timestamp of last successful sync. |
| status | ENUM (Active, Blocked) | No | Sync enablement state. |
| created_at | TIMESTAMP (UTC) | No | Creation timestamp. |
| updated_at | TIMESTAMP (UTC) | No | Last updated timestamp. |

**Constraints**

* Primary Key: `id`
* Foreign Keys: `tenant_id` → `platform_tenants.id`; `user_id` → `tenant_users.id`; `device_id` → `tenant_devices.id`
* Unique constraints: (`tenant_id`, `user_id`, `device_id`)
* Check constraints: `status` in allowed values

**Tenancy & Security**

* Tenant-scoped: Yes
* RLS Required: Yes
* Residency-bound: Yes

**Audit Requirements**

* Audited: Yes
* Audit Events Triggered: create, update, block

##### Table: `tenant_jobs`

**Purpose**
Represents tenant-scoped background jobs with explicit tenant context and lifecycle for observability and retries.

**Columns**

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |
| id | IDENTIFIER (ULID) | No | Primary identifier for the job. |
| tenant_id | IDENTIFIER (ULID) | No | References `platform_tenants.id`. |
| job_type | TEXT | No | Logical job type identifier. |
| status | ENUM (Queued, Running, Succeeded, Failed, Dead) | No | Job lifecycle. |
| payload | JSONB | Yes | Job parameters. |
| scheduled_at | TIMESTAMP (UTC) | Yes | When job is scheduled to start. |
| started_at | TIMESTAMP (UTC) | Yes | Actual start time. |
| completed_at | TIMESTAMP (UTC) | Yes | Completion time on success or terminal failure. |
| retry_count | INTEGER | No | Number of retries attempted. |
| created_at | TIMESTAMP (UTC) | No | Creation timestamp. |
| updated_at | TIMESTAMP (UTC) | No | Last updated timestamp. |

**Constraints**

* Primary Key: `id`
* Foreign Keys: `tenant_id` → `platform_tenants.id`
* Check constraints: `status` in allowed values; `retry_count` ≥ 0

**Tenancy & Security**

* Tenant-scoped: Yes
* RLS Required: Yes
* Residency-bound: Yes

**Audit Requirements**

* Audited: Yes
* Audit Events Triggered: create, status transitions, DLQ placement

##### Table: `tenant_ai_embeddings`

**Purpose**
Stores tenant vector embeddings for AI search and enrichment with residency and RLS enforcement.

**Columns**

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |
| id | IDENTIFIER (ULID) | No | Primary identifier for the embedding record. |
| tenant_id | IDENTIFIER (ULID) | No | References `platform_tenants.id`. |
| source_type | TEXT | No | Source entity type (e.g., document, message). |
| source_id | IDENTIFIER (ULID) | No | Identifier of the source entity within the tenant. |
| embedding | VECTOR (pgvector logical) | No | Vector representation (logical type). |
| status | ENUM (Active, Deprecated) | No | Embedding lifecycle. |
| created_at | TIMESTAMP (UTC) | No | Creation timestamp. |
| updated_at | TIMESTAMP (UTC) | No | Last updated timestamp. |

**Constraints**

* Primary Key: `id`
* Foreign Keys: `tenant_id` → `platform_tenants.id`
* Unique constraints: (`tenant_id`, `source_type`, `source_id`)
* Check constraints: `status` in allowed values

**Tenancy & Security**

* Tenant-scoped: Yes
* RLS Required: Yes
* Residency-bound: Yes (embedding storage pinned to tenant region)

**Audit Requirements**

* Audited: Yes
* Audit Events Triggered: create, update, deprecate

##### Table: `tenant_ai_requests_log`

**Purpose**
Logs AI requests and responses for attribution, auditing, and SLA enforcement per tenant.

**Columns**

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |
| id | IDENTIFIER (ULID) | No | Primary identifier for the AI request record. |
| tenant_id | IDENTIFIER (ULID) | No | References `platform_tenants.id`. |
| user_id | IDENTIFIER (ULID) | Yes | References `tenant_users.id` when user-initiated. |
| request_type | TEXT | No | Logical AI capability invoked. |
| input_reference | JSONB | Yes | References to input artifacts or prompts. |
| output_reference | JSONB | Yes | References to outputs or summaries. |
| status | ENUM (Pending, Succeeded, Failed) | No | Request lifecycle. |
| latency_ms | INTEGER | Yes | Observed latency in milliseconds. |
| created_at | TIMESTAMP (UTC) | No | Creation timestamp. |
| updated_at | TIMESTAMP (UTC) | No | Last updated timestamp. |

**Constraints**

* Primary Key: `id`
* Foreign Keys: `tenant_id` → `platform_tenants.id`; `user_id` → `tenant_users.id`
* Check constraints: `status` in allowed values; `latency_ms` ≥ 0 when present

**Tenancy & Security**

* Tenant-scoped: Yes
* RLS Required: Yes
* Residency-bound: Yes (processing and storage pinned to tenant region)

**Audit Requirements**

* Audited: Yes
* Audit Events Triggered: create, update, failure logging

**Additional Domain Tables**

Additional product-layer tables MAY be defined per product repository and MUST be explicitly labeled as **product-layer** outside this Baseline appendix.

---

“This iteration is strictly additive and clarifying. Add a Revision History section after the Executive Summary, and expand the Baseline Required Tables into canonical table definitions using the prescribed format. Do not reinterpret architecture, add new features, or change governance rules.”
