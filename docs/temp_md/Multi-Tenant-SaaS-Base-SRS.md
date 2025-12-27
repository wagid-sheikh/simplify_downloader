# Software Requirements Specification (SRS)

## TSV Universal Multi-Tenant SaaS Baseline

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

## 5. Overall Constraints and Assumptions

5.1 Shared schema with mandatory RLS (fail-closed).
5.2 No secrets in code; secrets injected from environment (GitHub Secrets).
5.3 Separate DEV/STAGING/PROD; no prod secrets in non-prod.
5.4 Offline-first for mobile is non-negotiable.
5.5 AI capabilities SHALL respect dataset classification and auditing.
5.6 Tenant data SHALL NOT cross regions except encrypted backups; tenants pinned to region at creation (US, EU, UK).

### 5.1 Tenant Context Invariants

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

### 7.9 Data Residency & Regioning

* NFR-DR-01 (Must): Supported regions v1: US, EU, UK; tenant pinned to region at creation.
* NFR-DR-02 (Must): Data SHALL NOT cross regions except encrypted backups; read replicas remain in-region.

## 8. Data Management

8.1 Offline storage encrypted, expirable, remote-wipe.
8.2 Disallowed offline: passwords, API keys/tokens, payment instruments, government IDs, biometric data, full audit logs.
8.3 Allowed offline: orders, customers (non-sensitive fields), operational metadata, cached reference data.
8.4 AI data: embeddings and processing in-tenant region; no cross-tenant/regional mixing; vector update SLAs per 6.4.
8.5 Retention: Audit/security events 7 years; access logs 90d; app logs 30d; metrics/traces 14–30d; legal hold indefinite.

## 9. Interfaces

9.1 API: Versioned FastAPI; strict timeouts; rate-limited.
9.2 Web: React mobile-first.
9.3 Mobile: React Native offline-first; device registry; conflict UX.
9.4 Messaging: WhatsApp via Playwright/Selenium MVP; future Meta Cloud API drop-in.
9.5 Integrations: Ticketing (v1), Analytics (v1), Payment Gateway (v2).
9.6 Exports: Signed URLs with TTL; watermarking for PDFs.

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
