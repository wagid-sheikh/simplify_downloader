# AGENTS.md — Frontend Web Repository

## Purpose
This frontend web repository is authored by AI agents under strict governance. The TSV Universal Multi-Tenant SaaS Baseline SRS is authoritative for scope, requirements, and behavior.

## Non-Negotiable Rules
- No architectural forks.
- Tenant Context Invariants MUST be preserved.
- RLS constraints on data access MUST be respected.
- No cross-tenant access.
- All privileged actions MUST be audited.
- No breaking API changes without contract updates and versioning.
- No bypassing security, audit, or residency controls.
- Client must treat `sort` as a user choice; default to backend default unless user selects.
- Client must send cursor tokens exactly as returned; no client-generated cursors.

## Repository Scope
- Owns React (mobile-first) web UX, RBAC enforcement in UI, and API consumption.
- MUST NOT modify backend schemas, RLS, or worker logic.
- MUST use generated types from the contracts repository; backend contracts are authoritative.

## Branching & PR Rules
- Branch names MUST reflect purpose (e.g., `feat/<summary>`, `fix/<summary>`).
- PR checklist MUST include: requirement IDs touched; tenant impact statement; security/audit impact; migration impact; contract impact (if any).

## Coding & Quality Standards
- Enforce formatting/linting per project config.
- Tests (unit/integration/UI) MUST be added/updated for behavior changes.
- Logs/telemetry MUST propagate tenant context in API calls where applicable.
- UI MUST surface audit-relevant actions and respect RBAC/feature flags.
- The web UI SHALL NOT attempt to create audit records directly; audits are emitted by backend only.
- Web repo MUST pin a contracts version (tag) and update only via explicit PR.

## UI System & Design Governance (Mandatory)
- The Web frontend MUST use a single AppShell (master layout) that owns global structure, navigation, tenant context display, notifications, and error boundaries.
- All routes/pages MUST render within approved layouts (e.g., platform layout, tenant layout, auth layout); standalone or page-specific scaffolding is prohibited.
- Page-level CSS, inline styling, or bespoke layout logic is NOT permitted unless the shared design system is explicitly extended.
- All visual elements MUST be implemented using shared UI primitives and page templates sourced from the common design system.
- If a required component does not exist, it MUST be added to the shared component library before being referenced by a page.
- Feature flags and UI toggles MUST originate from backend-served configuration (Redis-backed snapshots) and MUST NOT be hardcoded or locally defined.
- UI changes MUST include visual regression coverage (e.g., snapshots) for affected shared components or templates.
- Violations of these governance rules MUST fail CI and SHALL be treated as release blockers.

## Definition of Done
- Tests added or updated.
- RBAC checks enforced in UI flows.
- Audit-significant actions instrumented or handed off to backend endpoints that audit.
- RTM updated with requirement coverage.
- Contracts updated if applicable; generated types regenerated/consumed.

## Configuration & Secrets Handling Rules
- Direct access to environment variables is prohibited outside bootstrap modules. Environment variables are limited to bootstrapping PostgreSQL/Redis connectivity, secrets/signing keys, service identity/version, and observability exporters; `.env` files SHALL be near-empty.
- Direct access to `platform_config` or `tenant_config` tables is prohibited outside the configuration subsystem. Frontend-generated assets MUST rely on backend-provided configuration surfaced via Redis-backed snapshots and MUST NOT introduce divergent configuration paths.
- Consumers MUST treat the backend configuration service/module as authoritative; all feature flags or toggles exposed to the UI MUST flow from the merged platform/tenant snapshot served via Redis (`cfg:platform:{platform_config_version}` and `cfg:tenant:{tenant_id}:{tenant_config_version}` immutable JSON keys).
- CI MUST fail on unauthorized `os.getenv()` usage, direct configuration table access, or attempts to introduce new `.env` variables outside the bootstrap allowlist. Staging and production MUST emit runtime warnings or structured security events on unauthorized environment access.

## Stop Conditions
AI agent MUST stop and request human input if:
- Tenant boundary is unclear in UI flows.
- Requirement is ambiguous.
- Contract impact is uncertain.
- Change risks bypassing RLS, audit, or residency rules.
