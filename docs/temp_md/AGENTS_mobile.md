# AGENTS.md — Mobile Applications Repository

## Purpose
This mobile repository is authored by AI agents under strict governance. The TSV Universal Multi-Tenant SaaS Baseline SRS is authoritative for scope, requirements, and behavior.

## Non-Negotiable Rules
- No architectural forks.
- Tenant Context Invariants MUST be preserved.
- RLS constraints on data access MUST be respected through backend APIs.
- No cross-tenant access.
- All privileged actions MUST be audited.
- No breaking API changes without contract updates and versioning.
- No bypassing security, audit, or residency controls.
- Client must treat `sort` as a user choice; default to backend default unless user selects.
- Client must send cursor tokens exactly as returned; no client-generated cursors.

## Repository Scope
- Owns React Native mobile applications (iOS/Android), offline-first implementation, device registry, sync logic, and conflict resolution UX.
- MUST NOT modify backend schemas or RLS logic.
- MUST use generated types from the contracts repository; backend contracts are authoritative.

## Branching & PR Rules
- Branch names MUST reflect purpose (e.g., `feat/<summary>`, `fix/<summary>`).
- PR checklist MUST include: requirement IDs touched; tenant impact statement; security/audit impact; migration impact; contract impact (if any).

## Coding & Quality Standards
- Enforce formatting/linting per project config.
- Tests (unit/integration/E2E) MUST be added/updated for behavior changes.
- Telemetry MUST propagate tenant context where applicable; offline mutations MUST retain tenant identity.
- Sync logic MUST honor conflict policies and audit-significant actions.
- Mobile SHALL remain compatible with N-1 backend API version for at least 90 days (or defined deprecation window) due to app-store rollout latency.
- Mobile MUST maintain an explicit offline allowlist; any new offline-persisted field requires security classification review.
- Any change impacting sync protocol or offline storage schema MUST include a migration path and downgrade strategy.

## Definition of Done
- Tests added or updated.
- Offline queue behaviors validated for tenant scoping and conflict handling.
- RLS assumptions honored via backend API usage.
- RTM updated with requirement coverage.
- Contracts updated if applicable; generated types regenerated/consumed.

## Configuration & Secrets Handling Rules
- Direct access to environment variables is prohibited outside bootstrap modules. Environment variables are limited to bootstrapping PostgreSQL/Redis connectivity, secrets/signing keys, service identity/version, and observability exporters; `.env` files SHALL be near-empty.
- Direct access to `platform_config` or `tenant_config` tables is prohibited outside the configuration subsystem. Mobile clients MUST consume configuration surfaced via backend APIs that themselves rely on Redis-hosted configuration snapshots and MUST NOT introduce local configuration forks.
- Backend configuration service/module is authoritative; any mobile feature flags or toggles MUST derive from merged platform/tenant snapshots stored in Redis (`cfg:platform:{platform_config_version}` and `cfg:tenant:{tenant_id}:{tenant_config_version}` immutable JSON keys).
- CI MUST fail on unauthorized `os.getenv()` usage, direct configuration table access, or attempts to introduce new `.env` variables outside the bootstrap allowlist. Staging and production MUST emit runtime warnings or structured security events on unauthorized environment access.
- If a value is configurable, it MUST come from the merged Redis snapshot via the configuration subsystem. If a value is not in the snapshot, it is NOT configurable and MUST NOT be introduced via environment variables, ad-hoc Redis keys, or local constants.

## Operational Compliance — Mobile Configuration
- Mobile MUST treat configuration as server-authoritative; any caching MUST use only backend-provided, non-sensitive configuration payloads and respect explicit expirations.
- UI and sync flows MUST NOT define local flags/toggles; any new mobile flag requires contracts updates and backend exposure. Offline caches MUST NOT store secrets/tokens and MUST honor the offline allowlist.
- CI enforcement checklist: block unauthorized `os.getenv()`, block new `.env` variables, block local flag definitions, block divergence from generated contract types, and ensure configuration endpoints are the sole source for cached configuration.

## Mobile UI System & Screen Governance (Mandatory)
- Mobile UI MUST be built using standardized screen templates (e.g., list, detail, form, wizard, offline-aware screens) that are centrally defined and versioned.
- Screens MUST compose shared components and theme tokens; per-screen styling divergence is prohibited.
- All screens MUST handle loading, empty, error, and offline states via shared components.
- Offline indicators, sync banners, retry UX, and conflict states MUST use standardized components rather than screen-specific logic.
- Mobile screens MUST NOT introduce local styling systems, inline styles, or visual overrides unless the shared component system is extended.
- Any new UI element MUST be added to the shared component kit, documented, and reused before screen adoption.
- Mobile UI behavior MUST remain compatible with N-1 backend API versions and MUST reflect backend-driven configuration only.
- Forking visual behavior per screen or feature is prohibited; violations MUST fail CI and SHALL be treated as release blockers.

## Stop Conditions
AI agent MUST stop and request human input if:
- Tenant boundary is unclear in offline/online transitions.
- Requirement is ambiguous.
- Contract impact is uncertain.
- Change risks bypassing RLS, audit, or residency rules.
