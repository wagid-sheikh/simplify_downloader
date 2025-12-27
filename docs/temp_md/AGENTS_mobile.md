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

## Definition of Done
- Tests added or updated.
- Offline queue behaviors validated for tenant scoping and conflict handling.
- RLS assumptions honored via backend API usage.
- RTM updated with requirement coverage.
- Contracts updated if applicable; generated types regenerated/consumed.

## Stop Conditions
AI agent MUST stop and request human input if:
- Tenant boundary is unclear in offline/online transitions.
- Requirement is ambiguous.
- Contract impact is uncertain.
- Change risks bypassing RLS, audit, or residency rules.
