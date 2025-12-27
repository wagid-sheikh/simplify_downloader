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

## Definition of Done
- Tests added or updated.
- RBAC checks enforced in UI flows.
- Audit-significant actions instrumented or handed off to backend endpoints that audit.
- RTM updated with requirement coverage.
- Contracts updated if applicable; generated types regenerated/consumed.

## Stop Conditions
AI agent MUST stop and request human input if:
- Tenant boundary is unclear in UI flows.
- Requirement is ambiguous.
- Contract impact is uncertain.
- Change risks bypassing RLS, audit, or residency rules.
