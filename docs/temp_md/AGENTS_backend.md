# AGENTS.md — Backend Repository

## Purpose
This backend repository is authored by AI agents under strict governance. The TSV Universal Multi-Tenant SaaS Baseline SRS is authoritative for scope, requirements, and behavior.

## Non-Negotiable Rules
- No architectural forks.
- Tenant Context Invariants MUST be preserved.
- RLS MUST be fail-closed.
- No cross-tenant access.
- All privileged actions MUST be audited.
- No breaking API changes without contract updates and versioning.
- No bypassing security, audit, or residency controls.
- Backend CI MUST run breaking-change detection against OpenAPI and fail on breaking changes unless an API version bump is included.
- Contract changes:
  - Any API change MUST have a corresponding contracts update (linked PR) before merge.
- Pagination:
  - List endpoints MUST implement cursor pagination. Default is `sort=-id`. If `sort` is provided, enforce allowlist and stable ordering with `id` as tie-breaker.

## Repository Scope
- Owns FastAPI application, background workers, PostgreSQL schema/migrations, tenant isolation, RLS, audit, AI, and messaging logic.
- MUST NOT change frontend/mobile code, UX, or device/offline behaviors owned by other repositories.
- MUST respect contracts repository as the source of truth for APIs and generated types.

## Branching & PR Rules
- Branch names MUST reflect purpose (e.g., `feat/<summary>`, `fix/<summary>`).
- PR checklist MUST include: requirement IDs touched; tenant impact statement; security/audit impact; migration impact; contract impact (if any).

## Coding & Quality Standards
- Enforce formatting/linting per project config.
- Tests MUST be added/updated for behavior changes.
- Logs MUST propagate `tenant_id`; structured logging required.
- Observability instrumentation (metrics/traces) MUST include tenant context where applicable.
- All DB changes MUST be via Alembic; no manual production changes.
- Any schema migration MUST include a rollback plan (or explicit irreversible marker).
- **Alembic Migration Naming Constraint:** Whenever an Alembic migration script is created, both the script filename and the revision identifier MUST NOT exceed 32 characters. This rule exists to prevent downstream tooling, filesystem, and CI/CD issues and SHALL be enforced by code review and CI validation.
- RLS policies MUST be tested for any table added or modified.

## Definition of Done
- Tests added or updated.
- RLS verified for affected tables/queries.
- Audit events implemented where applicable.
- RTM updated with requirement coverage.
- Contracts updated if applicable; generated types consumed.

## Stop Conditions
AI agent MUST stop and request human input if:
- Tenant boundary is unclear.
- Requirement is ambiguous.
- Contract impact is uncertain.
- Change risks bypassing RLS, audit, or residency rules.
- Change touches auth/session/impersonation/export/AI; require explicit security review checklist completion.
