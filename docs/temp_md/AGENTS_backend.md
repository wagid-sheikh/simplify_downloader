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

## Configuration & Secrets Handling Rules
- Direct access to environment variables is prohibited outside bootstrap modules. Environment variables are limited to bootstrapping PostgreSQL/Redis connectivity, secrets/signing keys, service identity/version, and observability exporters; `.env` files SHALL be near-empty.
- Direct access to `platform_config` or `tenant_config` tables is prohibited outside the configuration subsystem. All runtime configuration MUST be read via Redis snapshots populated from these tables.
- All services/workers/agents MUST obtain configuration through a single configuration module/service that reads Redis, falls back to configuration tables on cache miss, merges platform/tenant snapshots, and returns immutable per-request/job snapshots. Redis keyspace conventions are mandatory: `cfg:platform:{platform_config_version}` and `cfg:tenant:{tenant_id}:{tenant_config_version}` with immutable JSON snapshots.
- CI MUST fail on unauthorized `os.getenv()` usage, direct configuration table access, or attempts to introduce new `.env` variables outside the bootstrap allowlist. Staging and production MUST emit runtime warnings or structured security events on unauthorized environment access.
- If a value is configurable, it MUST come from the merged Redis snapshot via the configuration subsystem. If a value is not in the snapshot, it is NOT configurable and MUST NOT be introduced via environment variables, ad-hoc Redis keys, or local constants.

## Operational Compliance — Configuration
- Handlers, workers, and CLI entrypoints MUST accept an injected `ConfigSnapshot` (or obtain it once at the entrypoint) and MUST NOT call the database or environment for configuration.
- No DB session may query `platform_config` or `tenant_config` outside the configuration subsystem; violations MUST raise structured security events in STAGING/PROD with stack traces.
- Any new setting MUST land in `platform_config`/`tenant_config` schemas, snapshot serializer, and validation logic before it is referenced in code; partial Redis keys or mutable overwrites are prohibited.
- Runtime guardrails: Unauthorized env access or direct configuration table reads MUST emit structured security events (classification + stack trace) and MAY abort execution in production to prevent drift.
- CI enforcement checklist (must stay enabled): block unauthorized `os.getenv()`, block direct config table queries outside the subsystem, block new `.env` variables beyond the allowlist, block contract drift (backend must consume generated types and config-surfacing endpoints defined in contracts).

## Stop Conditions
AI agent MUST stop and request human input if:
- Tenant boundary is unclear.
- Requirement is ambiguous.
- Contract impact is uncertain.
- Change risks bypassing RLS, audit, or residency rules.
- Change touches auth/session/impersonation/export/AI; require explicit security review checklist completion.
