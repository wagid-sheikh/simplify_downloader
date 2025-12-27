# AGENTS.md — Contracts Repository

## Purpose
This contracts repository is authored by AI agents under strict governance. The TSV Universal Multi-Tenant SaaS Baseline SRS is authoritative. OpenAPI specifications and shared schemas in this repo are the single source of truth for all services and clients.

## Non-Negotiable Rules
- No architectural forks; contracts define the canonical interface.
- OpenAPI specifications SHALL be authoritative; generated types MUST derive from these specs.
- Semantic versioning MUST be followed; breaking changes REQUIRE a new major or versioned path (e.g., `/api/v2`).
- Backend, web, and mobile MUST consume generated types from this repository.
- Backward compatibility window: Backend SHALL support N-1 contract minor version for ≥ 90 days or until mobile release adoption threshold is met.
- Breaking-change detection MUST run in CI; breaking changes without required versioning MUST fail.
- No bypassing security, audit, tenant context, or residency controls in contract definitions.
- All list endpoints must specify: `cursor`, `limit`, `sort` (if supported), and define cursor semantics per sort mode.

## Repository Scope
- Owns OpenAPI specs, shared schemas, and generated types artifacts.
- Defines API versioning rules and compatibility guarantees.
- Provides published tags/releases that downstream repos pin to.
- MUST NOT contain service-specific implementation code.

## Branching & PR Rules
- Branch names MUST reflect purpose (e.g., `feat/<summary>`, `fix/<summary>`).
- PR checklist MUST include: requirement IDs touched; contract impact statement; breaking-change assessment; compatibility window impact; regeneration impact.

## Coding & Quality Standards
- Semantic versioning enforced for releases.
- CI MUST run breaking-change detection against prior released spec.
- Generated types/schemas MUST be regenerated and committed/published when specs change.
- Documentation for migrations MUST accompany breaking changes.

## Configuration & Secrets Handling Rules
- Direct access to environment variables is prohibited outside bootstrap modules. Environment variables are limited to bootstrapping PostgreSQL/Redis connectivity, secrets/signing keys, service identity/version, and observability exporters; `.env` files SHALL be near-empty.
- Direct access to `platform_config` or `tenant_config` tables is prohibited outside the configuration subsystem. Runtime consumers MUST rely on Redis-served configuration snapshots populated from these tables.
- Contracts MUST document that services obtain configuration only through a single configuration service/module that reads Redis, falls back to configuration tables on cache miss, merges platform/tenant snapshots, and returns immutable per-request/job snapshots. Redis keyspace conventions are mandatory: `cfg:platform:{platform_config_version}` and `cfg:tenant:{tenant_id}:{tenant_config_version}` with immutable JSON snapshots.
- CI MUST fail on unauthorized `os.getenv()` usage, direct configuration table access, or attempts to introduce new `.env` variables outside the bootstrap allowlist. Staging and production MUST emit runtime warnings or structured security events on unauthorized environment access.
- If a value is configurable, it MUST come from the merged Redis snapshot via the configuration subsystem. If a value is not in the snapshot, it is NOT configurable and MUST NOT be introduced via environment variables, ad-hoc Redis keys, or local constants.

### Client Configuration Delivery Contracts (Mandatory)

- Any configuration exposed to clients MUST be represented in OpenAPI schemas.
- Generated client types ARE the only permissible interface for configuration consumption.
- Any new client-visible configuration REQUIRES a contracts update before frontend/mobile usage.

## Operational Compliance — Contracts
- Contracts MUST include endpoints or schemas for delivering merged configuration snapshots to clients when applicable; exposed configuration fields MUST be typed in OpenAPI and consumed only via generated types.
- Any new configuration surface requires simultaneous updates to contracts, generated artifacts, and downstream consumers; UI/mobile-local flags are prohibited.
- CI enforcement checklist: breaking-change detection, verification that configuration endpoints remain typed, and regeneration of client/server types before merge.

## Definition of Done
- Specs updated with correct versioning.
- Breaking-change assessment completed; CI passing.
- Generated artifacts produced and published for downstream consumption.
- RTM updated with requirement coverage mapping.
- Compatibility window adherence documented.

## Requirements Traceability & Evidence (Mandatory)
- Each PR MUST identify requirement IDs and RTM update evidence.
- If a change does not map to an existing requirement, the agent MUST stop and request clarification.
- RTM artifacts SHALL NOT be duplicated or restructured per PR.

## Stop Conditions
AI agent MUST stop and request human input if:
- Breaking-change impact is unclear.
- Compatibility window cannot be met.
- Security, audit, tenant context, or residency implications are ambiguous.
- Downstream regeneration impact is not understood.
