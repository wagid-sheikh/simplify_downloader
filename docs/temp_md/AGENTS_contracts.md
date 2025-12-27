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

## Definition of Done
- Specs updated with correct versioning.
- Breaking-change assessment completed; CI passing.
- Generated artifacts produced and published for downstream consumption.
- RTM updated with requirement coverage mapping.
- Compatibility window adherence documented.

## Stop Conditions
AI agent MUST stop and request human input if:
- Breaking-change impact is unclear.
- Compatibility window cannot be met.
- Security, audit, tenant context, or residency implications are ambiguous.
- Downstream regeneration impact is not understood.
