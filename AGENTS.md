# AGENTS.md

## Purpose of this file

This is the canonical operating guide for contributors (human + Codex) working in `simplify_downloader`.
Use this file plus `/docs/architecture.md` as the starting point. Many legacy markdown files exist and are not always current.

## Project overview

This repository is a Python 3.12 production pipeline service that:
- scrapes/ingests dashboard CSV data (`app/dashboard_downloader`),
- runs CRM order-sync pipelines for TD and UC (`app/crm_downloader`),
- generates and emails report PDFs (`app/reports`, `app/dashboard_downloader/report_generator.py`),
- runs lead assignment + PDF/outcome workflows (`app/lead_assignment`),
- persists operational telemetry (`pipeline_run_summaries`, `orders_sync_log`, `documents`) and sends templated notifications.

Core runtime entrypoint: `python -m app`.

## Repo layout (high-signal paths)

- `app/__main__.py` — top-level CLI/server wrapper.
- `app/config.py` — strict SSOT configuration loader (env + `system_config` DB + decryption).
- `app/common/` — shared DB/session, ingest, date/time, cleanup, audit helpers.
- `app/dashboard_downloader/` — single-session scraper pipeline, run summaries, notifications, reporting tail-step.
- `app/crm_downloader/` — TD/UC order-sync + profiler orchestrator.
- `app/reports/` — daily sales + pending deliveries report pipelines.
- `app/lead_assignment/` — assignment engine, PDF generation, outcomes ingestion.
- `alembic/` — migration environment + revision chain.
- `tests/` — pytest coverage for config, pipelines, migrations, scripts.
- `.github/workflows/` — CI test workflow + deploy workflow.
- `scripts/` — local/prod runner scripts.

## How to run / test / build

Use Poetry.

- Install deps: `poetry install`
- Run tests: `poetry run pytest`
- Run app CLI help: `poetry run python -m app --help`
- Run single-session pipeline: `poetry run python -m app run-single-session`
- Run DB upgrade: `poetry run python -m app db upgrade`
- DB checks: `poetry run python -m app db check`
- Notification diagnostics: `poetry run python -m app notifications test --pipeline <code> --run-id <run_id>`

Container paths:
- `docker compose up --build`
- Deploy workflow runs `docker compose run --rm --entrypoint alembic app upgrade head` on server.

## Coding and architectural conventions observed

1. **Config discipline is strict**
   - Read env/DB config only through `app.config` (`config` object).
   - `tests/test_config.py` enforces no ad-hoc `os.getenv` usage outside allowed files.
2. **Async DB access pattern**
   - Use `session_scope(database_url)` from `app/common/db.py`.
3. **Pipeline observability is first-class**
   - Use `JsonLogger` + `log_event`; include `run_id`, phase, status, store/window context.
4. **Store scope is DB-driven**
   - `store_master.etl_flag`, `report_flag`, `sync_orders_flag` control run scope.
5. **Notification contracts are DB-driven**
   - `pipelines`, `notification_profiles`, `email_templates`, `notification_recipients` are runtime dependencies.
6. **Date windows are centralized**
   - Use `app/common/date_utils.py` for timezone-aware daily/weekly/monthly logic.

## Change discipline (required)

- Keep changes scoped to the target subsystem; do not mix unrelated refactors.
- Inspect nearby tests first; extend existing test modules instead of creating detached patterns.
- Preserve pipeline names/codes and notification contracts unless intentionally migrating data + code together.
- When changing CLI behavior, update scripts under `scripts/` and relevant docs in `/docs`.
- When adding a new pipeline or report:
  - add run summary coverage,
  - validate notification profile/template expectations,
  - ensure document persistence contract remains consistent.

## Database / Alembic rules (non-negotiable)

- **Never modify historical Alembic migration files.**
- **Never insert a migration between existing revisions.**
- **Always create a new forward-only migration.**
- **Keep migration descriptive slug short and safe.**
- **Alembic migration descriptive slug must not exceed 32 characters.**
- **Verify `down_revision` carefully before creating a new migration.**
- **Review the full migration chain before finalizing.**
- Do not rely on docs alone for migration ordering; confirm current `alembic/versions` state.

## Documentation maintenance rules

Canonical docs to maintain first:
- `/AGENTS.md`
- `/docs/architecture.md`
- `/docs/decision-log.md`
- `/docs/pr-checklist.md`
- `/docs/feature-map.md`

Legacy docs under `/docs` (including `temp_md/`, `StepA-Response.md`, planning/spec prompts) are useful context but may be stale or contradictory. Keep canonical docs aligned to code/tests/workflows.

## Definition of done (for PRs)

- Relevant tests pass locally (`poetry run pytest`) for touched behavior.
- Migration policy followed (if schema touched).
- CLI/scripts/docs updated for operator-facing changes.
- No secrets committed; no credentials added to code/docs.
- Run summary + notification implications considered for pipeline changes.

## Forbidden actions / common mistakes

- Do not bypass `app.config` by reading env directly in random modules.
- Do not hardcode store lists when DB flags are the source of truth.
- Do not change historical migrations or reseed old migration data in place.
- Do not treat old markdown/spec files as authoritative without code confirmation.
- Do not introduce new logging style outside `JsonLogger`/`log_event` for pipeline flow telemetry.
