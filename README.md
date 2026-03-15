# crm-backend

Automated pipeline for TSV dashboard downloads including merge, ingestion, audit, and cleanup.

## Quick start

```bash
poetry install
poetry run pytest

# Prepare the database (creates tables / applies migrations)
poetry run python -m app db upgrade

# Execute the full download → ingest → audit pipeline via the new CLI
poetry run python -m app run --run-migrations
```

To trigger the downloader workflow using a single browser session for all
stores, use the helper script which ensures execution from the project root and
runs migrations before starting the pipeline:

```bash
./scripts/run_dashboard_pipeline_single_context.sh
```

Both entrypoints select stores based on `store_master.etl_flag = TRUE` so that
the ingestion scope comes directly from the database. Ensure a `DATABASE_URL`
variable points at the target Postgres instance when ingestion is desired.

## Legacy entrypoint migration

The legacy `python -m simplify_downloader` entrypoint has been removed. Scripts
and callers should invoke the service via `python -m app` instead. If external
automation still depends on the legacy name, publish a minimal stub package to
PyPI that forwards to `app` rather than introducing an in-repo alias.

See [`docs/configuration.md`](docs/configuration.md) for the authoritative list
of required environment variables, filesystem paths, and security guardrails
before running the pipelines in any environment.

## Docker

```bash
docker compose up --build
```

This starts Postgres (no public port) and runs the pipeline container.

## Database migrations

```bash
poetry run python -m app db upgrade
```

## Tests

```bash
poetry run pytest
```

## Orders sync profiler status semantics (alerting)

The `orders_sync_run_profiler` summary `overall_status` is intentionally
severity-driven for downstream alerting:

- `failed`: at least one failed window, or missing required windows when missing
  windows are not allowed.
- `partial`: no failures, but at least one partial window.
- `success_with_warnings`: no failed/partial windows, but at least one window
  completed with warning-class outcome (`success_with_warnings`).
- `success`: all completed windows are clean success/skipped with no warning-class
  outcomes.

This policy means warning windows are promoted to top-level
`success_with_warnings` so alerts and dashboards can reliably detect
non-clean runs without parsing per-window payloads.
