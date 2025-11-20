# crm-backend

Automated pipeline for TSV dashboard downloads including merge, ingestion, audit, and cleanup.

## Quick start

```bash
poetry install
poetry run pytest

# Prepare the database (creates tables / applies migrations)
poetry run python -m app db upgrade

# Execute the full download → ingest → audit pipeline via the new CLI
poetry run python -m app run --stores_list "A668,A817" --run-migrations
```

To trigger the downloader workflow using a single browser session for all
stores, use the helper script which ensures execution from the project root and
runs migrations before starting the pipeline:

```bash
./scripts/run_dashboard_pipeline_single_context.sh --stores_list "A668,A817"
```

The CLI and helper script honour the optional `--stores_list` flag (or the
`STORES_LIST` environment variable) and expect a `DATABASE_URL` environment
variable pointing at the target Postgres instance when ingestion is desired.

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
