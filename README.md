# simplify_downloader

Automated pipeline for Simplify TumbleDry MIS downloads including merge, ingestion, audit, and cleanup.

## Quick start

```bash
poetry install
poetry run pytest

# Prepare the database (creates tables / applies migrations)
poetry run python -m simplify_downloader db upgrade

# Execute the full download → ingest → audit pipeline
./scripts/run_dashboard_pipeline.sh --stores_list "A668,A817"
```

To trigger just the dashboard downloader workflow (without touching the
database), use the helper script which ensures execution from the project
root:

```bash
./scripts/run_dashboard_downloader.sh
```

Both scripts honour the optional `--stores_list` flag (or the `STORES_LIST`
environment variable) and expect a `DATABASE_URL` environment variable pointing
at the target Postgres instance when ingestion is desired.

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
poetry run python -m simplify_downloader db upgrade
```

## Tests

```bash
poetry run pytest
```
