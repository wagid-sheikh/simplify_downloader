# simplify_downloader

Automated pipeline for Simplify TumbleDry MIS downloads including merge, ingestion, audit, and cleanup.

## Quick start

```bash
poetry install
poetry run pytest
poetry run python -m simplify_downloader run --stores_list "UN3668,KN3817"
```

To trigger just the dashboard downloader workflow (without the full pipeline),
use the helper script which ensures execution from the project root:

```bash
./scripts/run_dashboard_downloader.sh
```

Set `DATABASE_URL` to a Postgres asyncpg connection string.

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
