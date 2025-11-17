#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

# Always ensure the database schema exists before running the pipeline. This
# prevents confusing runtime failures like "relation system_config does not
# exist" when a fresh environment hasn't been migrated yet.
echo "[dashboard] Ensuring database migrations are up to date..."
poetry run alembic upgrade head

# This uses the new CLI subcommand
exec poetry run python -m simplify_downloader run-single-session "$@"
