#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

# Export variables from .env (if present) so that tools outside Python (like
# Alembic) use the same configuration as the application itself. Without this
# step Alembic falls back to alembic.ini defaults (host "db"), which breaks in
# local development where developers typically point DATABASE_URL at localhost.
ENV_FILE="${REPO_ROOT}/.env"
if [[ -f "${ENV_FILE}" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${ENV_FILE}"
  set +a
fi

# Always ensure the database schema exists before running the pipeline. This
# prevents confusing runtime failures like "relation system_config does not
# exist" when a fresh environment hasn't been migrated yet.
echo "[dashboard] Ensuring database migrations are up to date..."
poetry run alembic upgrade head

# This uses the new CLI subcommand
exec poetry run python -m app run-single-session "$@"
