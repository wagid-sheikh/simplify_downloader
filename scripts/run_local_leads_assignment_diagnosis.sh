#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

# Export variables from .env (if present) so that tools outside Python (like
# Alembic) use the same configuration as the application itself.
ENV_FILE="${REPO_ROOT}/.env"
if [[ -f "${ENV_FILE}" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${ENV_FILE}"
  set +a
fi

echo "[leads-assignment] Ensuring database migrations are up to date..."
poetry run alembic upgrade head

exec poetry run python app/lead_assignment/assignment_failure_diagnosis.py "$@"
