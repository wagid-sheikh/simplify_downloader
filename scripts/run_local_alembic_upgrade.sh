#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

# Export variables from .env (if present) so that tools outside Python (like
# Alembic) use the same configuration as the application itself. Without this
# step Alembic falls back to alembic.ini defaults (host "db"), which breaks in
# local development where developers typically point POSTGRES_* settings at localhost.
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
echo "[alembic] Ensuring database migrations are up to date..."

upgrade_output_file="$(mktemp)"
set +e
poetry run alembic upgrade head >"${upgrade_output_file}" 2>&1
upgrade_exit_code=$?
set -e

if [[ ${upgrade_exit_code} -eq 0 ]]; then
  cat "${upgrade_output_file}"
  rm -f "${upgrade_output_file}"
  exit 0
fi

if rg -q "Duplicate.*already exists" "${upgrade_output_file}"; then
  target_revision="$(sed -nE "s/.*Running upgrade [^ ]+ -> ([^, ]+).*/\1/p" "${upgrade_output_file}" | tail -n1)"

  if [[ -n "${target_revision}" ]]; then
    echo "[alembic] Detected pre-existing database object while applying revision ${target_revision}."
    echo "[alembic] Stamping database to ${target_revision} and continuing upgrade."
    poetry run alembic stamp "${target_revision}"
    poetry run alembic upgrade head
    rm -f "${upgrade_output_file}"
    exit 0
  fi
fi

cat "${upgrade_output_file}"
rm -f "${upgrade_output_file}"
exit ${upgrade_exit_code}
