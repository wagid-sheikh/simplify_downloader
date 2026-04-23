#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

if [[ -f "${REPO_ROOT}/.env" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${REPO_ROOT}/.env"
  set +a
fi

# Optional TD leads worker tuning (defaults shown):
# export TD_LEADS_MAX_WORKERS="${TD_LEADS_MAX_WORKERS:-2}"
# export TD_LEADS_PARALLEL_ENABLED="${TD_LEADS_PARALLEL_ENABLED:-1}"

exec poetry run python -m app.crm_downloader.td_leads_sync.main "$@"
