#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

ENV_FILE="${REPO_ROOT}/.env"
if [[ -f "${ENV_FILE}" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${ENV_FILE}"
  set +a
fi

ensure_writable_dir() {
  local dir_path="$1"
  mkdir -p "${dir_path}"
  if [[ ! -w "${dir_path}" ]]; then
    echo "Error: ${dir_path} is not writable" >&2
    exit 1
  fi
}

TD_SYNC_PATHS=()
while IFS= read -r line; do
  TD_SYNC_PATHS+=("$line")
done < <(poetry run python - <<'PY'
from app.crm_downloader.config import default_download_dir, default_profiles_dir

print(default_download_dir())
print(default_profiles_dir())
PY
)

DOWNLOAD_DIR="${TD_SYNC_PATHS[0]}"
PROFILES_DIR="${TD_SYNC_PATHS[1]}"

ensure_writable_dir "${DOWNLOAD_DIR}"
ensure_writable_dir "${PROFILES_DIR}"

CLI_ARGS=("$@")

# Example invocations (uncomment one to choose a pipeline slice)
exec poetry run python -m app.crm_downloader.td_orders_sync.main ${CLI_ARGS[@]+"${CLI_ARGS[@]}"}
# exec poetry run python -m app.crm_downloader.td_orders_sync.main --orders-only ${CLI_ARGS[@]+"${CLI_ARGS[@]}"}
# exec poetry run python -m app.crm_downloader.td_orders_sync.main --sales-only ${CLI_ARGS[@]+"${CLI_ARGS[@]}"}
