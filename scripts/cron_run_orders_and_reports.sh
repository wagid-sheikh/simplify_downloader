#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${ENV_FILE:-${SCRIPT_DIR}/cron.env}"

if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
fi

# -------- CONFIG --------
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${REPO_ROOT}/logs"
TIMESTAMP="$(date '+%Y-%m-%d_%H-%M-%S')"
LOG_FILE="${LOG_DIR}/cron_run_orders_and_reports_${TIMESTAMP}.log"

mkdir -p "${LOG_DIR}"
cd "${REPO_ROOT}"

echo "=== CRON RUN STARTED @ $(date) ===" >> "${LOG_FILE}"

# IMPORTANT: ensure poetry is available to cron
# export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"
CRON_HOME="${CRON_HOME:-${HOME:-/tmp}}"
export HOME="${CRON_HOME}"

CRON_PATH="${CRON_PATH:-/usr/local/bin:/opt/homebrew/bin}"
export PATH="${CRON_PATH}:${PATH}"
echo "ENV_FILE=${ENV_FILE}" >> "${LOG_FILE}"
echo "HOME=${HOME}" >> "${LOG_FILE}"
echo "PATH=$PATH" >> "${LOG_FILE}"
echo "poetry=$(command -v poetry || echo NOT_FOUND)" >> "${LOG_FILE}"

bootstrap_poetry_env() {
  local preferred_python="${CRON_PYTHON_BIN:-python3.12}"
  local fallback_python="${CRON_PYTHON_FALLBACK_BIN:-python3}"
  local selected_python=""

  if command -v "${preferred_python}" >/dev/null 2>&1; then
    selected_python="${preferred_python}"
  elif command -v "${fallback_python}" >/dev/null 2>&1; then
    selected_python="${fallback_python}"
  fi

  if [[ -n "${selected_python}" ]]; then
    echo "Bootstrapping poetry env with ${selected_python}" >> "${LOG_FILE}"
    poetry env use "${selected_python}" >> "${LOG_FILE}" 2>&1
  else
    echo "No configured Python binary found for poetry env bootstrap; using poetry defaults." >> "${LOG_FILE}"
  fi

  if ! poetry run python -c "import sqlalchemy" >> "${LOG_FILE}" 2>&1; then
    echo "Poetry environment missing dependencies. Running poetry install --no-interaction --no-root --sync" >> "${LOG_FILE}"
    poetry install --no-interaction --no-root --sync >> "${LOG_FILE}" 2>&1
  fi
}

bootstrap_poetry_env

echo "--- Running Script 1: orders_sync_run_profiler ---" >> "${LOG_FILE}"
./scripts/orders_sync_run_profiler.sh >> "${LOG_FILE}" 2>&1

echo "--- Script 1 completed successfully ---" >> "${LOG_FILE}"

echo "--- Running Script 2: daily_sales_report ---" >> "${LOG_FILE}"
./scripts/run_local_reports_daily_sales.sh >> "${LOG_FILE}" 2>&1
echo "--- Script 2 completed successfully ---" >> "${LOG_FILE}"


echo "--- Running Script 3: pending_deliveries ---" >> "${LOG_FILE}"
./scripts/run_local_reports_pending_deliveries.sh >> "${LOG_FILE}" 2>&1
echo "--- Script 3 completed successfully ---" >> "${LOG_FILE}"

echo "=== CRON RUN FINISHED @ $(date) ===" >> "${LOG_FILE}"
