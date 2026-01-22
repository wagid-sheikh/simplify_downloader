#!/usr/bin/env bash
set -euo pipefail

# -------- CONFIG --------
REPO_ROOT="/Users/wagidsheikh/crm_backend/simplify_downloader"
LOG_DIR="${REPO_ROOT}/logs"
TIMESTAMP="$(date '+%Y-%m-%d_%H-%M-%S')"
LOG_FILE="${LOG_DIR}/cron_run_orders_and_reports_${TIMESTAMP}.log"

mkdir -p "${LOG_DIR}"
cd "${REPO_ROOT}"

echo "=== CRON RUN STARTED @ $(date) ===" >> "${LOG_FILE}"

# IMPORTANT: ensure poetry is available to cron
export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"

echo "--- Running Script 1: orders_sync_run_profiler ---" >> "${LOG_FILE}"
./scripts/orders_sync_run_profiler.sh >> "${LOG_FILE}" 2>&1

echo "--- Script 1 completed successfully ---" >> "${LOG_FILE}"

echo "--- Running Script 2: daily_sales_report ---" >> "${LOG_FILE}"
./scripts/run_local_reports_daily_sales.sh >> "${LOG_FILE}" 2>&1

echo "--- Script 2 completed successfully ---" >> "${LOG_FILE}"
echo "=== CRON RUN FINISHED @ $(date) ===" >> "${LOG_FILE}"