#!/usr/bin/env bash
# Run the orders sync profiler CLI.
#
# By default, a persisted profiler overall_status="failed" is non-breaking for the
# shell exit status so summaries and notifications remain operator-visible without
# blocking downstream report scripts. Set ORDERS_SYNC_PROFILER_FAIL_ON_FAILED_STATUS=1
# to make the profiler exit non-zero after the summary and notifications are persisted
# when overall_status="failed".
# UC_ONLY=1 exec poetry run python -m app.crm_downloader.orders_sync_run_profiler.main "$@"
set -euo pipefail
export ORDERS_SYNC_PROFILER_DB_POOL_SIZE="${ORDERS_SYNC_PROFILER_DB_POOL_SIZE:-8}"
export ORDERS_SYNC_PROFILER_DB_MAX_OVERFLOW="${ORDERS_SYNC_PROFILER_DB_MAX_OVERFLOW:-4}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ "${ORDERS_SYNC_SKIP_CONNECTIVITY_PREFLIGHT:-0}" != "1" ]]; then
  "${SCRIPT_DIR}/orders_sync_connectivity_preflight.sh"
fi

exec poetry run python -m app.crm_downloader.orders_sync_run_profiler.main "$@"
