#!/usr/bin/env bash
set -euo pipefail

# Usage examples:
#   # Local/manual run with default non-force mode.
#   ./scripts/run_local_reports_daily_sales.sh --report-date 2026-03-31
#
#   # Cron-style run that forces regeneration.
#   REPORT_FORCE=true ./scripts/run_local_reports_daily_sales.sh --report-date 2026-03-31
#
# REPORT_FORCE semantics:
#   true  -> append --force
#   false/unset -> do not append --force

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

force_mode="false"
report_force="${REPORT_FORCE:-false}"
FORCE_ARGS=()
if [[ "${report_force}" =~ ^([Tt][Rr][Uu][Ee])$ ]]; then
  force_mode="true"
  FORCE_ARGS+=("--force")
fi

report_date="<default>"
for ((i = 1; i <= $#; i++)); do
  if [[ "${!i}" == "--report-date" ]] && ((i + 1 <= $#)); then
    next_index=$((i + 1))
    report_date="${!next_index}"
    break
  fi
done

echo "[run_local_reports_daily_sales] pipeline=daily-sales report_date=${report_date} force=${force_mode}"

exec poetry run python -m app report daily-sales --env prod ${FORCE_ARGS[@]+"${FORCE_ARGS[@]}"} "$@"
