#!/usr/bin/env bash
set -euo pipefail

# Usage examples:
#   # Local/manual run with default regeneration mode.
#   ./scripts/run_local_reports_daily_sales.sh --report-date 2026-03-31
#
#   # Explicit regeneration for cron/manual callers.
#   ./scripts/run_local_reports_daily_sales.sh --force --report-date 2026-03-31
#
# REPORT_FORCE semantics for manual compatibility:
#   unset/true/TRUE/True/1 -> append --force unless already provided
#   false/FALSE/False/0 -> do not append --force; explicit --force is still honored

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

report_force="${REPORT_FORCE:-true}"
FORCE_ARGS=()
regenerate="false"
explicit_force="false"

report_date="<default>"
for ((i = 1; i <= $#; i++)); do
  if [[ "${!i}" == "--force" ]]; then
    explicit_force="true"
    regenerate="true"
  fi
  if [[ "${!i}" == "--report-date" ]] && ((i + 1 <= $#)); then
    next_index=$((i + 1))
    report_date="${!next_index}"
  fi
done

case "${report_force}" in
  true|TRUE|True|1)
  regenerate="true"
  if [[ "${explicit_force}" != "true" ]]; then
    FORCE_ARGS+=("--force")
  fi
  ;;
esac

echo "[run_local_reports_daily_sales] pipeline=daily-sales report_date=${report_date} regenerate=${regenerate}"

exec poetry run python -m app report daily-sales --env prod ${FORCE_ARGS[@]+"${FORCE_ARGS[@]}"} "$@"
