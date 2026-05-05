#!/usr/bin/env bash
set -euo pipefail

# Usage examples:
#   # Local/manual run in default force mode.
#   ./scripts/run_reports_sequential.sh --report-date 2026-03-31
#
#   # Run with force explicitly disabled.
#   REPORT_FORCE=false ./scripts/run_reports_sequential.sh --report-date 2026-03-31
#
# REPORT_FORCE semantics:
#   unset/true/TRUE/True/1 -> append --force
#   false/FALSE/False/0 -> do not append --force

CONTINUE_ON_ERROR=false
EXTRA_ARGS=()
FORCE_ARGS=()
REPORT_FORCE_MODE="false"
report_force="${REPORT_FORCE:-true}"

case "${report_force}" in
  true|TRUE|True|1)
  FORCE_ARGS+=("--force")
  REPORT_FORCE_MODE="true"
  ;;
esac

for arg in "$@"; do
  if [[ "$arg" == "--continue-on-error" ]]; then
    CONTINUE_ON_ERROR=true
  else
    EXTRA_ARGS+=("$arg")
  fi
done

report_date="<default>"
for ((i = 1; i <= $#; i++)); do
  if [[ "${!i}" == "--report-date" ]] && ((i + 1 <= $#)); then
    next_index=$((i + 1))
    report_date="${!next_index}"
    break
  fi
done

run_step() {
  local label=$1
  shift
  echo "--- Running report: ${label} report_date=${report_date} force=${REPORT_FORCE_MODE} ---"
  if "$@"; then
    echo "--- ${label} completed successfully ---"
  elif $CONTINUE_ON_ERROR; then
    echo "--- ${label} failed; continuing due to --continue-on-error ---" >&2
  else
    echo "--- ${label} failed; exiting ---" >&2
    exit 1
  fi
}

echo "--- Dependency order: reports.daily_sales_report must run before reports.pending_deliveries ---"

run_step "daily_sales_report" \
  poetry run python -m app report daily-sales ${FORCE_ARGS[@]+"${FORCE_ARGS[@]}"} "${EXTRA_ARGS[@]}"

echo "--- Dependency order: reports.pending_deliveries runs immediately after reports.daily_sales_report ---"

run_step "pending_deliveries" \
  poetry run python -m app report pending-deliveries ${FORCE_ARGS[@]+"${FORCE_ARGS[@]}"} "${EXTRA_ARGS[@]}"

run_step "mtd_same_day_fulfillment" \
  poetry run python -m app report mtd-same-day-fulfillment ${FORCE_ARGS[@]+"${FORCE_ARGS[@]}"} "${EXTRA_ARGS[@]}"
