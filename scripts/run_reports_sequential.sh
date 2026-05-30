#!/usr/bin/env bash
set -euo pipefail

# Usage examples:
#   # Local/manual sequential run; reports always regenerate.
#   ./scripts/run_reports_sequential.sh --report-date 2026-03-31
#
# Regeneration is mandatory for sequential report runs; --force is always passed
# to each report pipeline.

CONTINUE_ON_ERROR=false
EXTRA_ARGS=()
FORCE_ARGS=()
explicit_force="false"

for arg in "$@"; do
  if [[ "$arg" == "--continue-on-error" ]]; then
    CONTINUE_ON_ERROR=true
  else
    if [[ "$arg" == "--force" ]]; then
      explicit_force="true"
    fi
    EXTRA_ARGS+=("$arg")
  fi
done

if [[ "${explicit_force}" != "true" ]]; then
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

run_step() {
  local label=$1
  shift
  echo "--- Running report: ${label} report_date=${report_date} regenerate=true ---"
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
  poetry run python -m app report daily-sales ${FORCE_ARGS[@]+"${FORCE_ARGS[@]}"} ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}

echo "--- Dependency order: reports.pending_deliveries runs immediately after reports.daily_sales_report ---"

run_step "pending_deliveries" \
  poetry run python -m app report pending-deliveries ${FORCE_ARGS[@]+"${FORCE_ARGS[@]}"} ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}

run_step "mtd_same_day_fulfillment" \
  poetry run python -m app report mtd-same-day-fulfillment ${FORCE_ARGS[@]+"${FORCE_ARGS[@]}"} ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}
