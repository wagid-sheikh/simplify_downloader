#!/usr/bin/env bash
set -euo pipefail

CONTINUE_ON_ERROR=false
EXTRA_ARGS=()

for arg in "$@"; do
  if [[ "$arg" == "--continue-on-error" ]]; then
    CONTINUE_ON_ERROR=true
  else
    EXTRA_ARGS+=("$arg")
  fi
done

run_step() {
  local label=$1
  shift
  echo "--- Running report: ${label} ---"
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
  poetry run python -m app.reports.daily_sales_report.main "${EXTRA_ARGS[@]}"

echo "--- Dependency order: reports.pending_deliveries runs immediately after reports.daily_sales_report ---"

run_step "pending_deliveries" \
  poetry run python -m app.reports.pending_deliveries.main "${EXTRA_ARGS[@]}"
