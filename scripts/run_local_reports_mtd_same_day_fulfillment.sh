#!/usr/bin/env bash
set -euo pipefail

# Usage examples:
#   # Local/manual run with default force mode.
#   ./scripts/run_local_reports_mtd_same_day_fulfillment.sh --report-date 2026-03-31
#
#   # Run with force explicitly disabled.
#   REPORT_FORCE=false ./scripts/run_local_reports_mtd_same_day_fulfillment.sh --report-date 2026-03-31
#
# REPORT_FORCE semantics:
#   unset/true/TRUE/True/1 -> append --force
#   false/FALSE/False/0 -> do not append --force

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

report_force="${REPORT_FORCE:-true}"
FORCE_ARGS=()
force_mode="false"
case "${report_force}" in
  true|TRUE|True|1)
  force_mode="true"
  FORCE_ARGS+=("--force")
  ;;
esac

report_date="<default>"
for ((i = 1; i <= $#; i++)); do
  if [[ "${!i}" == "--report-date" ]] && ((i + 1 <= $#)); then
    next_index=$((i + 1))
    report_date="${!next_index}"
    break
  fi
done

echo "[run_local_reports_mtd_same_day_fulfillment] pipeline=mtd-same-day-fulfillment report_date=${report_date} force=${force_mode}"

exec poetry run python -m app report mtd-same-day-fulfillment --env prod ${FORCE_ARGS[@]+"${FORCE_ARGS[@]}"} "$@"
