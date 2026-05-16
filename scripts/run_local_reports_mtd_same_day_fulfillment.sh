#!/usr/bin/env bash
set -euo pipefail

# Usage examples:
#   # Local/manual run; reports always regenerate.
#   ./scripts/run_local_reports_mtd_same_day_fulfillment.sh --report-date 2026-03-31
#
# Regeneration is mandatory for report wrappers; --force is appended unless the
# caller already supplied it.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

FORCE_ARGS=()
explicit_force="false"
report_date="<default>"

for ((i = 1; i <= $#; i++)); do
  if [[ "${!i}" == "--force" ]]; then
    explicit_force="true"
  fi
  if [[ "${!i}" == "--report-date" ]] && ((i + 1 <= $#)); then
    next_index=$((i + 1))
    report_date="${!next_index}"
  fi
done

if [[ "${explicit_force}" != "true" ]]; then
  FORCE_ARGS+=("--force")
fi

echo "[run_local_reports_mtd_same_day_fulfillment] pipeline=mtd-same-day-fulfillment report_date=${report_date} regenerate=true"

exec poetry run python -m app report mtd-same-day-fulfillment --env prod ${FORCE_ARGS[@]+"${FORCE_ARGS[@]}"} "$@"
