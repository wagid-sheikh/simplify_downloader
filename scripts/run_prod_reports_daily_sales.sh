#!/usr/bin/env bash
set -euo pipefail

# Usage examples:
#   # Production-targeted run; reports always regenerate.
#   ./scripts/run_prod_reports_daily_sales.sh --report-date 2026-03-31
#
# Regeneration is mandatory for report wrappers. The report CLI always
# regenerates and appends new summaries/documents; --force is not required.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

report_date="<default>"

for ((i = 1; i <= $#; i++)); do
  if [[ "${!i}" == "--report-date" ]] && ((i + 1 <= $#)); then
    next_index=$((i + 1))
    report_date="${!next_index}"
    break
  fi
done

echo "[run_prod_reports_daily_sales] pipeline=daily-sales report_date=${report_date} regenerate=true"

exec poetry run python -m app report daily-sales --env prod "$@"
