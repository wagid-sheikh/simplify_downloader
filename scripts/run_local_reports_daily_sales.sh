#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

FORCE_FLAG=""
EXTRA_ARGS=()
for arg in "$@"; do
  if [[ "${arg}" == "--force" ]]; then
    FORCE_FLAG="--force"
  else
    EXTRA_ARGS+=("${arg}")
  fi
done

exec poetry run python -m app.reports.daily_sales_report.main ${FORCE_FLAG} "${EXTRA_ARGS[@]}"
