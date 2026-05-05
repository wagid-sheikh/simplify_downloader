#!/usr/bin/env bash
set -euo pipefail

# Usage examples:
#   # Run for the current month-to-date window using app defaults.
#   ./scripts/run_local_reports_mtd_same_day_fulfillment.sh
#
#   # Run for March 31, 2026; the report computes MTD internally
#   # from 2026-03-01 through 2026-03-31.
#   ./scripts/run_local_reports_mtd_same_day_fulfillment.sh --end-date 2026-03-31

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

exec poetry run python -m app report mtd-same-day-fulfillment --env prod --force "$@"
