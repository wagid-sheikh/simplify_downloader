#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

echo "[run_local_reports_pending_deliveries] pipeline=pending-deliveries mode=read-only"
exec poetry run python -m app report pending-deliveries --env prod "$@"
