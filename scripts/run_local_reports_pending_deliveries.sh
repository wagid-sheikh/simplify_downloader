#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

# The wrapper accepts report-only upstream freshness flags from cron, but the
# recovery pre-step has its own narrower CLI and must not receive them.
recovery_args=()
skip_next=0
for arg in "$@"; do
  if [[ "${skip_next}" -eq 1 ]]; then
    skip_next=0
    continue
  fi

  case "${arg}" in
    --orders-sync-upstream-status|--orders-sync-upstream-run-id)
      skip_next=1
      ;;
    --orders-sync-upstream-status=*|--orders-sync-upstream-run-id=*)
      ;;
    *)
      recovery_args+=("${arg}")
      ;;
  esac
done

echo "[run_local_reports_pending_deliveries] phase=recovery.mark-aged-pending-deliveries"
poetry run python -m app recovery mark-aged-pending-deliveries --env prod "${recovery_args[@]}"

echo "[run_local_reports_pending_deliveries] pipeline=pending-deliveries mode=read-only"
exec poetry run python -m app report pending-deliveries --env prod "$@"
