#!/usr/bin/env bash

# Backwards-compatible rollout wrapper. New operator workflows should call
# inspect_or_kill_pipeline_stale.sh with an explicit pipeline name.
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cat <<'EOF'
Legacy helper: inspecting orders-reports only.
For TD leads, run: ./scripts/inspect_or_kill_pipeline_stale.sh td-leads
For explicit orders/reports inspection, run: ./scripts/inspect_or_kill_pipeline_stale.sh orders-reports
EOF
exec "$SCRIPT_DIR/inspect_or_kill_pipeline_stale.sh" orders-reports "$@"
