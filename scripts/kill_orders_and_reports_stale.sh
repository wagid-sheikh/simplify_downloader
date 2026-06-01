#!/usr/bin/env bash

# Backwards-compatible rollout wrapper. New operator workflows should call
# inspect_or_kill_pipeline_stale.sh with an explicit pipeline name.
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$SCRIPT_DIR/inspect_or_kill_pipeline_stale.sh" orders-reports "$@"
