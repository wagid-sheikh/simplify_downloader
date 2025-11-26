#!/usr/bin/env bash
set -euo pipefail

PROJECT_CONTAINERS=("tsv-crm-backend-app" "tsv-crm-db")

echo "==> Truncating logs for project containers (without stopping)..."
for name in "${PROJECT_CONTAINERS[@]}"; do
  log_path="$(docker inspect --format '{{.LogPath}}' "$name" 2>/dev/null || true)"

  if [ -z "$log_path" ]; then
    echo "    No LogPath found for $name. Skipping."
    continue
  fi

  if [ -f "$log_path" ]; then
    echo "    Truncating log for $name: $log_path"
    sudo truncate -s 0 "$log_path"
  else
    echo "    Log file $log_path does not exist (nothing to truncate)."
  fi
done

echo "==> Done."
