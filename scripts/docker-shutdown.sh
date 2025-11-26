#!/usr/bin/env bash
set -euo pipefail

PROJECT_CONTAINERS=("tsv-crm-backend-app" "tsv-crm-db")
LOG_ROOT="/var/lib/docker/containers"

echo "==> Stopping project containers..."
for name in "${PROJECT_CONTAINERS[@]}"; do
  cid="$(docker ps --format '{{.ID}} {{.Names}}' | awk '$2=="'"$name"'" {print $1}')"

  if [ -n "${cid:-}" ]; then
    echo "    Stopping $name ($cid)..."
    docker stop "$cid"
  else
    echo "    $name is not running (ok)."
  fi
done

echo
echo "==> Truncating logs for project containers..."
for name in "${PROJECT_CONTAINERS[@]}"; do
  # Look for the container ID even if it's stopped
  cid="$(docker ps -a --format '{{.ID}} {{.Names}}' | awk '$2=="'"$name"'" {print $1}')"

  if [ -z "${cid:-}" ]; then
    echo "    No container found for $name (skipping log truncation)."
    continue
  fi

  log_file="$LOG_ROOT/$cid/${cid}-json.log"

  if [ -f "$log_file" ]; then
    echo "    Truncating log for $name: $log_file"
    sudo truncate -s 0 "$log_file"
  else
    echo "    Log file not found for $name at $log_file (nothing to truncate)."
  fi
done

echo
echo "==> Done."
echo "You can now manually start containers, for example:"
echo "  docker start tsv-crm-db tsv-crm-backend-app"
echo "  # or: docker compose up -d"


# docker logs --tail 20 tsv-crm-backend-app
# docker logs --tail 20 tsv-crm-db