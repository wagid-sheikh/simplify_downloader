#!/usr/bin/env bash
set -euo pipefail

PROJECT_CONTAINERS=("tsv-crm-backend-app" "tsv-crm-db")

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
  # Ask Docker for the actual log file path
  log_path="$(docker inspect --format '{{.LogPath}}' "$name" 2>/dev/null || true)"

  if [ -z "$log_path" ]; then
    echo "    No LogPath found for $name (maybe container never created?). Skipping."
    continue
  fi

  if [ -f "$log_path" ]; then
    echo "    Truncating log for $name: $log_path"
    sudo truncate -s 0 "$log_path"
  else
    echo "    Log file $log_path does not exist (nothing to truncate)."
  fi
done

echo
echo "==> Done."
echo "You can now manually start containers, for example:"
echo "  docker start tsv-crm-db tsv-crm-backend-app"
echo "  # or: docker compose up -d"



# docker logs --tail 20 tsv-crm-backend-app
# docker logs --tail 20 tsv-crm-db