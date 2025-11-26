#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="tsv-crm-backend-app"

state=$(docker inspect -f '{{.State.Running}}' "${CONTAINER_NAME}" 2>/dev/null || true)
if [[ "${state}" != "true" ]]; then
  echo "Error: container ${CONTAINER_NAME} is not running" >&2
  exit 1
fi

docker exec -t "${CONTAINER_NAME}" python -m app pipeline
