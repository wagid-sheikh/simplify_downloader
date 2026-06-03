#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Export PostgreSQL DB configured by .env into a gzipped SQL dump.
#
# - Reads POSTGRES_* from the project .env at repo root.
# - Local output goes into ./db_dumps/
# - Does not copy the dump to any remote server.
# ============================================================

# Resolve project root as "one level above scripts/"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Load .env if present (local dev)
if [[ -f "$PROJECT_ROOT/.env" ]]; then
  echo "[db_export_local] Loading env from $PROJECT_ROOT/.env"
  set -a
  # shellcheck source=/dev/null
  source "$PROJECT_ROOT/.env"
  set +a
else
  echo "[db_export_local] WARNING: .env not found at $PROJECT_ROOT/.env — expecting POSTGRES_* in shell env."
fi

# Ensure required env vars exist
: "${POSTGRES_HOST:?POSTGRES_HOST is required}"
: "${POSTGRES_PORT:?POSTGRES_PORT is required}"
: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"

DUMP_DIR="$PROJECT_ROOT/db_dumps"
mkdir -p "$DUMP_DIR"

TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
DUMP_FILE="${DUMP_DIR}/${POSTGRES_DB}_local_${TIMESTAMP}.sql.gz"

echo "[db_export_local] Dumping DB '$POSTGRES_DB' from $POSTGRES_HOST:$POSTGRES_PORT as $POSTGRES_USER"
echo "[db_export_local] Local output: $DUMP_FILE"

export PGPASSWORD="$POSTGRES_PASSWORD"

pg_dump \
  -h "$POSTGRES_HOST" \
  -p "$POSTGRES_PORT" \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  --clean \
  --if-exists \
  --no-owner \
  --no-privileges \
  | gzip > "$DUMP_FILE"

unset PGPASSWORD

echo "[db_export_local] Local dump created."
echo "[db_export_local] Done."

# ---------- COPY TO SERVER ----------
# Remote-copy behavior intentionally disabled. This script now only exports the
# database configured by POSTGRES_* values into the local ./db_dumps directory.
#
# echo "[db_export_local] Ensuring remote dump dir exists: ${SERVER_DUMP_DIR}"
# ssh "$SERVER_SSH_HOST" "mkdir -p '$SERVER_DUMP_DIR'"
#
# echo "[db_export_local] Copying dump to server..."
# scp "$DUMP_FILE" "${SERVER_SSH_HOST}:${SERVER_DUMP_DIR}/"
#
# echo "[db_export_local] Remote file should now be at:"
# echo "  ${SERVER_SSH_HOST}:${SERVER_DUMP_DIR}/$(basename "$DUMP_FILE")"
