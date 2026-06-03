#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Import a local SQL dump into the PostgreSQL DB configured by .env.
#
# - Reads POSTGRES_* from the project .env at repo root.
# - Looks for the dump file in ./db_dumps/ by filename.
# - Supports .sql and .sql.gz dump files.
# - Requires confirmation before import.
#
# WARNING: Imports can overwrite data. Dumps produced by
# scripts/db_export_local.sh include --clean statements that drop
# existing objects before recreating them.
# ============================================================

usage() {
  echo "Usage: $0 <dump-file-name.sql[.gz]>"
  echo "Example: $0 mydb_local_20260603_120000.sql.gz"
}

if [[ $# -ne 1 ]]; then
  usage
  exit 1
fi

DUMP_ARG="$1"

if [[ "$DUMP_ARG" == */* ]]; then
  echo "[db_import_local] ERROR: Pass only a filename from ./db_dumps, not a path: $DUMP_ARG"
  exit 1
fi

# Resolve project root as "one level above scripts/"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUMP_DIR="$PROJECT_ROOT/db_dumps"
DUMP_PATH="$DUMP_DIR/$DUMP_ARG"

# Load .env if present (local dev)
if [[ -f "$PROJECT_ROOT/.env" ]]; then
  echo "[db_import_local] Loading env from $PROJECT_ROOT/.env"
  set -a
  # shellcheck source=/dev/null
  source "$PROJECT_ROOT/.env"
  set +a
else
  echo "[db_import_local] WARNING: .env not found at $PROJECT_ROOT/.env — expecting POSTGRES_* in shell env."
fi

# Ensure required env vars exist
: "${POSTGRES_HOST:?POSTGRES_HOST is required}"
: "${POSTGRES_PORT:?POSTGRES_PORT is required}"
: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"

if [[ ! -f "$DUMP_PATH" ]]; then
  echo "[db_import_local] ERROR: Dump file not found: $DUMP_PATH"
  exit 1
fi

case "$DUMP_PATH" in
  *.sql|*.sql.gz)
    ;;
  *)
    echo "[db_import_local] ERROR: Dump file must end with .sql or .sql.gz: $DUMP_PATH"
    exit 1
    ;;
esac

echo "[db_import_local] Dump file: $DUMP_PATH"
echo "[db_import_local] Target DB:  $POSTGRES_DB"
echo "[db_import_local] Target host: $POSTGRES_HOST:$POSTGRES_PORT"
echo "[db_import_local] DB user:     $POSTGRES_USER"
echo
read -r -p "[db_import_local] This will import into '$POSTGRES_DB' and may overwrite data. Type IMPORT to continue: " CONFIRM
if [[ "$CONFIRM" != "IMPORT" ]]; then
  echo "[db_import_local] Aborted."
  exit 0
fi

export PGPASSWORD="$POSTGRES_PASSWORD"
trap 'unset PGPASSWORD' EXIT

echo "[db_import_local] Importing dump..."

if [[ "$DUMP_PATH" == *.sql.gz ]]; then
  gzip -dc "$DUMP_PATH" | psql \
    -h "$POSTGRES_HOST" \
    -p "$POSTGRES_PORT" \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -v ON_ERROR_STOP=1
else
  psql \
    -h "$POSTGRES_HOST" \
    -p "$POSTGRES_PORT" \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -v ON_ERROR_STOP=1 \
    -f "$DUMP_PATH"
fi

echo "[db_import_local] Import completed successfully."
