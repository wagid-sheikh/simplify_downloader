#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Import a SQL dump into the Docker Postgres container.
#
# - Assumes DB is running in container "tsv-crm-db".
# - Reads POSTGRES_DB and POSTGRES_USER from container env.
# - If no argument is given:
#     - Picks the latest db_dumps/*_local_*.sql.gz
#     - Unzips it to .sql
#     - Imports that .sql file.
# - If an argument is given:
#     - Uses that path (can be .sql or .sql.gz).
#
# WARNING: This will overwrite existing data in the target DB.
# ============================================================

CONTAINER_NAME="tsv-crm-db"

# Resolve project root as "one level above scripts/"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUMP_DIR="${PROJECT_ROOT}/db_dumps"

choose_default_dump() {
  # Latest *_local_*.sql.gz in db_dumps
  local latest
  latest="$(ls -1t "${DUMP_DIR}"/*_local_*.sql.gz 2>/dev/null | head -n 1 || true)"

  if [[ -z "$latest" ]]; then
    echo "[db_import_on_server] ERROR: No *_local_*.sql.gz dump found in ${DUMP_DIR}"
    return 1
  fi

  echo "$latest"
}

# -------- Determine dump file --------
if [[ $# -ge 1 ]]; then
  # Use provided path
  RAW_DUMP_PATH="$1"
  if [[ ! -f "$RAW_DUMP_PATH" ]]; then
    echo "[db_import_on_server] ERROR: Dump file not found: $RAW_DUMP_PATH"
    exit 1
  fi
  DUMP_PATH="$RAW_DUMP_PATH"
else
  echo "[db_import_on_server] No file argument given, selecting latest *_local_*.sql.gz in ${DUMP_DIR}"
  DUMP_PATH="$(choose_default_dump)" || exit 1
fi

echo "[db_import_on_server] Selected dump: $DUMP_PATH"

# Ensure container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}\$"; then
  echo "[db_import_on_server] ERROR: Container ${CONTAINER_NAME} is not running."
  exit 1
fi

# Read DB name and user from container env
DB_NAME="$(docker exec "${CONTAINER_NAME}" printenv POSTGRES_DB)"
DB_USER="$(docker exec "${CONTAINER_NAME}" printenv POSTGRES_USER)"

if [[ -z "$DB_NAME" || -z "$DB_USER" ]]; then
  echo "[db_import_on_server] ERROR: Could not read POSTGRES_DB/POSTGRES_USER from container env."
  exit 1
fi

echo "[db_import_on_server] Target DB: $DB_NAME"
echo "[db_import_on_server] DB User:   $DB_USER"
echo "[db_import_on_server] Container: $CONTAINER_NAME"

# Final confirmation
read -r -p "[db_import_on_server] This will overwrite data in DB '$DB_NAME'. Continue? [y/N] " CONFIRM
if [[ "$CONFIRM" != "y" && "$CONFIRM" != "Y" ]]; then
  echo "[db_import_on_server] Aborted."
  exit 0
fi

# -------- Prepare SQL file (unzip if needed) --------
SQL_FILE=""

if [[ "$DUMP_PATH" == *.sql.gz ]]; then
  SQL_FILE="${DUMP_PATH%.gz}"
  echo "[db_import_on_server] Unzipping $DUMP_PATH -> $SQL_FILE (keeping original .gz)..."
  gunzip -kf "$DUMP_PATH"
elif [[ "$DUMP_PATH" == *.sql ]]; then
  SQL_FILE="$DUMP_PATH"
else
  echo "[db_import_on_server] ERROR: Dump file must be .sql or .sql.gz: $DUMP_PATH"
  exit 1
fi

if [[ ! -f "$SQL_FILE" ]]; then
  echo "[db_import_on_server] ERROR: Expected SQL file not found after unzip: $SQL_FILE"
  exit 1
fi

echo "[db_import_on_server] Using SQL file: $SQL_FILE"

echo "[db_import_on_server] Dropping and recreating schema 'public' in $DB_NAME..."

docker exec -i "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" <<'EOF'
DO
$$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'public') THEN
    EXECUTE 'DROP SCHEMA public CASCADE';
  END IF;
  EXECUTE 'CREATE SCHEMA public AUTHORIZATION current_user';
END;
$$;
EOF

echo "[db_import_on_server] Importing dump into $DB_NAME..."

docker exec -i "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" < "$SQL_FILE"

echo "[db_import_on_server] Import completed successfully."
