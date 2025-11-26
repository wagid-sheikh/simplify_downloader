#!/usr/bin/env bash
set -euo pipefail

# ============================================================
#   TSV-CRM — Color-coded Smart Log Viewer
#
#   Usage:
#     ./scripts/check_logs.sh                # show both containers (20 lines)
#     ./scripts/check_logs.sh 50             # show both (50 lines)
#     ./scripts/check_logs.sh app            # show only backend logs
#     ./scripts/check_logs.sh db             # show only DB logs
#     ./scripts/check_logs.sh -f             # follow both logs (20 lines)
#     ./scripts/check_logs.sh -f app 50      # follow backend logs, 50 lines
# ============================================================

APP_CONTAINER="tsv-crm-backend-app"
DB_CONTAINER="tsv-crm-db"

# ---------- Colors ----------
RED="\033[31m"
GREEN="\033[32m"
YELLOW="\033[33m"
BLUE="\033[34m"
CYAN="\033[36m"
RESET="\033[0m"
# ----------------------------

FOLLOW_MODE="no"
TAIL_LINES=20
TARGET="both"

# ---------- Parse Arguments ----------
for arg in "$@"; do
    case "$arg" in
        -f|--follow)
            FOLLOW_MODE="yes"
            ;;
        app|APP)
            TARGET="app"
            ;;
        db|DB)
            TARGET="db"
            ;;
        ''|*[!0-9]*)
            # ignore non-numeric unless it's app/db/-f
            ;;
        *)
            TAIL_LINES="$arg"
            ;;
    esac
done
# --------------------------------------

# ---------- Helper: check if container exists ----------
container_running() {
    docker ps --format '{{.Names}}' | grep -q "^${1}\$"
}
# -------------------------------------------------------

# ---------- Helper: show logs ----------
show_logs() {
    local cname="$1"
    local label="$2"
    local color="$3"

    echo -e "${color}============================================${RESET}"
    echo -e "${color}${label} — last ${TAIL_LINES} lines${RESET}"
    echo -e "${color}============================================${RESET}"

    if container_running "$cname"; then
        if [[ "$FOLLOW_MODE" == "yes" ]]; then
            docker logs -f --tail "${TAIL_LINES}" "$cname"
        else
            docker logs --tail "${TAIL_LINES}" "$cname"
        fi
    else
        echo -e "${RED}⚠️  Container '${cname}' is not running.${RESET}"
    fi

    echo
}
# -------------------------------------------------------

# ---------- Execute ----------
case "$TARGET" in
    app)
        show_logs "$APP_CONTAINER" "📘 Backend Logs" "$BLUE"
        ;;
    db)
        show_logs "$DB_CONTAINER" "🟦 Postgres Logs" "$CYAN"
        ;;
    both)
        show_logs "$APP_CONTAINER" "📘 Backend Logs" "$BLUE"
        show_logs "$DB_CONTAINER" "🟦 Postgres Logs" "$CYAN"
        ;;
esac

echo -e "${GREEN}✔️ Done.${RESET}"
