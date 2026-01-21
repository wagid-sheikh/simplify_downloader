#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Cron-safe environment basics
# -----------------------------
# Cron provides a very small PATH; set a sane baseline.
export PATH="/Users/${USER}/.local/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"

# If HOME isn't set (can happen in cron), set it explicitly.
: "${HOME:=/Users/${USER}}"

export HOME

# -----------------------------
# Resolve repo paths (absolute)
# -----------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Optional: ensure we're in repo root
cd "${REPO_ROOT}"

# -----------------------------
# Load .env for shared settings
# -----------------------------
ENV_FILE="${REPO_ROOT}/.env"
if [[ -f "${ENV_FILE}" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${ENV_FILE}"
  set +a
fi

# -----------------------------
# Locate Poetry (absolute path)
# -----------------------------
# Prefer: POETRY_BIN in .env or environment. Fallback to common locations.
POETRY_BIN="${POETRY_BIN:-}"

if [[ -z "${POETRY_BIN}" ]]; then
  if command -v poetry >/dev/null 2>&1; then
    POETRY_BIN="$(command -v poetry)"
  elif [[ -x "${HOME}/.local/bin/poetry" ]]; then
    POETRY_BIN="${HOME}/.local/bin/poetry"
  elif [[ -x "/opt/homebrew/bin/poetry" ]]; then
    POETRY_BIN="/opt/homebrew/bin/poetry"
  elif [[ -x "/usr/local/bin/poetry" ]]; then
    POETRY_BIN="/usr/local/bin/poetry"
  else
    echo "[dashboard] ERROR: poetry not found. Set POETRY_BIN in ${ENV_FILE} or cron env."
    exit 1
  fi
fi

# -----------------------------
# (Optional) Ensure migrations
# -----------------------------
echo "[dashboard] Ensuring database migrations are up to date..."
# "${POETRY_BIN}" run alembic upgrade head

# -----------------------------
# Run pipeline module command
# -----------------------------
exec "${POETRY_BIN}" run python -m app run-single-session "$@"

