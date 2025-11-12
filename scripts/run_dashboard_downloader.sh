#!/usr/bin/env bash
set -euo pipefail

# Resolve repository root even when the script is invoked via symlink.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_ROOT}"

exec poetry run python -m dashboard_downloader.run_downloads "$@"
