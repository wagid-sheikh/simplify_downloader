#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
poetry run python -m app crm rebuild-order-line-items "$@"
