#!/usr/bin/env bash
set -euo pipefail

# Lightweight orders-sync network preflight.
# Verifies DNS resolution and TCP/443 reachability before Playwright starts so
# infrastructure failures are short and obvious in cron/operator logs.

TIMEOUT_SECONDS="${ORDERS_SYNC_CONNECTIVITY_PREFLIGHT_TIMEOUT_SECONDS:-5}"
TARGET_PORT="${ORDERS_SYNC_CONNECTIVITY_PREFLIGHT_PORT:-443}"

if ! [[ "${TIMEOUT_SECONDS}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  TIMEOUT_SECONDS=5
fi
if ! [[ "${TARGET_PORT}" =~ ^[0-9]+$ ]]; then
  TARGET_PORT=443
fi

log_preflight() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$*"
}

check_target() {
  local target_host="$1"
  local local_hostname
  local timestamp

  local_hostname="$(hostname 2>/dev/null || echo unknown)"
  timestamp="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

  PREFLIGHT_TARGET_HOST="${target_host}" \
  PREFLIGHT_TARGET_PORT="${TARGET_PORT}" \
  PREFLIGHT_TIMEOUT_SECONDS="${TIMEOUT_SECONDS}" \
  PREFLIGHT_LOCAL_HOSTNAME="${local_hostname}" \
  PREFLIGHT_TIMESTAMP="${timestamp}" \
  python3 <<'PY'
import os
import socket
import sys

target_host = os.environ["PREFLIGHT_TARGET_HOST"]
target_port = int(os.environ["PREFLIGHT_TARGET_PORT"])
timeout_seconds = float(os.environ["PREFLIGHT_TIMEOUT_SECONDS"])
local_hostname = os.environ["PREFLIGHT_LOCAL_HOSTNAME"]
timestamp = os.environ["PREFLIGHT_TIMESTAMP"]

try:
    addresses = socket.getaddrinfo(
        target_host,
        target_port,
        type=socket.SOCK_STREAM,
    )
except socket.gaierror as exc:
    print(
        "connectivity_preflight_failed "
        f"hostname={local_hostname} "
        f"target_host={target_host} "
        f"target_port={target_port} "
        f"timestamp={timestamp} "
        "failure_stage=dns "
        f"error={exc}",
        file=sys.stderr,
    )
    raise SystemExit(1)

last_error = ""
for family, socktype, proto, _canonname, sockaddr in addresses:
    try:
        with socket.socket(family, socktype, proto) as sock:
            sock.settimeout(timeout_seconds)
            sock.connect(sockaddr)
            print(
                "connectivity_preflight_ok "
                f"hostname={local_hostname} "
                f"target_host={target_host} "
                f"target_port={target_port} "
                f"timestamp={timestamp}"
            )
            raise SystemExit(0)
    except OSError as exc:
        last_error = str(exc)

print(
    "connectivity_preflight_failed "
    f"hostname={local_hostname} "
    f"target_host={target_host} "
    f"target_port={target_port} "
    f"timestamp={timestamp} "
    "failure_stage=tcp_connect "
    f"error={last_error or 'no_addresses_attempted'}",
    file=sys.stderr,
)
raise SystemExit(1)
PY
}

main() {
  local rc=0

  if [[ "$#" -eq 0 ]]; then
    set -- \
      subs.quickdrycleaning.com \
      store.ucleanlaundry.com \
      storepanel.ucleanlaundry.com
  fi

  log_preflight "connectivity_preflight_started target_port=${TARGET_PORT} timeout_seconds=${TIMEOUT_SECONDS}"

  while [[ "$#" -gt 0 ]]; do
    if ! check_target "$1"; then
      rc=1
    fi
    shift
  done

  if [[ "${rc}" -ne 0 ]]; then
    log_preflight "connectivity_preflight_failed_summary exit_code=${rc}"
    return "${rc}"
  fi

  log_preflight "connectivity_preflight_succeeded"
  return 0
}

main "$@"
