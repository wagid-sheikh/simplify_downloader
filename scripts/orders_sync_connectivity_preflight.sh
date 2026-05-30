#!/usr/bin/env bash
set -euo pipefail

# Lightweight orders-sync TCP/app-layer preflight.
# Verifies DNS resolution and TCP/443 reachability before Playwright starts so
# infrastructure failures are short and obvious in cron/operator logs. Optional
# HTTP checks prove the remote app layer responded, but are not a full readiness
# or authenticated workflow check.

TIMEOUT_SECONDS="${ORDERS_SYNC_CONNECTIVITY_PREFLIGHT_TIMEOUT_SECONDS:-5}"
TARGET_PORT="${ORDERS_SYNC_CONNECTIVITY_PREFLIGHT_PORT:-443}"
HTTP_ENABLED="${ORDERS_SYNC_APP_LAYER_PREFLIGHT:-1}"
HTTP_METHOD="${ORDERS_SYNC_APP_LAYER_PREFLIGHT_METHOD:-HEAD}"
HTTP_EXPECTED_CLASSES="${ORDERS_SYNC_APP_LAYER_PREFLIGHT_EXPECTED_CLASSES:-2xx,3xx,4xx}"
HTTP_SCHEME="${ORDERS_SYNC_APP_LAYER_PREFLIGHT_SCHEME:-https}"
HTTP_PATH="${ORDERS_SYNC_APP_LAYER_PREFLIGHT_PATH:-/}"

if ! [[ "${TIMEOUT_SECONDS}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  TIMEOUT_SECONDS=5
fi
if ! [[ "${TARGET_PORT}" =~ ^[0-9]+$ ]]; then
  TARGET_PORT=443
fi
if ! [[ "${HTTP_ENABLED}" =~ ^[01]$ ]]; then
  HTTP_ENABLED=1
fi
case "${HTTP_METHOD}" in
  HEAD|GET|head|get) ;;
  *) HTTP_METHOD="HEAD" ;;
esac
if [[ "${HTTP_PATH}" != /* ]]; then
  HTTP_PATH="/${HTTP_PATH}"
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
  PREFLIGHT_HTTP_ENABLED="${HTTP_ENABLED}" \
  PREFLIGHT_HTTP_METHOD="${HTTP_METHOD}" \
  PREFLIGHT_HTTP_EXPECTED_CLASSES="${HTTP_EXPECTED_CLASSES}" \
  PREFLIGHT_HTTP_SCHEME="${HTTP_SCHEME}" \
  PREFLIGHT_HTTP_PATH="${HTTP_PATH}" \
  python3 <<'PY'
import http.client
import os
import socket
import ssl
import sys
import time


def elapsed_ms(start: float) -> int:
    return int(round((time.monotonic() - start) * 1000))


def clean(value: object) -> str:
    return str(value).replace(" ", "_").replace("\n", "_")


target_host = os.environ["PREFLIGHT_TARGET_HOST"]
target_port = int(os.environ["PREFLIGHT_TARGET_PORT"])
timeout_seconds = float(os.environ["PREFLIGHT_TIMEOUT_SECONDS"])
local_hostname = os.environ["PREFLIGHT_LOCAL_HOSTNAME"]
timestamp = os.environ["PREFLIGHT_TIMESTAMP"]
http_enabled = os.environ["PREFLIGHT_HTTP_ENABLED"] == "1"
http_method = os.environ["PREFLIGHT_HTTP_METHOD"].upper()
expected_classes = {
    item.strip().lower()
    for item in os.environ["PREFLIGHT_HTTP_EXPECTED_CLASSES"].split(",")
    if item.strip()
}
http_scheme = os.environ["PREFLIGHT_HTTP_SCHEME"].lower()
http_path = os.environ["PREFLIGHT_HTTP_PATH"]
base_fields = (
    f"hostname={local_hostname} "
    f"target_host={target_host} "
    f"target_port={target_port} "
    f"timestamp={timestamp}"
)

dns_start = time.monotonic()
try:
    addresses = socket.getaddrinfo(
        target_host,
        target_port,
        type=socket.SOCK_STREAM,
    )
except socket.gaierror as exc:
    print(
        "tcp_connectivity_preflight_dns_failed "
        f"{base_fields} "
        f"latency_ms={elapsed_ms(dns_start)} "
        f"error={clean(exc)}",
        file=sys.stderr,
    )
    raise SystemExit(10)

print(
    "tcp_connectivity_preflight_dns_ok "
    f"{base_fields} "
    f"address_count={len(addresses)} "
    f"latency_ms={elapsed_ms(dns_start)}"
)

last_error = ""
for family, socktype, proto, _canonname, sockaddr in addresses:
    tcp_start = time.monotonic()
    try:
        with socket.socket(family, socktype, proto) as sock:
            sock.settimeout(timeout_seconds)
            sock.connect(sockaddr)
            print(
                "tcp_connectivity_preflight_tcp_ok "
                f"{base_fields} "
                f"remote_address={clean(sockaddr[0])} "
                f"latency_ms={elapsed_ms(tcp_start)}"
            )
            break
    except OSError as exc:
        last_error = str(exc)
else:
    print(
        "tcp_connectivity_preflight_tcp_failed "
        f"{base_fields} "
        f"latency_ms={elapsed_ms(tcp_start) if addresses else 0} "
        f"error={clean(last_error or 'no_addresses_attempted')}",
        file=sys.stderr,
    )
    raise SystemExit(11)

if not http_enabled:
    print(f"app_layer_preflight_http_skipped {base_fields} reason=disabled")
    raise SystemExit(0)

if not expected_classes:
    expected_classes = {"2xx", "3xx", "4xx"}

url = f"{http_scheme}://{target_host}{http_path}"
http_start = time.monotonic()
try:
    if http_scheme == "https":
        context = ssl.create_default_context()
        connection = http.client.HTTPSConnection(
            target_host,
            target_port,
            timeout=timeout_seconds,
            context=context,
        )
    elif http_scheme == "http":
        connection = http.client.HTTPConnection(target_host, target_port, timeout=timeout_seconds)
    else:
        raise ValueError(f"unsupported_scheme_{http_scheme}")

    try:
        connection.request(http_method, http_path, headers={"User-Agent": "simplify-downloader-preflight/1.0"})
        response = connection.getresponse()
        response.read(1024)
    finally:
        connection.close()
except Exception as exc:  # noqa: BLE001 - this is an operator-facing diagnostics script.
    print(
        "app_layer_preflight_http_failed "
        f"{base_fields} "
        f"url={url} "
        f"method={http_method} "
        f"expected_classes={','.join(sorted(expected_classes))} "
        f"latency_ms={elapsed_ms(http_start)} "
        f"error={clean(type(exc).__name__ + ':' + str(exc))}",
        file=sys.stderr,
    )
    raise SystemExit(12)

status_code = int(response.status)
response_class = f"{status_code // 100}xx"
if response_class in expected_classes:
    print(
        "app_layer_preflight_http_ok "
        f"{base_fields} "
        f"url={url} "
        f"method={http_method} "
        f"status_code={status_code} "
        f"response_class={response_class} "
        f"expected_classes={','.join(sorted(expected_classes))} "
        f"latency_ms={elapsed_ms(http_start)}"
    )
    raise SystemExit(0)

print(
    "app_layer_preflight_http_failed "
    f"{base_fields} "
    f"url={url} "
    f"method={http_method} "
    f"status_code={status_code} "
    f"response_class={response_class} "
    f"expected_classes={','.join(sorted(expected_classes))} "
    f"latency_ms={elapsed_ms(http_start)} "
    "error=unexpected_response_class",
    file=sys.stderr,
)
raise SystemExit(12)
PY
}

main() {
  local rc=0
  local target_rc=0
  local classification="tcp_ok_app_ok"
  local saw_tcp_failure=0
  local saw_app_failure=0

  if [[ "$#" -eq 0 ]]; then
    set -- \
      subs.quickdrycleaning.com \
      store.ucleanlaundry.com \
      storepanel.ucleanlaundry.com
  fi

  if [[ "${HTTP_ENABLED}" = "1" ]]; then
    classification="tcp_ok_app_ok"
  else
    classification="tcp_ok_app_skipped"
  fi

  log_preflight "tcp_connectivity_preflight_started target_port=${TARGET_PORT} timeout_seconds=${TIMEOUT_SECONDS} app_layer_http_enabled=${HTTP_ENABLED} app_layer_expected_classes=${HTTP_EXPECTED_CLASSES}"

  while [[ "$#" -gt 0 ]]; do
    target_rc=0
    check_target "$1" || target_rc=$?
    if [[ "${target_rc}" -ne 0 ]]; then
      rc=1
      if [[ "${target_rc}" -eq 12 ]]; then
        saw_app_failure=1
      else
        saw_tcp_failure=1
      fi
    fi
    shift
  done

  if [[ "${rc}" -ne 0 ]]; then
    if [[ "${saw_tcp_failure}" -eq 1 && "${saw_app_failure}" -eq 1 ]]; then
      classification="tcp_and_app_layer_failed"
    elif [[ "${saw_app_failure}" -eq 1 ]]; then
      classification="app_layer_failed"
    else
      classification="tcp_failed"
    fi
    log_preflight "orders_sync_preflight_summary classification=${classification} exit_code=${rc}"
    if [[ "${classification}" = "app_layer_failed" || "${classification}" = "tcp_and_app_layer_failed" ]]; then
      log_preflight "app_layer_preflight_failed_summary exit_code=${rc}"
    else
      log_preflight "tcp_connectivity_preflight_failed_summary exit_code=${rc}"
    fi
    return "${rc}"
  fi

  log_preflight "orders_sync_preflight_summary classification=${classification} exit_code=0"
  log_preflight "tcp_connectivity_preflight_succeeded classification=${classification}"
  return 0
}

main "$@"
