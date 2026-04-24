#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# cron_run_orders_and_reports.sh
#
# macOS Big Sur compatible, production-grade cron wrapper for:
#   1. orders_sync_run_profiler.sh
#   2. run_local_reports_daily_sales.sh
#   3. run_local_reports_pending_deliveries.sh
#
# Features:
# - macOS-safe lock using mkdir
# - PID-aware lock ownership
# - stale lock detection and cleanup
# - stale matching process detection/termination
# - detailed logging
# - safe cleanup on EXIT/ERR/INT/TERM
# - Bash 3.2 compatible (no associative arrays, no mapfile)
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${ENV_FILE:-${SCRIPT_DIR}/cron.env}"

if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
fi

REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${REPO_ROOT}/logs"
LOCK_DIR="${REPO_ROOT}/tmp"
GLOBAL_LOCK_DIR="${LOCK_DIR}/cron_heavy_pipelines.lock"
RUN_LOCK_DIR="${LOCK_DIR}/cron_run_orders_and_reports.lock"

TIMESTAMP="$(date '+%Y-%m-%d_%H-%M-%S')"
LOG_FILE="${LOG_DIR}/cron_run_orders_and_reports_${TIMESTAMP}.log"

LOCK_PID_FILE="${RUN_LOCK_DIR}/pid"
LOCK_STARTED_AT_FILE="${RUN_LOCK_DIR}/started_at"
LOCK_STARTED_AT_EPOCH_FILE="${RUN_LOCK_DIR}/started_at_epoch"
LOCK_HOST_FILE="${RUN_LOCK_DIR}/host"
LOCK_CWD_FILE="${RUN_LOCK_DIR}/cwd"
LOCK_CMD_FILE="${RUN_LOCK_DIR}/command"
LOCK_PGID_FILE="${RUN_LOCK_DIR}/pgid"
GLOBAL_LOCK_PID_FILE="${GLOBAL_LOCK_DIR}/pid"
GLOBAL_LOCK_STARTED_AT_FILE="${GLOBAL_LOCK_DIR}/started_at"
GLOBAL_LOCK_HOST_FILE="${GLOBAL_LOCK_DIR}/host"
GLOBAL_LOCK_CWD_FILE="${GLOBAL_LOCK_DIR}/cwd"
GLOBAL_LOCK_CMD_FILE="${GLOBAL_LOCK_DIR}/command"
GLOBAL_LOCK_PGID_FILE="${GLOBAL_LOCK_DIR}/pgid"

KILL_WAIT_SECONDS="${KILL_WAIT_SECONDS:-5}"
LOCK_WAIT_SECONDS="${LOCK_WAIT_SECONDS:-300}"
LOCK_POLL_SECONDS="${LOCK_POLL_SECONDS:-5}"
SAFE_MODE="${SAFE_MODE:-1}"
CRON_HOME="${CRON_HOME:-${HOME:-/tmp}}"
CRON_PATH="${CRON_PATH:-/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin}"
ORDERS_MAX_ATTEMPTS="${ORDERS_MAX_ATTEMPTS:-1}"
ORDERS_RETRY_DELAY_SECONDS="${ORDERS_RETRY_DELAY_SECONDS:-5}"
DAILY_MAX_ATTEMPTS="${DAILY_MAX_ATTEMPTS:-3}"
DAILY_RETRY_DELAY_SECONDS="${DAILY_RETRY_DELAY_SECONDS:-10}"
PENDING_MAX_ATTEMPTS="${PENDING_MAX_ATTEMPTS:-3}"
PENDING_RETRY_DELAY_SECONDS="${PENDING_RETRY_DELAY_SECONDS:-10}"
DAILY_RESCUE_AFTER_PENDING_SUCCESS="${DAILY_RESCUE_AFTER_PENDING_SUCCESS:-1}"
DAILY_RESCUE_MAX_ATTEMPTS="${DAILY_RESCUE_MAX_ATTEMPTS:-1}"
DAILY_RESCUE_RETRY_DELAY_SECONDS="${DAILY_RESCUE_RETRY_DELAY_SECONDS:-5}"

if ! [[ "${LOCK_WAIT_SECONDS}" =~ ^[0-9]+$ ]]; then
  LOCK_WAIT_SECONDS=300
fi
if ! [[ "${LOCK_POLL_SECONDS}" =~ ^[0-9]+$ ]] || [[ "${LOCK_POLL_SECONDS}" -eq 0 ]]; then
  LOCK_POLL_SECONDS=5
fi
if ! [[ "${SAFE_MODE}" =~ ^[01]$ ]]; then
  SAFE_MODE=1
fi
if ! [[ "${DAILY_RESCUE_AFTER_PENDING_SUCCESS}" =~ ^[01]$ ]]; then
  DAILY_RESCUE_AFTER_PENDING_SUCCESS=1
fi

mkdir -p "${LOG_DIR}" "${LOCK_DIR}"
cd "${REPO_ROOT}"

export HOME="${CRON_HOME}"
export PATH="${CRON_PATH}:${PATH}"
export LANG="${LANG:-en_US.UTF-8}"

GLOBAL_LOCK_ACQUIRED=0

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] $*" >> "${LOG_FILE}"
}

section() {
  log "================================================================"
  log "$*"
  log "================================================================"
}

safe_cat() {
  local file_path="$1"
  if [[ -f "${file_path}" ]]; then
    cat "${file_path}" 2>/dev/null || true
  fi
}

pid_is_alive() {
  local pid="$1"
  [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null
}

get_pid_elapsed_seconds() {
  local pid="$1"
  local etime

  etime="$(ps -o etime= -p "${pid}" 2>/dev/null | awk '{$1=$1; print}')"
  [[ -z "${etime}" ]] && return 1

  awk -v etime="${etime}" '
    function to_seconds(t,   n,a,d,h,m,s) {
      d=0; h=0; m=0; s=0
      if (t ~ /-/) {
        split(t,a,"-")
        d=a[1]
        t=a[2]
      }
      n=split(t,a,":")
      if (n==2) {
        m=a[1]; s=a[2]
      } else if (n==3) {
        h=a[1]; m=a[2]; s=a[3]
      } else {
        exit 1
      }
      print (d*86400)+(h*3600)+(m*60)+s
    }
    BEGIN { to_seconds(etime) }
  '
}

get_pid_command_line() {
  local pid="$1"
  ps -o command= -p "${pid}" 2>/dev/null | awk '{$1=$1; print}'
}

get_pid_start_epoch() {
  local pid="$1"
  local lstart
  lstart="$(ps -o lstart= -p "${pid}" 2>/dev/null | awk '{$1=$1; print}')"
  [[ -z "${lstart}" ]] && return 1
  python3 - "${lstart}" <<'PY' 2>/dev/null || true
import datetime
import sys
import time
try:
    dt = datetime.datetime.strptime(sys.argv[1], "%a %b %d %H:%M:%S %Y")
except Exception:
    sys.exit(1)
print(int(time.mktime(dt.timetuple())))
PY
}

pid_command_matches_expected_script() {
  local pid="$1"
  local expected_script="$2"
  local cmdline

  cmdline="$(get_pid_command_line "${pid}")"
  [[ "${cmdline}" = "${expected_script}" ]]
}

is_pid_safe_termination_candidate() {
  local pid="$1"
  local lock_started_epoch="$2"
  local expected_script="$3"
  local current_pid
  local parent_pid
  local pid_start_epoch
  local cmdline

  current_pid="$$"
  parent_pid="${PPID:-}"

  if [[ -z "${pid}" ]] || ! [[ "${pid}" =~ ^[0-9]+$ ]]; then
    log "WARNING: Cannot validate non-numeric lock pid='${pid}'."
    return 1
  fi

  if [[ "${pid}" = "${current_pid}" ]] || [[ -n "${parent_pid}" && "${pid}" = "${parent_pid}" ]]; then
    log "WARNING: Refusing to terminate current or parent process. pid=${pid}"
    return 1
  fi

  cmdline="$(get_pid_command_line "${pid}")"
  if ! pid_command_matches_expected_script "${pid}" "${expected_script}"; then
    log "WARNING: PID=${pid} command mismatch. expected='${expected_script}' actual='${cmdline:-<missing>}'"
    return 1
  fi

  if [[ -z "${lock_started_epoch}" ]] || ! [[ "${lock_started_epoch}" =~ ^[0-9]+$ ]]; then
    log "WARNING: Missing/invalid lock started_at_epoch='${lock_started_epoch:-<missing>}'."
    return 1
  fi

  pid_start_epoch="$(get_pid_start_epoch "${pid}")"
  if [[ -z "${pid_start_epoch}" ]] || ! [[ "${pid_start_epoch}" =~ ^[0-9]+$ ]]; then
    log "WARNING: Missing/invalid process start epoch for pid=${pid}. Cannot verify ownership."
    return 1
  fi

  if [[ "${pid_start_epoch}" -ge "${lock_started_epoch}" ]]; then
    log "WARNING: PID=${pid} started at ${pid_start_epoch}, not older than lock_started_epoch=${lock_started_epoch}."
    return 1
  fi

  return 0
}

terminate_pid_gracefully() {
  local pid="$1"
  local i

  if ! pid_is_alive "${pid}"; then
    return 0
  fi

  log "Attempting graceful termination for PID=${pid}"
  kill -TERM "${pid}" 2>/dev/null || true

  for ((i=1; i<=KILL_WAIT_SECONDS; i++)); do
    if ! pid_is_alive "${pid}"; then
      log "PID=${pid} terminated gracefully"
      return 0
    fi
    sleep 1
  done

  if pid_is_alive "${pid}"; then
    log "PID=${pid} still alive after ${KILL_WAIT_SECONDS}s, sending SIGKILL"
    kill -KILL "${pid}" 2>/dev/null || true
    sleep 1
  fi

  if pid_is_alive "${pid}"; then
    log "WARNING: PID=${pid} is still alive even after SIGKILL"
    return 1
  fi

  log "PID=${pid} terminated after SIGKILL"
  return 0
}

remove_lock_artifacts() {
  rm -f "${LOCK_PID_FILE}" 2>/dev/null || true
  rm -f "${LOCK_STARTED_AT_FILE}" 2>/dev/null || true
  rm -f "${LOCK_STARTED_AT_EPOCH_FILE}" 2>/dev/null || true
  rm -f "${LOCK_HOST_FILE}" 2>/dev/null || true
  rm -f "${LOCK_CWD_FILE}" 2>/dev/null || true
  rm -f "${LOCK_CMD_FILE}" 2>/dev/null || true
  rm -f "${LOCK_PGID_FILE}" 2>/dev/null || true
  rmdir "${RUN_LOCK_DIR}" 2>/dev/null || true
}

remove_global_lock_artifacts() {
  rm -f "${GLOBAL_LOCK_PID_FILE}" 2>/dev/null || true
  rm -f "${GLOBAL_LOCK_STARTED_AT_FILE}" 2>/dev/null || true
  rm -f "${GLOBAL_LOCK_HOST_FILE}" 2>/dev/null || true
  rm -f "${GLOBAL_LOCK_CWD_FILE}" 2>/dev/null || true
  rm -f "${GLOBAL_LOCK_CMD_FILE}" 2>/dev/null || true
  rm -f "${GLOBAL_LOCK_PGID_FILE}" 2>/dev/null || true
  rmdir "${GLOBAL_LOCK_DIR}" 2>/dev/null || true
}

write_lock_metadata() {
  echo "$$" > "${LOCK_PID_FILE}"
  date '+%Y-%m-%d %H:%M:%S %Z' > "${LOCK_STARTED_AT_FILE}"
  date '+%s' > "${LOCK_STARTED_AT_EPOCH_FILE}"
  hostname > "${LOCK_HOST_FILE}"
  pwd > "${LOCK_CWD_FILE}"
  printf '%s\n' "$0 $*" > "${LOCK_CMD_FILE}"
  ps -o pgid= -p "$$" 2>/dev/null | awk '{$1=$1; print}' > "${LOCK_PGID_FILE}" || true
}

acquire_fresh_lock() {
  if mkdir "${RUN_LOCK_DIR}" 2>/dev/null; then
    write_lock_metadata "$@"
    log "[local lock] Lock acquired successfully. PID=$$"
    return 0
  fi
  return 1
}

acquire_lock_with_wait() {
  local wait_started_at
  local now
  local waited_seconds
  local existing_pid
  local elapsed_secs
  local wait_started_logged=0

  wait_started_at="$(date +%s)"

  while true; do
    if acquire_fresh_lock "$@"; then
      now="$(date +%s)"
      waited_seconds=$((now - wait_started_at))
      if [[ "${wait_started_logged}" -eq 1 ]]; then
        log "[local lock] Lock wait ended. total_wait_seconds=${waited_seconds}"
      fi
      return 0
    fi

    existing_pid="$(safe_cat "${LOCK_PID_FILE}")"
    if [[ -n "${existing_pid}" ]] && pid_is_alive "${existing_pid}"; then
      now="$(date +%s)"
      waited_seconds=$((now - wait_started_at))
      elapsed_secs="$(get_pid_elapsed_seconds "${existing_pid}" 2>/dev/null || true)"

      if [[ "${LOCK_WAIT_SECONDS}" -eq 0 ]]; then
        log "[local lock] Lock held by live PID=${existing_pid} and waiting disabled (LOCK_WAIT_SECONDS=0). Exiting."
        exit 1
      fi

      if [[ "${wait_started_logged}" -eq 0 ]]; then
        log "[local lock] Lock wait started. owner_pid=${existing_pid} owner_elapsed_seconds=${elapsed_secs:-unknown} timeout_seconds=${LOCK_WAIT_SECONDS} poll_seconds=${LOCK_POLL_SECONDS}"
        wait_started_logged=1
      fi

      if [[ "${waited_seconds}" -ge "${LOCK_WAIT_SECONDS}" ]]; then
        log "[local lock] Timed out waiting for lock after ${waited_seconds}s (timeout=${LOCK_WAIT_SECONDS}s, owner_pid=${existing_pid}). Exiting."
        exit 1
      fi

      log "[local lock] Lock still held by live PID=${existing_pid}; waited=${waited_seconds}s/${LOCK_WAIT_SECONDS}s. Sleeping ${LOCK_POLL_SECONDS}s before retry."
      sleep "${LOCK_POLL_SECONDS}"
      continue
    fi

    maybe_cleanup_stale_lock "$@"
    now="$(date +%s)"
    waited_seconds=$((now - wait_started_at))
    if [[ "${wait_started_logged}" -eq 1 ]]; then
      log "[local lock] Lock wait ended after stale cleanup. total_wait_seconds=${waited_seconds}"
    fi
    return 0
  done
}

write_global_lock_metadata() {
  echo "$$" > "${GLOBAL_LOCK_PID_FILE}"
  date '+%Y-%m-%d %H:%M:%S %Z' > "${GLOBAL_LOCK_STARTED_AT_FILE}"
  hostname > "${GLOBAL_LOCK_HOST_FILE}"
  pwd > "${GLOBAL_LOCK_CWD_FILE}"
  printf '%s\n' "$0 $*" > "${GLOBAL_LOCK_CMD_FILE}"
  ps -o pgid= -p "$$" 2>/dev/null | awk '{$1=$1; print}' > "${GLOBAL_LOCK_PGID_FILE}" || true
}

acquire_global_lock_with_wait() {
  local wait_started_at
  local now
  local waited_seconds
  local existing_pid
  local elapsed_secs
  local wait_started_logged=0

  wait_started_at="$(date +%s)"

  while true; do
    if mkdir "${GLOBAL_LOCK_DIR}" 2>/dev/null; then
      write_global_lock_metadata "$@"
      GLOBAL_LOCK_ACQUIRED=1
      now="$(date +%s)"
      waited_seconds=$((now - wait_started_at))
      log "[global lock] Lock acquired successfully. PID=$$ total_wait_seconds=${waited_seconds}"
      return 0
    fi

    existing_pid="$(safe_cat "${GLOBAL_LOCK_PID_FILE}")"
    if [[ -n "${existing_pid}" ]] && pid_is_alive "${existing_pid}"; then
      now="$(date +%s)"
      waited_seconds=$((now - wait_started_at))
      elapsed_secs="$(get_pid_elapsed_seconds "${existing_pid}" 2>/dev/null || true)"

      if [[ "${LOCK_WAIT_SECONDS}" -eq 0 ]]; then
        log "[global lock] Lock held by live PID=${existing_pid} and waiting disabled (LOCK_WAIT_SECONDS=0). Exiting."
        exit 1
      fi

      if [[ "${wait_started_logged}" -eq 0 ]]; then
        log "[global lock] Lock wait started. owner_pid=${existing_pid} owner_elapsed_seconds=${elapsed_secs:-unknown} timeout_seconds=${LOCK_WAIT_SECONDS} poll_seconds=${LOCK_POLL_SECONDS}"
        wait_started_logged=1
      fi

      if [[ "${waited_seconds}" -ge "${LOCK_WAIT_SECONDS}" ]]; then
        log "[global lock] Timed out waiting for lock after ${waited_seconds}s (timeout=${LOCK_WAIT_SECONDS}s, owner_pid=${existing_pid}). Exiting."
        exit 1
      fi

      log "[global lock] Lock still held by live PID=${existing_pid}; waited=${waited_seconds}s/${LOCK_WAIT_SECONDS}s. Sleeping ${LOCK_POLL_SECONDS}s before retry."
      sleep "${LOCK_POLL_SECONDS}"
      continue
    fi

    log "[global lock] Removing stale lock artifacts."
    rm -rf "${GLOBAL_LOCK_DIR}"
  done
}

maybe_cleanup_stale_lock() {
  local existing_pid
  local existing_started_at
  local existing_started_epoch
  local existing_host
  local existing_cwd
  local existing_cmd
  local expected_script
  local elapsed_secs

  existing_pid="$(safe_cat "${LOCK_PID_FILE}")"
  existing_started_at="$(safe_cat "${LOCK_STARTED_AT_FILE}")"
  existing_started_epoch="$(safe_cat "${LOCK_STARTED_AT_EPOCH_FILE}")"
  existing_host="$(safe_cat "${LOCK_HOST_FILE}")"
  existing_cwd="$(safe_cat "${LOCK_CWD_FILE}")"
  existing_cmd="$(safe_cat "${LOCK_CMD_FILE}")"
  expected_script="${SCRIPT_DIR}/cron_run_orders_and_reports.sh"

  section "Existing lock detected, inspecting ownership"

  log "Existing lock metadata:"
  log "  pid=${existing_pid:-<missing>}"
  log "  started_at=${existing_started_at:-<missing>}"
  log "  started_at_epoch=${existing_started_epoch:-<missing>}"
  log "  host=${existing_host:-<missing>}"
  log "  cwd=${existing_cwd:-<missing>}"
  log "  command=${existing_cmd:-<missing>}"

  if [[ -n "${existing_pid}" ]] && pid_is_alive "${existing_pid}"; then
    elapsed_secs="$(get_pid_elapsed_seconds "${existing_pid}" 2>/dev/null || true)"
    log "Existing lock PID ${existing_pid} is alive. elapsed_seconds=${elapsed_secs:-unknown}"
    if ! is_pid_safe_termination_candidate "${existing_pid}" "${existing_started_epoch}" "${expected_script}"; then
      log "WARNING: Lock ownership is ambiguous. Failing safe and exiting without termination."
      exit 1
    fi

    if [[ "${SAFE_MODE}" -eq 1 ]]; then
      log "SAFE_MODE=1: validated stale owner pid=${existing_pid} but non-owner termination is disabled. Exiting safely."
      exit 1
    fi

    log "SAFE_MODE=0 and lock owner validated. Terminating stale owner PID=${existing_pid}"
    terminate_pid_gracefully "${existing_pid}" || {
      log "WARNING: Failed to terminate stale owner pid=${existing_pid}. Exiting safely."
      exit 1
    }
  else
    log "Lock owner PID is not alive or missing. Treating lock as stale."
  fi

  log "Removing stale lock artifacts."
  rm -rf "${RUN_LOCK_DIR}"

  if ! mkdir "${RUN_LOCK_DIR}" 2>/dev/null; then
    log "Failed to recreate lock directory after stale cleanup. Exiting."
    exit 1
  fi

  write_lock_metadata "$@"
  log "Fresh lock acquired after stale cleanup. PID=$$"
}

cleanup() {
  local exit_code="$1"
  local trap_name="$2"

  if [[ "${exit_code}" -eq 0 ]]; then
    log "Run completed successfully via trap=${trap_name}"
  else
    log "Run exiting with non-zero status=${exit_code} via trap=${trap_name}"
  fi

  remove_lock_artifacts
  if [[ "${GLOBAL_LOCK_ACQUIRED}" -eq 1 ]]; then
    remove_global_lock_artifacts
  fi
}

on_err() {
  local exit_code=$?
  log "ERROR trap triggered. exit_code=${exit_code}"
  cleanup "${exit_code}" "ERR"
  exit "${exit_code}"
}

on_exit() {
  local exit_code=$?
  cleanup "${exit_code}" "EXIT"
}

on_signal() {
  local sig="$1"
  log "Signal trap triggered: ${sig}"
  exit 1
}

trap on_err ERR
trap on_exit EXIT
trap 'on_signal INT' INT
trap 'on_signal TERM' TERM

section "CRON RUN STARTED"
log "ENV_FILE=${ENV_FILE}"
log "REPO_ROOT=${REPO_ROOT}"
log "LOG_FILE=${LOG_FILE}"
log "HOME=${HOME}"
log "PATH=${PATH}"
log "LANG=${LANG}"
log "LOCK_WAIT_SECONDS=${LOCK_WAIT_SECONDS}"
log "LOCK_POLL_SECONDS=${LOCK_POLL_SECONDS}"
log "SAFE_MODE=${SAFE_MODE}"
log "ORDERS_MAX_ATTEMPTS=${ORDERS_MAX_ATTEMPTS} ORDERS_RETRY_DELAY_SECONDS=${ORDERS_RETRY_DELAY_SECONDS}"
log "DAILY_MAX_ATTEMPTS=${DAILY_MAX_ATTEMPTS} DAILY_RETRY_DELAY_SECONDS=${DAILY_RETRY_DELAY_SECONDS}"
log "PENDING_MAX_ATTEMPTS=${PENDING_MAX_ATTEMPTS} PENDING_RETRY_DELAY_SECONDS=${PENDING_RETRY_DELAY_SECONDS}"
log "DAILY_RESCUE_AFTER_PENDING_SUCCESS=${DAILY_RESCUE_AFTER_PENDING_SUCCESS} DAILY_RESCUE_MAX_ATTEMPTS=${DAILY_RESCUE_MAX_ATTEMPTS} DAILY_RESCUE_RETRY_DELAY_SECONDS=${DAILY_RESCUE_RETRY_DELAY_SECONDS}"
log "GLOBAL_LOCK_DIR=${GLOBAL_LOCK_DIR}"
log "LOCAL_LOCK_DIR=${RUN_LOCK_DIR}"
log "poetry=$(command -v poetry || echo NOT_FOUND)"
log "shell_pid=$$"
log "parent_pid=${PPID:-unknown}"
log "hostname=$(hostname)"

acquire_global_lock_with_wait "$@"
acquire_lock_with_wait "$@"

run_step() {
  local step_name="$1"
  local step_cmd="$2"
  local max_attempts="${3:-1}"
  local retry_delay_seconds="${4:-5}"
  local step_start
  local step_end
  local duration
  local attempt=1
  local rc=0

  section "Running ${step_name}"
  log "Command: ${step_cmd}"
  log "Attempts configured: ${max_attempts}; retry_delay_seconds=${retry_delay_seconds}"

  while [[ "${attempt}" -le "${max_attempts}" ]]; do
    log "${step_name}: attempt ${attempt}/${max_attempts} starting"
    step_start="$(date +%s)"

    if bash -c "${step_cmd}" >> "${LOG_FILE}" 2>&1; then
      step_end="$(date +%s)"
      duration=$((step_end - step_start))
      log "${step_name}: attempt ${attempt}/${max_attempts} succeeded in ${duration}s"
      return 0
    fi

    rc=$?
    step_end="$(date +%s)"
    duration=$((step_end - step_start))
    log "WARNING: ${step_name}: attempt ${attempt}/${max_attempts} failed with exit_code=${rc} after ${duration}s"

    attempt=$((attempt + 1))
    if [[ "${attempt}" -le "${max_attempts}" ]]; then
      log "${step_name}: sleeping ${retry_delay_seconds}s before retry"
      sleep "${retry_delay_seconds}"
    fi
  done

  log "ERROR: ${step_name} failed after ${max_attempts} attempts"
  return "${rc}"
}

orders_rc=0
daily_rc=0
pending_rc=0
daily_rescue_rc=0
run_started_epoch="$(date +%s)"

run_step "Script 1: orders_sync_run_profiler" "./scripts/orders_sync_run_profiler.sh" "${ORDERS_MAX_ATTEMPTS}" "${ORDERS_RETRY_DELAY_SECONDS}" || orders_rc=$?
run_step "Script 2: daily_sales_report" "./scripts/run_local_reports_daily_sales.sh" "${DAILY_MAX_ATTEMPTS}" "${DAILY_RETRY_DELAY_SECONDS}" || daily_rc=$?
run_step "Script 3: pending_deliveries" "./scripts/run_local_reports_pending_deliveries.sh" "${PENDING_MAX_ATTEMPTS}" "${PENDING_RETRY_DELAY_SECONDS}" || pending_rc=$?

if [[ "${pending_rc}" -eq 0 && "${daily_rc}" -ne 0 && "${DAILY_RESCUE_AFTER_PENDING_SUCCESS}" -eq 1 ]]; then
  section "OPTIONAL DAILY RESCUE PASS"
  log "Pending deliveries succeeded while daily sales failed; running optional daily rescue pass."
  run_step \
    "Script 2B: daily_sales_report_rescue" \
    "./scripts/run_local_reports_daily_sales.sh" \
    "${DAILY_RESCUE_MAX_ATTEMPTS}" \
    "${DAILY_RESCUE_RETRY_DELAY_SECONDS}" || daily_rescue_rc=$?

  if [[ "${daily_rescue_rc}" -eq 0 ]]; then
    daily_rc=0
    log "Daily rescue pass succeeded; overriding daily_sales_report_rc to 0."
  else
    log "WARNING: Daily rescue pass failed with rc=${daily_rescue_rc}."
  fi
fi

run_finished_epoch="$(date +%s)"
run_duration_seconds=$((run_finished_epoch - run_started_epoch))

section "RUN STATUS SUMMARY"
log "orders_sync_run_profiler_rc=${orders_rc}"
log "daily_sales_report_rc=${daily_rc}"
log "pending_deliveries_rc=${pending_rc}"
log "daily_sales_report_rescue_rc=${daily_rescue_rc}"
log "total_duration_seconds=${run_duration_seconds}"

if [[ "${daily_rc}" -ne 0 && "${pending_rc}" -ne 0 ]]; then
  log "ERROR: Both report pipelines failed (daily_sales_report_rc=${daily_rc}, pending_deliveries_rc=${pending_rc})."
  exit 1
fi

section "CRON RUN FINISHED SUCCESSFULLY"
exit 0
