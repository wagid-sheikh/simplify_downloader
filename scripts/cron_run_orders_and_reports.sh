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
#
# Orders profiler exit semantics:
# - orders_sync_run_profiler.sh performs a DNS/TCP preflight named
#   tcp_connectivity_preflight before launching Playwright. Optional app-layer
#   HTTP checks are classified separately so operators do not mistake TCP
#   reachability for full application readiness.
# - orders_sync_run_profiler.sh normally preserves legacy non-breaking CLI behavior
#   for persisted overall_status="failed" after summaries/notifications are written.
# - Set ORDERS_SYNC_PROFILER_FAIL_ON_FAILED_STATUS=1 to make that profiler CLI step
#   return non-zero when the final profiler overall_status is "failed". This wrapper
#   logs orders_sync_run_profiler_rc and continues to report pipelines; required
#   report pipeline failures still control the final cron exit status below.
# ============================================================================
# Usage examples:
#   # Local/manual run; reports always regenerate.
#   ./scripts/cron_run_orders_and_reports.sh
#
# Daily Sales, MTD Same-Day Fulfillment, and Pending Deliveries regeneration are
# mandatory on the cron path. The underlying report CLIs always regenerate and
# append new summaries/documents, so this wrapper does not rely on --force.
#
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${ENV_FILE:-${SCRIPT_DIR}/cron.env}"

if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
fi

REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${REPO_ROOT}/logs"
LOCK_DIR="${REPO_ROOT}/tmp"
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

KILL_WAIT_SECONDS="${KILL_WAIT_SECONDS:-5}"
LOCK_WAIT_SECONDS="${LOCK_WAIT_SECONDS:-300}"
LOCK_POLL_SECONDS="${LOCK_POLL_SECONDS:-5}"
SAFE_MODE="${SAFE_MODE:-1}"
STALE_LOCK_MAX_AGE_SECONDS="${STALE_LOCK_MAX_AGE_SECONDS:-7200}"
ALLOW_STALE_OWNER_TERMINATION="${ALLOW_STALE_OWNER_TERMINATION:-0}"
CRON_HOME="${CRON_HOME:-${HOME:-/tmp}}"
CRON_PATH="${CRON_PATH:-/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin}"
ORDERS_MAX_ATTEMPTS="${ORDERS_MAX_ATTEMPTS:-3}"
ORDERS_RETRY_DELAY_SECONDS="${ORDERS_RETRY_DELAY_SECONDS:-30}"
ORDERS_RETRY_BACKOFF_MULTIPLIER="${ORDERS_RETRY_BACKOFF_MULTIPLIER:-2}"
ORDERS_RETRY_MAX_DELAY_SECONDS="${ORDERS_RETRY_MAX_DELAY_SECONDS:-300}"
ORDERS_RETRY_JITTER_SECONDS="${ORDERS_RETRY_JITTER_SECONDS:-10}"
DAILY_MAX_ATTEMPTS="${DAILY_MAX_ATTEMPTS:-3}"
DAILY_RETRY_DELAY_SECONDS="${DAILY_RETRY_DELAY_SECONDS:-10}"
PENDING_MAX_ATTEMPTS="${PENDING_MAX_ATTEMPTS:-3}"
PENDING_RETRY_DELAY_SECONDS="${PENDING_RETRY_DELAY_SECONDS:-10}"
MTD_SAME_DAY_MAX_ATTEMPTS="${MTD_SAME_DAY_MAX_ATTEMPTS:-3}"
MTD_SAME_DAY_RETRY_DELAY_SECONDS="${MTD_SAME_DAY_RETRY_DELAY_SECONDS:-10}"
DAILY_RESCUE_AFTER_PENDING_SUCCESS="${DAILY_RESCUE_AFTER_PENDING_SUCCESS:-1}"
DAILY_RESCUE_MAX_ATTEMPTS="${DAILY_RESCUE_MAX_ATTEMPTS:-1}"
DAILY_RESCUE_RETRY_DELAY_SECONDS="${DAILY_RESCUE_RETRY_DELAY_SECONDS:-5}"
ORDERS_SYNC_PROFILER_FAIL_ON_FAILED_STATUS="${ORDERS_SYNC_PROFILER_FAIL_ON_FAILED_STATUS:-1}"
ORDERS_STEP_TIMEOUT_SECONDS="${ORDERS_STEP_TIMEOUT_SECONDS:-5400}"
DAILY_SALES_STEP_TIMEOUT_SECONDS="${DAILY_SALES_STEP_TIMEOUT_SECONDS:-1800}"
PENDING_DELIVERIES_STEP_TIMEOUT_SECONDS="${PENDING_DELIVERIES_STEP_TIMEOUT_SECONDS:-1800}"

if ! [[ "${LOCK_WAIT_SECONDS}" =~ ^[0-9]+$ ]]; then
  LOCK_WAIT_SECONDS=300
fi
if ! [[ "${LOCK_POLL_SECONDS}" =~ ^[0-9]+$ ]] || [[ "${LOCK_POLL_SECONDS}" -eq 0 ]]; then
  LOCK_POLL_SECONDS=5
fi
if ! [[ "${SAFE_MODE}" =~ ^[01]$ ]]; then
  SAFE_MODE=1
fi
if ! [[ "${STALE_LOCK_MAX_AGE_SECONDS}" =~ ^[0-9]+$ ]]; then
  STALE_LOCK_MAX_AGE_SECONDS=7200
fi
if ! [[ "${ALLOW_STALE_OWNER_TERMINATION}" =~ ^[01]$ ]]; then
  ALLOW_STALE_OWNER_TERMINATION=0
fi
if ! [[ "${DAILY_RESCUE_AFTER_PENDING_SUCCESS}" =~ ^[01]$ ]]; then
  DAILY_RESCUE_AFTER_PENDING_SUCCESS=1
fi
for timeout_var_name in ORDERS_STEP_TIMEOUT_SECONDS DAILY_SALES_STEP_TIMEOUT_SECONDS PENDING_DELIVERIES_STEP_TIMEOUT_SECONDS; do
  timeout_var_value="${!timeout_var_name}"
  if ! [[ "${timeout_var_value}" =~ ^[0-9]+$ ]]; then
    case "${timeout_var_name}" in
      ORDERS_STEP_TIMEOUT_SECONDS) ORDERS_STEP_TIMEOUT_SECONDS=5400 ;;
      DAILY_SALES_STEP_TIMEOUT_SECONDS) DAILY_SALES_STEP_TIMEOUT_SECONDS=1800 ;;
      PENDING_DELIVERIES_STEP_TIMEOUT_SECONDS) PENDING_DELIVERIES_STEP_TIMEOUT_SECONDS=1800 ;;
    esac
  fi
done

mkdir -p "${LOG_DIR}" "${LOCK_DIR}"
cd "${REPO_ROOT}"

export HOME="${CRON_HOME}"
export PATH="${CRON_PATH}:${PATH}"
export LANG="${LANG:-en_US.UTF-8}"

ORDERS_SYNC_PREFLIGHT_CLASSIFICATION="not_run"
ORDERS_SYNC_PROFILER_RUN_ID=""
ORDERS_SYNC_PROFILER_STATUS="unknown"


log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] $*" >> "${LOG_FILE}"
}

section() {
  log "================================================================"
  log "$*"
  log "================================================================"
}

log "python3_version=$(python3 --version 2>&1)"

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

terminate_by_pgid_or_pid() {
  local pgid_file="$1"
  local pid="$2"
  local pgid
  local i

  pgid="$(safe_cat "${pgid_file}")"
  if [[ -n "${pgid}" ]] && [[ "${pgid}" =~ ^[0-9]+$ ]]; then
    log "Attempting graceful termination for process group PGID=${pgid} (owner PID=${pid})"
    kill -TERM "-${pgid}" 2>/dev/null || true

    for ((i=1; i<=KILL_WAIT_SECONDS; i++)); do
      if ! pid_is_alive "${pid}"; then
        log "Owner PID=${pid} terminated after process-group TERM"
        return 0
      fi
      sleep 1
    done

    if pid_is_alive "${pid}"; then
      log "Owner PID=${pid} still alive after ${KILL_WAIT_SECONDS}s, sending SIGKILL to PGID=${pgid}"
      kill -KILL "-${pgid}" 2>/dev/null || true
      sleep 1
    fi

    if pid_is_alive "${pid}"; then
      log "WARNING: Owner PID=${pid} still alive after process-group SIGKILL"
      return 1
    fi

    log "Owner PID=${pid} terminated via process-group kill."
    return 0
  fi

  log "Process-group metadata unavailable/invalid. Falling back to single PID termination for PID=${pid}"
  terminate_pid_gracefully "${pid}"
}

terminate_child_process_group() {
  local pgid="$1"
  local child_pid="$2"
  local i

  log "Attempting graceful termination for child process group PGID=${pgid} (child_pid=${child_pid})"
  kill -TERM "-${pgid}" 2>/dev/null || true
  for ((i=1; i<=KILL_WAIT_SECONDS; i++)); do
    if ! pid_is_alive "${child_pid}"; then
      log "Child PID=${child_pid} terminated after process-group TERM"
      return 0
    fi
    sleep 1
  done
  if pid_is_alive "${child_pid}"; then
    log "Child PID=${child_pid} still alive after ${KILL_WAIT_SECONDS}s, sending SIGKILL to PGID=${pgid}"
    kill -KILL "-${pgid}" 2>/dev/null || true
    sleep 1
  fi
  ! pid_is_alive "${child_pid}"
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


maybe_cleanup_stale_lock() {
  local existing_pid
  local existing_started_at
  local existing_started_epoch
  local existing_host
  local existing_cwd
  local existing_cmd
  local expected_script
  local elapsed_secs
  local lock_age_seconds
  local now_epoch

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
    now_epoch="$(date +%s)"
    lock_age_seconds=-1
    if [[ -n "${existing_started_epoch}" ]] && [[ "${existing_started_epoch}" =~ ^[0-9]+$ ]]; then
      lock_age_seconds=$((now_epoch - existing_started_epoch))
    fi
    elapsed_secs="$(get_pid_elapsed_seconds "${existing_pid}" 2>/dev/null || true)"
    log "Existing lock PID ${existing_pid} is alive. elapsed_seconds=${elapsed_secs:-unknown} lock_age_seconds=${lock_age_seconds}"
    if ! is_pid_safe_termination_candidate "${existing_pid}" "${existing_started_epoch}" "${expected_script}"; then
      log "WARNING: Lock ownership is ambiguous. Failing safe and exiting without termination."
      exit 1
    fi

    if [[ "${lock_age_seconds}" -lt "${STALE_LOCK_MAX_AGE_SECONDS}" ]]; then
      log "Lock owner PID validated but lock age (${lock_age_seconds}s) does not exceed STALE_LOCK_MAX_AGE_SECONDS=${STALE_LOCK_MAX_AGE_SECONDS}. Exiting safely."
      exit 1
    fi

    if [[ "${ALLOW_STALE_OWNER_TERMINATION}" -ne 1 ]]; then
      log "ALLOW_STALE_OWNER_TERMINATION=0: stale owner termination disabled. Exiting safely."
      exit 1
    fi

    if [[ "${SAFE_MODE}" -eq 1 ]]; then
      log "SAFE_MODE=1: stale owner termination blocked even though age threshold met. Exiting safely."
      exit 1
    fi

    log "Stale owner conditions met; attempting termination for PID=${existing_pid}"
    terminate_by_pgid_or_pid "${LOCK_PGID_FILE}" "${existing_pid}" || {
      log "WARNING: Failed to terminate stale owner pid=${existing_pid}. Exiting safely."
      exit 1
    }

    if pid_is_alive "${existing_pid}"; then
      log "WARNING: Owner PID ${existing_pid} still alive after termination attempts. Exiting safely."
      exit 1
    fi
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
log "ORDERS_RETRY_BACKOFF_MULTIPLIER=${ORDERS_RETRY_BACKOFF_MULTIPLIER} ORDERS_RETRY_MAX_DELAY_SECONDS=${ORDERS_RETRY_MAX_DELAY_SECONDS} ORDERS_RETRY_JITTER_SECONDS=${ORDERS_RETRY_JITTER_SECONDS}"
log "DAILY_MAX_ATTEMPTS=${DAILY_MAX_ATTEMPTS} DAILY_RETRY_DELAY_SECONDS=${DAILY_RETRY_DELAY_SECONDS}"
log "MTD_SAME_DAY_MAX_ATTEMPTS=${MTD_SAME_DAY_MAX_ATTEMPTS} MTD_SAME_DAY_RETRY_DELAY_SECONDS=${MTD_SAME_DAY_RETRY_DELAY_SECONDS}"
log "PENDING_MAX_ATTEMPTS=${PENDING_MAX_ATTEMPTS} PENDING_RETRY_DELAY_SECONDS=${PENDING_RETRY_DELAY_SECONDS}"
log "DAILY_RESCUE_AFTER_PENDING_SUCCESS=${DAILY_RESCUE_AFTER_PENDING_SUCCESS} DAILY_RESCUE_MAX_ATTEMPTS=${DAILY_RESCUE_MAX_ATTEMPTS} DAILY_RESCUE_RETRY_DELAY_SECONDS=${DAILY_RESCUE_RETRY_DELAY_SECONDS}"
log "ORDERS_SYNC_PROFILER_FAIL_ON_FAILED_STATUS=${ORDERS_SYNC_PROFILER_FAIL_ON_FAILED_STATUS}"
log "ORDERS_STEP_TIMEOUT_SECONDS=${ORDERS_STEP_TIMEOUT_SECONDS} DAILY_SALES_STEP_TIMEOUT_SECONDS=${DAILY_SALES_STEP_TIMEOUT_SECONDS} PENDING_DELIVERIES_STEP_TIMEOUT_SECONDS=${PENDING_DELIVERIES_STEP_TIMEOUT_SECONDS}"
log "LOCAL_LOCK_DIR=${RUN_LOCK_DIR}"
log "poetry=$(command -v poetry || echo NOT_FOUND)"
log "shell_pid=$$"
log "parent_pid=${PPID:-unknown}"
log "hostname=$(hostname)"

acquire_lock_with_wait "$@"

extract_report_date_from_cmd() {
  local step_cmd="$1"
  local report_date="<default>"
  if [[ "${step_cmd}" =~ --report-date[[:space:]]+([^[:space:]]+) ]]; then
    report_date="${BASH_REMATCH[1]}"
  fi
  echo "${report_date}"
}

run_step() {
  local step_name="$1"
  local step_cmd="$2"
  local max_attempts="${3:-1}"
  local retry_delay_seconds="${4:-5}"
  local retry_jitter_seconds="${5:-0}"
  local retry_backoff_multiplier="${6:-1}"
  local retry_max_delay_seconds="${7:-${retry_delay_seconds}}"
  local runtime_limit_seconds="${8:-0}"
  local step_start
  local step_end
  local duration
  local attempt=1
  local rc=0
  local attempt_log_file
  local report_date
  local sleep_seconds
  local jitter_seconds
  local child_pid
  local child_pgid
  local now
  local timed_out
  report_date="$(extract_report_date_from_cmd "${step_cmd}")"

  if ! [[ "${max_attempts}" =~ ^[0-9]+$ ]] || [[ "${max_attempts}" -lt 1 ]]; then
    max_attempts=1
  fi
  if ! [[ "${retry_delay_seconds}" =~ ^[0-9]+$ ]]; then
    retry_delay_seconds=5
  fi
  if ! [[ "${retry_jitter_seconds}" =~ ^[0-9]+$ ]]; then
    retry_jitter_seconds=0
  fi
  if ! [[ "${retry_backoff_multiplier}" =~ ^[0-9]+$ ]] || [[ "${retry_backoff_multiplier}" -lt 1 ]]; then
    retry_backoff_multiplier=1
  fi
  if ! [[ "${retry_max_delay_seconds}" =~ ^[0-9]+$ ]]; then
    retry_max_delay_seconds="${retry_delay_seconds}"
  fi
  if ! [[ "${runtime_limit_seconds}" =~ ^[0-9]+$ ]]; then
    runtime_limit_seconds=0
  fi

  section "Running ${step_name}"
  log "Command: ${step_cmd}"
  log "Attempts configured: ${max_attempts}; retry_delay_seconds=${retry_delay_seconds}; retry_backoff_multiplier=${retry_backoff_multiplier}; retry_max_delay_seconds=${retry_max_delay_seconds}; retry_jitter_seconds=${retry_jitter_seconds}; runtime_limit_seconds=${runtime_limit_seconds}"

  while [[ "${attempt}" -le "${max_attempts}" ]]; do
    log "${step_name}: attempt ${attempt}/${max_attempts} starting (report_date=${report_date}, regenerate=true)"
    step_start="$(date +%s)"

    attempt_log_file="$(mktemp "${LOCK_DIR}/cron_step_attempt.XXXXXX.log")"
    rc=0

    python3 -c 'import os, sys; os.setsid(); os.execvp("bash", ["bash", "-c", sys.argv[1]])' "${step_cmd}" > "${attempt_log_file}" 2>&1 &
    child_pid=$!
    child_pgid="${child_pid}"
    timed_out=0
    log "${step_name}: child_pid=${child_pid} child_pgid=${child_pgid} runtime_limit_seconds=${runtime_limit_seconds}"
    while pid_is_alive "${child_pid}"; do
      now="$(date +%s)"
      duration=$((now - step_start))
      if [[ "${runtime_limit_seconds}" -gt 0 && "${duration}" -ge "${runtime_limit_seconds}" ]]; then
        timed_out=1
        rc=124
        log "ERROR: ${step_name}: attempt ${attempt}/${max_attempts} exceeded runtime_limit_seconds=${runtime_limit_seconds}; terminating child_pid=${child_pid} child_pgid=${child_pgid}"
        terminate_child_process_group "${child_pgid}" "${child_pid}" || true
        break
      fi
      sleep 0.1
    done
    if [[ "${timed_out}" -eq 0 ]]; then
      wait "${child_pid}" || rc=$?
    else
      wait "${child_pid}" 2>/dev/null || true
      printf '%s\n' "step_runtime_timeout runtime_limit_seconds=${runtime_limit_seconds} child_pid=${child_pid} child_pgid=${child_pgid}" >> "${attempt_log_file}"
    fi

    if [[ "${rc}" -eq 0 ]]; then
      cat "${attempt_log_file}" >> "${LOG_FILE}"
      rm -f "${attempt_log_file}" 2>/dev/null || true
      step_end="$(date +%s)"
      duration=$((step_end - step_start))
      log "${step_name}: attempt ${attempt}/${max_attempts} succeeded in ${duration}s"
      return 0
    else
      cat "${attempt_log_file}" >> "${LOG_FILE}"

      if [[ "${timed_out}" -eq 1 ]]; then
        log "WARNING: ${step_name}: failure_class=step_runtime_timeout; retrying if attempts remain (exit_code=${rc})."
      elif is_deterministic_code_error "${attempt_log_file}"; then
        step_end="$(date +%s)"
        duration=$((step_end - step_start))
        log "ERROR: ${step_name}: failure_class=deterministic_environment_or_cli_error; retry_skipped=true; deterministic code, environment, or CLI error detected; failing fast without retries (exit_code=${rc}, duration=${duration}s)."
        log "ERROR: ${step_name}: retry_skipped_reason=deterministic_environment_or_cli_error"
        rm -f "${attempt_log_file}" 2>/dev/null || true
        return "${rc}"
      fi
      if [[ "${timed_out}" -eq 0 ]]; then
        log_retryable_failure_classification "${step_name}" "${attempt_log_file}" "${rc}"
      fi
      rm -f "${attempt_log_file}" 2>/dev/null || true
    fi
    step_end="$(date +%s)"
    duration=$((step_end - step_start))
    log "WARNING: ${step_name}: attempt ${attempt}/${max_attempts} failed with exit_code=${rc} after ${duration}s"

    attempt=$((attempt + 1))
    if [[ "${attempt}" -le "${max_attempts}" ]]; then
      sleep_seconds="${retry_delay_seconds}"
      if [[ "${retry_max_delay_seconds}" -gt 0 && "${sleep_seconds}" -gt "${retry_max_delay_seconds}" ]]; then
        sleep_seconds="${retry_max_delay_seconds}"
      fi
      jitter_seconds=0
      if [[ "${retry_jitter_seconds}" -gt 0 ]]; then
        jitter_seconds=$((RANDOM % (retry_jitter_seconds + 1)))
      fi
      sleep_seconds=$((sleep_seconds + jitter_seconds))
      log "${step_name}: sleeping ${sleep_seconds}s before retry (base_delay_seconds=${retry_delay_seconds}, jitter_seconds=${jitter_seconds}, next_base_delay_seconds=$((retry_delay_seconds * retry_backoff_multiplier)))"
      sleep "${sleep_seconds}"
      retry_delay_seconds=$((retry_delay_seconds * retry_backoff_multiplier))
      if [[ "${retry_max_delay_seconds}" -gt 0 && "${retry_delay_seconds}" -gt "${retry_max_delay_seconds}" ]]; then
        retry_delay_seconds="${retry_max_delay_seconds}"
      fi
    fi
  done

  log "ERROR: ${step_name} failed after ${max_attempts} attempts"
  return "${rc}"
}

log_retryable_failure_classification() {
  local step_name="$1"
  local output_file="$2"
  local rc="$3"

  if [[ ! -f "${output_file}" ]]; then
    log "WARNING: ${step_name}: failure_class=transient_or_unknown; no attempt output available; retrying if attempts remain (exit_code=${rc})."
    return 0
  fi

  if output_matches_pattern "${output_file}" "overall_status[=\": ]+failed|persisted.*overall_status.*failed|final profiler overall_status.*failed"; then
    log "WARNING: ${step_name}: failure_class=persisted_profiler_failed_status; persisted profiler overall_status=failed; retrying if attempts remain (exit_code=${rc})."
    return 0
  fi

  if output_matches_pattern "${output_file}" "Playwright|TimeoutError|Navigation timeout|net::ERR_|ERR_NAME_NOT_RESOLVED|ERR_CONNECTION|ERR_TIMED_OUT|Target page, context or browser has been closed|browser has disconnected"; then
    log "WARNING: ${step_name}: failure_class=transient_playwright_navigation_failure; transient Playwright/navigation failure detected; retrying if attempts remain (exit_code=${rc})."
    return 0
  fi

  log "WARNING: ${step_name}: failure_class=transient_or_unknown; retrying if attempts remain (exit_code=${rc})."
}

output_matches_pattern() {
  local output_file="$1"
  local pattern="$2"

  if command -v rg >/dev/null 2>&1; then
    rg -qi "${pattern}" "${output_file}"
  else
    grep -Eiq "${pattern}" "${output_file}"
  fi
}

is_deterministic_code_error() {
  local output_file="$1"

  if [[ ! -f "${output_file}" ]]; then
    return 1
  fi

  local deterministic_error_pattern
  deterministic_error_pattern="TypeError|SyntaxError|ImportError|ModuleNotFoundError|UndefinedFunctionError|UndefinedColumnError|psycopg2\\.errors\\.UndefinedFunction|psycopg2\\.errors\\.UndefinedColumn|sqlalchemy\\.exc\\.ProgrammingError|\\bProgrammingError\\b|unbound variable|usage: app|error: unrecognized arguments|No such file or directory|Poetry could not find a pyproject\.toml"

  if output_matches_pattern "${output_file}" "${deterministic_error_pattern}"; then
    return 0
  fi

  return 1
}


run_orders_connectivity_preflight() {
  local preflight_script="./scripts/orders_sync_connectivity_preflight.sh"
  local preflight_log_file
  local preflight_start
  local preflight_end
  local duration
  local rc=0

  if [[ "${ORDERS_SYNC_SKIP_CONNECTIVITY_PREFLIGHT:-0}" = "1" ]]; then
    section "Skipping orders sync tcp_connectivity_preflight"
    ORDERS_SYNC_PREFLIGHT_CLASSIFICATION="skipped"
    log "ORDERS_SYNC_SKIP_CONNECTIVITY_PREFLIGHT=1; assuming caller intentionally bypassed cron preflight."
    return 0
  fi

  section "Running orders sync tcp_connectivity_preflight"
  log "Command: ${preflight_script}"
  preflight_start="$(date +%s)"
  preflight_log_file="$(mktemp "${LOCK_DIR}/orders_sync_connectivity_preflight.XXXXXX.log")"

  if bash "${preflight_script}" > "${preflight_log_file}" 2>&1; then
    rc=0
  else
    rc=$?
  fi

  cat "${preflight_log_file}" >> "${LOG_FILE}"
  ORDERS_SYNC_PREFLIGHT_CLASSIFICATION="$(sed -n 's/.*orders_sync_preflight_summary classification=\([^ ]*\).*/\1/p' "${preflight_log_file}" | tail -n 1)"
  if [[ -z "${ORDERS_SYNC_PREFLIGHT_CLASSIFICATION}" ]]; then
    if [[ "${rc}" -eq 0 ]]; then
      ORDERS_SYNC_PREFLIGHT_CLASSIFICATION="legacy_success"
    else
      ORDERS_SYNC_PREFLIGHT_CLASSIFICATION="legacy_failed"
    fi
  fi
  rm -f "${preflight_log_file}" 2>/dev/null || true

  preflight_end="$(date +%s)"
  duration=$((preflight_end - preflight_start))

  if [[ "${rc}" -ne 0 ]]; then
    log "ERROR: failure_class=connectivity_preflight_failure; orders sync tcp_connectivity_preflight failed with classification=${ORDERS_SYNC_PREFLIGHT_CLASSIFICATION} exit_code=${rc} after ${duration}s; skipping orders_sync_run_profiler before Playwright launch."
    return "${rc}"
  fi

  log "orders sync tcp_connectivity_preflight completed with classification=${ORDERS_SYNC_PREFLIGHT_CLASSIFICATION} in ${duration}s"
  return 0
}

extract_orders_sync_observability() {
  local jsonl_file="${JSON_LOG_FILE:-${REPO_ROOT}/logs/simplify_downloader.jsonl}"
  local parse_output
  local run_id=""
  local overall_status="unknown"
  local failed_stores=""

  if [[ ! -f "${jsonl_file}" ]]; then
    log "WARNING: orders_sync observability skipped: jsonl file not found at ${jsonl_file}"
    log "orders_sync_failed_stores=[]"
    log "orders_sync_overall_status=unknown"
    return 0
  fi

  parse_output="$(
    LOG_FILE_PATH="${LOG_FILE}" python3 - "${jsonl_file}" <<'PY'
import json
import os
import re
import sys
from pathlib import Path
jsonl_path = Path(sys.argv[1])
log_path = Path(os.environ.get("LOG_FILE_PATH", ""))
SUMMARY_PHASE = "summary"
SUMMARY_MESSAGE = "Orders sync profiler summary"
run_id_pattern = re.compile(r'"run_id"\s*:\s*"([^"]+)"')


def _parse_json_line(raw):
    stripped = raw.strip()
    if not stripped or not stripped.startswith("{"):
        return None
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _is_summary_event(event, run_id=None):
    if str(event.get("phase") or "") != SUMMARY_PHASE:
        return False
    if str(event.get("message") or "") != SUMMARY_MESSAGE:
        return False
    if run_id is not None and str(event.get("run_id") or "") != run_id:
        return False
    return True


def _status_count(payload, status):
    try:
        return int(payload.get(status) or 0)
    except (TypeError, ValueError):
        return 0


def _store_failed(store_summary):
    status_counts = store_summary.get("status_counts")
    if isinstance(status_counts, dict) and _status_count(status_counts, "failed") > 0:
        return True
    status = str(
        store_summary.get("overall_status")
        or store_summary.get("status")
        or store_summary.get("store_outcome")
        or ""
    ).strip().lower()
    return status == "failed"


def _failed_stores_from_summary(summary_event):
    failed_stores = []
    store_totals = summary_event.get("store_totals")
    if isinstance(store_totals, dict):
        for raw_store_code, raw_store_summary in store_totals.items():
            store_code = str(raw_store_code or "").strip()
            if not store_code or not isinstance(raw_store_summary, dict):
                continue
            if _store_failed(raw_store_summary) and store_code not in failed_stores:
                failed_stores.append(store_code)
    return failed_stores


def _cron_step_events():
    events = []
    fallback_run_id = ""
    if not log_path.exists():
        return events, fallback_run_id

    in_orders_section = False
    with log_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if "Running Script 1: orders_sync_run_profiler" in line:
                in_orders_section = True
            elif in_orders_section and "Running Script 2:" in line:
                break
            if not in_orders_section:
                continue
            event = _parse_json_line(line)
            if event is not None:
                events.append(event)
            if '"run_id"' in line:
                match = run_id_pattern.search(line)
                if match:
                    fallback_run_id = match.group(1)
    return events, fallback_run_id


cron_events, fallback_run_id = _cron_step_events()
summary_event = next((event for event in reversed(cron_events) if _is_summary_event(event)), None)
run_id = str(summary_event.get("run_id") or "").strip() if summary_event else fallback_run_id

if not run_id:
    print("run_id=")
    print("overall_status=unknown")
    print("failed_stores=")
    raise SystemExit(0)

# The cron log is scoped to the current wrapper run, so prefer its summary event
# over the append-only JSONL file. Fall back to JSONL only when the summary line
# was not captured in stdout/stderr.
if summary_event is None:
    with jsonl_path.open("r", encoding="utf-8") as fh:
        for raw in fh:
            event = _parse_json_line(raw)
            if event is None:
                continue
            if _is_summary_event(event, run_id):
                summary_event = event

overall_status = "unknown"
failed_stores = []

if summary_event is not None:
    status = str(summary_event.get("overall_status") or "").strip().lower()
    if status:
        overall_status = status
    failed_stores = _failed_stores_from_summary(summary_event)

print(f"run_id={run_id}")
print(f"overall_status={overall_status}")
print("failed_stores=" + ",".join(failed_stores))
PY
  )"

  while IFS='=' read -r key value; do
    case "${key}" in
      run_id) run_id="${value}" ;;
      overall_status) overall_status="${value}" ;;
      failed_stores) failed_stores="${value}" ;;
    esac
  done <<< "${parse_output}"

  if [[ -z "${run_id}" ]]; then
    log "WARNING: orders_sync observability could not resolve profiler run_id from Script 1 output"
  fi

  local failed_store_array="[]"
  if [[ -n "${failed_stores}" ]]; then
    failed_store_array="[${failed_stores}]"
  fi

  ORDERS_SYNC_PROFILER_RUN_ID="${run_id}"
  ORDERS_SYNC_PROFILER_STATUS="${overall_status}"

  log "orders_sync_profiler_run_id=${run_id:-unknown}"
  log "orders_sync_preflight_classification=${ORDERS_SYNC_PREFLIGHT_CLASSIFICATION}"
  log "orders_sync_failed_stores=${failed_store_array}"
  log "orders_sync_overall_status=${overall_status}"

  if [[ "${overall_status}" = "failed" || -n "${failed_stores}" ]]; then
    if [[ "${overall_status}" = "failed" ]]; then
      log "ERROR: failure_class=persisted_profiler_failed_status; persisted profiler overall_status=failed for run_id=${run_id:-unknown}."
    fi
    log "ERROR: ORDERS SYNC WARNING: profiler run_id=${run_id:-unknown} overall_status=${overall_status} failed_stores=${failed_store_array} (script_rc=${orders_rc})"
  fi
}

orders_rc=0
orders_sync_report_args=""
daily_report_cmd="./scripts/run_local_reports_daily_sales.sh"
pending_report_cmd="./scripts/run_local_reports_pending_deliveries.sh"
daily_rc=0
pending_rc=0
daily_rescue_rc=0
run_started_epoch="$(date +%s)"

# ORDERS_SYNC_PROFILER_FAIL_ON_FAILED_STATUS=1 makes this step return non-zero
# for persisted overall_status="failed"; keep recording orders_rc while allowing
# the report pipeline steps to run.
if run_orders_connectivity_preflight; then
  run_step "Script 1: orders_sync_run_profiler" "ORDERS_SYNC_PROFILER_FAIL_ON_FAILED_STATUS=${ORDERS_SYNC_PROFILER_FAIL_ON_FAILED_STATUS} ORDERS_SYNC_SKIP_CONNECTIVITY_PREFLIGHT=1 ./scripts/orders_sync_run_profiler.sh" "${ORDERS_MAX_ATTEMPTS}" "${ORDERS_RETRY_DELAY_SECONDS}" "${ORDERS_RETRY_JITTER_SECONDS}" "${ORDERS_RETRY_BACKOFF_MULTIPLIER}" "${ORDERS_RETRY_MAX_DELAY_SECONDS}" "${ORDERS_STEP_TIMEOUT_SECONDS}" || orders_rc=$?
else
  orders_rc=$?
  log "Script 1: orders_sync_run_profiler skipped because tcp_connectivity_preflight failed (classification=${ORDERS_SYNC_PREFLIGHT_CLASSIFICATION}, orders_sync_run_profiler_rc=${orders_rc})."
fi
extract_orders_sync_observability
orders_sync_upstream_status="${ORDERS_SYNC_PROFILER_STATUS:-unknown}"
if [[ "${orders_rc}" -ne 0 ]]; then
  orders_sync_upstream_status="failed"
fi
orders_sync_report_args=""
if [[ "${orders_rc}" -ne 0 || "${orders_sync_upstream_status}" != "unknown" || -n "${ORDERS_SYNC_PROFILER_RUN_ID}" ]]; then
  orders_sync_report_args="--orders-sync-upstream-status ${orders_sync_upstream_status}"
  if [[ -n "${ORDERS_SYNC_PROFILER_RUN_ID}" ]]; then
    orders_sync_report_args="${orders_sync_report_args} --orders-sync-upstream-run-id ${ORDERS_SYNC_PROFILER_RUN_ID}"
  fi
fi
if [[ -n "${orders_sync_report_args}" ]]; then
  daily_report_cmd="${daily_report_cmd} ${orders_sync_report_args}"
  pending_report_cmd="${pending_report_cmd} ${orders_sync_report_args}"
fi
log "orders_sync_downstream_report_args=${orders_sync_report_args:-<none>}"
run_step "Script 2: daily_sales_report" "${daily_report_cmd}" "${DAILY_MAX_ATTEMPTS}" "${DAILY_RETRY_DELAY_SECONDS}" 0 1 "${DAILY_RETRY_DELAY_SECONDS}" "${DAILY_SALES_STEP_TIMEOUT_SECONDS}" || daily_rc=$?
# Pending Deliveries must not skip due to a prior successful summary; report CLIs always regenerate.
run_step "Script 3: pending_deliveries" "${pending_report_cmd}" "${PENDING_MAX_ATTEMPTS}" "${PENDING_RETRY_DELAY_SECONDS}" 0 1 "${PENDING_RETRY_DELAY_SECONDS}" "${PENDING_DELIVERIES_STEP_TIMEOUT_SECONDS}" || pending_rc=$?

if [[ "${pending_rc}" -eq 0 && "${daily_rc}" -ne 0 && "${DAILY_RESCUE_AFTER_PENDING_SUCCESS}" -eq 1 ]]; then
  section "OPTIONAL DAILY RESCUE PASS"
  log "Pending deliveries succeeded while daily sales failed; running optional daily rescue pass."
  run_step \
    "Script 2B: daily_sales_report_rescue" \
    "${daily_report_cmd}" \
    "${DAILY_RESCUE_MAX_ATTEMPTS}" \
    "${DAILY_RESCUE_RETRY_DELAY_SECONDS}" \
    0 \
    1 \
    "${DAILY_RESCUE_RETRY_DELAY_SECONDS}" \
    "${DAILY_SALES_STEP_TIMEOUT_SECONDS}" || daily_rescue_rc=$?

  if [[ "${daily_rescue_rc}" -eq 0 ]]; then
    log "Daily rescue pass succeeded; preserving original daily_sales_report_rc=${daily_rc} for required-step status."
  else
    log "WARNING: Daily rescue pass failed with rc=${daily_rescue_rc}."
  fi
fi

run_finished_epoch="$(date +%s)"
run_duration_seconds=$((run_finished_epoch - run_started_epoch))

log "Report regeneration mode: daily_sales_regenerate=true pending_deliveries_regenerate=true (report CLIs always regenerate; --force is not required)"

section "RUN STATUS SUMMARY"
log "orders_sync_run_profiler_rc=${orders_rc}"
log "orders_sync_preflight_classification=${ORDERS_SYNC_PREFLIGHT_CLASSIFICATION}"
log "orders_sync_profiler_fail_on_failed_status=${ORDERS_SYNC_PROFILER_FAIL_ON_FAILED_STATUS}"
log "daily_sales_report_rc=${daily_rc}"
log "pending_deliveries_rc=${pending_rc}"
log "daily_sales_report_rescue_rc=${daily_rescue_rc}"
log "total_duration_seconds=${run_duration_seconds}"

if [[ "${orders_rc}" -ne 0 || "${daily_rc}" -ne 0 || "${pending_rc}" -ne 0 ]]; then
  log "ERROR: One or more required cron steps failed (orders_sync_run_profiler_rc=${orders_rc}, daily_sales_report_rc=${daily_rc}, pending_deliveries_rc=${pending_rc})."
  exit 1
fi

section "CRON RUN FINISHED SUCCESSFULLY"
exit 0
