#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# cron_run_td_leads_sync.sh
#
# macOS Big Sur compatible, production-grade cron wrapper for:
#   scripts/run_local_td_leads_sync.sh
#
# Exact invocation syntax:
#   bash scripts/cron_run_td_leads_sync.sh
#
# Runtime flow semantics:
# - Acquires only its pipeline-specific lock (`tmp/cron_run_td_leads_sync.lock`).
# - If a younger live TD-leads owner holds the lock, this wrapper logs
#   status=skipped_due_to_active_same_pipeline_owner and exits 0 immediately.
# - Aged owners are recovered only after strict repository-wrapper and PID/PGID
#   validation; ambiguous locks are preserved for operator inspection.
# - Runs `scripts/run_local_td_leads_sync.sh`, which executes:
#     poetry run python -m app crm td-leads-sync
# - All stdout/stderr is written to:
#     logs/cron_run_td_leads_sync_<YYYY-mm-dd_HH-MM-SS>.log
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
RUN_LOCK_DIR="${LOCK_DIR}/cron_run_td_leads_sync.lock"

TIMESTAMP="$(date '+%Y-%m-%d_%H-%M-%S')"
LOG_FILE="${LOG_DIR}/cron_run_td_leads_sync_${TIMESTAMP}.log"

LOCK_PID_FILE="${RUN_LOCK_DIR}/pid"
LOCK_STARTED_AT_FILE="${RUN_LOCK_DIR}/started_at"
LOCK_STARTED_AT_EPOCH_FILE="${RUN_LOCK_DIR}/started_at_epoch"
LOCK_HOST_FILE="${RUN_LOCK_DIR}/host"
LOCK_CWD_FILE="${RUN_LOCK_DIR}/cwd"
LOCK_CMD_FILE="${RUN_LOCK_DIR}/command"
LOCK_PGID_FILE="${RUN_LOCK_DIR}/pgid"

KILL_WAIT_SECONDS="${KILL_WAIT_SECONDS:-5}"
TD_LEADS_MAX_RUNTIME_SECONDS="${TD_LEADS_MAX_RUNTIME_SECONDS:-300}"
# Deprecated compatibility override: when explicitly set, the legacy generic
# variable wins for this invocation. Prefer TD_LEADS_MAX_RUNTIME_SECONDS.
if [[ -n "${MAX_RUNTIME_SECONDS+x}" ]]; then
  TD_LEADS_MAX_RUNTIME_SECONDS="${MAX_RUNTIME_SECONDS}"
fi
TD_LEADS_STALE_OWNER_SECONDS="${TD_LEADS_STALE_OWNER_SECONDS:-300}"
TD_LEADS_WRAPPER_NOTIFICATION_TIMEOUT_SECONDS="${TD_LEADS_WRAPPER_NOTIFICATION_TIMEOUT_SECONDS:-30}"
TD_LEADS_NETWORK_PREFLIGHT_TIMEOUT_SECONDS="${TD_LEADS_NETWORK_PREFLIGHT_TIMEOUT_SECONDS:-5}"
STALE_OWNER_TERM_WAIT_SECONDS="${STALE_OWNER_TERM_WAIT_SECONDS:-5}"
STALE_OWNER_KILL_WAIT_SECONDS="${STALE_OWNER_KILL_WAIT_SECONDS:-5}"
CRON_HOME="${CRON_HOME:-${HOME:-/tmp}}"
CRON_PATH="${CRON_PATH:-/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin}"

if ! [[ "${TD_LEADS_MAX_RUNTIME_SECONDS}" =~ ^[0-9]+$ ]]; then
  TD_LEADS_MAX_RUNTIME_SECONDS=300
fi
if ! [[ "${TD_LEADS_STALE_OWNER_SECONDS}" =~ ^[0-9]+$ ]]; then
  TD_LEADS_STALE_OWNER_SECONDS=300
fi
if ! [[ "${TD_LEADS_WRAPPER_NOTIFICATION_TIMEOUT_SECONDS}" =~ ^[1-9][0-9]*$ ]]; then
  TD_LEADS_WRAPPER_NOTIFICATION_TIMEOUT_SECONDS=30
fi
if ! [[ "${STALE_OWNER_TERM_WAIT_SECONDS}" =~ ^[0-9]+$ ]]; then
  STALE_OWNER_TERM_WAIT_SECONDS=5
fi
if ! [[ "${STALE_OWNER_KILL_WAIT_SECONDS}" =~ ^[0-9]+$ ]]; then
  STALE_OWNER_KILL_WAIT_SECONDS=5
fi

mkdir -p "${LOG_DIR}" "${LOCK_DIR}"
cd "${REPO_ROOT}"

export HOME="${CRON_HOME}"
export PATH="${CRON_PATH}:${PATH}"
export LANG="${LANG:-en_US.UTF-8}"

RUN_LOCK_ACQUIRED=0
TIMEOUT_HANDLING_IN_PROGRESS=0
STALE_OWNER_RECOVERED=0
RECOVERED_OWNER_PID="unknown"
RECOVERED_OWNER_PGID="unknown"
RECOVERED_OWNER_AGE_SECONDS="unknown"

notify_wrapper_event() {
  local resulting_status="$1"
  local owner_pid="${2:-unknown}"
  local owner_pgid="${3:-unknown}"
  local owner_age_seconds="${4:-unknown}"
  local recovery_action="${5:-none}"
  local helper_module="${REPO_ROOT}/app/crm_downloader/td_leads_sync/wrapper_notifications.py"
  local output_file="${LOCK_DIR}/td_leads_wrapper_notification_$$.$(date +%s).log"
  local helper_pid
  local helper_pgid
  local helper_status=0
  local helper_started_at
  local duration
  local now
  local output

  if [[ ! -f "${helper_module}" ]]; then
    log "[wrapper notification] status=${resulting_status} delivery=skipped helper_unavailable=${helper_module}"
    return 0
  fi

  # Alert persistence and SMTP are best-effort. Keep them in an isolated process
  # group so neither a hung helper nor one of its descendants can retain this lock.
  helper_started_at="$(date +%s)"
  python3 -c 'import os, sys; os.setsid(); os.execvp(sys.argv[1], sys.argv[1:])' \
    poetry run python -m app.crm_downloader.td_leads_sync.wrapper_notifications \
    --wrapper-timestamp "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    --hostname "$(hostname)" \
    --local-lock-path "${RUN_LOCK_DIR}" \
    --owner-pid "${owner_pid}" \
    --owner-pgid "${owner_pgid}" \
    --owner-age-seconds "${owner_age_seconds}" \
    --recovery-action "${recovery_action}" \
    --status "${resulting_status}" > "${output_file}" 2>&1 &
  helper_pid=$!
  helper_pgid="${helper_pid}"

  while pid_is_alive "${helper_pid}"; do
    now="$(date +%s)"
    duration=$((now - helper_started_at))
    if [[ "${duration}" -ge "${TD_LEADS_WRAPPER_NOTIFICATION_TIMEOUT_SECONDS}" ]]; then
      log "WARNING: [wrapper notification] status=${resulting_status} delivery=timeout timeout_seconds=${TD_LEADS_WRAPPER_NOTIFICATION_TIMEOUT_SECONDS} helper_pid=${helper_pid} helper_pgid=${helper_pgid}"
      if terminate_child_process_group "${helper_pgid}" "${helper_pid}"; then
        log "[wrapper notification] status=${resulting_status} timeout_process_group_verification=success helper_pgid=${helper_pgid}"
      else
        log "WARNING: [wrapper notification] status=${resulting_status} timeout_process_group_verification=failure helper_pgid=${helper_pgid}; continuing lock-safe cleanup"
      fi
      wait "${helper_pid}" 2>/dev/null || true
      output="$(cat "${output_file}" 2>/dev/null || true)"
      rm -f "${output_file}" 2>/dev/null || true
      log "WARNING: [wrapper notification] status=${resulting_status} delivery=best_effort_abandoned result=${output}"
      return 0
    fi
    sleep 1
  done

  wait "${helper_pid}" || helper_status=$?
  output="$(cat "${output_file}" 2>/dev/null || true)"
  rm -f "${output_file}" 2>/dev/null || true
  if [[ "${helper_status}" -eq 0 ]]; then
    log "[wrapper notification] status=${resulting_status} delivery=success result=${output}"
  else
    log "WARNING: [wrapper notification] status=${resulting_status} delivery=failure helper_status=${helper_status} result=${output}"
  fi
  return 0
}

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
  local state
  [[ -n "${pid}" ]] || return 1
  state="$(ps -o state= -p "${pid}" 2>/dev/null | awk '{$1=$1; print}')"
  [[ -n "${state}" ]] && [[ "${state}" != Z* ]]
}

process_group_is_alive() {
  local pgid="$1"
  [[ -n "${pgid}" ]] || return 1
  # A killed child can remain as a zombie until its external parent reaps it.
  # Zombies cannot execute work, so only non-zombie group members retain a lock.
  ps -axo pgid=,state= 2>/dev/null | awk -v expected_pgid="${pgid}" '
    $1 == expected_pgid && $2 !~ /^Z/ { found=1 }
    END { exit(found ? 0 : 1) }
  '
}

get_pid_command_line() {
  local pid="$1"
  ps -o command= -p "${pid}" 2>/dev/null | awk '{$1=$1; print}'
}

get_pid_pgid() {
  local pid="$1"
  ps -o pgid= -p "${pid}" 2>/dev/null | awk '{$1=$1; print}'
}

calculate_lock_age_seconds() {
  local started_epoch="$1"
  local now_epoch

  if [[ -z "${started_epoch}" ]] || ! [[ "${started_epoch}" =~ ^[0-9]+$ ]]; then
    return 1
  fi
  now_epoch="$(date +%s)"
  if [[ "${started_epoch}" -gt "${now_epoch}" ]]; then
    return 1
  fi
  echo $((now_epoch - started_epoch))
}

command_references_expected_wrapper() {
  local command_line="$1"
  local expected_script="$2"
  [[ " ${command_line} " == *" ${expected_script} "* ]] || [[ "${command_line}" == "${expected_script}" ]] || [[ "${command_line}" == "${expected_script} "* ]]
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
  get_pid_pgid "$$" > "${LOCK_PGID_FILE}"
}

acquire_fresh_lock() {
  if mkdir "${RUN_LOCK_DIR}" 2>/dev/null; then
    write_lock_metadata "$@"
    RUN_LOCK_ACQUIRED=1
    log "[local lock] Lock acquired successfully. PID=$$"
    return 0
  fi
  return 1
}

log_existing_lock_metadata() {
  local existing_pid="$1"
  local existing_pgid="$2"
  local existing_cmd="$3"
  local existing_host="$4"
  local existing_started_at="$5"
  local lock_age_seconds="$6"

  section "Existing local lock detected, inspecting ownership"
  log "[local lock] metadata pid=${existing_pid:-<missing>} pgid=${existing_pgid:-<missing>} command=${existing_cmd:-<missing>} host=${existing_host:-<missing>} started_at=${existing_started_at:-<missing>} lock_age_seconds=${lock_age_seconds:-unknown}"
}

terminate_stale_owner_group() {
  local pgid="$1"
  local i

  log "[local lock] Sending TERM to stale-owner process group PGID=${pgid}"
  kill -TERM "-${pgid}" 2>/dev/null || true
  for ((i=0; i<STALE_OWNER_TERM_WAIT_SECONDS; i++)); do
    if ! process_group_is_alive "${pgid}"; then
      log "[local lock] Stale-owner process group PGID=${pgid} exited after TERM"
      return 0
    fi
    sleep 1
  done

  if process_group_is_alive "${pgid}"; then
    log "[local lock] Process group PGID=${pgid} survived TERM; sending KILL"
    kill -KILL "-${pgid}" 2>/dev/null || true
  fi
  for ((i=0; i<STALE_OWNER_KILL_WAIT_SECONDS; i++)); do
    if ! process_group_is_alive "${pgid}"; then
      log "[local lock] Stale-owner process group PGID=${pgid} exited after KILL"
      return 0
    fi
    sleep 1
  done

  if process_group_is_alive "${pgid}"; then
    log "WARNING: Stale-owner process group PGID=${pgid} is still alive after KILL. Leaving lock untouched."
    return 1
  fi
  return 0
}

reacquire_after_stale_cleanup() {
  local stale_pid="$1"
  local stale_pgid="$2"
  local current_pid
  local current_pgid
  shift 2

  if [[ -d "${RUN_LOCK_DIR}" ]]; then
    current_pid="$(safe_cat "${LOCK_PID_FILE}")"
    current_pgid="$(safe_cat "${LOCK_PGID_FILE}")"
    if [[ "${current_pid}" != "${stale_pid}" ]] || [[ "${current_pgid}" != "${stale_pgid}" ]]; then
      log "WARNING: Local-lock ownership changed during stale cleanup: expected_pid=${stale_pid} expected_pgid=${stale_pgid} current_pid=${current_pid:-<missing>} current_pgid=${current_pgid:-<missing>}. Leaving lock untouched."
      exit 1
    fi
    rm -rf "${RUN_LOCK_DIR}"
  fi
  if ! acquire_fresh_lock "$@"; then
    log "WARNING: Failed to reacquire local lock after stale cleanup. Exiting safely."
    exit 1
  fi
  log "[local lock] Fresh lock acquired after stale cleanup. PID=$$"
}

acquire_local_lock() {
  local existing_pid
  local existing_pgid
  local existing_started_at
  local existing_started_epoch
  local existing_host
  local existing_cmd
  local lock_age_seconds
  local live_pgid
  local live_cmd
  local expected_script="${SCRIPT_DIR}/cron_run_td_leads_sync.sh"

  if acquire_fresh_lock "$@"; then
    return 0
  fi

  existing_pid="$(safe_cat "${LOCK_PID_FILE}")"
  existing_pgid="$(safe_cat "${LOCK_PGID_FILE}")"
  existing_started_at="$(safe_cat "${LOCK_STARTED_AT_FILE}")"
  existing_started_epoch="$(safe_cat "${LOCK_STARTED_AT_EPOCH_FILE}")"
  existing_host="$(safe_cat "${LOCK_HOST_FILE}")"
  existing_cmd="$(safe_cat "${LOCK_CMD_FILE}")"
  lock_age_seconds="$(calculate_lock_age_seconds "${existing_started_epoch}" 2>/dev/null || true)"
  log_existing_lock_metadata "${existing_pid}" "${existing_pgid}" "${existing_cmd}" "${existing_host}" "${existing_started_at}" "${lock_age_seconds}"

  if [[ -z "${existing_pid}" ]] || ! [[ "${existing_pid}" =~ ^[0-9]+$ ]] || [[ -z "${existing_pgid}" ]] || ! [[ "${existing_pgid}" =~ ^[0-9]+$ ]]; then
    log "WARNING: Local-lock PID/PGID metadata is missing or malformed. Leaving lock untouched."
    notify_wrapper_event "lock_metadata_ambiguous" "${existing_pid:-unknown}" "${existing_pgid:-unknown}" "${lock_age_seconds:-unknown}" "preserved_lock_for_operator_inspection"
    exit 1
  fi

  if ! pid_is_alive "${existing_pid}"; then
    if process_group_is_alive "${existing_pgid}"; then
      log "WARNING: Local-lock owner PID=${existing_pid} is gone but PGID=${existing_pgid} is still alive. Leaving lock untouched."
      notify_wrapper_event "lock_metadata_ambiguous" "${existing_pid}" "${existing_pgid}" "${lock_age_seconds:-unknown}" "preserved_lock_for_operator_inspection"
      exit 1
    fi
    log "[local lock] Owner PID=${existing_pid} and PGID=${existing_pgid} are gone; removing stale lock."
    reacquire_after_stale_cleanup "${existing_pid}" "${existing_pgid}" "$@"
    return 0
  fi

  if [[ -z "${lock_age_seconds}" ]]; then
    log "WARNING: Live local-lock owner has missing, invalid, or future started_at_epoch metadata. Leaving lock untouched."
    notify_wrapper_event "lock_metadata_ambiguous" "${existing_pid}" "${existing_pgid}" "unknown" "preserved_lock_for_operator_inspection"
    exit 1
  fi

  if [[ "${lock_age_seconds}" -lt "${TD_LEADS_STALE_OWNER_SECONDS}" ]]; then
    log "[local lock] status=skipped_due_to_active_same_pipeline_owner owner_pid=${existing_pid} owner_pgid=${existing_pgid} lock_age_seconds=${lock_age_seconds} stale_owner_seconds=${TD_LEADS_STALE_OWNER_SECONDS}"
    notify_wrapper_event "skipped_due_to_active_same_pipeline_owner" "${existing_pid}" "${existing_pgid}" "${lock_age_seconds}" "suppressed_overlapping_invocation"
    exit 0
  fi

  live_pgid="$(get_pid_pgid "${existing_pid}")"
  live_cmd="$(get_pid_command_line "${existing_pid}")"
  if [[ "${live_pgid}" != "${existing_pgid}" ]]; then
    log "WARNING: PID/PGID mismatch for stale-owner candidate PID=${existing_pid}: metadata_pgid=${existing_pgid} live_pgid=${live_pgid:-<missing>}. Leaving lock untouched."
    notify_wrapper_event "lock_metadata_ambiguous" "${existing_pid}" "${existing_pgid}" "${lock_age_seconds}" "preserved_lock_for_operator_inspection"
    exit 1
  fi
  if ! command_references_expected_wrapper "${existing_cmd}" "${expected_script}" || ! command_references_expected_wrapper "${live_cmd}" "${expected_script}"; then
    log "WARNING: Stale-owner command does not belong to expected repository wrapper. expected=${expected_script} metadata_command=${existing_cmd:-<missing>} live_command=${live_cmd:-<missing>}. Leaving lock untouched."
    notify_wrapper_event "lock_metadata_ambiguous" "${existing_pid}" "${existing_pgid}" "${lock_age_seconds}" "preserved_lock_for_operator_inspection"
    exit 1
  fi

  log "[local lock] Validated stale same-pipeline owner PID=${existing_pid} PGID=${existing_pgid}; starting process-group recovery."
  if ! terminate_stale_owner_group "${existing_pgid}"; then
    exit 1
  fi
  if pid_is_alive "${existing_pid}" || process_group_is_alive "${existing_pgid}"; then
    log "WARNING: Stale-owner process group verification failed for PID=${existing_pid} PGID=${existing_pgid}. Leaving lock untouched."
    notify_wrapper_event "lock_metadata_ambiguous" "${existing_pid}" "${existing_pgid}" "${lock_age_seconds}" "preserved_lock_for_operator_inspection"
    exit 1
  fi

  log "[local lock] Confirmed stale-owner process group is gone; removing stale lock."
  STALE_OWNER_RECOVERED=1
  RECOVERED_OWNER_PID="${existing_pid}"
  RECOVERED_OWNER_PGID="${existing_pgid}"
  RECOVERED_OWNER_AGE_SECONDS="${lock_age_seconds}"
  reacquire_after_stale_cleanup "${existing_pid}" "${existing_pgid}" "$@"
  # Reacquire first: best-effort alert persistence/SMTP must not delay lock recovery.
  notify_wrapper_event "stale_owner_terminated" "${existing_pid}" "${existing_pgid}" "${lock_age_seconds}" "terminated_stale_owner_process_group"
}

terminate_child_process_group() {
  local pgid="$1"
  local child_pid="$2"
  local i

  log "Attempting graceful termination for child process group PGID=${pgid} (child_pid=${child_pid})"
  kill -TERM "-${pgid}" 2>/dev/null || true
  for ((i=0; i<KILL_WAIT_SECONDS; i++)); do
    if ! process_group_is_alive "${pgid}"; then
      log "Confirmed child process group PGID=${pgid} disappeared after TERM"
      return 0
    fi
    sleep 1
  done

  if process_group_is_alive "${pgid}"; then
    log "Child process group PGID=${pgid} still has non-zombie members after ${KILL_WAIT_SECONDS}s; sending KILL"
    kill -KILL "-${pgid}" 2>/dev/null || true
  fi
  for ((i=0; i<KILL_WAIT_SECONDS; i++)); do
    if ! process_group_is_alive "${pgid}"; then
      log "Confirmed child process group PGID=${pgid} disappeared after KILL"
      return 0
    fi
    sleep 1
  done

  if process_group_is_alive "${pgid}"; then
    log "WARNING: Child process group PGID=${pgid} still has non-zombie members after KILL"
    return 1
  fi
  log "Confirmed child process group PGID=${pgid} disappeared after KILL"
}

cleanup() {
  local exit_code="$1"
  local trap_name="$2"

  if [[ "${exit_code}" -eq 0 ]]; then
    log "Run completed successfully via trap=${trap_name}"
  else
    log "Run exiting with non-zero status=${exit_code} via trap=${trap_name}"
  fi

  if [[ "${RUN_LOCK_ACQUIRED}" -eq 1 ]]; then
    if [[ "${TIMEOUT_HANDLING_IN_PROGRESS}" -eq 1 ]]; then
      log "WARNING: Timeout process-group handling is still in progress; preserving local lock for safe recovery."
    else
      remove_lock_artifacts
    fi
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
log "TD_LEADS_STALE_OWNER_SECONDS=${TD_LEADS_STALE_OWNER_SECONDS}"
log "TD_LEADS_WRAPPER_NOTIFICATION_TIMEOUT_SECONDS=${TD_LEADS_WRAPPER_NOTIFICATION_TIMEOUT_SECONDS}"
log "TD_LEADS_NETWORK_PREFLIGHT_TIMEOUT_SECONDS=${TD_LEADS_NETWORK_PREFLIGHT_TIMEOUT_SECONDS}"
log "STALE_OWNER_TERM_WAIT_SECONDS=${STALE_OWNER_TERM_WAIT_SECONDS}"
log "STALE_OWNER_KILL_WAIT_SECONDS=${STALE_OWNER_KILL_WAIT_SECONDS}"
log "TD_LEADS_MAX_RUNTIME_SECONDS=${TD_LEADS_MAX_RUNTIME_SECONDS}"
log "LOCAL_LOCK_DIR=${RUN_LOCK_DIR}"
log "poetry=$(command -v poetry || echo NOT_FOUND)"
log "shell_pid=$$"
log "parent_pid=${PPID:-unknown}"
log "hostname=$(hostname)"

acquire_local_lock "$@"

run_step() {
  local step_name="$1"
  shift
  local step_cmd=("$@")
  local step_start
  local step_end
  local duration

  section "Running ${step_name}"
  log "Command: $(printf '%q ' "${step_cmd[@]}")"

  step_start="$(date +%s)"
  python3 -c 'import os, sys; os.setsid(); os.execvp(sys.argv[1], sys.argv[1:])' "${step_cmd[@]}" >> "${LOG_FILE}" 2>&1 &
  local child_pid=$!
  local child_pgid="${child_pid}"
  local child_status=0
  log "${step_name}: child_pid=${child_pid} child_pgid=${child_pgid} runtime_limit_seconds=${TD_LEADS_MAX_RUNTIME_SECONDS}"
  local now

  while pid_is_alive "${child_pid}"; do
    now="$(date +%s)"
    duration=$((now - step_start))
    if [[ "${TD_LEADS_MAX_RUNTIME_SECONDS}" -gt 0 && "${duration}" -ge "${TD_LEADS_MAX_RUNTIME_SECONDS}" ]]; then
      log "ERROR: ${step_name} exceeded TD_LEADS_MAX_RUNTIME_SECONDS=${TD_LEADS_MAX_RUNTIME_SECONDS}s; terminating child_pid=${child_pid} child_pgid=${child_pgid}"
      TIMEOUT_HANDLING_IN_PROGRESS=1
      if terminate_child_process_group "${child_pgid}" "${child_pid}"; then
        wait "${child_pid}" 2>/dev/null || true
      else
        log "WARNING: ${step_name}: child process group PGID=${child_pgid} did not disappear; preserving local lock for safe recovery."
        RUN_LOCK_ACQUIRED=0
      fi
      TIMEOUT_HANDLING_IN_PROGRESS=0
      notify_wrapper_event "watchdog_timeout" "${child_pid}" "${child_pgid}" "${duration}" "terminated_watchdog_child_process_group"
      return 124
    fi
    sleep 1
  done

  wait "${child_pid}" || child_status=$?
  step_end="$(date +%s)"
  duration=$((step_end - step_start))

  if [[ "${child_status}" -ne 0 ]]; then
    log "${step_name} failed with status=${child_status} after ${duration}s"
    return "${child_status}"
  fi

  log "${step_name} completed successfully in ${duration}s"
}

TD_LEADS_ARGS=()
td_leads_args_count=0
for raw_arg in "$@"; do
  if [[ "${raw_arg}" == reporting_mode=* ]]; then
    reporting_mode_value="${raw_arg#reporting_mode=}"
    if [[ "${reporting_mode_value}" == "meeting" || "${reporting_mode_value}" == "day_end" ]]; then
      TD_LEADS_ARGS+=("--reporting-mode" "${reporting_mode_value}")
      td_leads_args_count=$((td_leads_args_count + 2))
    else
      log "WARNING: Invalid reporting_mode '${reporting_mode_value}' ignored (allowed: meeting|day_end)"
    fi
  else
    TD_LEADS_ARGS+=("${raw_arg}")
    td_leads_args_count=$((td_leads_args_count + 1))
  fi
done

td_leads_args_values=""
if [[ ${td_leads_args_count} -gt 0 ]]; then
  td_leads_args_values="$(printf '%q ' "${TD_LEADS_ARGS[@]}")"
fi


run_td_leads_network_preflight() {
  local preflight_status=0

  section "Running TD leads network preflight"
  set +e
  poetry run python -m app.crm_downloader.td_leads_sync.network_preflight \
    --timeout-seconds "${TD_LEADS_NETWORK_PREFLIGHT_TIMEOUT_SECONDS}" >> "${LOG_FILE}" 2>&1
  preflight_status=$?
  set -e

  case "${preflight_status}" in
    0)
      log "TD leads network preflight passed; browser launch allowed and notifications healthy"
      return 0
      ;;
    20)
      log "WARNING: TD leads CRM DNS/TCP preflight failed; browser launch skipped after operational summary persistence"
      section "CRON RUN SKIPPED BY TD LEADS NETWORK PREFLIGHT"
      exit 0
      ;;
    30)
      log "WARNING: TD leads SMTP DNS/TCP preflight failed; continuing data sync with notification delivery degraded"
      export TD_LEADS_NOTIFICATION_PREFLIGHT_DEGRADED=1
      export TD_LEADS_NOTIFICATION_PREFLIGHT_DEGRADED_REASON="smtp_dns_tcp_preflight_failed"
      return 0
      ;;
    *)
      log "ERROR: TD leads network preflight failed with unexpected status=${preflight_status}"
      return "${preflight_status}"
      ;;
  esac
}

log "Parsed td_leads args count=${td_leads_args_count} values=${td_leads_args_values}"
run_td_leads_network_preflight
if [[ ${td_leads_args_count} -gt 0 ]]; then
  run_step "Script 1: td_leads_sync" "./scripts/run_local_td_leads_sync.sh" "${TD_LEADS_ARGS[@]}"
else
  run_step "Script 1: td_leads_sync" "./scripts/run_local_td_leads_sync.sh"
fi

# A healthy completion probes for an unresolved same-lock incident because the
# helper owns DB-backed incident state. The bounded helper suppresses this event
# when no alert edge is open, so ordinary runs do not email or risk lock retention.
if [[ "${STALE_OWNER_RECOVERED}" -eq 1 ]]; then
  notify_wrapper_event "fresh_run_succeeded_after_stale_owner_terminated" "${RECOVERED_OWNER_PID}" "${RECOVERED_OWNER_PGID}" "${RECOVERED_OWNER_AGE_SECONDS}" "fresh_td_leads_run_completed_successfully"
else
  notify_wrapper_event "fresh_run_succeeded_after_stale_owner_terminated" "$$" "$(get_pid_pgid "$$")" "0" "fresh_td_leads_run_completed_successfully"
fi

section "CRON RUN FINISHED SUCCESSFULLY"
exit 0
