#!/usr/bin/env bash

set -u

DRY_RUN="${DRY_RUN:-1}"
FORCE="${FORCE:-0}"
TERM_WAIT_SECONDS="${TERM_WAIT_SECONDS:-5}"
KILL_WAIT_SECONDS="${KILL_WAIT_SECONDS:-1}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$REPO_ROOT/tmp"
OBSOLETE_GLOBAL_LOCK_DIR="$TMP_DIR/cron_heavy_pipelines.lock"

usage() {
  echo "Usage: $0 {td-leads|orders-reports}" >&2
}

if [ "$#" -ne 1 ]; then
  usage
  exit 2
fi

PIPELINE_NAME="$1"
case "$PIPELINE_NAME" in
  td-leads)
    PIPELINE_LOCK_DIR="$TMP_DIR/cron_run_td_leads_sync.lock"
    ;;
  orders-reports)
    PIPELINE_LOCK_DIR="$TMP_DIR/cron_run_orders_and_reports.lock"
    ;;
  *)
    echo "Unknown pipeline: $PIPELINE_NAME" >&2
    usage
    exit 2
    ;;
esac

print_snapshot() {
  local label="$1"
  local pgid="$2"
  echo "$label"
  ps -axo pid=,ppid=,pgid=,stat=,etime=,command= | awk -v pgid="$pgid" '$3 == pgid'
}

read_required_metadata() {
  local lock_dir="$1"
  local metadata_name="$2"
  local metadata_file="$lock_dir/$metadata_name"
  local value=""

  if [ ! -f "$metadata_file" ]; then
    echo "Skipping: missing metadata file $metadata_file." >&2
    return 1
  fi

  value="$(cat "$metadata_file" 2>/dev/null)" || {
    echo "Skipping: unreadable metadata file $metadata_file." >&2
    return 1
  }

  if [ -z "$value" ] || [[ "$value" == *$'\n'* ]]; then
    echo "Skipping: malformed metadata file $metadata_file." >&2
    return 1
  fi

  printf '%s' "$value"
}

is_valid_target_command() {
  local command="$1"
  [[ "$command" == *"$REPO_ROOT/scripts/"* ]] || [[ "$command" == *"$REPO_ROOT/app/"* ]]
}

pgid_alive() {
  local pgid="$1"
  # Zombies cannot be terminated and should not keep a stale lock directory alive.
  ps -axo pgid=,stat= | awk -v pgid="$pgid" '$1 == pgid && $2 !~ /^Z/ { found=1 } END { exit !found }'
}

pid_matches_group() {
  local pid="$1"
  local expected_pgid="$2"
  local actual_pgid=""

  actual_pgid="$(ps -o pgid= -p "$pid" 2>/dev/null | awk '{$1=$1; print}')"
  [ "$actual_pgid" = "$expected_pgid" ]
}

pid_command() {
  local pid="$1"
  ps -o command= -p "$pid" 2>/dev/null
}

remove_lock_if_group_gone() {
  local lock_dir="$1"
  local pgid="$2"

  if pgid_alive "$pgid"; then
    echo "Process group $pgid is still present; lock directory not removed."
    return 1
  fi

  echo "Process group $pgid is gone; removing lock directory."
  rm -rf "$lock_dir"
}

inspect_or_kill_process_group() {
  local lock_dir="$1"
  local pid pgid command started_at host cwd live_command

  [ -d "$lock_dir" ] || return 0

  echo
  echo "Lock: $lock_dir"

  pid="$(read_required_metadata "$lock_dir" pid)" || return 0
  pgid="$(read_required_metadata "$lock_dir" pgid)" || return 0
  command="$(read_required_metadata "$lock_dir" command)" || return 0
  started_at="$(read_required_metadata "$lock_dir" started_at)" || return 0
  host="$(read_required_metadata "$lock_dir" host)" || return 0
  cwd="$(read_required_metadata "$lock_dir" cwd)" || return 0

  echo "Metadata: pid=$pid pgid=$pgid started_at=$started_at host=$host cwd=$cwd"
  echo "Command: $command"

  if ! [[ "$pid" =~ ^[1-9][0-9]*$ ]]; then
    echo "Skipping: non-numeric PID [$pid]."
    return 0
  fi

  if ! [[ "$pgid" =~ ^[1-9][0-9]*$ ]]; then
    echo "Skipping: non-numeric PGID [$pgid]."
    return 0
  fi

  if ! [[ "$started_at" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}[[:space:]][0-9]{2}:[0-9]{2}:[0-9]{2}[[:space:]][^[:space:]]+$ ]]; then
    echo "Skipping: malformed start time [$started_at]."
    return 0
  fi

  if ! [[ "$host" =~ ^[^[:space:]]+$ ]]; then
    echo "Skipping: malformed host [$host]."
    return 0
  fi

  if [[ "$cwd" != /* ]]; then
    echo "Skipping: malformed working directory [$cwd]."
    return 0
  fi

  print_snapshot "Before snapshot for PGID $pgid:" "$pgid"

  if ! is_valid_target_command "$command"; then
    echo "Skipping: lock command does not belong to repository $REPO_ROOT."
    print_snapshot "After snapshot for PGID $pgid:" "$pgid"
    return 0
  fi

  if ! pgid_alive "$pgid"; then
    if [ "$FORCE" = "1" ]; then
      remove_lock_if_group_gone "$lock_dir" "$pgid"
    else
      echo "Dry run only: process group $pgid is gone; stale lock directory retained."
    fi
    print_snapshot "After snapshot for PGID $pgid:" "$pgid"
    return 0
  fi

  if ! kill -0 "$pid" 2>/dev/null; then
    echo "Skipping: PGID $pgid is active but owner PID $pid is not alive."
    print_snapshot "After snapshot for PGID $pgid:" "$pgid"
    return 0
  fi

  if ! pid_matches_group "$pid" "$pgid"; then
    echo "Skipping: owner PID $pid does not belong to PGID $pgid."
    print_snapshot "After snapshot for PGID $pgid:" "$pgid"
    return 0
  fi

  live_command="$(pid_command "$pid")"
  if ! is_valid_target_command "$live_command"; then
    echo "Skipping: live owner PID $pid command does not belong to repository $REPO_ROOT."
    echo "Live command: ${live_command:-<missing>}"
    print_snapshot "After snapshot for PGID $pgid:" "$pgid"
    return 0
  fi

  if [ "$FORCE" != "1" ]; then
    echo "Dry run only: FORCE!=1, no termination executed."
    print_snapshot "After snapshot for PGID $pgid:" "$pgid"
    return 0
  fi

  echo "Sending TERM to process group -$pgid"
  kill -TERM "-$pgid" 2>/dev/null || true
  sleep "$TERM_WAIT_SECONDS"

  if pgid_alive "$pgid"; then
    echo "PGID $pgid still alive; sending KILL to process group -$pgid"
    kill -KILL "-$pgid" 2>/dev/null || true
    sleep "$KILL_WAIT_SECONDS"
  fi

  remove_lock_if_group_gone "$lock_dir" "$pgid" || true
  print_snapshot "After snapshot for PGID $pgid:" "$pgid"
}

echo "Pipeline=$PIPELINE_NAME DRY_RUN=$DRY_RUN FORCE=$FORCE"
if [ "$DRY_RUN" = "1" ] && [ "$FORCE" = "1" ]; then
  echo "FORCE=1 overrides dry run and allows termination."
fi

inspect_or_kill_process_group "$PIPELINE_LOCK_DIR"

# Rollout cleanup only: wrappers no longer create this directory. Keep this
# explicit path until deployed hosts have removed any legacy global lock.
if [ -d "$OBSOLETE_GLOBAL_LOCK_DIR" ]; then
  echo
  echo "Obsolete global lock rollout cleanup: $OBSOLETE_GLOBAL_LOCK_DIR"
  inspect_or_kill_process_group "$OBSOLETE_GLOBAL_LOCK_DIR"
fi

if [ "$FORCE" = "1" ]; then
  find "$TMP_DIR" -maxdepth 1 -name 'cron_step_attempt.*.log' -type f -delete 2>/dev/null || true
fi
