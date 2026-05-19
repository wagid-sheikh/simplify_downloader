#!/usr/bin/env bash

set -u

DRY_RUN="${DRY_RUN:-1}"
FORCE="${FORCE:-0}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$REPO_ROOT/tmp"
LOCK_FILES=(
  "$TMP_DIR/cron_run_orders_and_reports.lock"
  "$TMP_DIR/cron_heavy_pipelines.lock"
)

print_snapshot() {
  local label="$1"
  local pgid="$2"
  echo "$label"
  ps -axo pid=,ppid=,pgid=,stat=,etime=,command= | awk -v pgid="$pgid" '$3 == pgid'
}

read_lock_metadata() {
  local lock_file="$1"
  local pid=""
  local pgid=""
  local command=""
  local started_at=""

  [ -f "$lock_file" ] || return 1

  while IFS='=' read -r raw_key raw_value; do
    local key value
    key="$(echo "${raw_key}" | tr '[:upper:]' '[:lower:]' | xargs)"
    value="$(echo "${raw_value}" | sed 's/^ *//;s/ *$//')"
    case "$key" in
      pid) pid="$value" ;;
      pgid) pgid="$value" ;;
      command|cmd) command="$value" ;;
      started_at|startedat) started_at="$value" ;;
    esac
  done < "$lock_file"

  [ -n "$pid" ] && [ -n "$pgid" ] || return 1
  echo "$pid|$pgid|$command|$started_at"
}

is_valid_target_command() {
  local cmd="$1"
  local repo="$2"
  [[ "$cmd" == *"$repo/scripts/cron_run_orders_and_reports.sh"* ]] && return 0
  [[ "$cmd" == *"$repo/scripts/"* ]] && return 0
  [[ "$cmd" == *"$repo/app/"* ]] && return 0
  return 1
}

pgid_alive() {
  local pgid="$1"
  ps -axo pgid= | awk -v pgid="$pgid" '$1 == pgid { found=1 } END { exit !found }'
}

kill_process_group() {
  local lock_file="$1"
  local metadata pid pgid command started_at

  metadata="$(read_lock_metadata "$lock_file")" || {
    echo "Skipping $lock_file (missing or unreadable metadata)."
    return 0
  }

  IFS='|' read -r pid pgid command started_at <<< "$metadata"
  echo
  echo "Lock: $lock_file"
  echo "Metadata: pid=$pid pgid=$pgid started_at=${started_at:-<unknown>}"

  if ! [[ "$pid" =~ ^[0-9]+$ ]]; then
    echo "Skipping: non-numeric PID [$pid]."
    return 0
  fi

  if ! [[ "$pgid" =~ ^[0-9]+$ ]]; then
    echo "Skipping: non-numeric PGID [$pgid]."
    return 0
  fi

  if ! kill -0 "$pid" 2>/dev/null; then
    echo "Skipping: PID $pid is not alive."
    return 0
  fi

  if ! is_valid_target_command "$command" "$REPO_ROOT"; then
    echo "Skipping: lock command is not a repo cron/child script command."
    echo "Command: ${command:-<missing>}"
    return 0
  fi

  print_snapshot "Before snapshot for PGID $pgid:" "$pgid"

  if [ "$FORCE" != "1" ]; then
    echo "FORCE!=1, no kill executed."
    return 0
  fi

  echo "Sending TERM to process group -$pgid"
  kill -TERM "-$pgid" 2>/dev/null || true
  sleep 5

  if pgid_alive "$pgid"; then
    echo "PGID $pgid still alive; sending KILL to process group -$pgid"
    kill -KILL "-$pgid" 2>/dev/null || true
    sleep 1
  fi

  if pgid_alive "$pgid"; then
    echo "Process group $pgid still present; lock directory not removed."
  else
    echo "Process group $pgid is gone; removing lock directory."
    rm -rf "$lock_file"
  fi

  print_snapshot "After snapshot for PGID $pgid:" "$pgid"
}

echo "DRY_RUN=$DRY_RUN FORCE=$FORCE"
if [ "$DRY_RUN" = "1" ] && [ "$FORCE" = "1" ]; then
  echo "FORCE=1 overrides dry run and allows kill."
fi

for lock_file in "${LOCK_FILES[@]}"; do
  kill_process_group "$lock_file"
done

if [ "$FORCE" = "1" ]; then
  find "$TMP_DIR" -maxdepth 1 -name 'cron_step_attempt.*.log' -type f -delete 2>/dev/null || true
fi
