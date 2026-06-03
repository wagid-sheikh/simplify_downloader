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
  cat <<EOF
Usage: $0 [--force] {td-leads|orders-reports|orders-report} [FORCE=1]

Inspect or recover a stale pipeline lock. Default mode is dry-run: the script
prints metadata and process snapshots without terminating anything. Use --force,
environment FORCE=1, or trailing FORCE=1 only when you intend to terminate
the recorded process group.

Valid pipelines:
  td-leads        Inspect/recover tmp/cron_run_td_leads_sync.lock
  orders-reports  Inspect/recover tmp/cron_run_orders_and_reports.lock
                  Alias accepted: orders-report

Options:
  -h, --help      Show this help message
  --force         Terminate the process group with TERM, then KILL if needed,
                  and remove the lock only after the group is gone

Examples:
  $0 td-leads
  $0 orders-reports
  $0 orders-report
  $0 --force orders-reports
  $0 orders-reports --force
  FORCE=1 $0 orders-reports
  $0 orders-reports FORCE=1
EOF
}

PIPELINE_NAME=""
print_pipeline_correction() {
  local typed_name="$1"
  local corrected_name="$typed_name"

  case "$corrected_name" in
    orders-report)
      corrected_name="orders-reports"
      ;;
  esac

  case "$corrected_name" in
    td-leads|orders-reports)
      echo "Use: ./scripts/inspect_or_kill_pipeline_stale.sh $corrected_name" >&2
      ;;
  esac
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --force)
      FORCE="1"
      ;;
    FORCE=1)
      FORCE="1"
      ;;
    --*)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
    Pipeline=*|PIPELINE=*|pipeline=*)
      echo "Do not pass the pipeline as an environment-style assignment: $1" >&2
      print_pipeline_correction "${1#*=}"
      usage >&2
      exit 2
      ;;
    *)
      if [ -n "$PIPELINE_NAME" ]; then
        echo "Unexpected argument: $1" >&2
        usage >&2
        exit 2
      fi
      PIPELINE_NAME="$1"
      ;;
  esac
  shift
done

if [ -z "$PIPELINE_NAME" ]; then
  if [ -n "${Pipeline:-}" ]; then
    echo "Do not pass the pipeline as an environment variable: Pipeline=$Pipeline" >&2
    print_pipeline_correction "$Pipeline"
  elif [ -n "${PIPELINE:-}" ]; then
    echo "Do not pass the pipeline as an environment variable: PIPELINE=$PIPELINE" >&2
    print_pipeline_correction "$PIPELINE"
  elif [ -n "${pipeline:-}" ]; then
    echo "Do not pass the pipeline as an environment variable: pipeline=$pipeline" >&2
    print_pipeline_correction "$pipeline"
  else
    echo "Missing pipeline name." >&2
  fi
  usage >&2
  exit 2
fi

case "$PIPELINE_NAME" in
  orders-report)
    PIPELINE_NAME="orders-reports"
    ;;
esac

case "$PIPELINE_NAME" in
  td-leads)
    PIPELINE_LOCK_DIR="$TMP_DIR/cron_run_td_leads_sync.lock"
    ;;
  orders-reports)
    PIPELINE_LOCK_DIR="$TMP_DIR/cron_run_orders_and_reports.lock"
    ;;
  *)
    echo "Unknown pipeline: $PIPELINE_NAME" >&2
    usage >&2
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

process_cwd() {
  local pid="$1"
  readlink "/proc/$pid/cwd" 2>/dev/null || true
}

path_belongs_to_repo() {
  local path="$1"
  [ "$path" = "$REPO_ROOT" ] || [[ "$path" == "$REPO_ROOT"/* ]]
}

command_mentions_repo() {
  local command="$1"
  [[ "$command" == *"$REPO_ROOT/"* ]]
}

process_belongs_to_repo() {
  local pid="$1"
  local command="$2"
  local cwd

  if command_mentions_repo "$command"; then
    return 0
  fi

  cwd="$(process_cwd "$pid")"
  [ -n "$cwd" ] && path_belongs_to_repo "$cwd"
}

is_orders_reports_primary_command() {
  local pid="$1"
  local command="$2"

  if [[ "$command" == *"cron_run_orders_and_reports.sh"* ]] || \
     [[ "$command" == *"orders_sync_run_profiler.sh"* ]]; then
    process_belongs_to_repo "$pid" "$command"
    return $?
  fi

  if [[ "$command" == *"python"* ]] && [[ "$command" == *" -m app"* ]] && \
     { [[ "$command" == *"orders sync"* ]] || \
       [[ "$command" == *"orders-sync"* ]] || \
       [[ "$command" == *"orders_sync"* ]] || \
       [[ "$command" == *"profiler"* ]]; }; then
    process_belongs_to_repo "$pid" "$command"
    return $?
  fi

  return 1
}

is_likely_browser_command() {
  local command="$1"
  case "$command" in
    *playwright*|*chromium*|*chrome*|*Google\ Chrome*|*msedge*|*firefox*|*webkit*)
      return 0
      ;;
  esac
  return 1
}

print_orphan_candidates() {
  local candidates_file="$1"

  echo "PID PPID PGID ELAPSED COMMAND"
  sort -n -k1,1 "$candidates_file" | while IFS='|' read -r pid ppid pgid etime command; do
    [ -n "$pid" ] || continue
    printf '%s %s %s %s %s\n' "$pid" "$ppid" "$pgid" "$etime" "$command"
  done
}

inspect_orders_reports_orphans() {
  local candidates_file primary_pgids_file all_processes_file tmp_line
  local pid ppid pgid etime command

  candidates_file="$(mktemp "${TMPDIR:-/tmp}/orders_reports_orphans.XXXXXX")"
  primary_pgids_file="$(mktemp "${TMPDIR:-/tmp}/orders_reports_orphan_pgids.XXXXXX")"
  all_processes_file="$(mktemp "${TMPDIR:-/tmp}/orders_reports_processes.XXXXXX")"

  ps -axo pid=,ppid=,pgid=,etime=,command= >"$all_processes_file"

  while read -r pid ppid pgid etime command; do
    [ -n "${pid:-}" ] || continue
    if is_orders_reports_primary_command "$pid" "${command:-}"; then
      printf '%s|%s|%s|%s|%s\n' "$pid" "$ppid" "$pgid" "$etime" "$command" >>"$candidates_file"
      printf '%s\n' "$pgid" >>"$primary_pgids_file"
    fi
  done <"$all_processes_file"

  sort -u -o "$primary_pgids_file" "$primary_pgids_file"

  while read -r pid ppid pgid etime command; do
    [ -n "${pid:-}" ] || continue
    if is_likely_browser_command "${command:-}" && \
       grep -qx "$pgid" "$primary_pgids_file" && \
       process_belongs_to_repo "$pid" "${command:-}"; then
      tmp_line="${pid}|${ppid}|${pgid}|${etime}|${command}"
      grep -Fqx "$tmp_line" "$candidates_file" || printf '%s\n' "$tmp_line" >>"$candidates_file"
    fi
  done <"$all_processes_file"

  if [ ! -s "$candidates_file" ]; then
    rm -f "$candidates_file" "$primary_pgids_file" "$all_processes_file"
    return 0
  fi

  echo "No lock found, but possible orphaned orders/reports processes exist"
  print_orphan_candidates "$candidates_file"

  if [ "$FORCE" != "1" ]; then
    echo "Dry run only: FORCE!=1, no orphan termination executed."
    rm -f "$candidates_file" "$primary_pgids_file" "$all_processes_file"
    return 0
  fi

  echo "FORCE=1: terminating validated orphan process groups."
  cut -d'|' -f3 "$candidates_file" | sort -u | while read -r pgid; do
    [ -n "$pgid" ] || continue
    if ! [[ "$pgid" =~ ^[1-9][0-9]*$ ]]; then
      echo "Skipping malformed orphan PGID [$pgid]."
      continue
    fi
    echo "Sending TERM to orphan process group -$pgid"
    kill -TERM "-$pgid" 2>/dev/null || true
  done
  sleep "$TERM_WAIT_SECONDS"

  cut -d'|' -f3 "$candidates_file" | sort -u | while read -r pgid; do
    [ -n "$pgid" ] || continue
    if pgid_alive "$pgid"; then
      echo "Orphan PGID $pgid still alive; sending KILL to process group -$pgid"
      kill -KILL "-$pgid" 2>/dev/null || true
    fi
  done
  sleep "$KILL_WAIT_SECONDS"
  rm -f "$candidates_file" "$primary_pgids_file" "$all_processes_file"
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

if [ -d "$PIPELINE_LOCK_DIR" ]; then
  inspect_or_kill_process_group "$PIPELINE_LOCK_DIR"
else
  echo "No active/stale lock found for $PIPELINE_NAME at ${PIPELINE_LOCK_DIR#$REPO_ROOT/}."
  if [ "$PIPELINE_NAME" = "orders-reports" ]; then
    inspect_orders_reports_orphans
  fi
fi

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
