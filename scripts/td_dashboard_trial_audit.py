#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path


def _load_events(path: Path) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            events.append(payload)
    return events


def _is_trial_event(event: dict[str, object], source_mode: str) -> bool:
    if event.get("phase") != "api":
        return False
    if event.get("source_mode") != source_mode:
        return False
    return all(
        key in event
        for key in ("trial_attempted", "trial_success", "fallback_used", "runtime_delta_ms", "context_source")
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit dashboard-only trial logs from run_log.txt")
    parser.add_argument("--run-log", default="temp_run_time_stuff/run_log.txt")
    parser.add_argument("--source-mode", default="api_only")
    parser.add_argument("--min-successful-stores", type=int, default=2)
    args = parser.parse_args()

    path = Path(args.run_log)
    events = _load_events(path)
    trial_events = [e for e in events if _is_trial_event(e, args.source_mode)]

    latest_run_id = None
    for event in reversed(trial_events):
        rid = event.get("run_id")
        if isinstance(rid, str) and rid.strip():
            latest_run_id = rid
            break

    if latest_run_id:
        trial_events = [e for e in trial_events if e.get("run_id") == latest_run_id]

    per_store: dict[str, dict[str, object]] = defaultdict(dict)
    for event in trial_events:
        store = str(event.get("store_code") or "UNKNOWN")
        per_store[store] = {
            "attempted": bool(event.get("trial_attempted")),
            "success": bool(event.get("trial_success")),
            "fallback": bool(event.get("fallback_used")),
            "runtime_delta_ms": event.get("runtime_delta_ms"),
            "context_source": event.get("context_source"),
        }

    print("| store | attempted | success | fallback | runtime_delta_ms | context_source |")
    print("|---|---:|---:|---:|---:|---|")
    for store in sorted(per_store):
        row = per_store[store]
        print(
            f"| {store} | {row['attempted']} | {row['success']} | {row['fallback']} | "
            f"{row['runtime_delta_ms']} | {row['context_source']} |"
        )

    success_count = sum(1 for row in per_store.values() if row.get("success") is True)
    print(f"\nSuccessful stores: {success_count}")
    print(f"Minimum required successful stores: {args.min_successful_stores}")
    if success_count < args.min_successful_stores:
        print("Threshold NOT met: keep default behavior unchanged.")
        return 1

    print("Threshold met: safe to consider default behavior change.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
