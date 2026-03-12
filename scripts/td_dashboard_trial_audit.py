#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from statistics import mean

REQUIRED_KEYS = (
    "trial_attempted",
    "trial_success",
    "fallback_used",
    "runtime_delta_ms",
    "context_source",
)
VALID_CONTEXT_SOURCES = {"dashboard_only", "iframe_fallback"}


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
    return all(key in event for key in REQUIRED_KEYS)


def _bool(value: object) -> bool:
    return bool(value)


def _runtime_ms(value: object) -> int | None:
    if isinstance(value, (int, float)):
        return int(value)
    return None


def _pct(part: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round((part / total) * 100, 2)


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit dashboard-only trial logs from run_log.txt")
    parser.add_argument("--run-log", default="temp_run_time_stuff/run_log.txt")
    parser.add_argument("--source-mode", default="api_only")
    parser.add_argument("--min-successful-stores", type=int, default=2)
    parser.add_argument(
        "--min-store-success-rate",
        type=float,
        default=80.0,
        help="Store-level success-rate threshold (percentage) required for promotion gate",
    )
    parser.add_argument(
        "--max-runs",
        type=int,
        default=5,
        help="Only include the most recent N run_ids in the multi-run summary",
    )
    args = parser.parse_args()

    path = Path(args.run_log)
    events = _load_events(path)
    trial_events = [e for e in events if _is_trial_event(e, args.source_mode)]
    if not trial_events:
        print("No trial events found matching the requested source mode.")
        return 1

    ordered_run_ids: list[str] = []
    seen_run_ids: set[str] = set()
    for event in trial_events:
        rid = event.get("run_id")
        if isinstance(rid, str) and rid.strip() and rid not in seen_run_ids:
            seen_run_ids.add(rid)
            ordered_run_ids.append(rid)

    run_ids = ordered_run_ids[-max(1, args.max_runs) :]
    run_id_set = set(run_ids)
    filtered_events = [e for e in trial_events if e.get("run_id") in run_id_set]

    by_store: dict[str, list[dict[str, object]]] = defaultdict(list)
    for event in filtered_events:
        store = str(event.get("store_code") or "UNKNOWN")
        by_store[store].append(event)

    print(f"Runs analyzed ({len(run_ids)}): {', '.join(run_ids)}")
    print("\nPer-store trial payload key verification")
    print("| store | events | required_keys_present | context_source_values |")
    print("|---|---:|---|---|")
    for store in sorted(by_store):
        store_events = by_store[store]
        required_keys_present = all(all(key in event for key in REQUIRED_KEYS) for event in store_events)
        context_values = sorted({str(event.get("context_source")) for event in store_events})
        context_values_display = ", ".join(context_values)
        print(f"| {store} | {len(store_events)} | {required_keys_present} | {context_values_display} |")

    print("\nPer-store multi-run summary")
    print(
        "| store | attempts | success | fallback | success_rate_% | fallback_rate_% | "
        "runtime_avg_ms | runtime_min_ms | runtime_max_ms |"
    )
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|")

    store_gate_pass_count = 0
    for store in sorted(by_store):
        store_events = by_store[store]
        attempts = sum(1 for e in store_events if _bool(e.get("trial_attempted")))
        success = sum(1 for e in store_events if _bool(e.get("trial_success")))
        fallback = sum(1 for e in store_events if _bool(e.get("fallback_used")))
        runtimes = [_runtime_ms(e.get("runtime_delta_ms")) for e in store_events]
        runtime_values = [v for v in runtimes if v is not None]
        runtime_avg = round(mean(runtime_values), 2) if runtime_values else None
        runtime_min = min(runtime_values) if runtime_values else None
        runtime_max = max(runtime_values) if runtime_values else None
        success_rate = _pct(success, attempts)
        fallback_rate = _pct(fallback, attempts)

        if success_rate >= args.min_store_success_rate:
            store_gate_pass_count += 1

        print(
            f"| {store} | {attempts} | {success} | {fallback} | {success_rate} | {fallback_rate} | "
            f"{runtime_avg} | {runtime_min} | {runtime_max} |"
        )

    print("\nPromotion gate evaluation")
    print(f"- Minimum successful stores required: {args.min_successful_stores}")
    print(f"- Minimum per-store success rate required: {args.min_store_success_rate}%")
    print(f"- Stores meeting success-rate requirement: {store_gate_pass_count}")

    if store_gate_pass_count < args.min_successful_stores:
        print("Gate NOT met: keep default path unchanged.")
        return 1

    print("Gate met: eligible for default-path promotion.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
