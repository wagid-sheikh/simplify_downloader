from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping


def _clean(value: object | None) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _build_label(item: Mapping[str, object]) -> str:
    service = _clean(item.get("service_name"))
    garment = _clean(item.get("garment_name"))
    if not service and not garment:
        return ""
    if service and garment:
        return f"{service} {garment}"
    return service or garment


def _collapse_ranges(numbers: list[int]) -> str:
    parts: list[str] = []
    start = end = numbers[0]
    for num in numbers[1:]:
        if num == end + 1:
            end = num
        else:
            parts.append(str(start) if start == end else f"{start}–{end}")
            start = end = num
    parts.append(str(start) if start == end else f"{start}–{end}")
    return ", ".join(parts)


def summarize_line_items(items: Iterable[Mapping[str, object]]) -> str:
    label_counts: Counter[str] = Counter()
    for item in items:
        label = _build_label(item)
        if label:
            label_counts[label] += 1

    numbered: defaultdict[str, set[int]] = defaultdict(set)
    non_numbered_counts: Counter[str] = Counter()

    for label, count in label_counts.items():
        tokens = label.split()
        if tokens and tokens[-1].isdigit():
            base_label = " ".join(tokens[:-1]).strip()
            if base_label:
                numbered[base_label].add(int(tokens[-1]))
                continue
        non_numbered_counts[label] += count

    segments: list[str] = []
    for base_label in sorted(numbered, key=str.lower):
        nums = sorted(numbered[base_label])
        segments.append(f"{base_label} {_collapse_ranges(nums)}")

    for label in sorted(non_numbered_counts, key=str.lower):
        segments.append(f"{label} × {non_numbered_counts[label]}")

    return " | ".join(segments)
