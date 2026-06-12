"""Configurable value normalization for customer retention workbook inputs."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping

from .constants import WORKBOOK_OUTCOME_LABELS


@dataclass(frozen=True)
class ValueNormalizationResult:
    raw_value: Any
    normalized_value: str | None
    changed: bool
    invalid: bool
    warning_code: str | None = None
    warning_message: str | None = None


_PUNCT_RE = re.compile(r"[^a-z0-9]+")


def canonical_key(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = _PUNCT_RE.sub(" ", text)
    return " ".join(text.split())


DEFAULT_VALUE_MAPPINGS: dict[str, str] = {
    "whatsapp": "WhatsApp Sent",
    "whats app": "WhatsApp Sent",
    "watsapp": "WhatsApp Sent",
    "wa": "WhatsApp Sent",
    "wa sent": "WhatsApp Sent",
    "whatsapp sent": "WhatsApp Sent",
    "no resp": "No Response",
    "no response": "No Response",
    "not interested": "Not Interested",
    "dnd": "Do Not Contact",
    "do not contact": "Do Not Contact",
    "wrong no": "Wrong Number",
    "wrong number": "Wrong Number",
    "invalid": "Invalid Number",
    "invalid number": "Invalid Number",
    "pickup": "Pickup Requested",
    "pickup requested": "Pickup Requested",
    "call": "Call",
    "both": "Both",
    "not contacted": "Not Contacted",
    "yes": "Yes",
    "no": "No",
    "interested": "Interested",
}


class ValueNormalizer:
    """Central mapping service for messy user-entered values."""

    def __init__(self, mappings: Mapping[str, str] | None = None) -> None:
        source = mappings or DEFAULT_VALUE_MAPPINGS
        self._mappings = {canonical_key(key): value for key, value in source.items()}
        for label in WORKBOOK_OUTCOME_LABELS:
            self._mappings.setdefault(canonical_key(label), label)

    def normalize(self, value: Any, *, allowed_values: set[str] | tuple[str, ...] | None = None, field_name: str = "value", required: bool = False) -> ValueNormalizationResult:
        if value is None or not str(value).strip():
            if required:
                return ValueNormalizationResult(value, None, False, True, "required_blank", f"{field_name} is required")
            return ValueNormalizationResult(value, None, False, False)
        key = canonical_key(value)
        normalized = self._mappings.get(key)
        if normalized is None:
            raw_text = str(value).strip()
            candidates = set(allowed_values or ())
            if raw_text in candidates:
                normalized = raw_text
            else:
                return ValueNormalizationResult(value, raw_text, False, True, "value_unrecognized", f"{field_name} is not recognized")
        if allowed_values is not None and normalized not in set(allowed_values):
            return ValueNormalizationResult(value, normalized, normalized != str(value).strip(), True, "value_not_allowed", f"{field_name} is not allowed")
        return ValueNormalizationResult(value, normalized, normalized != str(value).strip(), False)


_default_normalizer = ValueNormalizer()


def normalize_value(value: Any, **kwargs: Any) -> ValueNormalizationResult:
    return _default_normalizer.normalize(value, **kwargs)
