"""Canonical mobile-number normalization for customer retention ingestion."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class MobileNormalizationStatus(StrEnum):
    VALID = "valid"
    BLANK = "blank"
    MALFORMED = "malformed"
    UNNORMALIZABLE = "unnormalizable"


@dataclass(frozen=True)
class MobileNormalizationResult:
    raw_value: Any
    normalized_mobile: str | None
    status: MobileNormalizationStatus
    warning_code: str | None = None
    warning_message: str | None = None

    @property
    def is_valid(self) -> bool:
        return self.status == MobileNormalizationStatus.VALID


_DIGIT_RE = re.compile(r"\d+")
_REPEATED_DIGITS_RE = re.compile(r"^(\d)\1{9}$")


def normalize_mobile(value: Any) -> MobileNormalizationResult:
    """Normalize Indian mobile numbers to a 10-digit canonical value.

    The raw value is retained only in the structured result for local row logic;
    callers must not write it to logs or warning summaries.
    """

    if value is None:
        return MobileNormalizationResult(value, None, MobileNormalizationStatus.BLANK, "mobile_blank", "Mobile number is blank")
    raw_text = str(value).strip()
    if not raw_text:
        return MobileNormalizationResult(value, None, MobileNormalizationStatus.BLANK, "mobile_blank", "Mobile number is blank")

    if re.search(r"[A-Za-z]", raw_text):
        return MobileNormalizationResult(value, None, MobileNormalizationStatus.MALFORMED, "mobile_malformed", "Mobile number contains non-numeric text")

    digits = "".join(_DIGIT_RE.findall(raw_text))
    if not digits:
        return MobileNormalizationResult(value, None, MobileNormalizationStatus.MALFORMED, "mobile_malformed", "Mobile number contains no digits")

    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]
    elif len(digits) != 10:
        return MobileNormalizationResult(value, None, MobileNormalizationStatus.MALFORMED, "mobile_malformed", "Mobile number must resolve to 10 digits")

    if not digits[0] in "6789" or _REPEATED_DIGITS_RE.match(digits):
        return MobileNormalizationResult(value, None, MobileNormalizationStatus.UNNORMALIZABLE, "mobile_unnormalizable", "Mobile number cannot be normalized to a valid customer mobile")

    return MobileNormalizationResult(value, digits, MobileNormalizationStatus.VALID)
