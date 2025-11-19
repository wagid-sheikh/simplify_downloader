"""Application package root."""

from __future__ import annotations

import importlib
import sys
from types import ModuleType
from typing import Iterable

__all__ = ["register_legacy_aliases"]

_LEGACY_NAMESPACE_PACKAGES: tuple[str, ...] = (
    "common",
    "dashboard_downloader",
    "crm_downloader",
    "tsv_dashboard",
)


def register_legacy_aliases(packages: Iterable[str] | None = None) -> None:
    """Expose legacy top-level packages for backward compatibility."""

    names = tuple(packages) if packages is not None else _LEGACY_NAMESPACE_PACKAGES
    for name in names:
        full_name = f"{__name__}.{name}"
        module = importlib.import_module(full_name)
        if isinstance(module, ModuleType):
            sys.modules.setdefault(name, module)


register_legacy_aliases()