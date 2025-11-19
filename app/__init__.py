"""Application package bootstrap."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

_PACKAGE_ROOT = Path(__file__).resolve().parent

_ALIAS_MODULES = {
    "dashboard_downloader": "app.dashboard_downloader",
    "crm_downloader": "app.crm_downloader",
    "common": "app.common",
    "tsv_dashboard": "app.tsv_dashboard",
}

for alias, target in _ALIAS_MODULES.items():
    if alias in sys.modules:
        continue
    sys.modules[alias] = importlib.import_module(target)

if str(_PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(_PACKAGE_ROOT))