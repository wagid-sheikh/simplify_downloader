"""Compatibility shims for migrated dashboard pipelines.

Use :mod:`app.dashboard_downloader.pipelines` instead.
"""

from app.dashboard_downloader.pipelines import (
    base,
    dashboard_monthly,
    dashboard_weekly,
    reporting,
)

__all__ = [
    "base",
    "dashboard_monthly",
    "dashboard_weekly",
    "reporting",
]
