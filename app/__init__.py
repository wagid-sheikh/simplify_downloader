"""Top-level package for the CRM downloader and ingestion orchestration."""

from typing import Any

__all__ = ["run_pipeline"]


def __getattr__(name: str) -> Any:
    if name == "run_pipeline":
        from app.dashboard_downloader.pipeline import run_pipeline as _run_pipeline

        return _run_pipeline
    raise AttributeError(name)
