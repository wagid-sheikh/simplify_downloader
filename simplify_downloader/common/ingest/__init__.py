"""Ingestion package."""

from typing import Any

__all__ = ["ingest_bucket", "Base"]


def __getattr__(name: str) -> Any:
    if name == "ingest_bucket":
        from .service import ingest_bucket as _ingest_bucket

        return _ingest_bucket
    if name == "Base":
        from .models import Base as _Base

        return _Base
    raise AttributeError(name)
