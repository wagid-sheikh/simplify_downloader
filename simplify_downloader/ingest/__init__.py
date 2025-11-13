"""Compatibility wrapper for ingestion helpers."""

from .service import ingest_bucket
from .models import Base

__all__ = ["ingest_bucket", "Base"]
