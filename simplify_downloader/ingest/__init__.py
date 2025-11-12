"""Ingestion package."""

from .service import ingest_bucket
from .models import Base

__all__ = ["ingest_bucket", "Base"]
