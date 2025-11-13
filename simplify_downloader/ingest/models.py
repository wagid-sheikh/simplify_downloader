"""Compatibility wrapper for ingestion models."""

from simplify_downloader.common.ingest.models import (
    Base,
    BUCKET_MODEL_MAP,
    MissedLead,
    RepeatCustomer,
    UndeliveredOrder,
)

__all__ = [
    "Base",
    "BUCKET_MODEL_MAP",
    "MissedLead",
    "RepeatCustomer",
    "UndeliveredOrder",
]
