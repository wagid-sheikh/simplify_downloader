"""Shared services for Simplify Downloader modules."""

from typing import Any

__all__ = [
    "audit_bucket",
    "cleanup_bucket",
    "run_alembic_upgrade",
    "session_scope",
    "ingest_bucket",
]


def __getattr__(name: str) -> Any:
    if name == "audit_bucket":
        from .audit import audit_bucket as _audit_bucket

        return _audit_bucket
    if name == "cleanup_bucket":
        from .cleanup import cleanup_bucket as _cleanup_bucket

        return _cleanup_bucket
    if name == "run_alembic_upgrade":
        from .db import run_alembic_upgrade as _run_alembic_upgrade

        return _run_alembic_upgrade
    if name == "session_scope":
        from .db import session_scope as _session_scope

        return _session_scope
    if name == "ingest_bucket":
        from .ingest.service import ingest_bucket as _ingest_bucket

        return _ingest_bucket
    raise AttributeError(name)
