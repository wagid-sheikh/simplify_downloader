"""Shared result objects for customer retention Phase 2 ingestion."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RowWarning:
    """Non-fatal row warning without customer-sensitive raw mobile data."""

    code: str
    message: str
    row_number: int | None = None
    source_file: str | None = None
    field_name: str | None = None
    lead_id: int | None = None
    cost_center: str | None = None


@dataclass(frozen=True)
class DiscoveredInputFile:
    path: Path
    relative_path: str
    file_name: str
    file_size: int
    content_sha256: str
    identity_key: str
    file_type: str


@dataclass
class ImportBatchResult:
    source_file: str | None = None
    import_batch_id: str | None = None
    rows_seen: int = 0
    raw_rows_inserted: int = 0
    raw_rows_existing: int = 0
    leads_created: int = 0
    leads_existing: int = 0
    rows_skipped: int = 0
    warnings: list[RowWarning] = field(default_factory=list)

    @property
    def warning_count(self) -> int:
        return len(self.warnings)


@dataclass
class AdapterConversionResult:
    rows_seen: int = 0
    leads_created: int = 0
    leads_existing: int = 0
    rows_skipped: int = 0
    warnings: list[RowWarning] = field(default_factory=list)

    @property
    def warning_count(self) -> int:
        return len(self.warnings)


@dataclass
class WorkbookIngestionResult:
    source_file: str
    file_identity: str
    rows_seen: int = 0
    history_inserted: int = 0
    history_existing: int = 0
    rows_pending_not_updated: int = 0
    protected_edits_ignored: int = 0
    rows_skipped: int = 0
    warnings: list[RowWarning] = field(default_factory=list)

    @property
    def warning_count(self) -> int:
        return len(self.warnings)


JsonDict = dict[str, Any]
