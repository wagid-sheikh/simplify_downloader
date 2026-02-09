"""UC orders sync orchestrator package."""

from app.crm_downloader.uc_orders_sync.archive_ingest import ingest_uc_archive_excels

__all__ = ["ingest_uc_archive_excels"]
