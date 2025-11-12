from __future__ import annotations

from downloader.json_logger import JsonLogger


def configure_logging(logger: JsonLogger) -> None:
    """Hook to extend logging configuration if needed."""
    _ = logger
