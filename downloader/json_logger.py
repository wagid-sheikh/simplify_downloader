# downloader/json_logger.py
from __future__ import annotations
import json, sys, time
from typing import Any, Dict, Optional

# Levels: DEBUG < INFO < WARN < ERROR
class JsonLogger:
    def __init__(self, app: str = "simplify_downloader", level: str = "INFO"):
        self.app = app
        self.level = level.upper()

    def _emit(self, level: str, event: str, **fields: Any) -> None:
        # ts in ISO-ish + epoch for easy parsing
        record = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "epoch": time.time(),
            "app": self.app,
            "level": level.upper(),
            "event": event,
        }
        record.update({k: v for k, v in fields.items() if v is not None})
        sys.stdout.write(json.dumps(record, ensure_ascii=False) + "\n")
        sys.stdout.flush()

    def debug(self, event: str, **fields: Any) -> None:
        if self.level in ("DEBUG",):
            self._emit("DEBUG", event, **fields)

    def info(self, event: str, **fields: Any) -> None:
        if self.level in ("DEBUG", "INFO"):
            self._emit("INFO", event, **fields)

    def warn(self, event: str, **fields: Any) -> None:
        if self.level in ("DEBUG", "INFO", "WARN"):
            self._emit("WARN", event, **fields)

    def error(self, event: str, **fields: Any) -> None:
        self._emit("ERROR", event, **fields)


# Exit code mapping (import and use in runner)
class ExitCodes:
    OK = 0                       # all good
    PARTIAL = 10                 # some downloads failed, but at least one succeeded
    AUTH_FAILED = 20             # could not authenticate / invalid or expired cookies
    BAD_CONFIG = 30              # missing env/paths/STORE_CODES_LIST/FILE_SPECS
    NET_TIMEOUT = 40             # repeated timeouts / network failures
    UNCAUGHT = 50                # unexpected exception
