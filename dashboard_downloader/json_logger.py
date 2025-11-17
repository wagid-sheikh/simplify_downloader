"""Structured JSON logger for the downloader and pipeline."""
from __future__ import annotations

import json
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Optional

__all__ = ["JsonLogger", "get_logger", "log_event", "timed_event", "new_run_id"]


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S%f")


def _default_log_file_path() -> str | None:
    from simplify_downloader.config import config

    raw = config.json_log_file.strip()
    return raw or None


_AUTO = object()


class JsonLogger:
    """Emit newline-delimited JSON events."""

    def __init__(
        self,
        run_id: Optional[str] = None,
        stream=None,
        *,
        log_file_path: str | None | object = _AUTO,
    ):
        self.run_id = run_id or new_run_id()
        self.stream = stream or sys.stdout
        self.default_context: Dict[str, Any] = {"run_id": self.run_id}
        if log_file_path is _AUTO:
            file_path = _default_log_file_path()
        else:
            file_path = log_file_path
        self.file_handle = open(file_path, "a", encoding="utf-8") if file_path else None
        self.aggregator = None

    def bind(self, **kwargs: Any) -> "JsonLogger":
        child = JsonLogger(run_id=self.run_id, stream=self.stream, log_file_path=None)
        child.default_context = {**self.default_context, **kwargs}
        child.file_handle = self.file_handle
        child.aggregator = self.aggregator
        return child

    def attach_aggregator(self, aggregator: Any) -> None:
        self.aggregator = aggregator

    def _emit(self, payload: Dict[str, Any]) -> None:
        event = {**self.default_context, **payload}
        event.setdefault("ts", datetime.now(timezone.utc).isoformat())
        encoded = json.dumps(event, default=str, ensure_ascii=False)
        self.stream.write(encoded + "\n")
        self.stream.flush()
        if self.file_handle:
            self.file_handle.write(encoded + "\n")
            self.file_handle.flush()

    def info(self, *, phase: str, status: str = "ok", message: str = "", **fields: Any) -> None:
        payload = {"phase": phase, "status": status, "message": message, **fields}
        if self.aggregator:
            try:
                self.aggregator.record_log_event(payload)
            except Exception:
                pass
        self._emit(payload)

    def warn(self, *, phase: str, message: str, **fields: Any) -> None:
        self.info(phase=phase, status="warn", message=message, **fields)

    def error(self, *, phase: str, message: str, **fields: Any) -> None:
        self.info(phase=phase, status="error", message=message, **fields)

    def close(self) -> None:
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None

    def __del__(self) -> None:  # pragma: no cover
        self.close()


def get_logger(run_id: Optional[str] = None) -> JsonLogger:
    return JsonLogger(run_id=run_id)


def log_event(*, logger: JsonLogger, phase: str, status: str = "ok", message: str = "", **extras: Any) -> None:
    logger.info(phase=phase, status=status, message=message, **extras)


@contextmanager
def timed_event(*, logger: JsonLogger, phase: str, message: str = "", **fields: Any) -> Iterator[None]:
    start = time.perf_counter()
    try:
        yield
        duration = int((time.perf_counter() - start) * 1000)
        logger.info(phase=phase, status="ok", message=message, duration_ms=duration, **fields)
    except Exception as exc:  # pragma: no cover
        duration = int((time.perf_counter() - start) * 1000)
        logger.error(
            phase=phase,
            message=f"{message} failed: {exc}",
            duration_ms=duration,
            extras={"exception": repr(exc)},
            **fields,
        )
        raise
