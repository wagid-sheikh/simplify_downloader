"""Structured JSON logger for the downloader and pipeline."""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from contextlib import contextmanager
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

LOG_STATUSES = frozenset({"debug", "ok", "info", "warning", "error"})
DEFAULT_MAX_EVENT_BYTES = max(1024, int(os.getenv("JSON_LOG_MAX_EVENT_BYTES", "65536")))

__all__ = ["LOG_STATUSES", "JsonLogger", "get_logger", "log_event", "timed_event", "new_run_id"]

STATUS_NORMALIZED_WARNING_TOKEN = "status_normalized:"


class StatusNormalizationSuppressionFilter(logging.Filter):
    """Suppress expected status normalization warnings and track suppression counts."""

    def __init__(self) -> None:
        super().__init__(name="status_normalization_suppression")
        self._suppressed_by_file: dict[str, int] = defaultdict(int)

    def filter(self, record: logging.LogRecord) -> bool:
        event = getattr(record, "event_payload", None)
        if not isinstance(event, dict):
            return True
        if str(event.get("status", "")).strip().lower() != "warning":
            return True
        if not self._contains_status_normalization(event):
            return True
        file_key = self._event_file_key(event)
        self._suppressed_by_file[file_key] += 1
        return False

    def pop_suppressed_count(self, *, file_key: str) -> int:
        return self._suppressed_by_file.pop(file_key, 0)

    @staticmethod
    def _contains_status_normalization(event: dict[str, Any]) -> bool:
        for key in ("message", "warning_code", "ingest_remarks"):
            value = event.get(key)
            if isinstance(value, str) and STATUS_NORMALIZED_WARNING_TOKEN in value:
                return True
        return False

    @staticmethod
    def _event_file_key(event: dict[str, Any]) -> str:
        source_file = event.get("source_file")
        if isinstance(source_file, str) and source_file.strip():
            return source_file.strip()
        file_value = event.get("file")
        if isinstance(file_value, str) and file_value.strip():
            return Path(file_value).name
        return "<unknown>"


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S%f")


def _default_log_file_path() -> tuple[str | None, str | None]:
    env_value = os.getenv("JSON_LOG_FILE", "").strip()
    if env_value:
        return env_value, "env:JSON_LOG_FILE"

    from app.config import config

    raw = config.json_log_file.strip()
    return (raw or None, "config.json_log_file" if raw else None)


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
            file_path, source = _default_log_file_path()
        else:
            file_path, source = log_file_path, "explicit"
        self.log_file_path_source: str | None = source
        self.log_file_path = self._resolve_path(file_path)
        self.file_handle = (
            open(self.log_file_path, "a", encoding="utf-8") if self.log_file_path else None
        )
        self._owns_file_handle = self.file_handle is not None
        self._owns_state = True
        self._state: Dict[str, bool] = {"closed": False}
        self.aggregator = None
        self.max_event_bytes = DEFAULT_MAX_EVENT_BYTES
        self._status_normalization_filter = StatusNormalizationSuppressionFilter()

    def bind(self, **kwargs: Any) -> "JsonLogger":
        child = JsonLogger(run_id=self.run_id, stream=self.stream, log_file_path=None)
        child.default_context = {**self.default_context, **kwargs}
        child.file_handle = self.file_handle
        child.log_file_path = self.log_file_path
        child.log_file_path_source = self.log_file_path_source
        child.aggregator = self.aggregator
        child.max_event_bytes = self.max_event_bytes
        child._owns_state = False
        child._state = self._state
        child._owns_file_handle = False
        return child

    @staticmethod
    def _resolve_path(raw_path: str | None) -> str | None:
        if not raw_path:
            return None
        path = Path(raw_path).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        return str(path)

    @property
    def closed(self) -> bool:
        return self._state["closed"]

    def attach_aggregator(self, aggregator: Any) -> None:
        self.aggregator = aggregator

    def _emit(self, payload: Dict[str, Any]) -> None:
        if self.closed:
            return
        event = {**self.default_context, **payload}
        event.setdefault("ts", datetime.now(timezone.utc).isoformat())
        encoded = self._encode_event(event)
        self.stream.write(encoded + "\n")
        self.stream.flush()
        if self.file_handle:
            self.file_handle.write(encoded + "\n")
            self.file_handle.flush()

    def _encode_event(self, event: Dict[str, Any]) -> str:
        encoded = json.dumps(event, default=str, ensure_ascii=False)
        if len(encoded.encode("utf-8")) <= self.max_event_bytes:
            return encoded

        trimmed_event = {
            **event,
            "log_truncated": True,
            "log_truncated_keys": sorted(event.keys()),
        }
        oversized_field_keys = [
            key
            for key, value in event.items()
            if key not in {"run_id", "ts", "phase", "status", "message"} and isinstance(value, (dict, list, tuple, set, str))
        ]
        for key in oversized_field_keys:
            trimmed_event[key] = "<truncated_for_size_guard>"
            candidate = json.dumps(trimmed_event, default=str, ensure_ascii=False)
            if len(candidate.encode("utf-8")) <= self.max_event_bytes:
                return candidate

        minimal_event = {
            "run_id": event.get("run_id"),
            "ts": event.get("ts"),
            "phase": event.get("phase"),
            "status": event.get("status"),
            "message": event.get("message"),
            "log_truncated": True,
            "log_truncated_reason": "max_event_bytes_exceeded",
            "max_event_bytes": self.max_event_bytes,
        }
        return json.dumps(minimal_event, default=str, ensure_ascii=False)

    @staticmethod
    def _validate_status(status: str) -> str:
        normalized = str(status or "").strip().lower()
        if normalized not in LOG_STATUSES:
            raise ValueError(
                f"Unsupported log status '{status}'. Allowed values: {', '.join(sorted(LOG_STATUSES))}"
            )
        return normalized

    def info(self, *, phase: str, status: str = "ok", message: str = "", **fields: Any) -> None:
        if self.closed:
            return
        status = self._validate_status(status)
        payload = {"phase": phase, "status": status, "message": message, **fields}
        if not self._passes_filters(payload):
            return
        if self.aggregator:
            try:
                self.aggregator.record_log_event(payload)
            except Exception:
                pass
        self._emit(payload)

    def _passes_filters(self, payload: Dict[str, Any]) -> bool:
        record = logging.makeLogRecord(
            {
                "name": "app.dashboard_downloader.json_logger",
                "level": logging.WARNING if payload.get("status") == "warning" else logging.INFO,
                "msg": payload.get("message", ""),
                "event_payload": payload,
            }
        )
        return self._status_normalization_filter.filter(record)

    def emit_suppressed_normalization_summary(self, *, phase: str, file_key: str) -> None:
        """Emit one info event for suppressed normalization warnings of a source file."""
        suppressed_count = self._status_normalization_filter.pop_suppressed_count(file_key=file_key)
        if suppressed_count <= 0:
            return
        self.info(
            phase=phase,
            status="info",
            message="normalization_events_suppressed",
            source_file=file_key,
            normalization_events_suppressed=suppressed_count,
        )

    def warn(self, *, phase: str, message: str, **fields: Any) -> None:
        self.info(phase=phase, status="warning", message=message, **fields)

    def error(self, *, phase: str, message: str, **fields: Any) -> None:
        self.info(phase=phase, status="error", message=message, **fields)

    def log_startup_event(self) -> None:
        self.info(
            phase="logger",
            message="Initialized JSON logger",
            log_file_path=self.log_file_path,
            log_file_source=self.log_file_path_source,
            run_id=self.run_id,
        )

    def close(self) -> None:
        if not self._owns_state:
            return
        if self.closed:
            return
        self._state["closed"] = True
        if self.file_handle and self._owns_file_handle:
            self.file_handle.close()
            self.file_handle = None

    def __del__(self) -> None:  # pragma: no cover
        try:
            self.close()
        except Exception:
            pass


def get_logger(run_id: Optional[str] = None) -> JsonLogger:
    logger = JsonLogger(run_id=run_id)
    logger.log_startup_event()
    return logger


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
