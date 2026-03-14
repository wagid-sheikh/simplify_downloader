from __future__ import annotations

import io
import json
import re
from pathlib import Path

import pytest

from app.crm_downloader.td_orders_sync import main as td_orders_main
from app.dashboard_downloader.json_logger import JsonLogger


class _FakeLoop:
    def __init__(self) -> None:
        self.now = 0.0

    def time(self) -> float:
        return self.now


class _FakeSimpleLocator:
    def __init__(self, text: str | None = None) -> None:
        self._text = text
        self.first = self

    async def inner_text(self) -> str:
        return self._text or ""


class _FakeRow:
    def __init__(self, range_text: str) -> None:
        self._range_text = range_text

    def locator(self, selector: str) -> _FakeSimpleLocator:
        if selector == "div.w-1\\/5.text-sm.italic":
            return _FakeSimpleLocator(self._range_text)
        return _FakeSimpleLocator(None)

    def get_by_role(self, *_args: object, **_kwargs: object) -> _FakeSimpleLocator:
        return _FakeSimpleLocator(None)


class _FakeContainer:
    async def scroll_into_view_if_needed(self) -> None:
        return None

    def get_by_role(self, *_args: object, **_kwargs: object) -> _FakeSimpleLocator:
        return _FakeSimpleLocator(None)

    def locator(self, *_args: object, **_kwargs: object) -> _FakeSimpleLocator:
        return _FakeSimpleLocator(None)


class _FakeFrame:
    def get_by_role(self, *_args: object, **_kwargs: object) -> _FakeSimpleLocator:
        return _FakeSimpleLocator(None)

    def locator(self, *_args: object, **_kwargs: object) -> _FakeSimpleLocator:
        return _FakeSimpleLocator(None)


class _FakeDownload:
    suggested_filename = "report.xlsx"

    async def save_as(self, _path: str) -> None:
        return None


class _FakeDownloadInfo:
    def __init__(self) -> None:
        self.value = self._resolve()

    async def _resolve(self) -> _FakeDownload:
        return _FakeDownload()


class _FakeDownloadContext:
    async def __aenter__(self) -> _FakeDownloadInfo:
        return _FakeDownloadInfo()

    async def __aexit__(self, *_exc: object) -> None:
        return None


class _FakeDownloadLocator:
    async def click(self) -> None:
        return None


class _FakePage:
    async def wait_for_timeout(self, _timeout_ms: int) -> None:
        return None

    def expect_download(self, *_args: object, **_kwargs: object) -> _FakeDownloadContext:
        return _FakeDownloadContext()


@pytest.mark.asyncio
async def test_pending_short_eta_does_not_force_135_seconds(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fake_loop = _FakeLoop()
    output = io.StringIO()
    logger = JsonLogger(stream=output, log_file_path=None)
    row = _FakeRow("Jan 1, 2025 - Jan 1, 2025")

    async def _fake_sleep(seconds: float) -> None:
        fake_loop.now += seconds

    async def _fake_collect_rows(*_args: object, **_kwargs: object) -> list[_FakeRow]:
        return [row]

    async def _fake_extract_status(*_args: object, **_kwargs: object) -> str:
        return "Pending ETA 10s"

    async def _fake_locate_download(*_args: object, **_kwargs: object) -> tuple[None, None]:
        return None, None

    async def _fake_first_visible(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(td_orders_main.asyncio, "get_event_loop", lambda: fake_loop)
    monkeypatch.setattr(td_orders_main.asyncio, "sleep", _fake_sleep)
    monkeypatch.setattr(td_orders_main, "_collect_report_request_rows", _fake_collect_rows)
    monkeypatch.setattr(td_orders_main, "_extract_row_status_text", _fake_extract_status)
    monkeypatch.setattr(td_orders_main, "_locate_report_request_download", _fake_locate_download)
    monkeypatch.setattr(td_orders_main, "_first_visible_locator", _fake_first_visible)

    downloaded, downloaded_path, _, _ = await td_orders_main._wait_for_report_request_download_link(
        _FakeFrame(),
        _FakePage(),
        _FakeContainer(),
        report_label="orders",
        expected_range_texts=["Jan 1, 2025 - Jan 1, 2025"],
        range_patterns=[re.compile("Jan 1")],
        logger=logger,
        store=td_orders_main.TdStore(store_code="A1", store_name=None, cost_center=None, sync_config={}),
        timeout_ms=1_000,
        download_path=tmp_path / "orders.xlsx",
        download_wait_timeout_ms=500,
    )

    assert downloaded is False
    assert downloaded_path is None
    logs = [json.loads(line) for line in output.getvalue().splitlines() if line.strip()]
    eta_log = next(log for log in logs if log.get("message") == "Extended report request poll window to ETA")
    assert eta_log["pending_eta_seconds"] < 135
    assert eta_log["new_timeout_ms"] < 135_000


@pytest.mark.asyncio
async def test_pending_without_eta_does_not_force_long_extension(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fake_loop = _FakeLoop()
    output = io.StringIO()
    logger = JsonLogger(stream=output, log_file_path=None)
    row = _FakeRow("Jan 1, 2025 - Jan 1, 2025")

    async def _fake_sleep(seconds: float) -> None:
        fake_loop.now += seconds

    async def _fake_collect_rows(*_args: object, **_kwargs: object) -> list[_FakeRow]:
        return [row]

    async def _fake_extract_status(*_args: object, **_kwargs: object) -> str:
        return "Pending"

    async def _fake_locate_download(*_args: object, **_kwargs: object) -> tuple[None, None]:
        return None, None

    async def _fake_first_visible(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(td_orders_main.asyncio, "get_event_loop", lambda: fake_loop)
    monkeypatch.setattr(td_orders_main.asyncio, "sleep", _fake_sleep)
    monkeypatch.setattr(td_orders_main, "_collect_report_request_rows", _fake_collect_rows)
    monkeypatch.setattr(td_orders_main, "_extract_row_status_text", _fake_extract_status)
    monkeypatch.setattr(td_orders_main, "_locate_report_request_download", _fake_locate_download)
    monkeypatch.setattr(td_orders_main, "_first_visible_locator", _fake_first_visible)

    await td_orders_main._wait_for_report_request_download_link(
        _FakeFrame(),
        _FakePage(),
        _FakeContainer(),
        report_label="orders",
        expected_range_texts=["Jan 1, 2025 - Jan 1, 2025"],
        range_patterns=[re.compile("Jan 1")],
        logger=logger,
        store=td_orders_main.TdStore(store_code="A1", store_name=None, cost_center=None, sync_config={}),
        timeout_ms=1_000,
        download_path=tmp_path / "orders.xlsx",
        download_wait_timeout_ms=500,
    )

    logs = [json.loads(line) for line in output.getvalue().splitlines() if line.strip()]
    timeout_log = next(log for log in logs if log.get("message") == "Report Requests download link not available before timeout")
    assert timeout_log["timeout_ms"] == 1_000
    assert all(log.get("message") != "Extended report request poll window to ETA" for log in logs)


@pytest.mark.asyncio
async def test_downloadable_row_exits_immediately(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    output = io.StringIO()
    logger = JsonLogger(stream=output, log_file_path=None)
    row = _FakeRow("Jan 1, 2025 - Jan 1, 2025")

    async def _fake_collect_rows(*_args: object, **_kwargs: object) -> list[_FakeRow]:
        return [row]

    async def _fake_extract_status(*_args: object, **_kwargs: object) -> str:
        return "Ready"

    async def _fake_locate_download(*_args: object, **_kwargs: object) -> tuple[_FakeDownloadLocator, str]:
        return _FakeDownloadLocator(), "fake"

    monkeypatch.setattr(td_orders_main, "_collect_report_request_rows", _fake_collect_rows)
    monkeypatch.setattr(td_orders_main, "_extract_row_status_text", _fake_extract_status)
    monkeypatch.setattr(td_orders_main, "_locate_report_request_download", _fake_locate_download)

    downloaded, downloaded_path, _, _ = await td_orders_main._wait_for_report_request_download_link(
        _FakeFrame(),
        _FakePage(),
        _FakeContainer(),
        report_label="orders",
        expected_range_texts=["Jan 1, 2025 - Jan 1, 2025"],
        range_patterns=[re.compile("Jan 1")],
        logger=logger,
        store=td_orders_main.TdStore(store_code="A1", store_name=None, cost_center=None, sync_config={}),
        timeout_ms=1_000,
        download_path=tmp_path / "orders.xlsx",
        download_wait_timeout_ms=500,
    )

    assert downloaded is True
    assert downloaded_path == str(tmp_path / "orders.xlsx")
