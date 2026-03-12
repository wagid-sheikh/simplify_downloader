from __future__ import annotations

import io
import json

import pytest

from app.crm_downloader.td_orders_sync import main as td_orders_main
from app.dashboard_downloader.json_logger import JsonLogger


class _FakeFirstLocator:
    def __init__(self, src_values: list[str | None]) -> None:
        self._src_values = src_values
        self._index = 0

    async def wait_for(self, *, state: str, timeout: int) -> None:
        assert state == "attached"
        assert timeout > 0

    async def get_attribute(self, name: str) -> str | None:
        assert name == "src"
        idx = min(self._index, len(self._src_values) - 1)
        value = self._src_values[idx]
        self._index += 1
        return value


class _FakeLocator:
    def __init__(self, src_values: list[str | None]) -> None:
        self.first = _FakeFirstLocator(src_values)


class _FakePage:
    def __init__(self, src_values: list[str | None]) -> None:
        self._src_values = src_values

    def locator(self, selector: str) -> _FakeLocator:
        assert selector == "#ifrmReport"
        return _FakeLocator(self._src_values)


def _read_logs(buffer: io.StringIO) -> list[dict[str, object]]:
    return [json.loads(line) for line in buffer.getvalue().splitlines() if line.strip()]


@pytest.mark.asyncio
async def test_auth_source_fast_path_uses_initial_iframe_src(monkeypatch: pytest.MonkeyPatch) -> None:
    output = io.StringIO()
    logger = JsonLogger(stream=output, log_file_path=None)

    async def _unexpected_fallback(*_args: object, **_kwargs: object) -> tuple[bool, str | None]:
        raise AssertionError("fallback polling should not be used when fast-path source is already valid")

    monkeypatch.setattr(td_orders_main, "_wait_for_report_iframe_auth_source", _unexpected_fallback)

    source = await td_orders_main._resolve_report_iframe_auth_source_for_api(
        _FakePage(["https://reports.quickdrycleaning.com/reports"]),
        store=td_orders_main.TdStore(store_code="A1", store_name=None, cost_center=None, sync_config={}),
        logger=logger,
        initial_iframe_src="https://reports.quickdrycleaning.com/reports",
    )

    assert source == "https://reports.quickdrycleaning.com/reports"
    assert any(log.get("message") == "auth_source_fast_path" for log in _read_logs(output))


@pytest.mark.asyncio
async def test_auth_source_fallback_polling_still_used_when_src_delayed(monkeypatch: pytest.MonkeyPatch) -> None:
    output = io.StringIO()
    logger = JsonLogger(stream=output, log_file_path=None)

    async def _fallback(*_args: object, **_kwargs: object) -> tuple[bool, str | None]:
        return True, "https://reports.quickdrycleaning.com/reports?token=1"

    monkeypatch.setattr(td_orders_main, "_wait_for_report_iframe_auth_source", _fallback)

    source = await td_orders_main._resolve_report_iframe_auth_source_for_api(
        _FakePage([None]),
        store=td_orders_main.TdStore(store_code="A1", store_name=None, cost_center=None, sync_config={}),
        logger=logger,
        initial_iframe_src=None,
    )

    assert source == "https://reports.quickdrycleaning.com/reports?token=1"
    assert any(log.get("message") == "auth_source_polled" for log in _read_logs(output))


@pytest.mark.asyncio
async def test_auth_source_unexpected_host_warns_and_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    output = io.StringIO()
    logger = JsonLogger(stream=output, log_file_path=None)

    async def _fallback(*_args: object, **_kwargs: object) -> tuple[bool, str | None]:
        return False, "https://unexpected.example/report"

    monkeypatch.setattr(td_orders_main, "_wait_for_report_iframe_auth_source", _fallback)

    source = await td_orders_main._resolve_report_iframe_auth_source_for_api(
        _FakePage(["https://unexpected.example/report"]),
        store=td_orders_main.TdStore(store_code="A1", store_name=None, cost_center=None, sync_config={}),
        logger=logger,
        initial_iframe_src="https://unexpected.example/report",
    )

    assert source == "https://unexpected.example/report"
    logs = _read_logs(output)
    assert any(log.get("message") == "Fast-path iframe auth source unavailable; using fallback polling" for log in logs)
    assert any(log.get("message") == "auth_source_fallback_unavailable" for log in logs)
