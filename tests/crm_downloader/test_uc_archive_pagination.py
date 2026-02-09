import io
import json
from unittest.mock import AsyncMock

import pytest

from app.crm_downloader.uc_orders_sync import main as uc_main
from app.dashboard_downloader.json_logger import JsonLogger


class _FakeLocator:
    def __init__(self, *, count: int = 1, text: str = "", is_disabled: bool = False) -> None:
        self._count = count
        self._text = text
        self._is_disabled = is_disabled
        self.click_count = 0

    @property
    def first(self) -> "_FakeLocator":
        return self

    async def count(self) -> int:
        return self._count

    def nth(self, _idx: int) -> "_FakeLocator":
        return self

    def locator(self, _selector: str) -> "_FakeLocator":
        return _FakeLocator(count=0)

    async def inner_text(self) -> str:
        return self._text

    async def click(self) -> None:
        self.click_count += 1

    async def is_disabled(self) -> bool:
        return self._is_disabled

    async def get_attribute(self, name: str) -> str | None:
        if name == "class":
            return "pagination-btn"
        return None


class _FakePage:
    def __init__(self, *, next_button: _FakeLocator) -> None:
        self._next_button = next_button

    async def wait_for_selector(self, _selector: str, timeout: int | None = None) -> None:
        return None

    async def wait_for_timeout(self, _timeout_ms: int) -> None:
        return None

    def locator(self, selector: str) -> _FakeLocator:
        if selector == uc_main.ARCHIVE_TABLE_ROW_SELECTOR:
            return _FakeLocator(count=0)
        if selector == uc_main.ARCHIVE_NEXT_BUTTON_SELECTOR:
            return self._next_button
        return _FakeLocator(count=0)


@pytest.mark.asyncio
async def test_collect_archive_orders_retries_once_then_marks_partial_on_non_advancing_paginator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = io.StringIO()
    logger = JsonLogger(stream=output, log_file_path=None)
    store = uc_main.UcStore(store_code="A100", store_name="Store A", cost_center=None, sync_config={})
    next_button = _FakeLocator(count=1, is_disabled=False)
    page = _FakePage(next_button=next_button)

    monkeypatch.setattr(uc_main, "_get_first_order_code", AsyncMock(return_value="ORD-001"))
    monkeypatch.setattr(uc_main, "_get_archive_page_number", AsyncMock(return_value=1))
    monkeypatch.setattr(
        uc_main,
        "_wait_for_archive_page_advance",
        AsyncMock(side_effect=[(False, 1, "ORD-001"), (False, 1, "ORD-001")]),
    )
    monkeypatch.setattr(
        uc_main,
        "_detect_archive_paginator_sticky_state",
        AsyncMock(
            return_value={
                "overlay_blocking": True,
                "disabled_next_mismatch": True,
                "disabled_signals": {
                    "is_disabled": False,
                    "aria_disabled": True,
                    "disabled_attr": False,
                    "class_disabled": False,
                },
            }
        ),
    )

    extract = await uc_main._collect_archive_orders(page=page, store=store, logger=logger)

    assert next_button.click_count == 2
    assert extract.partial_extraction is True
    assert extract.partial_reason == "pagination_non_advancing_after_retry"

    events = [json.loads(line) for line in output.getvalue().splitlines() if line.strip()]
    messages = [event.get("message") for event in events]
    assert "Pagination did not advance after first Next click; retrying once" in messages
    abort_event = next(
        event
        for event in events
        if event.get("message")
        == "Pagination still did not advance after retry; aborting pagination with partial extraction"
    )
    assert abort_event["sticky_paginator_state"]["overlay_blocking"] is True
    assert abort_event["sticky_paginator_state"]["disabled_next_mismatch"] is True
