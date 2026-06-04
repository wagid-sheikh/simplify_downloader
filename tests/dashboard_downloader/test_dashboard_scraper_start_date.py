from __future__ import annotations

from datetime import date
from io import StringIO
from typing import Any

import pytest

from app.dashboard_downloader.dashboard_scraper import extract_dashboard_summary
from app.dashboard_downloader.json_logger import JsonLogger


class _FakeLocator:
    def __init__(self, *, text: str | None = None, count: int = 0, children: dict[str, "_FakeLocator"] | None = None):
        self._text = text
        self._count = count
        self._children = children or {}
        self.first = self

    async def count(self) -> int:
        return self._count

    async def inner_text(self) -> str:
        return self._text or ""

    def locator(self, selector: str) -> "_FakeLocator":
        return self._children.get(selector, _FakeLocator())

    def nth(self, _index: int) -> "_FakeLocator":
        return self


class _FakePage:
    def __init__(self) -> None:
        self._title = _FakeLocator(
            text="Dashboard\nDemo Store\n01 January 2026",
            count=1,
            children={"small": _FakeLocator(text="01 January 2026", count=1)},
        )
        self._launch = _FakeLocator(
            count=1,
            children={"xpath=following-sibling::p[1]": _FakeLocator(text="15-02-2025", count=1)},
        )

    def locator(self, selector: str, *_args: Any, **_kwargs: Any) -> _FakeLocator:
        if selector == "h1.dashboard-title":
            return self._title
        if selector == 'h3.section-title:has-text("Launch Date")':
            return self._launch
        return _FakeLocator()


@pytest.mark.asyncio
async def test_extract_dashboard_summary_stores_launch_label_as_start_date() -> None:
    logger = JsonLogger(stream=StringIO(), log_file_path=None)

    dashboard_data = await extract_dashboard_summary(
        _FakePage(),
        {"store_code": "A001", "store_name": "Demo Store"},
        logger=logger,
    )

    assert dashboard_data["start_date"] == date(2025, 2, 15)
