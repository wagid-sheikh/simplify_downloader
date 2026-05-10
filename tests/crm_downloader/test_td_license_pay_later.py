from __future__ import annotations

import io
import json

import pytest

from app.crm_downloader.td_orders_sync import main as td_orders_main
from app.dashboard_downloader.json_logger import JsonLogger


class _FakeLocator:
    def __init__(self, visible: bool) -> None:
        self._visible = visible
        self.first = self

    async def is_visible(self) -> bool:
        return self._visible


class _FakeLicencePage:
    def __init__(self) -> None:
        self.url = "https://subs.quickdrycleaning.com/a817/App/frmLicence"
        self.clicked_selectors: list[str] = []
        self._home_ready = False

    def locator(self, selector: str) -> _FakeLocator:
        if selector == "#btnPayLater":
            return _FakeLocator(not self._home_ready)
        if selector == 'h5.card-title:has-text("Daily Operations Tracker")':
            return _FakeLocator(self._home_ready)
        if selector == "#achrOrderReport":
            return _FakeLocator(self._home_ready)
        return _FakeLocator(False)

    async def click(self, selector: str) -> None:
        self.clicked_selectors.append(selector)
        if selector != "#btnPayLater":
            raise AssertionError(f"Unexpected click selector: {selector}")
        self._home_ready = True
        self.url = "https://subs.quickdrycleaning.com/a817/App/home?EventClick=True"

    async def wait_for_selector(self, selector: str, *, timeout: int) -> object:
        if not self._home_ready:
            raise td_orders_main.TimeoutError(f"{selector} not ready after {timeout}ms")
        if selector in {"#achrOrderReport", 'h5.card-title:has-text("Daily Operations Tracker")'}:
            return object()
        raise td_orders_main.TimeoutError(f"Unexpected selector: {selector}")


@pytest.mark.asyncio
async def test_wait_for_home_clicks_pay_later_when_login_redirects_to_licence() -> None:
    output = io.StringIO()
    logger = JsonLogger(stream=output, log_file_path=None)
    page = _FakeLicencePage()
    store = td_orders_main.TdStore(store_code="A817", store_name=None, cost_center=None, sync_config={})

    home_ready = await td_orders_main._wait_for_home(
        page,
        store=store,
        logger=logger,
        nav_selector="#achrOrderReport",
        timeout_ms=1_000,
    )

    assert home_ready is True
    assert page.clicked_selectors == ["#btnPayLater"]
    assert page.url == "https://subs.quickdrycleaning.com/a817/App/home?EventClick=True"

    logs = [json.loads(line) for line in output.getvalue().splitlines() if line.strip()]
    before_click = next(log for log in logs if log.get("message") == "TD licence page detected; clicking pay later")
    assert before_click["phase"] == "login"
    assert before_click["store_code"] == "A817"
    assert before_click["final_url"] == "https://subs.quickdrycleaning.com/a817/App/frmLicence"

    after_click = next(log for log in logs if log.get("message") == "TD licence pay later handled")
    assert after_click["phase"] == "login"
    assert after_click["pay_later_clicked"] is True
    assert after_click["final_url"] == "https://subs.quickdrycleaning.com/a817/App/home?EventClick=True"
    assert after_click["error_text"] is None
