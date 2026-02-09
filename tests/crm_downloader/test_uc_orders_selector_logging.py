import io
import asyncio
import json

from types import SimpleNamespace

import pytest

from app.crm_downloader.uc_orders_sync import main as uc_main
from app.dashboard_downloader.json_logger import JsonLogger


@pytest.mark.asyncio
async def test_selector_cue_logging_skipped_when_dom_logging_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    output = io.StringIO()
    logger = JsonLogger(stream=output, log_file_path=None)
    monkeypatch.setattr(uc_main, "config", SimpleNamespace(pipeline_skip_dom_logging=True))

    await uc_main._log_selector_cues(logger=logger, store_code="A100", container=object(), page=object())

    log = json.loads(output.getvalue())
    assert log["phase"] == "selectors"
    assert log["message"].startswith("Skipped GST report selector cue capture")
    assert log["store_code"] == "A100"
    assert "controls" not in log
    assert "spinners" not in log


def test_normalize_order_info_key_handles_punctuation_and_spacing() -> None:
    assert uc_main._normalize_order_info_key(" Order Date : ") == "order date"
    assert uc_main._normalize_order_info_key("Order No. - UC123") == "order no - uc123"


def test_normalize_order_info_key_handles_empty_values() -> None:
    assert uc_main._normalize_order_info_key(None) == ""
    assert uc_main._normalize_order_info_key("   ") == ""


def test_parse_archive_footer_window_extracts_start_end_total() -> None:
    assert uc_main._parse_archive_footer_window("Showing results 1 to 30 of 785 total") == (1, 30, 785)
    assert uc_main._parse_archive_footer_window(" showing results 31 to 60 of 785 total ") == (31, 60, 785)


def test_parse_archive_footer_window_returns_none_for_non_matching_text() -> None:
    assert uc_main._parse_archive_footer_window("Showing page 1") is None


class _FakeRowLocator:
    def __init__(self, page):
        self._page = page

    async def count(self) -> int:
        return len(self._page.pages[self._page.index]["order_codes"])

    def nth(self, idx: int):
        return SimpleNamespace(order_code=self._page.pages[self._page.index]["order_codes"][idx])


class _FakeNextButton:
    def __init__(self, page):
        self._page = page

    @property
    def first(self):
        return self

    async def count(self) -> int:
        return 1

    async def is_disabled(self) -> bool:
        return bool(self._page.pages[self._page.index].get("next_disabled", False))

    async def get_attribute(self, _: str):
        return None

    async def click(self) -> None:
        if self._page.index < len(self._page.pages) - 1:
            self._page.index += 1

    async def scroll_into_view_if_needed(self, timeout: int | None = None) -> None:
        self._page.scroll_calls += 1


class _FakePage:
    def __init__(self, pages):
        self.pages = pages
        self.index = 0
        self.scroll_calls = 0
        self.timeout_calls = 0

    def locator(self, selector: str):
        if selector == uc_main.ARCHIVE_TABLE_ROW_SELECTOR:
            return _FakeRowLocator(self)
        if selector == uc_main.ARCHIVE_NEXT_BUTTON_SELECTOR:
            return _FakeNextButton(self)
        return _FakeNextButton(self)

    async def wait_for_selector(self, *args, **kwargs):
        return None

    async def wait_for_function(self, *args, **kwargs):
        return None

    async def wait_for_timeout(self, _ms: int):
        self.timeout_calls += 1
        return None


@pytest.mark.asyncio
async def test_collect_archive_orders_does_not_stop_on_page1_when_footer_has_more(monkeypatch: pytest.MonkeyPatch) -> None:
    pages = [
        {"order_codes": [f"O{i:03d}" for i in range(1, 31)], "footer": (1, 30, 785), "next_disabled": False},
        {"order_codes": [f"O{i:03d}" for i in range(31, 61)], "footer": (31, 60, 785), "next_disabled": True},
    ]
    page = _FakePage(pages)
    store = uc_main.UcStore(store_code="UCX", store_name=None, cost_center=None, sync_config={})
    logger = JsonLogger(stream=io.StringIO(), log_file_path=None)

    monkeypatch.setattr(uc_main, "_get_archive_footer_window", lambda _p: asyncio.sleep(0, result=pages[page.index]["footer"]))
    monkeypatch.setattr(uc_main, "_get_archive_footer_total", lambda _p: asyncio.sleep(0, result=None))
    monkeypatch.setattr(uc_main, "_get_archive_order_code", lambda row: asyncio.sleep(0, result=row.order_code))
    monkeypatch.setattr(uc_main, "_extract_archive_base_row", lambda **kwargs: asyncio.sleep(0, result={"order_code": kwargs["order_code"], "payment_text": "paid"}))
    monkeypatch.setattr(uc_main, "_extract_order_details", lambda **kwargs: asyncio.sleep(0, result=([], None, None, True)))
    monkeypatch.setattr(uc_main, "_extract_payment_details", lambda **kwargs: asyncio.sleep(0, result=[]))
    monkeypatch.setattr(uc_main, "_get_first_order_code", lambda _p: asyncio.sleep(0, result=pages[page.index]["order_codes"][0]))
    monkeypatch.setattr(uc_main, "_get_archive_page_number", lambda _p: asyncio.sleep(0, result=page.index + 1))
    monkeypatch.setattr(uc_main, "_is_button_disabled", lambda _b: asyncio.sleep(0, result=pages[page.index]["next_disabled"]))

    extract = await uc_main._collect_archive_orders(page=page, store=store, logger=logger)

    assert extract.page_count == 2
    assert len(extract.base_rows) == 60
    assert extract.skipped_order_counters["partial_extraction_footer_total_mismatch"] == 1


@pytest.mark.asyncio
async def test_collect_archive_orders_forces_retry_when_duplicates_but_footer_has_more(monkeypatch: pytest.MonkeyPatch) -> None:
    pages = [
        {"order_codes": [f"O{i:03d}" for i in range(1, 31)], "footer": (1, 30, 48), "next_disabled": False},
        {"order_codes": [f"O{i:03d}" for i in range(1, 31)], "footer": (1, 30, 48), "next_disabled": False},
        {"order_codes": [f"O{i:03d}" for i in range(31, 49)], "footer": (31, 48, 48), "next_disabled": True},
    ]
    page = _FakePage(pages)
    store = uc_main.UcStore(store_code="UCX", store_name=None, cost_center=None, sync_config={})
    logger = JsonLogger(stream=io.StringIO(), log_file_path=None)

    monkeypatch.setattr(uc_main, "_get_archive_footer_window", lambda _p: asyncio.sleep(0, result=pages[page.index]["footer"]))
    monkeypatch.setattr(uc_main, "_get_archive_footer_total", lambda _p: asyncio.sleep(0, result=None))
    monkeypatch.setattr(uc_main, "_get_archive_order_code", lambda row: asyncio.sleep(0, result=row.order_code))
    monkeypatch.setattr(uc_main, "_extract_archive_base_row", lambda **kwargs: asyncio.sleep(0, result={"order_code": kwargs["order_code"], "payment_text": "paid"}))
    monkeypatch.setattr(uc_main, "_extract_order_details", lambda **kwargs: asyncio.sleep(0, result=([], None, None, True)))
    monkeypatch.setattr(uc_main, "_extract_payment_details", lambda **kwargs: asyncio.sleep(0, result=[]))
    monkeypatch.setattr(uc_main, "_get_first_order_code", lambda _p: asyncio.sleep(0, result=pages[page.index]["order_codes"][0]))
    monkeypatch.setattr(uc_main, "_get_archive_page_number", lambda _p: asyncio.sleep(0, result=page.index + 1))
    monkeypatch.setattr(uc_main, "_is_button_disabled", lambda _b: asyncio.sleep(0, result=pages[page.index]["next_disabled"]))

    extract = await uc_main._collect_archive_orders(page=page, store=store, logger=logger)

    assert len(extract.base_rows) == 48
    assert extract.page_count == 3
    assert page.scroll_calls >= 1
    assert page.timeout_calls >= 1
