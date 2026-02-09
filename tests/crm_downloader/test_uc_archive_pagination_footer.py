import io
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path

import pytest

from app.crypto import encrypt_secret

_DB_PATH = Path(os.environ.get("POSTGRES_DB", "tests/config.db"))
_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
with sqlite3.connect(_DB_PATH) as _conn:
    _conn.execute(
        """
        CREATE TABLE IF NOT EXISTS system_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key TEXT NOT NULL UNIQUE,
            value TEXT NOT NULL,
            description TEXT,
            is_active BOOLEAN NOT NULL DEFAULT 1
        )
        """
    )
    _secret = os.environ.get("SECRET_KEY", "test-secret")
    required = {
        "ETL_STEP_TIMEOUT_SECONDS": "120",
        "PDF_RENDER_TIMEOUT_SECONDS": "120",
        "SKIP_UC_Pending_Delivery": "false",
        "TD_GLOBAL_PASSWORD": encrypt_secret(_secret, "change-me-global-password"),
        "REPORT_EMAIL_SMTP_PASSWORD": encrypt_secret(_secret, "change-me-smtp-password"),
    }
    for k, v in required.items():
        _conn.execute(
            "INSERT OR REPLACE INTO system_config (key, value, description, is_active) VALUES (?, ?, ?, 1)",
            (k, v, f"seed {k}"),
        )

from app.crm_downloader.uc_orders_sync import main as uc_main
from app.dashboard_downloader.json_logger import JsonLogger


@dataclass
class _Row:
    order_code: str


class _FakeRowLocator:
    def __init__(self, page: "_FakePage") -> None:
        self._page = page

    async def count(self) -> int:
        return len(self._page.rows_by_page[self._page.page_idx])

    def nth(self, idx: int) -> _Row:
        return _Row(order_code=self._page.rows_by_page[self._page.page_idx][idx])


class _FakeButton:
    def __init__(self, page: "_FakePage") -> None:
        self._page = page

    @property
    def first(self):
        return self

    async def count(self) -> int:
        return 1

    async def is_disabled(self) -> bool:
        return False

    async def get_attribute(self, name: str):
        return None

    async def click(self, force: bool = False) -> None:
        self._page.click_attempts += 1

    async def scroll_into_view_if_needed(self) -> None:
        return None


class _FakePage:
    def __init__(self, rows_by_page: list[list[str]]) -> None:
        self.rows_by_page = rows_by_page
        self.page_idx = 0
        self.click_attempts = 0

    async def wait_for_selector(self, selector: str, timeout: int) -> None:
        return None

    def locator(self, selector: str):
        if selector == uc_main.ARCHIVE_TABLE_ROW_SELECTOR:
            return _FakeRowLocator(self)
        if selector == uc_main.ARCHIVE_NEXT_BUTTON_SELECTOR:
            return _FakeButton(self)
        raise AssertionError(f"Unexpected selector: {selector}")


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("Showing results 1 to 30 of 785 total", (1, 30, 785)),
        ("Showing results 781 to 785 of 1,234 total", (781, 785, 1234)),
        ("total: unknown", None),
    ],
)
def test_parse_archive_footer_range(text: str, expected: tuple[int, int, int] | None) -> None:
    assert uc_main._parse_archive_footer_range(text) == expected


@pytest.mark.asyncio
async def test_collect_archive_orders_progresses_until_footer_complete(monkeypatch: pytest.MonkeyPatch) -> None:
    page = _FakePage(rows_by_page=[["ORD-1", "ORD-2"], ["ORD-3", "ORD-4"], ["ORD-5"]])
    logger = JsonLogger(stream=io.StringIO(), log_file_path=None)
    store = uc_main.UcStore(store_code="A100", store_name="S", cost_center=None, sync_config={})

    footer_values = [(1, 2, 5), (3, 4, 5), (5, 5, 5)]

    async def fake_get_footer(page, logger, store_code):
        return footer_values[page.page_idx]

    async def fake_wait_advance(**kwargs):
        page.page_idx += 1
        return True, footer_values[page.page_idx], f"sig-{page.page_idx}"

    async def fake_get_order_code(row):
        return row.order_code

    async def fake_base_row(**kw):
        return {"order_code": kw["order_code"]}

    async def fake_order_details(**kw):
        return [], None, None, True

    async def fake_payment_details(**kw):
        return []

    async def fake_is_disabled(locator):
        return False

    async def fake_page_number(page):
        return page.page_idx + 1

    async def fake_first_order(page):
        return page.rows_by_page[page.page_idx][0]

    async def fake_sig(page):
        return f"sig-{page.page_idx}"

    monkeypatch.setattr(uc_main, "_get_archive_footer_range", fake_get_footer)
    monkeypatch.setattr(uc_main, "_get_archive_order_code", fake_get_order_code)
    monkeypatch.setattr(uc_main, "_extract_archive_base_row", fake_base_row)
    monkeypatch.setattr(uc_main, "_extract_order_details", fake_order_details)
    monkeypatch.setattr(uc_main, "_extract_payment_details", fake_payment_details)
    monkeypatch.setattr(uc_main, "_is_button_disabled", fake_is_disabled)
    monkeypatch.setattr(uc_main, "_wait_for_archive_pagination_advance", fake_wait_advance)
    monkeypatch.setattr(uc_main, "_get_archive_page_number", fake_page_number)
    monkeypatch.setattr(uc_main, "_get_first_order_code", fake_first_order)
    monkeypatch.setattr(uc_main, "_get_first_row_signature", fake_sig)

    extract = await uc_main._collect_archive_orders(page=page, store=store, logger=logger)

    assert len(extract.base_rows) == 5
    assert extract.footer_total == 5
    assert extract.partial_extraction_reason is None


@pytest.mark.asyncio
async def test_collect_archive_orders_retries_next_click_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    page = _FakePage(rows_by_page=[["ORD-1"], ["ORD-2"]])
    logger = JsonLogger(stream=io.StringIO(), log_file_path=None)
    store = uc_main.UcStore(store_code="A100", store_name="S", cost_center=None, sync_config={})

    footer_values = [(1, 1, 2), (2, 2, 2)]

    async def fake_get_footer(page, logger, store_code):
        return footer_values[page.page_idx]

    calls = {"n": 0}

    async def fake_wait_advance(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return False, footer_values[0], "sig-0"
        page.page_idx = 1
        return True, footer_values[1], "sig-1"

    async def fake_get_order_code(row):
        return row.order_code

    async def fake_base_row(**kw):
        return {"order_code": kw["order_code"]}

    async def fake_order_details(**kw):
        return [], None, None, True

    async def fake_payment_details(**kw):
        return []

    async def fake_is_disabled(locator):
        return False

    async def fake_page_number(page):
        return page.page_idx + 1

    async def fake_first_order(page):
        return page.rows_by_page[page.page_idx][0]

    async def fake_sig(page):
        return f"sig-{page.page_idx}"

    monkeypatch.setattr(uc_main, "_get_archive_footer_range", fake_get_footer)
    monkeypatch.setattr(uc_main, "_get_archive_order_code", fake_get_order_code)
    monkeypatch.setattr(uc_main, "_extract_archive_base_row", fake_base_row)
    monkeypatch.setattr(uc_main, "_extract_order_details", fake_order_details)
    monkeypatch.setattr(uc_main, "_extract_payment_details", fake_payment_details)
    monkeypatch.setattr(uc_main, "_is_button_disabled", fake_is_disabled)
    monkeypatch.setattr(uc_main, "_wait_for_archive_pagination_advance", fake_wait_advance)
    monkeypatch.setattr(uc_main, "_get_archive_page_number", fake_page_number)
    monkeypatch.setattr(uc_main, "_get_first_order_code", fake_first_order)
    monkeypatch.setattr(uc_main, "_get_first_row_signature", fake_sig)

    extract = await uc_main._collect_archive_orders(page=page, store=store, logger=logger)

    assert len(extract.base_rows) == 2
    assert extract.partial_extraction_reason is None
    assert calls["n"] == 2


@pytest.mark.asyncio
async def test_collect_archive_orders_stops_with_warning_when_pagination_stalled(monkeypatch: pytest.MonkeyPatch) -> None:
    page = _FakePage(rows_by_page=[["ORD-1"]])
    logger = JsonLogger(stream=io.StringIO(), log_file_path=None)
    store = uc_main.UcStore(store_code="A100", store_name="S", cost_center=None, sync_config={})

    async def fake_get_footer(page, logger, store_code):
        return (1, 1, 3)

    async def fake_wait_advance(**kwargs):
        return False, (1, 1, 3), "sig-0"

    async def fake_get_order_code(row):
        return row.order_code

    async def fake_base_row(**kw):
        return {"order_code": kw["order_code"]}

    async def fake_order_details(**kw):
        return [], None, None, True

    async def fake_payment_details(**kw):
        return []

    async def fake_is_disabled(locator):
        return False

    async def fake_page_number(page):
        return 1

    async def fake_first_order(page):
        return "ORD-1"

    async def fake_sig(page):
        return "sig-0"

    monkeypatch.setattr(uc_main, "_get_archive_footer_range", fake_get_footer)
    monkeypatch.setattr(uc_main, "_get_archive_order_code", fake_get_order_code)
    monkeypatch.setattr(uc_main, "_extract_archive_base_row", fake_base_row)
    monkeypatch.setattr(uc_main, "_extract_order_details", fake_order_details)
    monkeypatch.setattr(uc_main, "_extract_payment_details", fake_payment_details)
    monkeypatch.setattr(uc_main, "_is_button_disabled", fake_is_disabled)
    monkeypatch.setattr(uc_main, "_wait_for_archive_pagination_advance", fake_wait_advance)
    monkeypatch.setattr(uc_main, "_get_archive_page_number", fake_page_number)
    monkeypatch.setattr(uc_main, "_get_first_order_code", fake_first_order)
    monkeypatch.setattr(uc_main, "_get_first_row_signature", fake_sig)

    extract = await uc_main._collect_archive_orders(page=page, store=store, logger=logger)

    assert len(extract.base_rows) == 1
    assert extract.footer_total == 3
    assert extract.partial_extraction_reason == "partial_extraction_pagination_stall"
