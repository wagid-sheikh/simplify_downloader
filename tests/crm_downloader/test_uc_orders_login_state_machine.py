import asyncio
import io
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import pytest
from unittest.mock import AsyncMock

from app.crm_downloader.uc_orders_sync import main as uc_main
from app.dashboard_downloader.json_logger import JsonLogger


@dataclass
class _FakePage:
    store_code: str
    url: str = "about:blank"
    session_state: str = "invalid"

    async def goto(self, url: str, wait_until: str = "domcontentloaded") -> None:
        self.url = url


class _FakeContext:
    def __init__(self, page: _FakePage) -> None:
        self._page = page

    async def new_page(self) -> _FakePage:
        return self._page

    async def storage_state(self, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text("{}")

    async def close(self) -> None:
        return None


class _FakeBrowser:
    def __init__(self) -> None:
        self._contexts: list[_FakeContext] = []

    async def new_context(self, storage_state: str | None = None) -> _FakeContext:
        store_code = Path(storage_state or "no-state").name.split("_")[0].upper() if storage_state else "UNKNOWN"
        page = _FakePage(store_code=store_code)
        context = _FakeContext(page)
        self._contexts.append(context)
        return context


@pytest.mark.asyncio
async def test_invalid_session_always_triggers_fallback_login_for_concurrent_stores(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    logger = JsonLogger(stream=io.StringIO(), log_file_path=None)
    summary = uc_main.UcOrdersDiscoverySummary(
        run_id="run-1",
        run_env="test",
        report_date=date(2025, 1, 1),
        report_end_date=date(2025, 1, 1),
        started_at=datetime.now(timezone.utc),
        store_codes=["A100", "B200"],
    )

    store_a = uc_main.UcStore(
        store_code="A100",
        store_name="Store A",
        cost_center=None,
        sync_config={
            "urls": {
                "home": "https://example.com/home",
                "orders_link": "https://example.com/orders",
                "login": "https://example.com/login",
            },
            "username": "user",
            "password": "pass",
        },
    )
    store_b = uc_main.UcStore(
        store_code="B200",
        store_name="Store B",
        cost_center=None,
        sync_config={
            "urls": {
                "home": "https://example.com/home",
                "orders_link": "https://example.com/orders",
                "login": "https://example.com/login",
            },
            "username": "user",
            "password": "pass",
        },
    )

    for store in (store_a, store_b):
        path = tmp_path / f"{store.store_code}_storage_state.json"
        path.write_text("{}")

    monkeypatch.setattr(uc_main, "default_profiles_dir", lambda: tmp_path)
    monkeypatch.setattr(uc_main, "_insert_orders_sync_log", AsyncMock(return_value=1))
    monkeypatch.setattr(uc_main, "_update_orders_sync_log", AsyncMock())
    monkeypatch.setattr(uc_main, "_navigate_to_gst_reports", AsyncMock(return_value=False))
    monkeypatch.setattr(uc_main, "_try_direct_gst_reports", AsyncMock(return_value=(False, None)))
    monkeypatch.setattr(uc_main, "_navigate_to_archive_orders", AsyncMock(return_value=False))

    async def _probe(*, page, store, logger, source):
        if source == "session" and page.session_state == "invalid":
            return False
        return True

    async def _login(*, page, store, logger):
        page.session_state = "valid"
        page.url = store.home_url or page.url
        return True

    login_mock = AsyncMock(side_effect=_login)
    monkeypatch.setattr(uc_main, "_assert_home_ready", _probe)
    monkeypatch.setattr(uc_main, "_perform_login", login_mock)

    browser = _FakeBrowser()

    await asyncio.gather(
        uc_main._run_store_discovery(
            browser=browser,
            store=store_a,
            logger=logger,
            run_env="test",
            run_id="run-1",
            run_date=datetime.now(timezone.utc),
            summary=summary,
            from_date=date(2025, 1, 1),
            to_date=date(2025, 1, 1),
            download_timeout_ms=1000,
        ),
        uc_main._run_store_discovery(
            browser=browser,
            store=store_b,
            logger=logger,
            run_env="test",
            run_id="run-1",
            run_date=datetime.now(timezone.utc),
            summary=summary,
            from_date=date(2025, 1, 1),
            to_date=date(2025, 1, 1),
            download_timeout_ms=1000,
        ),
    )

    assert login_mock.await_count == 2

    for code in ("A100", "B200"):
        outcome = summary.store_outcomes[code]
        assert outcome.session_probe_result is False
        assert outcome.fallback_login_attempted is True
        assert outcome.fallback_login_result is True
        assert outcome.status == "warning"
        assert outcome.message == "Archive Orders navigation failed"
