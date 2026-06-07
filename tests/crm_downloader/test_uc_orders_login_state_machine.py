import asyncio
import io
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

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
        self.context_kwargs: list[dict[str, object]] = []

    async def new_context(
        self,
        storage_state: str | None = None,
        ignore_https_errors: bool = False,
    ) -> _FakeContext:
        self.context_kwargs.append(
            {
                "storage_state": storage_state,
                "ignore_https_errors": ignore_https_errors,
            }
        )
        store_code = (
            Path(storage_state or "no-state").name.split("_")[0].upper()
            if storage_state
            else "UNKNOWN"
        )
        page = _FakePage(store_code=store_code)
        context = _FakeContext(page)
        self._contexts.append(context)
        return context


@pytest.mark.asyncio
async def test_migrated_uc_dashboard_url_is_ready_without_home_url_timeout() -> None:
    stream = io.StringIO()
    logger = JsonLogger(stream=stream, log_file_path=None)
    store = uc_main.UcStore(
        store_code="UC567",
        store_name="UC Store",
        cost_center="SC3567",
        sync_config={
            "urls": {
                "home": "https://storepanel.ucleanlaundry.com/dashboard",
                "login": "https://store.ucleanlaundry.com/login",
            }
        },
    )
    page = _DashboardProbePage(
        url="https://storepanel.ucleanlaundry.com/dashboard",
        visible_selectors={"nav"},
        html="<html></html>",
        api_responses=[_FakeApiResponse(status=200, payload={"latestOrderId": 12345})],
    )
    setattr(page, "_uc_home_response_status", 200)
    setattr(
        page,
        "_uc_home_response_url",
        "https://storepanel.ucleanlaundry.com/dashboard",
    )

    ready = await uc_main._wait_for_home_ready(
        page=page, store=store, logger=logger, source="test"
    )

    assert store.home_url == "https://storepanel.ucleanlaundry.com/dashboard"
    assert uc_main._url_matches_target(page.url, store.home_url)
    assert ready is True
    assert "Timed out waiting for home URL" not in stream.getvalue()
    assert not uc_main._url_matches_target(
        "https://store.ucleanlaundry.com/login", store.home_url
    )
    assert not uc_main._url_matches_target(
        "https://unrelated.example/dashboard", store.home_url
    )


@pytest.mark.asyncio
async def test_uc_dashboard_readiness_allows_missing_legacy_tracker_card() -> None:
    stream = io.StringIO()
    logger = JsonLogger(stream=stream, log_file_path=None)
    store = uc_main.UcStore(
        store_code="UC568",
        store_name="UC Store",
        cost_center="SC3568",
        sync_config={
            "urls": {
                "home": "https://storepanel.ucleanlaundry.com/dashboard",
                "login": "https://store.ucleanlaundry.com/login",
            }
        },
    )
    page = _DashboardProbePage(
        url="https://storepanel.ucleanlaundry.com/dashboard",
        visible_selectors={"nav"},
        html="<html><nav></nav></html>",
        api_responses=[_FakeApiResponse(status=200, payload={"latestOrderId": 12345})],
        title="UClean Suite 2.0",
    )

    ready = await uc_main._wait_for_home_ready(
        page=page, store=store, logger=logger, source="test"
    )

    assert ready is True
    output = stream.getvalue()
    assert "UC home staged readiness probe" in output
    assert '"page_title": "UClean Suite 2.0"' in output
    assert '"dashboard_shell_visible": true' in output
    assert '"target_card_visible": false' in output
    assert '"api_probe_ready": true' in output
    assert "UC dashboard selector/card missing" not in output


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("configured_value", "expected_ignore_https_errors"),
    [
        (False, False),
        (True, True),
    ],
)
async def test_prepare_uc_api_page_passes_uc_https_error_config_to_browser_context(
    monkeypatch: pytest.MonkeyPatch,
    configured_value: bool,
    expected_ignore_https_errors: bool,
) -> None:
    logger = JsonLogger(stream=io.StringIO(), log_file_path=None)
    store = uc_main.UcStore(
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

    monkeypatch.setattr(
        uc_main,
        "config",
        SimpleNamespace(
            database_url=None,
            pipeline_skip_dom_logging=True,
            uc_ignore_https_errors=configured_value,
        ),
    )
    monkeypatch.setattr(uc_main, "_assert_home_ready", AsyncMock(return_value=True))
    monkeypatch.setattr(uc_main, "_perform_login", AsyncMock(return_value=True))

    browser = _FakeBrowser()

    result = await uc_main.prepare_uc_api_page_for_store(
        browser=browser, store=store, logger=logger, source="test"
    )

    assert result.ok is True
    assert browser.context_kwargs == [
        {"storage_state": None, "ignore_https_errors": expected_ignore_https_errors}
    ]


def test_fake_browser_default_context_creation_uses_strict_tls_validation() -> None:
    browser = _FakeBrowser()

    context = asyncio.run(browser.new_context(storage_state=None))

    assert context in browser._contexts
    assert browser.context_kwargs == [{"storage_state": None, "ignore_https_errors": False}]


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
            setattr(page, "_uc_login_required", True)
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
        assert outcome.message in {"Archive Orders navigation failed", "navigation failed"}


@pytest.mark.asyncio
async def test_run_store_discovery_does_not_require_gst_ui_when_api_is_healthy(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    logger = JsonLogger(stream=io.StringIO(), log_file_path=None)
    summary = uc_main.UcOrdersDiscoverySummary(
        run_id="run-api",
        run_env="test",
        report_date=date(2025, 1, 1),
        report_end_date=date(2025, 1, 1),
        started_at=datetime.now(timezone.utc),
        store_codes=["A100"],
    )

    store = uc_main.UcStore(
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

    (tmp_path / f"{store.store_code}_storage_state.json").write_text("{}")

    monkeypatch.setattr(
        uc_main,
        "config",
        SimpleNamespace(
            database_url=None,
            pipeline_skip_dom_logging=True,
            uc_ignore_https_errors=False,
        ),
    )
    monkeypatch.setattr(uc_main, "default_profiles_dir", lambda: tmp_path)
    monkeypatch.setattr(uc_main, "_resolve_uc_download_dir", lambda *_: tmp_path)
    monkeypatch.setattr(uc_main, "_insert_orders_sync_log", AsyncMock(return_value=1))
    monkeypatch.setattr(uc_main, "_update_orders_sync_log", AsyncMock())
    monkeypatch.setattr(uc_main, "_assert_home_ready", AsyncMock(return_value=True))
    monkeypatch.setattr(uc_main, "_perform_login", AsyncMock(return_value=True))
    navigate_gst_mock = AsyncMock(return_value=False)
    direct_gst_mock = AsyncMock(return_value=(False, None))
    monkeypatch.setattr(uc_main, "_navigate_to_gst_reports", navigate_gst_mock)
    monkeypatch.setattr(uc_main, "_try_direct_gst_reports", direct_gst_mock)

    monkeypatch.setattr(
        uc_main,
        "collect_gst_orders_via_api",
        AsyncMock(
            return_value=uc_main.GstApiExtract(
                gst_rows=[{"order_number": "UC-A1"}],
                base_rows=[{"order_code": "UC-A1"}],
                order_detail_rows=[],
                payment_detail_rows=[],
            )
        ),
    )
    monkeypatch.setattr(uc_main, "_navigate_to_archive_orders", AsyncMock(return_value=False))

    browser = _FakeBrowser()

    await uc_main._run_store_discovery(
        browser=browser,
        store=store,
        logger=logger,
        run_env="test",
        run_id="run-api",
        run_date=datetime.now(timezone.utc),
        summary=summary,
        from_date=date(2025, 1, 1),
        to_date=date(2025, 1, 1),
        download_timeout_ms=1000,
    )

    assert navigate_gst_mock.await_count == 0
    assert direct_gst_mock.await_count == 0
    assert summary.store_outcomes["A100"].message in {"Archive Orders navigation failed", "navigation failed"}


@pytest.mark.asyncio
async def test_prepare_uc_api_page_for_store_reuses_valid_storage_state(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    logger = JsonLogger(stream=io.StringIO(), log_file_path=None)
    store = uc_main.UcStore(
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
    (tmp_path / f"{store.store_code}_storage_state.json").write_text("{}")
    monkeypatch.setattr(uc_main, "default_profiles_dir", lambda: tmp_path)
    home_ready = AsyncMock(return_value=True)
    login = AsyncMock(return_value=True)
    monkeypatch.setattr(uc_main, "_assert_home_ready", home_ready)
    monkeypatch.setattr(uc_main, "_perform_login", login)

    browser = _FakeBrowser()

    result = await uc_main.prepare_uc_api_page_for_store(
        browser=browser, store=store, logger=logger, source="test"
    )

    assert result.ok is True
    assert result.page is not None
    assert result.context is not None
    assert result.login_used is False
    assert result.session_probe_result is True
    assert result.fallback_login_attempted is False
    assert login.await_count == 0
    assert home_ready.await_count == 1
    assert browser.context_kwargs == [
        {
            "storage_state": str(tmp_path / "A100_storage_state.json"),
            "ignore_https_errors": False,
        }
    ]


@pytest.mark.asyncio
async def test_prepare_uc_api_page_for_store_refreshes_expired_storage_state(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    logger = JsonLogger(stream=io.StringIO(), log_file_path=None)
    store = uc_main.UcStore(
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
    storage_path = tmp_path / f"{store.store_code}_storage_state.json"
    storage_path.write_text("{}")
    monkeypatch.setattr(uc_main, "default_profiles_dir", lambda: tmp_path)

    async def _probe(*, page, store, logger, source):
        if source != "post-login":
            setattr(page, "_uc_login_required", True)
            return False
        return True

    async def _login(*, page, store, logger):
        page.session_state = "valid"
        page.url = store.home_url or page.url
        return True

    monkeypatch.setattr(uc_main, "_assert_home_ready", AsyncMock(side_effect=_probe))
    login = AsyncMock(side_effect=_login)
    monkeypatch.setattr(uc_main, "_perform_login", login)

    browser = _FakeBrowser()

    result = await uc_main.prepare_uc_api_page_for_store(
        browser=browser, store=store, logger=logger, source="test"
    )

    assert result.ok is True
    assert result.login_used is True
    assert result.session_probe_result is False
    assert result.fallback_login_attempted is True
    assert result.fallback_login_result is True
    assert login.await_count == 1
    assert storage_path.read_text() == "{}"


@pytest.mark.asyncio
async def test_prepare_uc_api_page_reports_post_login_api_401_as_token_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    stream = io.StringIO()
    logger = JsonLogger(stream=stream, log_file_path=None)
    store = uc_main.UcStore(
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
    (tmp_path / f"{store.store_code}_storage_state.json").write_text("{}")
    monkeypatch.setattr(uc_main, "default_profiles_dir", lambda: tmp_path)

    async def _probe(*, page, store, logger, source):
        if source == "session":
            setattr(page, "_uc_login_required", True)
            return False
        assert source == "post-login"
        uc_main._set_uc_readiness_failure(
            page,
            message="UC post-login API probe unauthorized",
            failure_class=uc_main.UC_FAILURE_AUTH_TOKEN_NOT_APPLIED,
        )
        setattr(page, "_uc_api_readiness_unauthorized", True)
        return False

    async def _login(*, page, store, logger):
        page.session_state = "valid"
        page.url = store.home_url or page.url
        return True

    monkeypatch.setattr(uc_main, "_assert_home_ready", AsyncMock(side_effect=_probe))
    monkeypatch.setattr(uc_main, "_perform_login", AsyncMock(side_effect=_login))

    result = await uc_main.prepare_uc_api_page_for_store(
        browser=_FakeBrowser(), store=store, logger=logger, source="test"
    )

    assert result.ok is False
    assert result.message == "UC post-login API probe unauthorized"
    assert result.login_error_code is None
    assert result.reason_codes == []
    assert result.fallback_login_result is False
    output = stream.getvalue()
    assert "UC post-login API probe unauthorized" in output
    assert '"failure_class": "uc_auth_token_not_applied"' in output
    assert "Login failed after session probe" not in output
    assert "Login DOM mismatch after session probe" not in output


@pytest.mark.asyncio
async def test_run_store_discovery_uses_uc_api_page_preparation_helper(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    logger = JsonLogger(stream=io.StringIO(), log_file_path=None)
    summary = uc_main.UcOrdersDiscoverySummary(
        run_id="run-helper",
        run_env="test",
        report_date=date(2025, 1, 1),
        report_end_date=date(2025, 1, 1),
        started_at=datetime.now(timezone.utc),
        store_codes=["A100"],
    )
    store = uc_main.UcStore(
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
    monkeypatch.setattr(uc_main, "default_profiles_dir", lambda: tmp_path)
    monkeypatch.setattr(uc_main, "_resolve_uc_download_dir", lambda *_: tmp_path)
    monkeypatch.setattr(uc_main, "_insert_orders_sync_log", AsyncMock(return_value=1))
    monkeypatch.setattr(uc_main, "_update_orders_sync_log", AsyncMock())
    monkeypatch.setattr(uc_main, "_navigate_to_archive_orders", AsyncMock(return_value=False))
    monkeypatch.setattr(uc_main, "_navigate_to_gst_reports", AsyncMock(return_value=False))
    monkeypatch.setattr(uc_main, "_try_direct_gst_reports", AsyncMock(return_value=(False, None)))
    monkeypatch.setattr(
        uc_main,
        "collect_gst_orders_via_api",
        AsyncMock(
            return_value=uc_main.GstApiExtract(
                gst_rows=[],
                base_rows=[],
                order_detail_rows=[],
                payment_detail_rows=[],
            )
        ),
    )

    browser = _FakeBrowser()
    context = await browser.new_context()
    page = await context.new_page()
    prepare = AsyncMock(
        return_value=uc_main.UcApiPagePreparationResult(
            ok=True,
            message="ready",
            context=context,
            page=page,
            login_used=False,
            session_probe_result=True,
            fallback_login_attempted=False,
            fallback_login_result=None,
        )
    )
    monkeypatch.setattr(uc_main, "prepare_uc_api_page_for_store", prepare)

    await uc_main._run_store_discovery(
        browser=browser,
        store=store,
        logger=logger,
        run_env="test",
        run_id="run-helper",
        run_date=datetime.now(timezone.utc),
        summary=summary,
        from_date=date(2025, 1, 1),
        to_date=date(2025, 1, 1),
        download_timeout_ms=1000,
    )

    prepare.assert_awaited_once()
    assert prepare.await_args.kwargs["source"] == "uc_orders_sync"
    assert uc_main.collect_gst_orders_via_api.await_count == 1


class _MappedLocator:
    def __init__(self, *, visible: bool = False, html: str = "") -> None:
        self.visible = visible
        self.html = html

    @property
    def first(self) -> "_MappedLocator":
        return self

    async def wait_for(self, *, state: str, timeout: int) -> None:
        assert state == "visible"
        assert timeout > 0
        if not self.visible:
            raise uc_main.TimeoutError("selector not visible")

    async def is_visible(self) -> bool:
        return self.visible

    async def count(self) -> int:
        return 1 if self.visible else 0


class _FakeApiResponse:
    def __init__(self, *, status: int, payload: object) -> None:
        self.status = status
        self._payload = payload

    async def json(self) -> object:
        return self._payload


class _FakeRequestContext:
    def __init__(self, responses: list[_FakeApiResponse]) -> None:
        self._responses = responses
        self.urls: list[str] = []
        self.headers: list[dict[str, str] | None] = []

    async def get(
        self, url: str, *, timeout: int, headers: dict[str, str] | None = None
    ) -> _FakeApiResponse:
        assert timeout == 15_000
        self.urls.append(url)
        self.headers.append(headers)
        if not self._responses:
            raise AssertionError("unexpected API readiness request")
        return self._responses.pop(0)


class _DashboardProbePage:
    def __init__(
        self,
        *,
        url: str,
        visible_selectors: set[str],
        html: str,
        api_responses: list[_FakeApiResponse] | None = None,
        title: str = "UC Dashboard",
    ) -> None:
        self.url = url
        self.visible_selectors = visible_selectors
        self._html = html
        self.request = _FakeRequestContext(api_responses or [])
        self._title = title
        self.screenshot_path: str | None = None

    async def wait_for_url(self, predicate, *, timeout: int) -> None:
        assert timeout == uc_main.NAV_TIMEOUT_MS
        if not predicate(self.url):
            raise uc_main.TimeoutError("home URL did not match")

    def locator(self, selector: str) -> _MappedLocator:
        return _MappedLocator(visible=selector in self.visible_selectors)

    async def title(self) -> str:
        return self._title

    async def content(self) -> str:
        return self._html

    async def screenshot(self, *, path: str, full_page: bool) -> None:
        assert full_page is True
        self.screenshot_path = path
        Path(path).write_bytes(b"fake-png")

    async def evaluate(self, _script: str) -> None:
        return None


@pytest.mark.asyncio
async def test_uc_dashboard_shell_without_tracker_card_logs_final_failure_payload(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    stream = io.StringIO()
    logger = JsonLogger(stream=stream, log_file_path=None)
    store = uc_main.UcStore(
        store_code="UC567",
        store_name="UC Store",
        cost_center="SC3567",
        sync_config={
            "urls": {
                "home": "https://storepanel.ucleanlaundry.com/dashboard",
                "login": "https://store.ucleanlaundry.com/login",
            }
        },
    )
    page = _DashboardProbePage(
        url="https://storepanel.ucleanlaundry.com/dashboard",
        visible_selectors={"nav"},
        html='<html><input type="password" value="secret"><body>operator@example.com</body></html>',
    )
    setattr(page, "_uc_home_response_status", 200)
    setattr(
        page,
        "_uc_home_response_url",
        "https://storepanel.ucleanlaundry.com/dashboard",
    )
    monkeypatch.setattr(uc_main, "default_download_dir", lambda: tmp_path)
    monkeypatch.setattr(
        uc_main,
        "config",
        SimpleNamespace(pipeline_skip_dom_logging=True, uc_ignore_https_errors=False),
    )

    ready = await uc_main._wait_for_home_ready(
        page=page, store=store, logger=logger, source="final-attempt"
    )

    assert ready is False
    output = stream.getvalue()
    assert (
        "UC dashboard shell loaded but legacy Daily Operations Tracker "
        "card selector is missing; API readiness did not succeed"
    ) in output
    assert "UC API readiness failed after dashboard shell loaded" in output
    assert '"failure_class": "uc_auth_token_not_applied"' in output
    assert '"final_url": "https://storepanel.ucleanlaundry.com/dashboard"' in output
    assert '"page_title": "UC Dashboard"' in output
    assert '"response_status": 200' in output
    assert '"login_page_visible": false' in output
    assert '"dashboard_shell_visible": true' in output
    assert '"target_card_visible": false' in output
    artifact_dir = tmp_path / "uc_navigation_failures"
    html_artifacts = list(artifact_dir.glob("*.html"))
    png_artifacts = list(artifact_dir.glob("*.png"))
    assert len(html_artifacts) == 1
    assert len(png_artifacts) == 1
    sanitized_html = html_artifacts[0].read_text(encoding="utf-8")
    assert "secret" not in sanitized_html
    assert "operator@example.com" not in sanitized_html
    assert "[REDACTED" in sanitized_html


@pytest.mark.asyncio
async def test_uc_dashboard_shell_without_tracker_card_is_ready_when_api_probe_succeeds() -> None:
    stream = io.StringIO()
    logger = JsonLogger(stream=stream, log_file_path=None)
    store = uc_main.UcStore(
        store_code="UC611",
        store_name="UC Store 611",
        cost_center="SC3611",
        sync_config={
            "urls": {
                "home": "https://storepanel.ucleanlaundry.com/dashboard",
                "login": "https://store.ucleanlaundry.com/login",
            }
        },
    )
    page = _DashboardProbePage(
        url="https://storepanel.ucleanlaundry.com/dashboard",
        visible_selectors={"nav"},
        html="<html></html>",
        api_responses=[_FakeApiResponse(status=200, payload={"latestOrderId": 12345})],
    )
    setattr(page, "_uc_home_response_status", 200)
    setattr(
        page,
        "_uc_home_response_url",
        "https://storepanel.ucleanlaundry.com/dashboard",
    )

    ready = await uc_main._wait_for_home_ready(
        page=page, store=store, logger=logger, source="test"
    )

    assert ready is True
    output = stream.getvalue()
    assert (
        "legacy Daily Operations Tracker card selector is missing; "
        "API readiness succeeded"
    ) in output
    assert '"response_status": 200' in output
    assert '"target_card_visible": false' in output
    assert '"api_probe_ready": true' in output
    assert '"status": "info"' in output

@pytest.mark.asyncio
@pytest.mark.parametrize("status", [401, 403])
async def test_uc_dashboard_shell_api_unauthorized_requires_login(status: int) -> None:
    stream = io.StringIO()
    logger = JsonLogger(stream=stream, log_file_path=None)
    store = uc_main.UcStore(
        store_code="UC612",
        store_name="UC Store 612",
        cost_center="SC3612",
        sync_config={
            "urls": {
                "home": "https://storepanel.ucleanlaundry.com/dashboard",
                "login": "https://store.ucleanlaundry.com/login",
            }
        },
    )
    page = _DashboardProbePage(
        url="https://storepanel.ucleanlaundry.com/dashboard",
        visible_selectors={"nav"},
        html="<html></html>",
        api_responses=[
            _FakeApiResponse(status=status, payload={"message": "unauthorized"})
        ],
    )

    ready = await uc_main._wait_for_home_ready(
        page=page, store=store, logger=logger, source="test"
    )

    assert ready is False
    assert getattr(page, "_uc_login_required") is True
    output = stream.getvalue()
    assert "UC API readiness probe returned unauthorized; login required" in output
    assert f'"api_probe_status": {status}' in output
    assert '"api_probe_unauthorized": true' in output


@pytest.mark.asyncio
async def test_uc_login_page_visible_is_not_home_ready() -> None:
    stream = io.StringIO()
    logger = JsonLogger(stream=stream, log_file_path=None)
    store = uc_main.UcStore(
        store_code="UC613",
        store_name="UC Store 613",
        cost_center="SC3613",
        sync_config={
            "urls": {
                "home": "https://storepanel.ucleanlaundry.com/dashboard",
                "login": "https://store.ucleanlaundry.com/login",
            }
        },
    )
    page = _DashboardProbePage(
        url="https://store.ucleanlaundry.com/login",
        visible_selectors={"#email", "#password"},
        html='<html><input id="email"><input id="password" type="password"></html>',
        api_responses=[_FakeApiResponse(status=200, payload={"latestOrderId": 12345})],
    )

    ready = await uc_main._wait_for_home_ready(
        page=page, store=store, logger=logger, source="test"
    )

    assert ready is False
    assert getattr(page, "_uc_login_required") is True
    output = stream.getvalue()
    assert "Home page not reached; login/session page is visible" in output
    assert '"login_page_visible": true' in output


@pytest.mark.asyncio
async def test_uc_dashboard_tracker_card_visible_still_requires_api_readiness() -> None:
    stream = io.StringIO()
    logger = JsonLogger(stream=stream, log_file_path=None)
    store = uc_main.UcStore(
        store_code="UC610",
        store_name="UC Store 610",
        cost_center="SC3610",
        sync_config={
            "urls": {
                "home": "https://storepanel.ucleanlaundry.com/dashboard",
                "login": "https://store.ucleanlaundry.com/login",
            }
        },
    )
    page = _DashboardProbePage(
        url="https://storepanel.ucleanlaundry.com/dashboard",
        visible_selectors={"nav", uc_main.UC_DASHBOARD_CARD_SELECTOR},
        html="<html></html>",
    )

    ready = await uc_main._wait_for_home_ready(
        page=page, store=store, logger=logger, source="test"
    )

    assert ready is False
    output = stream.getvalue()
    assert "UC home staged readiness probe" in output
    assert "UC API readiness failed after dashboard shell loaded" in output
    assert '"failure_class": "uc_auth_token_not_applied"' in output
    assert '"target_card_visible": true' in output
    assert '"api_probe_ready": false' in output


class _AuthenticatedReadinessPage(_DashboardProbePage):
    def __init__(self, *, login_payload: dict[str, object]) -> None:
        super().__init__(
            url="https://storepanel.ucleanlaundry.com/dashboard",
            visible_selectors={"nav"},
            html="<html><nav></nav></html>",
            api_responses=[
                _FakeApiResponse(status=401, payload={"message": "unauthorized"})
            ],
        )
        self.local_storage: dict[str, str] = {}
        self.fetch_calls: list[dict[str, object]] = []
        self.login_payload = login_payload

    async def evaluate(self, script: str, arg: object | None = None) -> object:
        if arg == "uc.jwt.token":
            self.local_storage["token"] = "Bearer uc.jwt.token"
            self.local_storage["authToken"] = "Bearer uc.jwt.token"
            return None
        if "window.localStorage" in script and arg is None:
            return self.local_storage.get("token")
        if isinstance(arg, dict) and "requestHeaders" in arg:
            headers = arg["requestHeaders"]
            assert isinstance(headers, dict)
            self.fetch_calls.append(headers)
            if headers.get("Authorization") == "Bearer uc.jwt.token":
                return {"status": 200, "payload": {"latestOrderId": 12345}}
            return {"status": 401, "payload": {"message": "unauthorized"}}
        return None


@pytest.mark.asyncio
async def test_uc_login_token_authenticates_readiness_when_dashboard_card_missing() -> None:
    stream = io.StringIO()
    logger = JsonLogger(stream=stream, log_file_path=None)
    store = uc_main.UcStore(
        store_code="UC614",
        store_name="UC Store 614",
        cost_center="SC3614",
        sync_config={
            "urls": {
                "home": "https://storepanel.ucleanlaundry.com/dashboard",
                "login": "https://store.ucleanlaundry.com/login",
            }
        },
    )
    page = _AuthenticatedReadinessPage(
        login_payload={"data": {"token": "uc.jwt.token"}}
    )
    login_token = uc_main._extract_uc_login_bearer_token(page.login_payload)

    assert login_token == "uc.jwt.token"
    assert await uc_main._persist_uc_login_bearer_token(page=page, token=login_token)

    ready = await uc_main._wait_for_home_ready(
        page=page, store=store, logger=logger, source="login"
    )

    assert ready is True
    assert page.request.headers[0]["Authorization"] == "Bearer uc.jwt.token"
    assert page.request.headers[0]["Origin"] == "https://storepanel.ucleanlaundry.com"
    assert page.request.headers[0]["Referer"] == "https://storepanel.ucleanlaundry.com/"
    assert page.fetch_calls[0]["Authorization"] == "Bearer uc.jwt.token"
    output = stream.getvalue()
    assert "Login failed after session probe" not in output
    assert '"target_card_visible": false' in output
    assert '"api_probe_ready": true' in output
    assert '"api_probe_unauthorized": false' in output
