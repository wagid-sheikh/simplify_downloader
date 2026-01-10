import asyncio
import io
import json
from pathlib import Path

from app.dashboard_downloader import run_downloads
from app.dashboard_downloader.json_logger import JsonLogger
from app.dashboard_downloader.run_downloads import (
    HOME_URL,
    LOGIN_URL,
    TD_BASE_URL,
    _bootstrap_session_via_home_and_tracker,
)


class FakeResponse:
    def __init__(self, status: int | None = None):
        self.status = status


class FakeContext:
    def __init__(self, *, ignore_https_errors: bool = False):
        self.browser = self
        self.ignore_https_errors = ignore_https_errors

    async def storage_state(self, *args, **kwargs):  # pragma: no cover - interface parity
        return {}

    async def new_context(self, **kwargs):  # pragma: no cover - interface parity
        return self

    async def new_page(self):  # pragma: no cover - interface parity
        return FakePage(context=self)


class FakePage:
    def __init__(self, context: FakeContext):
        self.context = context
        self.url = "about:blank"

    async def close(self):  # pragma: no cover - interface parity
        return None


class FakeArtifactPage:
    def __init__(self):
        self.content_called = False
        self.screenshot_called = False

    async def screenshot(self, *, path: str, full_page: bool = True):
        self.screenshot_called = True
        Path(path).write_text("screenshot", encoding="utf-8")

    async def content(self):
        self.content_called = True
        return "<html><body>content</body></html>"


def run(coro):
    return asyncio.run(coro)


async def _async_false(*_args, **_kwargs):
    return False


def test_bootstrap_bypasses_login_when_session_already_active(monkeypatch, tmp_path):
    log_stream = io.StringIO()
    logger = JsonLogger(run_id="test", stream=log_stream, log_file_path=None)

    fake_context = FakeContext()
    page = FakePage(context=fake_context)

    probe_payload = {"probe_url": HOME_URL, "current_url": HOME_URL}

    async def fake_run_session_probe(*_args, **_kwargs):
        return False, dict(probe_payload), fake_context

    async def fake_navigate_with_retry(page, url, timeout_ms, logger=None, store_code=None):
        page.url = TD_BASE_URL if "login" in url else url
        return page, FakeResponse(status=200)

    async def fake_persist_storage_state(ctx, *, target_path, logger, store_code):
        return tmp_path / "state.json"

    async def fake_navigate_via_home(page, store_cfg, logger, **kwargs):
        return page

    monkeypatch.setattr(run_downloads, "_run_session_probe", fake_run_session_probe)
    monkeypatch.setattr(run_downloads, "navigate_with_retry", fake_navigate_with_retry)
    monkeypatch.setattr(run_downloads, "_persist_storage_state", fake_persist_storage_state)
    monkeypatch.setattr(run_downloads, "_navigate_via_home_to_dashboard", fake_navigate_via_home)
    monkeypatch.setattr(run_downloads, "_is_login_page", _async_false)
    monkeypatch.setattr(run_downloads, "_resolve_global_credentials", lambda _settings: ("user", "pass"))

    store_cfg = {"store_code": "001", "home_url": HOME_URL, "login_url": LOGIN_URL}

    result_page = run(
        _bootstrap_session_via_home_and_tracker(
            page,
            store_cfg,
            logger,
            settings=None,
            storage_state_file=None,
            storage_state_source=None,
            nav_timeout_ms=5_000,
        )
    )

    assert result_page is page

    logs = [json.loads(line) for line in log_stream.getvalue().splitlines() if line.strip()]
    assert any(
        entry.get("message") == "login page bypassed; session already active"
        and entry.get("extras", {}).get("already_authenticated") is True
        for entry in logs
    )


def test_bootstrap_switches_to_insecure_context_after_probe(monkeypatch, tmp_path):
    log_stream = io.StringIO()
    logger = JsonLogger(run_id="test", stream=log_stream, log_file_path=None)

    secure_context = FakeContext(ignore_https_errors=False)
    insecure_context = FakeContext(ignore_https_errors=True)
    page = FakePage(context=secure_context)

    probe_payload = {"probe_url": HOME_URL, "current_url": HOME_URL}
    navigation_contexts: list[bool] = []

    async def fake_run_session_probe(*_args, **_kwargs):
        return True, dict(probe_payload), insecure_context

    async def fake_navigate_with_retry(page, url, timeout_ms, logger=None, store_code=None):
        navigation_contexts.append(page.context.ignore_https_errors)
        page.url = url
        return page, FakeResponse(status=200)

    async def fake_persist_storage_state(ctx, *, target_path, logger, store_code):
        return tmp_path / "state.json"

    async def fake_navigate_via_home(page, store_cfg, logger, **kwargs):
        return page

    monkeypatch.setattr(run_downloads, "_run_session_probe", fake_run_session_probe)
    monkeypatch.setattr(run_downloads, "navigate_with_retry", fake_navigate_with_retry)
    monkeypatch.setattr(run_downloads, "_persist_storage_state", fake_persist_storage_state)
    monkeypatch.setattr(run_downloads, "_navigate_via_home_to_dashboard", fake_navigate_via_home)
    monkeypatch.setattr(run_downloads, "_is_login_page", _async_false)
    monkeypatch.setattr(run_downloads, "_resolve_global_credentials", lambda _settings: ("user", "pass"))

    store_cfg = {"store_code": "001", "home_url": HOME_URL, "login_url": LOGIN_URL}

    result_page = run(
        _bootstrap_session_via_home_and_tracker(
            page,
            store_cfg,
            logger,
            settings=None,
            storage_state_file=None,
            storage_state_source=None,
            nav_timeout_ms=5_000,
        )
    )

    assert result_page.context.ignore_https_errors is True
    assert navigation_contexts == [True]


def test_capture_bootstrap_artifacts_skips_dom_logging(monkeypatch, tmp_path):
    artifacts_dir = tmp_path / "artifacts"
    monkeypatch.setattr(run_downloads, "BOOTSTRAP_ARTIFACTS_DIR", artifacts_dir)
    page = FakeArtifactPage()

    extras = run(
        run_downloads._capture_bootstrap_artifacts(
            page,
            store_code="001",
            prefix="unit_test",
            skip_dom_logging=True,
        )
    )

    assert "html_dump" not in extras
    assert page.content_called is False
    assert not list(artifacts_dir.glob("*.html"))
